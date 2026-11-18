"""PipedriveLakeflowConnect — Lakeflow Community Connector for the Pipedrive CRM API.

Authentication: personal API token sent in the ``x-api-token`` header against
``https://api.pipedrive.com`` (or ``https://{company_domain}.pipedrive.com``).

Tables and read strategies
--------------------------
    deals, deals_archived          cdc_with_deletes  v2, updated_since/until + sort update_time asc
    persons, organizations,
    activities                     cdc               v2, updated_since/until + sort update_time asc
    products                       cdc               v2, updated_since + sort (cap client-side)
    notes, leads                   cdc               v1, updated_since, id-sorted offset pages
    deal_flow                      append            v1 per-deal change log, parent-incremental
    everything else                snapshot          full read

Why not partitioned: Pipedrive enforces a strict per-token burst limit
(20 requests / 2 s on the lowest plan) and a daily token budget, so parallel
executor reads would only trade throughput for 429s / Cloudflare blocks.  All
reads run sequentially through one throttled client.

Incremental contract
--------------------
* ``self._init_ts`` is captured at construction; every incremental read is
  bounded by it (``updated_until`` server-side where supported, client-side
  otherwise) and short-circuits once its cursor reaches it, so
  ``Trigger.AvailableNow`` terminates.
* v2 tables sort by ``update_time`` ascending, so a micro-batch may stop after
  ``max_records_per_batch`` records and resume from the last cursor.
  ``updated_since`` is inclusive, so boundary rows are re-read and de-duplicated
  by the primary-key upsert.
* v1 notes / leads sort by immutable ``id`` (an edit can't shift rows across
  offset pages); the cursor is only valid once the whole filtered range is
  drained, so each micro-batch reads the full ``[since, init_ts]`` range and
  ``max_records_per_batch`` does not apply.
* An optional ``lookback_seconds`` is subtracted from the query lower bound on
  the first read of each table per trigger only; the stored cursor is never
  shifted.
"""

from typing import Iterator, Optional

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.pipedrive.pipedrive_schemas import (
    DEAL_PARENT_PATHS,
    FIELD_TABLES,
    JSON_STRING_ARRAY_FIELDS,
    JSON_STRING_FIELDS,
    SUPPORTED_TABLES,
    TABLE_CONFIG,
    TABLE_SCHEMAS,
    build_metadata,
)
from databricks.labs.community_connector.sources.pipedrive.pipedrive_utils import (
    CHILD_SKIP_CODES,
    DEFAULT_BASE_URL,
    DEFAULT_BURST_LIMIT,
    DEFAULT_START_DATE,
    OPTIONAL_TABLE_SKIP_CODES,
    PipedriveAPIError,
    PipedriveClient,
    normalize_record,
    normalize_ts,
    now_utc,
    record_cursor,
    shift_ts,
)

V2_PAGE_MAX = 500
DEFAULT_MAX_RECORDS_PER_BATCH = 5000
# v1 notes / leads: Airbyte applies a 1h lookback to guard against clock skew
# and late-committed writes.  v2 tables default to none.
DEFAULT_V1_LOOKBACK_SECONDS = 3600
DEFAULT_V2_LOOKBACK_SECONDS = 0
# deal_flow selects parent deals updated since (cursor - lookback); entries are
# then filtered by their own log_time, so a generous lookback is cheap insurance.
DEFAULT_DEAL_FLOW_LOOKBACK_SECONDS = 3600
DEAL_PRODUCTS_BATCH = 100  # /api/v2/deals/products accepts at most 100 deal_ids

DEALS_LIVE_STATUSES = "open,won,lost"
DEALS_DELETED_STATUS = "deleted"
DEALS_ALL_STATUSES = "open,won,lost,deleted"
PROJECT_ALL_STATUSES = "open,completed,canceled,deleted"


class PipedriveLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for Pipedrive (API v1 + v2)."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("Pipedrive connector requires 'api_token' in options")

        company_domain = (options.get("company_domain") or "").strip()
        if company_domain:
            # Accept "mycompany", "mycompany.pipedrive.com" or a full URL.
            host = company_domain.split("://", 1)[-1].strip("/")
            if not host.endswith(".pipedrive.com"):
                host = f"{host}.pipedrive.com"
            base_url = f"https://{host}"
        else:
            base_url = DEFAULT_BASE_URL

        start_date = options.get("start_date") or DEFAULT_START_DATE
        normalized_start = normalize_ts(start_date)
        if normalized_start is None:
            raise ValueError(f"Invalid 'start_date' {start_date!r}; expected RFC 3339")
        self._start_date = normalized_start

        burst_limit = int(options.get("burst_limit") or DEFAULT_BURST_LIMIT)
        self._client = PipedriveClient(api_token, base_url, burst_limit=burst_limit)

        # Upper bound for every incremental read in this trigger.  Data changed
        # after this instant is picked up by the next trigger (fresh instance).
        self._init_ts = now_utc()
        # Tables (and their delete streams) whose one-time lookback was applied.
        self._lookback_applied: set[str] = set()

    # ------------------------------------------------------------------ #
    # Interface: discovery / schema / metadata
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return build_metadata(table_name)

    # ------------------------------------------------------------------ #
    # Interface: reads
    # ------------------------------------------------------------------ #

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        table_options = table_options or {}
        kind = TABLE_CONFIG[table_name]["kind"]

        if kind == "v2_incremental":
            status = DEALS_LIVE_STATUSES if TABLE_CONFIG[table_name].get("deal_status") else None
            return self._read_v2_incremental(
                table_name, start_offset, table_options, status=status, stream_key=table_name
            )
        if kind == "v1_incremental":
            return self._read_v1_incremental(table_name, start_offset, table_options)
        if kind == "deal_flow":
            return self._read_deal_flow(start_offset, table_options)
        if kind == "v2_snapshot":
            return self._read_v2_snapshot(table_name), {}
        if kind == "v1_snapshot":
            return self._read_v1_snapshot(table_name), {}
        if kind == "projects":
            return self._read_projects(), {}
        if kind == "deal_products":
            return self._read_deal_products(), {}
        raise ValueError(f"Unhandled table kind {kind!r} for '{table_name}'")

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Soft-deleted deals (``status=deleted``, last 30 days) as delete tombstones.

        Pipedrive keeps deleted deals for 30 days with ``is_deleted=true`` and a
        bumped ``update_time``; ``status=deleted`` returns only those.  This is a
        separate stream with its own offset, read with the same incremental
        algorithm as ``read_table``.
        """
        self._validate_table(table_name)
        if TABLE_CONFIG[table_name].get("ingestion_type") != "cdc_with_deletes":
            raise ValueError(f"Table '{table_name}' does not support deleted records")
        return self._read_v2_incremental(
            table_name,
            start_offset,
            table_options or {},
            status=DEALS_DELETED_STATUS,
            stream_key=f"{table_name}::deletes",
        )

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_CONFIG:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {SUPPORTED_TABLES}"
            )

    def _normalize(self, table_name: str, record: dict) -> dict:
        cfg = TABLE_CONFIG[table_name]
        return normalize_record(
            record,
            json_fields=JSON_STRING_FIELDS.get(table_name, ()),
            json_array_fields=JSON_STRING_ARRAY_FIELDS if table_name in FIELD_TABLES else (),
            lead_custom_fields=bool(cfg.get("lead_custom_fields")),
            field_table=table_name in FIELD_TABLES,
            deal_flow=table_name == "deal_flow",
            fill_update_time=cfg.get("ingestion_type") in ("cdc", "cdc_with_deletes"),
        )

    @staticmethod
    def _int_option(table_options: dict, name: str, default: int, minimum: int = 0) -> int:
        raw = table_options.get(name)
        if raw is None or raw == "":
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Table option '{name}' must be an integer, got {raw!r}") from exc
        return max(minimum, value)

    def _query_lower_bound(self, stream_key: str, since: str, lookback_seconds: int) -> str:
        """Apply the lookback once per stream per trigger (never to the stored cursor)."""
        if lookback_seconds <= 0 or stream_key in self._lookback_applied:
            return since
        self._lookback_applied.add(stream_key)
        return shift_ts(since, -lookback_seconds)

    def _resolve_since(self, start_offset: Optional[dict]) -> Optional[str]:
        """Starting cursor, or ``None`` if the stream already caught up to init time."""
        cursor = (start_offset or {}).get("cursor")
        if cursor and cursor >= self._init_ts:
            return None
        return cursor or self._start_date

    # ------------------------------------------------------------------ #
    # v2 incremental (deals, deals_archived, persons, organizations,
    # activities, products)
    # ------------------------------------------------------------------ #

    def _read_v2_incremental(
        self,
        table_name: str,
        start_offset: Optional[dict],
        table_options: dict,
        *,
        status: Optional[str],
        stream_key: str,
    ) -> tuple[Iterator[dict], dict]:
        cfg = TABLE_CONFIG[table_name]
        since = self._resolve_since(start_offset)
        if since is None:
            return iter([]), start_offset

        max_records = self._int_option(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        page_size = min(
            V2_PAGE_MAX,
            self._int_option(table_options, "page_size", V2_PAGE_MAX, minimum=1),
            max_records,
        )
        lookback = self._int_option(table_options, "lookback_seconds", DEFAULT_V2_LOOKBACK_SECONDS)

        params = {
            "limit": page_size,
            "sort_by": "update_time",
            "sort_direction": "asc",
            "updated_since": self._query_lower_bound(stream_key, since, lookback),
        }
        if cfg.get("supports_until"):
            params["updated_until"] = self._init_ts
        if status:
            params["status"] = status
        include_fields = table_options.get("include_fields", cfg.get("default_include_fields"))
        if include_fields:
            params["include_fields"] = include_fields

        records: list[dict] = []
        last_cursor = since
        for page in self._client.iter_v2_pages(cfg["path"], params):
            reached_cap = False
            for raw in page:
                rc = record_cursor(raw)
                if rc is not None and rc > self._init_ts:
                    # Sorted ascending: everything after this is past the cap
                    # (only reachable where updated_until isn't supported).
                    reached_cap = True
                    break
                records.append(self._normalize(table_name, raw))
                if rc is not None and rc > last_cursor:
                    last_cursor = rc
            if reached_cap:
                break
            # Stop at a page boundary once the batch is full - but only after
            # the cursor has moved past ``since``; otherwise a run of rows that
            # share one update_time would never advance the offset.
            if len(records) >= max_records and last_cursor > since:
                break

        end_offset = {"cursor": last_cursor}
        if not records or (start_offset and start_offset == end_offset):
            return iter([]), start_offset if start_offset else end_offset
        return iter(records), end_offset

    # ------------------------------------------------------------------ #
    # v1 incremental (notes, leads)
    # ------------------------------------------------------------------ #

    def _read_v1_incremental(
        self, table_name: str, start_offset: Optional[dict], table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        cfg = TABLE_CONFIG[table_name]
        since = self._resolve_since(start_offset)
        if since is None:
            return iter([]), start_offset

        page_size = min(
            cfg.get("page_max", 500),
            self._int_option(table_options, "page_size", cfg.get("page_max", 500), minimum=1),
        )
        lookback = self._int_option(table_options, "lookback_seconds", DEFAULT_V1_LOOKBACK_SECONDS)
        params = {
            "limit": page_size,
            "start": 0,
            # Immutable sort key: an edit mid-read can't move a row onto an
            # already-read offset page.
            "sort": "id ASC",
            "updated_since": self._query_lower_bound(table_name, since, lookback),
        }

        # The cursor is only safe once the whole id-sorted range is drained,
        # so read every page in one micro-batch.
        records: list[dict] = []
        last_cursor = since
        for page in self._client.iter_v1_pages(cfg["path"], params):
            for raw in page:
                rc = record_cursor(raw)
                if rc is not None and rc > self._init_ts:
                    continue  # changed after init: next trigger picks it up
                records.append(self._normalize(table_name, raw))
                if rc is not None and rc > last_cursor:
                    last_cursor = rc

        end_offset = {"cursor": last_cursor}
        if not records or (start_offset and start_offset == end_offset):
            return iter([]), start_offset if start_offset else end_offset
        return iter(records), end_offset

    # ------------------------------------------------------------------ #
    # Snapshots
    # ------------------------------------------------------------------ #

    def _read_v2_snapshot(self, table_name: str) -> Iterator[dict]:
        cfg = TABLE_CONFIG[table_name]
        try:
            for page in self._client.iter_v2_pages(cfg["path"], {"limit": V2_PAGE_MAX}):
                for raw in page:
                    yield self._normalize(table_name, raw)
        except PipedriveAPIError as exc:
            if cfg.get("optional") and self._is_feature_unavailable(exc):
                return
            raise

    def _read_v1_snapshot(self, table_name: str) -> Iterator[dict]:
        cfg = TABLE_CONFIG[table_name]
        params: dict = {}
        if cfg.get("paginated"):
            params = {"start": 0, "limit": cfg.get("page_max", 500)}
            if cfg.get("sort"):
                params["sort"] = cfg["sort"]
        for page in self._client.iter_v1_pages(cfg["path"], params):
            for raw in page:
                yield self._normalize(table_name, raw)

    def _read_projects(self) -> Iterator[dict]:
        """Union of /projects and /projects/archived (all statuses), deduped on id."""
        cfg = TABLE_CONFIG["projects"]
        seen: set = set()
        params = {"limit": V2_PAGE_MAX, "status": PROJECT_ALL_STATUSES}
        try:
            for path in cfg["paths"]:
                for page in self._client.iter_v2_pages(path, params):
                    for raw in page:
                        rid = raw.get("id")
                        if rid in seen:
                            continue
                        seen.add(rid)
                        yield self._normalize("projects", raw)
        except PipedriveAPIError as exc:
            if self._is_feature_unavailable(exc):
                return
            raise

    @staticmethod
    def _is_feature_unavailable(exc: PipedriveAPIError) -> bool:
        """Projects add-on missing: 402/403/415 JSON errors, or a JSON 404.

        An HTML 403 is a Cloudflare block and an HTML 404 is an unknown route;
        both are real failures.
        """
        if not exc.is_json:
            return False
        return exc.status_code in OPTIONAL_TABLE_SKIP_CODES or exc.status_code == 404

    # ------------------------------------------------------------------ #
    # Deal children
    # ------------------------------------------------------------------ #

    def _iter_deal_ids(self) -> list:
        """All (non-deleted) deal IDs from /deals and /deals/archived, deduped."""
        ids: dict = {}
        for path in DEAL_PARENT_PATHS:
            for page in self._client.iter_v2_pages(path, {"limit": V2_PAGE_MAX}):
                for deal in page:
                    if deal.get("id") is not None:
                        ids[deal["id"]] = None
        return list(ids)

    def _read_deal_products(self) -> Iterator[dict]:
        deal_ids = self._iter_deal_ids()
        path = TABLE_CONFIG["deal_products"]["path"]
        for i in range(0, len(deal_ids), DEAL_PRODUCTS_BATCH):
            chunk = deal_ids[i : i + DEAL_PRODUCTS_BATCH]
            params = {"deal_ids": ",".join(str(d) for d in chunk), "limit": V2_PAGE_MAX}
            for page in self._client.iter_v2_pages(path, params):
                for raw in page:
                    yield self._normalize("deal_products", raw)

    def _read_deal_flow(
        self, start_offset: Optional[dict], table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        """Append-only deal change log, read via parent-incremental deals.

        Each trigger covers the log-time window ``(cursor, until]`` where
        ``until`` is the init time.  Parent deals updated in
        ``[cursor - lookback, until]`` are walked in ascending ``update_time``
        order — first ``/deals`` then ``/deals/archived`` — and for each one the
        flow entries inside the window are emitted.  The window is split across
        micro-batches by deal (``max_records_per_batch`` is best-effort: a
        deal's flow is never split).  In-progress offsets carry the phase and
        the ``update_time`` of the next unprocessed deal.
        """
        offset = start_offset or {}
        lookback = self._int_option(
            table_options, "lookback_seconds", DEFAULT_DEAL_FLOW_LOOKBACK_SECONDS
        )
        if offset.get("phase") is not None:
            lower = offset["cursor"]
            until = offset["until"]
            phase = int(offset["phase"])
            deal_since = offset["deal_since"]
        else:
            lower = self._resolve_since(start_offset)
            if lower is None:
                return iter([]), start_offset
            until = self._init_ts
            phase = 0
            deal_since = shift_ts(lower, -lookback) if lookback > 0 else lower

        max_records = self._int_option(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        params = {
            "limit": V2_PAGE_MAX,
            "sort_by": "update_time",
            "sort_direction": "asc",
            "updated_since": deal_since,
            "updated_until": until,
            "status": DEALS_ALL_STATUSES,
        }

        records: list[dict] = []
        last_deal_ts: Optional[str] = None
        stopped_at: Optional[str] = None
        for page in self._client.iter_v2_pages(DEAL_PARENT_PATHS[phase], params):
            for deal in page:
                deal_ts = record_cursor(deal) or deal_since
                # Only stop between deals with different update_time values,
                # so resuming from ``deal_since=deal_ts`` (inclusive) neither
                # skips nor re-reads a deal.
                if len(records) >= max_records and last_deal_ts and deal_ts > last_deal_ts:
                    stopped_at = deal_ts
                    break
                records.extend(self._read_one_deal_flow(deal.get("id"), lower, until))
                last_deal_ts = deal_ts
            if stopped_at:
                break

        if stopped_at:
            end_offset = {"cursor": lower, "until": until, "phase": phase, "deal_since": stopped_at}
        elif phase + 1 < len(DEAL_PARENT_PATHS):
            restart = shift_ts(lower, -lookback) if lookback > 0 else lower
            end_offset = {
                "cursor": lower,
                "until": until,
                "phase": phase + 1,
                "deal_since": restart,
            }
        else:
            end_offset = {"cursor": until}

        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _read_one_deal_flow(self, deal_id, lower: str, until: str) -> list[dict]:
        if deal_id is None:
            return []
        path = TABLE_CONFIG["deal_flow"]["path"].replace("{id}", str(deal_id))
        params = {"items": "dealChange", "all_changes": 1, "start": 0, "limit": 500}
        out: list[dict] = []
        try:
            for page in self._client.iter_v1_pages(path, params):
                for item in page:
                    if not isinstance(item, dict):
                        continue
                    if item.get("object", "dealChange") != "dealChange":
                        continue
                    entry = item.get("data")
                    if not isinstance(entry, dict):
                        continue
                    log_ts = normalize_ts(entry.get("log_time")) or normalize_ts(
                        item.get("timestamp")
                    )
                    if log_ts is None or not lower < log_ts <= until:
                        continue
                    out.append(self._normalize("deal_flow", entry))
        except PipedriveAPIError as exc:
            # Deal deleted / hidden between the parent read and this call.
            if exc.status_code in CHILD_SKIP_CODES and exc.is_json:
                return out
            raise
        return out
