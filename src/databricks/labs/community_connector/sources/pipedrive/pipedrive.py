"""PipedriveLakeflowConnect — Lakeflow Community Connector for the Pipedrive CRM API.

Authentication: personal API token sent in the ``x-api-token`` header against
``https://api.pipedrive.com`` (or ``https://{company_domain}.pipedrive.com``).

Tables and read strategies
--------------------------
    deals                          cdc_with_deletes  v2 live deals; tombstones from
                                                     status=deleted and archived moves
    deals_archived                 cdc_with_deletes  v2 archived deals; tombstones from
                                                     status=deleted and unarchived moves
    persons, organizations,
    activities, products           cdc               v2, updated_since, sort update_time asc
    notes                          cdc               v1, updated_since, sort update_time asc
    leads                          cdc_with_deletes  v1 live leads; tombstones from
                                                     /v1/leads/archived
    deal_flow                      cdc (on id)       v1 per-deal change log, parent-incremental
    everything else                snapshot          full read

Why not partitioned: Pipedrive enforces a strict per-token burst limit
(20 requests / 2 s on the lowest plan) and a daily token budget, so parallel
executor reads would only trade throughput for 429s / Cloudflare blocks.  All
reads run sequentially through one throttled client.

Incremental contract
--------------------
* A time-cursor stream keeps the offset ``{cursor, ids, start}``: ``cursor`` is
  the newest ``update_time`` consumed, ``ids`` are the ids consumed with
  ``update_time == cursor``, and ``start`` is kept for the offset shape (v1
  always resumes at the start of the tie group and relies on ``ids``).  A row
  is consumed iff ``update_time > cursor`` or it is a tie whose id is not in
  ``ids``.  The offset therefore changes whenever a row is consumed,
  so a row the framework would otherwise drop (records returned with an unchanged
  offset) cannot occur.
* Every incremental read is bounded above by ``until = init_time - settle``,
  where ``lookback_seconds`` is the settle margin: a change is read once it is at
  least that old.  It covers writes committed up to the margin after their
  ``update_time``; a later commit below the cursor is only caught by a full refresh.
* A batch holds at most ``max_records_per_batch`` emitted rows and stops right
  after the row that reaches the cap.  Rows consumed but filtered out (deleted
  rows on a live list, rows of the other archive state) still move the offset.
* v1 lists (notes, leads, /v1/leads/archived) are walked by keyset: each page
  request is ``updated_since=cursor`` with ``sort=update_time ASC, id ASC``, so a
  row removed mid-walk shifts nothing after the cursor.  Only a tie group larger
  than a page relies on ``start``.  If the server ignores ``updated_since`` the
  call falls back to a full ordered scan with client-side filtering.
* Delete streams have their own offsets.  Archive and unarchive moves are
  reported as id-only tombstones, and ``status=deleted`` rows are re-checked
  client-side because the filter is server-side only.
* deal_flow keeps its own offsets (a phase walk over parent deals) and uses
  ``lookback_seconds`` to widen its log-time window backwards.
"""

import heapq
import logging
import re
from typing import Callable, Iterable, Iterator, Optional
from urllib.parse import urlsplit

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.pipedrive.pipedrive_schemas import (
    DEAL_FLOW_PHASE_PATHS,
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

_LOG = logging.getLogger(__name__)

V2_PAGE_MAX = 500
DEFAULT_MAX_RECORDS_PER_BATCH = 5000
# Settle margin (``lookback_seconds``): a change is read once it is at least this
# old, so writes that commit late with an earlier update_time are not missed.
# v1 endpoints reflect writes more slowly, hence the larger default.
DEFAULT_V2_SETTLE_SECONDS = 60
DEFAULT_V1_SETTLE_SECONDS = 300
# v1 incremental lists: mutable update_time as the cursor, immutable id as tiebreak.
V1_INCREMENTAL_SORT = "update_time ASC, id ASC"
# deal_flow widens its log-time window backwards by this much (see _read_deal_flow).
DEFAULT_DEAL_FLOW_LOOKBACK_SECONDS = 3600
# Clock-skew margin for the final deal_flow /deals re-scan, which starts from
# the moment the /deals/archived phase began.
DEAL_FLOW_RESCAN_SKEW_SECONDS = 300
DEAL_PRODUCTS_BATCH = 100  # /api/v2/deals/products accepts at most 100 deal_ids

DEALS_LIVE_STATUSES = "open,won,lost"
DEALS_DELETED_STATUS = "deleted"
DEALS_ALL_STATUSES = "open,won,lost,deleted"
PROJECT_ALL_STATUSES = "open,completed,canceled,deleted"

# Delete sources for the deal tables, merged in update_time order.  Each entry is
# (source table, server status filter, kind):
#   DELETED  rows the source reports as deleted (soft delete, kept 30 days).
#            The status filter is only a request hint, so rows are re-checked.
#   MOVED    rows that left this table for its sibling (archived or unarchived).
#            Every such row is a tombstone; the sibling holds the live copy.
# The two sources never hold the same row at the same update_time, because a
# deal is either archived or not.  This is why one shared id set per cursor works.
_DELETED = "deleted"
_MOVED = "moved"
DEAL_DELETE_SOURCES: dict[str, tuple[tuple[str, str, str], ...]] = {
    "deals": (
        ("deals", DEALS_DELETED_STATUS, _DELETED),
        ("deals_archived", DEALS_ALL_STATUSES, _MOVED),
    ),
    "deals_archived": (
        ("deals_archived", DEALS_DELETED_STATUS, _DELETED),
        ("deals", DEALS_ALL_STATUSES, _MOVED),
    ),
}


def _is_deleted_row(raw: dict) -> bool:
    """Check the row itself, not the request: a server that ignores ``status=deleted``
    must not turn live deals into deletes."""
    return raw.get("is_deleted") is True or raw.get("status") == DEALS_DELETED_STATUS


def _tombstone(raw: dict) -> Optional[dict]:
    """Id-only delete record: the primary key plus the cursor field."""
    if raw.get("id") is None:
        return None
    return {"id": raw["id"], "update_time": record_cursor(raw)}


def _offset(cursor: str, ids: list, start: int = 0) -> dict:
    """Canonical time-cursor offset: empty ids and a zero start are omitted."""
    offset: dict = {"cursor": cursor}
    if ids:
        offset["ids"] = ids
    if start:
        offset["start"] = start
    return offset


def _page_data(body: dict) -> list:
    data = body.get("data") or []
    return data if isinstance(data, list) else [data]


def _older_than(raw: dict, since: str) -> bool:
    rc = record_cursor(raw)
    return rc is not None and rc < since


_PIPEDRIVE_HOST_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.pipedrive\.com")


def _base_url_for_domain(company_domain: str) -> str:
    """Accept ``mycompany``, ``mycompany.pipedrive.com`` or a full URL (any path)."""
    text = company_domain if "://" in company_domain else f"https://{company_domain}"
    host = (urlsplit(text).hostname or "").strip(".")
    if not host:
        raise ValueError(f"Invalid 'company_domain' {company_domain!r}")
    if "." not in host:
        host = f"{host}.pipedrive.com"
    # Only a company subdomain of pipedrive.com: the API token is sent to this host.
    if not _PIPEDRIVE_HOST_RE.fullmatch(host.lower()):
        raise ValueError(
            f"Invalid 'company_domain' {company_domain!r}: expected 'mycompany' or "
            "'mycompany.pipedrive.com'"
        )
    return f"https://{host.lower()}"


def _log_optional_skip(table_name: str, exc: PipedriveAPIError) -> None:
    # Status code only: the error text can echo response bodies.
    _LOG.warning(
        "Skipping optional Pipedrive table '%s': HTTP %s, so the feature is probably "
        "not enabled for this account.",
        table_name,
        exc.status_code,
    )


class PipedriveLakeflowConnect(LakeflowConnect):
    """Lakeflow connector for Pipedrive (API v1 + v2)."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("Pipedrive connector requires 'api_token' in options")

        company_domain = (options.get("company_domain") or "").strip()
        base_url = _base_url_for_domain(company_domain) if company_domain else DEFAULT_BASE_URL

        start_date = options.get("start_date") or DEFAULT_START_DATE
        normalized_start = normalize_ts(start_date)
        if normalized_start is None:
            raise ValueError(f"Invalid 'start_date' {start_date!r}; expected RFC 3339")
        self._start_date = normalized_start

        burst_limit = int(options.get("burst_limit") or DEFAULT_BURST_LIMIT)
        self._client = PipedriveClient(api_token, base_url, burst_limit=burst_limit)

        # Changes after this instant (less the settle margin) are read by the next
        # trigger, which constructs a fresh instance.
        self._init_time = now_utc()

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
            return self._read_v2_incremental(table_name, start_offset, table_options, status=status)
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
        """Delete tombstones for ``cdc_with_deletes`` tables, on their own offset.

        Deals: ``status=deleted`` rows, plus rows that moved to the sibling table
        (archived for ``deals``, unarchived for ``deals_archived``).  Leads: rows
        that were archived (``/v1/leads/archived``).  Tombstones carry the primary
        key and ``update_time`` only.
        """
        self._validate_table(table_name)
        if TABLE_CONFIG[table_name].get("ingestion_type") != "cdc_with_deletes":
            raise ValueError(f"Table '{table_name}' does not support deleted records")
        table_options = table_options or {}
        if table_name == "leads":
            archived = TABLE_CONFIG["leads_archived"]
            return self._read_v1_keyset(
                archived["path"],
                start_offset,
                table_options,
                page_max=archived.get("page_max", 500),
                emit=_tombstone,
            )
        return self._read_v2_deletes(table_name, start_offset, table_options)

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

    def _resolve_since(self, start_offset: Optional[dict]) -> Optional[str]:
        """deal_flow's starting cursor, or ``None`` once it caught up to init time."""
        cursor = (start_offset or {}).get("cursor")
        if cursor and cursor >= self._init_time:
            return None
        return cursor or self._start_date

    def _upper_bound(self, settle_seconds: int) -> str:
        """Newest update_time a read may consume: init time less the settle margin."""
        return shift_ts(self._init_time, -settle_seconds) if settle_seconds > 0 else self._init_time

    def _time_state(self, start_offset: Optional[dict]) -> tuple[str, list, int]:
        offset = start_offset or {}
        cursor = offset.get("cursor") or self._start_date
        return cursor, list(offset.get("ids") or []), int(offset.get("start") or 0)

    @staticmethod
    def _tagged(
        raw_rows: Iterable[dict], cursor: str, emit: Callable[[dict], Optional[dict]]
    ) -> Iterator[tuple]:
        """Yield ``(update_time, id, raw_id, out)`` for one sorted stream.

        Rows without a cursor or id, and rows below the committed cursor (a server
        that ignored ``updated_since``), are dropped here.  ``out`` is the emitted
        row, or None for a row that is consumed but filtered out.
        """
        for raw in raw_rows:
            rc = record_cursor(raw)
            if rc is None or raw.get("id") is None or rc < cursor:
                continue
            yield rc, str(raw["id"]), raw["id"], emit(raw)

    def _merge_scan(
        self,
        streams: list[Iterator[tuple]],
        cursor: str,
        ids: list,
        until: str,
        max_records: int,
    ) -> tuple[list[dict], str, set, int, bool]:
        """Consume the merged streams in ``(update_time, id)`` order.

        A row is consumed when its update_time is past ``cursor``, or equal to it
        with an id not yet recorded at the cursor.  Consumption stops at the first
        row past ``until`` or right after the ``max_records``-th emitted row.
        Returns ``(rows, cursor, ids_at_cursor, visits, capped)``.  ``visits``
        counts the consumed rows at the final cursor, and ``capped`` tells whether
        the batch was cut short by the cap rather than by reaching ``until``.
        """
        merged = heapq.merge(*streams, key=lambda item: (item[0], item[1]))
        cur, at_cur, visits = cursor, set(ids), 0
        rows: list[dict] = []
        for rc, _, raw_id, out in merged:
            if rc > until:
                return rows, cur, at_cur, visits, False
            if rc > cur:
                cur, at_cur, visits = rc, set(), 0
            visits += 1
            if raw_id in at_cur:
                continue
            at_cur.add(raw_id)
            if out is not None:
                rows.append(out)
                if len(rows) >= max_records:
                    return rows, cur, at_cur, visits, True
        return rows, cur, at_cur, visits, False

    @staticmethod
    def _sorted_ids(ids: set) -> list:
        return sorted(ids, key=str)

    def _deleted_emitter(self, table_name: str) -> Callable[[dict], Optional[dict]]:
        """Emit full rows from a ``status=deleted`` source, dropping any live row."""

        def emit(raw: dict) -> Optional[dict]:
            return self._normalize(table_name, raw) if _is_deleted_row(raw) else None

        return emit

    def _live_emitter(self, table_name: str, guard: bool) -> Callable[[dict], Optional[dict]]:
        """Emit normalised rows; on a status-filtered (deals) list, drop deleted rows."""

        def emit(raw: dict) -> Optional[dict]:
            if guard and _is_deleted_row(raw):
                return None
            return self._normalize(table_name, raw)

        return emit

    # ------------------------------------------------------------------ #
    # v2 incremental (deals, deals_archived, persons, organizations,
    # activities, products)
    # ------------------------------------------------------------------ #

    def _v2_batch_sizes(self, table_options: dict) -> tuple[int, int]:
        """``(max_records_per_batch, page_size)``; a page never exceeds the batch cap."""
        max_records = self._int_option(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        page_size = min(
            V2_PAGE_MAX,
            self._int_option(table_options, "page_size", V2_PAGE_MAX, minimum=1),
            max_records,
        )
        return max_records, page_size

    @staticmethod
    def _v2_params(
        cfg: dict, table_options: dict, *, status: Optional[str], page_size: int, until: str
    ) -> dict:
        params: dict = {
            "limit": page_size,
            "sort_by": "update_time",
            "sort_direction": "asc",
        }
        if cfg.get("supports_until"):
            params["updated_until"] = until
        if status:
            params["status"] = status
        include_fields = table_options.get("include_fields", cfg.get("default_include_fields"))
        if include_fields:
            params["include_fields"] = include_fields
        return params

    def _v2_rows(self, path: str, params: dict, since: str) -> Iterator[dict]:
        """Every row of a v2 list from ``since``, page by page (lazy)."""
        for page in self._client.iter_v2_pages(path, {**params, "updated_since": since}):
            yield from page

    def _read_v2_incremental(
        self, table_name: str, start_offset: Optional[dict], table_options: dict, *, status
    ) -> tuple[Iterator[dict], dict]:
        cfg = TABLE_CONFIG[table_name]
        max_records, page_size = self._v2_batch_sizes(table_options)
        until = self._upper_bound(
            self._int_option(table_options, "lookback_seconds", DEFAULT_V2_SETTLE_SECONDS)
        )
        cursor, ids, _ = self._time_state(start_offset)
        if cursor >= until:
            return iter([]), start_offset or _offset(cursor, ids)

        params = self._v2_params(
            cfg, table_options, status=status, page_size=page_size, until=until
        )
        emit = self._live_emitter(table_name, guard=status is not None)
        stream = self._tagged(self._v2_rows(cfg["path"], params, cursor), cursor, emit)
        rows, cur, at_cur, _, _ = self._merge_scan([stream], cursor, ids, until, max_records)
        return iter(rows), _offset(cur, self._sorted_ids(at_cur))

    def _read_v2_deletes(
        self, table_name: str, start_offset: Optional[dict], table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        """Tombstones for a deal table: its delete sources merged by update_time."""
        max_records, page_size = self._v2_batch_sizes(table_options)
        until = self._upper_bound(
            self._int_option(table_options, "lookback_seconds", DEFAULT_V2_SETTLE_SECONDS)
        )
        cursor, ids, _ = self._time_state(start_offset)
        if cursor >= until:
            return iter([]), start_offset or _offset(cursor, ids)

        streams = []
        for source_table, status, kind in DEAL_DELETE_SOURCES[table_name]:
            src_cfg = TABLE_CONFIG[source_table]
            params = self._v2_params(
                src_cfg, table_options, status=status, page_size=page_size, until=until
            )
            emit = _tombstone if kind == _MOVED else self._deleted_emitter(table_name)
            streams.append(
                self._tagged(self._v2_rows(src_cfg["path"], params, cursor), cursor, emit)
            )
        rows, cur, at_cur, _, _ = self._merge_scan(streams, cursor, ids, until, max_records)
        return iter(rows), _offset(cur, self._sorted_ids(at_cur))

    # ------------------------------------------------------------------ #
    # v1 incremental (notes, leads, leads_archived for deletes)
    # ------------------------------------------------------------------ #

    def _read_v1_incremental(
        self, table_name: str, start_offset: Optional[dict], table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        cfg = TABLE_CONFIG[table_name]
        return self._read_v1_keyset(
            cfg["path"],
            start_offset,
            table_options,
            page_max=cfg.get("page_max", 500),
            emit=lambda raw: self._normalize(table_name, raw),
        )

    def _read_v1_keyset(
        self,
        path: str,
        start_offset: Optional[dict],
        table_options: dict,
        *,
        page_max: int,
        emit: Callable[[dict], Optional[dict]],
    ) -> tuple[Iterator[dict], dict]:
        """Keyset walk of a v1 list by ``update_time``, resumable at any row.

        A batch starts at ``updated_since=cursor`` (inclusive), so only the
        cursor's tie group is re-read, and the ``ids`` already emitted there are
        skipped.  Pages inside the batch are re-anchored on the last update_time
        read (see ``_v1_rows``).
        """
        max_records = self._int_option(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        page_size = min(
            page_max,
            self._int_option(table_options, "page_size", page_max, minimum=1),
            max_records,
        )
        until = self._upper_bound(
            self._int_option(table_options, "lookback_seconds", DEFAULT_V1_SETTLE_SECONDS)
        )
        cursor, ids, start = self._time_state(start_offset)
        if cursor >= until:
            return iter([]), start_offset or _offset(cursor, ids, start)

        flags = {"fallback": False}
        # Always resume at the start of the cursor's tie group and skip the ids
        # already read there: a stored row count would skip a row if a read row
        # in that group was edited or removed between batches.
        stream = self._tagged(self._v1_rows(path, cursor, 0, page_size, flags), cursor, emit)
        rows, cur, at_cur, visits, capped = self._merge_scan(
            [stream], cursor, ids, until, max_records
        )
        del visits, capped  # position within a tie group comes from ``ids`` alone
        return iter(rows), _offset(cur, self._sorted_ids(at_cur), 0)

    def _v1_rows(
        self, path: str, since: str, start: int, page_size: int, flags: dict
    ) -> Iterator[dict]:
        """Rows of a v1 list sorted by ``update_time, id`` from ``since`` (lazy).

        ``flags["fallback"]`` is set when the server returns rows older than
        ``since``, which means it ignored ``updated_since`` (seen on
        ``/v1/leads/archived``).  The walk then restarts from the first row, and the
        caller filters by cursor.  Detection happens before any row is yielded.
        """

        def request(anchor: str, at: int) -> dict:
            return self._client.get_json(
                path,
                {
                    "limit": page_size,
                    "start": at,
                    "sort": V1_INCREMENTAL_SORT,
                    "updated_since": anchor,
                },
            )

        body = request(since, start)
        if any(_older_than(raw, since) for raw in _page_data(body)):
            flags["fallback"] = True
            yield from self._v1_offset_scan(body, start, lambda at: request(since, at))
            return
        # Every page is re-anchored on the last update_time read, with ``start``
        # counting only the rows already read at that instant.  A row that leaves
        # the list (archived, deleted, edited past the cap) can then shift rows
        # only inside one same-second tie group, never across the whole walk.
        anchor, at = since, start
        while True:
            data = _page_data(body)
            yield from data
            pagination = (body.get("additional_data") or {}).get("pagination") or {}
            if not pagination.get("more_items_in_collection") or not data:
                return
            last = record_cursor(data[-1])
            if last is None or last == anchor:
                at += len(data)
            else:
                anchor = last
                at = sum(1 for raw in data if record_cursor(raw) == last)
            body = request(anchor, at)

    @staticmethod
    def _v1_offset_scan(body: dict, start: int, request: Callable[[int], dict]) -> Iterator[dict]:
        """Plain offset paging, for a server that ignores ``updated_since``."""
        offset = start
        if start:
            offset = 0
            body = request(0)
        while True:
            data = _page_data(body)
            yield from data
            pagination = (body.get("additional_data") or {}).get("pagination") or {}
            if not pagination.get("more_items_in_collection") or not data:
                return
            next_start = pagination.get("next_start")
            if next_start is None:
                next_start = offset + len(data)
            if int(next_start) <= offset:
                return  # defensive: never loop on a non-advancing offset
            offset = int(next_start)
            body = request(offset)

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
                _log_optional_skip(table_name, exc)
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
                _log_optional_skip("projects", exc)
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
        """Deal change log, read via parent-incremental deals.

        Each trigger covers the log-time window ``(cursor - lookback, until]``
        where ``until`` is the init time.  Parent deals updated since
        ``cursor - lookback`` are walked in ascending ``update_time`` order in
        three phases: ``/deals``, ``/deals/archived``, then ``/deals`` again
        from the moment the archived phase began.  For each deal the flow
        entries inside the window are emitted.

        The parent query is deliberately *not* capped with ``updated_until``:
        the walk spans many micro-batches (one 40-token call per deal), and a
        deal edited or archived before the walk reaches it gets a fresh
        ``update_time`` past ``until``.  Uncapped, such a deal just moves
        further ahead in the sort (or into the not-yet-walked archived list)
        and is still visited; capped, its window entries would be lost for
        good.  The final ``/deals`` re-scan covers deals unarchived while the
        archived list was being walked.  Entry rows are bounded client-side by
        ``cursor - lookback < log_time <= until``.

        A deal can therefore be visited twice in one window (edited after it
        was processed).  Rows are immutable and keyed on ``id`` (``cdc``), so
        a re-emitted row is an idempotent upsert; within one micro-batch,
        repeated deals and entries are skipped outright.

        The window is split across micro-batches by deal
        (``max_records_per_batch`` is best-effort: a deal's flow is never
        split).  In-progress offsets carry the phase, the ``update_time`` of
        the next unprocessed deal and the re-scan start.
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
            rescan_since = offset.get("rescan_since")
        else:
            lower = self._resolve_since(start_offset)
            if lower is None:
                return iter([]), start_offset
            until = self._init_time
            phase = 0
            deal_since = shift_ts(lower, -lookback) if lookback > 0 else lower
            rescan_since = None

        # Entries are filtered by log_time against the lookback-widened window
        # on every batch, so a batch boundary never changes which entries count.
        entry_lower = shift_ts(lower, -lookback) if lookback > 0 else lower
        max_records = self._int_option(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        records, stopped_at = self._walk_deal_flow_phase(
            DEAL_FLOW_PHASE_PATHS[phase], deal_since, (entry_lower, until), max_records
        )

        end_offset = {"cursor": lower, "until": until}
        if stopped_at:
            end_offset.update(phase=phase, deal_since=stopped_at)
            if rescan_since:
                end_offset["rescan_since"] = rescan_since
        elif phase == 0:
            # Deals unarchived from here on re-enter /deals with an
            # update_time at or after this instant; the final phase
            # re-scans /deals from it.
            end_offset.update(
                phase=1,
                deal_since=shift_ts(lower, -lookback) if lookback > 0 else lower,
                rescan_since=shift_ts(now_utc(), -DEAL_FLOW_RESCAN_SKEW_SECONDS),
            )
        elif phase + 1 < len(DEAL_FLOW_PHASE_PATHS):
            end_offset.update(phase=phase + 1, deal_since=rescan_since or until)
        else:
            end_offset = {"cursor": until}

        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _walk_deal_flow_phase(
        self, path: str, deal_since: str, window: tuple[str, str], max_records: int
    ) -> tuple[list[dict], Optional[str]]:
        """Walk one parent list (uncapped) and collect window entries.

        Returns the entries and, if the batch filled up, the ``update_time`` of
        the next unprocessed deal (``None`` once the list is exhausted).
        """
        params = {
            "limit": V2_PAGE_MAX,
            "sort_by": "update_time",
            "sort_direction": "asc",
            "updated_since": deal_since,
            "status": DEALS_ALL_STATUSES,
        }
        records: list[dict] = []
        seen_deals: set = set()
        seen_entries: set = set()
        last_deal_ts: Optional[str] = None
        calls = skipped = 0
        for page in self._client.iter_v2_pages(path, params):
            for deal in page:
                deal_ts = record_cursor(deal) or deal_since
                # Only stop between deals with different update_time values,
                # so resuming from ``deal_since=deal_ts`` (inclusive) never
                # skips a deal.
                if len(records) >= max_records and last_deal_ts and deal_ts > last_deal_ts:
                    self._note_skipped_children(calls, skipped)
                    return records, deal_ts
                last_deal_ts = deal_ts
                if deal.get("id") in seen_deals:
                    continue  # moved ahead in the sort within this batch
                seen_deals.add(deal.get("id"))
                if deal.get("id") is not None:
                    calls += 1
                entries, was_skipped = self._read_one_deal_flow(deal.get("id"), *window)
                skipped += was_skipped
                for entry in entries:
                    if entry.get("id") is None or entry["id"] not in seen_entries:
                        seen_entries.add(entry.get("id"))
                        records.append(entry)
        self._note_skipped_children(calls, skipped)
        return records, None

    @staticmethod
    def _note_skipped_children(calls: int, skipped: int) -> None:
        """A batch whose every per-deal flow call was skipped gets no rows; say so."""
        if calls and skipped == calls:
            _LOG.warning(
                "deal_flow: all %d per-deal flow calls in this batch were skipped "
                "(the deals were deleted or hidden after the parent read).",
                calls,
            )

    def _read_one_deal_flow(self, deal_id, lower: str, until: str) -> tuple[list[dict], int]:
        """Window entries of one deal, and whether the call was skipped (deal gone)."""
        if deal_id is None:
            return [], 0
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
                return out, 1
            raise
        return out, 0
