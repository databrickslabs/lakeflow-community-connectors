"""Tabs Platform billing API connector for Lakeflow Community Connectors.

Implements ``LakeflowConnect`` + ``SupportsPartitionedStream`` against the
Tabs Platform API v3 (https://integrators.prod.api.tabsplatform.com).

Eight tables are exposed:

    Incremental (cdc):            invoices, payments, customers
    Incremental (cdc_w_deletes):  contracts (soft-delete via ``deletedAt``)
    Incremental child (cdc):      invoice_line_items (explode of invoices.lineItems)
    Snapshot:                     obligations, items, categories

Why partitioned-stream?
-----------------------
The Tabs ``filter`` query parameter supports **range queries** on the cursor
field (``lastUpdatedAt:gte:"..."`` combined with ``...:lte:"..."``). That is
the key signal for the partitioned-stream pattern: the date range between the
last committed cursor and "now" is split into independent windows that Spark
executors query in parallel via :meth:`read_partition`. ``latest_offset`` does
the lightweight high-water-mark discovery (capped at init time), and
``get_partitions`` slices the ``(start, end]`` range into ``window_days``-sized
windows.

Cursor semantics
----------------
Tabs cursors are **date-only** strings (YYYY-MM-DD). The API's ``filter``
parameter only accepts the date part even though the underlying field
(``lastUpdatedAt`` / ``receivedAt``) is a full ISO timestamp. The connector
therefore tracks offsets as YYYY-MM-DD strings and applies a configurable
``lookback_days`` (default 2) at read time to catch records edited later on a
day already partly synced. Downstream upsert on the primary key dedups any
re-read records.

Termination (Trigger AvailableNow)
----------------------------------
``latest_offset`` returns ``min(today, init_date)`` where ``init_date`` is
recorded once in ``__init__``. Because the value is capped at init time it
stabilises after the stream catches up, so successive ``latest_offset`` calls
return the same value and the trigger terminates. Data arriving after init is
picked up by the next trigger (a fresh connector instance with a newer
``init_date``).

Authentication
--------------
Tabs uses a **raw** API key in the ``Authorization`` header — NOT the
``Bearer <token>`` scheme. The connector passes the key verbatim.

Pagination
----------
Offset-based: ``page`` (1-indexed) + ``limit`` (default 50, max 100). A page
is drained by incrementing ``page`` until it returns fewer than ``limit``
records (or an empty array).
"""

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.tabs.tabs_schemas import (
    INCREMENTAL_SOURCE,
    INCREMENTAL_TABLES,
    SNAPSHOT_SOURCE,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

logger = logging.getLogger(__name__)


DEFAULT_BASE_URL = "https://integrators.prod.api.tabsplatform.com"

# Pagination defaults. The API default page size is 50; the documented safe
# max is 100.
DEFAULT_LIMIT = 100
MAX_LIMIT = 100

# Incremental read defaults.
DEFAULT_WINDOW_DAYS = 7
DEFAULT_LOOKBACK_DAYS = 2
# How far back the very first run reaches when no offset / start_date is
# supplied. Keeps the initial query bounded on large accounts.
DEFAULT_INITIAL_HISTORY_DAYS = 365

# Retry knobs for the exponential-backoff helper.
INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_TIMEOUT = 30


class TabsLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for the Tabs Platform API v3."""

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        api_key = options.get("api_key") or options.get("access_token")
        if not api_key:
            raise ValueError(
                "Tabs connector requires connection option 'api_key' "
                "(the raw API key created in the Tabs Developers section)."
            )

        self._api_key = api_key
        self._base_url = (options.get("base_url") or DEFAULT_BASE_URL).rstrip("/")

        # Cap the incremental cursor at init time so Trigger.AvailableNow
        # terminates even when the source is being written continuously.
        # Stored as a YYYY-MM-DD string because Tabs cursors are date-only.
        self._init_date = datetime.now(timezone.utc).date()
        self._init_date_iso = self._init_date.isoformat()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @property
    def _headers(self) -> dict[str, str]:
        # Raw key in Authorization — no "Bearer " prefix (see module docstring).
        return {
            "Authorization": self._api_key,
            "Accept": "application/json",
        }

    def _request(self, path: str, params: dict | None = None) -> requests.Response:
        """GET *path* with exponential backoff on 429/5xx.

        A fresh ``requests`` call is used per request (rather than a cached
        ``Session``) so the connector instance carries no non-picklable state
        when Spark ships it to executors for partitioned reads.
        """
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            resp = requests.get(
                url,
                headers=self._headers,
                params=params or {},
                timeout=DEFAULT_TIMEOUT,
            )
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            # Honour Retry-After when present; fall back to exponential backoff.
            retry_after = resp.headers.get("Retry-After")
            sleep_s = backoff
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except ValueError:
                    pass
            logger.warning(
                "Tabs GET %s returned %s; retrying in %.1fs (attempt %d/%d)",
                path,
                resp.status_code,
                sleep_s,
                attempt + 1,
                MAX_RETRIES,
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(sleep_s)
                backoff *= 2

        assert resp is not None  # always set inside the loop
        return resp

    def _get_json(self, path: str, params: dict | None = None) -> Any:
        resp = self._request(path, params=params)
        if resp.status_code // 100 != 2:
            raise RuntimeError(
                f"Tabs API GET {path} failed with HTTP {resp.status_code}: "
                f"{resp.text[:500]}"
            )
        return resp.json()

    @staticmethod
    def _unwrap_records(body: Any) -> list:
        """Return the records list from a Tabs response body.

        Tabs list endpoints return a bare JSON array of records. Some
        deployments wrap the array in ``{"data": [...]}`` / ``{"items":
        [...]}`` / ``{"results": [...]}``; accept all of those so the
        connector tolerates either shape. Anything unrecognised yields ``[]``.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("data", "items", "results", "records"):
                inner = body.get(key)
                if isinstance(inner, list):
                    return inner
        return []

    def _paginate(self, path: str, params: dict, limit: int) -> Iterator[dict]:
        """Yield every record across all pages of an offset-paginated endpoint.

        Increments ``page`` (1-indexed) until a page returns fewer than
        ``limit`` records or an empty array. Each call carries the caller's
        base ``params`` (e.g. the date-range ``filter``).
        """
        page = 1
        while True:
            page_params = dict(params)
            page_params["page"] = str(page)
            page_params["limit"] = str(limit)
            body = self._get_json(path, params=page_params)
            records = self._unwrap_records(body)
            if not records:
                break
            yield from records
            if len(records) < limit:
                break
            page += 1

    # ------------------------------------------------------------------
    # LakeflowConnect — schema / metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        # Tabs has no discovery endpoint; the object list is static.
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        # Only the date-range incremental tables partition by time window.
        # Snapshot tables fall back to the single-driver read_table path.
        return table_name in INCREMENTAL_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the high-water-mark offset, capped at init time.

        Lightweight, metadata-only: returns ``min(today, init_date)`` as a
        YYYY-MM-DD string. Capping at ``init_date`` guarantees the value
        stabilises so Trigger.AvailableNow terminates.
        """
        self._validate_table(table_name)
        return {"cursor": self._init_date_iso}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split the ``(start, end]`` date range into windowed partitions.

        Each partition descriptor is ``{"since": "YYYY-MM-DD", "until":
        "YYYY-MM-DD"}`` (both inclusive on the Tabs filter). Snapshot tables
        return a single all-encompassing partition.
        """
        self._validate_table(table_name)

        # Snapshot tables: one partition covering everything.
        if table_name not in INCREMENTAL_TABLES:
            return [{"snapshot": True}]

        window_days = self._parse_int(
            table_options.get("window_days"), DEFAULT_WINDOW_DAYS, minimum=1
        )
        lookback_days = self._parse_int(
            table_options.get("lookback_days"), DEFAULT_LOOKBACK_DAYS, minimum=0
        )

        end_iso = (end_offset or {}).get("cursor") or self._init_date_iso
        end_dt = self._parse_date(end_iso)
        # Never partition past init time.
        if end_dt > self._init_date:
            end_dt = self._init_date

        start_iso = (start_offset or {}).get("cursor")
        if not start_iso:
            # First run: bound the initial history. Prefer a user-supplied
            # start_date, else reach back DEFAULT_INITIAL_HISTORY_DAYS.
            start_iso = table_options.get("start_date")
        if start_iso:
            start_dt = self._parse_date(start_iso)
        else:
            start_dt = end_dt - timedelta(days=DEFAULT_INITIAL_HISTORY_DAYS)

        if start_dt >= end_dt:
            return []

        # Apply lookback at read time only — partition boundaries shift back
        # but the committed offset (end cursor) is unaffected.
        read_from = start_dt - timedelta(days=lookback_days)

        partitions: list[dict] = []
        cursor = read_from
        while cursor < end_dt:
            window_end = min(cursor + timedelta(days=window_days), end_dt)
            # Tabs filter range is inclusive on both ends. Request
            # [cursor, window_end] and let downstream upsert dedup the
            # one-day overlap between adjacent windows.
            partitions.append(
                {"since": cursor.isoformat(), "until": window_end.isoformat()}
            )
            cursor = window_end

        return partitions

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one partition on an executor. Self-contained — no driver state."""
        self._validate_table(table_name)

        # Snapshot partition.
        if partition.get("snapshot"):
            yield from self._read_snapshot_records(table_name, table_options)
            return

        since = partition["since"]
        until = partition["until"]
        limit = self._resolve_limit(table_options)
        source = INCREMENTAL_SOURCE[table_name]
        params = {
            "filter": self._date_range_filter(source["filter_field"], since, until)
        }

        if table_name == "invoice_line_items":
            yield from self._explode_line_items(
                self._paginate(source["path"], params, limit)
            )
        else:
            yield from self._paginate(source["path"], params, limit)

    # ------------------------------------------------------------------
    # LakeflowConnect.read_table — single-driver fallback
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver read.

        Used by ``simpleStreamReader`` for snapshot tables (where
        :meth:`is_partitioned` is False) and as the fallback path for the
        incremental tables when partitioning is unavailable.
        """
        self._validate_table(table_name)

        if table_name not in INCREMENTAL_TABLES:
            # Snapshot: read everything in one batch, no checkpointable offset.
            records = list(self._read_snapshot_records(table_name, table_options))
            return iter(records), {}

        return self._read_incremental(table_name, start_offset, table_options)

    def read_table_deletes(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Fetch soft-deleted contracts (``deletedAt`` is non-null).

        Only ``contracts`` carries a soft-delete field. The Tabs filter
        ``deletedAt:isnotnull`` selects deleted records; we additionally scope
        by ``lastUpdatedAt`` date range so the scan stays bounded and the
        cursor advances. Returned rows carry at least the primary key and
        cursor field.
        """
        self._validate_table(table_name)
        if table_name != "contracts":
            raise ValueError(
                f"Table '{table_name}' does not support deleted records"
            )

        start_offset = start_offset or {}
        since_iso = start_offset.get("cursor")
        if not since_iso:
            since_iso = table_options.get("start_date")
        if since_iso:
            since_dt = self._parse_date(since_iso)
        else:
            since_dt = self._init_date - timedelta(days=DEFAULT_INITIAL_HISTORY_DAYS)

        # Already caught up to init time — terminate.
        if since_dt >= self._init_date:
            return iter([]), start_offset or {"cursor": self._init_date_iso}

        lookback_days = self._parse_int(
            table_options.get("lookback_days"), DEFAULT_LOOKBACK_DAYS, minimum=0
        )
        read_from = (since_dt - timedelta(days=lookback_days)).isoformat()
        limit = self._resolve_limit(table_options)

        filter_clause = (
            f"deletedAt:isnotnull,"
            f"lastUpdatedAt:gte:\"{read_from}\","
            f"lastUpdatedAt:lte:\"{self._init_date_iso}\""
        )
        records = list(
            self._paginate("/v3/contracts", {"filter": filter_clause}, limit)
        )
        return iter(records), {"cursor": self._init_date_iso}

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver incremental read over one date window per call.

        Mirrors the partitioned path but drains the whole ``(since, init]``
        range in one batch. The offset is capped at ``init_date`` so a
        follow-up call with that offset short-circuits and the trigger ends.
        """
        start_offset = start_offset or {}
        since_iso = start_offset.get("cursor")
        if not since_iso:
            since_iso = table_options.get("start_date")
        if since_iso:
            since_dt = self._parse_date(since_iso)
        else:
            since_dt = self._init_date - timedelta(days=DEFAULT_INITIAL_HISTORY_DAYS)

        if since_dt >= self._init_date:
            return iter([]), start_offset or {"cursor": self._init_date_iso}

        lookback_days = self._parse_int(
            table_options.get("lookback_days"), DEFAULT_LOOKBACK_DAYS, minimum=0
        )
        read_from = (since_dt - timedelta(days=lookback_days)).isoformat()
        limit = self._resolve_limit(table_options)
        source = INCREMENTAL_SOURCE[table_name]
        params = {
            "filter": self._date_range_filter(
                source["filter_field"], read_from, self._init_date_iso
            )
        }

        raw = self._paginate(source["path"], params, limit)
        if table_name == "invoice_line_items":
            records = list(self._explode_line_items(raw))
        else:
            records = list(raw)

        return iter(records), {"cursor": self._init_date_iso}

    def _read_snapshot_records(
        self, table_name: str, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Drain a snapshot endpoint fully (all pages)."""
        path = SNAPSHOT_SOURCE[table_name]
        limit = self._resolve_limit(table_options)
        yield from self._paginate(path, {}, limit)

    @staticmethod
    def _explode_line_items(invoices: Iterator[dict]) -> Iterator[dict]:
        """Explode invoices.lineItems into one child row per line item.

        Each row carries ``invoiceId`` (FK) and ``invoiceLastUpdatedAt``
        (the parent cursor) so the child table can merge incrementally in
        lockstep with the parent invoices table.
        """
        for inv in invoices:
            invoice_id = inv.get("id")
            invoice_updated = inv.get("lastUpdatedAt")
            for li in inv.get("lineItems") or []:
                row = dict(li)
                row["invoiceId"] = invoice_id
                row["invoiceLastUpdatedAt"] = invoice_updated
                yield row

    @staticmethod
    def _date_range_filter(field: str, since: str, until: str) -> str:
        """Build a Tabs ``filter`` clause for an inclusive date range.

        Produces ``<field>:gte:"<since>",<field>:lte:"<until>"`` — comma is
        AND. Date values are YYYY-MM-DD (Tabs accepts only the date part).
        """
        return f'{field}:gte:"{since}",{field}:lte:"{until}"'

    def _resolve_limit(self, table_options: dict[str, str]) -> int:
        """Resolve the per-page ``limit`` honouring the API max of 100."""
        limit = self._parse_int(
            table_options.get("limit"), DEFAULT_LIMIT, minimum=1
        )
        return min(limit, MAX_LIMIT)

    # ------------------------------------------------------------------
    # Misc helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )

    @staticmethod
    def _parse_int(value: Any, default: int, *, minimum: int = 0) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return max(parsed, minimum)

    @staticmethod
    def _parse_date(value: str) -> date:
        """Parse a YYYY-MM-DD (or ISO timestamp) string into a ``date``."""
        if not value:
            raise ValueError("empty date string")
        if "T" in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        return date.fromisoformat(value)
