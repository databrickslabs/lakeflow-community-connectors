"""HubSpot property history tables (``{object}_property_history``).

One row per entry in HubSpot's ``propertiesWithHistory`` response, i.e.
one row per (object, property, change timestamp).

Read flow for one ``read_table`` call:

1. **Find changed parents** via ``POST /crm/v3/objects/{type}/search``
   filtered on the object's last-modified property, sorted ascending,
   bounded above by the connector's init time. Search does *not* accept
   ``propertiesWithHistory``, so it only returns IDs + ``updatedAt``.
2. **Fetch history** for those IDs via
   ``POST /crm/v3/objects/{type}/batch/read`` with ``propertiesWithHistory``
   in the JSON body (no URL-length limit, unlike the list endpoint).
3. **Emit** one row per history entry whose timestamp is at or after the
   query's lower bound (and ``history_since``, if set).

The offset is ``{"updatedAt": <max parent updatedAt processed>}``. Tables
are ``cdc`` keyed on ``(object_id, property, timestamp)`` so re-reads from
the lookback window or the inclusive (GTE) boundary are de-duplicated by
the pipeline's merge.
"""

import time
from datetime import datetime, timezone
from typing import Callable, Dict, Iterator, List, Optional, Tuple

import requests
from pyspark.sql.types import LongType, StringType, StructField, StructType

HISTORY_SUFFIX = "_property_history"

# Standard CRM objects that get a property history table. Custom objects
# discovered via the schemas API are added dynamically.
HISTORY_STANDARD_OBJECTS = ["contacts", "companies", "deals", "tickets"]

HISTORY_SCHEMA = StructType(
    [
        StructField("object_id", StringType(), True),
        StructField("property", StringType(), True),
        StructField("value", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("source_label", StringType(), True),
        StructField("updated_by_user_id", LongType(), True),
    ]
)

HISTORY_PRIMARY_KEYS = ["object_id", "property", "timestamp"]
HISTORY_CURSOR_FIELD = "timestamp"

# Table options (must also be listed in connector_spec.yaml allowlist).
OPT_PROPERTIES = "history_properties"
OPT_SINCE = "history_since"
OPT_LOOKBACK_MINUTES = "history_lookback_minutes"
OPT_MAX_RECORDS = "max_records_per_batch"

DEFAULT_LOOKBACK_MINUTES = 10

# HubSpot limits, see
# https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm#limits
SEARCH_PAGE_SIZE = 200  # max objects per search page
SEARCH_RESULT_CAP = 10_000  # paging past this returns HTTP 400
SEARCH_MIN_INTERVAL_S = 0.25  # search is limited to 5 req/s per account
BATCH_READ_SIZE = 50  # history reduces per-request capacity; stay well under 100
BATCH_MIN_INTERVAL_S = 0.1

# 429 handling
MAX_RETRIES = 5
MAX_BACKOFF_S = 30.0
REQUEST_TIMEOUT_S = 60


def is_history_table(table_name: str) -> bool:
    return table_name.endswith(HISTORY_SUFFIX) and len(table_name) > len(HISTORY_SUFFIX)


def history_table_name(object_type: str) -> str:
    return f"{object_type}{HISTORY_SUFFIX}"


def base_object_type(table_name: str) -> str:
    return table_name[: -len(HISTORY_SUFFIX)]


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    """Parse a HubSpot ISO-8601 timestamp (``Z`` suffix, optional millis)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class HubspotPropertyHistory:  # pylint: disable=too-many-instance-attributes
    """Reads ``{object}_property_history`` tables for the HubSpot connector."""

    def __init__(
        self,
        base_url: str,
        auth_header: Dict[str, str],
        init_ts: str,
        get_property_names: Callable[[str], List[str]],
        get_cursor_property: Callable[[str], str],
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url
        self.auth_header = auth_header
        self._init_ts = init_ts
        self._init_dt = _parse_ts(init_ts)
        self._get_property_names = get_property_names
        self._get_cursor_property = get_cursor_property
        self._sleep = sleep
        # Tables whose lookback has already been applied in this trigger.
        # A connector instance lives for one streaming query run, so this
        # applies the lookback once per trigger rather than on every call.
        self._lookback_applied: set = set()
        self._last_call: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @staticmethod
    def get_table_schema() -> StructType:
        return HISTORY_SCHEMA

    @staticmethod
    def get_table_metadata() -> dict:
        return {
            "primary_keys": list(HISTORY_PRIMARY_KEYS),
            "cursor_field": HISTORY_CURSOR_FIELD,
            "ingestion_type": "cdc",
        }

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read_table(  # pylint: disable=too-many-locals
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        object_type = base_object_type(table_name)
        table_options = table_options or {}
        start_offset = start_offset or {}
        watermark_dt = _parse_ts(start_offset.get("updatedAt"))

        # Termination: once the cursor reaches init time there is nothing
        # left to read in this trigger.
        if watermark_dt and self._init_dt and watermark_dt >= self._init_dt:
            return iter([]), start_offset

        lower_dt = self._lower_bound(table_name, watermark_dt, table_options)
        since_dt = _parse_ts(table_options.get(OPT_SINCE))
        bounds = [d for d in (lower_dt, since_dt) if d is not None]
        # Floor for emitted history entries, and lower bound for the search.
        entry_floor = max(bounds) if bounds else None
        query_lower = lower_dt or since_dt

        max_records = self._parse_max_records(table_options)
        properties = self._resolve_properties(object_type, table_options)
        cursor_property = self._get_cursor_property(object_type)

        rows: List[dict] = []
        max_updated: Optional[str] = None
        max_updated_dt: Optional[datetime] = None
        after: Optional[str] = None
        fetched = 0

        while True:
            parents, after = self._search_changed(
                object_type, cursor_property, query_lower, after
            )
            if not parents:
                break
            # Strict > boundary: the search filter is GTE-inclusive (plus the
            # per-trigger lookback), so parents at or below the start watermark
            # come back again. Emitting their rows without advancing the offset
            # violates the SimpleDataSourceStreamReader contract (a non-empty
            # batch MUST advance the end offset past the start) and the managed
            # pipeline aborts with SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE.
            # Their history was already ingested when the watermark was set, so
            # skipping them here loses nothing.
            if watermark_dt is not None:
                parents = [
                    p for p in parents
                    if (d := _parse_ts(p.get("updatedAt"))) is not None
                    and d > watermark_dt
                ]
                if not parents and after:
                    continue
                if not parents:
                    break
            fetched += len(parents)

            for chunk_start in range(0, len(parents), BATCH_READ_SIZE):
                chunk = parents[chunk_start : chunk_start + BATCH_READ_SIZE]
                results = self._batch_read_history(
                    object_type, [p["id"] for p in chunk], properties
                )
                rows.extend(self._history_rows(results, entry_floor))
                for parent in chunk:
                    updated = parent.get("updatedAt")
                    updated_dt = _parse_ts(updated)
                    if updated_dt and (max_updated_dt is None or updated_dt > max_updated_dt):
                        max_updated, max_updated_dt = updated, updated_dt

                if self._should_stop(rows, max_records, max_updated_dt, watermark_dt):
                    return iter(rows), self._next_offset(start_offset, max_updated)

            if not after:
                break
            # Stay under the 10k search-result cap: end this call and let the
            # next one re-anchor the search at the latest updatedAt seen.
            if fetched + SEARCH_PAGE_SIZE > SEARCH_RESULT_CAP and self._advanced(
                max_updated_dt, watermark_dt
            ):
                break

        return iter(rows), self._next_offset(start_offset, max_updated)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _lower_bound(
        self, table_name: str, watermark_dt: Optional[datetime], table_options: Dict[str, str]
    ) -> Optional[datetime]:
        """Watermark minus the lookback window, applied once per trigger."""
        if watermark_dt is None:
            return None
        if table_name in self._lookback_applied:
            return watermark_dt
        self._lookback_applied.add(table_name)
        minutes = self._parse_int(table_options.get(OPT_LOOKBACK_MINUTES))
        if minutes is None:
            minutes = DEFAULT_LOOKBACK_MINUTES
        if minutes <= 0:
            return watermark_dt
        return datetime.fromtimestamp(
            watermark_dt.timestamp() - minutes * 60, tz=timezone.utc
        )

    @staticmethod
    def _parse_int(raw) -> Optional[int]:
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def _parse_max_records(self, table_options: Dict[str, str]) -> Optional[int]:
        value = self._parse_int(table_options.get(OPT_MAX_RECORDS))
        return value if value and value > 0 else None

    def _resolve_properties(self, object_type: str, table_options: Dict[str, str]) -> List[str]:
        raw = table_options.get(OPT_PROPERTIES)
        if raw:
            return [p.strip() for p in raw.split(",") if p.strip()]
        return self._get_property_names(object_type)

    @staticmethod
    def _advanced(max_dt: Optional[datetime], watermark_dt: Optional[datetime]) -> bool:
        if max_dt is None:
            return False
        return watermark_dt is None or max_dt > watermark_dt

    def _should_stop(
        self,
        rows: List[dict],
        max_records: Optional[int],
        max_dt: Optional[datetime],
        watermark_dt: Optional[datetime],
    ) -> bool:
        # Only stop once the cursor has moved past the start watermark,
        # otherwise a burst of parents sharing one updatedAt would stall.
        return (
            max_records is not None
            and len(rows) >= max_records
            and self._advanced(max_dt, watermark_dt)
        )

    @staticmethod
    def _next_offset(start_offset: dict, max_updated: Optional[str]) -> dict:
        if not max_updated:
            return start_offset
        return {"updatedAt": max_updated}

    @staticmethod
    def _history_rows(results: List[dict], entry_floor: Optional[datetime]) -> Iterator[dict]:
        for obj in results:
            object_id = obj.get("id")
            history = obj.get("propertiesWithHistory") or {}
            for prop, entries in history.items():
                for entry in entries or []:
                    ts = entry.get("timestamp")
                    if entry_floor is not None:
                        ts_dt = _parse_ts(ts)
                        if ts_dt is not None and ts_dt < entry_floor:
                            continue
                    value = entry.get("value")
                    yield {
                        "object_id": object_id,
                        "property": prop,
                        "value": None if value == "" else value,
                        "timestamp": ts,
                        "source_type": entry.get("sourceType"),
                        "source_id": entry.get("sourceId"),
                        "source_label": entry.get("sourceLabel"),
                        "updated_by_user_id": entry.get("updatedByUserId"),
                    }

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _search_changed(
        self,
        object_type: str,
        cursor_property: str,
        lower_dt: Optional[datetime],
        after: Optional[str],
    ) -> Tuple[List[dict], Optional[str]]:
        filters = []
        if lower_dt is not None:
            filters.append(
                {"propertyName": cursor_property, "operator": "GTE", "value": str(_to_ms(lower_dt))}
            )
        if self._init_dt is not None:
            filters.append(
                {
                    "propertyName": cursor_property,
                    "operator": "LTE",
                    "value": str(_to_ms(self._init_dt)),
                }
            )
        body = {
            "filterGroups": [{"filters": filters}] if filters else [],
            "sorts": [{"propertyName": cursor_property, "direction": "ASCENDING"}],
            "limit": SEARCH_PAGE_SIZE,
            "properties": [cursor_property],
        }
        if after:
            body["after"] = after
        url = f"{self.base_url}/crm/v3/objects/{object_type}/search"
        data = self._post("search", url, body, SEARCH_MIN_INTERVAL_S)
        results = data.get("results", []) or []
        next_after = (data.get("paging") or {}).get("next", {}).get("after")
        return results, next_after

    def _batch_read_history(
        self, object_type: str, ids: List[str], properties: List[str]
    ) -> List[dict]:
        if not ids:
            return []
        body = {
            "inputs": [{"id": i} for i in ids],
            "properties": [],
            "propertiesWithHistory": properties,
        }
        url = f"{self.base_url}/crm/v3/objects/{object_type}/batch/read?archived=false"
        data = self._post("batch", url, body, BATCH_MIN_INTERVAL_S)
        return data.get("results", []) or []

    def _post(self, kind: str, url: str, body: dict, min_interval: float) -> dict:
        """POST with throttling and 429 retry (Retry-After or exponential backoff)."""
        for attempt in range(MAX_RETRIES + 1):
            self._throttle(kind, min_interval)
            resp = requests.post(
                url, headers=self.auth_header, json=body, timeout=REQUEST_TIMEOUT_S
            )
            # Batch read returns 207 when some IDs fail (e.g. deleted mid-run).
            if resp.status_code in (200, 207):
                return resp.json()
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                self._sleep(self._retry_delay(resp, attempt))
                continue
            raise RuntimeError(
                f"HubSpot API error for {url}: {resp.status_code} {resp.text}"
            )
        raise RuntimeError(f"HubSpot API error for {url}: exhausted retries")

    @staticmethod
    def _retry_delay(resp, attempt: int) -> float:
        retry_after = resp.headers.get("Retry-After") if resp.headers else None
        if retry_after:
            try:
                return min(max(float(retry_after), 0.0), MAX_BACKOFF_S)
            except ValueError:
                pass
        return min(2.0 ** attempt, MAX_BACKOFF_S)

    def _throttle(self, kind: str, min_interval: float) -> None:
        now = time.monotonic()
        last = self._last_call.get(kind)
        if last is not None and now - last < min_interval:
            self._sleep(min_interval - (now - last))
        self._last_call[kind] = time.monotonic()
