"""HubSpot property history tables (``{object}_property_history``).

One row per entry in HubSpot's ``propertiesWithHistory`` response, i.e.
one row per (object, property, change timestamp).

Reads select changed parents with a frozen last-modified time window, sorted
by numeric hs_object_id. Each page is a new query reanchored after the last
fully hydrated parent, avoiding the10,000-result paging window. In-progress
and completed JSON offsets retain the scan bounds and ID frontier so process
restarts and readBetweenOffsets replay the recorded range.

History entries are filtered only by explicit history_since. Lookback replays
parents through CDC upserts keyed by (object_id, property, timestamp), including
older entries for late-indexed parents. Indexing delays outside the configured
lookback and archived parents are not recovered by Search. Batch history is a
current API read, not a point-in-time snapshot at the frozen selection bound.
"""

import hashlib
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
OPT_INCLUDE = "include_properties"
OPT_EXCLUDE = "exclude_properties"

DEFAULT_LOOKBACK_MINUTES = 10

# HubSpot limits, see
# https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm#limits
SEARCH_PAGE_SIZE = 200  # max objects per search page
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


def _parse_property_list(raw: Optional[str]) -> Optional[List[str]]:
    """Parse a comma-separated table-option value into a list (None if unset)."""
    if not raw:
        return None
    return [p.strip() for p in raw.split(",") if p.strip()]


def filter_property_names(
    names: List[str],
    table_options: Optional[Dict[str, str]],
    cursor_property: Optional[str] = None,
) -> List[str]:
    """Apply the ``include_properties`` / ``exclude_properties`` options.

    Semantics (documented in the README):
      * ``include_properties`` (comma-separated) — if set, only these
        discovered properties are kept (unknown names are ignored, matching
        HubSpot's own behavior for unrecognised property names).
      * ``exclude_properties`` — removed from the remaining set.
      * The object's cursor property is always retained so incremental
        reads keep working, even if explicitly excluded.
    Discovery order is preserved and the result is a pure function of
    (names, table_options).
    """
    opts = table_options or {}
    include = _parse_property_list(opts.get(OPT_INCLUDE))
    exclude = set(_parse_property_list(opts.get(OPT_EXCLUDE)) or [])
    if include is not None:
        include_set = set(include)
        selected = [n for n in names if n in include_set]
    else:
        selected = list(names)
    if exclude:
        selected = [n for n in selected if n not in exclude]
    if cursor_property and cursor_property not in selected:
        selected.append(cursor_property)
    return selected


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
        get_api_object_type: Optional[Callable[[str], str]] = None,
    ) -> None:
        self.base_url = base_url
        self.auth_header = auth_header
        self._init_ts = init_ts
        self._init_dt = _parse_ts(init_ts)
        if self._init_dt is None:
            raise ValueError("HubSpot history init time must be an ISO-8601 timestamp")
        self._get_property_names = get_property_names
        self._get_cursor_property = get_cursor_property
        self._get_api_object_type = get_api_object_type or (lambda obj: obj)
        self._sleep = sleep
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

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        options = table_options or {}
        start = start_offset or {}
        watermark = self._offset_watermark(start)
        saved = self._checked_scan(start.get("history_scan"), watermark)
        if saved is not None and not saved["complete"]:
            scan = dict(saved)
        else:
            if watermark is not None and watermark >= self._init_dt:
                return iter([]), start
            scan = self._new_scan(watermark, options)
        rows, scan = self._read_scan(table_name, scan, options)
        end = {"history_scan": scan}
        if scan["complete"]:
            end["updatedAt"] = scan["upper"]
        elif watermark is not None:
            end["updatedAt"] = start["updatedAt"]
        return iter(rows), end

    def read_between_offsets(self, table_name, start_offset, end_offset, table_options):
        """Replay the recorded parent range, independently of admission limits."""
        options = table_options or {}
        start, end = start_offset or {}, end_offset or {}
        if start == end:
            return iter([])
        start_watermark = self._offset_watermark(start)
        end_watermark = self._offset_watermark(end)
        saved_start = self._checked_scan(start.get("history_scan"), start_watermark)
        saved_end = self._checked_scan(end.get("history_scan"), end_watermark)
        if saved_end is not None:
            scan = {**saved_end, "after_id": None, "complete": False,
                    "parent_count": 0, "parent_digest": hashlib.sha256(b"").hexdigest()}
            if saved_start is not None and not saved_start["complete"]:
                if (saved_start["lower"], saved_start["upper"]) != (scan["lower"], scan["upper"]):
                    raise RuntimeError("HubSpot history recovery scan bounds changed")
                scan["after_id"] = saved_start["after_id"]
                scan["parent_count"] = saved_start["parent_count"]
                scan["parent_digest"] = saved_start["parent_digest"]
            stop_id = saved_end["after_id"]
            if stop_id is None:
                return iter([])
            if scan["after_id"] is not None and int(stop_id) < int(scan["after_id"]):
                raise RuntimeError("HubSpot history recovery frontier regressed")
        else:
            # Checkpoints written before keyset scans contain only updatedAt.
            if end_watermark is None:
                raise RuntimeError("HubSpot history recovery end offset is missing")
            scan = self._new_scan(start_watermark, options)
            scan["upper"] = end["updatedAt"]
            stop_id = None
        rows, actual = self._read_scan(table_name, scan, options, recovery=True, stop_id=stop_id)
        if saved_end is not None and (
            actual["parent_count"] != saved_end["parent_count"] or
            actual["parent_digest"] != saved_end["parent_digest"]
        ):
            raise RuntimeError("HubSpot history recovery parent membership changed; checkpoint unchanged")
        return iter(rows)

    @staticmethod
    def _offset_watermark(offset):
        if not isinstance(offset, dict):
            raise RuntimeError("Malformed HubSpot history offset")
        value = offset.get("updatedAt")
        parsed = _parse_ts(value) if isinstance(value, str) else None
        if "updatedAt" in offset and parsed is None:
            raise RuntimeError("Malformed HubSpot history offset timestamp")
        return parsed

    @staticmethod
    def _checked_scan(value, watermark):
        if value is None:
            return None
        if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != 1:
            raise RuntimeError("Malformed HubSpot history scan version")
        if set(value) != {"version", "lower", "upper", "after_id", "complete",
                          "parent_count", "parent_digest"}:
            raise RuntimeError("Malformed HubSpot history scan fields")
        upper = _parse_ts(value["upper"]) if isinstance(value["upper"], str) else None
        lower = _parse_ts(value["lower"]) if isinstance(value["lower"], str) else None
        if upper is None or (value["lower"] is not None and lower is None) or (lower and lower > upper):
            raise RuntimeError("Malformed HubSpot history scan bounds")
        if type(value["complete"]) is not bool:
            raise RuntimeError("Malformed HubSpot history scan completion")
        count, digest = value["parent_count"], value["parent_digest"]
        if type(count) is not int or count < 0 or not isinstance(digest, str) or len(digest) != 64 or any(
            char not in "0123456789abcdef" for char in digest
        ):
            raise RuntimeError("Malformed HubSpot history parent membership proof")
        frontier = value["after_id"]
        if (frontier is None) != (count == 0):
            raise RuntimeError("Malformed HubSpot history parent count")
        if count == 0 and digest != hashlib.sha256(b"").hexdigest():
            raise RuntimeError("Malformed HubSpot history empty parent digest")
        if frontier is not None:
            HubspotPropertyHistory._parent_id(frontier)
        if not value["complete"] and frontier is None:
            raise RuntimeError("Malformed HubSpot history scan frontier")
        if value["complete"] and watermark != upper:
            raise RuntimeError("Malformed HubSpot history completed watermark")
        if watermark is not None and watermark > upper:
            raise RuntimeError("Malformed HubSpot history scan watermark")
        return dict(value)

    @staticmethod
    def _parent_id(value):
        if not isinstance(value, str) or not value.isascii() or not value.isdigit():
            raise RuntimeError("Malformed HubSpot history parent identity")
        return int(value)

    @staticmethod
    def _since(options):
        raw = options.get(OPT_SINCE)
        value = _parse_ts(raw) if isinstance(raw, str) else None
        if raw and value is None:
            raise ValueError("history_since must be an ISO-8601 timestamp")
        return value

    def _new_scan(self, watermark, options):
        since = self._since(options)
        lower = watermark
        if watermark is not None:
            minutes = self._parse_int(options.get(OPT_LOOKBACK_MINUTES))
            minutes = DEFAULT_LOOKBACK_MINUTES if minutes is None else max(0, minutes)
            lower = datetime.fromtimestamp(watermark.timestamp() - minutes * 60, tz=timezone.utc)
        bounds = [value for value in (lower, since) if value is not None]
        lower = min(max(bounds), self._init_dt) if bounds else None
        return {"version": 1, "lower": lower.isoformat() if lower else None,
                "upper": self._init_ts, "after_id": None, "complete": False,
                "parent_count": 0, "parent_digest": hashlib.sha256(b"").hexdigest()}

    def _read_scan(self, table_name, scan, options, recovery=False, stop_id=None):
        object_type = base_object_type(table_name)
        properties = self._resolve_properties(object_type, options)
        cursor_property = self._get_cursor_property(object_type)
        object_type = self._get_api_object_type(object_type)
        lower = _parse_ts(scan["lower"])
        upper = _parse_ts(scan["upper"])
        floor = self._since(options)
        max_records = None if recovery else self._parse_max_records(options)
        rows = []
        if (lower is not None and lower > upper) or (floor is not None and floor > upper):
            return rows, {**scan, "complete": True}
        while True:
            parents, has_more = self._search_changed(
                object_type, cursor_property, lower, scan["after_id"], upper, stop_id
            )
            if not parents:
                return rows, {**scan, "complete": True}
            last_id = self._parent_id(scan["after_id"]) if scan["after_id"] is not None else -1
            # Validate the entire page before hydrating or advancing a frontier.
            for parent in parents:
                identity = self._parent_id(parent.get("id")) if isinstance(parent, dict) else -1
                modified = _parse_ts(parent.get("updatedAt")) if isinstance(parent, dict) else None
                if identity <= last_id:
                    raise RuntimeError("HubSpot history parent identity order did not advance")
                if modified is None or modified > upper or (lower is not None and modified < lower):
                    raise RuntimeError("HubSpot history parent is outside scan bounds")
                if stop_id is not None and identity > int(stop_id):
                    raise RuntimeError("HubSpot history parent exceeds recovery frontier")
                last_id = identity
            for index in range(0, len(parents), BATCH_READ_SIZE):
                chunk = parents[index:index + BATCH_READ_SIZE]
                results = self._batch_read_history(object_type, [p["id"] for p in chunk], properties)
                rows.extend(self._history_rows(results, floor))
                digest = scan["parent_digest"]
                for parent in chunk:
                    digest = hashlib.sha256(
                        bytes.fromhex(digest) + b"\0" + parent["id"].encode("ascii")
                    ).hexdigest()
                scan = {**scan, "after_id": chunk[-1]["id"],
                        "parent_count": scan["parent_count"] + len(chunk),
                        "parent_digest": digest}
                at_page_end = index + len(chunk) == len(parents)
                at_end = at_page_end and not has_more
                if stop_id is not None and int(scan["after_id"]) == int(stop_id):
                    return rows, {**scan, "complete": True}
                if at_end:
                    return rows, {**scan, "complete": True}
                if max_records is not None and len(rows) >= max_records:
                    return rows, scan

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
            names = _parse_property_list(raw) or []
        else:
            names = self._get_property_names(object_type)
        # include_properties / exclude_properties apply to history tables too:
        # they bound which properties are requested in propertiesWithHistory.
        return filter_property_names(names, table_options)

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

    def _search_changed(self, object_type, cursor_property, lower_dt, after_id,
                        upper_dt=None, stop_id=None):
        upper_dt = upper_dt or self._init_dt
        filters = [{"propertyName": cursor_property, "operator": "LTE",
                    "value": str(_to_ms(upper_dt))}]
        if lower_dt is not None:
            filters.append({"propertyName": cursor_property, "operator": "GTE",
                            "value": str(_to_ms(lower_dt))})
        if after_id is not None:
            filters.append({"propertyName": "hs_object_id", "operator": "GT", "value": after_id})
        if stop_id is not None:
            filters.append({"propertyName": "hs_object_id", "operator": "LTE", "value": stop_id})
        body = {"filterGroups": [{"filters": filters}],
                "sorts": [{"propertyName": "hs_object_id", "direction": "ASCENDING"}],
                "limit": SEARCH_PAGE_SIZE, "properties": [cursor_property, "hs_object_id"]}
        url = f"{self.base_url}/crm/objects/2026-09/{object_type}/search"
        data = self._post("search", url, body, SEARCH_MIN_INTERVAL_S)
        if not isinstance(data, dict) or not isinstance(data.get("results"), list):
            raise RuntimeError("Malformed HubSpot history search results")
        results = data["results"]
        if len(results) > SEARCH_PAGE_SIZE:
            raise RuntimeError("Oversized HubSpot history search page")
        paging = data.get("paging", {})
        if not isinstance(paging, dict):
            raise RuntimeError("Malformed HubSpot history search paging")
        nxt = paging.get("next")
        if nxt is not None and (not isinstance(nxt, dict) or
                                not isinstance(nxt.get("after"), (str, int)) or
                                isinstance(nxt.get("after"), bool) or str(nxt["after"]) == ""):
            raise RuntimeError("Malformed HubSpot history search continuation")
        if nxt is not None and not results:
            raise RuntimeError("Empty HubSpot history search page has continuation")
        return results, nxt is not None or len(results) == SEARCH_PAGE_SIZE

    def _batch_read_history(
        self, object_type: str, ids: List[str], properties: List[str]
    ) -> List[dict]:
        if not ids:
            return []
        requested = list(dict.fromkeys(str(i) for i in ids))
        pending = requested
        received: Dict[str, dict] = {}
        url = f"{self.base_url}/crm/objects/2026-09/{object_type}/batch/read?archived=false"
        # A 207 (or even a 200) can omit requested parents. Advancing the
        # checkpoint on that subset would permanently skip their history.
        # Preserve successful results and retry only the unresolved IDs.
        for attempt in range(MAX_RETRIES + 1):
            body = {
                "inputs": [{"id": i} for i in pending],
                "properties": [],
                "propertiesWithHistory": properties,
            }
            data = self._post("batch", url, body, BATCH_MIN_INTERVAL_S)
            if not isinstance(data, dict) or not isinstance(data.get("results", []), list):
                raise RuntimeError("Malformed HubSpot history batch results")
            status = data.get("status")
            if status not in ("COMPLETE", "PENDING", "PROCESSING", "CANCELED"):
                raise RuntimeError("Malformed HubSpot history batch status")
            # A parent identity in a processing/canceled payload does not prove
            # that its property history is complete. Retry without committing it.
            if status != "COMPLETE":
                if attempt < MAX_RETRIES:
                    self._sleep(min(2.0 ** attempt, MAX_BACKOFF_S))
                continue
            for result in data.get("results", []) or []:
                object_id = str(result.get("id")) if isinstance(result, dict) else None
                if object_id not in pending:
                    raise RuntimeError("HubSpot history batch returned an unexpected object identity")
                received[object_id] = result
            pending = [i for i in requested if i not in received]
            if not pending:
                return [received[i] for i in requested]
            if attempt < MAX_RETRIES:
                self._sleep(min(2.0 ** attempt, MAX_BACKOFF_S))
        # Fail closed even for a parent deleted between search and batch read.
        # Deletion/error classification needs explicit evidence before skipping.
        raise RuntimeError(
            f"HubSpot history batch incomplete: {len(pending)} of "
            f"{len(requested)} parents unresolved after retries"
        )

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
