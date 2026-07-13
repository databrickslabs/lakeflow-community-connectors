"""LSEG LDMS (RDMS) REST API connector.

Implements LakeflowConnect + SupportsPartitionedStream for LSEG's Data
Management System (branded RDMS). LDMS is a single generic curve / time-series
API serving oil, gas, power, freight, refinery, OPIS and tabular datasets. The
same API surface is exposed on different hosts (oil / freight) with different
permissioning, so ``base_url`` is a configurable connection option — one Unity
Catalog connection per host, each with its own ``api_key`` (mirrors the GitHub
connector's base_url override for GitHub Enterprise).

Authentication: a static API key sent verbatim in the ``Authorization`` header,
including the ``apikey-v1 `` prefix, e.g.
``Authorization: apikey-v1 66kCwNW...``. No OAuth / token exchange.

Logical tables (see lseg_ldms_schemas.py):
  * curve_values   — partitioned stream. The batch endpoint supports value_date
                     range queries (minValueDate / maxValueDate), which fits
                     SupportsPartitionedStream: latest_offset returns an
                     init-time snapshot cursor and get_partitions splits the
                     value_date range into independent windows read in parallel
                     by executors via POST /api/v1/CurveValuesBatch.
  * curve_metadata — snapshot catalog via GET /api/v1/Metadata/Search.
  * tabular_data   — snapshot provider datasets via
                     GET /api/v1/TabularData/Data/{DataType}.

Only curve_values is partitioned; the other two fall back to the single-driver
simpleStreamReader path (is_partitioned() -> False).
"""

import json
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.lakeflow_connect import (
    LakeflowConnect,
)
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.lseg_ldms.lseg_ldms_schemas import (
    CURVE_METADATA,
    CURVE_VALUES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TABULAR_DATA,
    curve_values_metadata,
)

# LDMS timestamps are ISO 8601 in UTC.
EPOCH_ISO = "1970-01-01T00:00:00Z"
# A start cursor at or before this threshold is treated as "beginning of time"
# and read as a single open-ended partition (avoids exploding the partition
# count on an unbounded first-run backfill — see get_partitions).
FIRST_RUN_SENTINEL_THRESHOLD = "2000-01-01T00:00:00Z"

DEFAULT_WINDOW_DAYS = 30
# Upper bound on the number of value_date windows a single get_partitions call
# produces. When a bounded range would exceed this, the effective window is
# widened so the partition count stays bounded (keeps executor scheduling and
# batch-call fan-out sane on very long backfills).
DEFAULT_MAX_PARTITIONS = 500

DEFAULT_SCENARIO_ID = 0
DEFAULT_RESULT_TIMEZONE = "UTC"

# CurveValuesBatch volume caps. Exceeding a cap yields per-result status
# "Truncated" (not an error); the connector then splits the window and retries.
DEFAULT_TOTAL_MAX_VALUES = 1_000_000
DEFAULT_MAX_VALUES = 100_000
# Minimum window width (seconds) below which we stop splitting on truncation.
MIN_WINDOW_SECONDS = 1
MAX_TRUNCATION_SPLITS = 12

DEFAULT_MAX_RESULTS = 1000
DEFAULT_PAGE_SIZE = 1000

# Rate-limit breach is HTTP 403 (per doc §19); 5xx are transient too.
RETRIABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
REQUEST_TIMEOUT = 60

_ERROR_BODY_LIMIT = 256


def _redact_body(text: str) -> str:
    """Return text truncated and stripped of CR/LF for safe error messages."""
    if not text:
        return ""
    flattened = text.replace("\r", " ").replace("\n", " ")
    if len(flattened) <= _ERROR_BODY_LIMIT:
        return flattened
    return flattened[:_ERROR_BODY_LIMIT] + "...[truncated]"


class LSEGLDMSLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for LSEG LDMS (RDMS).

    Required connection options:
        base_url   Host-specific base URL, e.g.
                   https://oilprod1.rdms.refinitiv.com
        api_key    Full Authorization header value including the
                   ``apikey-v1 `` prefix.

    Per-table options:
      curve_values:
        curve_ids        Comma-separated CurveIDs to fetch. If absent, curves
                         are resolved from ``metadata_query``.
        metadata_query   Metadata/Search query used to resolve curves when
                         ``curve_ids`` is not given (default "*").
        scenario_id      ScenarioID (default 0 — standard PointConnect data).
        start_date       ISO lower bound for the first-run value_date backfill.
        result_timezone  ResultTimezone for returned values (default UTC).
        ingestion_mode   "append" (default, cursor value_date) or "cdc"
                         (cursor last_update_time — see read_table_metadata).
        window_days      Partition window size in days (default 30).
        max_partitions   Cap on windows per micro-batch (default 500).
      curve_metadata:
        metadata_query   Metadata/Search query (default "*").
        max_results      Page size for Metadata/Search (default 1000).
      tabular_data:
        data_type        Required — provider dataset type (e.g. JODI).
        fields           Comma-separated column projection (recommended).
        filter           TabularData Filter expression.
        order_by         TabularData OrderBy expression (+/- prefixes).
        page_size        Page size for offset paging (default 1000).
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        base_url = options.get("base_url")
        api_key = options.get("api_key")
        for name, value in [("base_url", base_url), ("api_key", api_key)]:
            if not value:
                raise ValueError(
                    f"LSEG LDMS connector requires connection option {name!r}"
                )

        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._session = requests.Session()

        # Cap the offset at init time so a Trigger.AvailableNow run terminates:
        # once latest_offset returns this fixed value twice, get_partitions of
        # (init_time, init_time] is empty and the trigger stops. Data arriving
        # after init_time is picked up by the next trigger's fresh instance.
        self._init_time = self._format_iso(datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # Schema / metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        schema = TABLE_SCHEMAS[table_name]
        if table_name == TABULAR_DATA:
            return self._project_tabular_schema(schema, table_options)
        return schema

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        if table_name == CURVE_VALUES:
            mode = (table_options.get("ingestion_mode") or "append").lower()
            return curve_values_metadata(mode)
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # SupportsPartitionedStream — only curve_values is partitioned
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        return table_name == CURVE_VALUES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the current high-water offset, capped at init time.

        A lightweight metadata-only call: LDMS has no cheap "max value_date"
        probe and the read is already bounded by the init-time cap, so we
        return the fixed init-time snapshot. This makes the offset stabilise
        after the range is drained, guaranteeing Trigger.AvailableNow
        termination.
        """
        self._validate_table(table_name)
        return {"cursor": self._init_time}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split the (start, end] value_date range into windowed partitions."""
        self._validate_table(table_name)

        window_days = self._parse_int(
            table_options.get("window_days"), DEFAULT_WINDOW_DAYS, minimum=1
        )
        max_partitions = self._parse_int(
            table_options.get("max_partitions"),
            DEFAULT_MAX_PARTITIONS,
            minimum=1,
        )

        if start_offset is None and end_offset is None:
            start_iso = table_options.get("start_date") or EPOCH_ISO
            end_iso = self._init_time
        else:
            start_iso = (start_offset or {}).get("cursor") or table_options.get(
                "start_date"
            ) or EPOCH_ISO
            end_iso = (end_offset or {}).get("cursor") or self._init_time

        start_dt = self._parse_iso(start_iso)
        end_dt = self._parse_iso(end_iso)
        if start_dt >= end_dt:
            return []

        # First-run / unbounded backfill: a single open-ended partition rather
        # than thousands of windows from the epoch.
        if start_dt <= self._parse_iso(FIRST_RUN_SENTINEL_THRESHOLD):
            return [{"min_value_date": start_iso, "max_value_date": end_iso}]

        # Widen the window if a bounded range would exceed the partition cap.
        total_seconds = (end_dt - start_dt).total_seconds()
        window_seconds = window_days * 86400
        n_windows = math.ceil(total_seconds / window_seconds)
        if n_windows > max_partitions:
            window_seconds = math.ceil(total_seconds / max_partitions)

        partitions: list[dict] = []
        cursor_dt = start_dt
        while cursor_dt < end_dt:
            next_dt = cursor_dt + timedelta(seconds=window_seconds)
            if next_dt > end_dt:
                next_dt = end_dt
            # Inclusive [min, max] windows made disjoint by a 1s shave on the
            # upper bound of every window except the one touching end_dt, so an
            # observation on a boundary is read by exactly one partition (matters
            # for the append table, which inserts without primary-key upsert).
            if next_dt < end_dt:
                max_dt = next_dt - timedelta(seconds=1)
            else:
                max_dt = next_dt
            partitions.append(
                {
                    "min_value_date": self._format_iso(cursor_dt),
                    "max_value_date": self._format_iso(max_dt),
                }
            )
            cursor_dt = next_dt

        return partitions

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one value_date window on an executor via CurveValuesBatch."""
        self._validate_table(table_name)
        if table_name != CURVE_VALUES:
            raise ValueError(f"Table {table_name!r} does not support partitioned reads")

        min_vd = partition["min_value_date"]
        max_vd = partition["max_value_date"]
        curve_ids = self._resolve_curve_ids(table_options)
        scenario_id = self._parse_int(
            table_options.get("scenario_id"), DEFAULT_SCENARIO_ID, minimum=0
        )
        result_tz = table_options.get("result_timezone") or DEFAULT_RESULT_TIMEZONE

        yield from self._fetch_curve_values(
            curve_ids, scenario_id, min_vd, max_vd, result_tz, split_depth=0
        )

    # ------------------------------------------------------------------
    # LakeflowConnect.read_table — fallback (curve_values) + snapshot tables
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if table_name == CURVE_VALUES:
            return self._read_curve_values_single_driver(start_offset, table_options)
        if table_name == CURVE_METADATA:
            return self._read_curve_metadata(table_options)
        return self._read_tabular_data(table_options)

    # ------------------------------------------------------------------
    # curve_values read paths
    # ------------------------------------------------------------------

    def _read_curve_values_single_driver(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver fallback used when partitioning is unavailable."""
        since = (start_offset or {}).get("cursor") or table_options.get(
            "start_date"
        ) or EPOCH_ISO
        if self._parse_iso(since) >= self._parse_iso(self._init_time):
            return iter([]), (start_offset or {"cursor": self._init_time})

        curve_ids = self._resolve_curve_ids(table_options)
        scenario_id = self._parse_int(
            table_options.get("scenario_id"), DEFAULT_SCENARIO_ID, minimum=0
        )
        result_tz = table_options.get("result_timezone") or DEFAULT_RESULT_TIMEZONE

        records = list(
            self._fetch_curve_values(
                curve_ids, scenario_id, since, self._init_time, result_tz, split_depth=0
            )
        )
        return iter(records), {"cursor": self._init_time}

    def _resolve_curve_ids(self, table_options: dict[str, str]) -> list[str]:
        """Resolve the CurveIDs to fetch, self-contained for executor use.

        Explicit ``curve_ids`` win; otherwise curves are discovered via
        Metadata/Search using ``metadata_query`` (default "*").
        """
        explicit = table_options.get("curve_ids")
        if explicit:
            ids = [c.strip() for c in explicit.split(",") if c.strip()]
            if ids:
                return ids

        query = table_options.get("metadata_query") or "*"
        max_results = self._parse_int(
            table_options.get("max_results"), DEFAULT_MAX_RESULTS, minimum=1
        )
        ids = [
            r.get("curveID") or r.get("CurveID") or r.get("curve_id")
            for r in self._search_metadata(query, max_results)
        ]
        return [str(i) for i in ids if i]

    def _fetch_curve_values(
        self,
        curve_ids: list[str],
        scenario_id: int,
        min_value_date: str,
        max_value_date: str,
        result_tz: str,
        split_depth: int,
    ) -> Iterator[dict]:
        """POST CurveValuesBatch for one window, splitting on truncation.

        A per-result status of "Truncated" means the value cap was hit; when
        that happens we split the [min, max] window in half and recurse so no
        observations are silently dropped. Splitting stops at MIN_WINDOW_SECONDS
        or MAX_TRUNCATION_SPLITS depth (best effort thereafter).
        """
        body = {
            "scenarioID": scenario_id,
            "minValueDate": min_value_date,
            "maxValueDate": max_value_date,
            "resultTimezone": result_tz,
            "totalMaxValues": DEFAULT_TOTAL_MAX_VALUES,
            "maxValues": DEFAULT_MAX_VALUES,
            "sortValuesDateDescending": False,
            "curveRequests": [{"curveID": cid} for cid in curve_ids],
        }
        resp = self._request_with_retry(
            "POST", "/api/v1/CurveValuesBatch", json_body=body
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"CurveValuesBatch failed ({resp.status_code}): "
                f"{_redact_body(resp.text)}"
            )

        payload = resp.json()
        results = payload.get("results") or payload.get("Results") or []
        truncated = False
        rows: list[dict] = []
        for result in results:
            status = str(result.get("status") or result.get("Status") or "Success")
            if "Error" in status:
                # DACS permissioning / unknown curve / no data — skip the curve.
                continue
            if "Truncated" in status:
                truncated = True
            curve_id = result.get("curveID") or result.get("CurveID")
            res_scenario = result.get("scenarioID")
            if res_scenario is None:
                res_scenario = result.get("ScenarioID", scenario_id)
            for value in result.get("values") or result.get("Values") or []:
                rows.append(
                    {
                        "curve_id": curve_id,
                        "scenario_id": res_scenario,
                        "forecast_date": value.get("forecastDate")
                        or value.get("ForecastDate"),
                        "value_date": value.get("valueDate")
                        or value.get("ValueDate"),
                        "value": value.get("value")
                        if value.get("value") is not None
                        else value.get("Value"),
                        "last_update_time": value.get("lastUpdateTime")
                        or value.get("LastUpdateTime"),
                    }
                )

        if not truncated or not self._can_split(min_value_date, max_value_date, split_depth):
            yield from rows
            return

        # Truncated: discard the partial page and re-read two half-windows.
        mid = self._midpoint(min_value_date, max_value_date)
        lower_max = self._format_iso(self._parse_iso(mid) - timedelta(seconds=1))
        yield from self._fetch_curve_values(
            curve_ids, scenario_id, min_value_date, lower_max, result_tz, split_depth + 1
        )
        yield from self._fetch_curve_values(
            curve_ids, scenario_id, mid, max_value_date, result_tz, split_depth + 1
        )

    # ------------------------------------------------------------------
    # curve_metadata / tabular_data read paths
    # ------------------------------------------------------------------

    def _read_curve_metadata(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        query = table_options.get("metadata_query") or "*"
        max_results = self._parse_int(
            table_options.get("max_results"), DEFAULT_MAX_RESULTS, minimum=1
        )
        records = []
        for r in self._search_metadata(query, max_results):
            records.append(
                {
                    "curve_id": r.get("curveID") or r.get("CurveID") or r.get("curve_id"),
                    "alias": r.get("alias"),
                    "name": r.get("name"),
                    "metadata_json": self._metadata_json(r),
                }
            )
        return iter(records), {}

    def _search_metadata(self, query: str, max_results: int) -> Iterator[dict]:
        """Yield curve metadata records paging Metadata/Search (MaxResults/SkipRows)."""
        skip = 0
        while True:
            params = {
                "query": query,
                "MaxResults": str(max_results),
                "SkipRows": str(skip),
            }
            resp = self._request_with_retry(
                "GET", "/api/v1/Metadata/Search", params=params
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Metadata/Search failed ({resp.status_code}): "
                    f"{_redact_body(resp.text)}"
                )
            payload = resp.json()
            batch = payload.get("results") or payload.get("Results") or []
            if not batch:
                break
            for record in batch:
                yield record
            if len(batch) < max_results:
                break
            skip += len(batch)

    def _read_tabular_data(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        data_type = table_options.get("data_type")
        if not data_type:
            raise ValueError(
                "tabular_data requires the 'data_type' table option (e.g. JODI)"
            )
        page_size = self._parse_int(
            table_options.get("page_size"), DEFAULT_PAGE_SIZE, minimum=1
        )
        fields = table_options.get("fields")
        params: dict[str, str] = {"PageSize": str(page_size)}
        if fields:
            params["Fields"] = fields
        if table_options.get("filter"):
            params["Filter"] = table_options["filter"]
        if table_options.get("order_by"):
            params["OrderBy"] = table_options["order_by"]

        records: list[dict] = []
        skip = 0
        path = f"/api/v1/TabularData/Data/{data_type}"
        while True:
            params["SkipSize"] = str(skip)
            resp = self._request_with_retry("GET", path, params=params)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"TabularData/Data failed for {data_type!r} "
                    f"({resp.status_code}): {_redact_body(resp.text)}"
                )
            batch = self._tabular_rows(resp.json())
            if not batch:
                break
            for row in batch:
                out = {"data_type": data_type}
                for key in ("country", "product", "flow", "period", "value", "unit"):
                    out[key] = row.get(key)
                records.append(out)
            if len(batch) < page_size:
                break
            skip += len(batch)

        return iter(records), {}

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        # The api_key already contains the "apikey-v1 " prefix.
        return {
            "Authorization": self._api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request_with_retry(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict | None = None,
    ) -> requests.Response:
        """Issue a request, retrying 403 (rate limit) / 429 / 5xx with backoff."""
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            if method == "GET":
                resp = self._session.get(
                    url, headers=self._headers(), params=params, timeout=REQUEST_TIMEOUT
                )
            elif method == "POST":
                resp = self._session.post(
                    url,
                    headers=self._headers(),
                    data=json.dumps(json_body or {}),
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            if attempt < MAX_RETRIES - 1:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else backoff
                except (TypeError, ValueError):
                    wait = backoff
                time.sleep(wait)
                backoff *= 2

        return resp  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table {table_name!r}; supported: {SUPPORTED_TABLES}"
            )

    @staticmethod
    def _project_tabular_schema(
        schema: StructType, table_options: dict[str, str]
    ) -> StructType:
        """Restrict the tabular schema to the requested ``fields`` (plus data_type)."""
        fields = table_options.get("fields")
        if not fields:
            return schema
        wanted = {f.strip() for f in fields.split(",") if f.strip()}
        wanted.add("data_type")
        projected = [f for f in schema.fields if f.name in wanted]
        return StructType(projected) if projected else schema

    @staticmethod
    def _tabular_rows(payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return [r for r in payload if isinstance(r, dict)]
        if isinstance(payload, dict):
            for key in ("rows", "Rows", "results", "Results", "data", "Data"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [r for r in value if isinstance(r, dict)]
        return []

    @staticmethod
    def _metadata_json(record: dict) -> str | None:
        """Return the full tag set as a JSON string.

        Uses the record's own ``metadata_json`` if the source already provides
        one; otherwise serialises the tag columns beyond the typed core.
        """
        existing = record.get("metadata_json")
        if isinstance(existing, str):
            return existing
        tags = {
            k: v
            for k, v in record.items()
            if k not in ("curveID", "CurveID", "curve_id", "alias", "name")
        }
        if not tags:
            return None
        try:
            return json.dumps(tags, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_int(value: Any, default: int, *, minimum: int = 0) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed >= minimum else default

    def _can_split(self, lo: str, hi: str, depth: int) -> bool:
        if depth >= MAX_TRUNCATION_SPLITS:
            return False
        span = (self._parse_iso(hi) - self._parse_iso(lo)).total_seconds()
        return span > MIN_WINDOW_SECONDS

    def _midpoint(self, lo: str, hi: str) -> str:
        lo_dt = self._parse_iso(lo)
        hi_dt = self._parse_iso(hi)
        mid = lo_dt + (hi_dt - lo_dt) / 2
        return self._format_iso(mid)

    @staticmethod
    def _parse_iso(iso_ts: str) -> datetime:
        normalised = iso_ts.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalised)
        except ValueError as e:
            raise ValueError(f"Invalid ISO timestamp {iso_ts!r}") from e
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _format_iso(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
