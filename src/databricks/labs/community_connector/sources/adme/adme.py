"""ADME (Azure Data Manager for Energy) — OSDU connector.

Implements ``LakeflowConnect`` + ``SupportsPartitionedStream`` for three
OSDU master-data kinds: Wellbore, Reservoir, and Rock_and_Fluid (which
maps to ``master-data--Sample`` — see ``adme_api_doc.md`` for the
mapping rationale).

Authentication: Azure AD OAuth2 client-credentials flow against
``login.microsoftonline.com``. Token cached in-memory; refreshed on
401 or just-before-expiry.

Read path: POST ``/api/search/v2/query_with_cursor`` with a Lucene
``modifyTime`` range filter for incremental sync. Pagination via the
opaque ``cursor`` field returned by the Search service.

Partitioned stream rationale: the OSDU Search API supports range
queries on ``modifyTime``, which fits ``SupportsPartitionedStream``
naturally — ``latest_offset`` returns a snapshot timestamp and
``get_partitions`` splits the time range into independent windows that
executors process in parallel.
"""

from __future__ import annotations

import json
import logging
import threading
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
from databricks.labs.community_connector.sources.adme.adme_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TABLE_TO_KIND_QUERY,
)


_LOG = logging.getLogger(__name__)

# OSDU Search "query_with_cursor" caps page size at 1000.
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000

# Default partition window for the time range. One day at a time keeps
# any single partition's work bounded; users can tune via the
# ``window_days`` table option.
DEFAULT_WINDOW_DAYS = 1
# Apply a small lookback so records in flight when the previous run's
# watermark was captured don't slip through. 5 minutes is conservative
# for OSDU's storage->indexer latency.
DEFAULT_LOOKBACK_MINUTES = 5

# Used as the lower bound on the very first run when there's no
# committed offset yet — Lucene's ``*`` matches everything.
EPOCH_ISO = "1970-01-01T00:00:00.000Z"

# HTTP retry configuration. ADME documents cursor inactivity timeout
# at 1 minute; transient 5xx and 429 are common.
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class _TokenCache:
    """In-memory bearer-token cache for the Azure AD client-credentials flow.

    Threadsafe: each driver-side and executor-side instance has its own
    cache, but multiple threads on the same instance share it.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> None:
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    @property
    def token_url(self) -> str:
        return (
            f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/token"
        )

    def get(self, force_refresh: bool = False) -> str:
        with self._lock:
            now = time.time()
            # Refresh ~60s before expiry to avoid mid-request expiry.
            if (
                not force_refresh
                and self._token
                and now < self._expires_at - 60
            ):
                return self._token
            self._token, self._expires_at = self._fetch()
            return self._token

    def invalidate(self) -> None:
        with self._lock:
            self._token = None
            self._expires_at = 0.0

    def _fetch(self) -> tuple[str, float]:
        body = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            # ``scope`` and ``resource`` are both required for ADME's
            # Azure AD v1 token endpoint per the ADME docs.
            "scope": f"{self._client_id}/.default",
            "resource": self._client_id,
        }
        resp = requests.post(
            self.token_url,
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ADME auth failed: {resp.status_code} {resp.text}"
            )
        payload = resp.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError(
                f"ADME auth response missing access_token: {payload}"
            )
        expires_in = float(payload.get("expires_in", 3600))
        return token, time.time() + expires_in


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class ADMELakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Azure Data Manager for Energy.

    Required connection options:
        tenant_id            Azure AD tenant ID
        client_id            App registration client ID
        client_secret        App registration client secret
        instance_url         e.g. ``https://admetest.energy.azure.com``
        data_partition_id    e.g. ``opendes`` or ``<inst>-opendes``

    Optional connection options:
        page_size            Search page size (default 1000, max 1000)

    Recognised per-table options:
        window_days            Partition size in days (default 1)
        lookback_minutes       Lookback applied to start cursor (default 5)
        max_records_per_batch  Cap on records yielded per partition /
                               read_table call. Unset or 0 = uncapped.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        tenant_id = options.get("tenant_id")
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        instance_url = options.get("instance_url")
        data_partition_id = options.get("data_partition_id")

        for name, value in [
            ("tenant_id", tenant_id),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("instance_url", instance_url),
            ("data_partition_id", data_partition_id),
        ]:
            if not value:
                raise ValueError(
                    f"ADME connector requires connection option {name!r}"
                )

        # Strip trailing slash for clean URL composition.
        self._instance_url = instance_url.rstrip("/")
        self._data_partition_id = data_partition_id
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret

        try:
            self._page_size = max(
                1, min(MAX_PAGE_SIZE, int(options.get("page_size") or DEFAULT_PAGE_SIZE))
            )
        except (TypeError, ValueError):
            self._page_size = DEFAULT_PAGE_SIZE

        self._token_cache = _TokenCache(tenant_id, client_id, client_secret)
        self._session = requests.Session()

        # Cap offsets at init time so Trigger.AvailableNow terminates
        # even when records are being continuously updated. Records
        # modified after init time get picked up by the next trigger,
        # which constructs a fresh connector instance with a newer cap.
        self._init_time = (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        )

    # ------------------------------------------------------------------
    # Schema / metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
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
        # Defensive copy so callers can't mutate the static metadata.
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        # All three OSDU tables use the same Search-cursor read path,
        # which is naturally partitionable by ``modifyTime`` window.
        return table_name in SUPPORTED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the most recent offset for incremental reading.

        We don't probe the source for the actual latest ``modifyTime``;
        instead we cap at the connector's init time. This keeps the
        offset stable across micro-batches in a single trigger
        (Trigger.AvailableNow termination guard) and avoids a
        non-paginated head query against the Search service.
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
        """Split the (start, end] ``modifyTime`` range into windowed partitions.

        For batch reads (no offsets) the range is ``EPOCH..init_time``.
        For streaming reads the range is the offsets supplied by Spark.
        Each partition is a ``(since, until)`` window the executor
        queries directly via ``read_partition``.
        """
        self._validate_table(table_name)

        window_days = self._parse_int(
            table_options.get("window_days"), DEFAULT_WINDOW_DAYS, minimum=1
        )
        lookback_minutes = self._parse_int(
            table_options.get("lookback_minutes"),
            DEFAULT_LOOKBACK_MINUTES,
            minimum=0,
        )

        if start_offset is None and end_offset is None:
            # Batch mode: start at the epoch and walk to init time.
            start_iso = EPOCH_ISO
            end_iso = self._init_time
        else:
            # Streaming mode: offsets come from initialOffset / latest_offset.
            start_iso = (start_offset or {}).get("cursor") or EPOCH_ISO
            end_iso = (end_offset or {}).get("cursor") or self._init_time

        # Empty range -> no work to do this trigger.
        if start_iso >= end_iso:
            return []

        # First-run / no-prior-watermark: emit a single open-ended
        # partition. We don't know the corpus age and walking from 1970
        # in daily windows would generate tens of thousands of empty
        # API calls. The single partition uses Lucene ``*`` for the
        # lower bound, which the OSDU index handles efficiently.
        if start_iso == EPOCH_ISO:
            return [{"since": EPOCH_ISO, "until": end_iso}]

        # Apply a lookback to the lower bound to handle records that
        # were in flight when the previous trigger sealed its watermark.
        if lookback_minutes > 0:
            start_iso = self._subtract_minutes(start_iso, lookback_minutes)
            # Re-check after the lookback shift; pathological clock cases
            # could otherwise produce inverted ranges.
            if start_iso >= end_iso:
                return []

        partitions: list[dict] = []
        cursor = start_iso
        while cursor < end_iso:
            window_end = self._add_days(cursor, window_days)
            if window_end > end_iso:
                window_end = end_iso
            partitions.append(
                {
                    "since": cursor,
                    "until": window_end,
                }
            )
            cursor = window_end

        return partitions

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one ``(since, until]`` window of records.

        Runs on Spark executors. Recreates the API state needed via
        ``self.options`` (set by the LakeflowConnect base ``__init__``)
        through the existing ``_session``/``_token_cache`` instance
        attributes that get pickled with the connector.

        Honors ``max_records_per_batch`` for admission control: when
        set, stops yielding once the cap is reached so a single
        partition cannot blow up driver/executor memory.
        """
        self._validate_table(table_name)

        since = partition["since"]
        until = partition["until"]
        kind_query = TABLE_TO_KIND_QUERY[table_name]
        cap = _parse_max_records(table_options)

        lucene = self._build_lucene_range(since, until)
        yielded = 0
        for record in self._search_paginated(kind_query, lucene):
            if cap is not None and yielded >= cap:
                return
            yield record
            yielded += 1

    # ------------------------------------------------------------------
    # LakeflowConnect.read_table — fallback for non-partitioned reads
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver fallback path.

        All three ADME tables prefer the partitioned stream path
        (``is_partitioned`` returns True), so this method is only used
        if the engine deliberately bypasses partitioning. We honour the
        contract by paginating sequentially and capping at init time.

        Honors ``max_records_per_batch``: when set, returns at most that
        many records and an offset that lets the next call resume mid-
        window from the same ``modifyTime`` lower bound.
        """
        self._validate_table(table_name)

        since = (start_offset or {}).get("cursor") or EPOCH_ISO
        # Already drained up to init time? Return a stable offset so
        # Trigger.AvailableNow terminates.
        if since >= self._init_time:
            return iter([]), start_offset or {"cursor": self._init_time}

        kind_query = TABLE_TO_KIND_QUERY[table_name]
        cap = _parse_max_records(table_options)
        lucene = self._build_lucene_range(since, self._init_time)

        records: list[dict] = []
        capped = False
        for record in self._search_paginated(kind_query, lucene):
            if cap is not None and len(records) >= cap:
                capped = True
                break
            records.append(record)

        # When the cap fires mid-window, hold the lower bound so the
        # next batch picks up the rest of the same window. When we drain
        # the window naturally, advance the cursor to init_time.
        end_offset = {"cursor": since if capped else self._init_time}
        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table {table_name!r}; "
                f"supported: {SUPPORTED_TABLES}"
            )

    @staticmethod
    def _parse_int(value: Any, default: int, *, minimum: int = 0) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed >= minimum else default

    @staticmethod
    def _add_days(iso_ts: str, days: int) -> str:
        dt = ADMELakeflowConnect._parse_iso(iso_ts) + timedelta(days=days)
        return ADMELakeflowConnect._format_iso(dt)

    @staticmethod
    def _subtract_minutes(iso_ts: str, minutes: int) -> str:
        dt = ADMELakeflowConnect._parse_iso(iso_ts) - timedelta(minutes=minutes)
        return ADMELakeflowConnect._format_iso(dt)

    @staticmethod
    def _parse_iso(iso_ts: str) -> datetime:
        # ``fromisoformat`` accepts ``Z`` only on 3.11+; normalise.
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
        # Match the OSDU storage convention: millisecond precision, ``Z`` suffix.
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    @staticmethod
    def _build_lucene_range(since: str, until: str) -> str:
        """Build the OSDU Lucene range filter on ``modifyTime``.

        OSDU Lucene quotes ISO timestamps and uses square brackets for
        inclusive bounds. We treat ``since`` as "anything modified since
        last sync" and ``until`` as the upper bound (exclusive logically;
        Lucene range is inclusive but our windows are non-overlapping so
        boundary collisions are safe).
        """
        if since == EPOCH_ISO:
            # Open-ended on the lower bound so the filter is permissive
            # on first run (and fast for OSDU's index).
            return f'modifyTime:[* TO "{until}"]'
        return f'modifyTime:["{since}" TO "{until}"]'

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _common_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token_cache.get()}",
            "data-partition-id": self._data_partition_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _post_with_retry(
        self, url: str, body: dict[str, Any]
    ) -> requests.Response:
        """POST with exponential backoff and one auth-refresh retry on 401."""
        backoff = INITIAL_BACKOFF
        auth_retried = False
        resp: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            resp = self._session.post(
                url,
                headers=self._common_headers(),
                data=json.dumps(body),
                timeout=60,
            )
            if resp.status_code == 401 and not auth_retried:
                # Token may be stale (e.g. revoked or expired early).
                self._token_cache.invalidate()
                auth_retried = True
                continue
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

        # All retries exhausted; return the last response so the caller
        # raises with full context.
        return resp  # type: ignore[return-value]

    def _search_paginated(
        self, kind_query: str, lucene_query: str
    ) -> Iterator[dict]:
        """Yield flat records by paginating ``query_with_cursor``."""
        url = f"{self._instance_url}/api/search/v2/query_with_cursor"
        cursor: str | None = None
        while True:
            body: dict[str, Any] = {
                "kind": kind_query,
                "query": lucene_query,
                "limit": self._page_size,
            }
            if cursor:
                body["cursor"] = cursor

            resp = self._post_with_retry(url, body)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"ADME search failed for kind={kind_query!r}: "
                    f"{resp.status_code} {resp.text}"
                )
            payload = resp.json()
            results = payload.get("results") or []
            for raw in results:
                yield _flatten_record(raw)

            next_cursor = payload.get("cursor")
            # OSDU stops returning a cursor (or returns an empty/null one)
            # when the result set is exhausted.
            if not next_cursor or not results:
                return
            cursor = next_cursor


# ---------------------------------------------------------------------------
# Record flattening
# ---------------------------------------------------------------------------


def _parse_max_records(table_options: dict[str, str]) -> int | None:
    """Parse the optional ``max_records_per_batch`` admission-control cap.

    Returns ``None`` when unset or non-numeric (uncapped).
    """
    raw = table_options.get("max_records_per_batch")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _flatten_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Project an OSDU envelope into the flat shape declared by TABLE_SCHEMAS.

    Envelope fields are lifted to typed columns; complex ``data.*``
    sub-objects are JSON-stringified into ``*_json`` columns. Whatever
    isn't lifted is preserved as ``data_json`` for downstream escape
    hatches.
    """
    if not isinstance(raw, dict):
        return {}

    data = raw.get("data") if isinstance(raw.get("data"), dict) else {}
    acl = raw.get("acl") if isinstance(raw.get("acl"), dict) else {}
    legal = raw.get("legal") if isinstance(raw.get("legal"), dict) else {}

    base: dict[str, Any] = {
        "id": raw.get("id"),
        "kind": raw.get("kind"),
        "version": _coerce_long(raw.get("version")),
        "createTime": raw.get("createTime"),
        "createUser": raw.get("createUser"),
        "modifyTime": raw.get("modifyTime") or raw.get("createTime"),
        "modifyUser": raw.get("modifyUser"),
        "acl_owners": _as_string_list(acl.get("owners")),
        "acl_viewers": _as_string_list(acl.get("viewers")),
        "legal_legaltags": _as_string_list(legal.get("legaltags")),
        "legal_status": legal.get("status"),
        "legal_otherRelevantDataCountries": _as_string_list(
            legal.get("otherRelevantDataCountries")
        ),
        "tags_json": _json_or_none(raw.get("tags")),
        "meta_json": _json_or_none(raw.get("meta")),
        "data_json": _json_or_none(data),
    }

    kind = raw.get("kind") or ""

    if "Wellbore" in kind:
        base.update(_flatten_wellbore(data))
    elif "Reservoir" in kind:
        base.update(_flatten_reservoir(data))
    elif "Sample" in kind:
        base.update(_flatten_sample(data))
    # Unknown kinds: just ship the envelope. The flattener won't add
    # any kind-specific columns so the row uses the column defaults.

    return base


def _flatten_wellbore(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "FacilityID": data.get("FacilityID"),
        "FacilityName": data.get("FacilityName"),
        "FacilityTypeID": data.get("FacilityTypeID"),
        "WellID": data.get("WellID"),
        "KickOffWellboreID": data.get("KickOffWellboreID"),
        "StatusSummary": data.get("StatusSummary"),
        "TargetFormation": data.get("TargetFormation"),
        "FacilityNameAliases_json": _json_or_none(
            data.get("FacilityNameAliases")
        ),
        "FacilityOperators_json": _json_or_none(data.get("FacilityOperators")),
        "VerticalMeasurements_json": _json_or_none(
            data.get("VerticalMeasurements")
        ),
        "SpatialLocation_json": _json_or_none(data.get("SpatialLocation")),
        "GeoContexts_json": _json_or_none(data.get("GeoContexts")),
        "DrillingReasons_json": _json_or_none(data.get("DrillingReasons")),
        "InitialCompletion_json": _json_or_none(data.get("InitialCompletion")),
        "ExtensionProperties_json": _json_or_none(
            data.get("ExtensionProperties")
        ),
    }


def _flatten_reservoir(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "ReservoirID": data.get("ReservoirID"),
        "ReservoirName": data.get("ReservoirName"),
        "ReservoirType": data.get("ReservoirType"),
        "ReservoirDescription": data.get("ReservoirDescription"),
        "FieldID": data.get("FieldID"),
        "BasinID": data.get("BasinID"),
        "FormationID": data.get("FormationID"),
        "FluidTypeID": data.get("FluidTypeID"),
        "DepthTopMD": _coerce_double(data.get("DepthTopMD")),
        "DepthBaseMD": _coerce_double(data.get("DepthBaseMD")),
        "DepthTopTVD": _coerce_double(data.get("DepthTopTVD")),
        "DepthBaseTVD": _coerce_double(data.get("DepthBaseTVD")),
        "GrossThickness": _coerce_double(data.get("GrossThickness")),
        "NetPayThickness": _coerce_double(data.get("NetPayThickness")),
        "PorosityAverage": _coerce_double(data.get("PorosityAverage")),
        "WaterSaturationAverage": _coerce_double(
            data.get("WaterSaturationAverage")
        ),
        "PermeabilityHorizontal": _coerce_double(
            data.get("PermeabilityHorizontal")
        ),
        "PermeabilityVertical": _coerce_double(
            data.get("PermeabilityVertical")
        ),
        "InitialReservoirPressure": _coerce_double(
            data.get("InitialReservoirPressure")
        ),
        "ReservoirTemperature": _coerce_double(
            data.get("ReservoirTemperature")
        ),
        "GeoContexts_json": _json_or_none(data.get("GeoContexts")),
        "NameAliases_json": _json_or_none(data.get("NameAliases")),
        "ExtensionProperties_json": _json_or_none(
            data.get("ExtensionProperties")
        ),
    }


def _flatten_sample(data: dict[str, Any]) -> dict[str, Any]:
    sa = data.get("SampleAcquisition") or {}
    detail = sa.get("SampleAcquisitionDetail") or {}
    formation_cond = (
        detail.get("FormationCondition")
        if isinstance(detail.get("FormationCondition"), dict)
        else {}
    )
    return {
        "SampleAcquisitionJobID": sa.get("SampleAcquisitionJobID"),
        "SampleAcquisitionTypeID": sa.get("SampleAcquisitionTypeID"),
        "SampleAcquisitionContainerID": sa.get("SampleAcquisitionContainerID"),
        "AcquisitionStartDate": sa.get("AcquisitionStartDate"),
        "AcquisitionEndDate": sa.get("AcquisitionEndDate"),
        "CollectionServiceCompanyID": sa.get("CollectionServiceCompanyID"),
        "HandlingServiceCompanyID": sa.get("HandlingServiceCompanyID"),
        "WellboreID": detail.get("WellboreID"),
        "ToolKind": detail.get("ToolKind"),
        "RunNumber": _coerce_str(detail.get("RunNumber")),
        "TopDepth": _coerce_double(detail.get("TopDepth")),
        "BaseDepth": _coerce_double(detail.get("BaseDepth")),
        "FormationPressure": _coerce_double(formation_cond.get("Pressure")),
        "FormationTemperature": _coerce_double(
            formation_cond.get("Temperature")
        ),
        "SampleAcquisition_json": _json_or_none(sa),
        "ExtensionProperties_json": _json_or_none(
            data.get("ExtensionProperties")
        ),
    }


def _as_string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return None


def _coerce_long(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_double(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
