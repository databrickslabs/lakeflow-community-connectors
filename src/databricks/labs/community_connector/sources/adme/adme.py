"""ADME (Azure Data Manager for Energy) — OSDU connector.

Implements LakeflowConnect + SupportsPartitionedStream for three OSDU
master-data kinds: Wellbore, Reservoir, and Rock_and_Fluid (which maps to
master-data--Sample).

Authentication: four modes selected via auth_mode:
  * service_principal (default) — Azure AD OAuth2 client-credentials flow.
  * managed_identity — Azure system- or user-assigned managed identity.
  * federated_identity — OIDC / Workload Identity federation.
  * static_token — pre-issued bearer token (testing / CI only).

Read path: POST /api/search/v2/query_with_cursor with a Lucene modifyTime
range filter for incremental sync. Pagination via the opaque cursor field.

Partitioned stream: the OSDU Search API supports range queries on modifyTime,
which fits SupportsPartitionedStream naturally — latest_offset returns a
snapshot timestamp and get_partitions splits the time range into independent
windows that executors process in parallel.
"""

import json
import os
import re
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

# OSDU Search "query_with_cursor" caps page size at 1000.
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000

DEFAULT_WINDOW_DAYS = 1
DEFAULT_LOOKBACK_MINUTES = 5

EPOCH_ISO = "1970-01-01T00:00:00.000Z"
FIRST_RUN_SENTINEL_THRESHOLD = "2000-01-01T00:00:00.000Z"

_DATA_PARTITION_ID_PATTERN = re.compile(r"[A-Za-z0-9]+(-[A-Za-z0-9]+)*")

RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0

_ERROR_BODY_LIMIT = 256


def _redact_body(text: str) -> str:
    """Return text truncated and stripped of CR/LF for safe error messages."""
    if not text:
        return ""
    flattened = text.replace("\r", " ").replace("\n", " ")
    if len(flattened) <= _ERROR_BODY_LIMIT:
        return flattened
    return flattened[:_ERROR_BODY_LIMIT] + "...[truncated]"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

AUTH_MODE_SERVICE_PRINCIPAL = "service_principal"
AUTH_MODE_MANAGED_IDENTITY = "managed_identity"
AUTH_MODE_FEDERATED_IDENTITY = "federated_identity"
AUTH_MODE_STATIC_TOKEN = "static_token"
VALID_AUTH_MODES = {
    AUTH_MODE_SERVICE_PRINCIPAL,
    AUTH_MODE_MANAGED_IDENTITY,
    AUTH_MODE_FEDERATED_IDENTITY,
    AUTH_MODE_STATIC_TOKEN,
}


class _TokenCache:
    """In-memory bearer-token cache for the Azure AD client-credentials flow."""

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        adme_api_client_id: str | None = None,
    ) -> None:
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._audience = adme_api_client_id or client_id
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    @property
    def token_url(self) -> str:
        return (
            f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token"
        )

    def get(self, force_refresh: bool = False) -> str:
        with self._lock:
            now = time.time()
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
            "scope": f"{self._audience}/.default",
        }
        resp = requests.post(
            self.token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ADME auth failed: {resp.status_code} {_redact_body(resp.text)}"
            )
        payload = resp.json()
        token = payload.get("access_token")
        if not token:
            redacted_keys = sorted(k for k in payload if k != "access_token")
            raise RuntimeError(
                "ADME auth response missing access_token; "
                f"keys present: {redacted_keys}"
            )
        expires_in = float(payload.get("expires_in", 3600))
        return token, time.time() + expires_in


class _AzureIdentityTokenProvider:
    """Token provider backed by azure.identity credentials.

    Covers managed_identity and federated_identity modes.
    """

    def __init__(self, credential: Any, scope: str) -> None:
        self._credential = credential
        self._scope = scope
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get(self, force_refresh: bool = False) -> str:
        with self._lock:
            now = time.time()
            if (
                not force_refresh
                and self._token
                and now < self._expires_at - 60
            ):
                return self._token
            tok = self._credential.get_token(self._scope)
            self._token = tok.token
            self._expires_at = float(tok.expires_on)
            return self._token

    def invalidate(self) -> None:
        with self._lock:
            self._token = None
            self._expires_at = 0.0


class _StaticTokenProvider:
    """Pre-issued bearer token (CI / testing)."""

    def __init__(self, token: str) -> None:
        if not token or not token.strip():
            raise ValueError("static_token mode requires a non-empty access_token")
        self._token = token.strip()

    def get(self, force_refresh: bool = False) -> str:
        return self._token

    def invalidate(self) -> None:
        pass


def _read_federated_token(token_inline: str | None, token_file: str | None) -> str:
    """Read federated assertion from inline value or token file."""
    if token_inline and token_inline.strip():
        return token_inline.strip()
    if token_file:
        path = os.path.expandvars(os.path.expanduser(token_file))
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    raise ValueError(
        "federated_identity mode requires federated_token or federated_token_file"
    )


def _build_token_provider(options: dict[str, str]) -> tuple[Any, str]:
    """Construct the auth provider implied by options['auth_mode'].

    Returns (provider, audience).
    """
    auth_mode = (options.get("auth_mode") or AUTH_MODE_SERVICE_PRINCIPAL).strip()
    if auth_mode not in VALID_AUTH_MODES:
        raise ValueError(
            f"unknown auth_mode {auth_mode!r}; expected one of "
            f"{sorted(VALID_AUTH_MODES)}"
        )

    tenant_id = options.get("tenant_id")
    client_id = options.get("client_id")
    adme_api_client_id = options.get("adme_api_client_id") or client_id

    if auth_mode == AUTH_MODE_SERVICE_PRINCIPAL:
        client_secret = options.get("client_secret")
        for name, value in [
            ("tenant_id", tenant_id),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ]:
            if not value:
                raise ValueError(
                    f"service_principal mode requires connection option {name!r}"
                )
        return (
            _TokenCache(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                adme_api_client_id=adme_api_client_id,
            ),
            adme_api_client_id,
        )

    if auth_mode == AUTH_MODE_STATIC_TOKEN:
        token = options.get("access_token")
        return _StaticTokenProvider(token), adme_api_client_id

    try:
        from azure.identity import (
            ClientAssertionCredential,
            ManagedIdentityCredential,
        )
    except ImportError as e:
        raise RuntimeError(
            f"auth_mode={auth_mode} requires the azure-identity package; "
            "install with `pip install azure-identity>=1.15.0`"
        ) from e

    if not adme_api_client_id:
        raise ValueError(
            f"auth_mode={auth_mode} requires connection option 'adme_api_client_id' "
            "(OAuth2 audience for the ADME API) when 'client_id' is not set"
        )

    scope = f"{adme_api_client_id}/.default"

    if auth_mode == AUTH_MODE_MANAGED_IDENTITY:
        mi_client_id = options.get("managed_identity_client_id")
        if mi_client_id:
            credential = ManagedIdentityCredential(client_id=mi_client_id)
        else:
            credential = ManagedIdentityCredential()
        return _AzureIdentityTokenProvider(credential, scope), adme_api_client_id

    # federated_identity
    if not tenant_id:
        raise ValueError("federated_identity mode requires tenant_id")
    sp_client_id = options.get("client_id")
    if not sp_client_id:
        raise ValueError(
            "federated_identity mode requires client_id (the service principal id)"
        )
    token_inline = options.get("federated_token")
    token_file = options.get("federated_token_file")
    _read_federated_token(token_inline, token_file)

    def _get_assertion() -> str:
        return _read_federated_token(token_inline, token_file)

    credential = ClientAssertionCredential(
        tenant_id=tenant_id,
        client_id=sp_client_id,
        func=_get_assertion,
    )
    return _AzureIdentityTokenProvider(credential, scope), adme_api_client_id


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class ADMELakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Azure Data Manager for Energy.

    Required connection options:
        base_url             ADME instance base URL (alias: instance_url)
        data_partition_id    e.g. opendes

    Auth options (selected by auth_mode):
        auth_mode            service_principal | managed_identity |
                             federated_identity | static_token
        tenant_id            Azure AD tenant ID
        client_id            SP / app registration client ID
        client_secret        SP secret (service_principal only)
        adme_api_client_id   Audience for the OAuth2 scope
        managed_identity_client_id   User-assigned MI client ID
        federated_token / federated_token_file   OIDC assertion source
        access_token         Pre-issued bearer token (static_token only)

    Optional:
        page_size            Search page size (default 1000, max 1000)

    Per-table options:
        window_days          Partition size in days (default 1)
        lookback_minutes     Lookback applied to start cursor (default 5)
        kind_query_<table>   Override OSDU kind query for a specific table
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        instance_url = options.get("base_url") or options.get("instance_url")
        data_partition_id = options.get("data_partition_id")

        for name, value in [
            ("base_url (or instance_url)", instance_url),
            ("data_partition_id", data_partition_id),
        ]:
            if not value:
                raise ValueError(
                    f"ADME connector requires connection option {name!r}"
                )

        if not _DATA_PARTITION_ID_PATTERN.fullmatch(data_partition_id):
            raise ValueError(
                "ADME connector option 'data_partition_id' must be "
                "alphanumeric segments joined by single hyphens "
                "(e.g. 'opendes', 'dp-int'); no leading/trailing hyphens, "
                "no consecutive hyphens, no whitespace or control chars"
            )

        self._instance_url = instance_url.rstrip("/")
        self._data_partition_id = data_partition_id

        try:
            self._page_size = max(
                1, min(MAX_PAGE_SIZE, int(options.get("page_size") or DEFAULT_PAGE_SIZE))
            )
        except (TypeError, ValueError):
            self._page_size = DEFAULT_PAGE_SIZE

        self._token_provider, self._adme_audience = _build_token_provider(options)
        self._session = requests.Session()

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
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        return table_name in SUPPORTED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the most recent offset (capped at init time)."""
        self._validate_table(table_name)
        return {"cursor": self._init_time}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split the (start, end] modifyTime range into windowed partitions."""
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
            start_iso = EPOCH_ISO
            end_iso = self._init_time
        else:
            start_iso = (start_offset or {}).get("cursor") or EPOCH_ISO
            end_iso = (end_offset or {}).get("cursor") or self._init_time

        # Compare on parsed datetimes so hand-edited or non-canonical
        # cursor strings (different precision, missing time component)
        # don't lex-order incorrectly against canonical EPOCH_ISO / init_time.
        start_dt = self._parse_iso(start_iso)
        end_dt = self._parse_iso(end_iso)
        if start_dt >= end_dt:
            return []

        # First-run optimization: single open-ended partition
        if start_dt <= self._parse_iso(FIRST_RUN_SENTINEL_THRESHOLD):
            return [{"since": EPOCH_ISO, "until": end_iso}]

        if lookback_minutes > 0:
            start_iso = self._subtract_minutes(start_iso, lookback_minutes)
            start_dt = self._parse_iso(start_iso)
            if start_dt >= end_dt:
                return []

        partitions: list[dict] = []
        cursor = start_iso
        cursor_dt = start_dt
        while cursor_dt < end_dt:
            next_iso = self._add_days(cursor, window_days)
            next_dt = self._parse_iso(next_iso)
            if next_dt > end_dt:
                next_iso = end_iso
                next_dt = end_dt
            partitions.append({"since": cursor, "until": next_iso})
            cursor = next_iso
            cursor_dt = next_dt

        return partitions

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one (since, until] window of records on an executor."""
        self._validate_table(table_name)

        since = partition["since"]
        until = partition["until"]
        kind_query = self._resolve_kind_query(table_name, table_options)

        lucene = self._build_lucene_range(since, until)
        yield from self._search_paginated(kind_query, lucene)

    # ------------------------------------------------------------------
    # LakeflowConnect.read_table — fallback for non-partitioned reads
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver fallback path."""
        self._validate_table(table_name)

        since = (start_offset or {}).get("cursor") or EPOCH_ISO
        if self._parse_iso(since) >= self._parse_iso(self._init_time):
            return iter([]), start_offset or {"cursor": self._init_time}

        kind_query = self._resolve_kind_query(table_name, table_options)
        lucene = self._build_lucene_range(since, self._init_time)

        records = list(self._search_paginated(kind_query, lucene))
        return iter(records), {"cursor": self._init_time}

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
    def _resolve_kind_query(table_name: str, table_options: dict[str, str]) -> str:
        """Return the OSDU kind query for a table, honouring per-table overrides.

        Checks table_options for kind_query_<table_name_lower> first (e.g.
        kind_query_rock_and_fluid), then falls back to TABLE_TO_KIND_QUERY.
        This allows operators to point a table at a different OSDU kind
        without code changes (e.g. RockSampleAnalysis instead of Sample).
        """
        override_key = f"kind_query_{table_name.lower()}"
        override = table_options.get(override_key)
        if override and override.strip():
            return override.strip()
        return TABLE_TO_KIND_QUERY[table_name]

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
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    @staticmethod
    def _build_lucene_range(since: str, until: str) -> str:
        """Build the OSDU Lucene range filter on modifyTime.

        Uses exclusive lower bound (`{`) on non-first partitions so back-to-back
        windows are disjoint — a record on a boundary is returned by exactly
        one partition. First-run uses inclusive `[*` to catch everything from
        the epoch. The epoch check parses the cursor so a hand-edited
        checkpoint that's chronologically at-or-before epoch still hits the
        open-ended branch.
        """
        if ADMELakeflowConnect._parse_iso(since) <= ADMELakeflowConnect._parse_iso(EPOCH_ISO):
            return f'modifyTime:[* TO "{until}"]'
        return f'modifyTime:{{"{since}" TO "{until}"]'

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _common_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token_provider.get()}",
            "data-partition-id": self._data_partition_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _post_with_retry(
        self, url: str, body: dict[str, Any]
    ) -> requests.Response:
        """POST with retry on transient failures.

        Budget per logical request:
        - Up to one budget-isolated auth-refresh retry on 401 (does not
          consume the MAX_RETRIES slots).
        - Up to MAX_RETRIES retries on retriable 5xx/429 with exponential
          backoff honouring ``Retry-After``.
        - Up to MAX_RETRIES retries on transient network errors
          (ConnectionError / Timeout / SSLError), sharing the same budget.

        Worst-case round-trip count is therefore ``MAX_RETRIES + 1`` POSTs
        (initial attempt plus one 401-refresh follow-up, both then subject
        to the retriable-failure budget).
        """
        backoff = INITIAL_BACKOFF
        auth_retried = False
        attempts = 0
        resp: requests.Response | None = None
        last_exc: Exception | None = None
        while attempts < MAX_RETRIES:
            try:
                resp = self._session.post(
                    url,
                    headers=self._common_headers(),
                    data=json.dumps(body),
                    timeout=60,
                )
            except (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.SSLError,
            ) as exc:
                last_exc = exc
                attempts += 1
                if attempts >= MAX_RETRIES:
                    raise
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code == 401 and not auth_retried:
                self._token_provider.invalidate()
                auth_retried = True
                continue
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            attempts += 1
            if attempts < MAX_RETRIES:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else backoff
                except (TypeError, ValueError):
                    wait = backoff
                time.sleep(wait)
                backoff *= 2

        if resp is None and last_exc is not None:
            raise last_exc
        return resp  # type: ignore[return-value]

    def _search_paginated(
        self, kind_query: str, lucene_query: str
    ) -> Iterator[dict]:
        """Yield flat records by paginating query_with_cursor."""
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
                    f"{resp.status_code} {_redact_body(resp.text)}"
                )
            payload = resp.json()
            results = payload.get("results") or []
            for raw in results:
                yield _flatten_record(raw)

            next_cursor = payload.get("cursor")
            if not next_cursor or not results:
                return
            cursor = next_cursor


# ---------------------------------------------------------------------------
# Record flattening
# ---------------------------------------------------------------------------


def _flatten_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Project an OSDU envelope into the flat shape declared by TABLE_SCHEMAS."""
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

    if "master-data--Wellbore" in kind:
        base.update(_flatten_wellbore(data))
    elif "master-data--Reservoir" in kind:
        base.update(_flatten_reservoir(data))
    elif "master-data--Sample" in kind:
        base.update(_flatten_sample(data))

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
        "FacilityNameAliases_json": _json_or_none(data.get("FacilityNameAliases")),
        "FacilityOperators_json": _json_or_none(data.get("FacilityOperators")),
        "VerticalMeasurements_json": _json_or_none(data.get("VerticalMeasurements")),
        "SpatialLocation_json": _json_or_none(data.get("SpatialLocation")),
        "GeoContexts_json": _json_or_none(data.get("GeoContexts")),
        "DrillingReasons_json": _json_or_none(data.get("DrillingReasons")),
        "InitialCompletion_json": _json_or_none(data.get("InitialCompletion")),
        "ExtensionProperties_json": _json_or_none(data.get("ExtensionProperties")),
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
        "WaterSaturationAverage": _coerce_double(data.get("WaterSaturationAverage")),
        "PermeabilityHorizontal": _coerce_double(data.get("PermeabilityHorizontal")),
        "PermeabilityVertical": _coerce_double(data.get("PermeabilityVertical")),
        "InitialReservoirPressure": _coerce_double(data.get("InitialReservoirPressure")),
        "ReservoirTemperature": _coerce_double(data.get("ReservoirTemperature")),
        "GeoContexts_json": _json_or_none(data.get("GeoContexts")),
        "NameAliases_json": _json_or_none(data.get("NameAliases")),
        "ExtensionProperties_json": _json_or_none(data.get("ExtensionProperties")),
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
        "FormationTemperature": _coerce_double(formation_cond.get("Temperature")),
        "SampleAcquisition_json": _json_or_none(sa),
        "ExtensionProperties_json": _json_or_none(data.get("ExtensionProperties")),
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
