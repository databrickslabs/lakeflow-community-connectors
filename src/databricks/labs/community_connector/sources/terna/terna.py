"""Terna Lakeflow community connector.

Ingests Italian electricity system data (load, generation, transmission)
from the Terna Public API. Uses OAuth 2.0 Client Credentials; optional
x_api_key for the Physical Foreign Flow (transmission) endpoint.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.terna.terna_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

logger = logging.getLogger(__name__)

# Token endpoint path (relative to base_url)
TOKEN_PATH = "/public-api/access-token"
# Default base URL for data and (if applicable) transmission APIs
DEFAULT_BASE_URL = "https://api.terna.it"
# Token refresh margin: refresh if expiry is within this many seconds
TOKEN_REFRESH_MARGIN_SEC = 60
# Retries for transient errors (403 with "Over Qps" is Terna rate limit)
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
RETRIABLE_STATUS_CODES = {429, 500, 502, 503}
# Backoff when 403 "Developer Over Qps" (longer to respect rate limit)
QPS_BACKOFF_SEC = 3.0
# Terna API allows at most this many days per request; longer ranges are chunked
TERNA_MAX_DAYS_PER_REQUEST = 60
# Terna API allows history only within the last N solar years (date_from not sooner than 01/01/(year-N))
TERNA_MAX_HISTORY_SOLAR_YEARS = 5


class TernaLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Terna Public API."""

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Terna connector.

        Expected options:
            - client_id: OAuth 2.0 Client Credentials application key (required).
            - client_secret: OAuth 2.0 Client Credentials secret (required).
            - base_url: Base URL for the API (default https://api.terna.it).
            - x_api_key: Optional API key for Physical Foreign Flow when that
              endpoint uses x-api-key instead of Bearer token.
        """
        super().__init__(options)

        client_id = options.get("client_id")
        client_secret = options.get("client_secret")

        if not client_id or not client_secret:
            raise ValueError(
                "Terna connector requires 'client_id' and 'client_secret' in options"
            )

        self._client_id = client_id
        self._client_secret = client_secret
        self._base_url = (options.get("base_url") or DEFAULT_BASE_URL).rstrip("/")

        self._oauth_token: str | None = None
        self._oauth_expires_at: float = 0.0
        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"


    def list_tables(self) -> list[str]:
        """List names of all tables supported by this connector."""
        return SUPPORTED_TABLES

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Return the Spark schema for the given table."""
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """Return metadata (primary_keys, cursor_field, ingestion_type) for the table."""
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read records by date-range chunks; cursor is the last 'date' value (yyyy-mm-dd hh:mm:ss)."""
        self._validate_table(table_name)
        reader = {
            "total_load": self._read_total_load,
            #"actual_generation": self._read_actual_generation,
            #"renewable_generation": self._read_renewable_generation,
            #"physical_foreign_flow": self._read_physical_foreign_flow,
        }[table_name]
        return reader(start_offset, table_options)

    def _get_token(self) -> str:
        """Obtain or refresh OAuth 2.0 access token (Client Credentials). Retries on 403/429 (rate limit)."""
        now = time.time()
        if self._oauth_token and now < self._oauth_expires_at - TOKEN_REFRESH_MARGIN_SEC:
            return self._oauth_token

        url = f"{self._base_url}{TOKEN_PATH}"
        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        backoff = QPS_BACKOFF_SEC
        last_error = None
        for attempt in range(MAX_RETRIES):
            resp = self._session.post(
                url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            if resp.status_code == 200:
                body = resp.json()
                self._oauth_token = body.get("access_token")
                if not self._oauth_token:
                    raise RuntimeError("Terna token response missing access_token")
                expires_in = int(body.get("expire_format_api_dates_in", 300))
                self._oauth_expires_at = now + expires_in
                return self._oauth_token
            last_error = RuntimeError(
                f"Terna token request failed: {resp.status_code} {resp.text}"
            )
            if resp.status_code in (403, 429) and attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise last_error
        raise last_error

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
    ) -> requests.Response:
        """Issue an API request with Bearer or x-api-key auth; retry on 429/5xx and 403 Over Qps."""
        params = params or {}
        url = f"{self._base_url}{path}"

        self._session.headers["Authorization"] = f"Bearer {self._get_token()}"
        self._session.headers.pop("x-api-key", None)

        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            resp = self._session.request(method, url, params=params, timeout=60)
            is_retriable = resp.status_code in RETRIABLE_STATUS_CODES or (
                resp.status_code == 403 and "qps" in (resp.text or "").lower()
            )
            if not is_retriable:
                return resp
            if attempt < MAX_RETRIES - 1:
                delay = QPS_BACKOFF_SEC if (resp.status_code == 403) else backoff
                time.sleep(delay)
                backoff *= 2
        return resp

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table {table_name!r} is not supported. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )

    # -------------------------------------------------------------------------
    # Date-range helpers (API uses dd/mm/yyyy; cursor is yyyy-mm-dd hh:mm:ss)
    # -------------------------------------------------------------------------

    @staticmethod
    def _format_api_date(dt: datetime) -> str:
        """Format datetime as dd/mm/yyyy for API query params."""
        return dt.strftime("%d/%m/%Y")

    @staticmethod
    def _string_to_datetime(date_string: str) -> datetime:
        """Format datetime as dd/mm/yyyy for API query params."""
        return datetime.strptime(date_string, "%d/%m/%Y").replace(tzinfo=timezone.utc)

    @staticmethod
    def _format_cursor(dt: datetime) -> str:
        """Format datetime for cursor/range_end (yyyy-mm-dd hh:mm:ss)."""
        return dt.strftime("%d/%m/%Y")

    def _read_table_chunk(
        self,
        table_name: str,
        path: str,
        date_from: datetime,
        date_to: datetime,
        table_options: dict[str, str],
        array_key: str,
        extra_params: dict[str, str] | None = None,
    ) -> list[dict]:
        """Request one date chunk and return the data array; empty list on error or no data."""

        logger.info(f"Querying data for api {table_name} from: {date_from}, to {date_to}")

        if date_from == date_to:
            return []
        
        params = {
            "dateFrom": self._format_api_date(date_from),
            "dateTo": self._format_api_date(date_to),
        }
        if extra_params:
            params.update(extra_params)

        logger.debug(f"Querying data for api {table_name} with params: {params}")

        resp = self._request("GET", path, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for {table_name}: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get(array_key)

        if not isinstance(data, list):
            return []

        return data

    def _read_total_load(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read total_load in one date chunk. Optional table_options: biddingZone (comma or repeated)."""

        extra = {}
        bidding_zone = (
            table_options.get("biddingZone")
            or table_options.get("bidding_zone")
            or table_options.get("biddingzone")
        )
        if bidding_zone:
            # API accepts multiple biddingZone params
            extra["biddingZone"] = bidding_zone.strip()
        
        date_from = (
            table_options.get("date_from")
            or table_options.get("dateFrom")
            or table_options.get("datefrom")
        )

        date_to = (
            table_options.get("date_to")
            or table_options.get("dateTo")
            or table_options.get("dateto")
        )

        if date_from is None:
            raise ValueError(
                "Terna connector, API total_load requires 'date_from'"
            )

        date_from = self._string_to_datetime(date_from)
        now = datetime.now(timezone.utc)
        min_allowed = datetime(
            now.year - TERNA_MAX_HISTORY_SOLAR_YEARS, 1, 1, tzinfo=timezone.utc
        )
        if date_from < min_allowed:
            raise ValueError(
                f"Terna connector: 'date_from' must be within the last {TERNA_MAX_HISTORY_SOLAR_YEARS} solar years, not sooner than 01/01/{now.year - TERNA_MAX_HISTORY_SOLAR_YEARS}"
            )

        if date_to is not None:
            date_to = self._string_to_datetime(date_to)
        else:
            date_to = datetime.now(timezone.utc)

        if self._format_cursor(date_from) == self._format_cursor(date_to):
            return iter([]), {'cursor': self._format_cursor(date_to)}

        if start_offset is None or start_offset.get("cursor") is None:
            # We are in a full refresh. Normally I want all data from date_from to date_to unless date_to is None
            pass       
        else:
            # We are in CDC. date_from is the cursor from the previous run. Normally I want all data from date_from to date_to unless date_to is None
            date_from = self._string_to_datetime(start_offset.get("cursor"))

            if date_to is None:
                # We are in the setup where I want all data from date_from to current time
                pass
        
        chunks = []
        current_start = date_from
        while current_start < date_to:
            current_end = min(current_start + timedelta(days=TERNA_MAX_DAYS_PER_REQUEST - 1), date_to)
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        logger.info(f"Requested more than 60 days, will be split in {len(chunks)} API calls.")

        records = []
        for chunk in chunks:
            date_from, date_to = chunk
            records.extend(self._read_table_chunk(
                "total_load",
                "/load/v2.0/total-load",
                date_from,
                date_to,
                table_options,
                "total_load",
                extra_params=extra if extra else None,
            ))

        last_chunk_end = chunks[-1][1]
        return iter(records), {'cursor': self._format_cursor(last_chunk_end)}
'''
    def _read_actual_generation(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read actual_generation in one date chunk. Optional table_options: type (primary source)."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        extra = {}
        type_opt = table_options.get("type")
        if type_opt:
            extra["type"] = type_opt.strip()

        records = self._read_table_chunk(
            "actual_generation",
            "/generation/v2.0/actual-generation",
            from_date,
            to_date,
            table_options,
            "actual_generation",
            extra_params=extra if extra else None,
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset

    def _read_renewable_generation(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read renewable_generation in one date chunk. Optional table_options: type."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        extra = {}
        type_opt = table_options.get("type")
        if type_opt:
            extra["type"] = type_opt.strip()

        records = self._read_table_chunk(
            "renewable_generation",
            "/generation/v2.0/renewable-generation",
            from_date,
            to_date,
            table_options,
            "renewable_generation",
            extra_params=extra if extra else None,
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset

    def _read_physical_foreign_flow(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read physical_foreign_flow in one date chunk."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        records = self._read_table_chunk(
            "physical_foreign_flow",
            "/transmission/v2.0/physical-foreign-flow",
            from_date,
            to_date,
            table_options,
            "physical_foreign_flow"
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset
'''