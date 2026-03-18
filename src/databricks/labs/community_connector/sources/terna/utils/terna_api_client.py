"""API client for Terna Public API: auth, requests, and date-range helpers."""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

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


class TernaApiClient:
    """Handles Terna API: OAuth token, HTTP requests, table validation, date formatting, and chunk reads."""

    def __init__(self, options: dict[str, str], supported_tables: list[str]) -> None:
        
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        if not client_id or not client_secret:
            raise ValueError(
                "Terna connector requires 'client_id' and 'client_secret' in options"
            )
        self._client_id = client_id
        self._client_secret = client_secret
        self._base_url = (options.get("base_url") or DEFAULT_BASE_URL).rstrip("/")
        self._supported_tables = supported_tables
        self._oauth_token: str | None = None
        self._oauth_expires_at: float = 0.0
        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"

    def get_token(self) -> str:
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
                expires_in = int(body.get("expires_in", 300))
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

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
    ) -> requests.Response:
        """Issue an API request with Bearer or x-api-key auth; retry on 429/5xx and 403 Over Qps."""
        params = params or {}
        url = f"{self._base_url}{path}"

        self._session.headers["Authorization"] = f"Bearer {self.get_token()}"
        self._session.headers["businessID"] = str(uuid.uuid4())

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

    def validate_table(self, table_name: str) -> None:
        """Raise ValueError if table_name is not in SUPPORTED_TABLES."""
        if table_name not in self._supported_tables:
            raise ValueError(
                f"Table {table_name!r} is not supported. "
                f"Supported tables: {self._supported_tables}"
            )

    @staticmethod
    def format_api_date(dt: datetime) -> str:
        """Format datetime as dd/mm/yyyy for API query params."""
        return dt.strftime("%d/%m/%Y")

    @staticmethod
    def string_to_datetime(date_string: str) -> datetime:
        """Parse date string dd/mm/yyyy to datetime (UTC)."""
        return datetime.strptime(date_string, "%d/%m/%Y").replace(tzinfo=timezone.utc)

    @staticmethod
    def format_cursor(dt: datetime) -> str:
        """Format datetime for cursor/range_end (dd/mm/yyyy)."""
        return dt.strftime("%d/%m/%Y")

    @staticmethod
    def validate_extra_params(extra_params: str | list[str]) -> None:
        return list(set([z.strip() for z in extra_params.split(",")]))

    def read_table_chunk(
        self,
        table_name: str,
        path: str,
        date_from: datetime,
        date_to: datetime,
        table_options: dict[str, str],
        array_key: str,
        extra_params: dict[str, str | list[str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Request one date chunk and return the data array; empty list on error or no data."""
        logger.info(
            "Querying data for api %s from %s, to %s, extra params: %s",
            table_name,
            date_from,
            date_to,
            extra_params,
        )

        if date_from == date_to:
            return []

        params = {
            "dateFrom": self.format_api_date(date_from),
            "dateTo": self.format_api_date(date_to),
        }
        if extra_params is not None:
            params.update(extra_params)

        logger.debug("Path: %s, Params: %s", path, params)

        resp = self.request("GET", path, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Terna API error for {table_name}: {resp.status_code} {resp.text}"
            )
        body = resp.json()
        data = body.get(array_key)

        if not isinstance(data, list):
            return []

        return data
