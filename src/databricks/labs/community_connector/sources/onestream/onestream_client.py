"""Minimal HTTP client for the OneStream REST API (DataProvider endpoints)."""

from __future__ import annotations

import json
import time
from decimal import Decimal

import requests

SQL_COMMAND_PATH = "/OneStreamApi/api/DataProvider/GetAdoDataSetForSqlCommand"
DEFAULT_API_VERSION = "5.2.0"
DEFAULT_TIMEOUT_SECONDS = 900  # cold partition reads were observed at ~266 s
RETRIABLE_STATUS = {408, 429, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2.0


class OneStreamError(RuntimeError):
    """Raised for non-retriable OneStream API errors."""


def normalize_base_url(base_url: str) -> str:
    url = (base_url or "").strip().rstrip("/")
    for suffix in ("/OneStreamWeb", "/OneStreamApi"):
        if url.lower().endswith(suffix.lower()):
            url = url[: -len(suffix)]
    if not url.lower().startswith("https://"):
        # The PAT is sent as a Bearer header, so never allow cleartext HTTP.
        raise ValueError(
            "base_url must start with https:// (e.g. https://<tenant>.onestreamcloud.com)"
        )
    return url


class OneStreamClient:
    """Executes connector-generated SELECT statements via GetAdoDataSetForSqlCommand.

    Holds only plain attributes so connector instances stay picklable for
    Spark executors. A new HTTP connection is made per call.
    """

    def __init__(
        self,
        base_url: str,
        application_name: str,
        token: str,
        api_version: str = DEFAULT_API_VERSION,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not application_name:
            raise ValueError("application_name is required")
        if not token:
            raise ValueError("personal_access_token is required")
        self.base_url = normalize_base_url(base_url)
        self.application_name = application_name
        self._token = token
        self.api_version = api_version or DEFAULT_API_VERSION
        self.timeout_seconds = float(timeout_seconds)

    def run_sql(self, sql: str) -> list[dict]:
        """Run one SELECT and return its rows (decimals parsed losslessly)."""
        body = {
            "BaseWebServerUrl": f"{self.base_url}/OneStreamWeb",
            "ApplicationName": self.application_name,
            "SqlQuery": sql,
            "DbLocation": "Application",
        }
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        url = f"{self.base_url}{SQL_COMMAND_PATH}"
        backoff = INITIAL_BACKOFF_SECONDS
        last_error = ""
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.post(
                    url,
                    params={"api-version": self.api_version},
                    headers=headers,
                    json=body,
                    timeout=self.timeout_seconds,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if resp.status_code == 200:
                    return _rows(json.loads(resp.text, parse_float=Decimal))
                if resp.status_code not in RETRIABLE_STATUS:
                    raise OneStreamError(_describe_error(resp, sql))
                last_error = f"HTTP {resp.status_code}"
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        raise OneStreamError(
            f"OneStream request failed after {MAX_RETRIES} attempts ({last_error})"
        )


def _rows(payload) -> list[dict]:
    """A serialized ADO DataSet is ``{"<TableName>": [rows]}``; take the first table.

    Anything else is an error, never an empty result: treating an unexpected
    200 body as "no rows" would make snapshot tables replace themselves with
    nothing downstream.
    """
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list):
                return value
    raise OneStreamError(f"Unexpected OneStream response (no result table): {str(payload)[:300]}")


def _describe_error(resp: requests.Response, sql: str) -> str:
    detail = resp.text[:1000]
    try:
        data = resp.json()
        detail = data.get("Detail") or data.get("detail") or data.get("title") or detail
    except ValueError:
        pass
    if resp.status_code == 401:
        return (
            "OneStream rejected the personal access token (HTTP 401). "
            "Check that the PAT is valid, not expired/revoked, and belongs to this tenant."
        )
    return f"OneStream API error HTTP {resp.status_code}: {detail} | SQL: {sql[:300]}"
