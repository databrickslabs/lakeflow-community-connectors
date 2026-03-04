"""Google Sheets and Google Docs connector for Lakeflow Community Connectors.

Uses OAuth2 (client_id, client_secret, refresh_token) to access Drive, Sheets,
and Docs APIs. Tables: spreadsheets (Drive file list), sheet_values (cell data),
documents (Drive file list + optional content).
"""

import time
from urllib.parse import quote
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

TOKEN_URL = "https://oauth2.googleapis.com/token"
DRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"
SHEETS_BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"
DOCS_BASE_URL = "https://docs.googleapis.com/v1/documents"

INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 503}


class GoogleSheetsDocsLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Google Sheets and Google Docs."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._client_id = options.get("client_id")
        self._client_secret = options.get("client_secret")
        self._refresh_token = options.get("refresh_token")
        if not self._client_id or not self._client_secret or not self._refresh_token:
            raise ValueError(
                "Google Sheets/Docs connector requires 'client_id', 'client_secret', and 'refresh_token' in options"
            )
        self._access_token: str | None = None
        self._token_expires_at: float = 0
        self._session = requests.Session()

    def _get_access_token(self) -> str:
        """Exchange refresh_token for access_token; cache with 60s buffer."""
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token
        resp = self._session.post(
            TOKEN_URL,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
                "grant_type": "refresh_token",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)
        return self._access_token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Issue request with exponential backoff on 429/5xx."""
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            if "headers" not in kwargs:
                kwargs["headers"] = self._headers()
            resp = self._session.request(method, url, params=params, **kwargs)
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        return resp

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported: {SUPPORTED_TABLES}"
            )

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

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if table_name == "spreadsheets":
            return self._read_spreadsheets(start_offset, table_options)
        if table_name == "sheet_values":
            return self._read_sheet_values(start_offset, table_options)
        if table_name == "documents":
            return self._read_documents(start_offset, table_options)
        return iter([]), {}

    def _read_spreadsheets(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """List Drive files with mimeType=spreadsheet; paginate via pageToken."""
        so = start_offset or {}
        if so.get("pageToken") is None and "pageToken" in so:
            return iter([]), so
        page_token = so.get("pageToken") if so.get("pageToken") else None
        params: dict[str, Any] = {
            "q": "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
            "pageSize": 100,
            "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,createdTime)",
            "orderBy": "modifiedTime desc",
        }
        if page_token:
            params["pageToken"] = page_token

        resp = self._request("GET", DRIVE_FILES_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        files = data.get("files", [])
        next_token = data.get("nextPageToken")

        records = []
        for f in files:
            records.append({
                "id": f.get("id"),
                "name": f.get("name"),
                "mimeType": f.get("mimeType"),
                "modifiedTime": f.get("modifiedTime"),
                "createdTime": f.get("createdTime"),
            })

        if next_token:
            next_offset = {"pageToken": next_token}
        else:
            next_offset = {"pageToken": None}
        return iter(records), next_offset

    def _read_sheet_values(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read cell data via Sheets API values.get. Requires spreadsheet_id (or spreadsheetId) in table_options."""
        spreadsheet_id = table_options.get("spreadsheet_id") or table_options.get(
            "spreadsheetId"
        )
        if not spreadsheet_id:
            raise ValueError(
                "table_options must include 'spreadsheet_id' or 'spreadsheetId' for sheet_values"
            )
        sheet_name = table_options.get("sheet_name", "Sheet1")
        range_a1 = table_options.get("range", "A:Z")
        if "!" not in range_a1:
            range_a1 = f"{sheet_name}!{range_a1}"

        url = f"{SHEETS_BASE_URL}/{spreadsheet_id}/values/{quote(range_a1, safe='')}"
        params = {"valueRenderOption": "UNFORMATTED_VALUE", "majorDimension": "ROWS"}
        resp = self._request("GET", url, params=params)
        if resp.status_code == 404:
            return iter([]), {}
        resp.raise_for_status()
        data = resp.json()
        values = data.get("values", [])

        records = []
        for i, row in enumerate(values):
            # Coerce cell values to string for schema compatibility
            str_row = [str(c) if c is not None else "" for c in row]
            records.append({"row_index": str(i + 1), "values": str_row})

        return iter(records), {}

    def _read_documents(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """List Drive files with mimeType=document; optionally fetch content via Docs API or export."""
        so = start_offset or {}
        if so.get("pageToken") is None and "pageToken" in so:
            return iter([]), so
        page_token = so.get("pageToken") if so.get("pageToken") else None
        params: dict[str, Any] = {
            "q": "mimeType='application/vnd.google-apps.document' and trashed=false",
            "pageSize": 100,
            "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,createdTime)",
            "orderBy": "modifiedTime desc",
        }
        if page_token:
            params["pageToken"] = page_token

        resp = self._request("GET", DRIVE_FILES_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        files = data.get("files", [])
        next_token = data.get("nextPageToken")

        include_content = table_options.get("include_content", "").lower() in (
            "true",
            "1",
            "yes",
        )

        records = []
        for f in files:
            doc_id = f.get("id")
            rec = {
                "id": doc_id,
                "name": f.get("name"),
                "mimeType": f.get("mimeType"),
                "modifiedTime": f.get("modifiedTime"),
                "createdTime": f.get("createdTime"),
                "content": None,
            }
            if include_content and doc_id:
                content = self._fetch_document_content(doc_id)
                rec["content"] = content
            records.append(rec)

        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _fetch_document_content(self, document_id: str) -> str | None:
        """Fetch plain text via Drive files.export (10 MB limit)."""
        url = f"https://www.googleapis.com/drive/v3/files/{document_id}/export"
        resp = self._request(
            "GET", url, params={"mimeType": "text/plain"}
        )
        if resp.status_code != 200:
            return None
        return resp.text if resp.text else None
