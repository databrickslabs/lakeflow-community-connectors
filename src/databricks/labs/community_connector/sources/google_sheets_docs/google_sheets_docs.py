"""Google Sheets and Google Docs connector for Lakeflow Community Connectors.

Uses OAuth2 (client_id, client_secret, refresh_token) to access Drive, Sheets,
and Docs APIs. Tables: spreadsheets (Drive file list), sheet_values (cell data),
documents (Drive file list + optional content).
"""

import re
import time
from urllib.parse import quote
from typing import Any, Iterator

import requests
from pyspark.sql.types import StructType, StructField, StringType

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
        if table_name == "sheet_values" and self._sheet_values_use_headers(table_options):
            schema = self._get_sheet_values_schema_with_headers(table_options)
            if schema is not None:
                return schema
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        meta = dict(TABLE_METADATA[table_name])
        if table_name == "sheet_values" and self._sheet_values_use_headers(table_options):
            headers = self._fetch_sheet_first_row(table_options)
            if headers:
                meta["primary_keys"] = [headers[0]]
        return meta

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

    def _sheet_values_use_headers(self, table_options: dict[str, str]) -> bool:
        """True if sheet_values should use first row as column headers (default True)."""
        v = (table_options.get("use_first_row_as_header") or "true").strip().lower()
        return v not in ("false", "0", "no")

    @staticmethod
    def _sanitize_column_name(raw: str, index: int) -> str:
        """Convert a sheet header to a Spark-safe column name (alphanumeric + underscore)."""
        s = (raw or "").strip()
        if not s:
            return f"_col{index}"
        # Replace non-alphanumeric (and non-underscore) with underscore, collapse underscores
        s = re.sub(r"[^a-zA-Z0-9_]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s or f"_col{index}"

    def _fetch_sheet_first_row(self, table_options: dict[str, str]) -> list[str] | None:
        """Fetch first row of the sheet; return Spark-safe column names (alphanumeric + underscore)."""
        spreadsheet_id = table_options.get("spreadsheet_id") or table_options.get("spreadsheetId")
        if not spreadsheet_id:
            return None
        sheet_name = table_options.get("sheet_name", "Sheet1")
        range_a1 = f"{sheet_name}!1:1"
        url = f"{SHEETS_BASE_URL}/{spreadsheet_id}/values/{quote(range_a1, safe='')}"
        params = {"valueRenderOption": "UNFORMATTED_VALUE", "majorDimension": "ROWS"}
        resp = self._request("GET", url, params=params)
        if resp.status_code != 200:
            return None
        data = resp.json()
        values = data.get("values", [])
        if not values:
            return None
        return [
            self._sanitize_column_name(str(h), i) for i, h in enumerate(values[0])
        ]

    def _get_sheet_values_schema_with_headers(self, table_options: dict[str, str]) -> StructType | None:
        """Build schema with row_index + one column per header (all string). Returns None if cannot fetch."""
        headers = self._fetch_sheet_first_row(table_options)
        if not headers:
            return None
        fields = [StructField("row_index", StringType(), nullable=True)]
        for col in headers:
            fields.append(StructField(col, StringType(), nullable=True))
        return StructType(fields)

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
        if resp.status_code == 400:
            try:
                err_body = resp.json()
                err_msg = err_body.get("error", {}).get("message", resp.text)
            except Exception:
                err_msg = resp.text or "Bad Request"
            raise ValueError(
                f"Sheets API rejected the request (400). The spreadsheet ID may point to an "
                f"Excel (.xlsx) file instead of a native Google Sheet. Convert the file: in Drive, "
                f"open the file with Google Sheets, then use File → Save as Google Sheets and use "
                f"the new file's ID. API error: {err_msg}"
            )
        resp.raise_for_status()
        data = resp.json()
        values = data.get("values", [])

        use_headers = self._sheet_values_use_headers(table_options)
        if use_headers and len(values) >= 1:
            headers = [
                self._sanitize_column_name(str(h), i) for i, h in enumerate(values[0])
            ]
            records = []
            for i, row in enumerate(values[1:], start=2):
                str_row = [str(c) if c is not None else "" for c in row]
                rec = {"row_index": str(i)}
                for j, col in enumerate(headers):
                    rec[col] = str_row[j] if j < len(str_row) else ""
                records.append(rec)
            return iter(records), {}
        records = []
        for i, row in enumerate(values):
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
