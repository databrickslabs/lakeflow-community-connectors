"""Google Sheets and Google Docs connector for Lakeflow Community Connectors.

This module implements the LakeflowConnect interface for ingesting data from
Google Drive, Google Sheets, and Google Docs. Authentication uses OAuth 2.0
with a refresh token (client_id, client_secret, refresh_token).

Tables exposed:
  - spreadsheets: Drive file list filtered by mimeType=spreadsheet.
  - sheet_values: Cell data from a specific sheet; supports first row as
    headers (named columns, Spark-safe) or raw row_index + values array.
  - documents: Drive file list filtered by mimeType=document, with optional
    plain-text content via Drive export or Docs API.
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

# Google API base URLs
TOKEN_URL = "https://oauth2.googleapis.com/token"
DRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"
SHEETS_BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"
DOCS_BASE_URL = "https://docs.googleapis.com/v1/documents"

# Retry configuration for rate limits and server errors
INITIAL_BACKOFF = 1.0
MAX_RETRIES = 5
RETRIABLE_STATUS_CODES = {429, 500, 503}


class GoogleSheetsDocsLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Google Sheets and Google Docs.

    Uses OAuth 2.0 refresh token flow to obtain access tokens for the Drive,
    Sheets, and Docs APIs. Options must include client_id, client_secret,
    and refresh_token.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the connector with OAuth credentials.

        Args:
            options: Connection parameters. Must contain:
                - client_id: OAuth 2.0 client ID from Google Cloud Console.
                - client_secret: OAuth 2.0 client secret.
                - refresh_token: Long-lived refresh token (access_type=offline).

        Raises:
            ValueError: If any of the three credentials are missing.
        """
        super().__init__(options)
        self._client_id = options.get("client_id")
        self._client_secret = options.get("client_secret")
        self._refresh_token = options.get("refresh_token")
        if not self._client_id or not self._client_secret or not self._refresh_token:
            raise ValueError(
                "Google Sheets/Docs connector requires 'client_id', 'client_secret', and 'refresh_token' in options"
            )
        # Cached access token; refreshed when near expiry
        self._access_token: str | None = None
        self._token_expires_at: float = 0
        self._session = requests.Session()

    def _get_access_token(self) -> str:
        """Exchange refresh_token for access_token; cache with 60s buffer.

        Returns a valid access token, refreshing from Google if cached token
        is missing or within 60 seconds of expiry.

        Returns:
            A valid Bearer access token for Google APIs.

        Raises:
            ValueError: If Google returns 401 (invalid/revoked credentials).
            requests.HTTPError: For other token endpoint errors.
        """
        # Use cached token if still valid with 60s safety margin
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
        # Provide clear guidance when credentials are rejected
        if resp.status_code == 401:
            try:
                err = resp.json()
                hint = err.get("error_description", err.get("error", resp.text))
            except Exception:
                hint = resp.text or "Unauthorized"
            raise ValueError(
                "Google OAuth returned 401 Unauthorized when refreshing the access token. "
                "Check that the connection's client_id, client_secret, and refresh_token are correct, "
                "that the refresh_token was obtained with the same OAuth client (same client_id), "
                "and that it has not been revoked. Re-run the OAuth flow if needed to get a new refresh_token. "
                f"Google response: {hint}"
            ) from None
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            raise ValueError(
                f"Google OAuth token endpoint returned invalid JSON: {e}"
            ) from e
        if "access_token" not in data:
            raise ValueError(
                "Google OAuth token endpoint did not return an access_token. "
                f"Response: {data}"
            )
        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)
        return self._access_token

    def _headers(self) -> dict[str, str]:
        """Return HTTP headers with a valid Bearer token for Google APIs."""
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
        """Issue HTTP request with exponential backoff on 429/5xx.

        Retries up to MAX_RETRIES times on rate limit (429) or server
        errors (500, 503). Other status codes are returned immediately.
        """
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
        """Raise ValueError if table_name is not one of the supported tables."""
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported: {SUPPORTED_TABLES}"
            )

    def list_tables(self) -> list[str]:
        """Return the list of supported table names (spreadsheets, sheet_values, documents)."""
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Return the Spark schema for the given table.

        For sheet_values with use_first_row_as_header and spreadsheet_id in
        table_options, fetches the first row and builds a dynamic schema
        (row_index + one column per header, Spark-safe names). Otherwise
        returns the static schema (e.g. row_index + values array).
        """
        self._validate_table(table_name)
        if table_name == "sheet_values" and self._sheet_values_use_headers(table_options):
            schema = self._get_sheet_values_schema_with_headers(table_options)
            if schema is not None:
                return schema
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """Return table metadata (primary_keys, cursor_field, ingestion_type).

        For sheet_values with headers enabled, sets primary_keys to the
        first column name (e.g. ID) when the first row can be fetched.
        """
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
        """Read records from the given table; returns (iterator, next_offset).

        Dispatches to the appropriate reader for spreadsheets, sheet_values,
        or documents. Offsets are used for Drive list pagination (pageToken).
        """
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
        """List Drive files with mimeType=spreadsheet; paginate via pageToken.

        Uses Drive API files.list. start_offset may contain pageToken for
        the next page; when pageToken is explicitly None we signal end.
        """
        so = start_offset or {}
        # Sentinel: if caller passed pageToken: null, we're done paginating
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
        if resp.status_code == 403:
            raise ValueError(
                "Drive API returned 403 Forbidden. Check that the refresh token has "
                "https://www.googleapis.com/auth/drive.readonly scope and that the "
                "Google Cloud project has the Drive API enabled."
            )
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            raise ValueError(
                f"Drive API returned invalid JSON: {e}"
            ) from e
        files = data.get("files", [])
        next_token = data.get("nextPageToken")

        records = [
            {
                "id": f.get("id"),
                "name": f.get("name"),
                "mimeType": f.get("mimeType"),
                "modifiedTime": f.get("modifiedTime"),
                "createdTime": f.get("createdTime"),
            }
            for f in files
        ]
        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _sheet_values_use_headers(self, table_options: dict[str, str]) -> bool:
        """Return True if sheet_values should treat the first row as column headers.

        Default is True unless use_first_row_as_header is 'false', '0', or 'no'.
        """
        v = (table_options.get("use_first_row_as_header") or "true").strip().lower()
        return v not in ("false", "0", "no")

    @staticmethod
    def _sanitize_column_name(raw: str, index: int) -> str:
        """Convert a sheet header to a Spark-safe column name.

        Produces only [a-zA-Z0-9_]; spaces and special chars become underscores.
        Empty or blank headers become _col{index}. Used to avoid "column not
        resolved" when Spark references columns without backticks.
        """
        s = (raw or "").strip()
        if not s:
            return f"_col{index}"
        # Replace non-alphanumeric (and non-underscore) with underscore, collapse underscores
        s = re.sub(r"[^a-zA-Z0-9_]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s or f"_col{index}"

    def _fetch_sheet_first_row(self, table_options: dict[str, str]) -> list[str] | None:
        """Fetch the first row of the sheet and return Spark-safe column names.

        Calls Sheets API values.get for range {sheet_name}!1:1. Returns None
        if spreadsheet_id is missing, request fails, or no values returned.
        """
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
        try:
            data = resp.json()
        except ValueError:
            return None
        values = data.get("values", [])
        if not values:
            return None
        return [
            self._sanitize_column_name(str(h), i) for i, h in enumerate(values[0])
        ]

    def _get_sheet_values_schema_with_headers(self, table_options: dict[str, str]) -> StructType | None:
        """Build a StructType with row_index plus one string column per header.

        Used when use_first_row_as_header is true so that get_table_schema
        returns a schema matching the named columns we emit in read_table.
        Returns None if the first row cannot be fetched (no spreadsheet_id or API error).
        """
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
        """Read cell data via Sheets API values.get.

        Requires spreadsheet_id (or spreadsheetId) in table_options. Optional:
        sheet_name (default Sheet1), range (default A:Z), use_first_row_as_header
        (default true). When use_first_row_as_header is true, first row becomes
        column names and data rows are emitted as named columns (Spark-safe);
        otherwise each row is row_index + values array.
        """
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
        if resp.status_code == 403:
            raise ValueError(
                "Sheets API returned 403 Forbidden. Check that the refresh token has "
                "https://www.googleapis.com/auth/spreadsheets.readonly scope and that "
                "the Google Cloud project has the Sheets API enabled."
            )
        # 400 often means ID is an .xlsx file; Sheets API only supports native spreadsheets
        if resp.status_code == 400:
            try:
                err_body = resp.json()
                err_msg = err_body.get("error", {}).get("message", resp.text)
            except ValueError:
                err_msg = resp.text or "Bad Request"
            raise ValueError(
                f"Sheets API rejected the request (400). The spreadsheet ID may point to an "
                f"Excel (.xlsx) file instead of a native Google Sheet. Convert the file: in Drive, "
                f"open the file with Google Sheets, then use File → Save as Google Sheets and use "
                f"the new file's ID. API error: {err_msg}"
            )
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            raise ValueError(
                f"Sheets API returned invalid JSON: {e}"
            ) from e
        values = data.get("values", [])

        use_headers = self._sheet_values_use_headers(table_options)
        if use_headers and len(values) >= 1:
            # First row = headers (sanitized); remaining rows = data with named columns
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
        # Raw mode: every row is row_index + values array
        records = [
            {"row_index": str(i + 1), "values": [str(c) if c is not None else "" for c in row]}
            for i, row in enumerate(values)
        ]
        return iter(records), {}

    def _read_documents(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """List Drive files with mimeType=document; optionally fetch body content.

        Paginates via pageToken. When include_content is true/1/yes, each
        file's plain-text content is fetched via Drive files.export.
        """
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
        if resp.status_code == 403:
            raise ValueError(
                "Drive API returned 403 Forbidden. Check that the refresh token has "
                "https://www.googleapis.com/auth/drive.readonly scope and that the "
                "Google Cloud project has the Drive API enabled."
            )
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            raise ValueError(
                f"Drive API returned invalid JSON: {e}"
            ) from e
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
                rec["content"] = self._fetch_document_content(doc_id)
            records.append(rec)

        next_offset = {"pageToken": next_token} if next_token else {"pageToken": None}
        return iter(records), next_offset

    def _fetch_document_content(self, document_id: str) -> str | None:
        """Fetch document body as plain text via Drive files.export.

        Uses mimeType=text/plain. Returns None on failure (e.g. 403, 404,
        or non-200). Subject to Drive export size limits (e.g. 10 MB).
        Does not raise; failures are treated as missing content.
        """
        url = f"https://www.googleapis.com/drive/v3/files/{document_id}/export"
        try:
            resp = self._request(
                "GET", url, params={"mimeType": "text/plain"}
            )
        except Exception:
            return None
        if resp.status_code != 200:
            return None
        return resp.text if resp.text else None
