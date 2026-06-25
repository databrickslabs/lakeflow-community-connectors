# Gmail API Client Utilities
# Handles HTTP requests, batch operations, and parallel fetching against the
# Gmail API. The client supports two auth modes, selected by which
# credentials the connection supplied:
#
# 1. Pre-issued access token — the Unity Catalog COMMUNITY connection
#    (u2m / u2m_per_user OAuth flow) owns the OAuth dance and injects a fresh
#    bearer token at query time. Treated as opaque: no refresh, no token
#    endpoint. A 401 means the token expired / was revoked and the user
#    re-authorizes through the connection.
# 2. In-code refresh — a connection created before the u2m migration stores
#    client_id + client_secret + refresh_token; the client exchanges the
#    refresh token for short-lived access tokens at Google's token endpoint
#    and caches them. Keeps those older connections working.

import base64
import json
import re
import time
from typing import Dict, List, Optional, Generator, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


# Batch API settings
BATCH_SIZE = 50  # Gmail batch API supports up to 100, using 50 for safety
MAX_WORKERS = 3  # Concurrent workers for parallel fetching

# Drive API endpoints. The Gmail connector calls these whenever a message
# carries a Drive-hosted attachment (>25 MB or shared via Drive). Requires
# the OAuth grant to include ``drive.readonly`` alongside ``gmail.readonly``
# at consent time.
DRIVE_API_BASE = "https://www.googleapis.com/drive/v3"
GOOGLE_NATIVE_MIME_PREFIX = "application/vnd.google-apps."

# Regex catches the common Drive / Docs / Sheets / Slides URL shapes Gmail
# inlines into HTML bodies. The file ID is 25+ chars of [A-Za-z0-9_-].
_DRIVE_ID_PATTERN = re.compile(
    r"(?:drive\.google\.com/(?:file/d/|open\?id=|uc\?[^\"'\s]*?id=)|"
    r"docs\.google\.com/(?:document|spreadsheets|presentation|drawings)/d/)"
    r"([A-Za-z0-9_-]{25,})"
)


def b64url_decode(data: str) -> bytes:
    """Decode a base64url string, padding-tolerant.

    Gmail/Drive responses strip ``=`` padding on base64url data; the stdlib
    decoder rejects unpadded input. Re-pad before calling it.
    """
    if not data:
        return b""
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded)


def extract_drive_file_ids(text: str) -> List[str]:
    """Extract Drive file IDs from a message body (HTML or plain text).

    Returns IDs in insertion order, deduplicated. Empty list if no Drive
    URLs are present. Caller is responsible for choosing the right
    download path (binary via ``alt=media`` vs export for native types).
    """
    if not text:
        return []
    seen: dict[str, None] = {}
    for match in _DRIVE_ID_PATTERN.finditer(text):
        seen.setdefault(match.group(1), None)
    return list(seen.keys())


class GmailApiError(Exception):
    """Typed Gmail/Drive API failure carrying the HTTP status.

    Agent ops translate ``status`` to ErrorCode (404→not_found, 403→
    permission_denied, 401→auth_failed). Plain ``make_request`` callers
    that previously swallowed 403/404 as ``None`` are unaffected — this
    type is only raised by paths that opt in (Drive downloads, attachment
    fetches when ``raise_on_error=True``).
    """

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(f"HTTP {status}: {message}")


class GmailApiClient:
    """Gmail HTTP client supporting two OAuth modes.

    Pass a pre-issued ``access_token`` (UC COMMUNITY u2m / u2m_per_user flow
    injects it) for the opaque-token mode, or pass ``client_id`` +
    ``client_secret`` + ``refresh_token`` for the in-code refresh mode used
    by connections created before the u2m migration. The access-token mode
    takes precedence when both are present.
    """

    BASE_URL = "https://gmail.googleapis.com/gmail/v1"
    BATCH_URL = "https://gmail.googleapis.com/batch/gmail/v1"
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    DRIVE_BASE_URL = DRIVE_API_BASE

    def __init__(
        self,
        access_token: Optional[str] = None,
        user_id: str = "me",
        *,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
    ) -> None:
        self.user_id = user_id
        self._session = requests.Session()

        # Mode 1: opaque pre-issued bearer token (no refresh in code).
        self.access_token = access_token

        # Mode 2: in-code refresh credentials + cached short-lived token.
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._refreshed_token: Optional[str] = None
        self._token_expires_at = 0.0

    def get_access_token(self) -> str:
        """Return a valid bearer token for the active auth mode.

        Opaque-token mode returns the injected token verbatim. Refresh mode
        exchanges the refresh token at Google's token endpoint, caching the
        result until ~60s before expiry.
        """
        if self.access_token:
            return self.access_token

        if self._refreshed_token and time.time() < self._token_expires_at - 60:
            return self._refreshed_token

        response = requests.post(
            self.TOKEN_URL,
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        self._refreshed_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)
        return self._refreshed_token

    def get_headers(self) -> Dict[str, str]:
        """Bearer-token headers for Gmail API calls."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Accept": "application/json",
        }

    def make_request(
        self, method: str, endpoint: str, params: Optional[Dict] = None, retry_count: int = 3
    ) -> Optional[Dict]:
        """Make API request with retry and rate limit handling."""
        url = f"{self.BASE_URL}{endpoint}"

        for attempt in range(retry_count):
            response = self._session.request(
                method, url, headers=self.get_headers(), params=params, timeout=60
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - exponential backoff
                wait_time = (2**attempt) + 1
                time.sleep(wait_time)
                continue
            elif response.status_code == 404:
                # History ID expired or resource not found
                return None
            elif response.status_code == 403:
                # Forbidden - missing OAuth scope or permission
                return None
            else:
                response.raise_for_status()

        raise Exception(f"Failed after {retry_count} retries")

    def make_batch_request(
        self, endpoints: List[str], params_list: Optional[List[Dict]] = None
    ) -> List[Dict]:
        """
        Make batch API request for efficient bulk data retrieval.

        Gmail batch API allows up to 100 requests in a single HTTP call,
        reducing network overhead significantly.
        """
        if not endpoints:
            return []

        if params_list is None:
            params_list = [{}] * len(endpoints)

        # Build multipart batch request body
        boundary = "batch_gmail_connector"
        body_parts: List[str] = []

        for i, (endpoint, params) in enumerate(zip(endpoints, params_list)):
            url = f"{self.BASE_URL}{endpoint}"
            if params:
                query_string = "&".join(f"{k}={v}" for k, v in params.items())
                url = f"{url}?{query_string}"

            part = f"--{boundary}\r\n"
            part += "Content-Type: application/http\r\n"
            part += f"Content-ID: <item{i}>\r\n\r\n"
            part += f"GET {url}\r\n"
            body_parts.append(part)

        body = "\r\n".join(body_parts) + f"\r\n--{boundary}--"

        headers = self.get_headers()
        headers["Content-Type"] = f"multipart/mixed; boundary={boundary}"

        response = self._session.post(self.BATCH_URL, headers=headers, data=body, timeout=120)

        if response.status_code != 200:
            # Fall back to sequential requests on batch failure
            return self._fetch_sequential(endpoints, params_list)

        parsed = self._parse_batch_response(response.text, boundary)

        # A 200 is not proof the batch succeeded. Gmail's global batch endpoint
        # can answer 200 with a body our multipart parser cannot read, yielding
        # zero rows with no exception — which silently empties every batch-backed
        # operation (search_messages, search_threads, mailbox_overview, and the
        # incremental message/thread reads). Treat an empty parse against a
        # non-empty request as a batch failure and fall back to the per-request
        # path the table reads already rely on.
        if not parsed:
            return self._fetch_sequential(endpoints, params_list)

        return parsed

    def _parse_batch_response(self, response_text: str, boundary: str) -> List[Dict]:
        """Parse multipart batch response."""
        results = []
        parts = response_text.split(f"--{boundary}")

        for part in parts:
            if "Content-Type: application/json" in part or '{"' in part:
                # Extract JSON from the response part
                try:
                    json_start = part.find("{")
                    json_end = part.rfind("}") + 1
                    if 0 <= json_start < json_end:
                        json_str = part[json_start:json_end]
                        results.append(json.loads(json_str))
                except (json.JSONDecodeError, ValueError):
                    continue

        return results

    def _fetch_sequential(
        self, endpoints: List[str], params_list: List[Dict]
    ) -> List[Dict]:
        """Fallback sequential fetch when batch fails."""
        results = []
        for endpoint, params in zip(endpoints, params_list):
            result = self.make_request("GET", endpoint, params)
            if result:
                results.append(result)
        return results

    def fetch_details_parallel(
        self, ids: List[str], fetch_func, max_workers: int = MAX_WORKERS
    ) -> Generator[Dict, None, None]:
        """
        Fetch details in parallel using thread pool.
        Yields results as they complete for true streaming.
        """
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_func, id_): id_ for id_ in ids
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        yield result
                except Exception:
                    # Skip failed fetches, continue with others
                    continue

    # ─── Attachment / Drive download ──────────────────────────────────────

    def get_attachment(
        self, message_id: str, attachment_id: str
    ) -> bytes:
        """Fetch a Gmail attachment by ID and return decoded bytes.

        Calls ``users.messages.attachments.get`` and base64url-decodes the
        response. Raises :class:`GmailApiError` on 4xx/5xx so callers can
        map status codes to typed errors.
        """
        url = (
            f"{self.BASE_URL}/users/{self.user_id}"
            f"/messages/{message_id}/attachments/{attachment_id}"
        )
        response = self._session.get(url, headers=self.get_headers(), timeout=120)
        if response.status_code != 200:
            raise GmailApiError(response.status_code, response.text)
        payload = response.json()
        return b64url_decode(payload.get("data", ""))

    def get_drive_file_metadata(self, file_id: str) -> Dict:
        """Fetch Drive file metadata (id, name, mimeType, size).

        Needed before downloading to pick between binary download
        (``alt=media``) and export (Docs/Sheets/Slides). Same access token
        as Gmail — but only works if the user consented to the
        ``drive.readonly`` scope at OAuth time.
        """
        url = f"{self.DRIVE_BASE_URL}/files/{file_id}"
        response = self._session.get(
            url,
            headers=self.get_headers(),
            params={"fields": "id,name,mimeType,size"},
            timeout=60,
        )
        if response.status_code != 200:
            raise GmailApiError(response.status_code, response.text)
        return response.json()

    def download_drive_file(
        self,
        file_id: str,
        dest_path: str,
        export_mime_type: Optional[str] = None,
        chunk_size: int = 1024 * 1024,
    ) -> Tuple[int, str, str]:
        """Stream a Drive file to ``dest_path`` and return ``(size, name, mime)``.

        Binary files are fetched via ``files.get?alt=media``. Google-native
        formats (Docs/Sheets/Slides) require ``files.export`` with a
        target ``export_mime_type`` — picks ``application/pdf`` if the
        caller didn't supply one.

        Caller pre-creates the parent directory. Raises
        :class:`GmailApiError` on 4xx/5xx (use ``status == 403`` to detect
        the recipient-not-shared case).
        """
        metadata = self.get_drive_file_metadata(file_id)
        mime_type = metadata.get("mimeType", "application/octet-stream")
        name = metadata.get("name", file_id)

        if mime_type.startswith(GOOGLE_NATIVE_MIME_PREFIX):
            export_mime = export_mime_type or "application/pdf"
            url = f"{self.DRIVE_BASE_URL}/files/{file_id}/export"
            params = {"mimeType": export_mime}
            effective_mime = export_mime
        else:
            url = f"{self.DRIVE_BASE_URL}/files/{file_id}"
            params = {"alt": "media"}
            effective_mime = mime_type

        bytes_written = 0
        with self._session.get(
            url, headers=self.get_headers(), params=params, timeout=300, stream=True
        ) as response:
            if response.status_code != 200:
                raise GmailApiError(response.status_code, response.text)
            with open(dest_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fh.write(chunk)
                        bytes_written += len(chunk)
        return bytes_written, name, effective_mime
