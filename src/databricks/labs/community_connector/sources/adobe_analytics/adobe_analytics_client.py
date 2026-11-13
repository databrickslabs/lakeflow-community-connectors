"""HTTP client for the Adobe Analytics 2.0 API: OAuth token, company discovery,
rate limiting, retries and tolerant list paging."""

import logging
import random
import time
from collections import deque
from typing import Any

import requests

logger = logging.getLogger(__name__)

TOKEN_URL = "https://ims-na1.adobelogin.com/ims/token/v3"
API_HOST = "https://analytics.adobe.io"
DISCOVERY_URL = f"{API_HOST}/discovery/me"
DEFAULT_SCOPES = "openid,AdobeID,read_organizations,additional_info.projectedProductContext"

# (connect, read) seconds. Adobe's own server-side timeout is 60 s, so the read
# timeout leaves room for a slow but successful response.
REQUEST_TIMEOUT = (15, 120)
MAX_ATTEMPTS = 5
# A successful status with a body that isn't JSON is rare; retry it, but fewer times.
MAX_INVALID_JSON_ATTEMPTS = 3
INITIAL_BACKOFF_SECONDS = 2.0
MAX_WAIT_SECONDS = 120.0
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MARGIN_SECONDS = 300

# Adobe allows 12 requests per 6 s per user; stay a little below it.
RATE_LIMIT_REQUESTS = 10
RATE_LIMIT_WINDOW_SECONDS = 6.0

LIST_PAGE_SIZE = 100
MAX_LIST_PAGES = 10_000


class AdobeAnalyticsAPIError(RuntimeError):
    """A non-retriable (or retries exhausted) error response from Adobe."""

    def __init__(self, status_code: int | None, method: str, path: str, message: str):
        self.status_code = status_code
        self.path = path
        status = f"HTTP {status_code}" if status_code is not None else "a connection error"
        super().__init__(f"Adobe Analytics API {method} {path} failed with {status}: {message}")


class AdobeAnalyticsTimeoutError(AdobeAnalyticsAPIError):
    """The request kept timing out (client timeout or HTTP 504) after all retries."""


class AdobeAnalyticsInvalidResponseError(AdobeAnalyticsAPIError):
    """A successful status kept arriving with a body that isn't valid JSON (usually cut off)."""


def describe_body_size(resp: requests.Response) -> str:
    """Compare the Content-Length header with the bytes received, to tell whether a
    cut-off body was sent that way by Adobe or lost on the way. Never reads the body text."""
    decoded = len(resp.content or b"")
    header = resp.headers.get("Content-Length")
    wire = None
    tell = getattr(resp.raw, "tell", None)
    if callable(tell):
        try:
            wire = int(tell())
        except (TypeError, ValueError, OSError):
            wire = None
    received = wire if wire is not None else decoded
    encoding = resp.headers.get("Content-Encoding")
    parts = [f"{decoded} bytes after decoding"]
    if encoding:
        parts.append(f"Content-Encoding {encoding}")
    if header is None:
        parts.append("no Content-Length header (streamed response), so completeness is unknown")
    elif not header.strip().isdigit():
        parts.append(f"unreadable Content-Length header {header!r}")
    elif wire is None and encoding:
        parts.append(f"Content-Length {header}; bytes on the wire not measured")
    elif int(header) == received:
        parts.append(
            f"Content-Length {header} matches the {received} bytes received, so the body "
            "arrived exactly as Adobe sent it"
        )
    else:
        parts.append(
            f"Content-Length {header} but {received} bytes received, so bytes were lost "
            "between Adobe and this machine"
        )
    return "; ".join(parts)


def describe_error_body(resp: requests.Response) -> str:
    """Pull the most useful message out of Adobe's several error body shapes."""
    try:
        body = resp.json()
    except ValueError:
        return (resp.text or "").strip()[:1000] or "(empty response body)"
    if not isinstance(body, dict):
        return str(body)[:1000]
    code = body.get("errorCode") or body.get("error_code") or body.get("error")
    message = (
        body.get("errorDescription")
        or body.get("message")
        or body.get("error_description")
        or body.get("errorMessage")
    )
    parts = [str(p) for p in (code, message) if p]
    return " - ".join(parts) if parts else str(body)[:1000]


def extract_list_page(body: Any) -> tuple[list[dict], bool | None]:
    """Return ``(items, last_page)`` from any of Adobe's list response shapes.

    ``last_page`` is None when the shape carries no paging signal (bare array or
    single object), so the caller falls back to counting items.
    """
    if body is None:
        return [], True
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)], None
    if isinstance(body, dict):
        if isinstance(body.get("content"), list):
            last_page = body.get("lastPage")
            if last_page is None and "totalPages" in body and "number" in body:
                try:
                    last_page = int(body["number"]) + 1 >= int(body["totalPages"])
                except (TypeError, ValueError):
                    last_page = None
            return [x for x in body["content"] if isinstance(x, dict)], last_page
        if "id" in body or "rsid" in body:
            return [body], True
    keys = sorted(body.keys()) if isinstance(body, dict) else type(body).__name__
    raise ValueError(
        f"Unexpected list response shape from Adobe Analytics (keys: {keys}). "
        "Expected a JSON array or an object with a 'content' array."
    )


class AdobeAnalyticsClient:
    """Authenticated, rate-limited access to analytics.adobe.io for one credential."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        scopes: str = DEFAULT_SCOPES,
        global_company_id: str | None = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._scopes = scopes
        self._global_company_id = global_company_id
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._recent_requests: deque = deque()

    # ----- auth --------------------------------------------------------

    def _access_token(self, force_refresh: bool = False) -> str:
        if (
            not force_refresh
            and self._token
            and time.time() < self._token_expires_at - TOKEN_REFRESH_MARGIN_SECONDS
        ):
            return self._token
        resp = self._send(
            "POST",
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": self._scopes,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if resp.status_code != 200:
            detail = describe_error_body(resp)
            if resp.status_code in (400, 401, 403):
                raise ValueError(
                    f"Adobe rejected the OAuth credentials (HTTP {resp.status_code}: {detail}). "
                    "Check client_id and client_secret of the OAuth Server-to-Server credential "
                    "in Adobe Developer Console. A rotated secret must be updated in the "
                    "connection, and a custom 'scopes' value must match the scopes listed on "
                    "the credential page."
                )
            raise AdobeAnalyticsAPIError(resp.status_code, "POST", TOKEN_URL, detail)
        body = resp.json()
        token = body.get("access_token")
        if not token:
            raise AdobeAnalyticsAPIError(
                resp.status_code, "POST", TOKEN_URL, "response contained no access_token"
            )
        self._token = token
        try:
            expires_in = float(body.get("expires_in", 86399))
        except (TypeError, ValueError):
            expires_in = 86399.0
        self._token_expires_at = time.time() + expires_in
        return token

    @property
    def global_company_id(self) -> str:
        if not self._global_company_id:
            self._global_company_id = self._discover_company()
        return self._global_company_id

    def _discover_company(self) -> str:
        body = self._request_json("GET", DISCOVERY_URL, company_scoped=False)
        companies = []
        for org in (body or {}).get("imsOrgs") or []:
            for company in org.get("companies") or []:
                if company.get("globalCompanyId"):
                    companies.append(company)
        if len(companies) == 1:
            return companies[0]["globalCompanyId"]
        if not companies:
            raise ValueError(
                "This credential has no access to any Adobe Analytics company. Add the "
                "credential's technical account to an Adobe Analytics product profile in "
                "Adobe Admin Console, or set the 'global_company_id' connection option."
            )
        choices = ", ".join(
            f"{c['globalCompanyId']} ({c.get('companyName') or 'unnamed'})" for c in companies
        )
        raise ValueError(
            "This credential can access more than one Adobe Analytics company, so the "
            "connector cannot pick one automatically. Set the 'global_company_id' "
            f"connection option to one of: {choices}."
        )

    # ----- public request helpers -------------------------------------

    def get(self, path: str, params: dict | None = None) -> Any:
        return self._request_json("GET", path, params=params)

    def post(self, path: str, body: dict) -> Any:
        return self._request_json("POST", path, json_body=body)

    def get_paged(self, path: str, params: dict | None = None) -> list[dict]:
        """Read every page of a list endpoint (``page`` is 0-based)."""
        items: list[dict] = []
        previous_keys = None
        for page in range(MAX_LIST_PAGES):
            body = self.get(path, {**(params or {}), "limit": LIST_PAGE_SIZE, "page": page})
            batch, last_page = extract_list_page(body)
            if not batch:
                break
            keys = [x.get("id") or x.get("rsid") for x in batch]
            # Guards against a server that ignores the page parameter.
            if keys == previous_keys:
                break
            items.extend(batch)
            if last_page is True:
                break
            if last_page is None and len(batch) < LIST_PAGE_SIZE:
                break
            previous_keys = keys
        return items

    # ----- internals ---------------------------------------------------

    def _request_json(
        self,
        method: str,
        path_or_url: str,
        params: dict | None = None,
        json_body: dict | None = None,
        company_scoped: bool = True,
    ) -> Any:
        if company_scoped:
            company = self.global_company_id
            url = f"{API_HOST}/api/{company}{path_or_url}"
        else:
            company = None
            url = path_or_url

        refreshed = False
        invalid_json = 0
        while True:
            headers = {
                "Authorization": f"Bearer {self._access_token()}",
                "x-api-key": self._client_id,
                "Accept": "application/json",
            }
            if company:
                headers["x-proxy-global-company-id"] = company
            if json_body is not None:
                headers["Content-Type"] = "application/json"
            resp = self._send(method, url, params=params, json=json_body, headers=headers)
            if resp.status_code == 401 and not refreshed:
                refreshed = True
                self._access_token(force_refresh=True)
                continue
            if 200 <= resp.status_code < 300 and resp.content:
                try:
                    return resp.json()
                except ValueError as exc:
                    # Seen live on ~5 MB report pages that arrive cut off, so retry a
                    # few times. The cause names the parse error, content type and
                    # sizes, never the body, which may hold report data.
                    invalid_json += 1
                    cause = (
                        f"HTTP {resp.status_code} response body is not valid JSON ({exc}; "
                        f"content type {resp.headers.get('Content-Type') or 'not set'}; "
                        f"{describe_body_size(resp)})"
                    )
                    if invalid_json >= MAX_INVALID_JSON_ATTEMPTS:
                        raise AdobeAnalyticsInvalidResponseError(
                            resp.status_code,
                            method,
                            path_or_url,
                            f"{cause}, on all {invalid_json} attempts",
                        ) from exc
                    self._sleep(
                        self._backoff(invalid_json), cause, invalid_json, MAX_INVALID_JSON_ATTEMPTS
                    )
                    continue
            break

        if 200 <= resp.status_code < 300:
            return None
        error_cls = (
            AdobeAnalyticsTimeoutError if resp.status_code == 504 else AdobeAnalyticsAPIError
        )
        raise error_cls(resp.status_code, method, path_or_url, describe_error_body(resp))

    def _send(self, method: str, url: str, **kwargs) -> requests.Response:
        """Send with throttling; retry 429/5xx/timeouts with backoff and jitter."""
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._throttle()
            try:
                resp = requests.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            except requests.Timeout as exc:
                if attempt == MAX_ATTEMPTS:
                    raise AdobeAnalyticsTimeoutError(
                        None, method, url, f"timed out after {MAX_ATTEMPTS} attempts"
                    ) from exc
                self._sleep(self._backoff(attempt), "request timed out", attempt)
                continue
            except requests.ConnectionError as exc:
                if attempt == MAX_ATTEMPTS:
                    raise AdobeAnalyticsAPIError(None, method, url, str(exc)) from exc
                self._sleep(self._backoff(attempt), "connection error", attempt)
                continue

            if resp.status_code not in RETRIABLE_STATUS_CODES or attempt == MAX_ATTEMPTS:
                return resp
            wait = self._backoff(attempt)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = max(float(retry_after), 1.0) + random.uniform(0, 1)
                except (TypeError, ValueError):
                    pass
            self._sleep(wait, f"HTTP {resp.status_code}", attempt)
        raise AssertionError("unreachable")

    @staticmethod
    def _backoff(attempt: int) -> float:
        return INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 1)

    @staticmethod
    def _sleep(seconds: float, reason: str, attempt: int, max_attempts: int = MAX_ATTEMPTS) -> None:
        seconds = min(seconds, MAX_WAIT_SECONDS)
        logger.warning(
            "Adobe Analytics: %s; retrying in %.1f s (attempt %d of %d)",
            reason,
            seconds,
            attempt + 1,
            max_attempts,
        )
        time.sleep(seconds)

    def _throttle(self) -> None:
        now = time.monotonic()
        while self._recent_requests and now - self._recent_requests[0] >= RATE_LIMIT_WINDOW_SECONDS:
            self._recent_requests.popleft()
        if len(self._recent_requests) >= RATE_LIMIT_REQUESTS:
            time.sleep(RATE_LIMIT_WINDOW_SECONDS - (now - self._recent_requests[0]))
            now = time.monotonic()
            while (
                self._recent_requests
                and now - self._recent_requests[0] >= RATE_LIMIT_WINDOW_SECONDS
            ):
                self._recent_requests.popleft()
        self._recent_requests.append(now)
