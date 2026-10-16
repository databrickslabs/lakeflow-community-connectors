"""HTTP client for the Egnyte Public API: auth, throttling, error decoding.

Everything Egnyte-specific about *talking* to the API lives here so the
connector module stays about tables and offsets.

Rate limiting (``egnyte_api_doc.md`` → Rate Limiting + Known Quirks)
--------------------------------------------------------------------
Egnyte documents throttling **two different ways on two different pages**,
and a robust client has to honour both:

* legacy: ``403`` with ``X-Mashery-Error-Code:
  ERR_403_DEVELOPER_OVER_QPS`` (2 calls/sec/token) or
  ``ERR_403_DEVELOPER_OVER_RATE`` (1,000 calls/day/token), plus
  ``Retry-After``;
* modern: ``429`` with ``Retry-After``.

A bare ``403`` with no Mashery header is a *permissions* failure (the audit
API is admin-gated, for instance) and must NOT be retried — retrying would
burn the daily quota against a request that can never succeed.

We also read the ``X-Accesstoken-Qps-*`` headroom headers and pause
proactively, and space successive calls by ``min_request_interval`` so a
recursive tree walk or a wide partition fan-out doesn't trip the 2 QPS
ceiling on its own.

The OAuth token endpoint (``/puboauth/token``) is a separate 100/hour bucket
that answers ``409`` — not ``429`` — when throttled.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable, Mapping
from urllib.parse import quote

import requests

from databricks.labs.community_connector.sources.egnyte.egnyte_schemas import (
    DEFAULT_MIN_REQUEST_INTERVAL,
    DEFAULT_TIMEOUT,
    INITIAL_BACKOFF,
    MASHERY_ERROR_HEADER,
    MASHERY_THROTTLE_CODES,
    MAX_RETRIES,
    OAUTH_THROTTLE_STATUS,
    QPS_ALLOTTED_HEADER,
    QPS_COOLDOWN_SECONDS,
    QPS_CURRENT_HEADER,
    RETRIABLE_STATUS_CODES,
)

_LOG = logging.getLogger(__name__)


class EgnyteApiError(RuntimeError):
    """Non-retriable error response from the Egnyte API."""

    def __init__(self, status_code: int, message: str, url: str) -> None:
        super().__init__(f"Egnyte API error {status_code} for {url}: {message}")
        self.status_code = status_code
        self.url = url


# ---------------------------------------------------------------------------
# URL / path helpers
# ---------------------------------------------------------------------------


def resolve_base_url(domain: str) -> str:
    """Turn a tenant identifier into the API base URL.

    The doc is explicit that the domain is a required per-tenant config value
    with no discovery path. Mirrors the official SDK's ``Session.__init__``:
    a bare label gets ``.egnyte.com`` appended, anything already containing a
    dot is used verbatim (custom-branded hostnames), and a full URL is
    accepted as-is.
    """
    value = (domain or "").strip().rstrip("/")
    if not value:
        raise ValueError(
            "Egnyte connector requires the 'domain' option (tenant subdomain, "
            "e.g. 'acmecorp', or a full custom hostname)"
        )
    if value.startswith(("http://", "https://")):
        return value
    if "." in value:
        return f"https://{value}"
    return f"https://{value}.egnyte.com"


def encode_fs_path(path: str) -> str:
    """Percent-encode a filesystem path one segment at a time.

    Known quirk: each segment must be encoded individually and the ``/``
    separators must stay literal, so
    ``Shared/example?path/$file.txt`` becomes
    ``Shared/example%3Fpath/%24file.txt``.
    """
    segments = [seg for seg in (path or "").split("/") if seg]
    return "/".join(quote(seg, safe="") for seg in segments)


def normalize_fs_path(path: str) -> str:
    """Canonicalize a filesystem path to a leading-slash, no-trailing-slash form."""
    stripped = (path or "").strip().strip("/")
    return f"/{stripped}" if stripped else "/"


def extract_error_message(response: requests.Response) -> str:
    """Best-effort decode of Egnyte's two documented error envelopes.

    Shape A (best-practices page):  ``{"Errors": [{"description": ..., "code": ...}]}``
    Shape B (implied by the SDK):   ``{"errors": {"inputErrors": [{"message": ...}]}}``

    The doc flags the casing inconsistency explicitly, so both are checked
    and anything unrecognized falls back to the raw body.
    """
    try:
        body = response.json()
    except ValueError:
        return (response.text or "")[:500]

    messages: list[str] = []

    def _collect(node: Any) -> None:
        if isinstance(node, dict):
            for key in ("description", "message", "error", "reason"):
                value = node.get(key)
                if isinstance(value, str):
                    messages.append(value)
            for key in ("inputErrors", "errors", "Errors"):
                if key in node:
                    _collect(node[key])
        elif isinstance(node, list):
            for item in node:
                _collect(item)

    if isinstance(body, dict):
        for key in ("Errors", "errors"):
            if key in body:
                _collect(body[key])
        if not messages:
            _collect(body)

    if messages:
        return "; ".join(dict.fromkeys(messages))
    return (response.text or "")[:500]


def is_throttled(response: requests.Response) -> bool:
    """True when the response is a throttle signal in either documented style."""
    if response.status_code == 429:
        return True
    if response.status_code == 403:
        code = response.headers.get(MASHERY_ERROR_HEADER, "")
        return any(marker in code for marker in MASHERY_THROTTLE_CODES)
    return False


def retry_after_seconds(response: requests.Response, fallback: float) -> float:
    """Read ``Retry-After`` (seconds form), falling back to the caller's backoff."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return fallback
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return fallback


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class EgnyteClient:
    """Thin ``requests`` wrapper that owns auth, retries, and throttle handling.

    Constructed lazily by the connector (and re-constructed on each Spark
    executor) so no live socket ever has to survive pickling.
    """

    def __init__(
        self,
        base_url: str,
        access_token: str,
        *,
        timeout: int = DEFAULT_TIMEOUT,
        min_request_interval: float = DEFAULT_MIN_REQUEST_INTERVAL,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.min_request_interval = max(0.0, min_request_interval)
        self._last_request_at = 0.0
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
            }
        )

    # -- low level ---------------------------------------------------------

    def _throttle_gate(self) -> None:
        """Space consecutive calls so we stay under the 2 QPS token ceiling."""
        if self.min_request_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        if 0 <= elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)

    @staticmethod
    def _qps_cooldown(response: requests.Response) -> None:
        """Pause when the QPS headroom headers say we are at the allotment."""
        current = response.headers.get(QPS_CURRENT_HEADER)
        allotted = response.headers.get(QPS_ALLOTTED_HEADER)
        if not current or not allotted:
            return
        try:
            if float(current) >= float(allotted):
                time.sleep(QPS_COOLDOWN_SECONDS)
        except (TypeError, ValueError):
            return

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Any = None,
        allow_redirects: bool = True,
        expected: Iterable[int] = (200,),
    ) -> requests.Response:
        """Issue one request with throttle-aware retries.

        Returns the response whenever its status is in ``expected``; raises
        ``EgnyteApiError`` otherwise. Retriable statuses (429/5xx and
        throttle-flavoured 403s) are retried with ``Retry-After``-aware
        exponential backoff before the final failure is raised.
        """
        expected_codes = set(expected)
        backoff = INITIAL_BACKOFF
        response: requests.Response | None = None

        for attempt in range(MAX_RETRIES):
            self._throttle_gate()
            response = self._session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=self.timeout,
                allow_redirects=allow_redirects,
            )
            self._last_request_at = time.monotonic()

            if response.status_code in expected_codes:
                self._qps_cooldown(response)
                return response

            throttled = is_throttled(response)
            retriable = throttled or response.status_code in RETRIABLE_STATUS_CODES
            if not retriable or attempt == MAX_RETRIES - 1:
                break

            wait = retry_after_seconds(response, backoff)
            _LOG.warning(
                "Egnyte %s %s returned %s (%s) — retrying in %.1fs",
                method,
                url,
                response.status_code,
                "throttled" if throttled else "retriable",
                wait,
            )
            time.sleep(wait)
            backoff *= 2

        assert response is not None  # loop always assigns at least once
        raise EgnyteApiError(
            response.status_code, extract_error_message(response), url
        )

    # -- convenience -------------------------------------------------------

    def url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def get_json(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        expected: Iterable[int] = (200,),
    ) -> dict:
        response = self.request("GET", self.url(path), params=params, expected=expected)
        return _json_or_empty(response)

    def get_raw(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        expected: Iterable[int] = (200,),
        allow_redirects: bool = True,
    ) -> requests.Response:
        return self.request(
            "GET",
            self.url(path),
            params=params,
            expected=expected,
            allow_redirects=allow_redirects,
        )

    def post_json(
        self,
        path: str,
        *,
        json_body: Any = None,
        expected: Iterable[int] = (200, 201, 202),
    ) -> dict:
        response = self.request(
            "POST", self.url(path), json_body=json_body, expected=expected
        )
        return _json_or_empty(response)


def _json_or_empty(response: requests.Response) -> dict:
    """Parse a JSON body, tolerating the empty payloads Egnyte sends on 204."""
    if not (response.content or b"").strip():
        return {}
    try:
        body = response.json()
    except ValueError as exc:
        raise EgnyteApiError(
            response.status_code,
            f"response was not valid JSON: {exc}",
            response.url or "",
        ) from exc
    return body if isinstance(body, dict) else {"_list": body}


# ---------------------------------------------------------------------------
# OAuth
# ---------------------------------------------------------------------------


def fetch_access_token(
    base_url: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> str:
    """Exchange a stored refresh token for a fresh access token.

    This is the project-convention flow: the interactive authorization-code
    step happens once, out of band, and the connector only ever refreshes.
    Access tokens live 30 days, so one exchange per run is ample — which also
    keeps us well clear of the endpoint's 100-requests/hour cap.

    That cap answers ``409`` (not ``429``) when breached; both it and the
    ordinary retriable statuses are honoured here with ``Retry-After``.
    """
    url = f"{base_url.rstrip('/')}/puboauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    backoff = INITIAL_BACKOFF
    response: requests.Response | None = None

    with requests.Session() as session:
        for attempt in range(MAX_RETRIES):
            response = session.post(url, data=payload, timeout=timeout)
            if response.status_code == 200:
                break
            retriable = (
                response.status_code == OAUTH_THROTTLE_STATUS
                or is_throttled(response)
                or response.status_code in RETRIABLE_STATUS_CODES
            )
            if not retriable or attempt == MAX_RETRIES - 1:
                break
            wait = retry_after_seconds(response, backoff)
            _LOG.warning(
                "Egnyte token endpoint returned %s — retrying in %.1fs",
                response.status_code,
                wait,
            )
            time.sleep(wait)
            backoff *= 2

    assert response is not None
    if response.status_code != 200:
        raise EgnyteApiError(
            response.status_code, extract_error_message(response), url
        )

    body = _json_or_empty(response)
    token = body.get("access_token")
    if not token:
        raise EgnyteApiError(
            response.status_code,
            "token response did not contain an 'access_token'",
            url,
        )
    return str(token)
