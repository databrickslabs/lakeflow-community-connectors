"""HTTP + timestamp helpers for the WordPress connector.

Isolated from the connector class so both the driver-side offset/partition
logic and the executor-side ``read_partition`` path can construct their own
self-contained clients without sharing state.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import requests

# WordPress core caps ``per_page`` at 100 server-side.
MAX_PER_PAGE = 100
DEFAULT_PER_PAGE = 100

# Requests that should be retried with exponential backoff.
RETRIABLE_STATUS_CODES = {429, 500, 502, 503}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry

# Credential / capability errors — fail fast, do not retry.
FATAL_STATUS_CODES = {401, 403}

# WordPress error code returned (with HTTP 400) when a requested ``page`` is
# beyond ``X-WP-TotalPages``.  Treated as end-of-pagination, not a failure.
# WordPress emits per-resource variants (rest_post_invalid_page_number,
# rest_comment_invalid_page_number, ...), all ending in this suffix.
INVALID_PAGE_SUFFIX = "invalid_page_number"

TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


class WordPressError(RuntimeError):
    """Raised for non-retriable, non-terminal WordPress API errors."""


def build_session(username: str, application_password: str) -> requests.Session:
    """Create a ``requests.Session`` pre-configured for WordPress Basic Auth.

    WordPress strips spaces from Application Passwords internally, so the
    space-grouped secret is sent verbatim.
    """
    session = requests.Session()
    session.auth = (username, application_password)
    session.headers.update({"Accept": "application/json"})
    return session


def _is_invalid_page_error(response: requests.Response) -> bool:
    """Return True when the response is a ``*_invalid_page_number`` 400."""
    if response.status_code != 400:
        return False
    try:
        code = (response.json() or {}).get("code", "")
    except ValueError:
        return False
    return isinstance(code, str) and code.endswith(INVALID_PAGE_SUFFIX)


def _error_message(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        body = response.text
    return f"HTTP {response.status_code}: {body}"


def request_with_retry(
    session: requests.Session,
    url: str,
    params: dict[str, Any] | None,
    timeout: int = 30,
) -> requests.Response:
    """Issue a GET, retrying 429/5xx with exponential backoff.

    Honors the ``Retry-After`` header on 429 when present.  Fails fast on
    401/403 (credential / capability problems).  ``*_invalid_page_number``
    400 responses are returned as-is so the caller can treat them as
    end-of-pagination.
    """
    backoff = INITIAL_BACKOFF
    response: requests.Response | None = None
    for attempt in range(MAX_RETRIES):
        response = session.get(url, params=params, timeout=timeout)

        if response.status_code in FATAL_STATUS_CODES:
            raise WordPressError(
                "WordPress rejected the request (check the Application Password "
                f"and the user's capabilities). {_error_message(response)}"
            )

        if response.status_code not in RETRIABLE_STATUS_CODES:
            return response

        if attempt < MAX_RETRIES - 1:
            wait = backoff
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = max(wait, float(retry_after))
                except (TypeError, ValueError):
                    pass
            time.sleep(wait)
            backoff *= 2

    # Exhausted retries — surface the last response.
    raise WordPressError(
        f"WordPress API still failing after {MAX_RETRIES} attempts. "
        f"{_error_message(response) if response is not None else ''}"
    )


def paginate(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    *,
    per_page: int = DEFAULT_PER_PAGE,
    max_records: int | None = None,
    timeout: int = 30,
) -> Iterator[dict]:
    """Yield every record across pages for a WordPress list endpoint.

    Pagination stops when:
      * a page returns an ``*_invalid_page_number`` 400 (past the last page),
      * the returned array is empty or shorter than ``per_page``, or
      * ``max_records`` records have been yielded.

    Header-based termination (``X-WP-TotalPages``) is intentionally *not*
    relied upon: the count-and-short-page approach works identically against
    the live API and the offline simulator (which serves no WP headers).
    """
    page = 1
    yielded = 0
    query = dict(params)
    query["per_page"] = per_page

    while True:
        query["page"] = page
        response = request_with_retry(session, url, query, timeout=timeout)

        if _is_invalid_page_error(response):
            return
        if response.status_code != 200:
            raise WordPressError(_error_message(response))

        batch = response.json()
        if not isinstance(batch, list):
            raise WordPressError(f"Expected a JSON array from {url!r}, got {type(batch).__name__}")
        if not batch:
            return

        for record in batch:
            yield record
            yielded += 1
            if max_records is not None and yielded >= max_records:
                return

        if len(batch) < per_page:
            return
        page += 1


# --------------------------------------------------------------------------- #
# Timestamp helpers (UTC, second precision, lexicographically comparable)
# --------------------------------------------------------------------------- #


def now_utc_iso() -> str:
    """Current wall-clock time as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return datetime.now(timezone.utc).strftime(TS_FMT)


def parse_ts(value: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp to an aware datetime, or None on failure."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def normalize_ts(value: str | None) -> str | None:
    """Normalise a timestamp to ``TS_FMT`` so string compares are consistent."""
    dt = parse_ts(value)
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime(TS_FMT)


def add_seconds(value: str, seconds: int) -> str:
    """Return ``value`` shifted forward by ``seconds`` in ``TS_FMT``."""
    dt = parse_ts(value)
    if dt is None:
        raise ValueError(f"{value!r} is not a valid ISO 8601 timestamp")
    return (dt + timedelta(seconds=seconds)).astimezone(timezone.utc).strftime(TS_FMT)
