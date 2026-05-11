"""Utility helpers for the Energy Quantified connector.

Encapsulates:

- HTTP request retry with exponential backoff + jitter for 429 / 5xx.
- Proactive client-side rate limiting (~15 req/s) matching the
  official SDK's ``RateLimiter`` to stay inside EQ's fair-use quota.
- URL-encoding for curve names — EQ accepts ``+`` in curve names
  (e.g. ``M+1``) and these MUST be encoded as ``%2B`` because ``+``
  decodes to a space in URL paths.  ``urllib.parse.quote`` with
  ``safe=''`` is the only encoding that does this correctly.
- Small parsers for the ``max_records_per_batch`` / ``lookback_days``
  / date helpers shared across the readers.
"""

import logging
import random
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import requests

from databricks.labs.community_connector.sources.energy_quantified.energy_quantified_schemas import (
    INITIAL_BACKOFF,
    MAX_RETRIES,
    MIN_INTERVAL_BETWEEN_REQUESTS,
    REQUEST_CONNECT_TIMEOUT,
    REQUEST_READ_TIMEOUT,
    RETRIABLE_STATUS_CODES,
)


_LOG = logging.getLogger(__name__)

_TIMEOUT = (REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT)


# --------------------------------------------------------------------------- #
# URL encoding
# --------------------------------------------------------------------------- #


def encode_curve_name(name: str) -> str:
    """URL-encode a curve name for use as a path segment.

    Energy Quantified accepts ``+`` in curve names (for example
    ``DE Power Base M+1 EUR/MWh EEX OHLC``).  In URL path segments
    ``+`` is *literal*, but most encoders (including ``requests``'
    default body-style encoder and ``urllib.parse.quote_plus``)
    convert ``+`` to ``%20`` or leave it bare and rely on the server
    to decode it as a space.  Either way the EQ server returns 404
    because the lookup is exact-match.

    The fix is ``quote(name, safe='')`` which percent-encodes
    every reserved character — including ``+`` → ``%2B``, ``/`` →
    ``%2F``, and spaces → ``%20``.  This is what the official SDK
    does in ``base.py``.
    """
    if name is None:
        raise ValueError("curve_name is required")
    return quote(name, safe="")


# --------------------------------------------------------------------------- #
# Rate limiter
# --------------------------------------------------------------------------- #


class RateLimiter:
    """Proactive minimum-interval rate limiter.

    Sleeps before each request so the average issue rate stays at or
    below ``1 / min_interval`` calls per second.  Matches the
    behaviour of the official EQ SDK's ``RateLimiter`` (constant
    inter-request delay, no token-bucket bursting).

    Thread-safety is not required: each Spark task constructs its own
    connector instance and therefore its own limiter, and within a
    single task requests are serial.
    """

    def __init__(self, min_interval: float = MIN_INTERVAL_BETWEEN_REQUESTS) -> None:
        self._min_interval = min_interval
        self._last_request_time: float = 0.0

    def wait(self) -> None:
        """Block until ``min_interval`` has elapsed since the last call."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.monotonic()


# --------------------------------------------------------------------------- #
# HTTP wrappers
# --------------------------------------------------------------------------- #


def _sleep_with_retry_after(resp: requests.Response | None, attempt: int, backoff: float) -> float:
    """Sleep before retrying, honouring ``Retry-After`` when present.

    Falls back to exponential backoff with jitter.  Returns the
    doubled backoff value so the caller can keep doubling.
    """
    retry_after = resp.headers.get("Retry-After") if resp is not None else None
    try:
        wait = float(retry_after) if retry_after else backoff
    except (TypeError, ValueError):
        wait = backoff
    # Jitter to avoid the thundering-herd problem.
    wait += random.uniform(0, 1)
    # Hard cap so a misbehaving Retry-After header can't stall the run.
    wait = min(wait, 64.0)
    _LOG.debug(
        "Energy Quantified retry attempt %d: sleeping %.2fs before retry",
        attempt + 1,
        wait,
    )
    time.sleep(wait)
    return backoff * 2


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    rate_limiter: RateLimiter,
    params: dict[str, Any] | None = None,
) -> requests.Response:
    """Issue an HTTP request with retries on 429 / 5xx.

    Args:
        session: A ``requests.Session`` configured with the
            ``X-API-Key`` header (and any defaults).
        method: ``"GET"`` only — EQ's public API is read-only.
        url: Fully qualified URL.
        rate_limiter: Shared rate limiter that paces every call.
        params: Query string parameters.
    """
    if method != "GET":
        raise ValueError(f"Unsupported HTTP method: {method}")

    backoff = INITIAL_BACKOFF
    resp: requests.Response | None = None
    for attempt in range(MAX_RETRIES):
        rate_limiter.wait()
        resp = session.get(url, params=params, timeout=_TIMEOUT)
        if resp.status_code not in RETRIABLE_STATUS_CODES:
            return resp
        if attempt < MAX_RETRIES - 1:
            backoff = _sleep_with_retry_after(resp, attempt, backoff)

    # Exhausted retries — return the last response so the caller can
    # surface the failure detail in its RuntimeError.
    return resp  # type: ignore[return-value]


def api_get(
    session: requests.Session,
    url: str,
    label: str,
    *,
    rate_limiter: RateLimiter,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch JSON from ``url`` and return the decoded body.

    Raises ``RuntimeError`` on any non-2xx status with the response
    body included for debugging.  EQ returns errors as JSON objects
    ``{"http_status": 400, "message": "..."}`` — the raw text is the
    most useful form for diagnostics.
    """
    resp = request_with_retry(session, "GET", url, rate_limiter=rate_limiter, params=params)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Energy Quantified API error for {label}: {resp.status_code} {resp.text[:500]}"
        )
    try:
        return resp.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Energy Quantified API returned non-JSON for {label}: {resp.text[:200]!r}"
        ) from exc


# --------------------------------------------------------------------------- #
# Table option parsing
# --------------------------------------------------------------------------- #


def parse_max_records(table_options: dict[str, str]) -> int | None:
    """Parse the optional ``max_records_per_batch`` cap.

    Returns ``None`` when unset or non-numeric (uncapped).
    """
    raw = table_options.get("max_records_per_batch")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def parse_int_option(table_options: dict[str, str], key: str, default: int) -> int:
    """Parse an integer table option with a fallback default."""
    raw = table_options.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def require_option(table_options: dict[str, str], key: str, table_name: str) -> str:
    """Return a required table option, raising ``ValueError`` if missing."""
    value = table_options.get(key)
    if not value:
        raise ValueError(f"Table '{table_name}' requires table_options['{key}']")
    return value


# --------------------------------------------------------------------------- #
# Date helpers
# --------------------------------------------------------------------------- #


def parse_iso_date_or_datetime(value: str | None) -> datetime | None:
    """Best-effort ISO 8601 parser tolerating ``Z`` suffix and date-only.

    Returns ``None`` for missing / unparseable input rather than
    raising — callers decide whether the fallback is acceptable.
    """
    if not value:
        return None
    text = value.strip()
    # Date-only form: pad to midnight UTC.
    if len(text) == 10:
        try:
            d = date.fromisoformat(text)
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        except ValueError:
            return None
    # Tolerate trailing 'Z' (Python <3.11 cannot).
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def to_iso_date(value: datetime) -> str:
    """Return ``YYYY-MM-DD`` for a datetime (UTC interpretation)."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).date().isoformat()


def to_iso_datetime(value: datetime) -> str:
    """Return a normalised ISO 8601 datetime in UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def days_ago_iso_date(days: int, now: datetime | None = None) -> str:
    """Return ``YYYY-MM-DD`` for ``now() - days``."""
    base = now or datetime.now(timezone.utc)
    return to_iso_date(base - timedelta(days=days))
