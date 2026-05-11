"""Utility helpers for the Klaviyo connector.

This module centralises:

* HTTP retry/backoff (429 with ``Retry-After``; 500/502/503/504).
* Per-table rate-limit pacing (Klaviyo's S-tier 3/s, 60/m for ``flows``).
* Link-cursor pagination (parse ``links.next`` → opaque ``page[cursor]``).
* JSON:API record flattening (hoist ``id``/``type`` + unwrap
  ``attributes`` → flat top-level fields).
* Argument parsing (``max_records_per_batch``, ``page_size``,
  ``lookback_seconds``).
"""

import logging
import random
import threading
import time
from collections import deque
from typing import Any, Iterator
from urllib.parse import parse_qs, urlparse

import requests


_LOG = logging.getLogger(__name__)

# Retriable HTTP status codes — 429 (rate-limit) plus the usual 5xx
# transient errors.  We do not retry 4xx other than 429 because they
# represent programmer errors (bad filter, missing scope, etc.).
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

MAX_RETRIES = 6
INITIAL_BACKOFF_SECONDS = 1.0  # doubled each retry; jittered ±20%.

# Per-request timeouts.  Tuple form: (connect, read).  Read timeout is
# larger because Klaviyo can be slow on large pages (events, templates).
REQUEST_TIMEOUT: tuple[float, float] = (5.0, 30.0)


# --------------------------------------------------------------------------- #
# Argument parsing helpers
# --------------------------------------------------------------------------- #


def parse_int_option(
    table_options: dict[str, str], key: str, default: int | None = None
) -> int | None:
    """Read a positive integer option from ``table_options``.

    Returns ``default`` when the key is missing, the value is empty, or
    the value fails to parse / is not strictly positive.
    """
    raw = table_options.get(key)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


# --------------------------------------------------------------------------- #
# Rate-limit pacing
# --------------------------------------------------------------------------- #


class RatePacer:
    """Thread-safe rate-limit pacer using a fixed-window sliding log.

    Configured with two rolling budgets (burst-per-second and
    steady-per-minute) and blocks the caller until the next request
    would fall within both budgets.  Used for the ``flows`` table which
    has a strict 3/s, 60/m limit on Klaviyo.

    Implementation note: a deque of request timestamps is a simple
    sliding-window algorithm.  Klaviyo's documented algorithm is a
    fixed-window one which is *more* permissive than ours — so a
    conservative sliding window is fine and keeps us under quota.
    """

    def __init__(self, burst_per_sec: float, steady_per_min: float) -> None:
        self._burst_limit = burst_per_sec
        self._steady_limit = steady_per_min
        # Timestamps of the last requests.  We keep at most the larger
        # of the two budgets and prune older entries on each acquire.
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until issuing one more request fits both budgets."""
        if self._burst_limit <= 0 and self._steady_limit <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                # Drop everything older than 60 s — anything older
                # never constrains either budget.
                while self._timestamps and now - self._timestamps[0] > 60.0:
                    self._timestamps.popleft()

                # Count requests in the burst (1 s) window.
                burst_count = sum(1 for t in self._timestamps if now - t < 1.0)
                steady_count = len(self._timestamps)

                wait_burst = 0.0
                if self._burst_limit > 0 and burst_count >= self._burst_limit:
                    # Wait until the oldest in-burst entry rolls off.
                    oldest_in_burst = next(t for t in self._timestamps if now - t < 1.0)
                    wait_burst = max(0.0, 1.0 - (now - oldest_in_burst))

                wait_steady = 0.0
                if self._steady_limit > 0 and steady_count >= self._steady_limit:
                    wait_steady = max(0.0, 60.0 - (now - self._timestamps[0]))

                wait = max(wait_burst, wait_steady)
                if wait <= 0.0:
                    self._timestamps.append(now)
                    return

            # Drop the lock before sleeping so siblings can also wait
            # in parallel without serialising on the lock.
            time.sleep(wait + 0.01)


def make_pacer(table_name: str) -> RatePacer | None:
    """Return a per-table pacer for tables that need explicit pacing.

    Only ``flows`` (S-tier: 3/s, 60/m) gets an explicit pacer.  Higher
    tiers (L, XL) are unlikely to be hit by a single-driver connector
    and the cost of acquiring the pacer would dwarf the benefit.

    Returning ``None`` means "no pacing — issue requests as fast as the
    server allows; rely on 429 retry/backoff if we ever do hit limits."
    """
    if table_name == "flows":
        return RatePacer(burst_per_sec=3, steady_per_min=60)
    return None


# --------------------------------------------------------------------------- #
# HTTP request with retry / backoff
# --------------------------------------------------------------------------- #


def _compute_retry_after(response: requests.Response, fallback: float) -> float:
    """Return the wait time (s) for a retriable response."""
    raw = response.headers.get("Retry-After")
    if raw is None:
        return fallback
    try:
        wait = float(raw)
    except (TypeError, ValueError):
        return fallback
    # Klaviyo's Retry-After is in seconds; clamp to a sane upper bound
    # to avoid pathological waits in case of misbehaving headers.
    return max(0.0, min(wait, 300.0))


def request_with_retry(
    session: requests.Session,
    url: str,
    params: dict[str, str] | None = None,
    pacer: RatePacer | None = None,
) -> requests.Response:
    """Issue a GET request with exponential backoff on retriable errors.

    * Honors ``Retry-After`` when the server provides it (Klaviyo
      returns this on 429 with a value in seconds).
    * Adds ±20% jitter to the backoff to avoid herd retries.
    * Caps retries at ``MAX_RETRIES``; the final response (success or
      failure) is returned to the caller.
    * Every request uses ``REQUEST_TIMEOUT`` to bound socket waits.
    """
    backoff = INITIAL_BACKOFF_SECONDS
    response: requests.Response | None = None
    for attempt in range(MAX_RETRIES):
        if pacer is not None:
            pacer.acquire()
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if response.status_code not in RETRIABLE_STATUS_CODES:
            return response
        if attempt >= MAX_RETRIES - 1:
            break
        wait = _compute_retry_after(response, fallback=backoff)
        # Jitter ±20%.
        wait *= 1.0 + random.uniform(-0.2, 0.2)
        _LOG.warning(
            "Klaviyo %s returned %s; retrying in %.2fs (attempt %d/%d)",
            url,
            response.status_code,
            wait,
            attempt + 1,
            MAX_RETRIES,
        )
        time.sleep(max(0.0, wait))
        backoff *= 2.0
    # If we got here we exhausted retries — let the caller see the
    # last response (and decide whether to raise).
    return response  # type: ignore[return-value]


def api_get(
    session: requests.Session,
    url: str,
    params: dict[str, str] | None,
    label: str,
    pacer: RatePacer | None = None,
) -> tuple[dict[str, Any], requests.Response]:
    """GET a Klaviyo endpoint and return ``(json_body, response)``.

    Raises ``RuntimeError`` with a descriptive label on non-200 status.
    The response object is returned alongside the body so callers can
    extract headers (e.g. rate-limit headers) when useful.
    """
    response = request_with_retry(session, url, params=params, pacer=pacer)
    if response is None or response.status_code != 200:
        status = response.status_code if response is not None else "no response"
        body = response.text if response is not None else ""
        raise RuntimeError(f"Klaviyo API error for {label}: {status} {body}")
    return response.json(), response


# --------------------------------------------------------------------------- #
# Link-cursor pagination
# --------------------------------------------------------------------------- #


def extract_next_cursor(payload: dict[str, Any]) -> str | None:
    """Extract the opaque ``page[cursor]`` value from a Klaviyo response.

    Klaviyo returns ``links.next`` as a full URL with a URL-encoded
    ``page[cursor]`` query parameter.  Returns ``None`` when no more
    pages remain.
    """
    links = payload.get("links")
    if not isinstance(links, dict):
        return None
    next_url = links.get("next")
    if not next_url:
        return None
    try:
        parsed = urlparse(next_url)
    except (TypeError, ValueError):
        return None
    qs = parse_qs(parsed.query)
    # The bracketed key is ``page[cursor]`` — parse_qs preserves it
    # verbatim.
    cursor_values = qs.get("page[cursor]")
    if not cursor_values:
        return None
    return cursor_values[0]


def paginate_get(
    session: requests.Session,
    url: str,
    initial_params: dict[str, str],
    label: str,
    pacer: RatePacer | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield ``data[*]`` records by following ``links.next`` cursors.

    The first request uses ``initial_params``.  Each follow-up request
    keeps the same params + adds ``page[cursor]`` from the previous
    response.  Stops when ``links.next`` is null.
    """
    params = dict(initial_params)
    while True:
        body, _ = api_get(session, url, params, label, pacer=pacer)
        data = body.get("data")
        if isinstance(data, list):
            yield from data
        elif isinstance(data, dict):
            # Single-resource response — not expected for collection
            # endpoints, but be defensive.
            yield data
        cursor = extract_next_cursor(body)
        if not cursor:
            return
        params["page[cursor]"] = cursor


# --------------------------------------------------------------------------- #
# JSON:API record flattening
# --------------------------------------------------------------------------- #


def flatten_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Flatten one JSON:API record to a flat dict.

    Klaviyo records look like::

        {
          "type": "profile",
          "id": "01H...",
          "attributes": {"email": "...", "updated": "..."},
          "relationships": {...},
          "links": {...}
        }

    This function hoists ``id`` and ``type`` to the top level and
    unwraps every key in ``attributes``.  Relationship FKs are NOT
    extracted by default — callers that need specific FKs (e.g. the
    ``events`` table's ``profile`` and ``metric`` relationships) should
    extract them via :func:`relationship_id` after calling this.

    ``relationships`` and ``links`` are dropped from the output to keep
    the row shape clean.  The original raw record is not mutated.

    Tolerant of records already in flat shape (no ``attributes`` key,
    e.g. when served by the source-simulator's corpus-from-schema
    bootstrapper) — those are returned as a shallow copy with only the
    ``relationships``/``links`` envelope keys stripped.
    """
    attrs = raw.get("attributes")
    if isinstance(attrs, dict):
        flat: dict[str, Any] = dict(attrs)
        # Wrapper fields override any same-named attribute (shouldn't
        # happen in practice, but be deterministic).
        flat["id"] = raw.get("id")
        flat["type"] = raw.get("type")
        return flat
    # Already-flat record (no ``attributes`` envelope).  Strip
    # ``relationships`` / ``links`` if present — anything else is a
    # legitimate top-level field.
    flat = {k: v for k, v in raw.items() if k not in ("relationships", "links")}
    return flat


def relationship_id(raw: dict[str, Any], rel_name: str) -> str | None:
    """Pull ``relationships[rel_name].data.id`` from a raw JSON:API record.

    Returns ``None`` when the relationship is absent, ``data`` is null
    (meaning "no related resource"), or any layer is missing.
    """
    rels = raw.get("relationships")
    if not isinstance(rels, dict):
        return None
    rel = rels.get(rel_name)
    if not isinstance(rel, dict):
        return None
    data = rel.get("data")
    if not isinstance(data, dict):
        return None
    rel_id = data.get("id")
    return str(rel_id) if rel_id is not None else None


# --------------------------------------------------------------------------- #
# Cursor / datetime helpers
# --------------------------------------------------------------------------- #


def normalise_iso(value: str | None) -> str | None:
    """Best-effort: return the input or None.

    Klaviyo emits ISO 8601 strings already; we keep them as-is rather
    than reparsing into datetimes (which would risk timezone drift).
    Callers compare strings lexically — safe for RFC 3339 dates with
    a fixed offset (UTC).
    """
    return value
