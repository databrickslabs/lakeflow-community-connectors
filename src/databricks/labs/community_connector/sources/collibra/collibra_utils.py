"""HTTP, retry, and pagination helpers for the Collibra connector.

The Collibra Core REST API v2 exposes two pagination models:

* **Cursor-based** (``/assets``, ``/attributes``, ``/domains``): pass
  ``cursor=""`` for the first page and follow the ``nextCursor`` field in the
  response body. ``nextCursor`` is omitted on the last page.
* **Offset/limit** (``/responsibilities``): cursor pagination is *not*
  supported for this endpoint; page by advancing ``offset`` until a short
  page is returned.

All requests carry an explicit ``timeout`` and retry on 429/5xx with
exponential backoff (honouring ``Retry-After`` when present).
"""

import time
from typing import Any, Iterator

import requests

from databricks.labs.community_connector.sources.collibra.collibra_schemas import (
    INITIAL_BACKOFF,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
)

DEFAULT_TIMEOUT = 30  # seconds


def request_with_retry(
    session: requests.Session,
    url: str,
    params: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> requests.Response:
    """Issue a GET with exponential backoff on retriable status codes.

    Honors the ``Retry-After`` header when present, otherwise doubles the
    backoff (1, 2, 4, 8, 16 s). Always passes an explicit ``timeout``.
    """
    backoff = INITIAL_BACKOFF
    resp = None
    for attempt in range(MAX_RETRIES):
        resp = session.get(url, params=params, timeout=timeout)
        if resp.status_code not in RETRIABLE_STATUS_CODES:
            return resp

        if attempt < MAX_RETRIES - 1:
            retry_after = resp.headers.get("Retry-After")
            try:
                wait = float(retry_after) if retry_after else backoff
            except (TypeError, ValueError):
                wait = backoff
            time.sleep(wait)
            backoff *= 2

    return resp


def api_get(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    label: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict:
    """GET and return parsed JSON, raising RuntimeError on non-200 responses."""
    response = request_with_retry(session, url, params, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(
            f"Collibra API error for {label}: "
            f"{response.status_code} {response.text}"
        )
    return response.json()


def cursor_paginate(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    label: str,
    records_key: str = "results",
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterator[dict[str, Any]]:
    """Yield records from a cursor-paginated Collibra endpoint.

    Starts with ``cursor=""`` (empty string, first page) and follows the
    ``nextCursor`` field in each response body until it is absent. Used for
    ``/assets``, ``/attributes``, and ``/domains``.

    NOTE (needs-live-testing): The exact cursor mechanism for ``/assets``
    differs from ``/attributes`` per the API docs — ``/attributes`` returns
    ``nextCursor`` explicitly, while ``/assets`` may require ID-keyset
    pagination (sort by ``ID ASC``, filter ``id > last_seen_id``). This helper
    follows ``nextCursor`` uniformly; confirm on a live instance and switch
    ``/assets`` to keyset pagination if ``nextCursor`` is not returned.
    """
    page_params = dict(params)
    page_params.setdefault("cursor", "")
    while True:
        body = api_get(session, url, page_params, label, timeout=timeout)
        batch = body.get(records_key, []) or []
        yield from batch

        next_cursor = body.get("nextCursor")
        if not next_cursor or not batch:
            break
        page_params["cursor"] = next_cursor


def offset_paginate(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    label: str,
    *,
    page_size: int,
    records_key: str = "results",
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterator[dict[str, Any]]:
    """Yield records from an offset/limit-paginated Collibra endpoint.

    Advances ``offset`` by ``page_size`` until a short page (fewer than
    ``page_size`` records) signals the end. Used for ``/responsibilities``,
    which does not support cursor pagination.
    """
    offset = 0
    while True:
        page_params = dict(params)
        page_params["offset"] = str(offset)
        page_params["limit"] = str(page_size)

        body = api_get(session, url, page_params, label, timeout=timeout)
        batch = body.get(records_key, []) or []
        yield from batch

        if len(batch) < page_size:
            break
        offset += page_size


def resource_ref(value: Any) -> dict[str, Any] | None:
    """Normalize a nested resource reference to the declared struct shape.

    Returns ``None`` for absent/empty references (rather than an empty dict)
    so the field maps cleanly onto the nullable struct, and keeps only the
    declared keys so extra API fields don't break schema coercion.
    """
    if not isinstance(value, dict) or not value:
        return None
    return {
        "id": value.get("id"),
        "name": value.get("name"),
        "resourceType": value.get("resourceType"),
        "resourceDiscriminator": value.get("resourceDiscriminator"),
    }
