"""HTTP, retry, and pagination helpers for the Microsoft Purview connector.

The Unified Catalog Data Governance data-plane API exposes two pagination
models across the endpoints this connector uses:

* **``$skipToken`` continuation** (``/datagovernance/catalog/businessdomains``):
  the response carries an opaque continuation via a ``nextLink`` URL; the
  connector follows ``nextLink`` until it is absent.
* **``skip`` / ``top`` + ``nextLink``** (``/datagovernance/catalog/dataProducts``,
  ``/datagovernance/catalog/terms``): page by following the absolute
  ``nextLink`` URL returned in each ``PagedX`` body until it is absent.

Both list shapes wrap records in a ``value`` array and (except on the last
page) a ``nextLink`` URL. A single ``next_link_paginate`` helper follows
``nextLink`` for all three endpoints, so the connector never has to construct
continuation URLs itself.

All requests carry an explicit ``timeout`` and retry on 429/5xx with
exponential backoff (honouring ``Retry-After`` when present).
"""

import time
from typing import Any, Iterator

import requests

from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_schemas import (  # pylint: disable=line-too-long
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
    params: dict[str, str] | None,
    label: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict:
    """GET and return parsed JSON, raising RuntimeError on non-200 responses."""
    response = request_with_retry(session, url, params, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(
            f"Microsoft Purview API error for {label}: "
            f"{response.status_code} {response.text}"
        )
    return response.json()


def next_link_paginate(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    label: str,
    records_key: str = "value",
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterator[dict[str, Any]]:
    """Yield records from a ``nextLink``-paginated Unified Catalog endpoint.

    The first request uses ``url`` + ``params``; every subsequent page follows
    the absolute ``nextLink`` URL from the previous ``PagedX`` body (which
    already embeds ``api-version``, ``$skipToken``/``skip``, and any filters),
    so ``params`` is only applied to the first call. Stops when ``nextLink`` is
    absent or a page is empty.

    Used for ``businessdomains`` (``$skipToken`` continuation), ``dataProducts``
    and ``terms`` (``skip``/``top``) — all three share the ``{value, nextLink}``
    envelope.
    """
    page_params: dict[str, str] | None = dict(params)
    next_url: str | None = url
    while next_url:
        body = api_get(session, next_url, page_params, label, timeout=timeout)
        batch = body.get(records_key) or []
        yield from batch

        next_url = body.get("nextLink")
        # nextLink is a fully-formed absolute URL; don't re-apply query params.
        page_params = None
        if not batch:
            break


def normalize_contacts(value: Any) -> dict[str, Any] | None:
    """Normalize a ``ContactsMap`` to the declared struct shape.

    Keeps only the declared roles (``owner`` / ``expert`` / ``databaseAdmin``)
    and coerces each contact to ``{id, description}`` so unexpected API keys
    don't break schema coercion. Returns ``None`` for an absent/empty map so
    the field maps cleanly onto the nullable struct.
    """
    if not isinstance(value, dict) or not value:
        return None
    out: dict[str, Any] = {}
    for role in ("owner", "expert", "databaseAdmin"):
        entries = value.get(role)
        if isinstance(entries, list):
            out[role] = [
                {"id": c.get("id"), "description": c.get("description")}
                for c in entries
                if isinstance(c, dict)
            ]
        else:
            out[role] = None
    return out
