"""Utility functions for the Shopify connector.

This module contains helper functions for API interaction, retry logic,
and Link-header cursor pagination used across all tables.
"""

import re
import time
from typing import Any

import requests


RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry


def request_with_retry(
    session: requests.Session,
    url: str,
    params: dict[str, str] | None = None,
) -> requests.Response:
    """Issue a GET with exponential backoff on retriable errors.

    Honors the ``Retry-After`` header when present (Shopify returns this
    on 429 with a value in seconds), otherwise doubles the backoff
    (1, 2, 4, 8, 16 s).
    """
    backoff = INITIAL_BACKOFF
    resp = None
    for attempt in range(MAX_RETRIES):
        resp = session.get(url, params=params, timeout=30)
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
) -> tuple[dict[str, Any], requests.Response]:
    """GET a Shopify endpoint and return (json_body, response).

    Returns the response too so callers can extract the Link header for
    cursor pagination.

    Raises RuntimeError on non-200 status with a descriptive label.
    """
    response = request_with_retry(session, url, params)
    if response.status_code != 200:
        raise RuntimeError(
            f"Shopify API error for {label}: "
            f"{response.status_code} {response.text}"
        )
    return response.json(), response


# --------------------------------------------------------------------------- #
# Link-header cursor pagination
# --------------------------------------------------------------------------- #

# Shopify returns pagination cursors in the Link response header, e.g.:
#   Link: <https://shop.myshopify.com/admin/api/2026-04/products.json
#          ?page_info=hijgklmn&limit=50>; rel="next"
# We extract the next-page URL (or just the page_info value) for the next call.
_NEXT_LINK_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


def extract_next_link(response: requests.Response) -> str | None:
    """Return the next-page URL from a Shopify Link response header.

    Returns ``None`` when there is no next page (i.e. drained).
    """
    link = response.headers.get("Link")
    if not link:
        return None
    match = _NEXT_LINK_RE.search(link)
    return match.group(1) if match else None


def nullify_empty(record: dict, *keys: str) -> None:
    """Replace missing or empty-dict nested structs with ``None``."""
    for key in keys:
        if key not in record or record[key] == {}:
            record[key] = None
