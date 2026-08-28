"""Trim recorded responses to a small sample + strip pagination hints.

Turns a "full page of results + Link: next" response into a "N sample records
+ no next link" response, so replayed connectors stop after one call and
cassettes stay small.
"""

from __future__ import annotations

import json
import re
from typing import Any, List, Optional, Tuple

# Default body fields that connectors read for "is there more?"
DEFAULT_PAGINATION_BODY_KEYS: Tuple[str, ...] = (
    "next",
    "next_page",
    "next_url",
    "nextLink",
    "@odata.nextLink",
    "nextPageToken",
    "next_page_token",
    "paging",
    "cursor",
)

# Common record-array field names when the payload wraps records in an object.
RECORDS_KEY_HINTS: Tuple[str, ...] = (
    "records",
    "items",
    "data",
    "results",
    "entries",
    "values",
    "elements",
)


def sample_body(
    body_text: Optional[str],
    *,
    max_records: int,
    records_key_hints: Tuple[str, ...] = RECORDS_KEY_HINTS,
    pagination_body_keys: Tuple[str, ...] = DEFAULT_PAGINATION_BODY_KEYS,
) -> Optional[str]:
    """Return a trimmed + pagination-stripped version of ``body_text``.

    Non-JSON bodies are returned unchanged. A body that doesn't look like a
    record response (no list of dicts detected) is also returned unchanged.
    """
    if body_text is None or not body_text:
        return body_text
    try:
        payload = json.loads(body_text)
    except (ValueError, TypeError):
        return body_text

    trimmed = _trim_records(payload, max_records=max_records, records_key_hints=records_key_hints)
    if isinstance(trimmed, dict):
        _null_pagination_keys(trimmed, pagination_body_keys)

    return json.dumps(trimmed, separators=(",", ":"), ensure_ascii=False)


def strip_link_header_next(headers: dict) -> dict:
    """Return a copy of ``headers`` with any ``rel="next"`` entry removed
    from the Link header. Connectors that follow Link-header pagination
    will see "no more pages" after this.
    """
    out = dict(headers or {})
    for key in list(out.keys()):
        if key.lower() == "link":
            stripped = _strip_next_rel(out[key])
            if stripped:
                out[key] = stripped
            else:
                del out[key]
    return out


# ----- internals ---------------------------------------------------------


def _trim_records(
    payload: Any, *, max_records: int, records_key_hints: Tuple[str, ...]
) -> Any:
    # Top-level array of dicts → truncate it.
    if isinstance(payload, list):
        if _looks_like_records_list(payload):
            return payload[:max_records]
        return payload

    if not isinstance(payload, dict):
        return payload

    # Hint-first: look for known record-array field names.
    for hint in records_key_hints:
        if hint in payload and _looks_like_records_list(payload[hint]):
            payload[hint] = payload[hint][:max_records]
            return payload

    # Fall back: truncate the single (or largest) list-of-dicts field.
    candidates = [
        (k, v) for k, v in payload.items() if _looks_like_records_list(v)
    ]
    if candidates:
        candidates.sort(key=lambda kv: len(kv[1]), reverse=True)
        k, v = candidates[0]
        payload[k] = v[:max_records]

    return payload


def _looks_like_records_list(value: Any) -> bool:
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(x, dict) for x in value)
    )


def _null_pagination_keys(obj: dict, keys: Tuple[str, ...]) -> None:
    for k in list(obj.keys()):
        if k in keys:
            obj[k] = None


_NEXT_REL_RE = re.compile(r"""<[^>]+>\s*;\s*rel\s*=\s*["']?next["']?""", re.IGNORECASE)


def _strip_next_rel(link_header: str) -> str:
    """Remove any ``<url>; rel="next"`` entry from a comma-separated Link header."""
    parts = [p.strip() for p in link_header.split(",")]
    kept = [p for p in parts if not _NEXT_REL_RE.search(p)]
    return ", ".join(kept)
