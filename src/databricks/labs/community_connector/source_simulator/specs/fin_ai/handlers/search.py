"""Custom simulator handlers for Fin.ai (Intercom).

Two endpoint shapes can't be expressed with the declarative param-role
pipeline (which only reads URL query params), so they use handlers:

* ``search`` — the ``POST /{resource}/search`` endpoints (conversations,
  contacts, tickets). Intercom's Search API takes its filter, sort and
  pagination inside a JSON *body*, not query params. The handler:
    1. parses the body,
    2. picks the corpus + response ``records_key`` from the path,
    3. filters records by the ``updated_at`` range in ``query.value``,
    4. synthesises a few future-dated ``updated_at`` clones on top of the
       corpus so the connector's init-time cap is exercised (the connector's
       ``updated_at <= until`` filter must exclude them),
    5. sorts ascending by ``updated_at`` and paginates via an opaque
       ``starting_after`` cursor,
    6. wraps the page as ``{"type": ..., "<records_key>": [...],
       "total_count": N, "pages": {..., "next": {"starting_after": ...}}}``.

* ``companies_scroll`` — ``GET /companies/scroll``. The Scroll API paginates
  with an opaque ``scroll_param`` query param and signals end-of-scroll by
  returning an empty ``data`` array. The handler implements that with an
  offset-encoded ``scroll_param`` so the connector's drain loop terminates.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# path suffix -> (corpus name, response records_key)
_SEARCH_ROUTES: dict[str, tuple[str, str]] = {
    "/conversations/search": ("conversations", "conversations"),
    "/contacts/search": ("contacts", "data"),
    "/tickets/search": ("tickets", "tickets"),
}

_CURSOR_FIELD = "updated_at"
_DEFAULT_PER_PAGE = 150
_FUTURE_RECORDS = 3
_SECONDS_PER_YEAR = 365 * 86_400
_SCROLL_PREFIX = "scroll-"
_SCROLL_PAGE = 1000


# ---------------------------------------------------------------------------
# POST /{resource}/search
# ---------------------------------------------------------------------------


def search(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    path = urlsplit(prep.url or "").path
    route = _resolve_route(path)
    if route is None:
        return _json_response(prep, 404, {"errors": [{"message": f"no route for {path}"}]})
    corpus_name, records_key = route

    body = _parse_json(prep.body)
    per_page = _coerce_int(_dig(body, "pagination", "per_page"), _DEFAULT_PER_PAGE)
    starting_after = _dig(body, "pagination", "starting_after")

    records = corpus.get(corpus_name) or []
    if not isinstance(records, list):
        records = []

    # Append future-dated clones so cap-validation tests can detect a connector
    # that fails to bound ``updated_at`` at its init-time snapshot.
    records = _augment_with_future(records)

    lo, lo_inclusive, hi, hi_inclusive = _extract_updated_at_bounds(body.get("query"))
    filtered = [
        r
        for r in records
        if _in_range(r.get(_CURSOR_FIELD), lo, lo_inclusive, hi, hi_inclusive)
    ]
    filtered.sort(key=lambda r: _sort_key(r.get(_CURSOR_FIELD)))

    offset = _decode_cursor(starting_after)
    page = filtered[offset : offset + per_page]
    new_offset = offset + len(page)
    has_more = new_offset < len(filtered) and bool(page)

    pages: dict[str, Any] = {
        "type": "pages",
        "page": (offset // per_page) + 1 if per_page else 1,
        "per_page": per_page,
        "total_pages": max(1, (len(filtered) + per_page - 1) // per_page) if per_page else 1,
    }
    if has_more:
        pages["next"] = {"per_page": per_page, "starting_after": _encode_cursor(new_offset)}
    else:
        pages["next"] = None

    payload = {
        "type": f"{corpus_name}.list",
        records_key: page,
        "total_count": len(filtered),
        "pages": pages,
    }
    return _json_response(prep, 200, payload)


# ---------------------------------------------------------------------------
# GET /companies/scroll
# ---------------------------------------------------------------------------


def companies_scroll(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    query = dict(parse_qsl(urlsplit(prep.url or "").query, keep_blank_values=True))
    offset = _decode_scroll(query.get("scroll_param"))

    records = corpus.get("companies") or []
    if not isinstance(records, list):
        records = []

    page = records[offset : offset + _SCROLL_PAGE]
    new_offset = offset + len(page)
    # End-of-scroll: empty ``data`` and no scroll_param (matches Intercom).
    scroll_param = _encode_scroll(new_offset) if page else None

    payload = {
        "type": "list",
        "data": page,
        "pages": None,
        "total_count": len(records),
        "scroll_param": scroll_param,
    }
    return _json_response(prep, 200, payload)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _resolve_route(path: str) -> tuple[str, str] | None:
    for suffix, route in _SEARCH_ROUTES.items():
        if path.endswith(suffix):
            return route
    return None


def _parse_json(body: Any) -> dict:
    if body is None:
        return {}
    if isinstance(body, (bytes, bytearray)):
        try:
            body = body.decode("utf-8")
        except UnicodeDecodeError:
            return {}
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dig(data: Any, *keys: str) -> Any:
    cur = data
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _extract_updated_at_bounds(
    query: Any,
) -> tuple[int | None, bool, int | None, bool]:
    """Return ``(lo, lo_inclusive, hi, hi_inclusive)`` from a search query.

    Handles both the wrapped ``{"operator": "AND", "value": [...]}`` form and a
    single-clause ``{"field": ..., "operator": ..., "value": ...}`` form.
    """
    lo: int | None = None
    lo_incl = False
    hi: int | None = None
    hi_incl = False

    for clause in _iter_clauses(query):
        if clause.get("field") != _CURSOR_FIELD:
            continue
        op = clause.get("operator")
        val = _coerce_int(clause.get("value"), None)
        if val is None:
            continue
        if op == ">":
            lo, lo_incl = val, False
        elif op == ">=":
            lo, lo_incl = val, True
        elif op == "<":
            hi, hi_incl = val, False
        elif op == "<=":
            hi, hi_incl = val, True
    return lo, lo_incl, hi, hi_incl


def _iter_clauses(query: Any):
    if not isinstance(query, dict):
        return
    value = query.get("value")
    if isinstance(value, list):
        for clause in value:
            if isinstance(clause, dict):
                # One level of nesting (AND/OR groups).
                if isinstance(clause.get("value"), list):
                    for inner in clause["value"]:
                        if isinstance(inner, dict):
                            yield inner
                else:
                    yield clause
    elif "field" in query:
        yield query


def _in_range(
    value: Any,
    lo: int | None,
    lo_incl: bool,
    hi: int | None,
    hi_incl: bool,
) -> bool:
    val = _coerce_int(value, None)
    if val is None:
        return lo is None and hi is None
    if lo is not None:
        if lo_incl and val < lo:
            return False
        if not lo_incl and val <= lo:
            return False
    if hi is not None:
        if hi_incl and val > hi:
            return False
        if not hi_incl and val >= hi:
            return False
    return True


def _augment_with_future(records: list[dict]) -> list[dict]:
    """Append future-dated clones (int ``updated_at`` past now) to the corpus."""
    if not records:
        return records
    template = records[-1]
    base = int(datetime.now(timezone.utc).timestamp()) + _SECONDS_PER_YEAR
    clones: list[dict] = []
    for i in range(_FUTURE_RECORDS):
        clone = copy.deepcopy(template)
        clone["id"] = f"{clone.get('id', 'future')}-future-{i}"
        clone[_CURSOR_FIELD] = base + i * 3600
        clones.append(clone)
    return list(records) + clones


def _sort_key(value: Any):
    val = _coerce_int(value, None)
    return (val is None, val if val is not None else 0)


def _coerce_int(value: Any, default: int | None) -> int | None:
    if value is None:
        return default
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _decode_cursor(cursor: Any) -> int:
    if not cursor:
        return 0
    text = str(cursor)
    if text.startswith("after-"):
        text = text[len("after-") :]
    try:
        return max(0, int(text))
    except (TypeError, ValueError):
        return 0


def _encode_cursor(offset: int) -> str:
    return f"after-{offset}"


def _decode_scroll(scroll_param: Any) -> int:
    if not scroll_param:
        return 0
    text = str(scroll_param)
    if text.startswith(_SCROLL_PREFIX):
        text = text[len(_SCROLL_PREFIX) :]
    try:
        return max(0, int(text))
    except (TypeError, ValueError):
        return 0


def _encode_scroll(offset: int) -> str:
    return f"{_SCROLL_PREFIX}{offset}"


def _json_response(prep: PreparedRequest, status: int, payload: dict) -> Response:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
