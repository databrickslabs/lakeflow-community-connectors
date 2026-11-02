"""Custom simulator handler for e-conomic OpenAPI cursor list endpoints.

The connector hits ``GET /{resource-api}/{version}/{Resource}`` with two
optional query params:

* ``filter`` — e-conomic's MongoDB-inspired filter syntax:
  ``property$operator:value`` clauses chained with ``$and:`` (and, less
  commonly, ``$or:``). The connector emits e.g.
  ``lastUpdated$gte:2024-01-01T00:00:00$and:number$gt:42``. Supported
  operators: ``eq ne gt gte lt lte like in nin null``. Datetime values are
  compared as naive UTC timestamps (``Z`` / offsets in corpus records are
  normalized), integers numerically, strings case-insensitively — matching
  the real API's case-insensitive string filtering. An unparsable clause
  returns HTTP 400 like the real API.
* ``cursor`` — opaque continuation token from the previous page.

Response envelope: ``{"items": [...], "cursor": "<next>"}`` with ``cursor``
omitted on the last page. The real API serves up to 1000 items per page;
the simulator uses a small page size so the connector's cursor-following
loop is exercised by small synthetic corpora. Records are served in corpus
order (the real API returns identifier order; the bootstrap corpus is
generated with ascending primary keys). The opaque cursor encodes the
offset of the next page's first item.

``synthesize_future_records`` clones are injected into the corpus by the
Simulator itself before this handler runs, so no augmentation happens here.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# Smaller than every demo table (5-10 live rows), so the connector follows
# at least one ``cursor`` within the test suite's 8-record sample.
_PAGE_SIZE = 5
_CURSOR_PREFIX = "sim-"

_CLAUSE_RE = re.compile(
    r"^(?P<field>[A-Za-z_][A-Za-z0-9_.]*)"
    r"\$(?P<op>gte|gt|lte|lt|eq|ne|like|nin|in|null):"
    r"(?P<value>.*)$",
    re.DOTALL,
)
_FRACTION_RE = re.compile(r"\.\d+")


class _BadFilter(ValueError):
    pass


def serve_cursor_list(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    parsed = urlsplit(prep.url or "")
    query = {k: v[-1] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}

    records: List[Dict[str, Any]] = list(corpus.get(spec.corpus) or [])

    filter_expr = query.get("filter")
    if filter_expr:
        try:
            records = [r for r in records if _eval_filter(r, filter_expr)]
        except _BadFilter as exc:
            return _json_response(
                prep,
                400,
                {
                    "title": "Bad request.",
                    "status": 400,
                    "detail": str(exc),
                    "errorCode": "InvalidFilter",
                },
            )

    offset = _decode_cursor(query.get("cursor"))
    page = records[offset : offset + _PAGE_SIZE]
    payload: Dict[str, Any] = {"items": page}
    next_offset = offset + len(page)
    if page and next_offset < len(records):
        payload["cursor"] = f"{_CURSOR_PREFIX}{next_offset}"
    return _json_response(prep, 200, payload)


# ---------------------------------------------------------------------------
# filter evaluation
# ---------------------------------------------------------------------------


def _eval_filter(record: Dict[str, Any], expr: str) -> bool:
    # ``$or:`` binds looser than ``$and:``. Parenthesised groups are not
    # emitted by the connector and are not modelled.
    or_groups = expr.split("$or:")
    return any(
        all(_eval_clause(record, clause) for clause in group.split("$and:"))
        for group in or_groups
    )


def _eval_clause(record: Dict[str, Any], clause: str) -> bool:
    m = _CLAUSE_RE.match(clause.strip())
    if not m:
        raise _BadFilter(f"Unsupported filter clause: {clause!r}")
    field, op, raw = m.group("field"), m.group("op"), m.group("value")
    actual = _get_field(record, field)

    if op == "null":
        want_null = raw.strip().lower() in ("", "true")
        return (actual is None) == want_null
    if op in ("in", "nin"):
        values = [v.strip() for v in raw.strip().strip("[]").split(",") if v.strip()]
        hit = any(_compare(actual, v) == 0 for v in values)
        return hit if op == "in" else not hit
    if op == "like":
        if actual is None:
            return False
        pattern = re.escape(_unescape(raw).lower()).replace(r"\*", ".*")
        if "*" not in raw:
            pattern = f".*{pattern}.*"
        return re.fullmatch(pattern, str(actual).lower()) is not None

    if actual is None:
        return op == "ne"
    cmp = _compare(actual, raw)
    return {
        "eq": cmp == 0,
        "ne": cmp != 0,
        "gt": cmp > 0,
        "gte": cmp >= 0,
        "lt": cmp < 0,
        "lte": cmp <= 0,
    }[op]


def _compare(actual: Any, raw: str) -> int:
    """Three-way compare a record value against a filter literal."""
    literal = _unescape(raw.strip())
    if isinstance(actual, bool):
        expected = literal.lower() == "true"
        return (actual > expected) - (actual < expected)
    if isinstance(actual, (int, float)):
        try:
            num = float(literal)
        except ValueError:
            raise _BadFilter(f"Expected a number, got {literal!r}") from None
        return (actual > num) - (actual < num)
    actual_dt = _parse_dt(actual)
    literal_dt = _parse_dt(literal)
    if actual_dt is not None and literal_dt is not None:
        return (actual_dt > literal_dt) - (actual_dt < literal_dt)
    a, b = str(actual).lower(), literal.lower()
    return (a > b) - (a < b)


def _parse_dt(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or len(value) < 10 or value[4:5] != "-":
        return None
    text = _FRACTION_RE.sub("", value.strip()).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _unescape(value: str) -> str:
    """Undo e-conomic's ``$``-prefix escaping of special characters."""
    return re.sub(r"\$([$()*,\[\]])", r"\1", value)


def _get_field(record: Dict[str, Any], path: str) -> Any:
    cur: Any = record
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _decode_cursor(cursor: Optional[str]) -> int:
    if not cursor:
        return 0
    try:
        return max(int(cursor[len(_CURSOR_PREFIX):]), 0)
    except ValueError:
        return 0


def _json_response(prep: PreparedRequest, status: int, payload: Dict[str, Any]) -> Response:
    body = json.dumps(payload, ensure_ascii=False)
    content_type = "application/json; charset=utf-8"
    if status >= 400:
        content_type = "application/problem+json; charset=utf-8"
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": content_type, "X-CallCost": "3"},
        body_text=body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
