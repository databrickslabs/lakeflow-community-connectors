"""Custom simulator handler for OData v4 entity-set endpoints.

The connector hits ``GET {service_url}/{EntitySet}`` with OData query
options: ``$top``, ``$skip``, ``$filter``, ``$orderby``, ``$select``.
This handler implements just enough of the v4 query semantics to
exercise the connector's CDC, pagination, and discovery paths:

  * ``$filter`` — supports ``field gt VALUE``, ``field ge VALUE``,
    ``field lt VALUE``, ``field le VALUE``, ``field eq VALUE``, joined
    by ``and``. Parentheses around individual conjuncts (the connector
    emits ``(A) and (B)``) are tolerated. Date / timestamp literals
    pass through bare; string literals are single-quoted.
  * ``$orderby`` — comma-separated list of ``field [asc|desc]``
    terms; subsequent terms are tie-breakers.
  * ``$top`` — page-size cap on the response.
  * ``$skip`` — offset into the filtered+sorted list.
  * ``$select`` — projects the named columns (passed through unchanged
    in the response).
  * ``@odata.nextLink`` — set on the response whenever the current
    slice does not exhaust the filtered result set. The link encodes
    ``$skip=<next_offset>`` so the connector's ``urljoin``-based
    pagination follow walks the corpus cleanly.

A pagination boundary that landed inside a same-cursor cohort used to
silently drop the cohort tail (the connector's ``cursor gt <last>``
re-fetch filter excluded it). The current connector appends the
primary key to ``$orderby`` for stability — this handler honours the
extra term so that regression remains covered in tests.

Optional ``synthesize_future_records`` directive (declared in
``endpoints.yaml``) clones a tail record with a far-future cursor
value to exercise ``test_read_terminates``'s ``_init_time`` cap. The
connector's ``cursor le _init_ts`` filter must exclude those rows;
if it doesn't, the read loop never terminates and the test fails.
"""

from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


_FILTER_CLAUSE_RE = re.compile(
    r"\s*\(?\s*(?P<field>[A-Za-z_][A-Za-z0-9_]*)\s+"
    r"(?P<op>gt|ge|lt|le|eq|ne)\s+"
    r"(?P<value>'(?:''|[^'])*'|[^)\s]+)\s*\)?\s*"
)


def serve_entity_set(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    """Apply OData query options to the corpus and return a v4 envelope."""
    parsed = urlsplit(prep.url or "")
    query = {k: v[-1] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}

    corpus_name = spec.corpus
    records: List[Dict[str, Any]] = list(corpus.get(corpus_name) or [])

    sfr = getattr(spec, "synthesize_future_records", None)
    if sfr is not None and records:
        records = _augment_with_future(records, sfr.cursor_field, sfr.count)

    filter_expr = query.get("$filter")
    if filter_expr:
        records = [r for r in records if _matches_filter(r, filter_expr)]

    orderby = query.get("$orderby")
    if orderby:
        records = _apply_orderby(records, orderby)

    skip = _parse_int(query.get("$skip"), default=0, minimum=0)
    top = _parse_int(query.get("$top"), default=len(records), minimum=0)

    total = len(records)
    page = records[skip : skip + top]
    next_offset = skip + len(page)

    payload: Dict[str, Any] = {"value": page}
    if next_offset < total and len(page) > 0:
        next_link = _build_next_link(prep.url or "", next_offset, top)
        payload["@odata.nextLink"] = next_link

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json; charset=utf-8"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


# ---------------------------------------------------------------------------
# $filter
# ---------------------------------------------------------------------------


def _matches_filter(record: Dict[str, Any], expr: str) -> bool:
    """Evaluate a conjunction of simple ``field <op> value`` clauses.

    The connector only emits ``and``-joined clauses with the six
    standard comparison operators. Anything more elaborate raises so
    a test mis-specification doesn't silently pass.
    """
    clauses = _split_and(_strip_outer_parens(expr))
    for clause in clauses:
        m = _FILTER_CLAUSE_RE.fullmatch(_strip_outer_parens(clause))
        if not m:
            raise ValueError(f"Unsupported $filter clause: {clause!r}")
        field = m.group("field")
        op = m.group("op")
        literal = _decode_literal(m.group("value"))
        if not _compare(record.get(field), op, literal):
            return False
    return True


def _strip_outer_parens(expr: str) -> str:
    """Strip a single pair of outer parens if they enclose the whole expression."""
    s = expr.strip()
    if not (s.startswith("(") and s.endswith(")")):
        return s
    # Ensure the opening `(` matches the closing one — i.e. it's not
    # ``(A) and (B)`` where the outer parens are illusory.
    depth = 0
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and i < len(s) - 1:
                return s  # parens close before the end → not a single wrapper
    return s[1:-1].strip()


def _split_and(expr: str) -> List[str]:
    """Split on top-level ``and`` boundaries, preserving parenthesised clauses."""
    depth = 0
    out: List[str] = []
    start = 0
    i = 0
    expr_lower = expr.lower()
    while i < len(expr):
        ch = expr[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and expr_lower[i : i + 5] == " and " and i + 5 <= len(expr):
            out.append(expr[start:i])
            i += 5
            start = i
            continue
        i += 1
    out.append(expr[start:])
    return [c.strip() for c in out if c.strip()]


def _decode_literal(token: str) -> Any:
    """Decode an OData literal into a comparable Python value."""
    token = token.strip()
    if token.startswith("'") and token.endswith("'") and len(token) >= 2:
        return token[1:-1].replace("''", "'")
    if token.lower() in {"true", "false"}:
        return token.lower() == "true"
    if token.lower() == "null":
        return None
    if _looks_like_iso8601(token):
        return token  # keep as ISO string; record values are strings too
    try:
        if "." in token:
            return float(token)
        return int(token)
    except ValueError:
        return token


def _compare(actual: Any, op: str, expected: Any) -> bool:
    if actual is None:
        return False  # OData: comparisons with null are false-y for our ops
    try:
        if op == "gt":
            return actual > expected
        if op == "ge":
            return actual >= expected
        if op == "lt":
            return actual < expected
        if op == "le":
            return actual <= expected
        if op == "eq":
            return actual == expected
        if op == "ne":
            return actual != expected
    except TypeError:
        return False
    return False


# ---------------------------------------------------------------------------
# $orderby
# ---------------------------------------------------------------------------


def _apply_orderby(records: List[Dict[str, Any]], orderby: str) -> List[Dict[str, Any]]:
    terms = [t.strip() for t in orderby.split(",") if t.strip()]
    if not terms:
        return records

    def key(record: Dict[str, Any]) -> Tuple[Any, ...]:
        return tuple(_sort_key(record.get(_term_field(t))) for t in terms)

    sorted_records = sorted(records, key=key)

    # Apply descending order term-by-term — Python's `sorted` is stable
    # so reverse-sorting against the last desc term first preserves the
    # multi-key intent.
    for term in reversed(terms):
        field = _term_field(term)
        if _term_direction(term) == "desc":
            sorted_records.sort(key=lambda r: _sort_key(r.get(field)), reverse=True)
        else:
            sorted_records.sort(key=lambda r: _sort_key(r.get(field)))
    return sorted_records


def _term_field(term: str) -> str:
    return term.split()[0]


def _term_direction(term: str) -> str:
    parts = term.split()
    if len(parts) >= 2 and parts[1].lower() == "desc":
        return "desc"
    return "asc"


def _sort_key(value: Any) -> Tuple[int, Any]:
    """Make ``None`` sort before everything; otherwise compare normally."""
    if value is None:
        return (0, "")
    return (1, value)


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def _build_next_link(current_url: str, next_offset: int, top: int) -> str:
    """Construct the ``@odata.nextLink`` URL preserving filter/orderby."""
    parts = urlsplit(current_url)
    pairs = []
    for chunk in parts.query.split("&"):
        if not chunk:
            continue
        if chunk.startswith("$skip="):
            continue
        pairs.append(chunk)
    pairs.append(f"$skip={next_offset}")
    if not any(p.startswith("$top=") for p in pairs):
        pairs.append(f"$top={top}")
    return f"{parts.scheme}://{parts.netloc}{parts.path}?{'&'.join(pairs)}"


def _parse_int(raw: Optional[str], *, default: int, minimum: int) -> int:
    if raw is None:
        return default
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return default
    return max(minimum, v)


# ---------------------------------------------------------------------------
# Future-record synthesis (cursor cap testing)
# ---------------------------------------------------------------------------


def _augment_with_future(
    records: List[Dict[str, Any]], cursor_field: str, count: int
) -> List[Dict[str, Any]]:
    if not records or count <= 0:
        return records
    template = records[-1]
    base = datetime.now(timezone.utc) + timedelta(days=365)
    out = list(records)
    for i in range(count):
        clone = copy.deepcopy(template)
        ts = (base + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        clone[cursor_field] = ts
        # Make the synthetic record's identity unique on every PK-shaped
        # field so MERGE-on-PK at the destination keeps it distinct.
        for k, v in list(clone.items()):
            if k == cursor_field:
                continue
            if isinstance(v, int):
                clone[k] = v + 1_000_000 + i
            elif isinstance(v, str) and k.endswith("ID"):
                clone[k] = f"{v}-future-{i}"
        out.append(clone)
    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _looks_like_iso8601(s: str) -> bool:
    if len(s) < 10 or s[4] != "-" or s[7] != "-":
        return False
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False
