"""Custom handler for the GCP Healthcare HL7v2 ``messages`` list endpoint.

Google Cloud Healthcare API filters list responses with an AIP-160 expression
passed in the single ``filter`` query parameter, e.g.::

    filter=createTime > "2024-01-15T00:00:00Z" AND createTime <= "2024-01-16T00:00:00Z"

This shape doesn't fit the simulator's role-based ``params:`` map (each role
takes a single literal value), so we use a custom handler.

The handler also honors ``orderBy`` (``sendTime asc`` / ``createTime desc``),
``pageSize`` (defaults to 100, capped at 1000 to match GCP), and pagination
via ``pageToken``. Future-record synthesis (cap-validation for
``test_read_terminates``) is applied via ``synthesize_future_records`` on
the spec — see ``synthesizer.py``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    request_record_from_prepared,
    response_from_record,
)


_FILTER_TOKEN = re.compile(
    r'(\w+)\s*(>=|<=|>|<|=|!=)\s*"([^"]*)"'
)


def _parse_filter(expr: str) -> List[Tuple[str, str, str]]:
    """Parse a Google AIP-160 filter expression into ``(field, op, value)`` tuples.

    Handles only the small subset the connector emits today —
    ``createTime > "X" AND createTime <= "Y"`` — which is enough for offline
    test coverage. Unknown operators / fields are returned verbatim and
    applied as exact-match string comparisons.
    """
    return _FILTER_TOKEN.findall(expr or "")


def _matches(record: Dict[str, Any], clauses: List[Tuple[str, str, str]]) -> bool:
    for field, op, value in clauses:
        actual = record.get(field)
        if actual is None:
            return False
        if op == ">":
            if not (actual > value):
                return False
        elif op == ">=":
            if not (actual >= value):
                return False
        elif op == "<":
            if not (actual < value):
                return False
        elif op == "<=":
            if not (actual <= value):
                return False
        elif op == "=":
            if actual != value:
                return False
        elif op == "!=":
            if actual == value:
                return False
    return True


def _sort_records(records: List[Dict[str, Any]], order_by: Optional[str]) -> List[Dict[str, Any]]:
    if not order_by:
        return records
    parts = order_by.strip().split()
    field = parts[0]
    direction = parts[1].lower() if len(parts) > 1 else "asc"
    reverse = direction == "desc"
    return sorted(records, key=lambda r: r.get(field) or "", reverse=reverse)


def _build_response_body(
    page: List[Dict[str, Any]], next_token: Optional[str]
) -> Dict[str, Any]:
    body: Dict[str, Any] = {"hl7V2Messages": page}
    if next_token:
        body["nextPageToken"] = next_token
    return body


def serve_messages_list(prep: PreparedRequest, spec, corpus) -> Response:
    """List handler for ``GET …/messages`` honoring AIP-160 ``filter``."""
    req = request_record_from_prepared(prep)
    # ``inject_future_records`` (called once at simulator boot) has already
    # mutated the corpus in place to add cap-validation records when the
    # spec's ``synthesize_future_records`` directive is present.
    raw: List[Dict[str, Any]] = corpus.get(spec.corpus) or []

    clauses = _parse_filter(req.query.get("filter", ""))
    filtered = [r for r in raw if _matches(r, clauses)]

    sorted_records = _sort_records(filtered, req.query.get("orderBy"))

    # Pagination: pageSize + pageToken (offset-based; tokens are str(int)).
    raw_page_size = req.query.get("pageSize", "100")
    try:
        page_size = max(1, min(int(raw_page_size), 1000))
    except ValueError:
        page_size = 100
    raw_token = req.query.get("pageToken")
    try:
        offset = int(raw_token) if raw_token else 0
    except ValueError:
        offset = 0

    page = sorted_records[offset:offset + page_size]
    next_offset = offset + len(page)
    next_token = str(next_offset) if next_offset < len(sorted_records) else None

    body = _build_response_body(page, next_token)
    body_bytes = json.dumps(body).encode("utf-8")
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=body_bytes.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
