"""Custom simulator handler for Linear's ``POST /graphql`` endpoint.

Linear is a single GraphQL endpoint whose JSON request body carries the
table selector (which root connection it queries), the ``updatedAt`` range
filter (``gte``/``lte``), and the Relay ``first``/``after`` pagination
cursor. None of that lives in the URL query string, so the simulator's
declarative param-role pipeline can't match it — this is the documented
custom-handler escape hatch.

The handler:
  1. Parses the GraphQL request body (``query`` + ``variables``).
  2. Routes to the ``issues`` or ``projects`` corpus by inspecting which
     root connection the query selects.
  3. Augments the corpus with a few future-dated clones so the cap-
     validation termination tests can detect an uncapped connector.
  4. Applies the ``updatedAt`` ``gte``/``lte`` window from ``variables``.
  5. Sorts ascending by ``updatedAt`` (Linear's ``orderBy: updatedAt``).
  6. Slices a Relay page using an integer offset encoded as the opaque
     cursor and renders the ``{data: {<conn>: {nodes, pageInfo}}}``
     GraphQL envelope.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# Root connection field names this handler can serve. ``projects`` is
# checked before ``issues`` since neither query contains the other's
# connection token (``issues(`` / ``projects(``).
_CONNECTIONS = ("projects", "issues")

_CURSOR_FIELD = "updatedAt"
_DEFAULT_FIRST = 50
_FUTURE_RECORDS = 3


def graphql_handler(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    """Serve a Linear GraphQL list query from the corpus."""
    query, variables = _parse_body(prep.body)
    connection = _detect_connection(query)

    records = corpus.get(connection) or []
    if not isinstance(records, list):
        records = []

    # Append future-dated clones so the cap-validation termination tests
    # have something to detect: a connector that correctly caps ``until``
    # at its init time excludes them; an uncapped one leaks them and the
    # simulate-mode termination test diverges.
    records = _augment_with_future(records)

    since = variables.get("since")
    until = variables.get("until")
    filtered = [
        r for r in records if _within_window(r.get(_CURSOR_FIELD), since, until)
    ]

    # Linear orders ascending by updatedAt for incremental reads.
    ordered = sorted(filtered, key=lambda r: r.get(_CURSOR_FIELD) or "")

    first = _as_int(variables.get("first"), default=_DEFAULT_FIRST)
    offset = max(0, _as_int(variables.get("after"), default=0))

    page = ordered[offset : offset + first]
    new_offset = offset + len(page)
    has_next_page = new_offset < len(ordered)
    end_cursor: Optional[str] = str(new_offset) if page else None

    payload = {
        "data": {
            connection: {
                "nodes": page,
                "pageInfo": {
                    "hasNextPage": has_next_page,
                    "endCursor": end_cursor,
                },
            }
        }
    }
    return _build_response(prep, payload)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_body(body: Any) -> tuple[str, dict]:
    if body is None:
        return "", {}
    if isinstance(body, (bytes, bytearray)):
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return "", {}
    else:
        text = str(body)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return "", {}
    if not isinstance(parsed, dict):
        return "", {}
    query = str(parsed.get("query") or "")
    variables = parsed.get("variables") or {}
    if not isinstance(variables, dict):
        variables = {}
    return query, variables


def _detect_connection(query: str) -> str:
    for conn in _CONNECTIONS:
        if f"{conn}(" in query:
            return conn
    return "issues"


def _within_window(
    value: Any, since: Optional[str], until: Optional[str]
) -> bool:
    if not isinstance(value, str):
        return False
    if since is not None and value < since:
        return False
    if until is not None and value > until:
        return False
    return True


def _augment_with_future(records: list[dict]) -> list[dict]:
    """Append ``_FUTURE_RECORDS`` future-dated clones to the corpus."""
    if not records:
        return records
    base = datetime.now(timezone.utc) + timedelta(days=365)
    template = records[-1]
    future = []
    for i in range(_FUTURE_RECORDS):
        clone = copy.deepcopy(template)
        clone["id"] = f"{clone.get('id', 'future')}::future-{i}"
        clone[_CURSOR_FIELD] = (base + timedelta(hours=i)).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        future.append(clone)
    return list(records) + future


def _as_int(value: Any, *, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_response(prep: PreparedRequest, payload: Any) -> Response:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
