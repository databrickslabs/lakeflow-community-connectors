"""Custom simulator handler for Linear's single GraphQL endpoint.

Linear is a single POST endpoint (``/graphql``) whose query, variables, and
Relay-style pagination all live in the JSON request body, so the simulator's
declarative param-role pipeline (built for URL query params) can't match it.
This handler:

  1. Parses the request body (``query`` + ``variables``).
  2. Picks the corpus by the GraphQL root field (``issues`` / ``projects``).
  3. Re-nests connection fields (``labels`` / ``members`` / ``teams`` /
     ``projectMilestones``) into the GraphQL ``{ "nodes": [...] }`` envelope so
     the connector's flatten step round-trips correctly. The corpus is stored
     in the connector's *flattened* shape (one JSON file per table, generated
     from the connector StructType), which is the inverse of what live Linear
     returns.
  4. Sorts ascending by ``updatedAt`` and applies the ``updatedAt: { gte }``
     filter the connector sends for incremental reads.
  5. Paginates with an opaque Relay cursor encoding the next offset, mirroring
     ``pageInfo { hasNextPage endCursor }``.
"""

from __future__ import annotations

import copy
import json
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# Root field -> corpus name (corpus files are issues.json / projects.json).
_ROOT_TO_CORPUS = {
    "issues": "issues",
    "projects": "projects",
}

# Connection fields per root field, re-nested into { "nodes": [...] }.
_CONNECTION_FIELDS = {
    "issues": ["labels"],
    "projects": ["members", "teams", "projectMilestones"],
}

_DEFAULT_FIRST = 50
_CURSOR_PREFIX = "cursor-"


def serve_graphql(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    body = _parse_body(prep.body)
    query = body.get("query") or ""
    variables = body.get("variables") or {}

    root = _resolve_root_field(query)
    if root is None:
        return _build_response(
            prep,
            status=200,
            payload={"errors": [{"message": "Unsupported query: no known root field"}]},
        )

    corpus_name = _ROOT_TO_CORPUS[root]
    records = corpus.get(corpus_name) or []
    if not isinstance(records, list):
        records = []

    # Sort ascending by updatedAt (the connector orders by updatedAt and relies
    # on ascending order for cursor checkpointing).
    records = sorted(records, key=lambda r: r.get("updatedAt") or "")

    # Apply the updatedAt >= gte filter, when the connector sends one.
    gte = _extract_updated_after(variables)
    if gte is not None:
        records = [r for r in records if (r.get("updatedAt") or "") >= gte]

    # Relay pagination via opaque cursor that encodes the next offset.
    first = _coerce_int(variables.get("first"), _DEFAULT_FIRST)
    offset = _decode_cursor(variables.get("after"))
    page = records[offset : offset + first]
    new_offset = offset + len(page)
    has_next = new_offset < len(records) and bool(page)
    end_cursor = _encode_cursor(new_offset) if page else None

    nodes = [_renest(root, copy.deepcopy(r)) for r in page]

    payload = {
        "data": {
            root: {
                "nodes": nodes,
                "pageInfo": {
                    "hasNextPage": has_next,
                    "endCursor": end_cursor,
                },
            }
        }
    }
    return _build_response(prep, status=200, payload=payload)


# --- helpers ---------------------------------------------------------------


def _parse_body(body: Any) -> dict[str, Any]:
    if body is None:
        return {}
    if isinstance(body, (bytes, bytearray)):
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            return {}
    else:
        text = str(body)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _resolve_root_field(query: str) -> str | None:
    # ``projects(`` only appears in the projects query; ``issues(`` only in the
    # issues query (the issues query references ``project { ... }`` without a
    # paren). Check projects first to avoid any ambiguity.
    if "projects(" in query:
        return "projects"
    if "issues(" in query:
        return "issues"
    return None


def _extract_updated_after(variables: dict[str, Any]) -> str | None:
    filt = variables.get("filter")
    if not isinstance(filt, dict):
        return None
    updated = filt.get("updatedAt")
    if isinstance(updated, dict):
        val = updated.get("gte") or updated.get("gt")
        return str(val) if val is not None else None
    return None


def _renest(root: str, record: dict[str, Any]) -> dict[str, Any]:
    """Wrap flattened connection arrays back into ``{ "nodes": [...] }``."""
    for field in _CONNECTION_FIELDS.get(root, []):
        value = record.get(field)
        if isinstance(value, list):
            record[field] = {"nodes": value}
        elif value is None:
            record[field] = {"nodes": []}
    return record


def _coerce_int(value: Any, default: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return n if n > 0 else default


def _decode_cursor(cursor: Any) -> int:
    if cursor in (None, "", 0):
        return 0
    if isinstance(cursor, int):
        return max(0, cursor)
    cursor_str = str(cursor)
    if cursor_str.startswith(_CURSOR_PREFIX):
        cursor_str = cursor_str[len(_CURSOR_PREFIX) :]
    try:
        return max(0, int(cursor_str))
    except (TypeError, ValueError):
        return 0


def _encode_cursor(offset: int) -> str:
    return f"{_CURSOR_PREFIX}{offset}"


def _build_response(
    prep: PreparedRequest, *, status: int, payload: dict[str, Any]
) -> Response:
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


def serve_oauth_token(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Stub Linear OAuth token endpoint — issues a placeholder access token.

    Only hit when the connector is configured with OAuth credentials instead of
    a personal API key. The simulator never validates auth, so any token works.
    """
    return _build_response(
        prep,
        status=200,
        payload={
            "access_token": "simulated-linear-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "read",
        },
    )
