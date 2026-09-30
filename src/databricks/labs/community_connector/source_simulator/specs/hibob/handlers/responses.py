"""Custom simulator handlers for HiBob endpoints whose response shape is not
a flat record array.

The corpus files hold rows in the connector's *output* shape (one row per
table record, as generated from ``TABLE_SCHEMAS``). These handlers re-nest
those rows into the API's wire shape:

* ``serve_bulk`` — ``GET /v1/bulk/people/{work|employment|lifecycle|salaries}``:
  groups rows by ``employeeId`` into ``results[].values[]`` and paginates the
  employee groups with an opaque ``cursor`` + ``limit``
  (``response_metadata.next_cursor``). Honours ``employeeIds``.
* ``serve_named_lists`` — ``GET /v1/company/named-lists``: groups rows by
  ``list_name`` into ``{"lists": [{"name", "items": [...]}]}`` and re-nests
  items under their ``parent_id`` as ``children`` when the parent exists.
* ``serve_policy_types`` — ``GET /v1/timeoff/policy-types``: returns
  ``{"policyTypes": [<name>, ...]}``.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_CURSOR_PREFIX = "cursor-"
_BULK_DEFAULT_LIMIT = 50
_BULK_MAX_LIMIT = 200


def serve_bulk(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    query = _query(prep)
    rows = _rows(corpus, spec.corpus)

    wanted = query.get("employeeIds")
    wanted_ids = {e.strip() for e in wanted.split(",") if e.strip()} if wanted else None

    groups: dict[str, list[dict]] = {}
    for row in rows:
        emp_id = row.get("employeeId")
        if emp_id is None:
            continue
        emp_id = str(emp_id)
        if wanted_ids is not None and emp_id not in wanted_ids:
            continue
        value = {k: v for k, v in row.items() if k != "employeeId"}
        groups.setdefault(emp_id, []).append(value)

    results = [{"employeeId": k, "values": v} for k, v in groups.items()]

    limit = _int(query.get("limit"), _BULK_DEFAULT_LIMIT)
    limit = max(1, min(limit, _BULK_MAX_LIMIT))
    offset = _decode_cursor(query.get("cursor"))
    page = results[offset:offset + limit]
    new_offset = offset + len(page)
    next_cursor = f"{_CURSOR_PREFIX}{new_offset}" if page and new_offset < len(results) else None

    return _json_response(prep, {
        "results": page,
        "response_metadata": {"next_cursor": next_cursor},
        "errors": [],
    })


def serve_named_lists(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    rows = _rows(corpus, spec.corpus)
    include_archived = (_query(prep).get("includeArchived") or "false").lower() == "true"

    lists: dict[str, list[dict]] = {}
    for row in rows:
        if not include_archived and row.get("archived") is True:
            continue
        lists.setdefault(str(row.get("list_name")), []).append(row)

    payload_lists = []
    for name, list_rows in lists.items():
        nodes: dict[str, dict] = {}
        for row in list_rows:
            nodes[str(row.get("item_id"))] = {
                "id": row.get("item_id"),
                "value": row.get("value"),
                "name": row.get("name"),
                "archived": row.get("archived"),
                "children": [],
            }
        roots = []
        for row in list_rows:
            node = nodes[str(row.get("item_id"))]
            parent = row.get("parent_id")
            if parent is not None and str(parent) in nodes and str(parent) != str(row.get("item_id")):
                nodes[str(parent)]["children"].append(node)
            else:
                roots.append(node)
        payload_lists.append({"name": name, "items": roots})

    return _json_response(prep, {"lists": payload_lists})


def serve_policy_types(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:
    rows = _rows(corpus, spec.corpus)
    names = [r.get("name") for r in rows if r.get("name") is not None]
    return _json_response(prep, {"policyTypes": names})


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _rows(corpus: Any, name: str | None) -> list[dict]:
    records = corpus.get(name) if name else None
    return [r for r in records if isinstance(r, dict)] if isinstance(records, list) else []


def _query(prep: PreparedRequest) -> dict[str, str]:
    parsed = parse_qs(urlsplit(prep.url or "").query, keep_blank_values=True)
    return {k: v[-1] for k, v in parsed.items() if v}


def _int(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _decode_cursor(cursor: Any) -> int:
    if not cursor:
        return 0
    text = str(cursor)
    if text.startswith(_CURSOR_PREFIX):
        text = text[len(_CURSOR_PREFIX):]
    try:
        return max(0, int(text))
    except ValueError:
        return 0


def _json_response(prep: PreparedRequest, payload: dict) -> Response:
    body = json.dumps(payload, ensure_ascii=False)
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
