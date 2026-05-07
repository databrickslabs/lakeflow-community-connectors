"""Custom handler for the Databricks SQL Statement Execution API used in
HL7 v2 ``source_type=delta`` mode.

The connector issues three statement shapes against the SQL warehouse:

1. ``SELECT data, date_format(createTime, …) AS createTime, name FROM <table>
   ORDER BY createTime`` — preload mode.
2. ``SELECT data, date_format(createTime, …), name FROM <table>
   WHERE … > 'X' AND … <= 'Y' ORDER BY createTime`` — per-window mode.
3. ``SELECT date_format(MIN(createTime), …) FROM <table>`` — peek-oldest.

We synthesize results from the same ``messages`` corpus the GCP handler
serves, projecting the fields the connector expects. The
``data_array`` rows mirror the column ordering above.

Two endpoints together implement the SDK's wait-then-poll loop:

- ``POST /api/2.0/sql/statements/`` — submit; returns ``statement_id`` and
  the result body inline (state SUCCEEDED) so the connector never has to
  poll. We support the polling endpoint anyway for robustness.
- ``GET  /api/2.0/sql/statements/{statement_id}`` — poll; returns the
  same body as the submit response.
"""

from __future__ import annotations

import base64
import json
import re
from typing import Any, Dict, List

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


_WHERE_RANGE_RE = re.compile(
    r"date_format\(createTime[^)]*\)\s*>\s*'([^']*)'\s+AND\s+"
    r"date_format\(createTime[^)]*\)\s*<=\s*'([^']*)'",
    re.IGNORECASE,
)
_MIN_CREATETIME_RE = re.compile(r"MIN\(createTime\)", re.IGNORECASE)


def _decode_data(raw_b64: str) -> str:
    try:
        return base64.b64decode(raw_b64).decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001 — malformed corpus row, surface as empty
        return ""


def _build_data_array(
    messages: List[Dict[str, Any]], stmt: str
) -> List[List[Any]]:
    """Project corpus messages into the connector's expected row shape."""
    if _MIN_CREATETIME_RE.search(stmt):
        if not messages:
            return [[None]]
        oldest = min(m.get("createTime", "") for m in messages)
        return [[oldest]]

    range_match = _WHERE_RANGE_RE.search(stmt)
    selected = messages
    if range_match:
        since, until = range_match.group(1), range_match.group(2)
        selected = [
            m for m in messages
            if since < (m.get("createTime") or "") <= until
        ]

    selected = sorted(selected, key=lambda m: m.get("createTime") or "")

    rows: List[List[Any]] = []
    for m in selected:
        # Delta column order: data (raw HL7 text), createTime, name.
        rows.append([
            _decode_data(m.get("data", "")),
            m.get("createTime", ""),
            m.get("name", ""),
        ])
    return rows


def _make_response(prep: PreparedRequest, body: Dict[str, Any]) -> Response:
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


def _wrap_result(stmt: str, data_array: List[List[Any]]) -> Dict[str, Any]:
    return {
        "statement_id": "stmt-hl7v2-sim",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "schema": {"column_count": 3, "columns": []},
            "total_row_count": len(data_array),
        },
        "result": {"data_array": data_array, "row_count": len(data_array)},
    }


def submit_statement(prep: PreparedRequest, spec, corpus) -> Response:
    """Handle ``POST /api/2.0/sql/statements/`` — submit + inline result."""
    body_bytes = prep.body or b""
    if isinstance(body_bytes, str):
        body_bytes = body_bytes.encode("utf-8")
    try:
        payload = json.loads(body_bytes or b"{}")
    except (ValueError, TypeError):
        payload = {}
    stmt = str(payload.get("statement", ""))

    messages: List[Dict[str, Any]] = corpus.get("messages") or []
    data_array = _build_data_array(messages, stmt)
    return _make_response(prep, _wrap_result(stmt, data_array))


def poll_statement(prep: PreparedRequest, spec, corpus) -> Response:
    """Handle ``GET /api/2.0/sql/statements/{statement_id}`` — already-done."""
    # The submit handler returned SUCCEEDED inline, so the SDK shouldn't poll
    # in practice. If it does, return an empty SUCCEEDED result to unblock —
    # the connector won't fall back to polling for these queries.
    return _make_response(prep, _wrap_result("", []))
