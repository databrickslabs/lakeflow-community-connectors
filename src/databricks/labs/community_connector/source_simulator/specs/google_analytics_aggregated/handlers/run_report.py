"""Custom handler for ``POST /v1beta/properties/{id}:runReport``.

The runReport endpoint takes a JSON body whose ``dimensions`` and ``metrics``
arrays determine the response shape — a static corpus can't satisfy this
because each test passes a different combination. The handler reads the
posted body, builds matching ``dimensionHeaders``/``metricHeaders``, and
emits one stub row per ``dateRanges`` entry with synthetic values.

It also enforces the GA4 API limit of 4 ``dateRanges`` per request: when
exceeded, returns 400 with a body that mentions ``dateRange`` so the
connector's ``_make_api_request`` surfaces the error.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)


_INTEGER_TYPES = {"TYPE_INTEGER"}
_FLOAT_TYPES = {"TYPE_FLOAT", "TYPE_SECONDS", "TYPE_CURRENCY", "TYPE_PERCENT"}


def _stub_dim_value(name: str, idx: int) -> str:
    if name == "date":
        return f"2024010{(idx % 9) + 1}"
    if name == "country":
        return "United States"
    if name == "city":
        return "San Francisco"
    return f"{name}_value_{idx}"


def _stub_metric_value(metric_type: str, idx: int) -> str:
    if metric_type in _INTEGER_TYPES:
        return str(100 + idx)
    if metric_type in _FLOAT_TYPES:
        return f"{1.5 + idx:.4f}"
    return str(idx)


def _metric_type_for(name: str, corpus) -> str:
    metadata = corpus.get("metadata") or {}
    for entry in metadata.get("metrics", []) or []:
        if entry.get("apiName") == name:
            return entry.get("type", "TYPE_INTEGER")
    return "TYPE_INTEGER"


def _json_response(status: int, body: Dict[str, Any], prep: PreparedRequest) -> Response:
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def serve_run_report(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    raw_body = prep.body or b""
    if isinstance(raw_body, (bytes, bytearray)):
        raw_body = raw_body.decode("utf-8")
    try:
        body = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError:
        body = {}

    date_ranges: List[Dict[str, str]] = body.get("dateRanges") or []
    if len(date_ranges) > 4:
        err = {
            "error": {
                "code": 400,
                "message": (
                    f"Too many dateRanges in request: got {len(date_ranges)}, "
                    "maximum is 4."
                ),
                "status": "INVALID_ARGUMENT",
            }
        }
        return _json_response(400, err, prep)

    dimensions = [d.get("name") for d in (body.get("dimensions") or []) if d.get("name")]
    metrics = [m.get("name") for m in (body.get("metrics") or []) if m.get("name")]

    offset = int(body.get("offset", 0) or 0)
    if offset > 0:
        # Connector paginates with offset; second page should be empty so the
        # loop terminates after consuming the first page's stub rows.
        empty: Dict[str, Any] = {
            "dimensionHeaders": [{"name": d} for d in dimensions],
            "metricHeaders": [
                {"name": m, "type": _metric_type_for(m, corpus)} for m in metrics
            ],
            "rows": [],
            "rowCount": 0,
            "metadata": {"currencyCode": "USD", "timeZone": "UTC"},
            "kind": "analyticsData#runReport",
        }
        return _json_response(200, empty, prep)

    rows: List[Dict[str, Any]] = []
    n_rows = max(len(date_ranges), 1)
    for i in range(n_rows):
        rows.append(
            {
                "dimensionValues": [
                    {"value": _stub_dim_value(d, i)} for d in dimensions
                ],
                "metricValues": [
                    {"value": _stub_metric_value(_metric_type_for(m, corpus), i)}
                    for m in metrics
                ],
            }
        )

    response: Dict[str, Any] = {
        "dimensionHeaders": [{"name": d} for d in dimensions],
        "metricHeaders": [
            {"name": m, "type": _metric_type_for(m, corpus)} for m in metrics
        ],
        "rows": rows,
        "rowCount": len(rows),
        "metadata": {"currencyCode": "USD", "timeZone": "UTC"},
        "kind": "analyticsData#runReport",
    }
    return _json_response(200, response, prep)
