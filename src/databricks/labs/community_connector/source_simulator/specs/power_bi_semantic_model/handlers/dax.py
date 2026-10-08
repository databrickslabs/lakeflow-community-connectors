"""Simulator handler for ``POST .../datasets/{datasetId}/executeQueries``.

This endpoint cannot be served by the declarative spec pipeline for two
reasons:

* Its response is an envelope of fixed nesting depth
  (``results[0].tables[0].rows``) rather than a record collection under one key.
* Its rows *are* the user's DAX query result, so their column set is defined by
  whatever query the pipeline was configured with — there is no corpus table
  that could stand in for "the answer to an arbitrary DAX statement".

So the handler returns a small, fixed, deterministic result set shaped like a
real ``EVALUATE`` response: DAX-style bracketed column names, mixed value types,
and one BLANK() rendered as a JSON null. That is enough to exercise both of the
connector's output modes — the ``columns`` string map (no ``dax_columns``
option) and the typed-column mode (``dax_columns`` declared against these
names).
"""

from __future__ import annotations

import json
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# Column names mirror the two forms DAX actually emits: `Table[Column]` for
# columns projected from a table, and `[Measure]` for a measure or an
# expression named in an ADDCOLUMNS/SUMMARIZECOLUMNS.
_ROWS: list[dict[str, Any]] = [
    {
        "Sales[Region]": "North",
        "Sales[OrderDate]": "2024-01-15T00:00:00",
        "Sales[Amount]": 15230.75,
        "[Total Units]": 412,
        "[Is Target Met]": True,
    },
    {
        "Sales[Region]": "South",
        "Sales[OrderDate]": "2024-02-03T00:00:00",
        "Sales[Amount]": 9875.5,
        "[Total Units]": 268,
        "[Is Target Met]": False,
    },
    {
        "Sales[Region]": "East",
        "Sales[OrderDate]": "2024-02-27T00:00:00",
        "Sales[Amount]": 22104.0,
        "[Total Units]": 631,
        "[Is Target Met]": True,
    },
    {
        "Sales[Region]": "West",
        "Sales[OrderDate]": "2024-03-11T00:00:00",
        "Sales[Amount]": 4310.25,
        "[Total Units]": 97,
        "[Is Target Met]": False,
    },
    {
        "Sales[Region]": "Unassigned",
        "Sales[OrderDate]": None,
        # BLANK() with serializerSettings.includeNulls=true.
        "Sales[Amount]": None,
        "[Total Units]": 0,
        "[Is Target Met]": False,
    },
]


def execute_queries(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Return a fixed DAX result set in the executeQueries envelope."""
    body = _parse_json_body(prep.body)
    queries = body.get("queries") or []
    if not queries or not str((queries[0] or {}).get("query") or "").strip():
        # The real API rejects an empty query batch; mirroring that keeps a
        # connector bug from looking like an empty-but-valid result.
        return _json_response(
            prep,
            400,
            {
                "error": {
                    "code": "InvalidRequest",
                    "message": "The 'queries' collection must contain a query.",
                }
            },
        )

    return _json_response(
        prep,
        200,
        {"results": [{"tables": [{"rows": [dict(row) for row in _ROWS]}]}]},
    )


def _parse_json_body(body: Any) -> dict[str, Any]:
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


def _json_response(prep: PreparedRequest, status: int, payload: dict[str, Any]) -> Response:
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(payload, ensure_ascii=False),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
