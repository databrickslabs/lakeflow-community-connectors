"""Custom simulator handlers for Power BI's Entra token + Scanner API.

Three of this connector's endpoints don't fit the declarative
filter/sort/paginate pipeline:

* ``POST /{tenant}/oauth2/v2.0/token`` — form-encoded body, returns a token
  envelope rather than records.
* ``POST /admin/workspaces/getInfo`` + ``GET .../scanStatus/{id}`` — an async
  handshake that returns a scan ID and a status, not a record collection.
* ``GET .../scanResult/{id}`` — one deeply nested tree
  (``workspaces[].datasets[].tables[].columns[]/measures[]``) from which the
  connector derives *three* flat tables.

The scan-result handler stitches that tree together out of the flat corpus
tables the schema bootstrapper produces (``workspaces``, ``datasets``,
``dataset_tables``, ``dataset_columns``, ``dataset_measures``), distributing
children across parents round-robin.  The connector re-derives
``workspace_id`` / ``dataset_id`` / ``table_name`` from the nesting, so the
rows it emits are internally consistent regardless of what the flat corpus
records happened to carry in those columns.
"""

from __future__ import annotations

import itertools
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

# scan id -> requested workspace ids. The simulator runs in-process, and each
# worker drives its own getInfo -> scanStatus -> scanResult sequence, so plain
# module state is enough to carry the request through the handshake.
_SCANS: dict[str, list[str]] = {}
_SCAN_IDS = itertools.count(1)


# ---------------------------------------------------------------------------
# Entra ID token
# ---------------------------------------------------------------------------


def oauth_token(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Stub the client-credentials token exchange.

    The simulator never validates auth, but the connector fetches a bearer
    before its first Power BI call, so this has to return a 200-shaped
    envelope or every read fails with UnknownEndpoint.
    """
    return _json_response(
        prep,
        200,
        {
            "token_type": "Bearer",
            "expires_in": 3600,
            "ext_expires_in": 3600,
            "access_token": "simulated-power-bi-access-token",
        },
    )


# ---------------------------------------------------------------------------
# Scanner API
# ---------------------------------------------------------------------------


def post_workspace_info(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Accept a scan request and hand back a scan ID (real API returns 202)."""
    body = _parse_json_body(prep.body)
    workspace_ids = [str(w) for w in (body.get("workspaces") or []) if w]

    scan_id = f"sim-scan-{next(_SCAN_IDS):04d}"
    _SCANS[scan_id] = workspace_ids

    return _json_response(
        prep,
        202,
        {
            "id": scan_id,
            "createdDateTime": _now_iso(),
            "status": "NotStarted",
        },
    )


def get_scan_status(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Report the scan as already finished so tests don't sit in a poll loop."""
    scan_id = _last_path_segment(prep.url)
    return _json_response(
        prep,
        200,
        {
            "id": scan_id,
            "createdDateTime": _now_iso(),
            "status": "Succeeded",
        },
    )


def get_scan_result(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Return the nested workspace/dataset/table metadata tree."""
    scan_id = _last_path_segment(prep.url)
    requested = _SCANS.get(scan_id) or []

    return _json_response(
        prep,
        200,
        {
            "workspaces": _build_tree(corpus, requested),
            "datasourceInstances": [],
            "misconfiguredDatasourceInstances": [],
        },
    )


# ---------------------------------------------------------------------------
# tree assembly
# ---------------------------------------------------------------------------


def _build_tree(corpus: Any, requested: list[str]) -> list[dict[str, Any]]:
    workspaces = _records(corpus, "workspaces")
    datasets = _records(corpus, "datasets")
    tables = _records(corpus, "dataset_tables")
    columns = _records(corpus, "dataset_columns")
    measures = _records(corpus, "dataset_measures")

    workspace_ids = [w["id"] for w in workspaces if w.get("id")]
    if requested:
        scoped = [wid for wid in workspace_ids if wid in requested]
        # A corpus whose workspace IDs don't line up with the request would
        # otherwise yield an empty scan; fall back to the requested IDs so the
        # connector still gets a populated tree.
        workspace_ids = scoped or list(requested)
    if not workspace_ids:
        workspace_ids = ["sim-workspace-0001"]

    name_by_id = {w.get("id"): w.get("name") for w in workspaces}
    ws_nodes: list[dict[str, Any]] = [
        {
            "id": wid,
            "name": name_by_id.get(wid) or f"Simulated workspace {i + 1}",
            "type": "Workspace",
            "state": "Active",
            "datasets": [],
        }
        for i, wid in enumerate(workspace_ids)
    ]

    ds_nodes = _attach(
        parents=ws_nodes,
        child_key="datasets",
        records=datasets or [{}],
        build=lambda rec, i: {
            "id": rec.get("id") or f"sim-dataset-{i + 1:04d}",
            "name": rec.get("name") or f"Simulated dataset {i + 1}",
            "configuredBy": rec.get("configuredBy"),
            "targetStorageMode": rec.get("targetStorageMode"),
            "tables": [],
            "relationships": [],
        },
    )

    tbl_nodes = _attach(
        parents=ds_nodes,
        child_key="tables",
        records=tables or [{}],
        build=lambda rec, i: {
            "name": rec.get("name") or f"SimulatedTable{i + 1}",
            "isHidden": rec.get("isHidden"),
            "description": rec.get("description"),
            "source": rec.get("source") or [],
            "columns": [],
            "measures": [],
        },
    )

    _attach(
        parents=tbl_nodes,
        child_key="columns",
        records=columns,
        build=lambda rec, i: {
            "name": rec.get("name") or f"SimulatedColumn{i + 1}",
            "dataType": rec.get("dataType") or "String",
            "dataCategory": rec.get("dataCategory"),
            "formatString": rec.get("formatString"),
            "isHidden": rec.get("isHidden"),
            "sortByColumn": rec.get("sortByColumn"),
            "summarizeBy": rec.get("summarizeBy"),
        },
    )

    _attach(
        parents=tbl_nodes,
        child_key="measures",
        records=measures,
        build=lambda rec, i: {
            "name": rec.get("name") or f"SimulatedMeasure{i + 1}",
            "expression": rec.get("expression") or "COUNTROWS(SimulatedTable1)",
            "description": rec.get("description"),
            "formatString": rec.get("formatString"),
            "isHidden": rec.get("isHidden"),
        },
    )

    return ws_nodes


def _attach(
    parents: list[dict[str, Any]],
    child_key: str,
    records: list[dict[str, Any]],
    build,
) -> list[dict[str, Any]]:
    """Build one child node per record and spread them across ``parents``."""
    if not parents:
        return []
    children: list[dict[str, Any]] = []
    for i, record in enumerate(records):
        node = build(record if isinstance(record, dict) else {}, i)
        parents[i % len(parents)][child_key].append(node)
        children.append(node)
    return children


def _records(corpus: Any, name: str) -> list[dict[str, Any]]:
    value = corpus.get(name) if corpus is not None else None
    if isinstance(value, list):
        return [rec for rec in value if isinstance(rec, dict)]
    if isinstance(value, dict):
        return [value]
    return []


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


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


def _last_path_segment(url: str | None) -> str:
    path = urlsplit(url or "").path
    return path.rstrip("/").rsplit("/", 1)[-1] or "sim-scan-0001"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json_response(prep: PreparedRequest, status: int, payload: dict[str, Any]) -> Response:
    body = json.dumps(payload, ensure_ascii=False)
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
