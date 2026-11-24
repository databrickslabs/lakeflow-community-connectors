"""Custom handler for ``GET /fhir/Patient`` that honors ``_lastUpdated``.

FHIR uses inline prefix operators on search values
(``_lastUpdated=gt2024-01-05T00:00:00Z``), which doesn't fit the simulator's
``params: {_lastUpdated: {role: filter, op: gt}}`` shape (the value would
include the ``gt`` prefix). A small handler reads the param, strips the
prefix, and filters the corpus before wrapping in a Bundle.

Without this, the connector's micro-batch offset contract sees that the
"server" returns the same records on every call regardless of the cursor,
and (correctly) raises an error about the FHIR server ignoring
``_lastUpdated``.
"""

from __future__ import annotations

import json
from typing import Any, List, Optional

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    request_record_from_prepared,
    response_from_record,
)


_PREFIXES = ("gt", "ge", "lt", "le", "eq", "ne")


def _parse_prefix(raw: Optional[str]):
    if not raw:
        return None, None
    for p in _PREFIXES:
        if raw.startswith(p) and len(raw) > len(p):
            return p, raw[len(p):]
    return "eq", raw


def _matches(value: Optional[str], prefix: str, target: str) -> bool:
    if value is None:
        return False
    if prefix == "eq":
        return value == target
    if prefix == "ne":
        return value != target
    if prefix == "gt":
        return value > target
    if prefix == "ge":
        return value >= target
    if prefix == "lt":
        return value < target
    if prefix == "le":
        return value <= target
    return True


def serve_patient_bundle(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    req = request_record_from_prepared(prep)
    raw_resources: List[dict] = corpus.get("Patient") or []

    prefix, target = _parse_prefix(req.query.get("_lastUpdated"))
    filtered: List[dict] = []
    for r in raw_resources:
        if prefix is None:
            filtered.append(r)
            continue
        last_updated = ((r.get("meta") or {}).get("lastUpdated"))
        if _matches(last_updated, prefix, target):
            filtered.append(r)

    bundle: dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": len(filtered),
        "link": [{"relation": "self", "url": prep.url or ""}],
        "entry": [{"resource": r} for r in filtered],
    }
    body_bytes = json.dumps(bundle).encode("utf-8")
    rec = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/fhir+json"},
        body_text=body_bytes.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
