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

Repeated ``_lastUpdated`` values are ANDed, as FHIR search specifies (the
connector sends ``gt<cursor>`` and ``le<init_ts>``), and ``_sort=_lastUpdated``
is honoured. The shared query parser keeps only the last value of a repeated
key, so the query string is parsed here.
"""

from __future__ import annotations

import json
from typing import Any, List, Optional
from urllib.parse import parse_qsl, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
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
    query = parse_qsl(urlsplit(prep.url or "").query, keep_blank_values=True)
    bounds = [_parse_prefix(v) for k, v in query if k == "_lastUpdated"]
    raw_resources: List[dict] = corpus.get("Patient") or []

    filtered: List[dict] = [
        r
        for r in raw_resources
        if all(
            _matches((r.get("meta") or {}).get("lastUpdated"), prefix, target)
            for prefix, target in bounds
        )
    ]
    if ("_sort", "_lastUpdated") in query:
        filtered.sort(key=lambda r: (r.get("meta") or {}).get("lastUpdated") or "")

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
