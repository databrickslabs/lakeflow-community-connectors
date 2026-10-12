"""Custom simulator handler for HubSpot ``POST /crm/v3/objects/{type}/batch/read``.

Batch read takes its inputs in the JSON body (``inputs`` IDs plus
``propertiesWithHistory``), which the declarative param-role pipeline
can't express. This handler:

  1. Resolves the object type from the URL path.
  2. Serves records from the ``{type}_history`` corpus whose ``id`` is in
     the request's ``inputs``.
  3. Keeps only the ``propertiesWithHistory`` keys the request asked for,
     mirroring how HubSpot ignores properties that weren't requested.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_PATH_RE = re.compile(r"/crm/v3/objects/(?P<object_type>[^/]+)/batch/read")


def serve_batch_read(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    match = _PATH_RE.search(prep.url or "")
    if not match:
        return _build_response(prep, status=404, payload={"message": "unknown path"})
    object_type = match.group("object_type")

    body = _parse_body(prep.body)
    wanted_ids = {str(i.get("id")) for i in body.get("inputs") or [] if isinstance(i, dict)}
    wanted_props = set(body.get("propertiesWithHistory") or [])

    records = corpus.get(f"{object_type}_history") or []
    results = []
    for record in records:
        if str(record.get("id")) not in wanted_ids:
            continue
        out = dict(record)
        history = record.get("propertiesWithHistory") or {}
        out["propertiesWithHistory"] = {
            k: v for k, v in history.items() if not wanted_props or k in wanted_props
        }
        results.append(out)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    payload = {
        "status": "COMPLETE",
        "results": results,
        "startedAt": now,
        "completedAt": now,
    }
    return _build_response(prep, status=200, payload=payload)


def _parse_body(raw: Any) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


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
