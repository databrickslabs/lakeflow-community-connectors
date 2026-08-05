"""Serve QuickBooks CDC requests, including transaction tombstones."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_SUPPORTED_ENTITIES = {"Invoice", "Bill"}


def serve_cdc(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    params = parse_qs(urlsplit(prep.url or "").query)
    entity = (params.get("entities") or [""])[-1].capitalize()
    changed_since = (params.get("changedSince") or [""])[-1]
    if entity not in _SUPPORTED_ENTITIES or not changed_since:
        return _response(prep, 400, {"Fault": {"Error": [{"Message": "Invalid CDC query"}]}})

    try:
        lower = datetime.fromisoformat(changed_since.replace("Z", "+00:00"))
    except ValueError:
        return _response(prep, 400, {"Fault": {"Error": [{"Message": "Invalid timestamp"}]}})
    if lower.tzinfo is None:
        lower = lower.replace(tzinfo=timezone.utc)

    records = corpus.get(f"Deleted{entity}") or []
    tombstone_time = _format_datetime(lower + timedelta(seconds=30))
    rows = []
    for record in records:
        if not isinstance(record, dict):
            continue
        row = dict(record)
        row["MetaData"] = {"LastUpdatedTime": tombstone_time}
        rows.append(row)

    payload = {
        "CDCResponse": [
            {
                "QueryResponse": [
                    {
                        entity: rows,
                        "startPosition": 1,
                        "maxResults": len(rows),
                    }
                ]
            }
        ],
        "time": _format_datetime(lower + timedelta(seconds=60)),
    }
    return _response(prep, 200, payload)


def _format_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _response(prep: PreparedRequest, status: int, payload: dict) -> Response:
    body = json.dumps(payload, ensure_ascii=False)
    record = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json; charset=utf-8"},
        body_text=body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)
