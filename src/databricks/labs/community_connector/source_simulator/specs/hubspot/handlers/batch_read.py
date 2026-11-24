"""Shared response helper for HubSpot simulator handlers."""

from __future__ import annotations

import json
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import response_from_record


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
    record = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=body.decode("utf-8"),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)
