"""Serve QuickBooks SQL-like entity queries from the simulator corpus."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_QUERY_RE = re.compile(
    r"^SELECT \* FROM (?P<entity>Customer|Vendor|Account|Item|Invoice|Bill) "
    r"(?:WHERE (?:Active IN \(true, false\)(?: AND )?)?"
    r"(?:MetaData\.LastUpdatedTime >= '(?P<lower>[^']+)' "
    r"AND MetaData\.LastUpdatedTime <= '(?P<upper>[^']+)')? )?"
    r"STARTPOSITION (?P<start>[1-9][0-9]*) MAXRESULTS (?P<limit>[1-9][0-9]*)$",
    re.IGNORECASE,
)


def serve_query(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    params = parse_qs(urlsplit(prep.url or "").query)
    query = (params.get("query") or [""])[-1]
    match = _QUERY_RE.fullmatch(query)
    if not match:
        return _response(prep, 400, {"Fault": {"Error": [{"Message": "Invalid query"}]}})

    entity = match.group("entity").capitalize()
    records = corpus.get(entity) or []
    if not isinstance(records, list):
        records = []
    records = [
        _give_synthetic_future_record_unique_id(record)
        for record in records
        if isinstance(record, dict)
    ]
    lower = match.group("lower")
    upper = match.group("upper")
    if lower and upper:
        lower_dt = _parse_datetime(lower)
        upper_dt = _parse_datetime(upper)
        records = [
            record
            for record in records
            if isinstance(record, dict)
            and isinstance(record.get("MetaData"), dict)
            and isinstance(record["MetaData"].get("LastUpdatedTime"), str)
            and lower_dt <= _parse_datetime(record["MetaData"]["LastUpdatedTime"]) <= upper_dt
        ]
    start = int(match.group("start")) - 1
    limit = min(int(match.group("limit")), 1000)
    page = records[start : start + limit]
    payload = {
        "QueryResponse": {
            entity: page,
            "startPosition": start + 1,
            "maxResults": len(page),
        },
        "time": "2026-07-26T00:00:00.000Z",
    }
    return _response(prep, 200, payload)


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _give_synthetic_future_record_unique_id(record: dict) -> dict:
    """Keep synthesized future rows distinct when a corpus record is cloned."""
    metadata = record.get("MetaData")
    if not isinstance(metadata, dict):
        return record
    timestamp = metadata.get("LastUpdatedTime")
    if not isinstance(timestamp, str) or _parse_datetime(timestamp) <= datetime.now(timezone.utc):
        return record
    cloned = dict(record)
    cloned["Id"] = f"{record.get('Id', 'record')}-future-{timestamp}"
    return cloned


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
