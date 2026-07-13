"""Custom simulator handlers for LSEG LDMS (RDMS) endpoints.

Two LDMS endpoints don't fit the declarative param-role pipeline:

  * ``POST /api/v1/CurveValuesBatch`` — the request parameters (scenarioID,
    minValueDate / maxValueDate, curveRequests) live in the JSON body, and the
    response nests observations under ``results[].values[]``. This handler
    reads the flat ``curve_values`` corpus, filters by the value_date range,
    groups rows by ``curve_id`` into the nested batch shape, and synthesises a
    few future-dated observations on top of the corpus so the connector's
    init-time offset cap is actually exercised (mirrors the declarative
    ``synthesize_future_records:`` directive).

  * ``GET /api/v1/Metadata/Search`` — used both by the ``curve_metadata`` table
    and by ``curve_values`` curve resolution. Serves the ``curve_metadata``
    corpus paginated by MaxResults / SkipRows, echoing each curve's id as the
    real API's ``curveID`` field.

The connector deliberately ignores the exact requested CurveIDs in the batch
response (the simulator's ``curve_values`` and ``curve_metadata`` corpora are
independent, so their ids don't line up) — a believable stand-in, per the
simulator's validation philosophy.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_FUTURE_RECORDS = 3


def curve_values_batch(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    body = _parse_body(prep.body)
    scenario_id = body.get("scenarioID", 0)
    lo = body.get("minValueDate")
    hi = body.get("maxValueDate")

    records = corpus.get("curve_values") or []
    if not isinstance(records, list):
        records = []

    # Append future-dated observations so an uncapped connector would leak them
    # (its ``maxValueDate`` cap must exclude them for termination tests to
    # converge).
    records = _augment_with_future(records)

    filtered = [r for r in records if _value_date_in_range(r, lo, hi)]

    # Group flat rows by curve_id into the nested batch response shape.
    grouped: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in filtered:
        cid = str(row.get("curve_id"))
        if cid not in grouped:
            grouped[cid] = {
                "curveID": row.get("curve_id"),
                "scenarioID": row.get("scenario_id", scenario_id),
                "status": "Success",
                "values": [],
            }
            order.append(cid)
        grouped[cid]["values"].append(
            {
                "forecastDate": row.get("forecast_date"),
                "valueDate": row.get("value_date"),
                "value": row.get("value"),
                "lastUpdateTime": row.get("last_update_time"),
            }
        )

    payload = {"results": [grouped[cid] for cid in order]}
    return _build_response(prep, status=200, payload=payload)


def metadata_search(
    prep: PreparedRequest, spec: Any, corpus: Any
) -> Response:  # noqa: ARG001
    query = _query_params(prep)
    max_results = _to_int(query.get("MaxResults"), 1000)
    skip_rows = _to_int(query.get("SkipRows"), 0)

    records = corpus.get("curve_metadata") or []
    if not isinstance(records, list):
        records = []

    page = records[skip_rows : skip_rows + max_results]
    results = [
        {
            "curveID": rec.get("curve_id"),
            "alias": rec.get("alias"),
            "name": rec.get("name"),
            "metadata_json": rec.get("metadata_json"),
        }
        for rec in page
    ]
    payload = {"results": results, "totalCount": len(records)}
    return _build_response(prep, status=200, payload=payload)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_body(body: Any) -> dict[str, Any]:
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


def _query_params(prep: PreparedRequest) -> dict[str, str]:
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(prep.url or "")
    return {k: v[0] for k, v in parse_qs(parsed.query).items() if v}


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _value_date_in_range(record: dict[str, Any], lo: Any, hi: Any) -> bool:
    """Lenient value_date range check.

    Records whose value_date can't be parsed are kept (the synthetic corpus may
    mangle a timestamp that is simultaneously a primary key). The synthesised
    future records always carry a clean, parseable, out-of-range value_date, so
    the cap-exclusion behaviour is still exercised.
    """
    ts = _parse_ts(record.get("value_date"))
    if ts is None:
        return True
    lo_dt = _parse_ts(lo)
    hi_dt = _parse_ts(hi)
    if lo_dt is not None and ts < lo_dt:
        return False
    if hi_dt is not None and ts > hi_dt:
        return False
    return True


def _augment_with_future(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not records:
        return records
    base = datetime.now(timezone.utc) + timedelta(days=365)
    template = records[-1]
    future = []
    for i in range(_FUTURE_RECORDS):
        clone = copy.deepcopy(template)
        clone["curve_id"] = f"{clone.get('curve_id', 'future')}::future-{i}"
        ts = (base + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        clone["value_date"] = ts
        clone["last_update_time"] = ts
        future.append(clone)
    return list(records) + future


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
