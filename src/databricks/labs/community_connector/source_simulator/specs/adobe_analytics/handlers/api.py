"""Custom handlers for the Adobe Analytics simulator.

* ``serve_token`` — IMS client-credentials token (stub).
* ``serve_discovery`` — ``/discovery/me``. Returns exactly one placeholder
  company, so connector auto-discovery succeeds. The stand-in client id (sent
  as ``x-api-key``) selects the other scenarios: one containing
  ``no-company`` gets no companies, and one containing ``two-companies`` gets
  two companies in two IMS orgs.
* ``serve_reports`` — ``POST /api/{company}/reports``. The response shape depends
  on the posted body (dimension, metrics, dateRange, paging), which a static
  corpus cannot express. Time dimensions (``variables/daterange*``) return one
  row per bucket in the requested range with Adobe-style digit itemIds; any
  other dimension returns a few ranked rows. Every request is a single page.
"""

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_MAX_TIME_ROWS = 5000
_RANKED_VALUES = ("value-a", "value-b", "value-c")


def _json_response(status: int, body: Any, prep: PreparedRequest) -> Response:
    rec = ResponseRecord(
        status_code=status,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)


def serve_token(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    return _json_response(
        200,
        {"access_token": "simulated-access-token", "token_type": "bearer", "expires_in": 86399},
        prep,
    )


def _company(company_id: str, name: str) -> Dict[str, Any]:
    return {
        "globalCompanyId": company_id,
        "companyName": name,
        "apiRateLimitPolicy": "aa_api_tier10_tp",
    }


def serve_discovery(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    client_id = prep.headers.get("x-api-key") or ""
    if "no-company" in client_id:
        # An IMS org the credential belongs to, without any Analytics company.
        orgs = [{"imsOrgId": "SIMULATEDORG@AdobeOrg", "companies": []}]
    elif "two-companies" in client_id:
        orgs = [
            {
                "imsOrgId": "SIMULATEDORG@AdobeOrg",
                "companies": [_company("examplecompany", "Example Company")],
            },
            {
                "imsOrgId": "SIMULATEDORG2@AdobeOrg",
                "companies": [_company("examplecompany2", "Second Example Company")],
            },
        ]
    else:
        orgs = [
            {
                "imsOrgId": "SIMULATEDORG@AdobeOrg",
                "companies": [_company("examplecompany", "Example Company")],
            }
        ]
    return _json_response(200, {"imsUserId": "SIMULATEDUSER@AdobeID", "imsOrgs": orgs}, prep)


def _item_id(day: date, hour: int | None = None, month_only: bool = False) -> str:
    """Adobe's layout: century digit, YY, zero-based MM (00 = January), DD, optional HH."""
    century = (day.year - 1900) // 100
    yy = day.year % 100
    dd = 1 if month_only else day.day
    text = f"{century}{yy:02d}{day.month - 1:02d}{dd:02d}"
    return text if hour is None else f"{text}{hour:02d}"


def _day_label(day: date) -> str:
    # Adobe's English labels don't zero-pad the day: "Feb 4, 2025".
    return f"{day:%b} {day.day}, {day.year}"


def _parse_range(body: Dict[str, Any]) -> tuple[datetime, datetime]:
    for flt in body.get("globalFilters") or []:
        if flt.get("type") == "dateRange" and flt.get("dateRange"):
            start_text, end_text = flt["dateRange"].split("/", 1)
            return (
                datetime.fromisoformat(start_text[:19]),
                datetime.fromisoformat(end_text[:19]),
            )
    today = datetime.combine(date.today(), datetime.min.time())
    return today, today + timedelta(days=1)


def _time_rows(dimension: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """One row per bucket starting in [start, end); end is exclusive as in Adobe."""
    rows: List[Dict[str, Any]] = []
    if dimension.endswith("hour"):
        cur = start.replace(minute=0, second=0, microsecond=0)
        while cur < end and len(rows) < _MAX_TIME_ROWS:
            rows.append(
                {"itemId": _item_id(cur.date(), cur.hour), "value": cur.strftime("%H:00 %Y-%m-%d")}
            )
            cur += timedelta(hours=1)
    elif dimension.endswith("week"):
        # Monday-based weeks; real suites may start weeks on another day.
        cur = start.date() - timedelta(days=start.weekday())
        while datetime.combine(cur, datetime.min.time()) < end and len(rows) < _MAX_TIME_ROWS:
            rows.append({"itemId": _item_id(cur), "value": _day_label(cur)})
            cur += timedelta(days=7)
    elif dimension.endswith("month"):
        cur = start.date().replace(day=1)
        while datetime.combine(cur, datetime.min.time()) < end and len(rows) < _MAX_TIME_ROWS:
            rows.append({"itemId": _item_id(cur, month_only=True), "value": cur.strftime("%b %Y")})
            cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    else:
        cur = start.date()
        while datetime.combine(cur, datetime.min.time()) < end and len(rows) < _MAX_TIME_ROWS:
            rows.append({"itemId": _item_id(cur), "value": _day_label(cur)})
            cur += timedelta(days=1)
    return rows


def serve_reports(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    raw = prep.body or b""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    try:
        body = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return _json_response(
            400, {"errorCode": "invalid_json", "errorDescription": "Invalid JSON"}, prep
        )

    dimension = body.get("dimension") or ""
    metrics = (body.get("metricContainer") or {}).get("metrics") or []
    if not metrics:
        return _json_response(
            400, {"errorCode": "invalid_request", "errorDescription": "No metrics requested"}, prep
        )
    column_ids = [str(m.get("columnId", i)) for i, m in enumerate(metrics)]
    page = int((body.get("settings") or {}).get("page", 0) or 0)

    if page > 0:
        rows: List[Dict[str, Any]] = []
    elif dimension.startswith("variables/daterange"):
        start, end = _parse_range(body)
        rows = _time_rows(dimension, start, end)
    else:
        rows = [{"itemId": str(1000001 + i), "value": v} for i, v in enumerate(_RANKED_VALUES)]

    for r_idx, row in enumerate(rows):
        row["data"] = [float(10 * (r_idx + 1) + m_idx) for m_idx in range(len(metrics))]

    totals = [sum(r["data"][i] for r in rows) for i in range(len(metrics))]
    response = {
        "totalPages": 1,
        "firstPage": page == 0,
        "lastPage": True,
        "numberOfElements": len(rows),
        "number": page,
        "totalElements": len(rows),
        "columns": {"dimension": {"id": dimension, "type": "string"}, "columnIds": column_ids},
        "rows": rows,
        "summaryData": {"totals": totals, "filteredTotals": totals},
    }
    return _json_response(200, response, prep)
