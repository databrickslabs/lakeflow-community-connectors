"""Deterministic simulator for the CM360 asynchronous reporting lifecycle."""

from __future__ import annotations

import csv
import io
import json
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

_PROFILE = {
    "accountId": "7001",
    "accountName": "Example Global",
    "subAccountId": "7101",
    "subAccountName": "Example Media",
    "userName": "Simulator Analyst",
}
_DIMENSIONS = ["country", "advertiserId", "advertiser", "campaignId", "campaign"]
_METRICS = [
    "impressions",
    "clicks",
    "mediaCost",
    "richMediaVideoPlays",
    "richMediaVideoViews",
    "richMediaVideoMidpoints",
    "richMediaVideoCompletions",
]
_REACH_DIMENSIONS = [
    "advertiser",
    "advertiserId",
    "campaign",
    "campaignId",
    "country",
    "date",
]
_REACH_METRICS = ["uniqueReachImpressionReach"]
_CSV_HEADER = [
    "Country",
    "Advertiser ID",
    "Advertiser",
    "Campaign ID",
    "Campaign",
    "Impressions",
    "Clicks",
    "Media Cost",
    "Video Plays",
    "Video Views",
    "Video Mid-points",
    "Video Completions",
]
_CSV_KEYS = [
    "delivery_country",
    "advertiser_id",
    "advertiser_name",
    "campaign_id",
    "campaign_name",
    "impressions",
    "clicks",
    "media_cost",
    "video_starts",
    "qualified_video_views",
    "video_views_50_percent",
    "video_views_100_percent",
]
_REACH_CSV_HEADER = [
    "Advertiser",
    "Advertiser ID",
    "Campaign",
    "Campaign ID",
    "Country",
    "Date",
    "Unique Reach: Impression Reach",
]
_PLACEMENT_DIMENSIONS = [
    "advertiserId",
    "advertiser",
    "campaignId",
    "campaign",
    "placementId",
    "placement",
]
_PLACEMENT_METRICS = [*_METRICS, "dv360Cost"]
_PLACEMENT_CSV_HEADER = [
    "Advertiser ID",
    "Advertiser",
    "Campaign ID",
    "Campaign",
    "Placement ID",
    "Placement",
    "Impressions",
    "Clicks",
    "Media Cost",
    "Video Plays",
    "Video Views",
    "Video Mid-points",
    "Video Completions",
    "DV360 cost (Account currency)",
]
_PLACEMENT_INTEGER_KEYS = [
    "impressions",
    "clicks",
    "video_starts",
    "qualified_video_views",
    "video_views_50_percent",
    "video_views_100_percent",
]
_COUNTRY_DIMENSIONS = {
    "United States": "2840",
    "Germany": "2276",
    "Switzerland": "2756",
}
_PERIOD_METADATA_DIMENSIONS = [
    *_REACH_DIMENSIONS[:-1],
    "campaignStartDate",
    "campaignEndDate",
]
_PERIOD_METADATA_HEADER = [
    *_REACH_CSV_HEADER[:5],
    "Campaign Start Date",
    "Campaign End Date",
    "Impressions",
]
_PERIOD_METADATA_KEYS = [
    "advertiser_name",
    "advertiser_id",
    "campaign_name",
    "campaign_id",
    "delivery_country",
    "campaign_start_date",
    "campaign_end_date",
    "impressions",
]
_PERIOD_REACH_KEYS = [
    *_PERIOD_METADATA_KEYS[:5],
    "unique_reach_impression_reach",
]


def _response(
    prep: PreparedRequest,
    status: int,
    *,
    json_body: dict[str, Any] | None = None,
    text_body: str | None = None,
    content_type: str = "application/json; charset=UTF-8",
) -> Response:
    if json_body is not None:
        text_body = json.dumps(json_body)
    record = ResponseRecord(
        status_code=status,
        headers={"Content-Type": content_type},
        body_text=text_body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)


def _request_body(prep: PreparedRequest) -> dict:
    raw = prep.body or b""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    return json.loads(raw) if raw else {}


def _exact_report_date(body: dict) -> str:
    criteria_name = "reachCriteria" if body.get("type") == "REACH" else "criteria"
    date_range = body.get(criteria_name, {}).get("dateRange", {})
    start_date = date_range.get("startDate")
    end_date = date_range.get("endDate")
    assert start_date and start_date == end_date, "report must cover one exact one-day range"
    return start_date


def _period_kind(body: dict) -> str | None:
    if body.get("type") == "STANDARD" and body.get("criteria", {}).get("dimensions") == [
        {"name": name} for name in _PERIOD_METADATA_DIMENSIONS
    ]:
        return "metadata"
    if body.get("type") == "REACH" and body.get("reachCriteria", {}).get("dimensions") == [
        {"name": name} for name in _REACH_DIMENSIONS[:-1]
    ]:
        return "reach"
    return None


def _validate_compatible_fields_request(prep: PreparedRequest) -> None:
    body = _request_body(prep)
    period = _period_kind(body)
    if period:
        criteria = body["criteria" if period == "metadata" else "reachCriteria"]
        bounds = criteria["dateRange"]
        assert date.fromisoformat(bounds["startDate"]) <= date.fromisoformat(bounds["endDate"])
        assert criteria["metricNames"] == (
            ["impressions"] if period == "metadata" else _REACH_METRICS
        )
        assert ("reachCriteria" if period == "metadata" else "criteria") not in body
        return
    _exact_report_date(body)
    report_type = body.get("type")
    if report_type == "REACH":
        assert "criteria" not in body, "REACH reports must not use criteria"
        criteria = body.get("reachCriteria", {})
        assert criteria.get("dimensions") == [
            {"name": name} for name in _REACH_DIMENSIONS
        ], "unexpected compatible-fields reach dimensions"
        assert (
            criteria.get("metricNames") == _REACH_METRICS
        ), "unexpected compatible-fields reach metrics"
        return
    assert report_type == "STANDARD", "unexpected compatible-fields report type"
    assert "reachCriteria" not in body, "STANDARD reports must not use reachCriteria"
    criteria = body.get("criteria", {})
    if _is_placement_report(body):
        assert (
            criteria.get("metricNames") == _PLACEMENT_METRICS
        ), "unexpected compatible-fields placement metrics"
        return
    assert criteria.get("dimensions") == [
        {"name": name} for name in _DIMENSIONS
    ], "unexpected compatible-fields dimensions"
    assert criteria.get("metricNames") == _METRICS, "unexpected compatible-fields metrics"


def _is_placement_report(body: dict) -> bool:
    return body.get("type") == "STANDARD" and body.get("criteria", {}).get("dimensions") == [
        {"name": name} for name in _PLACEMENT_DIMENSIONS
    ]


def _state(corpus) -> dict:
    return corpus.tables.setdefault("_state", {})


def _log(state: dict, operation: str, prep: PreparedRequest) -> None:
    parsed = urlsplit(prep.url)
    state.setdefault("request_log", []).append(
        {
            "operation": operation,
            "method": prep.method,
            "path": parsed.path,
            "query": parse_qs(parsed.query),
        }
    )


def _report_id_from_file_path(path: str) -> str:
    return path.split("/reports/", 1)[1].split("/", 1)[0]


def _render_csv(
    corpus, report_date: str, delivery_country: str | None, advertiser_ids: list[str]
) -> str:
    records = corpus.get("ad_report_daily") or []
    matching = [
        record
        for record in records
        if record["report_date"] == report_date
        and (delivery_country is None or record["delivery_country"] == delivery_country)
        and (not advertiser_ids or record["advertiser_id"] in advertiser_ids)
    ]
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["Report Name", f"Lakeflow CM360 {report_date}"])
    writer.writerow(["Report Fields"])
    writer.writerow(_CSV_HEADER)
    for record in matching:
        writer.writerow([record[key] for key in _CSV_KEYS])
    writer.writerow(["Grand Total:"])
    return buffer.getvalue()


def _split_decimal(value: Decimal) -> tuple[Decimal, Decimal]:
    first = (value / 2).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return first, value - first


def _render_placement_csv(corpus, report_date: str, advertiser_ids: list[str]) -> str:
    """Aggregate campaign rows across countries and split each campaign into two placements.

    Placement sums therefore equal the campaign sums across countries.
    """
    campaigns: dict[tuple[str, str, str, str], dict] = {}
    for record in corpus.get("ad_report_daily") or []:
        if record["report_date"] != report_date or (
            advertiser_ids and record["advertiser_id"] not in advertiser_ids
        ):
            continue
        key = (
            record["advertiser_id"],
            record["advertiser_name"],
            record["campaign_id"],
            record["campaign_name"],
        )
        totals = campaigns.setdefault(
            key, {"media_cost": Decimal("0"), **{name: 0 for name in _PLACEMENT_INTEGER_KEYS}}
        )
        totals["media_cost"] += Decimal(record["media_cost"])
        for name in _PLACEMENT_INTEGER_KEYS:
            totals[name] += int(record[name])

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["Report Name", f"Lakeflow CM360 Placement {report_date}"])
    writer.writerow(["Report Fields"])
    writer.writerow(_PLACEMENT_CSV_HEADER)
    for (advertiser_id, advertiser_name, campaign_id, campaign_name), totals in sorted(
        campaigns.items(), key=lambda item: item[0][2]
    ):
        media_first, media_second = _split_decimal(totals["media_cost"])
        halves = {name: totals[name] // 2 for name in _PLACEMENT_INTEGER_KEYS}
        for index, (media_cost, dv360_cost) in enumerate(
            ((media_first, Decimal("0.50")), (media_second, Decimal("0.00"))), start=1
        ):
            values = {
                name: halves[name] if index == 1 else totals[name] - halves[name]
                for name in _PLACEMENT_INTEGER_KEYS
            }
            writer.writerow(
                [
                    advertiser_id,
                    advertiser_name,
                    campaign_id,
                    campaign_name,
                    str(int(campaign_id) * 100 + index),
                    f"{campaign_name} Placement {index}",
                    values["impressions"],
                    values["clicks"],
                    str(media_cost),
                    values["video_starts"],
                    values["qualified_video_views"],
                    values["video_views_50_percent"],
                    values["video_views_100_percent"],
                    str(dv360_cost),
                ]
            )
    writer.writerow(["Grand Total:"])
    return buffer.getvalue()


def _render_reach_csv(
    report_date: str, delivery_country: str | None, advertiser_ids: list[str]
) -> str:
    country = delivery_country or "United States"
    rows = [
        ["Example North America", "3001", "Summer Launch", "1001", country, report_date, "1000"],
        ["Example North America", "3001", "Brand Awareness", "1002", country, report_date, "-"],
        ["Example Europe", "3002", "Product Launch", "1003", country, report_date, ""],
    ]
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["Report Name", f"Lakeflow CM360 Reach {report_date}"])
    writer.writerow(["Report Fields"])
    writer.writerow(_REACH_CSV_HEADER)
    writer.writerows(row for row in rows if not advertiser_ids or row[1] in advertiser_ids)
    writer.writerow(["Grand Total:"])
    return buffer.getvalue()


def _render_period_csv(corpus, report: dict) -> str:
    """Serve independently specified native period reach, never a daily sum."""
    metadata = report["period_kind"] == "metadata"
    keys = _PERIOD_METADATA_KEYS if metadata else _PERIOD_REACH_KEYS
    header = (
        _PERIOD_METADATA_HEADER if metadata else [*_REACH_CSV_HEADER[:5], _REACH_CSV_HEADER[-1]]
    )
    records = [
        record
        for record in (corpus.get("campaign_period") or [])
        if record["delivery_country"] == report["delivery_country"]
        and (not report["advertiser_ids"] or record["advertiser_id"] in report["advertiser_ids"])
        and (
            report["date"] <= record["delivery_date"] <= report["end_date"]
            if metadata
            else record["campaign_id"] == report["campaign_id"]
        )
    ]
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["Report Time Zone", "UTC"])
    writer.writerow(
        [
            "Date Range",
            " - ".join(
                date.fromisoformat(report[key]).strftime("%d/%m/%Y") for key in ("date", "end_date")
            ),
        ]
    )
    writer.writerow(["Report Fields"])
    writer.writerow(header)
    writer.writerows([record[key] for key in keys] for record in records)
    writer.writerow(["Grand Total:", *["---"] * (len(header) - 1)])
    return buffer.getvalue()


def _insert_report(prep: PreparedRequest, state: dict) -> Response:
    _log(state, "insert", prep)
    body = _request_body(prep)
    report_id = "report-1"
    report_type = body.get("type")
    criteria_name = "reachCriteria" if report_type == "REACH" else "criteria"
    _validate_compatible_fields_request(prep)
    country_filters = [
        item
        for item in body.get(criteria_name, {}).get("dimensionFilters", [])
        if item.get("dimensionName") == "country"
    ]
    assert len(country_filters) <= 1, "report must contain at most one country filter"
    placement = _is_placement_report(body)
    assert not (placement and country_filters), "placement reports have no country filter"
    delivery_country = None
    if country_filters:
        country_id = country_filters[0].get("id")
        delivery_country = state.get("resolved_countries", {}).get(country_id)
        assert delivery_country is not None, "report country ID was not resolved"
        assert _COUNTRY_DIMENSIONS[delivery_country] == country_id, "country ID/name mismatch"
    state["reports"][report_id] = {
        "date": body[criteria_name]["dateRange"]["startDate"],
        "end_date": body[criteria_name]["dateRange"]["endDate"],
        "period_kind": _period_kind(body),
        "campaign_id": next(
            (
                item["id"]
                for item in body[criteria_name].get("dimensionFilters", [])
                if item.get("dimensionName") == "campaign"
            ),
            None,
        ),
        "delivery_country": delivery_country,
        "type": report_type,
        "placement": placement,
        "advertiser_ids": [
            item["id"]
            for item in body.get(criteria_name, {}).get("dimensionFilters", [])
            if item.get("dimensionName") == "advertiser"
        ],
        "poll_count": 0,
    }
    return _response(prep, 200, json_body={"id": report_id})


def _file_response(prep: PreparedRequest, corpus, state: dict, path: str, query: dict) -> Response:
    report_id = _report_id_from_file_path(path)
    report = state["reports"][report_id]
    if query.get("alt") == ["media"]:
        _log(state, "download", prep)
        if report["period_kind"]:
            csv_text = _render_period_csv(corpus, report)
            if (
                report["campaign_id"] == state.get("truncate_campaign_id")
                and report["period_kind"] == "reach"
            ):
                csv_text = csv_text.split("Grand Total:", 1)[0]
        elif report["type"] == "REACH":
            csv_text = _render_reach_csv(
                report["date"], report["delivery_country"], report["advertiser_ids"]
            )
        elif report["placement"]:
            csv_text = _render_placement_csv(corpus, report["date"], report["advertiser_ids"])
        else:
            csv_text = _render_csv(
                corpus, report["date"], report["delivery_country"], report["advertiser_ids"]
            )
        return _response(
            prep,
            200,
            text_body=csv_text,
            content_type="text/csv; charset=UTF-8",
        )
    _log(state, "poll", prep)
    report["poll_count"] += 1
    if report["poll_count"] == 1:
        return _response(prep, 200, json_body={"id": "file-1", "status": "PROCESSING"})
    return _response(
        prep,
        200,
        json_body={
            "id": "file-1",
            "status": "REPORT_AVAILABLE",
            "lastModifiedTime": report["last_modified"],
        },
    )


def handle_request(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    parsed = urlsplit(prep.url)
    path = parsed.path
    query = parse_qs(parsed.query)
    method = prep.method.upper()
    segments = path.strip("/").split("/")
    state = _state(corpus)
    reports = state.setdefault("reports", {})

    if method == "GET" and len(segments) >= 2 and segments[-2] == "userprofiles":
        _log(state, "profile", prep)
        return _response(
            prep,
            200,
            json_body={"profileId": segments[-1], **_PROFILE},
        )

    if method == "POST" and path.endswith("/dimensionvalues/query"):
        _log(state, "dimension-values", prep)
        body = _request_body(prep)
        filters = body.get("filters")
        assert query == {"maxResults": ["100"]}, "unexpected country query pagination"
        assert body.get("dimensionName") == "country", "unexpected dimension lookup"
        assert isinstance(filters, list) and len(filters) == 1, "unexpected country filter"
        country_name = filters[0].get("value")
        assert filters[0].get("dimensionName") == "country", "unexpected country filter"
        assert country_name in _COUNTRY_DIMENSIONS, "unsupported simulator country"
        assert body.get("startDate") and body.get("endDate"), "missing country query dates"
        country_id = _COUNTRY_DIMENSIONS[country_name]
        state.setdefault("resolved_countries", {})[country_id] = country_name
        return _response(
            prep,
            200,
            json_body={
                "items": [
                    {
                        "dimensionName": "country",
                        "value": country_name,
                        "id": country_id,
                        "matchType": "EXACT",
                    }
                ]
            },
        )

    if method == "POST" and path.endswith("/reports/compatiblefields/query"):
        _log(state, "compatible", prep)
        _validate_compatible_fields_request(prep)
        return _response(
            prep,
            200,
            json_body={
                "reportCompatibleFields": {
                    "dimensions": [],
                    "metrics": [],
                },
            },
        )

    if method == "POST" and path.endswith("/reports"):
        return _insert_report(prep, state)

    if method == "POST" and path.endswith("/run"):
        _log(state, "run", prep)
        report_id = path.rsplit("/", 2)[-2]
        revision = int(state.get("revision", 0)) + 1
        state["revision"] = revision
        timestamp = datetime(2026, 8, 24, 8, tzinfo=timezone.utc) + timedelta(seconds=revision)
        reports[report_id]["last_modified"] = str(int(timestamp.timestamp() * 1000))
        reports[report_id]["poll_count"] = 0
        assert query == {"synchronous": ["false"]}
        return _response(prep, 200, json_body={"id": "file-1", "status": "PROCESSING"})

    if method == "GET" and "/files/" in path:
        return _file_response(prep, corpus, state, path, query)

    if method == "DELETE" and "/reports/" in path:
        _log(state, "delete", prep)
        report_id = path.rsplit("/", 1)[-1]
        reports.pop(report_id, None)
        return _response(prep, 204, text_body="")

    return _response(prep, 404, json_body={"error": "unsupported simulator request"})
