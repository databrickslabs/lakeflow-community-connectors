"""Offline unit tests for Adobe Analytics helpers that the simulator suite cannot
reach: itemId parsing, column naming, option errors, stale-row deletes, the
includeType fallback and 429 retries."""

import json
import logging
from datetime import date, timedelta, timezone
from unittest import mock

import pytest
import requests

from databricks.labs.community_connector.sources.adobe_analytics import adobe_analytics_client
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics import (
    AdobeAnalyticsLakeflowConnect,
)
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_client import (
    MAX_ATTEMPTS,
    MAX_INVALID_JSON_ATTEMPTS,
    MAX_WAIT_SECONDS,
    AdobeAnalyticsAPIError,
    AdobeAnalyticsClient,
    AdobeAnalyticsInvalidResponseError,
    describe_body_size,
)
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics_reports import (
    FALLBACK_PAGE_LIMIT,
    MAX_PAGE_LIMIT,
    TRENDED_PAD_DAYS,
    metric_column_names,
    parse_report_config,
    parse_time_item_id,
    sanitize_column_name,
)

FAKE_OPTIONS = {
    "client_id": "example-client-id",
    "client_secret": "example-client-secret",
    "global_company_id": "examplecompany",
}
RSID = "examplersid"


@pytest.fixture(autouse=True)
def _block_network():
    """These tests mock every HTTP call; fail loudly if one slips through."""

    def refuse(method, url, **_kwargs):
        raise AssertionError(f"Unexpected real HTTP call: {method} {url}")

    with mock.patch.object(adobe_analytics_client.requests, "request", side_effect=refuse):
        yield


def _connector() -> AdobeAnalyticsLakeflowConnect:
    connector = AdobeAnalyticsLakeflowConnect(FAKE_OPTIONS)
    # Skip the report-suite time zone lookup; every test here runs in UTC.
    connector._timezones[RSID] = timezone.utc
    return connector


# ---------------------------------------------------------------------------
# parse_time_item_id
# ---------------------------------------------------------------------------


def _item_id(day: date, hour: int | None = None) -> str:
    """Encode a date the way Adobe does: century, YY, zero-based MM, DD, optional HH."""
    text = f"{(day.year - 1900) // 100}{day:%y}{day.month - 1:02d}{day.day:02d}"
    return text if hour is None else f"{text}{hour:02d}"


# Constructed itemId / label pairs in Adobe's encoding. Month is zero-based.
@pytest.mark.parametrize(
    "item_id, granularity, expected",
    [
        ("1250114", "day", (date(2025, 2, 14), None)),  # labelled "Feb 14, 2025"
        ("125011409", "hour", (date(2025, 2, 14), 9)),  # labelled "09:00 2025-02-14"
        ("1250601", "month", (date(2025, 7, 1), None)),  # labelled "Jul 2025"
        ("1250807", "week", (date(2025, 9, 7), None)),  # labelled "Sep 7, 2025"
    ],
)
def test_parse_time_item_id_matches_labels(item_id, granularity, expected):
    assert parse_time_item_id(item_id, granularity) == expected


@pytest.mark.parametrize(
    "item_id, granularity, expected",
    [
        ("0991131", "day", (date(1999, 12, 31), None)),
        (1240129, "day", (date(2024, 2, 29), None)),
        ("1260000", "month", (date(2026, 1, 1), None)),
        ("12600", "month", (date(2026, 1, 1), None)),
        ("125011423", "hour", (date(2025, 2, 14), 23)),
    ],
)
def test_parse_time_item_id(item_id, granularity, expected):
    assert parse_time_item_id(item_id, granularity) == expected


@pytest.mark.parametrize(
    "item_id, granularity, expected",
    [
        ("1251131", "day", (date(2025, 12, 31), None)),
        ("1260001", "day", (date(2026, 1, 1), None)),
        ("125113123", "hour", (date(2025, 12, 31), 23)),
        ("126000100", "hour", (date(2026, 1, 1), 0)),
        ("1251101", "month", (date(2025, 12, 1), None)),
        ("1260001", "month", (date(2026, 1, 1), None)),
        # Week of Dec 28, 2025 to Jan 3, 2026 is keyed by its start date.
        ("1251128", "week", (date(2025, 12, 28), None)),
    ],
)
def test_parse_time_item_id_across_year_boundary(item_id, granularity, expected):
    assert parse_time_item_id(item_id, granularity) == expected


def test_item_id_round_trip_over_a_year_end():
    day = date(2025, 12, 25)
    while day <= date(2026, 1, 7):
        assert parse_time_item_id(_item_id(day), "day") == (day, None)
        assert parse_time_item_id(_item_id(day, 23), "hour") == (day, 23)
        day += timedelta(days=1)


@pytest.mark.parametrize(
    "item_id, granularity, message",
    [
        (None, "day", "expected 7 digits"),
        ("Nov 6, 2026", "day", "expected 7 digits"),
        ("126110613", "day", "expected 7 digits"),
        ("1261106", "hour", "expected 9 digits"),
        ("12611", "week", "expected 7 digits"),
        ("1261201", "day", "valid date"),  # zero-based month 12 does not exist
        ("1261031", "day", "valid date"),  # Nov 31
        ("1230129", "week", "valid date"),  # Feb 29, 2023
        ("126110624", "hour", "valid hour"),
    ],
)
def test_parse_time_item_id_rejects_bad_input(item_id, granularity, message):
    with pytest.raises(ValueError, match=message):
        parse_time_item_id(item_id, granularity)


# ---------------------------------------------------------------------------
# Metric id -> column name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "metric_id, expected",
    [
        ("metrics/visits", "visits"),
        ("metrics/pageviews", "pageviews"),
        ("metrics/averagetimespentonsite", "averagetimespentonsite"),
        ("metrics/bounceRate", "bounce_rate"),
        ("metrics/event1", "event1"),
        ("metrics/3rdPartyViews", "m_3rd_party_views"),
        ("metrics/page-views.total", "page_views_total"),
        ("cm000000000_000000000000000000000001", "cm000000000_000000000000000000000001"),
        ("metrics/!!!", "metric"),
    ],
)
def test_sanitize_column_name(metric_id, expected):
    assert sanitize_column_name(metric_id) == expected


def test_metric_column_names_resolve_clashes():
    names = metric_column_names(
        ["metrics/visits", "other/visits", "metrics/date", "metrics/rsid", "metrics/visits_2"]
    )
    assert names == ["visits", "visits_2", "date_2", "rsid_2", "visits_2_2"]


# ---------------------------------------------------------------------------
# Option validation
# ---------------------------------------------------------------------------

_BASE = {"rsid": RSID, "metrics": "metrics/visits"}


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"rsid": ""}, "needs the table option 'rsid'"),
        ({"metrics": ""}, "needs the table option 'metrics'"),
        ({"metrics": "[]"}, "needs the table option 'metrics'"),
        ({"metrics": "[not json"}, "as a JSON list"),
        ({"metrics": "metrics/visits,metrics/visits"}, "more than once"),
        ({"metrics": "visits"}, "not in the full form"),
        ({"dimension": "variables/daterangeday"}, "is a time dimension"),
        ({"dimension": "evar1"}, "variables/evar1"),
        ({"granularity": "minute"}, "'granularity' must be one of"),
        ({"granularity": "week", "dimension": "variables/evar1"}, "not supported yet"),
        ({"granularity": "month", "dimension": "variables/page"}, "not supported yet"),
        ({"start_date": "11/06/2026"}, "YYYY-MM-DD"),
        ({"nones_behavior": "maybe"}, "'nones_behavior' must be"),
        ({"limit": "50001"}, "at most 50000"),
        ({"limit": "0"}, "at least 1"),
        ({"lookback_days": "three"}, "whole number"),
        ({"lookback_days": "-1"}, "at least 0"),
        ({"max_rows_per_bucket": "0"}, "at least 1"),
        ({"count_repeat_instances": "sometimes"}, "true or false"),
        ({"delete_stale_rows": "maybe"}, "true or false"),
    ],
)
def test_parse_report_config_errors(overrides, message):
    with pytest.raises(ValueError, match=message):
        parse_report_config("example_report", {**_BASE, **overrides})


def test_parse_report_config_defaults():
    cfg = parse_report_config("example_report", {**_BASE, "segment_ids": "s2, s1"})
    assert cfg.granularity == "day"
    assert cfg.lookback_days == 3
    assert cfg.limit == 50_000
    assert cfg.nones_behavior == "return-nones"
    assert cfg.count_repeat_instances is True
    assert cfg.segment_key == "s1,s2"
    assert cfg.tracks_deletes is False  # no dimension


def test_connector_rejects_missing_credentials():
    with pytest.raises(ValueError, match="missing client_id, client_secret"):
        AdobeAnalyticsLakeflowConnect({})


def test_connector_rejects_unknown_table_without_report_options():
    connector = _connector()
    with pytest.raises(ValueError, match="not a built-in table"):
        connector.read_table_metadata("daily_traffic", {})


def test_read_table_deletes_rejects_tables_without_deletes():
    connector = _connector()
    with pytest.raises(ValueError, match="does not produce deletes"):
        connector.read_table_deletes("daily_traffic", {}, _BASE)


# ---------------------------------------------------------------------------
# Stale-row delete flow
# ---------------------------------------------------------------------------


def _ranked_body(values: list[str]) -> dict:
    return {
        "columns": {"columnIds": ["0"]},
        "rows": [
            {"itemId": str(1000 + i), "value": v, "data": [float(i + 1)]}
            for i, v in enumerate(values)
        ],
        "lastPage": True,
    }


def _fake_reports(values_by_day: dict[str, list[str]]):
    """Fake ``client.post``: return ranked rows chosen by the request's start day."""

    def post(path, body):
        assert path == "/reports"
        date_range = body["globalFilters"][0]["dateRange"]
        return _ranked_body(values_by_day.get(date_range[:10], []))

    return post


def test_value_missing_on_reread_produces_a_delete():
    opts = {
        **_BASE,
        "dimension": "variables/evar1",
        "lookback_days": "1",
        "segment_ids": "s000000000_000000000000000000000001",
    }

    run_1 = _connector()
    today = run_1._init_utc.date()
    yesterday = today - timedelta(days=1)
    opts["start_date"] = (today - timedelta(days=5)).isoformat()
    all_values = ["value-a", "value-b", "value-c"]
    run_1._client = mock.Mock()
    run_1._client.post.side_effect = _fake_reports(
        {yesterday.isoformat(): all_values, today.isoformat(): all_values}
    )
    deletes, offset_1 = run_1.read_table_deletes("example_report", {}, opts)
    assert list(deletes) == []  # nothing remembered yet
    assert offset_1["keys"] == {yesterday.isoformat(): all_values, today.isoformat(): all_values}

    # Same instance, same run: no new requests, offset unchanged.
    calls = run_1._client.post.call_count
    again, same_offset = run_1.read_table_deletes("example_report", offset_1, opts)
    assert list(again) == [] and same_offset == offset_1
    assert run_1._client.post.call_count == calls

    # Next run: value-b vanished from yesterday.
    run_2 = _connector()
    run_2._run_id = run_1._run_id + "-next"
    run_2._client = mock.Mock()
    run_2._client.post.side_effect = _fake_reports(
        {yesterday.isoformat(): ["value-a", "value-c"], today.isoformat(): all_values}
    )
    deletes, offset_2 = run_2.read_table_deletes(
        "example_report", json.loads(json.dumps(offset_1)), opts
    )
    deletes = list(deletes)
    assert len(deletes) == 1
    assert deletes[0]["date"] == yesterday.isoformat()
    assert deletes[0]["dimension_value"] == "value-b"
    assert deletes[0]["rsid"] == RSID
    assert deletes[0]["segment_ids"] == "s000000000_000000000000000000000001"
    assert offset_2["keys"][yesterday.isoformat()] == ["value-a", "value-c"]
    assert offset_2["run"] == run_2._run_id


def test_max_tracked_keys_limits_remembered_buckets(caplog):
    opts = {
        **_BASE,
        "dimension": "variables/evar1",
        "lookback_days": "2",
        "max_tracked_keys": "4",
    }
    connector = _connector()
    today = connector._init_utc.date()
    opts["start_date"] = (today - timedelta(days=10)).isoformat()
    values = ["value-a", "value-b", "value-c"]
    connector._client = mock.Mock()
    connector._client.post.side_effect = lambda path, body: _ranked_body(values)
    with caplog.at_level(logging.WARNING):
        _, offset = connector.read_table_deletes("example_report", {}, opts)
    # Only the newest bucket fits under the cap of 4 values.
    assert list(offset["keys"]) == [today.isoformat()]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "max_tracked_keys=4 was reached" in w and "will NOT be deleted" in w for w in warnings
    )


# ---------------------------------------------------------------------------
# Read window and partial buckets
# ---------------------------------------------------------------------------


def test_read_start_is_last_date_minus_lookback():
    cfg = parse_report_config("example_report", {**_BASE, "lookback_days": "3"})
    today = date(2026, 11, 9)
    start_date = date(2026, 1, 1)
    last_date = date(2026, 11, 8)
    # The offset holds only the date cursor; retrieved_at plays no part.
    offset = {"cursor": last_date.isoformat(), "run": "previous-run"}
    window = AdobeAnalyticsLakeflowConnect._window_start(cfg, offset, last_date, start_date, today)
    assert window == date(2026, 11, 5)
    # Never earlier than start_date.
    early = AdobeAnalyticsLakeflowConnect._window_start(
        cfg, offset, date(2026, 1, 2), start_date, today
    )
    assert early == start_date


def test_partial_week_bucket_is_dropped_and_logged(caplog):
    cfg = parse_report_config("example_report", {**_BASE, "granularity": "week"})
    connector = _connector()
    window_from = date(2026, 11, 4)
    request_from = window_from - timedelta(days=TRENDED_PAD_DAYS["week"])  # 2026-10-28
    partial_start = date(2026, 10, 26)  # week that began before the request
    full_start = date(2026, 11, 2)

    def week_row(day: date, value: float) -> dict:
        return {"itemId": _item_id(day), "value": day.isoformat(), "data": [value]}

    connector._client = mock.Mock()
    connector._client.post.return_value = {
        "columns": {"dimension": {"id": "variables/daterangeweek"}, "columnIds": ["0"]},
        "rows": [week_row(partial_start, 1.0), week_row(full_start, 2.0)],
        "lastPage": True,
    }
    with caplog.at_level(logging.INFO):
        records, _cursor, _done = connector._read_trended_window(
            cfg, window_from, date(2026, 11, 9), date(2026, 1, 1), None
        )
    assert [r["date"] for r in records] == [full_start.isoformat()]
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any(
        f"dropped partial week bucket starting {partial_start}" in m
        and f"request began on {request_from}" in m
        for m in messages
    )


def test_rows_outside_requested_dates_are_skipped_and_logged(caplog):
    cfg = parse_report_config("example_report", {**_BASE, "granularity": "day"})
    connector = _connector()
    before, first, last, after = (
        date(2026, 11, 9),
        date(2026, 11, 10),
        date(2026, 11, 11),
        date(2026, 11, 12),
    )
    connector._client = mock.Mock()
    connector._client.post.return_value = {
        "columns": {"dimension": {"id": "variables/daterangeday"}, "columnIds": ["0"]},
        "rows": [
            {"itemId": _item_id(d), "value": "secret-label", "data": [float(i)]}
            for i, d in enumerate((before, first, last, after))
        ],
        "lastPage": True,
    }
    with caplog.at_level(logging.WARNING):
        records, _cursor, _done = connector._read_trended_window(
            cfg, first, last, date(2026, 1, 1), None
        )
    assert [r["date"] for r in records] == [first.isoformat(), last.isoformat()]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "skipped 2 row(s) dated 2026-11-09, 2026-11-12" in warnings[0]
    assert "2026-11-10 to 2026-11-11" in warnings[0]
    # Only the count and dates are logged: no itemId, label or metric value.
    assert "secret-label" not in warnings[0] and _item_id(before) not in warnings[0]


def test_week_spanning_new_year_is_kept_under_its_start_date():
    cfg = parse_report_config("example_report", {**_BASE, "granularity": "week"})
    connector = _connector()
    spanning, following = date(2025, 12, 28), date(2026, 1, 4)
    connector._client = mock.Mock()
    connector._client.post.return_value = {
        "columns": {"dimension": {"id": "variables/daterangeweek"}, "columnIds": ["0"]},
        "rows": [
            {"itemId": _item_id(spanning), "value": "Dec 28, 2025", "data": [1.0]},
            {"itemId": _item_id(following), "value": "Jan 4, 2026", "data": [2.0]},
        ],
        "lastPage": True,
    }
    # Request starts Dec 25 (Jan 1 minus the 7-day pad), so the Dec 28 week is complete.
    records, _cursor, _done = connector._read_trended_window(
        cfg, date(2026, 1, 1), date(2026, 1, 11), date(2025, 1, 1), None
    )
    assert [r["date"] for r in records] == [spanning.isoformat(), following.isoformat()]


# ---------------------------------------------------------------------------
# includeType fallback
# ---------------------------------------------------------------------------


def _segments_client(refuse: set):
    client = mock.Mock()

    def get_paged(path, params):
        if params.get("includeType") in refuse:
            raise AdobeAnalyticsAPIError(403, "GET", path, "insufficient_access")
        return [{"id": "s000000000_000000000000000000000001", "name": "Example segment"}]

    client.get_paged.side_effect = get_paged
    return client


def test_include_type_all_falls_back_to_shared_on_403():
    connector = _connector()
    connector._client = _segments_client(refuse={"all"})
    records = list(connector.read_table("segments", {}, {})[0])
    assert [r["id"] for r in records] == ["s000000000_000000000000000000000001"]
    include_types = [
        c.args[1].get("includeType") for c in connector._client.get_paged.call_args_list
    ]
    assert include_types == ["all", "shared"]


def test_include_type_falls_back_to_owned_when_shared_also_refused():
    connector = _connector()
    connector._client = _segments_client(refuse={"all", "shared"})
    records = list(connector.read_table("calculated_metrics", {}, {})[0])
    assert len(records) == 1
    include_types = [
        c.args[1].get("includeType") for c in connector._client.get_paged.call_args_list
    ]
    assert include_types == ["all", "shared", None]


def test_explicit_include_type_does_not_fall_back():
    connector = _connector()
    connector._client = _segments_client(refuse={"all"})
    with pytest.raises(AdobeAnalyticsAPIError, match="HTTP 403"):
        list(connector.read_table("segments", {}, {"include_type": "all"})[0])
    assert connector._client.get_paged.call_count == 1


def test_include_type_none_sends_no_include_type():
    connector = _connector()
    connector._client = _segments_client(refuse=set())
    list(connector.read_table("date_ranges", {}, {"include_type": "none"})[0])
    params = connector._client.get_paged.call_args.args[1]
    assert "includeType" not in params


# ---------------------------------------------------------------------------
# 429 retries
# ---------------------------------------------------------------------------


def _response(status: int, body=None, headers=None) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status
    resp.headers.update(headers or {})
    resp._content = json.dumps(body if body is not None else {}).encode()
    return resp


@pytest.fixture
def no_wait():
    """Remove jitter and real sleeping from the client."""
    with (
        mock.patch.object(adobe_analytics_client.time, "sleep") as sleep,
        mock.patch.object(adobe_analytics_client.random, "uniform", return_value=0.0),
    ):
        yield sleep


def test_429_honors_retry_after(no_wait):
    client = AdobeAnalyticsClient("example-client-id", "example-client-secret")
    responses = [_response(429, headers={"Retry-After": "7"}), _response(200, {"ok": True})]
    with mock.patch.object(
        adobe_analytics_client.requests, "request", side_effect=responses
    ) as req:
        resp = client._send("GET", "https://analytics.adobe.io/api/examplecompany/segments")
    assert resp.status_code == 200
    assert req.call_count == 2
    no_wait.assert_called_once_with(7.0)


def test_429_without_retry_after_uses_backoff(no_wait):
    client = AdobeAnalyticsClient("example-client-id", "example-client-secret")
    responses = [_response(429), _response(429, headers={"Retry-After": "soon"}), _response(200)]
    with mock.patch.object(adobe_analytics_client.requests, "request", side_effect=responses):
        client._send("GET", "https://analytics.adobe.io/api/examplecompany/segments")
    assert [c.args[0] for c in no_wait.call_args_list] == [2.0, 4.0]


def test_429_retry_after_is_capped(no_wait):
    client = AdobeAnalyticsClient("example-client-id", "example-client-secret")
    responses = [_response(429, headers={"Retry-After": "3600"}), _response(200)]
    with mock.patch.object(adobe_analytics_client.requests, "request", side_effect=responses):
        client._send("GET", "https://analytics.adobe.io/api/examplecompany/segments")
    no_wait.assert_called_once_with(MAX_WAIT_SECONDS)


def test_429_gives_up_after_max_attempts(no_wait):
    client = AdobeAnalyticsClient(
        "example-client-id", "example-client-secret", global_company_id="examplecompany"
    )
    client._token, client._token_expires_at = "example-token", float("inf")
    throttled = [_response(429, headers={"Retry-After": "1"}) for _ in range(MAX_ATTEMPTS)]
    with mock.patch.object(
        adobe_analytics_client.requests, "request", side_effect=throttled
    ) as req:
        with pytest.raises(AdobeAnalyticsAPIError, match="HTTP 429"):
            client.get("/segments")
    assert req.call_count == MAX_ATTEMPTS
    assert no_wait.call_count == MAX_ATTEMPTS - 1


# ---------------------------------------------------------------------------
# Successful status with a body that isn't JSON
# ---------------------------------------------------------------------------


def _not_json(status: int = 200) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status
    resp.headers["Content-Type"] = "application/json"
    resp._content = b'{"rows": [{"value": "secret-dimension-value", "data": [1'
    return resp


def _ready_client() -> AdobeAnalyticsClient:
    client = AdobeAnalyticsClient(
        "example-client-id", "example-client-secret", global_company_id="examplecompany"
    )
    client._token, client._token_expires_at = "example-token", float("inf")
    return client


def test_non_json_success_is_retried(no_wait, caplog):
    client = _ready_client()
    responses = [_not_json(), _response(200, {"rows": []})]
    with caplog.at_level(logging.WARNING):
        with mock.patch.object(
            adobe_analytics_client.requests, "request", side_effect=responses
        ) as req:
            assert client.post("/reports", {"rsid": RSID}) == {"rows": []}
    assert req.call_count == 2
    message = next(r.getMessage() for r in caplog.records if "not valid JSON" in r.getMessage())
    assert "HTTP 200" in message and "application/json" in message and "bytes" in message
    assert f"attempt 2 of {MAX_INVALID_JSON_ATTEMPTS}" in message
    # The body may hold report data, so it must never be logged.
    assert "secret-dimension-value" not in message


def test_non_json_success_gives_up_after_cap(no_wait):
    client = _ready_client()
    responses = [_not_json() for _ in range(MAX_INVALID_JSON_ATTEMPTS)]
    with mock.patch.object(
        adobe_analytics_client.requests, "request", side_effect=responses
    ) as req:
        with pytest.raises(AdobeAnalyticsAPIError, match="not valid JSON") as excinfo:
            client.post("/reports", {"rsid": RSID})
    assert req.call_count == MAX_INVALID_JSON_ATTEMPTS
    assert no_wait.call_count == MAX_INVALID_JSON_ATTEMPTS - 1
    assert isinstance(excinfo.value, AdobeAnalyticsInvalidResponseError)
    assert excinfo.value.status_code == 200
    assert "secret-dimension-value" not in str(excinfo.value)


def _sized(content_length: str | None, wire_bytes: int | None, encoding: str | None = None):
    resp = _not_json()
    if content_length is not None:
        resp.headers["Content-Length"] = content_length
    if encoding:
        resp.headers["Content-Encoding"] = encoding
    if wire_bytes is not None:
        resp.raw = mock.Mock()
        resp.raw.tell.return_value = wire_bytes
    return resp


def test_body_size_reports_matching_content_length():
    resp = _sized(str(len(_not_json().content)), None)
    text = describe_body_size(resp)
    assert "matches" in text and "arrived exactly as Adobe sent it" in text


def test_body_size_reports_bytes_lost_in_transit():
    text = describe_body_size(_sized("5274350", 5000000, encoding="gzip"))
    assert "Content-Length 5274350 but 5000000 bytes received" in text
    assert "lost between Adobe and this machine" in text


def test_body_size_without_content_length_says_unknown():
    text = describe_body_size(_sized(None, None))
    assert "no Content-Length header" in text
    assert "secret-dimension-value" not in text


# ---------------------------------------------------------------------------
# Report page size: row cap and fallback after cut-off pages
# ---------------------------------------------------------------------------


def _dimension_cfg(**extra):
    return parse_report_config("example_report", {**_BASE, "dimension": "variables/evar1", **extra})


def _page(limits: list, cut_off: int = 0):
    """Fake client.post that records each page's limit and fails the first cut_off calls."""

    def post(_path, body):
        limits.append(body["settings"]["limit"])
        if len(limits) <= cut_off:
            raise AdobeAnalyticsInvalidResponseError(200, "POST", "/reports", "cut off")
        return {"columns": {"columnIds": ["0"]}, "rows": [], "lastPage": True}

    return post


def test_row_cap_limits_requested_page_size():
    connector = _connector()
    limits: list = []
    connector._client = mock.Mock()
    connector._client.post.side_effect = _page(limits)
    connector._fetch_report_rows(_dimension_cfg(), "range", trended=False, row_cap=50)
    assert limits == [50]


def test_cut_off_pages_fall_back_to_smaller_pages(caplog):
    connector = _connector()
    limits: list = []
    connector._client = mock.Mock()
    connector._client.post.side_effect = _page(limits, cut_off=1)
    with caplog.at_level(logging.WARNING):
        connector._fetch_report_rows(_dimension_cfg(), "range", trended=False)
    assert limits == [MAX_PAGE_LIMIT, FALLBACK_PAGE_LIMIT]
    assert any("keep arriving cut off" in r.getMessage() for r in caplog.records)


def test_cut_off_pages_at_fallback_size_stop_the_read():
    connector = _connector()
    limits: list = []
    connector._client = mock.Mock()
    connector._client.post.side_effect = _page(limits, cut_off=2)
    with pytest.raises(AdobeAnalyticsInvalidResponseError):
        connector._fetch_report_rows(_dimension_cfg(), "range", trended=False)
    assert limits == [MAX_PAGE_LIMIT, FALLBACK_PAGE_LIMIT]
