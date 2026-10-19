"""Focused unit tests for the Google Campaign Manager 360 connector."""

import importlib.util
import json
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import yaml

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    TimestampType,
)

from databricks.labs.community_connector.source_simulator.specs.google_cm360.handlers.report_flow import (  # noqa: E501
    handle_request as handle_simulator_request,
)
from databricks.labs.community_connector.sources.google_cm360 import (
    GoogleCm360DataSource,
    GoogleCm360LakeflowConnect,
)
from databricks.labs.community_connector.sources.google_cm360.google_cm360 import (
    Cm360ApiError,
    Cm360Client,
    Cm360ConfigurationError,
    Cm360DataError,
    Cm360ReportError,
    ProfileMetadata,
    ReadState,
    RequestTelemetry,
    _build_placement_report_definition,
    _build_profile_metadata,
    _build_reach_report_definition,
    _build_report_definition,
    _next_offset,
    _parse_delivery_country_codes,
    _parse_read_options,
    _parse_report_csv,
    _poll_until_available,
    _resolve_read_state,
    _subtract_calendar_months,
)
from databricks.labs.community_connector.sources.google_cm360.google_cm360_schemas import (
    PLACEMENT_FIELD_SPECS,
    REACH_FIELD_SPECS,
)

EXPECTED_SCHEMA = [
    ("profile_id", "string", False),
    ("account_id", "string", False),
    ("account_name", "string", False),
    ("subaccount_id", "string", True),
    ("subaccount_name", "string", True),
    ("report_date", "date", False),
    ("delivery_country", "string", False),
    ("advertiser_id", "string", False),
    ("advertiser_name", "string", False),
    ("campaign_id", "string", False),
    ("campaign_name", "string", True),
    ("impressions", "bigint", True),
    ("clicks", "bigint", True),
    ("media_cost", "decimal(38,9)", True),
    ("video_starts", "bigint", True),
    ("qualified_video_views", "bigint", True),
    ("video_views_50_percent", "bigint", True),
    ("video_views_100_percent", "bigint", True),
    ("report_file_last_modified_at", "timestamp", False),
]
EXPECTED_PROFILE_METADATA = ProfileMetadata(
    account_id="7001",
    account_name="Example Global",
    subaccount_id="7101",
    subaccount_name="Example Media",
)
EXPECTED_METADATA = {
    "primary_keys": [
        "profile_id",
        "report_date",
        "delivery_country",
        "advertiser_id",
        "campaign_id",
    ],
    "cursor_field": "report_file_last_modified_at",
    "ingestion_type": "cdc",
}
EXPECTED_REACH_SCHEMA = [
    ("profile_id", "string", False),
    ("account_id", "string", False),
    ("account_name", "string", False),
    ("subaccount_id", "string", True),
    ("subaccount_name", "string", True),
    ("report_date", "date", False),
    ("delivery_country", "string", False),
    ("advertiser_id", "string", False),
    ("advertiser_name", "string", False),
    ("campaign_id", "string", False),
    ("campaign_name", "string", True),
    ("unique_reach_impression_reach", "string", True),
    ("report_file_last_modified_at", "timestamp", False),
]
EXPECTED_REACH_METADATA = {
    "primary_keys": [
        "profile_id",
        "report_date",
        "delivery_country",
        "advertiser_id",
        "campaign_id",
    ],
    "cursor_field": "report_file_last_modified_at",
    "ingestion_type": "cdc",
}
EXPECTED_PLACEMENT_SCHEMA = [
    ("profile_id", "string", False),
    ("account_id", "string", False),
    ("account_name", "string", False),
    ("subaccount_id", "string", True),
    ("subaccount_name", "string", True),
    ("report_date", "date", False),
    ("advertiser_id", "string", False),
    ("advertiser_name", "string", False),
    ("campaign_id", "string", False),
    ("campaign_name", "string", True),
    ("placement_id", "string", False),
    ("placement_name", "string", True),
    ("impressions", "bigint", True),
    ("clicks", "bigint", True),
    ("media_cost", "decimal(38,9)", True),
    ("dv360_cost", "decimal(38,9)", True),
    ("video_starts", "bigint", True),
    ("qualified_video_views", "bigint", True),
    ("video_views_50_percent", "bigint", True),
    ("video_views_100_percent", "bigint", True),
    ("report_file_last_modified_at", "timestamp", False),
]
EXPECTED_PLACEMENT_METADATA = {
    "primary_keys": [
        "profile_id",
        "report_date",
        "advertiser_id",
        "campaign_id",
        "placement_id",
    ],
    "cursor_field": "report_file_last_modified_at",
    "ingestion_type": "cdc",
}
EXPECTED_TABLES = [
    "ad_report_daily",
    "reach_report_daily",
    "ad_report_placement_daily",
    "reach_report_period",
]

REPOSITORY_ROOT = Path(__file__).parents[4]
SOURCE_DIR = REPOSITORY_ROOT / "src/databricks/labs/community_connector/sources/google_cm360"


def _connector() -> GoogleCm360LakeflowConnect:
    return GoogleCm360LakeflowConnect({"access_token": "test-token", "profile_id": "123"})


def _schema_signature(schema):
    return [(field.name, field.dataType.simpleString(), field.nullable) for field in schema.fields]


def test_contract_is_fixed():
    connector = _connector()

    assert connector.list_tables() == EXPECTED_TABLES
    assert _schema_signature(connector.get_table_schema("ad_report_daily", {})) == EXPECTED_SCHEMA
    assert connector.read_table_metadata("ad_report_daily", {}) == EXPECTED_METADATA
    assert (
        _schema_signature(connector.get_table_schema("reach_report_daily", {}))
        == EXPECTED_REACH_SCHEMA
    )
    assert connector.read_table_metadata("reach_report_daily", {}) == EXPECTED_REACH_METADATA
    assert (
        _schema_signature(connector.get_table_schema("ad_report_placement_daily", {}))
        == EXPECTED_PLACEMENT_SCHEMA
    )
    assert (
        connector.read_table_metadata("ad_report_placement_daily", {})
        == EXPECTED_PLACEMENT_METADATA
    )


def test_reach_report_definition_has_exact_daily_contract():
    assert _build_reach_report_definition(date(2025, 1, 1), (), "2840") == {
        "name": "lakeflow-google-cm360-reach-2025-01-01",
        "type": "REACH",
        "format": "CSV",
        "reachCriteria": {
            "dateRange": {"startDate": "2025-01-01", "endDate": "2025-01-01"},
            "dimensions": [
                {"name": "advertiser"},
                {"name": "advertiserId"},
                {"name": "campaign"},
                {"name": "campaignId"},
                {"name": "country"},
                {"name": "date"},
            ],
            "metricNames": ["uniqueReachImpressionReach"],
            "dimensionFilters": [{"dimensionName": "country", "id": "2840"}],
        },
    }


def test_table_metadata_cannot_mutate_the_global_contract():
    metadata = _connector().read_table_metadata("ad_report_daily", {})

    metadata["primary_keys"].clear()

    assert _connector().read_table_metadata("ad_report_daily", {}) == EXPECTED_METADATA


def test_generated_connector_accepts_and_lists_ad_report_daily():
    generated_path = SOURCE_DIR / "_generated_google_cm360_python_source.py"
    module_spec = importlib.util.spec_from_file_location(
        "generated_google_cm360_python_source", generated_path
    )
    assert module_spec is not None
    assert module_spec.loader is not None
    generated_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(generated_module)

    class DataSourceRegistry:
        registered_class = None

        def register(self, data_source_class):
            self.registered_class = data_source_class

    class Spark:
        dataSource = DataSourceRegistry()

    spark = Spark()
    generated_module.register_lakeflow_source(spark)
    assert spark.dataSource.registered_class is not None

    options = {
        "tableName": "ad_report_daily",
        "access_token": "test-token",
        "profile_id": "123",
        "sync_start_date": "2026-08-22",
    }
    package_source = GoogleCm360DataSource(options)
    generated_source = spark.dataSource.registered_class(options)

    class ParityClient:
        def get_profile(self):
            return {
                "profileId": "123",
                "accountId": "7001",
                "accountName": "Example Global",
                "subAccountId": "7101",
                "subAccountName": "Example Media",
            }

        def validate_compatible_fields(self, _report):
            return None

        def insert_report(self, _report):
            return "report-1"

        def run_report(self, report_id):
            assert report_id == "report-1"
            return "file-1"

        def get_file(self, report_id, file_id):
            assert (report_id, file_id) == ("report-1", "file-1")
            return {"status": "REPORT_AVAILABLE", "lastModifiedTime": "1787558400000"}

        def download_file(self, report_id, file_id):
            assert (report_id, file_id) == ("report-1", "file-1")
            return "\n".join(
                [
                    "Report Name,Lakeflow CM360",
                    "Report Fields",
                    (
                        "Country,Advertiser ID,Advertiser,Campaign ID,Campaign,"
                        "Impressions,Clicks,Media Cost,Video Plays,Video Views,"
                        "Video Mid-points,Video Completions"
                    ),
                    (
                        "United States,3001,Example North America,1001,Summer,"
                        "1000,25,123.45,800,600,500,400"
                    ),
                    "Grand Total:",
                ]
            )

        def delete_report(self, report_id):
            assert report_id == "report-1"

    sources = (package_source, generated_source)
    assert [source.lakeflow_connect.list_tables() for source in sources] == [
        EXPECTED_TABLES,
        EXPECTED_TABLES,
    ]
    assert [
        _schema_signature(source.lakeflow_connect.get_table_schema("ad_report_placement_daily", {}))
        for source in sources
    ] == [EXPECTED_PLACEMENT_SCHEMA, EXPECTED_PLACEMENT_SCHEMA]
    assert [
        source.lakeflow_connect.read_table_metadata("ad_report_placement_daily", {})
        for source in sources
    ] == [EXPECTED_PLACEMENT_METADATA, EXPECTED_PLACEMENT_METADATA]
    assert [_schema_signature(source.schema()) for source in sources] == [
        EXPECTED_SCHEMA,
        EXPECTED_SCHEMA,
    ]
    assert [
        source.lakeflow_connect.read_table_metadata("ad_report_daily", {}) for source in sources
    ] == [EXPECTED_METADATA, EXPECTED_METADATA]

    results = []
    for source in sources:
        connector = source.lakeflow_connect
        connector._upper_bounds["ad_report_daily"] = date(2026, 8, 22)
        connector._client_instance = ParityClient()
        rows, offset = connector.read_table(
            "ad_report_daily", None, {"sync_start_date": "2026-08-22"}
        )
        results.append((list(rows), offset))

    assert results[0] == results[1]
    assert set(results[0][0][0]) == {name for name, _, _ in EXPECTED_SCHEMA}
    assert results[0][1] == {"high_watermark": "2026-08-22"}


def test_schema_uses_explicit_wide_types():
    schema = _connector().get_table_schema("ad_report_daily", {})
    types_by_name = {field.name: field.dataType for field in schema.fields}

    assert types_by_name == {
        "profile_id": StringType(),
        "account_id": StringType(),
        "account_name": StringType(),
        "subaccount_id": StringType(),
        "subaccount_name": StringType(),
        "report_date": DateType(),
        "delivery_country": StringType(),
        "advertiser_id": StringType(),
        "advertiser_name": StringType(),
        "campaign_id": StringType(),
        "campaign_name": StringType(),
        "impressions": LongType(),
        "clicks": LongType(),
        "media_cost": DecimalType(38, 9),
        "video_starts": LongType(),
        "qualified_video_views": LongType(),
        "video_views_50_percent": LongType(),
        "video_views_100_percent": LongType(),
        "report_file_last_modified_at": TimestampType(),
    }


def test_datasource_binds_connector_class():
    assert GoogleCm360DataSource._lakeflow_connect_cls is GoogleCm360LakeflowConnect


def test_default_initial_state_is_twelve_calendar_months():
    opts = _parse_read_options({})

    assert _resolve_read_state(None, opts, date(2026, 8, 23)) == ReadState(
        high_watermark=None,
        next_date=date(2025, 8, 23),
        upper_bound=date(2026, 8, 23),
        initial_start=date(2025, 8, 23),
    )


def test_empty_offset_is_also_a_first_load():
    opts = _parse_read_options({})

    assert _resolve_read_state({}, opts, date(2026, 8, 23)) == _resolve_read_state(
        None, opts, date(2026, 8, 23)
    )


def test_explicit_start_overrides_month_lookback():
    opts = _parse_read_options({"sync_start_date": "2026-01-15", "initial_lookback_months": "3"})

    state = _resolve_read_state({}, opts, date(2026, 8, 23))

    assert state.next_date == date(2026, 1, 15)
    assert state.initial_start == date(2026, 1, 15)


def test_completed_offset_replays_thirty_days_once():
    opts = _parse_read_options({})

    state = _resolve_read_state({"high_watermark": "2026-08-20"}, opts, date(2026, 8, 23))

    assert state.next_date == date(2026, 7, 22)
    assert state.high_watermark == date(2026, 8, 20)


def test_completed_offset_replay_does_not_precede_initial_start():
    opts = _parse_read_options({"sync_start_date": "2026-08-01"})

    state = _resolve_read_state({"high_watermark": "2026-08-20"}, opts, date(2026, 8, 23))

    assert state.next_date == date(2026, 8, 1)


def test_in_progress_offset_resumes_without_reapplying_lookback():
    offset = {
        "high_watermark": "2026-08-20",
        "next_date": "2026-08-01",
        "upper_bound": "2026-08-23",
        "initial_start": "2025-08-23",
    }

    state = _resolve_read_state(offset, _parse_read_options({}), date(2026, 9, 1))

    assert state.next_date == date(2026, 8, 1)
    assert state.upper_bound == date(2026, 8, 23)


def test_in_progress_offset_extends_when_sync_end_date_increases():
    offset = {
        "high_watermark": None,
        "next_date": "2026-09-10",
        "upper_bound": "2026-09-13",
        "initial_start": "2025-01-01",
    }
    options = _parse_read_options({"sync_start_date": "2025-01-01", "sync_end_date": "2026-09-14"})

    state = _resolve_read_state(offset, options, date(2026, 9, 14))

    assert state.next_date == date(2026, 9, 10)
    assert state.upper_bound == date(2026, 9, 14)
    assert state.initial_start == date(2025, 1, 1)


def test_in_progress_offset_rejects_sync_end_date_regression():
    offset = {
        "high_watermark": None,
        "next_date": "2026-09-10",
        "upper_bound": "2026-09-13",
        "initial_start": "2025-01-01",
    }
    options = _parse_read_options({"sync_start_date": "2025-01-01", "sync_end_date": "2026-09-12"})

    with pytest.raises(ValueError, match="in-progress upper_bound"):
        _resolve_read_state(offset, options, date(2026, 9, 12))


def test_next_offset_preserves_state_and_formats_dates():
    state = ReadState(
        high_watermark=date(2026, 8, 20),
        next_date=date(2026, 8, 2),
        upper_bound=date(2026, 8, 23),
        initial_start=date(2025, 8, 23),
    )

    assert _next_offset(state) == {
        "high_watermark": "2026-08-20",
        "next_date": "2026-08-02",
        "upper_bound": "2026-08-23",
        "initial_start": "2025-08-23",
    }


def test_string_options_validate_timezone_and_json_advertiser_ids():
    opts = _parse_read_options(
        {
            "reporting_timezone": "Europe/Zurich",
            "advertiser_ids": '["123", "456"]',
            "max_records_per_batch": "25",
        }
    )

    assert opts.reporting_timezone.key == "Europe/Zurich"
    assert opts.advertiser_ids == ("123", "456")
    assert opts.max_records_per_batch == 25


def test_read_options_ignore_databricks_injected_options_without_mutating_them():
    complete_options = {
        "sync_start_date": "2026-01-15",
        "initial_lookback_months": "3",
        "lookback_window_days": "14",
        "reporting_timezone": "Europe/Zurich",
        "advertiser_ids": '["123", "456"]',
        "max_records_per_batch": "25",
        "access_token": "test-token",
        "refresh_token": "test-refresh-token",
        "profile_id": "123",
        "client_secret": "test-client-secret",
        "tablename": "ad_report_daily",
        "sourcename": "google_cm360",
    }
    original_options = dict(complete_options)

    opts = _parse_read_options(complete_options)
    connector = GoogleCm360LakeflowConnect(complete_options)

    assert opts.sync_start_date == date(2026, 1, 15)
    assert opts.initial_lookback_months == 3
    assert opts.lookback_window_days == 14
    assert opts.reporting_timezone.key == "Europe/Zurich"
    assert opts.advertiser_ids == ("123", "456")
    assert opts.max_records_per_batch == 25
    assert complete_options == original_options
    assert connector._access_token == "test-token"
    assert connector._profile_id == "123"


def test_default_options_are_public_contract_values():
    opts = _parse_read_options({})

    assert opts.sync_start_date is None
    assert opts.initial_lookback_months == 12
    assert opts.lookback_window_days == 30
    assert opts.reporting_timezone.key == "UTC"
    assert opts.advertiser_ids == ()
    assert opts.delivery_country_codes == ()
    assert opts.sync_end_date is None
    assert opts.max_records_per_batch == 100_000


@pytest.mark.parametrize("code", ["US", "DE", "CH"])
def test_delivery_country_codes_accepts_one_supported_code(code):
    assert _parse_delivery_country_codes(json.dumps([code])) == (code,)


@pytest.mark.parametrize(
    "value",
    ["not-json", "{}", "[]", '["US", "DE"]', '["US", "US"]', "[123]", '["CA"]'],
)
def test_delivery_country_codes_rejects_invalid_values_before_api(value, monkeypatch):
    calls = []
    monkeypatch.setattr(
        requests.Session,
        "request",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    with pytest.raises(ValueError, match="delivery_country_codes"):
        _parse_read_options({"delivery_country_codes": value})

    assert calls == []


@pytest.mark.parametrize("value", ["bad-date", "2027-01-01", "2024-12-31"])
def test_sync_end_date_rejects_malformed_future_or_inverted_ranges(value):
    options = {"sync_start_date": "2025-01-01", "sync_end_date": value}
    with pytest.raises(ValueError, match="sync_end_date"):
        _resolve_read_state(None, _parse_read_options(options), date(2026, 9, 14))


def test_sync_end_date_is_the_exact_inclusive_upper_bound():
    options = _parse_read_options({"sync_start_date": "2025-01-01", "sync_end_date": "2026-09-13"})

    state = _resolve_read_state(None, options, date(2026, 9, 14))

    assert state.upper_bound == date(2026, 9, 13)


@pytest.mark.parametrize("value", ["2026-02-30", "20260823", "08/23/2026", "", "tomorrow"])
def test_invalid_sync_start_date_is_rejected(value):
    with pytest.raises(ValueError, match="sync_start_date"):
        _parse_read_options({"sync_start_date": value})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("initial_lookback_months", "0"),
        ("initial_lookback_months", "-1"),
        ("lookback_window_days", "0"),
        ("lookback_window_days", "-30"),
        ("max_records_per_batch", "0"),
        ("max_records_per_batch", "-100"),
        ("max_records_per_batch", "many"),
    ],
)
def test_non_positive_or_non_integer_options_are_rejected(name, value):
    with pytest.raises(ValueError, match=name):
        _parse_read_options({name: value})


def test_invalid_timezone_is_rejected():
    with pytest.raises(ValueError, match="reporting_timezone"):
        _parse_read_options({"reporting_timezone": "Europe/Not-A-City"})


@pytest.mark.parametrize(
    "value",
    ["not-json", "{}", "[123]", '["123", 456]', '[""]'],
)
def test_invalid_advertiser_ids_are_rejected(value):
    with pytest.raises(ValueError, match="advertiser_ids"):
        _parse_read_options({"advertiser_ids": value})


@pytest.mark.parametrize(
    "offset",
    [
        {"high_watermark": "not-a-date"},
        {"next_date": "2026-08-01"},
        {
            "high_watermark": "2026-08-20",
            "next_date": "2026-08-01",
            "upper_bound": "2026-08-23",
        },
    ],
)
def test_malformed_offsets_are_rejected(offset):
    with pytest.raises(ValueError, match="offset"):
        _resolve_read_state(offset, _parse_read_options({}), date(2026, 8, 23))


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"access_token": "", "profile_id": "123"},
        {"access_token": "test-token"},
        {"access_token": "test-token", "profile_id": "profile-123"},
    ],
)
def test_missing_or_invalid_credentials_are_rejected(options):
    with pytest.raises(ValueError):
        GoogleCm360LakeflowConnect(options)


@pytest.mark.parametrize("method_name", ["get_table_schema", "read_table_metadata"])
def test_invalid_table_names_are_rejected(method_name):
    method = getattr(_connector(), method_name)

    with pytest.raises(ValueError, match="not supported"):
        method("campaigns", {})


def test_calendar_month_subtraction_clamps_to_last_valid_day():
    assert _subtract_calendar_months(date(2024, 3, 31), 1) == date(2024, 2, 29)
    assert _subtract_calendar_months(date(2025, 1, 31), 1) == date(2024, 12, 31)


def _response(status: int, body=None, headers=None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers or {})
    if body is not None:
        response._content = json.dumps(body).encode("utf-8")
        response.headers["Content-Type"] = "application/json"
    else:
        response._content = b""
    return response


def _install_responses(monkeypatch, responses):
    captured = []
    queue = iter(responses)

    def fake_send(_session, request, **kwargs):
        captured.append((request, kwargs))
        response = next(queue)
        response.request = request
        response.url = request.url
        return response

    monkeypatch.setattr(requests.Session, "send", fake_send)
    return captured


def _client() -> Cm360Client:
    return Cm360Client("test-token", "123")


def test_profile_check_uses_bearer_token_and_timeout(monkeypatch):
    captured = _install_responses(
        monkeypatch, [_response(200, {"profileId": "123", "userName": "Analyst"})]
    )

    profile = _client().get_profile()

    request, send_kwargs = captured[0]
    assert profile["profileId"] == "123"
    assert request.method == "GET"
    assert request.url.endswith("/dfareporting/v5/userprofiles/123")
    assert request.headers["Authorization"] == "Bearer test-token"
    assert send_kwargs["timeout"] == 30


def test_country_resolution_uses_reporting_scope_endpoint(monkeypatch):
    captured = _install_responses(
        monkeypatch,
        [
            _response(
                200,
                {
                    "items": [
                        {
                            "dimensionName": "country",
                            "value": "United States",
                            "id": "2840",
                            "matchType": "EXACT",
                        }
                    ]
                },
            )
        ],
    )

    result = _client().query_dimension_values(
        "country",
        "United States",
        date(2025, 1, 1),
        date(2026, 9, 14),
    )

    request, send_kwargs = captured[0]
    assert result["items"][0]["id"] == "2840"
    assert request.method == "POST"
    assert request.url.endswith(
        "/dfareporting/v5/userprofiles/123/dimensionvalues/query?maxResults=100"
    )
    assert json.loads(request.body) == {
        "dimensionName": "country",
        "filters": [{"dimensionName": "country", "value": "United States"}],
        "startDate": "2025-01-01",
        "endDate": "2026-09-14",
    }
    assert send_kwargs["timeout"] == 30


def test_country_resolution_follows_pages_and_keeps_only_exact_match(monkeypatch):
    captured = _install_responses(
        monkeypatch,
        [
            _response(
                200,
                {
                    "items": [
                        {
                            "dimensionName": "country",
                            "value": "Canada",
                            "id": "2124",
                            "matchType": "EXACT",
                        }
                    ],
                    "nextPageToken": "page-2",
                },
            ),
            _response(
                200,
                {
                    "items": [
                        {
                            "dimensionName": "country",
                            "value": "United States",
                            "id": "2840",
                        }
                    ]
                },
            ),
        ],
    )

    result = _client().query_dimension_values(
        "country",
        "United States",
        date(2025, 1, 1),
        date(2026, 9, 14),
    )

    assert result == {
        "items": [
            {
                "dimensionName": "country",
                "value": "United States",
                "id": "2840",
            }
        ]
    }
    assert len(captured) == 2
    assert captured[0][0].url.endswith("?maxResults=100")
    assert captured[1][0].url.endswith("?maxResults=100&pageToken=page-2")


def test_country_resolution_stops_after_exact_match_on_paginated_page(monkeypatch):
    captured = _install_responses(
        monkeypatch,
        [
            _response(
                200,
                {
                    "items": [
                        {
                            "dimensionName": "country",
                            "value": "United States",
                            "id": "2840",
                            "matchType": "EXACT",
                        }
                    ],
                    "nextPageToken": "page-2",
                },
            ),
            _response(
                200,
                {
                    "items": [
                        {
                            "dimensionName": "country",
                            "value": "Canada",
                            "id": "2124",
                            "matchType": "EXACT",
                        }
                    ]
                },
            ),
        ],
    )

    result = _client().query_dimension_values(
        "country",
        "United States",
        date(2025, 1, 1),
        date(2026, 9, 14),
    )

    assert result["items"] == [
        {
            "dimensionName": "country",
            "value": "United States",
            "id": "2840",
            "matchType": "EXACT",
        }
    ]
    assert len(captured) == 1


def test_retry_honors_retry_after_then_succeeds(monkeypatch):
    _install_responses(
        monkeypatch,
        [
            _response(429, {"error": "response-secret"}, {"Retry-After": "2"}),
            _response(200, {"profileId": "123"}),
        ],
    )
    sleep_calls = []
    monkeypatch.setattr("time.sleep", sleep_calls.append)

    assert _client().get_profile()["profileId"] == "123"
    assert sleep_calls == [2.0]


def test_retry_after_is_bounded_and_invalid_values_use_fallback(monkeypatch):
    _install_responses(
        monkeypatch,
        [
            _response(429, headers={"Retry-After": "86400"}),
            _response(429, headers={"Retry-After": "-1"}),
            _response(429, headers={"Retry-After": "invalid"}),
            _response(429, headers={"Retry-After": "NaN"}),
            _response(200, {"profileId": "123"}),
        ],
    )
    sleep_calls = []
    monkeypatch.setattr("time.sleep", sleep_calls.append)

    _client().get_profile()

    assert sleep_calls[0] == 30.0
    assert all(0 <= wait <= 30 for wait in sleep_calls)


def test_retry_after_http_date_is_parsed(monkeypatch):
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=5)
    _install_responses(
        monkeypatch,
        [
            _response(503, headers={"Retry-After": format_datetime(retry_at)}),
            _response(200, {"profileId": "123"}),
        ],
    )
    sleep_calls = []
    monkeypatch.setattr("time.sleep", sleep_calls.append)

    _client().get_profile()

    assert len(sleep_calls) == 1
    assert 0 < sleep_calls[0] <= 5


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_transient_server_errors_are_retried(monkeypatch, status):
    captured = _install_responses(
        monkeypatch,
        [_response(status), _response(200, {"profileId": "123"})],
    )
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert _client().get_profile()["profileId"] == "123"
    assert len(captured) == 2


def test_exhausted_retries_raise_sanitized_error(monkeypatch):
    captured = _install_responses(monkeypatch, [_response(503)] * 5)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(Cm360ApiError, match="HTTP 503.*5 attempts"):
        _client().get_profile()

    assert len(captured) == 5


def test_permanent_403_is_not_retried_and_body_is_not_logged(monkeypatch, caplog):
    captured = _install_responses(monkeypatch, [_response(403, {"error": "response-secret"})])

    with pytest.raises(Cm360ApiError, match="HTTP 403") as error:
        _client().get_profile()

    assert len(captured) == 1
    assert "test-token" not in caplog.text
    assert "response-secret" not in caplog.text
    assert "test-token" not in str(error.value)
    assert "response-secret" not in str(error.value)


def test_invalid_json_is_reported_without_response_body(monkeypatch):
    response = _response(200)
    response._content = b"response-secret"
    _install_responses(monkeypatch, [response])

    with pytest.raises(Cm360ApiError, match="invalid JSON") as error:
        _client().get_profile()

    assert "response-secret" not in str(error.value)


def test_unexpected_profile_id_is_rejected(monkeypatch):
    _install_responses(monkeypatch, [_response(200, {"profileId": "999"})])

    with pytest.raises(Cm360ConfigurationError, match="profile_id"):
        _client().get_profile()


@pytest.mark.parametrize("profile_id", [None, 123, "profile-123"])
def test_malformed_profile_id_is_rejected(monkeypatch, profile_id):
    _install_responses(monkeypatch, [_response(200, {"profileId": profile_id})])

    with pytest.raises(Cm360ConfigurationError, match="profileId"):
        _client().get_profile()


PROFILE = {
    "profileId": "123",
    "accountId": "7001",
    "accountName": "Example Global",
    "subAccountId": "7101",
    "subAccountName": "Example Media",
}


def test_profile_metadata_uses_profile_account_and_subaccount():
    assert _build_profile_metadata(PROFILE) == EXPECTED_PROFILE_METADATA


def test_profile_metadata_allows_profile_without_subaccount():
    profile = {key: value for key, value in PROFILE.items() if not key.startswith("subAccount")}

    metadata = _build_profile_metadata(profile)

    assert metadata.subaccount_id is None
    assert metadata.subaccount_name is None


@pytest.mark.parametrize(
    "profile",
    [
        {**PROFILE, "accountId": "account-7001"},
        {**PROFILE, "accountName": ""},
        {**PROFILE, "subAccountId": None},
        {**PROFILE, "subAccountName": ""},
    ],
)
def test_profile_metadata_rejects_malformed_or_partial_profile_identity(profile):
    with pytest.raises(Cm360ConfigurationError):
        _build_profile_metadata(profile)


def test_profile_metadata_error_does_not_expose_response_values():
    profile = {**PROFILE, "accountName": "", "unrelated": "source-secret-marker"}

    with pytest.raises(Cm360ConfigurationError, match="accountName") as error:
        _build_profile_metadata(profile)

    assert "source-secret-marker" not in str(error.value)


def test_compatible_fields_accepts_selected_fields_not_echoed_by_query(monkeypatch):
    report = _build_report_definition(date(2026, 8, 23), ())
    compatible = {
        "reportCompatibleFields": {
            "dimensions": [],
            "metrics": [],
        },
    }
    captured = _install_responses(monkeypatch, [_response(200, compatible)])

    _client().validate_compatible_fields(report)

    assert len(captured) == 1
    assert captured[0][0].url.endswith(
        "/dfareporting/v5/userprofiles/123/reports/compatiblefields/query"
    )
    assert captured[0][0].method == "POST"
    assert json.loads(captured[0][0].body) == report


def test_simulator_rejects_obsolete_compatible_fields_request():
    report = _build_report_definition(date(2026, 8, 23), ())
    report["criteria"]["metricNames"][3] = "videoViews"
    request = requests.Request(
        "POST",
        "https://dfareporting.googleapis.com/dfareporting/v5/userprofiles/123/reports/compatiblefields/query",
        json=report,
    ).prepare()

    with pytest.raises(AssertionError, match="compatible-fields metrics"):
        handle_simulator_request(request, None, SimpleNamespace(tables={}))


def test_simulator_rejects_multi_day_report_insert():
    report = _build_report_definition(date(2026, 8, 23), ())
    report["criteria"]["dateRange"]["endDate"] = "2026-08-24"
    request = requests.Request(
        "POST",
        "https://dfareporting.googleapis.com/dfareporting/v5/userprofiles/123/reports",
        json=report,
    ).prepare()

    with pytest.raises(AssertionError, match="exact one-day"):
        handle_simulator_request(request, None, SimpleNamespace(tables={}))


def test_report_definition_has_exact_daily_dimensions_metrics_and_filters():
    report = _build_report_definition(date(2026, 8, 23), ("10", "20"), "2840")

    assert report["name"] == "lakeflow-google-cm360-2026-08-23"
    assert report["type"] == "STANDARD"
    assert report["format"] == "CSV"
    assert report["criteria"]["dateRange"] == {
        "startDate": "2026-08-23",
        "endDate": "2026-08-23",
    }
    assert [item["name"] for item in report["criteria"]["dimensions"]] == [
        "country",
        "advertiserId",
        "advertiser",
        "campaignId",
        "campaign",
    ]
    assert "adId" not in json.dumps(report)
    assert '"ad"' not in json.dumps(report)
    assert report["criteria"]["metricNames"] == [
        "impressions",
        "clicks",
        "mediaCost",
        "richMediaVideoPlays",
        "richMediaVideoViews",
        "richMediaVideoMidpoints",
        "richMediaVideoCompletions",
    ]
    assert not {
        "advertiserCurrencyCode",
        "videoPlays",
        "videoViews",
        "videoMidpoints",
        "videoCompletions",
    } & {
        *[item["name"] for item in report["criteria"]["dimensions"]],
        *report["criteria"]["metricNames"],
    }
    assert report["criteria"]["dimensionFilters"] == [
        {"dimensionName": "advertiser", "id": "10"},
        {"dimensionName": "advertiser", "id": "20"},
        {"dimensionName": "country", "id": "2840"},
    ]


def test_preflight_profile_and_compatible_fields_run_once(monkeypatch):
    report = _build_report_definition(date(2026, 8, 23), ())
    compatible = {
        "reportCompatibleFields": {
            "dimensions": [{"name": item["name"]} for item in report["criteria"]["dimensions"]],
            "metrics": [{"name": name} for name in report["criteria"]["metricNames"]],
        },
    }
    captured = _install_responses(
        monkeypatch,
        [
            _response(200, PROFILE),
            _response(200, compatible),
        ],
    )
    connector = _connector()

    connector._validate_preflight("ad_report_daily", report)
    connector._validate_preflight("ad_report_daily", report)

    assert connector._preflight_complete_tables == {"ad_report_daily"}
    assert connector._profile_metadata_by_table == {"ad_report_daily": EXPECTED_PROFILE_METADATA}
    assert len(captured) == 2


CSV_HEADER = (
    "Country,Advertiser ID,Advertiser,Campaign ID,Campaign,Impressions,Clicks,Media Cost,"
    "Video Plays,Video Views,Video Mid-points,Video Completions"
)
REORDERED_CSV_HEADER = (
    "Campaign,Country,Advertiser,Campaign ID,Advertiser ID,Clicks,Impressions,Media Cost,"
    "Video Completions,Video Plays,Video Mid-points,Video Views"
)


def _report_csv(
    *rows: str,
    include_marker: bool = True,
    include_footer: bool = True,
    header: str = CSV_HEADER,
) -> str:
    lines = ["Report Name,Lakeflow CM360", "Generated,2026-08-24"]
    if include_marker:
        lines.append("Report Fields")
    lines.append(header)
    lines.extend(rows)
    if include_footer:
        lines.append("Grand Total:")
    return "\n".join(lines)


VALID_CSV_ROW = (
    "United States,3001,Example North America,1001,Summer,1000,25,123.45,800,600,500,400"
)
CANADA_CSV_ROW = "Canada,3001,Example North America,1001,Summer,500,12,61.25,390,300,250,200"
REACH_CSV_HEADER = (
    "Advertiser,Advertiser ID,Campaign,Campaign ID,Country,Date,Unique Reach: Impression Reach"
)


@pytest.mark.parametrize(
    ("metric", "expected"),
    [("1000", "1000"), ("-", "-"), ("", None)],
)
def test_reach_csv_parser_preserves_source_metric(metric, expected):
    rows = _parse_report_csv(
        _report_csv(
            f"Example North America,3001,Summer,1001,United States,2026-08-22,{metric}",
            header=REACH_CSV_HEADER,
        ),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
        field_specs=REACH_FIELD_SPECS,
    )

    assert rows[0]["unique_reach_impression_reach"] == expected
    assert rows[0]["report_date"] == "2026-08-22"


def test_reach_csv_parser_rejects_a_date_outside_the_one_day_request():
    with pytest.raises(Cm360DataError, match="Date does not match"):
        _parse_report_csv(
            _report_csv(
                "Example North America,3001,Summer,1001,United States,2026-08-21,1000",
                header=REACH_CSV_HEADER,
            ),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
            field_specs=REACH_FIELD_SPECS,
        )


def test_csv_parser_emits_exact_raw_values_and_types():
    csv_text = _report_csv(
        VALID_CSV_ROW,
        CANADA_CSV_ROW,
        "Grand Total:",
    )

    rows = _parse_report_csv(
        csv_text,
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
    )

    assert rows[0] == {
        "profile_id": "123",
        "account_id": "7001",
        "account_name": "Example Global",
        "subaccount_id": "7101",
        "subaccount_name": "Example Media",
        "report_date": "2026-08-22",
        "delivery_country": "United States",
        "advertiser_id": "3001",
        "advertiser_name": "Example North America",
        "campaign_id": "1001",
        "campaign_name": "Summer",
        "impressions": 1000,
        "clicks": 25,
        "media_cost": "123.450000000",
        "video_starts": 800,
        "qualified_video_views": 600,
        "video_views_50_percent": 500,
        "video_views_100_percent": 400,
        "report_file_last_modified_at": "2026-08-24T08:00:00+00:00",
    }
    assert rows[1]["delivery_country"] == "Canada"
    assert rows[1]["media_cost"] == "61.250000000"
    second_rows = _parse_report_csv(
        _report_csv(VALID_CSV_ROW),
        profile_id="123",
        expected_report_date=date(2026, 8, 23),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
    )
    key_names = (
        "profile_id",
        "report_date",
        "delivery_country",
        "advertiser_id",
        "campaign_id",
    )
    first_key = tuple(rows[0][name] for name in key_names)
    second_key = tuple(second_rows[0][name] for name in key_names)
    assert first_key == ("123", "2026-08-22", "United States", "3001", "1001")
    assert second_key == ("123", "2026-08-23", "United States", "3001", "1001")
    assert first_key != second_key


def test_csv_parser_trims_delivery_country_for_stable_keys():
    row = VALID_CSV_ROW.replace("United States", "  United States  ", 1)

    rows = _parse_report_csv(
        _report_csv(row),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
    )

    assert rows[0]["delivery_country"] == "United States"


@pytest.mark.parametrize(
    "duplicate_row",
    [VALID_CSV_ROW, VALID_CSV_ROW.replace("1000", "999", 1)],
)
def test_csv_parser_rejects_duplicate_primary_keys(duplicate_row):
    with pytest.raises(Cm360DataError, match="duplicate primary key"):
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW, duplicate_row),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


def test_csv_parser_converts_file_epoch_millis_to_utc_timestamp():
    rows = _parse_report_csv(
        _report_csv(VALID_CSV_ROW),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400123",
    )

    assert rows[0]["report_file_last_modified_at"] == "2026-08-24T08:00:00.123000+00:00"


@pytest.mark.parametrize("campaign_name", ["Grand Total:", "Report Time:"])
def test_csv_parser_maps_full_width_rows_independently_of_order(campaign_name):
    reordered_row = (
        f"{campaign_name},United States,Example North America,1001,3001,"
        "25,1000,123.45,400,800,500,600"
    )

    rows = _parse_report_csv(
        _report_csv(reordered_row, header=REORDERED_CSV_HEADER),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
    )

    assert rows[0] == {
        "profile_id": "123",
        "account_id": "7001",
        "account_name": "Example Global",
        "subaccount_id": "7101",
        "subaccount_name": "Example Media",
        "report_date": "2026-08-22",
        "delivery_country": "United States",
        "advertiser_id": "3001",
        "advertiser_name": "Example North America",
        "campaign_id": "1001",
        "campaign_name": campaign_name,
        "impressions": 1000,
        "clicks": 25,
        "media_cost": "123.450000000",
        "video_starts": 800,
        "qualified_video_views": 600,
        "video_views_50_percent": 500,
        "video_views_100_percent": 400,
        "report_file_last_modified_at": "2026-08-24T08:00:00+00:00",
    }


def test_csv_parser_rejects_extra_column_when_row_starts_with_footer_label():
    with pytest.raises(Cm360DataError, match="invalid column count"):
        _parse_report_csv(
            _report_csv(f"Grand Total:,{VALID_CSV_ROW}"),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


def test_csv_parser_rejects_a_header_only_truncated_report():
    with pytest.raises(Cm360DataError, match="footer"):
        _parse_report_csv(
            _report_csv(include_footer=False),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


def test_csv_parser_requires_report_fields_marker():
    with pytest.raises(Cm360DataError, match="Report Fields"):
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW, include_marker=False),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


@pytest.mark.parametrize(
    "header",
    [
        CSV_HEADER.replace(",Clicks", ""),
        CSV_HEADER.replace("Clicks", "Clicks,Clicks"),
    ],
)
def test_csv_parser_rejects_missing_or_duplicate_headers(header):
    with pytest.raises(Cm360DataError, match="header"):
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW, header=header),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


def test_csv_parser_names_only_missing_and_unexpected_headers():
    header = CSV_HEADER.replace("Clicks", "Click count")

    with pytest.raises(Cm360DataError) as error:
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW, header=header),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )

    assert str(error.value) == (
        "CM360 CSV header does not match the configured fields "
        "(missing: Clicks; unexpected: Click count)"
    )


def test_csv_parser_accepts_footer_only_empty_report():
    rows = _parse_report_csv(
        _report_csv("Grand Total:", "Report Time:,2026-08-24"),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
    )

    assert rows == []


def test_csv_parser_ignores_non_data_metadata_after_footer():
    rows = _parse_report_csv(
        _report_csv(
            VALID_CSV_ROW,
            "Grand Total:",
            "Report Time Zone:,UTC",
        ),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
    )

    assert [row["campaign_id"] for row in rows] == ["1001"]


def test_csv_parser_ignores_full_width_grand_total_footer():
    footer = "Grand Total:,,,,1000,25,123.45,800,600,500,400"

    rows = _parse_report_csv(
        _report_csv(VALID_CSV_ROW, footer),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
    )

    assert [row["campaign_id"] for row in rows] == ["1001"]


def test_csv_parser_rejects_short_label_prefixed_data_row():
    short_data_row = "Grand Total:,Video B,1002,2002"

    with pytest.raises(Cm360DataError, match="column count"):
        _parse_report_csv(
            _report_csv(short_data_row, header=REORDERED_CSV_HEADER),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


def test_csv_parser_rejects_short_label_prefixed_data_after_footer():
    short_data_row = "Grand Total:,Video B,1002,2002"

    with pytest.raises(Cm360DataError, match="after its footer"):
        _parse_report_csv(
            _report_csv("Grand Total:", short_data_row, header=REORDERED_CSV_HEADER),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


def test_csv_parser_rejects_data_after_footer():
    with pytest.raises(Cm360DataError, match="after its footer"):
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW, "Grand Total:", VALID_CSV_ROW),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (VALID_CSV_ROW.replace("United States", "", 1), "delivery_country"),
        (VALID_CSV_ROW.replace("United States", " ", 1), "delivery_country"),
        (VALID_CSV_ROW.replace("3001", "", 1), "advertiser_id"),
        (VALID_CSV_ROW.replace("3001", " ", 1), "advertiser_id"),
        (VALID_CSV_ROW.replace("3001", "advertiser-3001", 1), "advertiser_id"),
        (VALID_CSV_ROW.replace("Example North America", "", 1), "advertiser_name"),
        (VALID_CSV_ROW.replace("Example North America", " ", 1), "advertiser_name"),
        (VALID_CSV_ROW.replace("1001", "", 1), "campaign_id"),
        (VALID_CSV_ROW.replace("1001", " ", 1), "campaign_id"),
        (VALID_CSV_ROW.replace("1000", "many", 1), "impressions"),
        (VALID_CSV_ROW.replace("1000", '"1,000"', 1), "impressions"),
        (VALID_CSV_ROW.replace("123.45", "1e3", 1), "media_cost"),
        (VALID_CSV_ROW.replace("123.45", '"123,45"', 1), "media_cost"),
    ],
)
def test_csv_parser_rejects_invalid_required_or_numeric_values(row, message):
    with pytest.raises(Cm360DataError, match=message):
        _parse_report_csv(
            _report_csv(row),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


@pytest.mark.parametrize(
    "timestamp",
    [
        "not-a-timestamp",
        "-1",
        "999999999999999999999999999999",
        "2026-08-24T08:00:00Z",
        None,
        1787558400123,
    ],
)
def test_csv_parser_rejects_invalid_file_timestamp(timestamp):
    with pytest.raises(Cm360DataError) as error:
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at=timestamp,
        )

    assert str(error.value) == "File lastModifiedTime is not valid epoch milliseconds"


def test_csv_parser_rejects_malformed_nonempty_rows():
    with pytest.raises(Cm360DataError, match="column count"):
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW, "unexpected,row"),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
        )


class _FakeCm360Client:
    def __init__(
        self,
        csv_text: str = "",
        file_states=None,
        delete_error=None,
        dimension_response=None,
    ):
        self.csv_text = csv_text
        self.file_states = iter(
            file_states
            or [
                {"status": "PROCESSING"},
                {
                    "status": "REPORT_AVAILABLE",
                    "lastModifiedTime": "1787558400000",
                },
            ]
        )
        self.delete_error = delete_error
        self.dimension_response = (
            {
                "items": [
                    {
                        "dimensionName": "country",
                        "value": "United States",
                        "id": "2840",
                        "matchType": "EXACT",
                    }
                ]
            }
            if dimension_response is None
            else dimension_response
        )
        self.dimension_queries = []
        self.calls = []

    def get_profile(self):
        self.calls.append("profile")
        return PROFILE

    def query_dimension_values(self, dimension_name, exact_value, start_date, end_date):
        self.calls.append("dimension-values")
        self.dimension_queries.append((dimension_name, exact_value, start_date, end_date))
        return self.dimension_response

    def validate_compatible_fields(self, _report):
        self.calls.append("compatible")

    def insert_report(self, _report):
        self.calls.append("insert")
        return "report-1"

    def run_report(self, report_id):
        assert report_id == "report-1"
        self.calls.append("run")
        return "file-1"

    def get_file(self, report_id, file_id):
        assert (report_id, file_id) == ("report-1", "file-1")
        self.calls.append("poll")
        return next(self.file_states)

    def download_file(self, report_id, file_id):
        assert (report_id, file_id) == ("report-1", "file-1")
        self.calls.append("download")
        return self.csv_text

    def delete_report(self, report_id):
        assert report_id == "report-1"
        self.calls.append("delete")
        if self.delete_error:
            raise self.delete_error


@pytest.mark.parametrize("rows", [(), (VALID_CSV_ROW,), (VALID_CSV_ROW, "Report Time:,now")])
def test_truncated_daily_report_does_not_commit_and_can_retry(monkeypatch, rows):
    connector = _connector()
    options = {"sync_start_date": "2026-08-22", "sync_end_date": "2026-08-22"}
    prior_offset = {"high_watermark": "2026-08-21"}
    failed_client = _FakeCm360Client(_report_csv(*rows, include_footer=False))
    connector._client_instance = failed_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(Cm360DataError, match="footer"):
        connector.read_table("ad_report_daily", prior_offset, options)

    assert prior_offset == {"high_watermark": "2026-08-21"}
    assert connector._terminal_offsets == {}
    assert failed_client.calls[-1] == "delete"
    connector._client_instance = _FakeCm360Client(_report_csv(VALID_CSV_ROW))
    result, offset = connector.read_table("ad_report_daily", prior_offset, options)
    assert [row["report_date"] for row in result] == ["2026-08-22"]
    assert offset == {"high_watermark": "2026-08-22"}


@pytest.mark.parametrize("rows", [(), (VALID_CSV_ROW,)])
def test_complete_daily_report_commits_even_when_empty(monkeypatch, rows):
    connector = _connector()
    connector._client_instance = _FakeCm360Client(_report_csv(*rows))
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    result, offset = connector.read_table(
        "ad_report_daily",
        None,
        {"sync_start_date": "2026-08-22", "sync_end_date": "2026-08-22"},
    )
    assert len(list(result)) == len(rows)
    assert offset == {"high_watermark": "2026-08-22"}


@pytest.mark.parametrize("native_date", ["8/22/26", "22/08/2026", "2026-08-21"])
def test_daily_reach_rejects_unsupported_or_mismatched_dates(native_date):
    with pytest.raises(Cm360DataError, match="date|Date"):
        _parse_report_csv(
            _report_csv(
                f"Example,3001,Campaign,1001,United States,{native_date},1000",
                header=REACH_CSV_HEADER,
            ),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
            field_specs=REACH_FIELD_SPECS,
        )


def test_two_daily_reports_reuse_preflight_metadata(monkeypatch):
    available = {
        "status": "REPORT_AVAILABLE",
        "lastModifiedTime": "1787558400000",
    }
    fake_client = _FakeCm360Client(
        _report_csv(VALID_CSV_ROW),
        file_states=[{"status": "PROCESSING"}, available] * 2,
    )
    connector = _connector()
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    options = _parse_read_options({})

    connector._execute_daily_report(
        "ad_report_daily", date(2026, 8, 22), options, None, date(2026, 8, 23)
    )
    connector._execute_daily_report(
        "ad_report_daily", date(2026, 8, 23), options, None, date(2026, 8, 23)
    )

    assert fake_client.calls == [
        "profile",
        "compatible",
        "insert",
        "run",
        "poll",
        "poll",
        "download",
        "delete",
        "insert",
        "run",
        "poll",
        "poll",
        "download",
        "delete",
    ]


def test_filtered_reports_resolve_country_once_and_reuse_it(monkeypatch):
    available = {
        "status": "REPORT_AVAILABLE",
        "lastModifiedTime": "1787558400000",
    }
    fake_client = _FakeCm360Client(
        _report_csv(VALID_CSV_ROW),
        file_states=[{"status": "PROCESSING"}, available] * 2,
    )
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    options = {
        "sync_start_date": "2026-08-22",
        "sync_end_date": "2026-08-23",
        "delivery_country_codes": '["US"]',
    }

    _, offset = connector.read_table("ad_report_daily", None, options)
    connector.read_table("ad_report_daily", offset, options)

    assert fake_client.dimension_queries == [
        ("country", "United States", date(2026, 8, 22), date(2026, 8, 23))
    ]
    assert fake_client.calls == [
        "profile",
        "dimension-values",
        "compatible",
        "insert",
        "run",
        "poll",
        "poll",
        "download",
        "delete",
        "insert",
        "run",
        "poll",
        "poll",
        "download",
        "delete",
    ]


def test_filtered_report_rejects_row_from_a_different_country(monkeypatch):
    germany_row = VALID_CSV_ROW.replace("United States", "Germany", 1)
    fake_client = _FakeCm360Client(_report_csv(germany_row))
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 22)
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(Cm360DataError) as error:
        connector.read_table(
            "ad_report_daily",
            None,
            {
                "sync_start_date": "2026-08-22",
                "delivery_country_codes": '["US"]',
            },
        )

    assert connector._terminal_offsets.get("ad_report_daily") is None
    assert "Germany" not in str(error.value)
    assert "test-token" not in str(error.value)


def test_country_resolution_accepts_omitted_match_type_default():
    response = {
        "items": [
            {
                "dimensionName": "country",
                "value": "United States",
                "id": "2840",
            }
        ]
    }

    assert GoogleCm360LakeflowConnect._resolve_delivery_country_id(response, "US") == "2840"


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        ({}, "items_missing"),
        ({"items": []}, "item_count_0"),
        ({"items": [{}]}, "dimension_name_mismatch"),
        (
            {"items": [{"dimensionName": "country", "value": "source-secret-marker"}]},
            "value_mismatch",
        ),
        (
            {
                "items": [
                    {
                        "dimensionName": "country",
                        "value": "United States",
                        "matchType": "BEGINS_WITH",
                    }
                ]
            },
            "match_type_mismatch",
        ),
        ({"items": [{}], "nextPageToken": "source-secret-marker"}, "paginated"),
    ],
)
def test_country_resolution_reports_safe_failure_reason(response, reason):
    with pytest.raises(Cm360ConfigurationError) as error:
        GoogleCm360LakeflowConnect._resolve_delivery_country_id(response, "US")

    assert f"reason={reason}" in str(error.value)
    assert "source-secret-marker" not in str(error.value)


@pytest.mark.parametrize(
    "dimension_response",
    [
        {},
        {"items": []},
        {
            "items": [
                {
                    "dimensionName": "country",
                    "value": "United States",
                    "id": "2840",
                    "matchType": "EXACT",
                },
                {
                    "dimensionName": "country",
                    "value": "United States",
                    "id": "2841",
                    "matchType": "EXACT",
                },
            ]
        },
        {
            "items": [
                {
                    "dimensionName": "country",
                    "value": "United States",
                    "id": "2840",
                    "matchType": "EXACT",
                }
            ],
            "nextPageToken": "source-secret-marker",
        },
        {
            "items": [
                {
                    "dimensionName": "country",
                    "value": "United States",
                    "matchType": "EXACT",
                }
            ]
        },
        {
            "items": [
                {
                    "dimensionName": "advertiser",
                    "value": "United States",
                    "id": "2840",
                    "matchType": "EXACT",
                }
            ]
        },
        {
            "items": [
                {
                    "dimensionName": "country",
                    "value": "Canada",
                    "id": "2840",
                    "matchType": "EXACT",
                }
            ]
        },
        {
            "items": [
                {
                    "dimensionName": "country",
                    "value": "United States",
                    "id": "2840",
                    "matchType": "BEGINS_WITH",
                }
            ]
        },
    ],
)
def test_invalid_country_resolution_fails_before_report_insertion(dimension_response):
    dimension_response["source-secret-marker"] = "source-secret-marker"
    fake_client = _FakeCm360Client(dimension_response=dimension_response)
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)
    connector._client_instance = fake_client

    with pytest.raises(Cm360ConfigurationError) as error:
        connector.read_table(
            "ad_report_daily",
            None,
            {
                "sync_start_date": "2026-08-23",
                "sync_end_date": "2026-08-23",
                "delivery_country_codes": '["US"]',
            },
        )

    assert "insert" not in fake_client.calls
    assert "source-secret-marker" not in str(error.value)


def test_daily_lifecycle_runs_in_order_and_always_deletes(monkeypatch):
    fake_client = _FakeCm360Client(_report_csv(VALID_CSV_ROW))
    connector = _connector()
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    rows = connector._execute_daily_report(
        "ad_report_daily",
        date(2026, 8, 22),
        _parse_read_options({}),
        None,
        date(2026, 8, 22),
    )

    assert rows[0]["delivery_country"] == "United States"
    assert fake_client.calls == [
        "profile",
        "compatible",
        "insert",
        "run",
        "poll",
        "poll",
        "download",
        "delete",
    ]


@pytest.mark.parametrize("status", ["FAILED", "CANCELLED"])
def test_poll_fails_immediately_for_terminal_failure(status):
    fake_client = _FakeCm360Client(file_states=[{"status": status}])

    with pytest.raises(Cm360ReportError, match=status):
        _poll_until_available(fake_client, "report-1", "file-1")

    assert fake_client.calls == ["poll"]


def test_poll_exhaustion_is_bounded(monkeypatch):
    fake_client = _FakeCm360Client(file_states=[{"status": "PROCESSING"}] * 60)
    sleep_calls = []
    monkeypatch.setattr("time.sleep", sleep_calls.append)

    with pytest.raises(Cm360ReportError, match="60 polls"):
        _poll_until_available(fake_client, "report-1", "file-1")

    assert len(sleep_calls) == 59
    assert max(sleep_calls) <= 30


def test_parser_failure_still_deletes_temporary_report(monkeypatch):
    fake_client = _FakeCm360Client("invalid csv")
    connector = _connector()
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(Cm360DataError):
        connector._execute_daily_report(
            "ad_report_daily",
            date(2026, 8, 22),
            _parse_read_options({}),
            None,
            date(2026, 8, 22),
        )

    assert fake_client.calls[-1] == "delete"


def test_cleanup_error_does_not_mask_parser_failure(monkeypatch, caplog):
    fake_client = _FakeCm360Client("invalid csv", delete_error=Cm360ApiError("cleanup-secret"))
    connector = _connector()
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(Cm360DataError, match="Report Fields"):
        connector._execute_daily_report(
            "ad_report_daily",
            date(2026, 8, 22),
            _parse_read_options({}),
            None,
            date(2026, 8, 22),
        )

    assert "cleanup-secret" not in caplog.text
    assert "temporary report cleanup failed" in caplog.text


def test_cleanup_error_after_success_is_best_effort(monkeypatch, caplog):
    fake_client = _FakeCm360Client(
        _report_csv(VALID_CSV_ROW), delete_error=Cm360ApiError("response-secret")
    )
    connector = _connector()
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    rows = connector._execute_daily_report(
        "ad_report_daily",
        date(2026, 8, 22),
        _parse_read_options({}),
        None,
        date(2026, 8, 22),
    )

    assert len(rows) == 1
    assert "response-secret" not in caplog.text


def test_read_table_advances_one_atomic_day_and_then_terminates(monkeypatch):
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)
    extracted_dates = []

    def execute(_table_name, report_date, _options, _telemetry, _upper_bound):
        extracted_dates.append(report_date)
        return [{"report_date": report_date.isoformat()}]

    monkeypatch.setattr(connector, "_execute_daily_report", execute)
    options = {"sync_start_date": "2026-08-22"}

    rows_1, offset_1 = connector.read_table("ad_report_daily", None, options)
    rows_2, offset_2 = connector.read_table("ad_report_daily", offset_1, options)
    rows_3, offset_3 = connector.read_table("ad_report_daily", offset_2, options)

    assert list(rows_1) == [{"report_date": "2026-08-22"}]
    assert offset_1 == {
        "high_watermark": None,
        "next_date": "2026-08-23",
        "upper_bound": "2026-08-23",
        "initial_start": "2026-08-22",
    }
    assert list(rows_2) == [{"report_date": "2026-08-23"}]
    assert offset_2 == {"high_watermark": "2026-08-23"}
    assert list(rows_3) == []
    assert offset_3 == offset_2
    assert extracted_dates == [date(2026, 8, 22), date(2026, 8, 23)]


def test_tables_keep_independent_offsets_and_upper_bounds(monkeypatch):
    connector = _connector()
    connector._upper_bounds = {
        "ad_report_daily": date(2026, 8, 22),
        "reach_report_daily": date(2026, 8, 23),
    }
    calls = []

    def execute(table_name, report_date, _options, _telemetry, _upper_bound):
        calls.append((table_name, report_date))
        return [{"report_date": report_date.isoformat()}]

    monkeypatch.setattr(connector, "_execute_daily_report", execute)

    _, ad_offset = connector.read_table("ad_report_daily", None, {"sync_start_date": "2026-08-22"})
    _, reach_offset_1 = connector.read_table(
        "reach_report_daily", None, {"sync_start_date": "2026-08-22"}
    )
    _, reach_offset_2 = connector.read_table(
        "reach_report_daily", reach_offset_1, {"sync_start_date": "2026-08-22"}
    )
    rows, repeated_ad_offset = connector.read_table(
        "ad_report_daily", ad_offset, {"sync_start_date": "2026-08-22"}
    )

    assert list(rows) == []
    assert repeated_ad_offset == ad_offset == {"high_watermark": "2026-08-22"}
    assert reach_offset_2 == {"high_watermark": "2026-08-23"}
    assert calls == [
        ("ad_report_daily", date(2026, 8, 22)),
        ("reach_report_daily", date(2026, 8, 22)),
        ("reach_report_daily", date(2026, 8, 23)),
    ]


def test_request_telemetry_is_strict_and_only_emitted_when_enabled(monkeypatch):
    connector = _connector()
    connector._upper_bounds = {"reach_report_daily": date(2026, 8, 22)}

    def execute(_table, day, _options, telemetry, _upper_bound):
        if telemetry is not None:
            telemetry.increment("daily_reports_completed")
        return [{"report_date": day.isoformat()}]

    monkeypatch.setattr(connector, "_execute_daily_report", execute)
    _, plain_offset = connector.read_table(
        "reach_report_daily", None, {"sync_start_date": "2026-08-22"}
    )
    connector._terminal_offsets.clear()
    _, instrumented_offset = connector.read_table(
        "reach_report_daily",
        None,
        {"sync_start_date": "2026-08-22", "emit_request_telemetry": "true"},
    )

    assert plain_offset == {"high_watermark": "2026-08-22"}
    assert instrumented_offset["high_watermark"] == "2026-08-22"
    assert instrumented_offset["request_telemetry"]["daily_reports_completed"] == 1
    assert set(instrumented_offset["request_telemetry"]) == {
        "daily_reports_completed",
        "http_attempts",
        "http_retries",
        "file_poll_calls",
        "transport_errors",
        "response_status_counts",
        "retry_status_counts",
    }
    for invalid in ("TRUE", "1", "yes", "false "):
        with pytest.raises(ValueError, match="strict boolean"):
            connector.read_table(
                "reach_report_daily",
                None,
                {"sync_start_date": "2026-08-22", "emit_request_telemetry": invalid},
            )


def test_instrumented_offset_rejects_missing_unknown_or_negative_counters():
    telemetry = RequestTelemetry().as_dict()
    invalid_values = [
        {key: value for key, value in telemetry.items() if key != "http_attempts"},
        {**telemetry, "unexpected": 0},
        {**telemetry, "http_attempts": -1},
        {
            **telemetry,
            "response_status_counts": {**telemetry["response_status_counts"], "418": 1},
        },
    ]

    for invalid in invalid_values:
        with pytest.raises(ValueError, match="request_telemetry"):
            RequestTelemetry.from_dict(invalid)


@pytest.mark.parametrize("retry_status", [429, 503])
def test_request_telemetry_counts_attempts_responses_and_actual_retries(monkeypatch, retry_status):
    _install_responses(
        monkeypatch,
        [_response(retry_status), _response(200, {"profileId": "123"})],
    )
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    telemetry = RequestTelemetry()

    assert _client().get_profile(telemetry=telemetry)["profileId"] == "123"
    assert telemetry.http_attempts == 2
    assert telemetry.http_retries == 1
    assert telemetry.response_status_counts[str(retry_status)] == 1
    assert telemetry.response_status_counts["200"] == 1
    assert telemetry.retry_status_counts[str(retry_status)] == 1


def test_request_telemetry_counts_terminal_and_transport_failures(monkeypatch):
    _install_responses(monkeypatch, [_response(403)])
    terminal = RequestTelemetry()
    with pytest.raises(Cm360ApiError, match="HTTP 403"):
        _client().get_profile(telemetry=terminal)
    assert terminal.http_attempts == 1
    assert terminal.response_status_counts["403"] == 1
    assert terminal.http_retries == 0

    def fail_request(*_args, **_kwargs):
        raise requests.ConnectionError("transport-secret")

    monkeypatch.setattr(requests.Session, "request", fail_request)
    transport = RequestTelemetry()
    with pytest.raises(Cm360ApiError) as error:
        _client().get_profile(telemetry=transport)
    assert transport.http_attempts == 1
    assert transport.transport_errors == 1
    assert "transport-secret" not in str(error.value)


def test_fresh_connector_replays_lookback_once_from_completed_checkpoint(monkeypatch):
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)
    extracted_dates = []
    monkeypatch.setattr(
        connector,
        "_execute_daily_report",
        lambda _table, report_date, _options, _telemetry, _upper: (
            extracted_dates.append(report_date) or []
        ),
    )

    _, offset = connector.read_table(
        "ad_report_daily",
        {"high_watermark": "2026-08-23"},
        {"sync_start_date": "2026-08-22"},
    )

    assert extracted_dates == [date(2026, 8, 22)]
    assert offset["next_date"] == "2026-08-23"
    assert offset["high_watermark"] == "2026-08-23"


def test_failed_day_does_not_advance_or_set_terminal_offset(monkeypatch):
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)
    monkeypatch.setattr(
        connector,
        "_execute_daily_report",
        lambda *_args: (_ for _ in ()).throw(Cm360ReportError("failed")),
    )

    with pytest.raises(Cm360ReportError):
        connector.read_table("ad_report_daily", None, {"sync_start_date": "2026-08-23"})

    assert connector._terminal_offsets.get("ad_report_daily") is None


def test_empty_daily_report_advances_normally(monkeypatch):
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)
    monkeypatch.setattr(connector, "_execute_daily_report", lambda *_args: [])

    rows, offset = connector.read_table("ad_report_daily", None, {"sync_start_date": "2026-08-23"})

    assert list(rows) == []
    assert offset == {"high_watermark": "2026-08-23"}


def test_future_start_converges_without_http_calls():
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)

    rows_1, offset_1 = connector.read_table(
        "ad_report_daily", None, {"sync_start_date": "2026-08-24"}
    )
    rows_2, offset_2 = connector.read_table(
        "ad_report_daily", offset_1, {"sync_start_date": "2026-08-24"}
    )

    assert list(rows_1) == []
    assert offset_1 == {"high_watermark": "2026-08-23"}
    assert list(rows_2) == []
    assert offset_2 == offset_1
    assert connector._client_instance is None


def test_completed_checkpoint_never_moves_back_when_upper_bound_is_earlier():
    connector = _connector()
    connector._upper_bounds["ad_report_daily"] = date(2026, 8, 23)

    rows, offset = connector.read_table(
        "ad_report_daily",
        {"high_watermark": "2026-08-24"},
        {"lookback_window_days": "1"},
    )

    assert list(rows) == []
    assert offset == {"high_watermark": "2026-08-24"}
    assert connector._client_instance is None


def test_connector_spec_matches_oauth_and_option_contract():
    with open(SOURCE_DIR / "connector_spec.yaml", encoding="utf-8") as handle:
        spec = yaml.safe_load(handle)

    assert spec["display_name"] == "Google Campaign Manager 360"
    parameters = {item["name"]: item for item in spec["connection"]["parameters"]}
    assert set(parameters) == {"client_id", "client_secret", "profile_id"}
    assert parameters["client_id"]["secret"] is True
    assert parameters["client_secret"]["secret"] is True
    assert parameters["profile_id"].get("secret", False) is False
    assert all(item["required"] is True for item in parameters.values())
    assert spec["connection"]["oauth"] == {
        "flow": "u2m",
        "pkce": True,
        "scopes": "https://www.googleapis.com/auth/dfareporting",
        "authorization_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "extra_auth_params": {"access_type": "offline", "prompt": "consent"},
    }
    assert spec["external_options_allowlist"] == (
        "sync_start_date,sync_end_date,initial_lookback_months,lookback_window_days,"
        "reporting_timezone,advertiser_ids,delivery_country_codes,max_records_per_batch"
    )


def test_connector_spec_requests_reporting_scope_only():
    spec = yaml.safe_load((SOURCE_DIR / "connector_spec.yaml").read_text(encoding="utf-8"))

    assert spec["connection"]["oauth"]["scopes"] == ("https://www.googleapis.com/auth/dfareporting")


def test_source_package_dependencies_are_runtime_safe():
    with open(SOURCE_DIR / "pyproject.toml", "rb") as handle:
        project = tomllib.load(handle)

    dependencies = project["project"]["dependencies"]
    assert "pydantic>=2.0.0,<3.0" in dependencies
    assert "requests>=2.28.0,<3.0" in dependencies
    assert "lakeflow-community-connectors>=0.1.0" in dependencies
    assert not any(dependency.lower().startswith("pyspark") for dependency in dependencies)
    assert not any(
        dependency.lower().startswith("google-api-python-client") for dependency in dependencies
    )


def test_public_readme_documents_required_user_contract():
    readme = (SOURCE_DIR / "README.md").read_text(encoding="utf-8")

    for required_text in (
        "https://www.googleapis.com/auth/dfareporting",
        "google_cm360",
        "client_id",
        "client_secret",
        "profile_id",
        *EXPECTED_TABLES,
        "unique_reach_impression_reach",
        "literal `-`",
        "Reach is non-additive",
        "initial_lookback_months",
        "12",
        "sync_end_date",
        "lookback_window_days",
        "30",
        "advertiser_ids",
        "delivery_country_codes",
        "report_file_last_modified_at",
        "qualified_video_views",
        "delivery_country",
        "externalOptionsAllowList",
        "https://<workspace-host>/login/oauth/http.html",
        "coverage_status",
        "global",
    ):
        assert required_text in readme

    for obsolete_claim in (
        "currently exposes only OAuth machine-to-machine fields",
        "cannot create this connector's user-to-machine connection",
        "It keeps the client secret out of shell history and terminal output",
        "https://www.googleapis.com/auth/dfatrafficking",
        "emit_request_telemetry",
    ):
        assert obsolete_claim not in readme


def test_record_mode_keeps_live_configs_and_simulator_validation(monkeypatch):
    from tests.unit.sources.google_cm360.test_google_cm360_lakeflow_connect import (
        TestGoogleCm360Connector,
    )

    monkeypatch.setenv("CONNECTOR_TEST_MODE", "record")
    options = TestGoogleCm360Connector._resolve_mode_and_simulator_args()
    assert options["spec_path"].name == "endpoints.yaml"
    assert options["corpus_dir"].name == "corpus"
    assert TestGoogleCm360Connector._load_table_configs() == {}


def test_contribution_guide_uses_harness_table_config_path():
    from tests.unit.sources.google_cm360.test_google_cm360_lakeflow_connect import (
        TestGoogleCm360Connector,
    )

    config_path = TestGoogleCm360Connector._config_dir() / "dev_table_config.json"
    repository_root = SOURCE_DIR.parents[5]
    guide = (SOURCE_DIR / "CONTRIBUTING.md").read_text(encoding="utf-8")
    assert config_path.relative_to(repository_root).as_posix() in guide


def test_readme_pipeline_example_parses_with_framework():
    from databricks.labs.community_connector.libs.spec_parser import SpecParser

    readme = (SOURCE_DIR / "README.md").read_text(encoding="utf-8")
    example = readme.split("```json\n", 1)[1].split("```", 1)[0]
    parser = SpecParser(json.loads(example))
    assert parser.connection_name() == "cm360_connection"
    assert parser.get_table_list() == ["ad_report_daily"]


def test_api_reference_names_every_output_column():
    api_doc = (SOURCE_DIR / "google_cm360_api_doc.md").read_text(encoding="utf-8")
    connector = _connector()
    for table in connector.list_tables():
        assert f"`{table}`" in api_doc
        for column in connector.get_table_schema(table, {}).fieldNames():
            assert f"`{column}`" in api_doc, (table, column)


# --- ad_report_placement_daily ---------------------------------------------

PLACEMENT_CSV_HEADER = (
    "Advertiser ID,Advertiser,Campaign ID,Campaign,Placement ID,Placement,Impressions,Clicks,"
    "Media Cost,Video Plays,Video Views,Video Mid-points,Video Completions,"
    "DV360 cost (Account currency)"
)
PLACEMENT_CSV_ROW = "3001,Example North America,1001,Summer,500001,Example Video Placement,600,15,70.25,480,360,300,240,1.5"
SECOND_PLACEMENT_CSV_ROW = "3001,Example North America,1001,Summer,500002,Example Banner Placement,400,10,53.2,320,240,200,160,"
PLACEMENT_KEY_FIELDS = ("advertiser_id", "campaign_id", "placement_id")


def _parse_placement_csv(*rows: str) -> list[dict]:
    return _parse_report_csv(
        _report_csv(*rows, header=PLACEMENT_CSV_HEADER),
        profile_id="123",
        expected_report_date=date(2026, 8, 22),
        profile_metadata=EXPECTED_PROFILE_METADATA,
        file_last_modified_at="1787558400000",
        field_specs=PLACEMENT_FIELD_SPECS,
        key_fields=PLACEMENT_KEY_FIELDS,
    )


def test_placement_report_definition_has_exact_daily_contract():
    assert _build_placement_report_definition(date(2026, 8, 23), ("10", "20")) == {
        "name": "lakeflow-google-cm360-placement-2026-08-23",
        "type": "STANDARD",
        "format": "CSV",
        "criteria": {
            "dateRange": {"startDate": "2026-08-23", "endDate": "2026-08-23"},
            "dimensions": [
                {"name": "advertiserId"},
                {"name": "advertiser"},
                {"name": "campaignId"},
                {"name": "campaign"},
                {"name": "placementId"},
                {"name": "placement"},
            ],
            "metricNames": [
                "impressions",
                "clicks",
                "mediaCost",
                "richMediaVideoPlays",
                "richMediaVideoViews",
                "richMediaVideoMidpoints",
                "richMediaVideoCompletions",
                "dv360Cost",
            ],
            "dimensionFilters": [
                {"dimensionName": "advertiser", "id": "10"},
                {"dimensionName": "advertiser", "id": "20"},
            ],
        },
    }
    assert "country" not in json.dumps(_build_placement_report_definition(date(2026, 8, 23), ()))


def test_placement_report_definition_rejects_a_country_filter():
    with pytest.raises(Cm360ConfigurationError, match="delivery-country filter"):
        _build_placement_report_definition(date(2026, 8, 23), (), "2840")


def test_placement_csv_parser_keeps_placements_of_one_campaign_distinct():
    rows = _parse_placement_csv(PLACEMENT_CSV_ROW, SECOND_PLACEMENT_CSV_ROW, "Grand Total:")

    assert rows[0] == {
        "profile_id": "123",
        "account_id": "7001",
        "account_name": "Example Global",
        "subaccount_id": "7101",
        "subaccount_name": "Example Media",
        "report_date": "2026-08-22",
        "advertiser_id": "3001",
        "advertiser_name": "Example North America",
        "campaign_id": "1001",
        "campaign_name": "Summer",
        "placement_id": "500001",
        "placement_name": "Example Video Placement",
        "impressions": 600,
        "clicks": 15,
        "media_cost": "70.250000000",
        "video_starts": 480,
        "qualified_video_views": 360,
        "video_views_50_percent": 300,
        "video_views_100_percent": 240,
        "dv360_cost": "1.500000000",
        "report_file_last_modified_at": "2026-08-24T08:00:00+00:00",
    }
    assert rows[1]["placement_id"] == "500002"
    assert rows[1]["dv360_cost"] is None
    assert set(rows[0]) == {name for name, _, _ in EXPECTED_PLACEMENT_SCHEMA}
    keys = [
        tuple(row[name] for name in EXPECTED_PLACEMENT_METADATA["primary_keys"]) for row in rows
    ]
    assert keys == [
        ("123", "2026-08-22", "3001", "1001", "500001"),
        ("123", "2026-08-22", "3001", "1001", "500002"),
    ]


def test_placement_csv_parser_rejects_non_numeric_placement_id():
    with pytest.raises(Cm360DataError, match="placement_id must be a numeric CM360 ID"):
        _parse_placement_csv(PLACEMENT_CSV_ROW.replace("500001", "P500001"))


def test_placement_csv_parser_rejects_duplicate_placement_key():
    with pytest.raises(Cm360DataError, match="duplicate primary key"):
        _parse_placement_csv(PLACEMENT_CSV_ROW, PLACEMENT_CSV_ROW.replace(",600,", ",599,"))


def test_placement_csv_parser_rejects_the_campaign_report_header():
    with pytest.raises(Cm360DataError, match="header does not match"):
        _parse_report_csv(
            _report_csv(VALID_CSV_ROW),
            profile_id="123",
            expected_report_date=date(2026, 8, 22),
            profile_metadata=EXPECTED_PROFILE_METADATA,
            file_last_modified_at="1787558400000",
            field_specs=PLACEMENT_FIELD_SPECS,
            key_fields=PLACEMENT_KEY_FIELDS,
        )


def test_placement_ignores_the_pipeline_delivery_country_filter(monkeypatch):
    fake_client = _FakeCm360Client(
        _report_csv(PLACEMENT_CSV_ROW, SECOND_PLACEMENT_CSV_ROW, header=PLACEMENT_CSV_HEADER)
    )
    inserted_reports = []
    original_insert = fake_client.insert_report

    def capture_insert(report):
        inserted_reports.append(report)
        return original_insert(report)

    fake_client.insert_report = capture_insert
    connector = _connector()
    connector._client_instance = fake_client
    connector._upper_bounds["ad_report_placement_daily"] = date(2026, 8, 22)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    rows, offset = connector.read_table(
        "ad_report_placement_daily",
        None,
        {
            "sync_start_date": "2026-08-22",
            "sync_end_date": "2026-08-22",
            "delivery_country_codes": '["CH"]',
        },
    )

    assert [row["placement_id"] for row in rows] == ["500001", "500002"]
    assert offset == {"high_watermark": "2026-08-22"}
    assert "dimension-values" not in fake_client.calls
    assert len(inserted_reports) == 1
    assert "country" not in json.dumps(inserted_reports[0])


def test_placement_daily_lifecycle_has_no_country_lookup(monkeypatch):
    fake_client = _FakeCm360Client(
        _report_csv(PLACEMENT_CSV_ROW, SECOND_PLACEMENT_CSV_ROW, header=PLACEMENT_CSV_HEADER)
    )
    connector = _connector()
    connector._client_instance = fake_client
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    rows = connector._execute_daily_report(
        "ad_report_placement_daily",
        date(2026, 8, 22),
        _parse_read_options({}),
        None,
        date(2026, 8, 22),
    )

    assert [row["placement_id"] for row in rows] == ["500001", "500002"]
    assert all("delivery_country" not in row for row in rows)
    assert fake_client.calls == [
        "profile",
        "compatible",
        "insert",
        "run",
        "poll",
        "poll",
        "download",
        "delete",
    ]
