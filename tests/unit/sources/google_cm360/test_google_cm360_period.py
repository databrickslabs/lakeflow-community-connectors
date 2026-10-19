"""Campaign-period snapshots must be complete and must never add reach."""

from datetime import date
from unittest.mock import Mock

import pytest

from databricks.labs.community_connector.sources.google_cm360.google_cm360 import (
    GoogleCm360LakeflowConnect,
)


def test_period_is_an_explicit_snapshot():
    source = GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    assert "reach_report_period" in source.list_tables()
    assert source.read_table_metadata("reach_report_period", {}) == {
        "primary_keys": ["profile_id", "delivery_country", "advertiser_id", "campaign_id"],
        "ingestion_type": "snapshot",
    }


def test_inclusive_period_boundary_and_old_completed_campaign():
    from databricks.labs.community_connector.sources.google_cm360.google_cm360 import (
        _campaign_period,
    )

    assert _campaign_period("2026-07-01", "2026-09-30", date(2026, 10, 13)) == (
        date(2026, 7, 1),
        date(2026, 9, 30),
        "COMPLETE",
    )
    assert _campaign_period("2026-07-01", "2026-10-01", date(2026, 10, 13))[2] == "WINDOW_LIMIT"
    assert _campaign_period("2026-01-01", "2026-01-31", date(2026, 10, 13))[2] == "COMPLETE"
    assert _campaign_period(None, None, date(2026, 10, 13))[2] == "UNPROVEN_START"


def test_native_period_builder_has_no_time_dimension():
    from databricks.labs.community_connector.sources.google_cm360.google_cm360 import (
        _build_period_reach_report,
    )

    report = _build_period_reach_report(date(2026, 7, 15), date(2026, 8, 14), "123", "756")
    criteria = report["reachCriteria"]
    assert criteria["dateRange"] == {"startDate": "2026-07-15", "endDate": "2026-08-14"}
    assert not any(d["name"] in {"date", "month"} for d in criteria["dimensions"])
    assert criteria["metricNames"] == ["uniqueReachImpressionReach"]
    assert {"dimensionName": "campaign", "id": "123"} in criteria["dimensionFilters"]


def test_snapshot_drains_all_campaigns_even_above_batch_cap():
    source = GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    source._period_campaigns = Mock(return_value=[{"campaign_id": str(i)} for i in range(5)])
    source._period_campaign_row = Mock(
        side_effect=lambda campaign, *args: {
            "campaign_id": campaign["campaign_id"],
            "unique_reach_impression_reach": "3",
        }
    )
    rows, offset = source.read_table(
        "reach_report_period",
        None,
        {
            "sync_start_date": "2025-01-01",
            "sync_end_date": "2026-10-13",
            "delivery_country_codes": '["CH"]',
            "max_records_per_batch": "1",
        },
    )
    assert list(rows) == [
        {"campaign_id": str(i), "unique_reach_impression_reach": "3"} for i in range(5)
    ]
    assert offset == {}


def test_failure_after_first_campaign_does_not_return_partial_snapshot():
    source = GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    source._period_campaigns = Mock(return_value=[{"campaign_id": "1"}, {"campaign_id": "2"}])
    source._period_campaign_row = Mock(
        side_effect=[{"campaign_id": "1"}, RuntimeError("source failure")]
    )
    with pytest.raises(RuntimeError, match="source failure"):
        source.read_table(
            "reach_report_period",
            None,
            {
                "sync_start_date": "2025-01-01",
                "sync_end_date": "2026-10-13",
                "delivery_country_codes": '["CH"]',
            },
        )


@pytest.mark.parametrize("value", ["-", "", "0", "3"])
def test_period_parser_preserves_native_reach(value):
    from databricks.labs.community_connector.sources.google_cm360.google_cm360 import (
        ProfileMetadata,
        _parse_report_csv,
    )
    from databricks.labs.community_connector.sources.google_cm360.google_cm360_schemas import (
        PERIOD_REACH_FIELD_SPECS,
    )

    text = (
        "Report Fields\nAdvertiser,Advertiser ID,Campaign,Campaign ID,Country,"
        "Unique Reach: Impression Reach\n"
        f"Example,1,Campaign,2,Switzerland,{value}\nGrand Total:,---,---,---,---,---\n"
    )
    rows = _parse_report_csv(
        text,
        profile_id="123456",
        expected_report_date=date(2026, 10, 13),
        profile_metadata=ProfileMetadata("7001", "Example", None, None),
        file_last_modified_at="1792006000000",
        expected_delivery_country="Switzerland",
        field_specs=PERIOD_REACH_FIELD_SPECS,
    )
    assert rows[0]["unique_reach_impression_reach"] == value


def test_window_limit_never_requests_partial_reach():
    source = GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    source._period_export = Mock(side_effect=AssertionError("must not query a partial window"))
    from databricks.labs.community_connector.sources.google_cm360.google_cm360 import (
        _parse_read_options,
    )

    row = source._period_campaign_row(
        {
            "campaign_id": "2",
            "campaign_start_date": "2026-07-01",
            "campaign_end_date": "2026-10-01",
        },
        _parse_read_options(
            {
                "sync_start_date": "2025-01-01",
                "sync_end_date": "2026-10-13",
                "delivery_country_codes": '["CH"]',
            }
        ),
    )
    assert row["coverage_status"] == "WINDOW_LIMIT"
    assert row["unique_reach_impression_reach"] is None


def test_csv_period_must_match_requested_period(monkeypatch):
    from databricks.labs.community_connector.sources.google_cm360 import google_cm360 as module
    from databricks.labs.community_connector.sources.google_cm360.google_cm360_schemas import (
        PERIOD_REACH_FIELD_SPECS,
    )

    source = GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    source._client_instance = Mock()
    source._period_profile_metadata = module.ProfileMetadata("7001", "Example", None, None)
    source._client_instance.download_file.return_value = (
        "Report Time Zone,Europe/Paris\nDate Range,01/07/2026 - 30/07/2026\n"
        "Report Fields\nAdvertiser,Advertiser ID,Campaign,Campaign ID,Country,"
        "Unique Reach: Impression Reach\n"
        "Example,1,Campaign,2,Switzerland,3\n"
    )
    monkeypatch.setattr(
        module, "_poll_until_available", lambda *args: {"lastModifiedTime": "1792006000000"}
    )
    options = module._parse_read_options(
        {
            "sync_start_date": "2025-01-01",
            "sync_end_date": "2026-10-13",
            "delivery_country_codes": '["CH"]',
            "reporting_timezone": "Europe/Paris",
        }
    )
    report = module._build_period_reach_report(date(2026, 7, 1), date(2026, 7, 31), "2", "222")
    with pytest.raises(module.Cm360DataError, match="period"):
        source._period_export(report, options, PERIOD_REACH_FIELD_SPECS)


@pytest.mark.parametrize("fields_name", ["CAMPAIGN_PERIOD_FIELD_SPECS", "PERIOD_REACH_FIELD_SPECS"])
@pytest.mark.parametrize("keep_data", [True, False])
def test_snapshot_csv_prefix_is_rejected(monkeypatch, fields_name, keep_data):
    from databricks.labs.community_connector.sources.google_cm360 import google_cm360 as module
    from databricks.labs.community_connector.sources.google_cm360 import (
        google_cm360_schemas as schemas,
    )

    fields = getattr(schemas, fields_name)
    source = GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    source._client_instance = Mock()
    source._period_profile_metadata = module.ProfileMetadata("7001", "Example", None, None)
    headers = ",".join(f.csv_header for f in fields)
    values = {
        "Advertiser": "Example",
        "Advertiser ID": "1",
        "Campaign": "Campaign",
        "Campaign ID": "2",
        "Country": "Switzerland",
        "Campaign Start Date": "2026-07-01",
        "Campaign End Date": "2026-07-31",
        "Impressions": "4",
        "Unique Reach: Impression Reach": "3",
    }
    text = (
        "Report Time Zone,Europe/Paris\nDate Range,01/07/2026 - 31/07/2026\nReport Fields\n"
        + headers
        + "\n"
    )
    if keep_data:
        text += ",".join(values[f.csv_header] for f in fields) + "\n"
    source._client_instance.download_file.return_value = text
    monkeypatch.setattr(
        module, "_poll_until_available", lambda *args: {"lastModifiedTime": "1792006000000"}
    )
    options = module._parse_read_options(
        {
            "sync_start_date": "2025-01-01",
            "sync_end_date": "2026-10-13",
            "delivery_country_codes": '["CH"]',
            "reporting_timezone": "Europe/Paris",
        }
    )
    report = module._build_period_reach_report(date(2026, 7, 1), date(2026, 7, 31), "2", "222")
    with pytest.raises(module.Cm360DataError, match="incomplete"):
        source._period_export(report, options, fields)
    source._client_instance.download_file.return_value = (
        text + "Grand Total:," + ",".join(["---"] * (len(fields) - 1)) + "\n"
    )
    assert len(source._period_export(report, options, fields)) == int(keep_data)


@pytest.mark.parametrize(
    "start,end",
    [
        ("2025-01-01", "2026-10-15"),
        ("2025-01-01", "2026-10-16"),
        ("2026-10-14", "2026-10-13"),
    ],
)
def test_period_rejects_incomplete_or_reversed_metadata_bounds(monkeypatch, start, end):
    from datetime import datetime, timezone

    from databricks.labs.community_connector.sources.google_cm360 import google_cm360 as module

    class SourceClock(datetime):
        @classmethod
        def now(cls, tz=None):
            # It is already October 15 in the report timezone, but still October 14 UTC.
            return datetime(2026, 10, 14, 22, 30, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(module, "datetime", SourceClock)
    source = GoogleCm360LakeflowConnect({"access_token": "test", "profile_id": "123456"})
    source._period_campaigns = Mock(return_value=[])
    with pytest.raises(ValueError, match="completed|bounds"):
        source.read_table(
            "reach_report_period",
            None,
            {
                "sync_start_date": start,
                "sync_end_date": end,
                "delivery_country_codes": '["CH"]',
                "reporting_timezone": "Europe/Paris",
            },
        )
    source._period_campaigns.assert_not_called()
