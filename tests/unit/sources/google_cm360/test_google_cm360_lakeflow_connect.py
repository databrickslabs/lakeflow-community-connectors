"""Repository-harness tests for the Google CM360 connector."""

import json
import os

import pytest
from datetime import date
from decimal import Decimal

from databricks.labs.community_connector.sources.google_cm360 import (
    GoogleCm360LakeflowConnect,
)
from databricks.labs.community_connector.sources.google_cm360 import (
    google_cm360 as google_cm360_module,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGoogleCm360Connector(LakeflowConnectTests):
    connector_class = GoogleCm360LakeflowConnect
    simulator_source = "google_cm360"
    sample_records = 6
    read_termination_max_iterations = 45
    replay_config = {"access_token": "simulator-token", "profile_id": "123"}

    @staticmethod
    def _live_mode():
        return os.environ.get("CONNECTOR_TEST_MODE", "simulate").strip().lower() in (
            "live",
            "record",
        )

    @classmethod
    def _resolve_mode_and_simulator_args(cls):
        kwargs = super()._resolve_mode_and_simulator_args()
        if kwargs["mode"] == "record":
            kwargs.update(
                spec_path=cls._simulator_spec_path(), corpus_dir=cls._simulator_corpus_dir()
            )
        return kwargs

    @classmethod
    def _load_table_configs(cls):
        if cls._live_mode():
            return super()._load_table_configs()
        return {
            "ad_report_daily": {"sync_start_date": "2026-08-20"},
            "reach_report_daily": {
                "sync_start_date": "2026-08-20",
                "sync_end_date": "2026-08-23",
            },
            "ad_report_placement_daily": {"sync_start_date": "2026-08-20"},
            "reach_report_period": {
                "sync_start_date": "2026-08-20",
                "sync_end_date": "2026-08-23",
                "delivery_country_codes": '["US"]',
                "reporting_timezone": "UTC",
            },
        }

    @classmethod
    def setup_class(cls):
        cls._original_sleep = google_cm360_module.time.sleep
        if not cls._live_mode():
            google_cm360_module.time.sleep = lambda _seconds: None
        try:
            super().setup_class()
        except Exception:
            google_cm360_module.time.sleep = cls._original_sleep
            raise
        if not cls._live_mode():
            cls.connector._upper_bounds = {
                "ad_report_daily": date(2026, 8, 23),
                "reach_report_daily": date(2026, 8, 23),
                "ad_report_placement_daily": date(2026, 8, 23),
            }

    @classmethod
    def teardown_class(cls):
        try:
            super().teardown_class()
        finally:
            google_cm360_module.time.sleep = cls._original_sleep

    def _simulator_state(self):
        if self._live_mode():
            pytest.skip("Synthetic simulator assertion; shared contract tests cover live reads")
        return self._record_replay_patch.corpus.tables.setdefault("_state", {})

    def test_shared_harness_includes_period_snapshot(self):
        assert set(self._tables()) == set(self.connector.list_tables())

    def _period_options(self):
        return {
            "sync_start_date": "2026-08-20",
            "sync_end_date": "2026-08-23",
            "delivery_country_codes": '["US"]',
            "reporting_timezone": "UTC",
            "max_records_per_batch": "1",
        }

    def test_simulator_materializes_complete_period_snapshot(self):
        state = self._simulator_state()
        state.clear()
        rows, offset = self.connector_class(self.config).read_table(
            "reach_report_period", None, self._period_options()
        )
        rows = list(rows)
        assert len(rows) == 6
        assert offset == {}
        assert [r["unique_reach_impression_reach"] for r in rows] == [
            "1500",
            "-",
            "",
            None,
            None,
            "0",
        ]
        assert [r["coverage_status"] for r in rows] == [
            "COMPLETE",
            "COMPLETE",
            "COMPLETE",
            "WINDOW_LIMIT",
            "UNPROVEN_START",
            "COMPLETE",
        ]
        assert all(r["source_cutoff_date"] == "2026-08-23" for r in rows)
        assert all(r["delivery_country"] == "United States" for r in rows)
        assert all(
            r["period_start"] == "2026-08-01" for r in rows if r["coverage_status"] == "COMPLETE"
        )
        assert state["reports"] == {}
        operations = [e["operation"] for e in state["request_log"]]
        assert operations.count("download") == 5  # discovery plus four native reach reports
        assert operations.count("delete") == 5

    def test_simulator_period_snapshot_can_be_empty(self):
        self._simulator_state().clear()
        rows, offset = self.connector_class(self.config).read_table(
            "reach_report_period",
            None,
            {**self._period_options(), "advertiser_ids": '["9999"]'},
        )
        assert list(rows) == []
        assert offset == {}

    def test_simulator_period_late_failure_returns_no_partial_snapshot(self):
        state = self._simulator_state()
        state.clear()
        state["truncate_campaign_id"] = "4002"
        with pytest.raises(google_cm360_module.Cm360DataError, match="incomplete"):
            self.connector_class(self.config).read_table(
                "reach_report_period", None, self._period_options()
            )
        operations = [e["operation"] for e in state["request_log"]]
        assert operations.count("download") == 3  # discovery, success, then failure
        assert state["reports"] == {}

    def test_simulator_runs_full_report_sequence(self):
        state = self._simulator_state()
        state.clear()
        connector = self.connector_class(self.config)
        connector._upper_bounds["ad_report_daily"] = date(2026, 8, 20)

        rows, offset = connector.read_table(
            "ad_report_daily", None, {"sync_start_date": "2026-08-20"}
        )

        rows = list(rows)
        assert rows
        assert offset == {"high_watermark": "2026-08-20"}
        assert {row["account_id"] for row in rows} == {"7001"}
        assert {row["delivery_country"] for row in rows} == {
            "Canada",
            "Germany",
            "United States",
        }
        assert {(row["advertiser_id"], row["advertiser_name"]) for row in rows} == {
            ("3001", "Example North America"),
            ("3002", "Example Europe"),
        }
        operations = [entry["operation"] for entry in state["request_log"]]
        assert operations == [
            "profile",
            "compatible",
            "insert",
            "run",
            "poll",
            "poll",
            "download",
            "delete",
        ]

    def test_simulator_filters_one_delivery_country(self):
        state = self._simulator_state()
        state.clear()
        connector = self.connector_class(self.config)
        connector._upper_bounds["ad_report_daily"] = date(2026, 8, 20)

        rows, offset = connector.read_table(
            "ad_report_daily",
            None,
            {
                "sync_start_date": "2026-08-20",
                "sync_end_date": "2026-08-20",
                "delivery_country_codes": '["US"]',
            },
        )

        rows = list(rows)
        assert rows
        assert offset == {"high_watermark": "2026-08-20"}
        assert {row["delivery_country"] for row in rows} == {"United States"}
        assert [entry["operation"] for entry in state["request_log"]] == [
            "profile",
            "dimension-values",
            "compatible",
            "insert",
            "run",
            "poll",
            "poll",
            "download",
            "delete",
        ]

    def test_simulator_runs_reach_report_sequence(self):
        state = self._simulator_state()
        state.clear()
        connector = self.connector_class(self.config)
        connector._upper_bounds = {"reach_report_daily": date(2026, 8, 20)}

        rows, offset = connector.read_table(
            "reach_report_daily",
            None,
            {
                "sync_start_date": "2026-08-20",
                "sync_end_date": "2026-08-20",
                "delivery_country_codes": '["US"]',
            },
        )

        rows = list(rows)
        assert [row["unique_reach_impression_reach"] for row in rows] == ["1000", "-", None]
        assert offset == {"high_watermark": "2026-08-20"}
        assert [entry["operation"] for entry in state["request_log"]] == [
            "profile",
            "dimension-values",
            "compatible",
            "insert",
            "run",
            "poll",
            "poll",
            "download",
            "delete",
        ]

    def test_replay_same_date_keeps_keys_and_advances_file_timestamp(self):
        state = self._simulator_state()
        state.clear()

        first = self.connector_class(self.config)
        first._upper_bounds["ad_report_daily"] = date(2026, 8, 20)
        first_rows, _ = first.read_table("ad_report_daily", None, {"sync_start_date": "2026-08-20"})

        second = self.connector_class(self.config)
        second._upper_bounds["ad_report_daily"] = date(2026, 8, 20)
        second_rows, _ = second.read_table(
            "ad_report_daily", None, {"sync_start_date": "2026-08-20"}
        )

        first_rows = list(first_rows)
        second_rows = list(second_rows)
        assert [
            (
                row["profile_id"],
                row["report_date"],
                row["delivery_country"],
                row["advertiser_id"],
                row["campaign_id"],
            )
            for row in first_rows
        ] == [
            (
                row["profile_id"],
                row["report_date"],
                row["delivery_country"],
                row["advertiser_id"],
                row["campaign_id"],
            )
            for row in second_rows
        ]
        assert [
            {key: value for key, value in row.items() if key != "report_file_last_modified_at"}
            for row in first_rows
        ] == [
            {key: value for key, value in row.items() if key != "report_file_last_modified_at"}
            for row in second_rows
        ]
        assert (
            second_rows[0]["report_file_last_modified_at"]
            > first_rows[0]["report_file_last_modified_at"]
        )

    def test_adjacent_days_keep_same_entity_ids_in_distinct_keys(self):
        self._simulator_state().clear()
        keys = []
        for report_day in (date(2026, 8, 20), date(2026, 8, 21)):
            connector = self.connector_class(self.config)
            connector._upper_bounds["ad_report_daily"] = report_day
            rows, _ = connector.read_table(
                "ad_report_daily",
                None,
                {"sync_start_date": report_day.isoformat()},
            )
            matching = [
                row
                for row in rows
                if row["campaign_id"] == "1001" and row["delivery_country"] == "United States"
            ]
            assert len(matching) == 1
            keys.append(
                tuple(
                    matching[0][name]
                    for name in (
                        "profile_id",
                        "report_date",
                        "delivery_country",
                        "advertiser_id",
                        "campaign_id",
                    )
                )
            )

        assert keys == [
            ("123", "2026-08-20", "United States", "3001", "1001"),
            ("123", "2026-08-21", "United States", "3001", "1001"),
        ]
        assert len(set(keys)) == 2

    @pytest.mark.parametrize("table", ["ad_report_daily", "reach_report_daily"])
    @pytest.mark.parametrize(
        "advertiser_ids, expected",
        [
            ([], {"3001", "3002"}),
            (["3001"], {"3001"}),
            (["3002"], {"3002"}),
            (["3001", "3002"], {"3001", "3002"}),
            (["9999"], set()),
        ],
    )
    def test_simulator_daily_reports_filter_advertisers(self, table, advertiser_ids, expected):
        self._simulator_state().clear()
        connector = self.connector_class(self.config)
        rows, offset = connector.read_table(
            table,
            None,
            {
                "sync_start_date": "2026-08-20",
                "sync_end_date": "2026-08-20",
                "advertiser_ids": json.dumps(advertiser_ids),
            },
        )
        assert {row["advertiser_id"] for row in rows} == expected
        assert offset == {"high_watermark": "2026-08-20"}

    @pytest.mark.parametrize(
        "advertiser_ids, expected",
        [
            (["3001"], {"3001"}),
            (["3002"], {"3002"}),
            (["3001", "3002"], {"3001", "3002"}),
            (["9999"], set()),
        ],
    )
    def test_simulator_placement_report_filters_advertisers(self, advertiser_ids, expected):
        self._simulator_state().clear()
        connector = self.connector_class(self.config)
        connector._upper_bounds["ad_report_placement_daily"] = date(2026, 8, 20)
        rows, offset = connector.read_table(
            "ad_report_placement_daily",
            None,
            {
                "sync_start_date": "2026-08-20",
                "sync_end_date": "2026-08-20",
                "advertiser_ids": json.dumps(advertiser_ids),
                "delivery_country_codes": '["CH"]',
            },
        )
        rows = list(rows)
        assert {row["advertiser_id"] for row in rows} == expected
        assert len(rows) == 2 * len(expected)
        assert offset == {"high_watermark": "2026-08-20"}

    def test_simulator_placement_report_matches_campaign_totals(self):
        state = self._simulator_state()
        state.clear()
        connector = self.connector_class(self.config)
        connector._upper_bounds = {
            "ad_report_daily": date(2026, 8, 20),
            "ad_report_placement_daily": date(2026, 8, 20),
        }
        options = {"sync_start_date": "2026-08-20", "sync_end_date": "2026-08-20"}

        placement_rows, offset = connector.read_table("ad_report_placement_daily", None, options)
        placement_rows = list(placement_rows)
        placement_operations = [entry["operation"] for entry in state["request_log"]]
        campaign_rows, _ = connector.read_table("ad_report_daily", None, options)
        campaign_rows = list(campaign_rows)

        assert offset == {"high_watermark": "2026-08-20"}
        assert placement_operations == [
            "profile",
            "compatible",
            "insert",
            "run",
            "poll",
            "poll",
            "download",
            "delete",
        ]
        assert all("delivery_country" not in row for row in placement_rows)
        keys = [
            (
                row["profile_id"],
                row["report_date"],
                row["advertiser_id"],
                row["campaign_id"],
                row["placement_id"],
            )
            for row in placement_rows
        ]
        assert len(keys) == len(set(keys))
        placements_per_campaign = {}
        for row in placement_rows:
            placements_per_campaign.setdefault(row["campaign_id"], set()).add(row["placement_id"])
        assert placements_per_campaign
        assert all(len(ids) >= 2 for ids in placements_per_campaign.values())

        def totals(rows):
            result = {}
            for row in rows:
                key = (row["advertiser_id"], row["campaign_id"])
                bucket = result.setdefault(key, {"media_cost": Decimal("0")})
                bucket["media_cost"] += Decimal(row["media_cost"])
                for name in (
                    "impressions",
                    "clicks",
                    "video_starts",
                    "qualified_video_views",
                    "video_views_50_percent",
                    "video_views_100_percent",
                ):
                    bucket[name] = bucket.get(name, 0) + row[name]
            return result

        assert totals(placement_rows) == totals(campaign_rows)
