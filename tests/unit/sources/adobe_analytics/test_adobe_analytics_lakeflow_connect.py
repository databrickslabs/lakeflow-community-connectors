from zoneinfo import ZoneInfo

import pytest

from databricks.labs.community_connector.source_simulator import MODE_SIMULATE
from databricks.labs.community_connector.sources.adobe_analytics import adobe_analytics_client
from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics import (
    AdobeAnalyticsLakeflowConnect,
)
from tests.unit.sources.test_suite import (
    LakeflowConnectTests,
    _resolve_env_mode_for_simulator,
)

# Recording is disabled for this connector. Live/record modes would write account
# data (IDs, component names, traffic numbers) into cassettes and coverage reports,
# and the token response holds a live access token, which the recorder does not yet
# scrub. Stop at collection, before the harness opens a cassette.
if _resolve_env_mode_for_simulator("adobe_analytics") != MODE_SIMULATE:
    pytest.exit(
        "Live and record modes are disabled for adobe_analytics so nothing from a live "
        "account is written to disk. Unset CONNECTOR_TEST_MODE to run the offline simulator "
        "tests, or run tests/unit/sources/adobe_analytics/live_smoke_check.py for a "
        "read-only live check.",
        returncode=2,
    )


class TestAdobeAnalyticsConnector(LakeflowConnectTests):
    connector_class = AdobeAnalyticsLakeflowConnect
    simulator_source = "adobe_analytics"
    sample_records = 50
    replay_config = {
        "client_id": "example-client-id",
        "client_secret": "example-client-secret",
        "global_company_id": "examplecompany",
        "scopes": adobe_analytics_client.DEFAULT_SCOPES,
    }

    _saved_rate_limit = None

    @classmethod
    def setup_class(cls):
        # The client's 10-requests-per-6-s throttle protects the real API; the
        # in-process simulator needs no protection and would only slow the run.
        if _resolve_env_mode_for_simulator(cls.simulator_source) == MODE_SIMULATE:
            cls._saved_rate_limit = adobe_analytics_client.RATE_LIMIT_REQUESTS
            adobe_analytics_client.RATE_LIMIT_REQUESTS = 10**9
        super().setup_class()

    @classmethod
    def teardown_class(cls):
        super().teardown_class()
        if cls._saved_rate_limit is not None:
            adobe_analytics_client.RATE_LIMIT_REQUESTS = cls._saved_rate_limit
            cls._saved_rate_limit = None

    def _tables(self):
        # Report tables are user-defined, so list_tables() never returns them.
        # Every non-built-in entry in dev_table_config.json is a report table.
        builtin = self.connector.list_tables()
        return builtin + [t for t in self.table_configs if t not in builtin]

    # ------------------------------------------------------------------
    # Report-table routing and metadata
    # ------------------------------------------------------------------

    def test_report_tables_are_exercised(self):
        reports = [t for t in self._tables() if t not in self.connector.list_tables()]
        assert len(reports) >= 5, reports

    def test_report_metadata_ingestion_types(self):
        meta = self.connector.read_table_metadata
        segmented = meta(
            "report_daily_by_evar1_segmented", self._opts("report_daily_by_evar1_segmented")
        )
        assert segmented["ingestion_type"] == "cdc_with_deletes"
        assert segmented["primary_keys"] == ["rsid", "date", "dimension_value", "segment_ids"]
        assert segmented["cursor_field"] == "retrieved_at"

        hourly = meta("report_hourly_trended", self._opts("report_hourly_trended"))
        assert hourly["ingestion_type"] == "cdc"
        assert hourly["primary_keys"] == ["rsid", "date", "hour", "segment_ids"]

        vrs = meta("report_vrs_daily_by_page", self._opts("report_vrs_daily_by_page"))
        assert vrs["ingestion_type"] == "cdc"  # delete_stale_rows=false

    def test_segmented_report_records(self):
        opts = self._opts("report_daily_by_evar1_segmented")
        records, offset = self.connector.read_table("report_daily_by_evar1_segmented", {}, opts)
        records = list(records)
        assert records
        assert {r["segment_ids"] for r in records} == {"s000000000_000000000000000000000001"}
        assert all(r["dimension_value"] for r in records)
        assert "cm000000000_000000000000000000000001" in records[0]
        # max_records_per_batch=30 stops a 31-day backfill early and asks to resume.
        assert offset.get("resume") is True

    def test_trended_report_has_one_row_per_bucket(self):
        opts = self._opts("report_hourly_trended")
        records, _ = self.connector.read_table("report_hourly_trended", {}, opts)
        keys = [(r["date"], r["hour"]) for r in records]
        assert keys and len(keys) == len(set(keys))
        assert all(0 <= hour <= 23 for _, hour in keys)

    def test_virtual_report_suite_uses_its_own_time_zone(self):
        assert self.connector._report_timezone("vrs_examplersid") == ZoneInfo("America/Los_Angeles")

    def test_virtual_report_suite_without_time_zone_uses_parent(self):
        assert self.connector._report_timezone("vrs_examplersid2") == ZoneInfo("America/New_York")

    # ------------------------------------------------------------------
    # Company auto-discovery (/discovery/me)
    # ------------------------------------------------------------------

    def _connector_without_company(self, client_id: str) -> AdobeAnalyticsLakeflowConnect:
        # The simulator's discovery handler picks its scenario from the client id.
        options = {k: v for k, v in self.config.items() if k != "global_company_id"}
        options["client_id"] = client_id
        return AdobeAnalyticsLakeflowConnect(options)

    def test_discovery_selects_the_only_company(self):
        connector = self._connector_without_company("example-client-id")
        records, _ = connector.read_table("report_suites", {}, {})
        assert list(records)
        assert connector._client.global_company_id == "examplecompany"

    def test_discovery_without_companies_explains_how_to_grant_access(self):
        connector = self._connector_without_company("example-client-id-no-company")
        with pytest.raises(ValueError, match="no access to any Adobe Analytics company") as exc:
            connector.read_table("report_suites", {}, {})
        assert "product profile" in str(exc.value)
        assert "global_company_id" in str(exc.value)

    def test_discovery_with_several_companies_lists_the_ids(self):
        connector = self._connector_without_company("example-client-id-two-companies")
        with pytest.raises(ValueError, match="more than one Adobe Analytics company") as exc:
            connector.read_table("report_suites", {}, {})
        message = str(exc.value)
        assert "examplecompany (Example Company)" in message
        assert "examplecompany2 (Second Example Company)" in message
        assert "global_company_id" in message

    # ------------------------------------------------------------------
    # Validation against the metric / dimension catalog
    # ------------------------------------------------------------------

    def test_unknown_metric_is_rejected_with_suggestion(self):
        opts = {"rsid": "examplersid", "metrics": "metrics/visitz"}
        with pytest.raises(ValueError, match=r"unknown metric 'metrics/visitz'.*metrics/visits"):
            self.connector.get_table_schema("bad_metric_report", opts)

    def test_unknown_dimension_is_rejected(self):
        opts = {"rsid": "examplersid", "metrics": "metrics/visits", "dimension": "variables/evar99"}
        with pytest.raises(ValueError, match="unknown dimension 'variables/evar99'"):
            self.connector.get_table_schema("bad_dimension_report", opts)

    def test_builtin_table_name_cannot_be_a_report(self):
        with pytest.raises(ValueError, match="built-in table name"):
            self.connector.get_table_schema(
                "segments", {"rsid": "examplersid", "metrics": "metrics/visits"}
            )
