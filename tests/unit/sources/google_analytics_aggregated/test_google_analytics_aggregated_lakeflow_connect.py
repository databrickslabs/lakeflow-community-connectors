import json
from typing import Any, Dict, Optional

import pytest

from databricks.labs.community_connector.sources.google_analytics_aggregated.google_analytics_aggregated import GoogleAnalyticsAggregatedLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


def _build_simulator_creds() -> Dict[str, Any]:
    """Build a fake service-account config with a real-shaped RSA key.

    Google's ``service_account.Credentials.from_service_account_info`` parses
    the private_key as PEM RSA, so a real key shape is required. We generate
    one in-process — never used to sign anything since the simulator returns
    a canned access token.
    """
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    creds = {
        "type": "service_account",
        "project_id": "simulator-project",
        "private_key_id": "simulator-key-id",
        "private_key": pem,
        "client_email": "simulator@simulator-project.iam.gserviceaccount.com",
        "client_id": "0",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    return {
        "property_ids": '["123456789"]',
        "credentials_json": json.dumps(creds),
    }


class TestGoogleAnalyticsAggregatedConnector(LakeflowConnectTests):
    connector_class = GoogleAnalyticsAggregatedLakeflowConnect
    simulator_source = "google_analytics_aggregated"
    _generated_replay_config: Optional[Dict[str, Any]] = None

    @classmethod
    def _replay_config(cls) -> Optional[Dict[str, Any]]:
        # Cache across the test class — RSA generation is non-trivial.
        if cls._generated_replay_config is None:
            cls._generated_replay_config = _build_simulator_creds()
        return cls._generated_replay_config

    def test_invalid_table_name(self):
        """GA4 accepts arbitrary table names by design (custom reports built
        from request-time dimensions/metrics), so the harness's "must reject
        unknown table" contract doesn't apply."""
        pytest.skip("GA4 accepts arbitrary table names as custom report definitions.")

    # Extra Google Analytics Aggregated specific integration tests.

    # ------------------------------------------------------------------
    # Dimension / metric validation
    # ------------------------------------------------------------------

    def test_invalid_dimension(self):
        """Invalid dimension should raise ValueError."""
        invalid_options = {"dimensions": '["date", "contry"]', "metrics": '["activeUsers"]'}
        with pytest.raises(ValueError, match="Unknown dimensions"):
            self.connector.get_table_schema("test_invalid", invalid_options)

    def test_invalid_metric(self):
        """Invalid metric should raise ValueError."""
        invalid_options = {"dimensions": '["date"]', "metrics": '["activUsers"]'}
        with pytest.raises(ValueError, match="Unknown metrics"):
            self.connector.get_table_schema("test_invalid", invalid_options)

    def test_multiple_invalid_fields(self):
        """Multiple invalid dimensions and metrics should all be reported."""
        invalid_options = {
            "dimensions": '["date", "contry", "deivce"]',
            "metrics": '["activUsers", "sesions"]',
        }
        with pytest.raises(ValueError) as exc_info:
            self.connector.get_table_schema("test_invalid", invalid_options)
        error_msg = str(exc_info.value)
        assert "Unknown dimensions" in error_msg
        assert "Unknown metrics" in error_msg
        assert "contry" in error_msg and "deivce" in error_msg
        assert "activUsers" in error_msg and "sesions" in error_msg

    def test_too_many_dimensions(self):
        """10 dimensions should be rejected (maximum 9)."""
        options = {
            "dimensions": '["date", "country", "city", "deviceCategory", "browser", "operatingSystem", "language", "sessionSource", "sessionMedium", "newVsReturning"]',
            "metrics": '["activeUsers"]',
            "start_date": "7daysAgo",
        }
        with pytest.raises(ValueError, match="Too many dimensions"):
            self.connector.get_table_schema("test_10_dimensions", options)

    def test_too_many_metrics(self):
        """11 metrics should be rejected (maximum 10)."""
        options = {
            "dimensions": '["date"]',
            "metrics": '["activeUsers", "sessions", "screenPageViews", "eventCount", "newUsers", "engagementRate", "averageSessionDuration", "bounceRate", "sessionsPerUser", "screenPageViewsPerSession", "totalUsers"]',
            "start_date": "7daysAgo",
        }
        with pytest.raises(ValueError, match="Too many metrics"):
            self.connector.get_table_schema("test_11_metrics", options)

    def test_combined_validation_issues(self):
        """Combined limit violations and unknown fields should all be caught."""
        options = {
            "dimensions": '["date", "country", "city", "deviceCategory", "browser", "operatingSystem", "language", "sessionSource", "sessionMedium", "invalidDim"]',
            "metrics": '["activeUsers", "sessions", "screenPageViews", "eventCount", "newUsers", "engagementRate", "averageSessionDuration", "bounceRate", "sessionsPerUser", "screenPageViewsPerSession", "invalidMetric"]',
            "start_date": "7daysAgo",
        }
        with pytest.raises(ValueError) as exc_info:
            self.connector.get_table_schema("test_validation_combined", options)
        error_msg = str(exc_info.value)
        assert "Too many dimensions" in error_msg
        assert "Too many metrics" in error_msg

    # ------------------------------------------------------------------
    # Prebuilt report loading, caching, and usage
    # ------------------------------------------------------------------

    def test_prebuilt_report_loading(self):
        """Prebuilt reports should load as a non-empty dict."""
        prebuilt_reports = self.connector._load_prebuilt_reports()
        assert isinstance(prebuilt_reports, dict)
        assert len(prebuilt_reports) >= 1

    def test_prebuilt_reports_caching(self):
        """Prebuilt reports should be cached (same object on repeated calls)."""
        reports1 = self.connector._load_prebuilt_reports()
        reports2 = self.connector._load_prebuilt_reports()
        assert reports1 is reports2

    def test_invalid_prebuilt_report_name(self):
        """Invalid prebuilt report name should raise ValueError."""
        invalid_options = {"prebuilt_report": "nonexistent_report"}
        with pytest.raises(ValueError, match="not found"):
            self.connector._resolve_table_options(invalid_options)

    def test_prebuilt_report_by_table_name(self):
        """Prebuilt reports should work using just table name (no config needed)."""
        tables = self.connector.list_tables()
        assert "traffic_by_country" in tables

        empty_options = {}
        schema = self.connector.get_table_schema("traffic_by_country", empty_options)
        assert schema is not None
        field_names = [f.name for f in schema.fields]
        assert "date" in field_names
        assert "country" in field_names
        assert "property_id" in field_names
        assert field_names[0] == "property_id"

        metadata = self.connector.read_table_metadata("traffic_by_country", empty_options)
        assert metadata["primary_keys"] == ["property_id", "date", "country"]

        records, offset = self.connector.read_table("traffic_by_country", {}, empty_options)
        records_list = list(records)
        assert len(records_list) > 0
        if records_list:
            assert "property_id" in records_list[0]

    def test_prebuilt_report_with_overrides(self):
        """Prebuilt report options can be overridden."""
        override_options = {
            "prebuilt_report": "traffic_by_country",
            "start_date": "7daysAgo",
            "lookback_days": "1",
        }
        resolved = self.connector._resolve_table_options(override_options)
        assert "dimensions" in resolved
        assert "metrics" in resolved
        assert resolved["start_date"] == "7daysAgo"
        assert resolved["lookback_days"] == "1"

    def test_shadow_prebuilt_with_custom_config(self):
        """Custom dimensions should override prebuilt report."""
        shadow_options = {
            "dimensions": '["date", "city"]',
            "metrics": '["sessions"]',
            "primary_keys": ["property_id", "date", "city"],
        }
        schema = self.connector.get_table_schema("traffic_by_country", shadow_options)
        field_names = [f.name for f in schema.fields]
        assert "city" in field_names
        assert "country" not in field_names

        metadata = self.connector.read_table_metadata("traffic_by_country", shadow_options)
        assert metadata["primary_keys"] == ["property_id", "date", "city"]

    # ------------------------------------------------------------------
    # property_id field handling (schema stability)
    # ------------------------------------------------------------------

    def test_property_id_always_in_schema(self):
        """property_id should always be the first field in schema."""
        schema = self.connector.get_table_schema("traffic_by_country", {})
        field_names = [f.name for f in schema.fields]
        assert "property_id" in field_names
        assert field_names[0] == "property_id"

    def test_property_id_always_in_primary_keys(self):
        """property_id should always be first in primary_keys."""
        metadata = self.connector.read_table_metadata("traffic_by_country", {})
        assert "property_id" in metadata["primary_keys"]
        assert metadata["primary_keys"][0] == "property_id"

    def test_single_property_mode(self):
        """Single property connector should still include property_id for schema stability."""
        single_config = self.config.copy()
        single_config["property_ids"] = [self.connector.property_ids[0]]
        single_connector = GoogleAnalyticsAggregatedLakeflowConnect(single_config)
        assert len(single_connector.property_ids) == 1

        empty_options = {}
        schema = single_connector.get_table_schema("traffic_by_country", empty_options)
        field_names = [f.name for f in schema.fields]
        assert "property_id" in field_names
        assert field_names[0] == "property_id"

        metadata = single_connector.read_table_metadata("traffic_by_country", empty_options)
        assert "property_id" in metadata["primary_keys"]
        assert metadata["primary_keys"] == ["property_id", "date", "country"]

        records, _ = single_connector.read_table("traffic_by_country", {}, empty_options)
        records_list = list(records)
        if records_list:
            assert "property_id" in records_list[0]

    def test_custom_report_with_explicit_primary_keys(self):
        """Custom report with explicit primary_keys should use them as-is."""
        options = {
            "dimensions": '["date", "country", "city"]',
            "metrics": '["sessions"]',
            "primary_keys": ["property_id", "city", "date"],
            "start_date": "7daysAgo",
        }
        metadata = self.connector.read_table_metadata("custom_with_pk", options)
        assert metadata["primary_keys"] == ["property_id", "city", "date"]

    def test_custom_report_without_primary_keys_infers_from_dimensions(self):
        """Custom report without explicit primary_keys should infer from dimensions."""
        options = {
            "dimensions": '["date", "country"]',
            "metrics": '["sessions"]',
            "start_date": "7daysAgo",
        }
        metadata = self.connector.read_table_metadata("custom_without_pk", options)
        assert metadata["primary_keys"] == ["property_id", "date", "country"]

    # ------------------------------------------------------------------
    # GA4 API date range limits
    # ------------------------------------------------------------------

    def test_five_date_ranges_rejected_by_api(self):
        """5 date ranges should be rejected by the GA4 API (limit is 4)."""
        request_body = {
            "dateRanges": [
                {"startDate": "2024-01-01", "endDate": "2024-01-07"},
                {"startDate": "2024-01-08", "endDate": "2024-01-14"},
                {"startDate": "2024-01-15", "endDate": "2024-01-21"},
                {"startDate": "2024-01-22", "endDate": "2024-01-28"},
                {"startDate": "2024-01-29", "endDate": "2024-02-04"},
            ],
            "dimensions": [{"name": "date"}],
            "metrics": [{"name": "activeUsers"}],
            "limit": 100,
        }
        with pytest.raises(Exception) as exc_info:
            self.connector._make_api_request(
                "runReport", request_body, self.connector.property_ids[0]
            )
        error_msg = str(exc_info.value)
        assert "400" in error_msg and "dateRange" in error_msg
