"""
Validation tests for the Google Analytics Aggregated connector.

These tests verify error handling, input validation, and edge cases
without requiring real API credentials. They improve test coverage
for the connector's initialization and configuration logic.

Run with: pytest sources/google_analytics_aggregated/test/test_connector_validation.py -v
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from databricks.labs.community_connector.sources.google_analytics_aggregated.google_analytics_aggregated import GoogleAnalyticsAggregatedLakeflowConnect


# Valid fake credentials for testing
# Build the fake private key string dynamically to avoid secret scanner false positives
_FAKE_PK = f"-----BEGIN RSA {'PRIVATE'} KEY-----\nfake\n-----END RSA {'PRIVATE'} KEY-----\n"
VALID_CREDENTIALS = json.dumps({
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "fake-key-id",
    "private_key": _FAKE_PK,
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
})

# Fake metadata for mocking
FAKE_METADATA = {
    "metric_types": {
        "activeUsers": "TYPE_INTEGER",
        "sessions": "TYPE_INTEGER",
        "screenPageViews": "TYPE_INTEGER",
        "newUsers": "TYPE_INTEGER",
        "eventCount": "TYPE_INTEGER",
        "engagementRate": "TYPE_FLOAT",
        "averageSessionDuration": "TYPE_FLOAT",
        "bounceRate": "TYPE_FLOAT",
    },
    "available_dimensions": {
        "date", "country", "city", "sessionSource", "sessionMedium",
        "eventName", "pagePath", "pageTitle", "deviceCategory", "browser",
    },
    "available_metrics": {
        "activeUsers", "sessions", "screenPageViews", "newUsers",
        "eventCount", "engagementRate", "averageSessionDuration", "bounceRate",
    },
}


def mock_credentials():
    """Create a mock credentials object."""
    mock_creds = MagicMock()
    mock_creds.valid = True
    mock_creds.token = "fake_token"
    return mock_creds


class TestInitValidation:
    """Test validation in __init__ method."""

    def test_missing_property_ids_raises_error(self):
        """Connector should raise error when property_ids is missing."""
        with pytest.raises(ValueError, match="requires 'property_ids'"):
            GoogleAnalyticsAggregatedLakeflowConnect({"credentials_json": VALID_CREDENTIALS})

    def test_empty_property_ids_string_raises_error(self):
        """Connector should raise error for empty property_ids list."""
        with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()):
            with pytest.raises(ValueError, match="non-empty list"):
                GoogleAnalyticsAggregatedLakeflowConnect({
                    "property_ids": "[]",
                    "credentials_json": VALID_CREDENTIALS
                })

    def test_property_ids_not_list_raises_error(self):
        """Connector should raise error when property_ids is not a list."""
        with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()):
            with pytest.raises(ValueError, match="non-empty list"):
                GoogleAnalyticsAggregatedLakeflowConnect({
                    "property_ids": '"123456789"',  # String, not list
                    "credentials_json": VALID_CREDENTIALS
                })

    def test_property_ids_with_non_string_raises_error(self):
        """Connector should raise error when property_ids contains non-strings."""
        with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()):
            with pytest.raises(ValueError, match="must be strings"):
                GoogleAnalyticsAggregatedLakeflowConnect({
                    "property_ids": "[123456789]",  # Number, not string
                    "credentials_json": VALID_CREDENTIALS
                })

    def test_invalid_property_ids_json_raises_error(self):
        """Connector should raise error for malformed JSON in property_ids."""
        with pytest.raises(ValueError, match="Invalid 'property_ids'"):
            GoogleAnalyticsAggregatedLakeflowConnect({
                "property_ids": "[invalid json",
                "credentials_json": VALID_CREDENTIALS
            })

    def test_missing_credentials_raises_error(self):
        """Connector should raise error when credentials_json is missing."""
        with pytest.raises(ValueError, match="requires 'credentials_json'"):
            GoogleAnalyticsAggregatedLakeflowConnect({"property_ids": '["123456789"]'})

    def test_invalid_credentials_json_raises_error(self):
        """Connector should raise error for malformed JSON in credentials."""
        with pytest.raises(ValueError, match="Invalid JSON"):
            GoogleAnalyticsAggregatedLakeflowConnect({
                "property_ids": '["123456789"]',
                "credentials_json": "not-valid-json"
            })

    def test_missing_credential_fields_raises_error(self):
        """Connector should raise error when required credential fields are missing."""
        incomplete_creds = json.dumps({"type": "service_account"})  # Missing fields
        with pytest.raises(ValueError, match="missing required fields"):
            GoogleAnalyticsAggregatedLakeflowConnect({
                "property_ids": '["123456789"]',
                "credentials_json": incomplete_creds
            })

    def test_valid_single_property_initializes(self):
        """Connector should initialize with valid single property."""
        with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()):
            connector = GoogleAnalyticsAggregatedLakeflowConnect({
                "property_ids": '["123456789"]',
                "credentials_json": VALID_CREDENTIALS
            })
            assert connector.property_ids == ["123456789"]
            assert len(connector.property_ids) == 1

    def test_valid_multiple_properties_initializes(self):
        """Connector should initialize with valid multiple properties."""
        with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()):
            connector = GoogleAnalyticsAggregatedLakeflowConnect({
                "property_ids": '["123456789", "987654321"]',
                "credentials_json": VALID_CREDENTIALS
            })
            assert connector.property_ids == ["123456789", "987654321"]
            assert len(connector.property_ids) == 2

    def test_property_ids_as_list_directly(self):
        """Connector should accept property_ids as list (not just JSON string)."""
        with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()):
            connector = GoogleAnalyticsAggregatedLakeflowConnect({
                "property_ids": ["123456789"],  # Direct list, not JSON string
                "credentials_json": VALID_CREDENTIALS
            })
            assert connector.property_ids == ["123456789"]

    def test_credentials_as_dict_directly(self):
        """Connector should accept credentials as dict (not just JSON string)."""
        with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()):
            creds_dict = json.loads(VALID_CREDENTIALS)
            connector = GoogleAnalyticsAggregatedLakeflowConnect({
                "property_ids": '["123456789"]',
                "credentials_json": creds_dict  # Direct dict
            })
            assert connector.credentials == creds_dict


@pytest.fixture
def connector():
    """Create a connector with mocked credentials and metadata."""
    with patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=mock_credentials()), \
         patch.object(GoogleAnalyticsAggregatedLakeflowConnect, '_fetch_metadata', return_value=FAKE_METADATA):
        conn = GoogleAnalyticsAggregatedLakeflowConnect({
            "property_ids": '["123456789"]',
            "credentials_json": VALID_CREDENTIALS
        })
        conn._metadata_cache = FAKE_METADATA
        return conn


class TestResolveTableOptions:
    """Test _resolve_table_options and prebuilt report logic."""

    def test_prebuilt_report_resolves_correctly(self, connector):
        """Prebuilt report name should resolve to its config."""
        resolved = connector._resolve_table_options({"prebuilt_report": "traffic_by_country"})
        assert "dimensions" in resolved
        assert "metrics" in resolved
        assert "date" in resolved["dimensions"]

    def test_invalid_prebuilt_report_raises_error(self, connector):
        """Invalid prebuilt report name should raise error."""
        with pytest.raises(ValueError, match="not found"):
            connector._resolve_table_options({"prebuilt_report": "nonexistent_report"})

    def test_custom_options_override_prebuilt(self, connector):
        """Custom dimensions/metrics should override prebuilt defaults."""
        resolved = connector._resolve_table_options({
            "prebuilt_report": "traffic_by_country",
            "start_date": "7daysAgo",  # Override
        })
        assert resolved["start_date"] == "7daysAgo"

    def test_table_name_as_prebuilt_report(self, connector):
        """Table name matching prebuilt report should use that config."""
        schema = connector.get_table_schema("traffic_by_country", {})
        field_names = [f.name for f in schema.fields]
        assert "date" in field_names
        assert "country" in field_names


class TestSchemaGeneration:
    """Test schema generation edge cases."""

    def test_custom_dimensions_override_prebuilt(self, connector):
        """Custom dimensions should override prebuilt report dimensions."""
        options = {
            "dimensions": '["city"]',
            "metrics": '["sessions"]'
        }
        schema = connector.get_table_schema("traffic_by_country", options)
        field_names = [f.name for f in schema.fields]
        assert "city" in field_names
        assert "country" not in field_names  # Prebuilt dimension NOT used

    def test_schema_with_no_date_dimension(self, connector):
        """Schema without date should still work."""
        options = {
            "dimensions": '["country"]',
            "metrics": '["activeUsers"]'
        }
        schema = connector.get_table_schema("custom_no_date", options)
        field_names = [f.name for f in schema.fields]
        assert "country" in field_names
        assert "date" not in field_names

    def test_property_id_always_first_field(self, connector):
        """property_id should always be the first field in schema."""
        schema = connector.get_table_schema("traffic_by_country", {})
        assert schema.fields[0].name == "property_id"


class TestMetadataGeneration:
    """Test metadata generation (ingestion type, primary keys, etc.)."""

    def test_report_with_date_uses_cdc_ingestion(self, connector):
        """Reports with date dimension should use CDC ingestion for settlement-aware sync."""
        metadata = connector.read_table_metadata("traffic_by_country", {})
        assert metadata["ingestion_type"] == "cdc"
        assert metadata.get("cursor_field") == "date"

    def test_report_without_date_uses_snapshot_ingestion(self, connector):
        """Reports without date dimension should use snapshot ingestion."""
        options = {
            "dimensions": '["country"]',
            "metrics": '["activeUsers"]'
        }
        metadata = connector.read_table_metadata("custom_no_date", options)
        assert metadata["ingestion_type"] == "snapshot"
        assert metadata.get("cursor_field") is None

    def test_primary_keys_include_property_id(self, connector):
        """Primary keys should always include property_id first."""
        metadata = connector.read_table_metadata("traffic_by_country", {})
        assert "property_id" in metadata["primary_keys"]
        assert metadata["primary_keys"][0] == "property_id"

    def test_primary_keys_derived_from_dimensions(self, connector):
        """Primary keys should be derived from dimensions."""
        options = {
            "dimensions": '["date", "city", "browser"]',
            "metrics": '["sessions"]'
        }
        metadata = connector.read_table_metadata("custom", options)
        assert metadata["primary_keys"] == ["property_id", "date", "city", "browser"]


class TestListTables:
    """Test list_tables functionality."""

    def test_list_tables_returns_prebuilt_reports(self, connector):
        """list_tables should return all prebuilt report names."""
        tables = connector.list_tables()
        assert isinstance(tables, list)
        assert "traffic_by_country" in tables
        assert len(tables) >= 1

    def test_list_tables_returns_list(self, connector):
        """list_tables should always return a list."""
        tables = connector.list_tables()
        assert isinstance(tables, list)


class TestPrebuiltReportsCache:
    """Test prebuilt reports caching behavior."""

    def test_prebuilt_reports_are_cached(self, connector):
        """Prebuilt reports should be cached after first load."""
        reports1 = connector._load_prebuilt_reports()
        reports2 = connector._load_prebuilt_reports()
        assert reports1 is reports2  # Same object reference


class TestValidationErrors:
    """Test dimension/metric validation error messages."""

    def test_unknown_dimension_raises_error(self, connector):
        """Unknown dimension should raise descriptive error."""
        options = {
            "dimensions": '["invalid_dimension"]',
            "metrics": '["sessions"]'
        }
        with pytest.raises(ValueError, match="Unknown dimensions"):
            connector.get_table_schema("custom", options)

    def test_unknown_metric_raises_error(self, connector):
        """Unknown metric should raise descriptive error."""
        options = {
            "dimensions": '["date"]',
            "metrics": '["invalid_metric"]'
        }
        with pytest.raises(ValueError, match="Unknown metrics"):
            connector.get_table_schema("custom", options)

    def test_too_many_dimensions_raises_error(self, connector):
        """More than 9 dimensions should raise error."""
        # Create 10 dimensions (GA4 limit is 9)
        ten_dims = '["date", "country", "city", "browser", "deviceCategory", "sessionSource", "sessionMedium", "eventName", "pagePath", "pageTitle"]'
        options = {
            "dimensions": ten_dims,
            "metrics": '["sessions"]'
        }
        with pytest.raises(ValueError, match="Too many dimensions"):
            connector.get_table_schema("custom", options)

    def test_missing_metrics_raises_error(self, connector):
        """At least one metric is required."""
        options = {
            "dimensions": '["date"]',
            "metrics": '[]'
        }
        with pytest.raises(ValueError):
            connector.get_table_schema("custom", options)
