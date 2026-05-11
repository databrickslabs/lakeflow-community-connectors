"""
Integration tests for verifying prebuilt reports work with LakeflowConnect.

These tests verify that the prebuilt report JSON configurations correctly flow
through the Python connector logic to produce valid PySpark schemas.

The tests mock the Google Analytics API calls to avoid requiring real credentials
while still validating the full integration path:
    prebuilt_reports.json → LakeflowConnect → PySpark StructType

Run with: pytest sources/google_analytics_aggregated/test/test_connector_integration.py -v
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

# We need to mock the google.oauth2 import before importing LakeflowConnect
# This allows tests to run without google-auth installed
import sys

# Create mock modules for google.oauth2
mock_service_account = MagicMock()
mock_credentials = MagicMock()
mock_credentials.valid = True
mock_credentials.token = "fake_token"
mock_service_account.Credentials.from_service_account_info.return_value = mock_credentials

sys.modules['google'] = MagicMock()
sys.modules['google.oauth2'] = MagicMock()
sys.modules['google.oauth2.service_account'] = mock_service_account
sys.modules['google.auth'] = MagicMock()
sys.modules['google.auth.transport'] = MagicMock()
sys.modules['google.auth.transport.requests'] = MagicMock()

# Now import the connector
from databricks.labs.community_connector.sources.google_analytics_aggregated.google_analytics_aggregated import GoogleAnalyticsAggregatedLakeflowConnect


# Path to prebuilt reports (in the source connector directory)
PREBUILT_REPORTS_PATH = Path(__file__).resolve().parents[4] / "src" / "databricks" / "labs" / "community_connector" / "sources" / "google_analytics_aggregated" / "prebuilt_reports.json"


# Fake metadata that mimics the GA4 API response structure
# This includes all dimensions and metrics used in our prebuilt reports
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
        "totalUsers": "TYPE_INTEGER",
        "conversions": "TYPE_INTEGER",
        "keyEvents": "TYPE_INTEGER",
        "totalRevenue": "TYPE_CURRENCY",
        "itemsViewed": "TYPE_INTEGER",
        "itemsAddedToCart": "TYPE_INTEGER",
        "itemsPurchased": "TYPE_INTEGER",
        "itemRevenue": "TYPE_CURRENCY",
        "itemsViewedInPromotion": "TYPE_INTEGER",
        "itemsClickedInPromotion": "TYPE_INTEGER",
        "engagedSessions": "TYPE_INTEGER",
        "advertiserAdCost": "TYPE_CURRENCY",
    },
    "available_dimensions": {
        "date",
        "dateHour",
        "dateHourMinute",
        "firstSessionDate",
        "country",
        "city",
        "sessionSource",
        "sessionMedium",
        "sessionCampaignName",
        "eventName",
        "pagePath",
        "pageTitle",
        "deviceCategory",
        "browser",
        "operatingSystem",
        "language",
        "landingPage",
        "itemName",
        "itemId",
        "itemPromotionName",
        "newVsReturning",
        "audienceName",
        "defaultChannelGroup",
    },
    "available_metrics": {
        "activeUsers",
        "sessions",
        "screenPageViews",
        "newUsers",
        "eventCount",
        "engagementRate",
        "averageSessionDuration",
        "bounceRate",
        "totalUsers",
        "conversions",
        "keyEvents",
        "totalRevenue",
        "itemsViewed",
        "itemsAddedToCart",
        "itemsPurchased",
        "itemRevenue",
        "itemsViewedInPromotion",
        "itemsClickedInPromotion",
        "engagedSessions",
        "advertiserAdCost",
    },
}


@pytest.fixture
def mock_connector():
    """
    Create a LakeflowConnect instance with mocked API calls.
    
    This fixture:
    1. Mocks the credential validation (no real Google credentials needed)
    2. Mocks the metadata fetch (no real API calls)
    3. Returns a fully functional connector for testing schema generation
    """
    # Build fake private key dynamically to avoid secret scanner false positives
    fake_pk = f"-----BEGIN RSA {'PRIVATE'} KEY-----\nfake\n-----END RSA {'PRIVATE'} KEY-----\n"
    fake_config = {
        "property_ids": '["123456789"]',
        "credentials_json": json.dumps({
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "fake-key-id",
            "private_key": fake_pk,
            "client_email": "test@test-project.iam.gserviceaccount.com",
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }),
    }
    
    # Create connector with mocked internals
    with patch.object(GoogleAnalyticsAggregatedLakeflowConnect, '_fetch_metadata', return_value=FAKE_METADATA):
        connector = GoogleAnalyticsAggregatedLakeflowConnect(fake_config)
        # Inject the fake metadata cache directly
        connector._metadata_cache = FAKE_METADATA
        yield connector


@pytest.fixture
def prebuilt_reports():
    """Load prebuilt reports configuration."""
    with open(PREBUILT_REPORTS_PATH) as f:
        return json.load(f)


class TestPrebuiltReportSchemas:
    """Test that prebuilt reports produce valid PySpark schemas."""
    
    def test_traffic_by_country_schema(self, mock_connector):
        """Verify traffic_by_country produces correct schema."""
        schema = mock_connector.get_table_schema("traffic_by_country", {})
        field_names = [f.name for f in schema.fields]
        
        # Check expected fields are present
        assert "property_id" in field_names, "Should have property_id field"
        assert "date" in field_names, "Should have date dimension"
        assert "country" in field_names, "Should have country dimension"
        assert "activeUsers" in field_names, "Should have activeUsers metric"
        assert "sessions" in field_names, "Should have sessions metric"
        assert "screenPageViews" in field_names, "Should have screenPageViews metric"
        
        # Verify property_id is first
        assert field_names[0] == "property_id", "property_id should be first field"
        
        # Verify field count (property_id + 2 dims + 3 metrics = 6)
        assert len(field_names) == 6, f"Expected 6 fields, got {len(field_names)}: {field_names}"
    
    def test_user_acquisition_schema(self, mock_connector):
        """Verify user_acquisition produces correct schema."""
        schema = mock_connector.get_table_schema("user_acquisition", {})
        field_names = [f.name for f in schema.fields]
        
        # Check expected fields are present
        assert "property_id" in field_names, "Should have property_id field"
        assert "date" in field_names, "Should have date dimension"
        assert "sessionSource" in field_names, "Should have sessionSource dimension"
        assert "sessionMedium" in field_names, "Should have sessionMedium dimension"
        assert "sessions" in field_names, "Should have sessions metric"
        assert "activeUsers" in field_names, "Should have activeUsers metric"
        assert "newUsers" in field_names, "Should have newUsers metric"
        assert "engagementRate" in field_names, "Should have engagementRate metric"
        
        # Verify property_id is first
        assert field_names[0] == "property_id", "property_id should be first field"
        
        # Verify field count (property_id + 3 dims + 4 metrics = 8)
        assert len(field_names) == 8, f"Expected 8 fields, got {len(field_names)}: {field_names}"
    
    def test_events_summary_schema(self, mock_connector):
        """Verify events_summary produces correct schema."""
        schema = mock_connector.get_table_schema("events_summary", {})
        field_names = [f.name for f in schema.fields]
        
        # Check expected fields
        assert "property_id" in field_names
        assert "date" in field_names
        assert "eventName" in field_names
        assert "eventCount" in field_names
        assert "activeUsers" in field_names
        
        # Verify field count (property_id + 2 dims + 2 metrics = 5)
        assert len(field_names) == 5, f"Expected 5 fields, got {len(field_names)}: {field_names}"
    
    def test_page_performance_schema(self, mock_connector):
        """Verify page_performance produces correct schema."""
        schema = mock_connector.get_table_schema("page_performance", {})
        field_names = [f.name for f in schema.fields]
        
        # Check expected fields
        assert "property_id" in field_names
        assert "date" in field_names
        assert "pagePath" in field_names
        assert "pageTitle" in field_names
        assert "screenPageViews" in field_names
        assert "averageSessionDuration" in field_names
        assert "bounceRate" in field_names
        
        # Verify field count (property_id + 3 dims + 3 metrics = 7)
        assert len(field_names) == 7, f"Expected 7 fields, got {len(field_names)}: {field_names}"
    
    def test_device_breakdown_schema(self, mock_connector):
        """Verify device_breakdown produces correct schema."""
        schema = mock_connector.get_table_schema("device_breakdown", {})
        field_names = [f.name for f in schema.fields]
        
        # Check expected fields
        assert "property_id" in field_names
        assert "date" in field_names
        assert "deviceCategory" in field_names
        assert "browser" in field_names
        assert "activeUsers" in field_names
        assert "sessions" in field_names
        assert "engagementRate" in field_names
        
        # Verify field count (property_id + 3 dims + 3 metrics = 7)
        assert len(field_names) == 7, f"Expected 7 fields, got {len(field_names)}: {field_names}"


class TestPrebuiltReportMetadata:
    """Test that prebuilt reports produce correct metadata."""
    
    def test_traffic_by_country_metadata(self, mock_connector):
        """Verify traffic_by_country metadata is correct."""
        metadata = mock_connector.read_table_metadata("traffic_by_country", {})
        
        assert metadata["ingestion_type"] == "cdc", "Should be cdc (has date) for settlement-aware sync"
        assert metadata["cursor_field"] == "date", "Cursor should be date"
        assert metadata["primary_keys"] == ["property_id", "date", "country"]
    
    def test_user_acquisition_metadata(self, mock_connector):
        """Verify user_acquisition metadata is correct."""
        metadata = mock_connector.read_table_metadata("user_acquisition", {})
        
        assert metadata["ingestion_type"] == "cdc", "Should be cdc (has date) for settlement-aware sync"
        assert metadata["cursor_field"] == "date", "Cursor should be date"
        assert metadata["primary_keys"] == ["property_id", "date", "sessionSource", "sessionMedium"]
    
    def test_events_summary_metadata(self, mock_connector):
        """Verify events_summary metadata is correct."""
        metadata = mock_connector.read_table_metadata("events_summary", {})
        
        assert metadata["ingestion_type"] == "cdc"
        assert metadata["primary_keys"] == ["property_id", "date", "eventName"]
    
    def test_page_performance_metadata(self, mock_connector):
        """Verify page_performance metadata is correct."""
        metadata = mock_connector.read_table_metadata("page_performance", {})
        
        assert metadata["ingestion_type"] == "cdc"
        assert metadata["primary_keys"] == ["property_id", "date", "pagePath", "pageTitle"]
    
    def test_device_breakdown_metadata(self, mock_connector):
        """Verify device_breakdown metadata is correct."""
        metadata = mock_connector.read_table_metadata("device_breakdown", {})
        
        assert metadata["ingestion_type"] == "cdc"
        assert metadata["primary_keys"] == ["property_id", "date", "deviceCategory", "browser"]


class TestListTables:
    """Test that list_tables returns all prebuilt reports."""
    
    def test_list_tables_returns_all_prebuilt_reports(self, mock_connector, prebuilt_reports):
        """Verify list_tables returns all prebuilt report names."""
        tables = mock_connector.list_tables()
        
        # Should return all prebuilt report names
        for report_name in prebuilt_reports.keys():
            assert report_name in tables, f"list_tables should include '{report_name}'"
        
        # Count should match
        assert len(tables) == len(prebuilt_reports), \
            f"Expected {len(prebuilt_reports)} tables, got {len(tables)}"


class TestSchemaDataTypes:
    """Test that schema fields have correct PySpark data types."""
    
    def test_metric_types_are_correct(self, mock_connector):
        """Verify metrics have correct data types based on GA4 metadata."""
        from pyspark.sql.types import LongType, DoubleType, StringType, DateType
        
        schema = mock_connector.get_table_schema("user_acquisition", {})
        field_dict = {f.name: f.dataType for f in schema.fields}
        
        # Integer metrics should be LongType
        assert isinstance(field_dict["sessions"], LongType), "sessions should be LongType"
        assert isinstance(field_dict["activeUsers"], LongType), "activeUsers should be LongType"
        assert isinstance(field_dict["newUsers"], LongType), "newUsers should be LongType"
        
        # Float metrics should be DoubleType
        assert isinstance(field_dict["engagementRate"], DoubleType), "engagementRate should be DoubleType"
        
        # Dimensions should be StringType (except date)
        assert isinstance(field_dict["sessionSource"], StringType), "sessionSource should be StringType"
        assert isinstance(field_dict["sessionMedium"], StringType), "sessionMedium should be StringType"
        
        # Date should be DateType
        assert isinstance(field_dict["date"], DateType), "date should be DateType"
        
        # property_id should be StringType
        assert isinstance(field_dict["property_id"], StringType), "property_id should be StringType"
    
    def test_date_dimension_types(self, mock_connector):
        """Verify date-related dimensions have correct types.
        
        Critical test: dateHour and dateHourMinute must be StringType (not DateType)
        because they contain time components that DateType cannot represent.
        """
        from pyspark.sql.types import StringType, DateType
        
        # Test with all date-related dimensions
        table_options = {
            "dimensions": '["date", "firstSessionDate", "dateHour", "dateHourMinute"]',
            "metrics": '["activeUsers"]'
        }
        
        schema = mock_connector.get_table_schema("test_date_types", table_options)
        field_dict = {f.name: f.dataType for f in schema.fields}
        
        # Pure date dimensions (YYYYMMDD format) should be DateType
        assert isinstance(field_dict["date"], DateType), \
            "date should be DateType (YYYYMMDD format)"
        assert isinstance(field_dict["firstSessionDate"], DateType), \
            "firstSessionDate should be DateType (YYYYMMDD format)"
        
        # DateTime dimensions (contain time components) MUST be StringType
        assert isinstance(field_dict["dateHour"], StringType), \
            "dateHour should be StringType (YYYYMMDDHH format contains time)"
        assert isinstance(field_dict["dateHourMinute"], StringType), \
            "dateHourMinute should be StringType (YYYYMMDDHHmm format contains time)"


class TestAllPrebuiltReportsIntegration:
    """Comprehensive test that validates all prebuilt reports."""
    
    def test_all_prebuilt_reports_produce_valid_schemas(self, mock_connector, prebuilt_reports):
        """Verify every prebuilt report can produce a valid schema."""
        for report_name in prebuilt_reports.keys():
            # This should not raise any exceptions
            schema = mock_connector.get_table_schema(report_name, {})
            
            # Basic validation
            assert schema is not None, f"{report_name}: schema should not be None"
            assert len(schema.fields) > 0, f"{report_name}: schema should have fields"
            
            # property_id should always be first
            assert schema.fields[0].name == "property_id", \
                f"{report_name}: property_id should be first field"
            
            print(f"✅ {report_name}: {len(schema.fields)} fields")
    
    def test_all_prebuilt_reports_produce_valid_metadata(self, mock_connector, prebuilt_reports):
        """Verify every prebuilt report can produce valid metadata."""
        for report_name in prebuilt_reports.keys():
            # This should not raise any exceptions
            metadata = mock_connector.read_table_metadata(report_name, {})
            
            # Basic validation
            assert metadata is not None, f"{report_name}: metadata should not be None"
            assert "primary_keys" in metadata, f"{report_name}: should have primary_keys"
            assert "ingestion_type" in metadata, f"{report_name}: should have ingestion_type"
            
            # property_id should always be in primary keys
            assert "property_id" in metadata["primary_keys"], \
                f"{report_name}: property_id should be in primary_keys"
            
            print(f"✅ {report_name}: {metadata['ingestion_type']}, keys={metadata['primary_keys']}")

