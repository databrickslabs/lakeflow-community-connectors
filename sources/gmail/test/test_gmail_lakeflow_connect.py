"""
Gmail Connector Tests

Unit tests run without credentials and validate connector structure.
Integration tests require real credentials and validate API functionality.

To run unit tests only (no credentials):
    PYTHONPATH=. pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v -k "unit"

To run integration tests (requires credentials in dev_config.json):
    PYTHONPATH=. pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v -k "integration"

To run all tests:
    PYTHONPATH=. pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v
"""

import os
from pathlib import Path

import pytest
from pyspark.sql.types import StructType, IntegerType, LongType

from sources.gmail.gmail import LakeflowConnect


# =============================================================================
# Unit Tests - No credentials required
# =============================================================================

class TestGmailConnectorUnit:
    """Unit tests that validate connector structure without API calls."""

    @pytest.fixture
    def connector(self):
        """Create connector with dummy credentials for structure tests."""
        return LakeflowConnect({
            "client_id": "dummy_client_id",
            "client_secret": "dummy_client_secret",
            "refresh_token": "dummy_refresh_token",
        })

    def test_unit_initialization_with_valid_options(self, connector):
        """Test connector initializes with all required options."""
        assert connector.client_id == "dummy_client_id"
        assert connector.client_secret == "dummy_client_secret"
        assert connector.refresh_token == "dummy_refresh_token"
        assert connector.user_id == "me"  # default

    def test_unit_initialization_missing_client_id(self):
        """Test initialization fails without client_id."""
        with pytest.raises(ValueError, match="client_id"):
            LakeflowConnect({
                "client_secret": "secret",
                "refresh_token": "token",
            })

    def test_unit_initialization_missing_client_secret(self):
        """Test initialization fails without client_secret."""
        with pytest.raises(ValueError, match="client_secret"):
            LakeflowConnect({
                "client_id": "id",
                "refresh_token": "token",
            })

    def test_unit_initialization_missing_refresh_token(self):
        """Test initialization fails without refresh_token."""
        with pytest.raises(ValueError, match="refresh_token"):
            LakeflowConnect({
                "client_id": "id",
                "client_secret": "secret",
            })

    def test_unit_list_tables_returns_all_tables(self, connector):
        """Test list_tables returns all 10 expected tables."""
        tables = connector.list_tables()
        
        expected_tables = [
            "messages", "threads", "labels", "drafts", "profile",
            "settings", "filters", "forwarding_addresses", "send_as", "delegates"
        ]
        
        assert isinstance(tables, list)
        assert len(tables) == 10
        assert set(tables) == set(expected_tables)

    def test_unit_get_table_schema_returns_struct_type(self, connector):
        """Test get_table_schema returns StructType for all tables."""
        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            assert isinstance(schema, StructType), f"{table_name} schema is not StructType"
            assert len(schema.fields) > 0, f"{table_name} schema has no fields"

    def test_unit_schemas_use_long_type_not_integer(self, connector):
        """Test all numeric fields use LongType instead of IntegerType."""
        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            for field in schema.fields:
                assert not isinstance(field.dataType, IntegerType), (
                    f"Table '{table_name}' field '{field.name}' uses IntegerType. "
                    "Use LongType instead per best practices."
                )

    def test_unit_read_table_metadata_returns_required_keys(self, connector):
        """Test read_table_metadata returns required metadata keys."""
        for table_name in connector.list_tables():
            metadata = connector.read_table_metadata(table_name, {})
            
            assert isinstance(metadata, dict)
            assert "primary_keys" in metadata, f"{table_name} missing primary_keys"
            assert "ingestion_type" in metadata, f"{table_name} missing ingestion_type"
            assert isinstance(metadata["primary_keys"], list)

    def test_unit_cdc_tables_have_cursor_field(self, connector):
        """Test CDC tables have cursor_field in metadata."""
        cdc_tables = ["messages", "threads"]
        
        for table_name in cdc_tables:
            metadata = connector.read_table_metadata(table_name, {})
            assert metadata["ingestion_type"] == "cdc", f"{table_name} should be cdc"
            assert "cursor_field" in metadata, f"{table_name} missing cursor_field"

    def test_unit_snapshot_tables_no_cursor_field(self, connector):
        """Test snapshot tables don't require cursor_field."""
        snapshot_tables = [
            "labels", "drafts", "profile", "settings",
            "filters", "forwarding_addresses", "send_as", "delegates"
        ]
        
        for table_name in snapshot_tables:
            metadata = connector.read_table_metadata(table_name, {})
            assert metadata["ingestion_type"] == "snapshot", f"{table_name} should be snapshot"

    def test_unit_primary_keys_exist_in_schema(self, connector):
        """Test primary_keys fields exist in table schema."""
        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            metadata = connector.read_table_metadata(table_name, {})
            
            schema_field_names = [f.name for f in schema.fields]
            for pk in metadata["primary_keys"]:
                assert pk in schema_field_names, (
                    f"Table '{table_name}' primary_key '{pk}' not in schema"
                )

    def test_unit_invalid_table_raises_error(self, connector):
        """Test invalid table name raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported table"):
            connector.get_table_schema("invalid_table", {})

    def test_unit_messages_schema_has_required_fields(self, connector):
        """Test messages schema has essential fields."""
        schema = connector.get_table_schema("messages", {})
        field_names = [f.name for f in schema.fields]
        
        required_fields = ["id", "threadId", "labelIds", "snippet"]
        for field in required_fields:
            assert field in field_names, f"messages schema missing '{field}'"

    def test_unit_profile_schema_has_email(self, connector):
        """Test profile schema has emailAddress field."""
        schema = connector.get_table_schema("profile", {})
        field_names = [f.name for f in schema.fields]
        
        assert "emailAddress" in field_names


# =============================================================================
# Integration Tests - Require credentials
# =============================================================================

def _load_test_config():
    """Load test configuration if available."""
    from tests.test_utils import load_config
    
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"
    
    if not config_path.exists():
        return None, None
    
    config = load_config(config_path)
    
    # Check for placeholder values
    placeholders = ["YOUR_", "REPLACE_", "dummy"]
    for key in ["client_id", "client_secret", "refresh_token"]:
        value = config.get(key, "")
        if any(p in value for p in placeholders) or not value:
            return None, None
    
    table_config = load_config(table_config_path) if table_config_path.exists() else {}
    return config, table_config


def _skip_if_no_credentials():
    """Skip test if credentials are not available."""
    config, _ = _load_test_config()
    if config is None:
        pytest.skip("No valid credentials in dev_config.json")


class TestGmailConnectorIntegration:
    """Integration tests that require real Gmail API credentials."""

    @pytest.fixture(autouse=True)
    def check_credentials(self):
        """Skip integration tests if no credentials available."""
        _skip_if_no_credentials()
        self.config, self.table_config = _load_test_config()

    @pytest.fixture
    def connector(self):
        """Create connector with real credentials."""
        config, _ = _load_test_config()
        return LakeflowConnect(config)

    def test_integration_read_profile(self, connector):
        """Test reading user profile from Gmail API."""
        records, offset = connector.read_table("profile", {}, {})
        records_list = list(records)
        
        assert len(records_list) == 1, "Profile should return exactly 1 record"
        assert "emailAddress" in records_list[0]
        assert "@" in records_list[0]["emailAddress"]

    def test_integration_read_labels(self, connector):
        """Test reading labels from Gmail API."""
        records, offset = connector.read_table("labels", {}, {})
        records_list = list(records)
        
        assert len(records_list) > 0, "Should have at least some labels"
        
        # Check for standard Gmail labels
        label_ids = [r.get("id", "") for r in records_list]
        assert any("INBOX" in lid for lid in label_ids), "Should have INBOX label"

    def test_integration_read_messages_with_limit(self, connector):
        """Test reading messages with maxResults limit."""
        records, offset = connector.read_table(
            "messages", {}, {"maxResults": "5"}
        )
        records_list = list(records)
        
        # May have fewer if inbox is empty, but not more than limit
        assert len(records_list) <= 5

    def test_integration_read_settings(self, connector):
        """Test reading account settings."""
        records, offset = connector.read_table("settings", {}, {})
        records_list = list(records)
        
        assert len(records_list) == 1
        assert "emailAddress" in records_list[0]

    def test_integration_read_filters(self, connector):
        """Test reading email filters."""
        records, offset = connector.read_table("filters", {}, {})
        records_list = list(records)
        
        # Filters list may be empty, but call should succeed
        assert isinstance(records_list, list)

    def test_integration_full_test_suite(self):
        """Run the full LakeflowConnect test suite."""
        from tests import test_suite
        from tests.test_suite import LakeflowConnectTester
        
        test_suite.LakeflowConnect = LakeflowConnect
        tester = LakeflowConnectTester(self.config, self.table_config)
        
        report = tester.run_all_tests()
        tester.print_report(report, show_details=True)
        
        assert report.passed_tests == report.total_tests, (
            f"Test suite had failures: {report.failed_tests} failed, "
            f"{report.error_tests} errors"
        )


# =============================================================================
# Table Options Tests - Validate custom options work correctly
# =============================================================================

class TestGmailTableOptions:
    """Test table-specific options work correctly."""

    @pytest.fixture(autouse=True)
    def check_credentials(self):
        """Skip if no credentials available."""
        _skip_if_no_credentials()
        self.config, _ = _load_test_config()

    @pytest.fixture
    def connector(self):
        """Create connector with real credentials."""
        config, _ = _load_test_config()
        return LakeflowConnect(config)

    def test_integration_messages_with_query_filter(self, connector):
        """Test messages table with q (query) option."""
        records, offset = connector.read_table(
            "messages", {}, {"q": "newer_than:30d", "maxResults": "5"}
        )
        records_list = list(records)
        # Should not raise, may return 0 if no recent messages
        assert isinstance(records_list, list)

    def test_integration_messages_with_label_filter(self, connector):
        """Test messages table with labelIds option."""
        records, offset = connector.read_table(
            "messages", {}, {"labelIds": "INBOX", "maxResults": "5"}
        )
        records_list = list(records)
        assert isinstance(records_list, list)

    def test_integration_messages_with_format_metadata(self, connector):
        """Test messages table with format=metadata option."""
        records, offset = connector.read_table(
            "messages", {}, {"format": "metadata", "maxResults": "3"}
        )
        records_list = list(records)
        # metadata format should still have basic fields
        if records_list:
            assert "id" in records_list[0]

    def test_integration_threads_with_max_results(self, connector):
        """Test threads table with maxResults option."""
        records, offset = connector.read_table(
            "threads", {}, {"maxResults": "3"}
        )
        records_list = list(records)
        assert len(records_list) <= 3

    def test_integration_drafts_with_max_results(self, connector):
        """Test drafts table with maxResults option."""
        records, offset = connector.read_table(
            "drafts", {}, {"maxResults": "5"}
        )
        records_list = list(records)
        # Drafts may be empty
        assert isinstance(records_list, list)
