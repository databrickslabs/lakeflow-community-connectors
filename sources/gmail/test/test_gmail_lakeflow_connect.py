"""
Gmail Connector Tests

These tests validate the Gmail connector against the Gmail API.

To run tests with real credentials:
1. Copy dev_config.json.template to dev_config.json
2. Fill in your OAuth credentials (client_id, client_secret, refresh_token)
3. Run: PYTHONPATH=. pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v

Tests will be skipped if:
- dev_config.json doesn't exist
- Credentials are placeholder values
- GMAIL_SKIP_LIVE_TESTS environment variable is set
"""

import os
import json
from pathlib import Path

import pytest

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.gmail.gmail import LakeflowConnect


def _has_valid_credentials(config: dict) -> bool:
    """Check if config contains real credentials (not placeholders)."""
    placeholder_values = [
        "YOUR_GOOGLE_CLIENT_ID",
        "YOUR_GOOGLE_CLIENT_SECRET",
        "YOUR_REFRESH_TOKEN",
        "REPLACE_WITH_YOUR_CLIENT_ID",
        "REPLACE_WITH_YOUR_CLIENT_SECRET",
        "REPLACE_WITH_YOUR_REFRESH_TOKEN",
    ]
    for key in ["client_id", "client_secret", "refresh_token"]:
        value = config.get(key, "")
        if not value or value in placeholder_values:
            return False
    return True


def _should_skip_live_tests() -> tuple[bool, str]:
    """Determine if live tests should be skipped and return reason."""
    # Check environment variable
    if os.environ.get("GMAIL_SKIP_LIVE_TESTS"):
        return True, "GMAIL_SKIP_LIVE_TESTS environment variable is set"

    # Check if config file exists
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"

    if not config_path.exists():
        return True, (
            f"Config file not found: {config_path}. "
            "Copy dev_config.json.template to dev_config.json and add your credentials."
        )

    # Check if credentials are valid
    try:
        config = load_config(config_path)
        if not _has_valid_credentials(config):
            return True, (
                "Config contains placeholder credentials. "
                "Update dev_config.json with real OAuth credentials."
            )
    except (json.JSONDecodeError, IOError) as e:
        return True, f"Failed to load config: {e}"

    return False, ""


# Unit tests - always run (no credentials needed)
class TestGmailConnectorUnit:
    """Unit tests for Gmail connector that don't require credentials."""

    def test_list_tables_returns_expected_tables(self):
        """Test that list_tables returns all expected Gmail tables."""
        # Create connector with dummy credentials (won't make API calls)
        connector = LakeflowConnect({
            "client_id": "dummy",
            "client_secret": "dummy",
            "refresh_token": "dummy",
        })

        tables = connector.list_tables()

        expected_tables = [
            "messages", "threads", "labels", "drafts", "profile",
            "settings", "filters", "forwarding_addresses", "send_as", "delegates"
        ]
        assert set(tables) == set(expected_tables)
        assert len(tables) == 10

    def test_get_table_schema_returns_struct_type(self):
        """Test that get_table_schema returns valid StructType for all tables."""
        from pyspark.sql.types import StructType

        connector = LakeflowConnect({
            "client_id": "dummy",
            "client_secret": "dummy",
            "refresh_token": "dummy",
        })

        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            assert isinstance(schema, StructType), f"{table_name} schema is not StructType"
            assert len(schema.fields) > 0, f"{table_name} schema has no fields"

    def test_read_table_metadata_returns_required_keys(self):
        """Test that read_table_metadata returns required metadata."""
        connector = LakeflowConnect({
            "client_id": "dummy",
            "client_secret": "dummy",
            "refresh_token": "dummy",
        })

        for table_name in connector.list_tables():
            metadata = connector.read_table_metadata(table_name, {})
            assert isinstance(metadata, dict)
            assert "primary_keys" in metadata
            assert "ingestion_type" in metadata

    def test_no_integer_type_in_schemas(self):
        """Verify all schemas use LongType instead of IntegerType."""
        from pyspark.sql.types import IntegerType

        connector = LakeflowConnect({
            "client_id": "dummy",
            "client_secret": "dummy",
            "refresh_token": "dummy",
        })

        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            for field in schema.fields:
                assert not isinstance(field.dataType, IntegerType), (
                    f"Table '{table_name}' field '{field.name}' uses IntegerType. "
                    "Use LongType instead."
                )


# Integration tests - require real credentials
class TestGmailConnectorIntegration:
    """Integration tests that require real Gmail API credentials."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup for integration tests - skip if no credentials."""
        should_skip, reason = _should_skip_live_tests()
        if should_skip:
            pytest.skip(reason)

        parent_dir = Path(__file__).parent.parent
        self.config = load_config(parent_dir / "configs" / "dev_config.json")
        self.table_config = load_config(parent_dir / "configs" / "dev_table_config.json")

    def test_gmail_connector_full_suite(self):
        """Run the full LakeflowConnect test suite against live Gmail API."""
        # Inject the Gmail LakeflowConnect class
        test_suite.LakeflowConnect = LakeflowConnect

        # Create tester with real credentials
        tester = LakeflowConnectTester(self.config, self.table_config)

        # Run all tests
        report = tester.run_all_tests()
        tester.print_report(report, show_details=True)

        # Assert all tests passed
        assert report.passed_tests == report.total_tests, (
            f"Test suite had failures: {report.failed_tests} failed, "
            f"{report.error_tests} errors"
        )

    def test_read_profile(self):
        """Test reading user profile from Gmail API."""
        connector = LakeflowConnect(self.config)
        records, offset = connector.read_table("profile", {}, {})

        records_list = list(records)
        assert len(records_list) == 1, "Profile should return exactly 1 record"
        assert "emailAddress" in records_list[0]

    def test_read_labels(self):
        """Test reading labels from Gmail API."""
        connector = LakeflowConnect(self.config)
        records, offset = connector.read_table("labels", {}, {})

        records_list = list(records)
        assert len(records_list) > 0, "Should have at least some labels"

        # Verify standard labels exist
        label_names = [r.get("name") for r in records_list]
        assert "INBOX" in label_names or any("INBOX" in str(r) for r in records_list)

    def test_read_messages_with_limit(self):
        """Test reading messages with maxResults limit."""
        connector = LakeflowConnect(self.config)
        records, offset = connector.read_table(
            "messages",
            {},
            {"maxResults": "5"}
        )

        records_list = list(records)
        # May have fewer if inbox is empty
        assert len(records_list) <= 5

