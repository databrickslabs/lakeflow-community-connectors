"""Unit tests for Google User Activity Lakeflow connector.

Tests cover:
- Initialization with access_token (UC injection)
- Optional impersonated_admin_email parameter
- Error handling (missing access_token points to COMMUNITY connection)
- Table discovery and schema
- Metadata validation
- Append-only ingestion type
"""

import pytest
from pyspark.sql.types import LongType, IntegerType

from databricks.labs.community_connector.sources.google_user_activity import (
    GoogleUserActivityLakeflowConnect,
)


class TestGoogleUserActivityLakeflowConnect:
    """Test suite for Google User Activity connector."""

    @pytest.fixture
    def connector(self):
        """Create a connector instance with a dummy access token."""
        return GoogleUserActivityLakeflowConnect({"access_token": "dummy-access-token"})

    def test_initialization_with_access_token(self, connector):
        """Connector initializes with access_token from UC."""
        assert connector.access_token == "dummy-access-token"
        assert connector.impersonated_admin_email is None

    def test_initialization_with_impersonated_admin_email(self):
        """Connector initializes with optional impersonated_admin_email."""
        connector = GoogleUserActivityLakeflowConnect(
            {
                "access_token": "dummy-access-token",
                "impersonated_admin_email": "admin@example.com",
            }
        )
        assert connector.impersonated_admin_email == "admin@example.com"

    def test_initialization_missing_access_token(self):
        """__init__ fails fast with COMMUNITY-pointing error message."""
        with pytest.raises(ValueError, match="access_token") as info:
            GoogleUserActivityLakeflowConnect({})
        error_msg = str(info.value)
        assert "COMMUNITY" in error_msg
        assert "admin.reports.audit.readonly" in error_msg

    def test_list_tables(self, connector):
        """list_tables returns supported applications."""
        tables = connector.list_tables()
        assert "admin" in tables
        assert "login" in tables
        assert "saml" in tables
        assert "user_accounts" in tables
        assert "groups" in tables
        assert "groups_enterprise" in tables
        assert len(tables) == 6

    def test_get_table_schema(self, connector):
        """get_table_schema returns valid StructType for each table."""
        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            assert schema is not None
            assert len(schema.fields) > 0
            # Verify no IntegerType (per best practice #173)
            for field in schema.fields:
                assert not isinstance(
                    field.dataType, IntegerType
                ), f"Field '{field.name}' uses IntegerType; use LongType instead"

    def test_get_table_metadata(self, connector):
        """get_table_metadata returns required fields."""
        metadata = connector.read_table_metadata("login", {})
        assert "primary_keys" in metadata
        assert "ingestion_type" in metadata
        assert metadata["primary_keys"] == ["lw_id"]
        assert metadata["ingestion_type"] == "append"

    def test_read_table_with_invalid_table(self, connector):
        """read_table raises error for unsupported application."""
        with pytest.raises(ValueError, match="Unsupported application"):
            list(connector.read_table("invalid_app", {}, {}))

    def test_read_table_returns_iterator_and_offset(self, connector):
        """read_table returns iterator and offset dict.

        Phase 1 (simulate mode): Returns empty iterator.
        """
        for table_name in connector.list_tables():
            records, offset = connector.read_table(table_name, {}, {})
            assert hasattr(records, "__iter__"), f"Table {table_name} should return iterator"
            assert isinstance(offset, dict), f"Table {table_name} should return offset dict"
            # Phase 1: Empty iterator
            assert list(records) == []

    def test_all_applications_supported(self, connector):
        """Test each application returns valid schema and metadata."""
        for app in connector.list_tables():
            schema = connector.get_table_schema(app, {})
            metadata = connector.read_table_metadata(app, {})
            records, offset = connector.read_table(app, {}, {})

            assert schema is not None
            assert metadata["primary_keys"] == ["lw_id"]
            assert metadata["ingestion_type"] == "append"
            assert isinstance(offset, dict)
