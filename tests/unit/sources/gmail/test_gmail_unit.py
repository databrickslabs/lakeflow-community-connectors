import pytest
from pyspark.sql.types import IntegerType

from databricks.labs.community_connector.sources.gmail.gmail import GmailLakeflowConnect


class TestGmailConnectorUnit:
    """Unit tests that validate connector structure without API calls."""

    @pytest.fixture
    def connector(self):
        """Create connector with dummy credentials for structure tests."""
        return GmailLakeflowConnect(
            {
                "client_id": "dummy_client_id",
                "client_secret": "dummy_client_secret",
                "refresh_token": "dummy_refresh_token",
            }
        )

    def test_unit_initialization_with_valid_options(self, connector):
        """Test connector initializes with all required options."""
        assert connector.client_id == "dummy_client_id"
        assert connector.client_secret == "dummy_client_secret"
        assert connector.refresh_token == "dummy_refresh_token"
        assert connector.user_id == "me"  # default

    def test_unit_initialization_missing_client_id(self):
        """Test initialization fails without client_id."""
        with pytest.raises(ValueError, match="client_id"):
            GmailLakeflowConnect(
                {
                    "client_secret": "secret",
                    "refresh_token": "token",
                }
            )

    def test_unit_initialization_missing_client_secret(self):
        """Test initialization fails without client_secret."""
        with pytest.raises(ValueError, match="client_secret"):
            GmailLakeflowConnect(
                {
                    "client_id": "id",
                    "refresh_token": "token",
                }
            )

    def test_unit_initialization_missing_refresh_token(self):
        """Test initialization fails without refresh_token."""
        with pytest.raises(ValueError, match="refresh_token"):
            GmailLakeflowConnect(
                {
                    "client_id": "id",
                    "client_secret": "secret",
                }
            )

    def test_unit_schemas_use_long_type_not_integer(self, connector):
        """Test all numeric fields use LongType instead of IntegerType."""
        for table_name in connector.list_tables():
            schema = connector.get_table_schema(table_name, {})
            for field in schema.fields:
                assert not isinstance(field.dataType, IntegerType), (
                    f"Table '{table_name}' field '{field.name}' uses IntegerType. "
                    "Use LongType instead per best practices."
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
