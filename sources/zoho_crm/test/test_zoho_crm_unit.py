"""
Unit tests for Zoho CRM connector classes.

These tests use mocks to test the connector logic without requiring
actual Zoho CRM API credentials.
"""

import json
import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime, timedelta

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DoubleType,
    TimestampType,
    ArrayType,
    MapType,
)

# Import the classes under test
from sources.zoho_crm._auth_client import AuthClient
from sources.zoho_crm._metadata_manager import MetadataManager
from sources.zoho_crm._schema_generator import SchemaGenerator
from sources.zoho_crm._data_reader import DataReader
from sources.zoho_crm.zoho_crm import LakeflowConnect


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def valid_options():
    """Valid connector options for testing."""
    return {
        "client_id": "test_client_id",
        "client_value_tmp": "test_client_secret",
        "refresh_value_tmp": "test_refresh_token",
        "base_url": "https://accounts.zoho.com",
        "initial_load_start_date": "2024-01-01T00:00:00Z",
    }


@pytest.fixture
def mock_auth_client(valid_options):
    """Create a mock AuthClient for testing."""
    with patch.object(AuthClient, '__init__', lambda self, options: None):
        client = AuthClient({})
        client.client_id = valid_options["client_id"]
        client.client_secret = valid_options["client_value_tmp"]
        client.refresh_token = valid_options["refresh_value_tmp"]
        client.accounts_url = "https://accounts.zoho.com"
        client.api_url = "https://www.zohoapis.com"
        client.access_token = "test_access_token"
        client.token_expires_at = datetime.now() + timedelta(hours=1)
        client._session = Mock()
        client.make_request = Mock()
        client.get_access_token = Mock(return_value="test_access_token")
        return client


@pytest.fixture
def derived_tables():
    """Standard derived tables configuration."""
    return {
        "Users": {"type": "settings", "endpoint": "/crm/v8/users", "data_key": "users"},
        "Roles": {"type": "settings", "endpoint": "/crm/v8/settings/roles", "data_key": "roles"},
        "Profiles": {"type": "settings", "endpoint": "/crm/v8/settings/profiles", "data_key": "profiles"},
        "Quoted_Items": {"type": "subform", "parent_module": "Quotes", "subform_field": "Quoted_Items"},
        "Campaigns_Leads": {"type": "related", "parent_module": "Campaigns", "related_module": "Leads"},
    }


@pytest.fixture
def mock_modules_response():
    """Mock response for modules API."""
    return {
        "modules": [
            {"api_name": "Leads", "api_supported": True, "generated_type": "default"},
            {"api_name": "Contacts", "api_supported": True, "generated_type": "default"},
            {"api_name": "Accounts", "api_supported": True, "generated_type": "default"},
            {"api_name": "Deals", "api_supported": True, "generated_type": "default"},
            {"api_name": "Visits", "api_supported": True, "generated_type": "default"},  # Should be excluded
        ]
    }


@pytest.fixture
def mock_fields_response():
    """Mock response for fields API."""
    return {
        "fields": [
            {"api_name": "id", "data_type": "bigint", "required": True},
            {"api_name": "First_Name", "data_type": "text", "required": False},
            {"api_name": "Last_Name", "data_type": "text", "required": True},
            {"api_name": "Email", "data_type": "email", "required": False},
            {"api_name": "Phone", "data_type": "phone", "required": False},
            {"api_name": "Annual_Revenue", "data_type": "currency", "required": False},
            {"api_name": "Email_Opt_Out", "data_type": "boolean", "required": False},
            {"api_name": "Created_Time", "data_type": "datetime", "required": False},
            {"api_name": "Modified_Time", "data_type": "datetime", "required": False},
            {"api_name": "Owner", "data_type": "ownerlookup", "required": False},
            {"api_name": "Tags", "data_type": "multiselectpicklist", "required": False},
        ]
    }


@pytest.fixture
def mock_records_response():
    """Mock response for records API."""
    return {
        "data": [
            {
                "id": "12345",
                "First_Name": "John",
                "Last_Name": "Doe",
                "Email": "john.doe@example.com",
                "Modified_Time": "2024-06-01T10:00:00+00:00",
            },
            {
                "id": "12346",
                "First_Name": "Jane",
                "Last_Name": "Smith",
                "Email": "jane.smith@example.com",
                "Modified_Time": "2024-06-02T10:00:00+00:00",
            },
        ],
        "info": {"more_records": False, "count": 2},
    }


# =============================================================================
# AuthClient Tests
# =============================================================================

class TestAuthClient:
    """Tests for the AuthClient class."""

    def test_init_with_valid_options(self, valid_options):
        """Test AuthClient initialization with valid options."""
        with patch('sources.zoho_crm._auth_client.requests.Session'):
            client = AuthClient(valid_options)
            
            assert client.client_id == "test_client_id"
            assert client.client_secret == "test_client_secret"
            assert client.refresh_token == "test_refresh_token"
            assert client.accounts_url == "https://accounts.zoho.com"
            assert client.api_url == "https://www.zohoapis.com"

    def test_init_missing_credentials(self):
        """Test AuthClient stores None for missing credentials.
        
        Note: AuthClient doesn't validate credentials on init.
        Validation happens when get_access_token() is called.
        """
        incomplete_options = {"client_id": "test"}
        
        with patch('sources.zoho_crm._auth_client.requests.Session'):
            client = AuthClient(incomplete_options)
            
            assert client.client_id == "test"
            assert client.client_secret is None  # Missing
            assert client.refresh_token is None  # Missing

    def test_api_url_derivation_eu(self):
        """Test API URL is correctly derived for EU region."""
        options = {
            "client_id": "test",
            "client_value_tmp": "test",
            "refresh_value_tmp": "test",
            "base_url": "https://accounts.zoho.eu",
        }
        
        with patch('sources.zoho_crm._auth_client.requests.Session'):
            client = AuthClient(options)
            assert client.api_url == "https://www.zohoapis.eu"

    def test_api_url_derivation_in(self):
        """Test API URL is correctly derived for India region."""
        options = {
            "client_id": "test",
            "client_value_tmp": "test",
            "refresh_value_tmp": "test",
            "base_url": "https://accounts.zoho.in",
        }
        
        with patch('sources.zoho_crm._auth_client.requests.Session'):
            client = AuthClient(options)
            assert client.api_url == "https://www.zohoapis.in"

    def test_get_access_token_returns_cached(self, mock_auth_client):
        """Test that cached token is returned if not expired."""
        mock_auth_client.get_access_token = AuthClient.get_access_token.__get__(mock_auth_client)
        
        token = mock_auth_client.get_access_token()
        assert token == "test_access_token"

    @patch('sources.zoho_crm._auth_client.requests.post')
    def test_get_access_token_refreshes_expired(self, mock_post, valid_options):
        """Test that token is refreshed when expired."""
        with patch('sources.zoho_crm._auth_client.requests.Session'):
            client = AuthClient(valid_options)
            client.access_token = None  # Force refresh
            
            mock_response = Mock()
            mock_response.json.return_value = {
                "access_token": "new_token",
                "expires_in": 3600,
            }
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response
            
            token = client.get_access_token()
            
            assert token == "new_token"
            assert client.access_token == "new_token"


# =============================================================================
# MetadataManager Tests
# =============================================================================

class TestMetadataManager:
    """Tests for the MetadataManager class."""

    def test_list_tables_includes_modules_and_derived(self, mock_auth_client, derived_tables, mock_modules_response):
        """Test list_tables returns both modules and derived tables."""
        mock_auth_client.make_request.return_value = mock_modules_response
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        tables = manager.list_tables()
        
        # Should include standard modules (excluding Visits)
        assert "Leads" in tables
        assert "Contacts" in tables
        assert "Accounts" in tables
        assert "Deals" in tables
        assert "Visits" not in tables  # Should be excluded
        
        # Should include derived tables
        assert "Users" in tables
        assert "Roles" in tables
        assert "Profiles" in tables
        assert "Quoted_Items" in tables
        assert "Campaigns_Leads" in tables

    def test_get_fields_caches_results(self, mock_auth_client, derived_tables, mock_fields_response):
        """Test that get_fields caches results."""
        mock_auth_client.make_request.return_value = mock_fields_response
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        
        # First call
        fields1 = manager.get_fields("Leads")
        # Second call
        fields2 = manager.get_fields("Leads")
        
        # Should only make one API call due to caching
        assert mock_auth_client.make_request.call_count == 1
        assert fields1 == fields2

    def test_read_table_metadata_cdc_table(self, mock_auth_client, derived_tables, mock_modules_response, mock_fields_response):
        """Test read_table_metadata for CDC table."""
        mock_auth_client.make_request.side_effect = [mock_modules_response, mock_fields_response]
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        schema_gen = SchemaGenerator(manager, derived_tables)
        
        metadata = manager.read_table_metadata("Leads", schema_gen.get_table_schema, {})
        
        assert metadata["primary_keys"] == ["id"]
        assert metadata["cursor_field"] == "Modified_Time"
        assert metadata["ingestion_type"] == "cdc"

    def test_read_table_metadata_derived_settings(self, mock_auth_client, derived_tables):
        """Test read_table_metadata for derived settings table (Users)."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        
        metadata = manager._get_derived_table_metadata("Users")
        
        assert metadata["primary_keys"] == ["id"]
        assert metadata["cursor_field"] == "Modified_Time"
        assert metadata["ingestion_type"] == "cdc"

    def test_read_table_metadata_derived_subform(self, mock_auth_client, derived_tables):
        """Test read_table_metadata for derived subform table."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        
        metadata = manager._get_derived_table_metadata("Quoted_Items")
        
        assert metadata["primary_keys"] == ["id"]
        assert metadata["ingestion_type"] == "snapshot"

    def test_read_table_metadata_derived_junction(self, mock_auth_client, derived_tables):
        """Test read_table_metadata for derived junction table."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        
        metadata = manager._get_derived_table_metadata("Campaigns_Leads")
        
        assert metadata["primary_keys"] == ["_junction_id"]
        assert metadata["ingestion_type"] == "snapshot"


# =============================================================================
# SchemaGenerator Tests
# =============================================================================

class TestSchemaGenerator:
    """Tests for the SchemaGenerator class."""

    def test_get_table_schema_standard_module(self, mock_auth_client, derived_tables, mock_fields_response):
        """Test get_table_schema for a standard module."""
        # Only need fields response since we're calling get_table_schema directly
        mock_auth_client.make_request.return_value = mock_fields_response
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        schema = generator.get_table_schema("Leads", {})
        
        assert isinstance(schema, StructType)
        field_names = schema.fieldNames()
        
        assert "id" in field_names
        assert "First_Name" in field_names
        assert "Email" in field_names
        assert "Modified_Time" in field_names

    def test_get_spark_type_bigint(self, mock_auth_client, derived_tables):
        """Test bigint type mapping."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        spark_type = generator._get_spark_type("bigint")
        
        assert isinstance(spark_type, LongType)

    def test_get_spark_type_text(self, mock_auth_client, derived_tables):
        """Test text type mapping."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        spark_type = generator._get_spark_type("text")
        
        assert isinstance(spark_type, StringType)

    def test_get_spark_type_boolean(self, mock_auth_client, derived_tables):
        """Test boolean type mapping."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        spark_type = generator._get_spark_type("boolean")
        
        assert isinstance(spark_type, BooleanType)

    def test_get_spark_type_currency(self, mock_auth_client, derived_tables):
        """Test currency type mapping."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        spark_type = generator._get_spark_type("currency")
        
        assert isinstance(spark_type, DoubleType)

    def test_get_spark_type_lookup(self, mock_auth_client, derived_tables):
        """Test lookup type mapping (stored as string ID)."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        spark_type = generator._get_spark_type("lookup")
        
        # Lookup fields are stored as string IDs
        assert isinstance(spark_type, StringType)

    def test_get_spark_type_multiselectpicklist(self, mock_auth_client, derived_tables):
        """Test multiselectpicklist type mapping (array)."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        spark_type = generator._get_spark_type("multiselectpicklist")
        
        assert isinstance(spark_type, ArrayType)
        assert isinstance(spark_type.elementType, StringType)

    def test_get_settings_table_schema_users(self, mock_auth_client, derived_tables):
        """Test schema generation for Users settings table."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        schema = generator._get_settings_table_schema("Users")
        
        field_names = schema.fieldNames()
        assert "id" in field_names
        assert "name" in field_names
        assert "email" in field_names
        assert "role" in field_names
        assert "profile" in field_names
        assert "Modified_Time" in field_names

    def test_get_subform_table_schema(self, mock_auth_client, derived_tables):
        """Test schema generation for subform table."""
        # Mock the fields response for parent module (Quotes)
        mock_quotes_fields = {
            "fields": [
                {
                    "api_name": "Quoted_Items",
                    "data_type": "subform",
                    "subform": {
                        "fields": [
                            {"api_name": "id", "data_type": "bigint"},
                            {"api_name": "Product_Name", "data_type": "text"},
                            {"api_name": "Quantity", "data_type": "integer"},
                            {"api_name": "Unit_Price", "data_type": "currency"},
                        ]
                    }
                }
            ]
        }
        mock_auth_client.make_request.return_value = mock_quotes_fields
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        schema = generator._get_subform_table_schema("Quoted_Items", derived_tables["Quoted_Items"])
        
        field_names = schema.fieldNames()
        assert "_parent_id" in field_names

    def test_get_related_table_schema(self, mock_auth_client, derived_tables):
        """Test schema generation for junction table."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        generator = SchemaGenerator(manager, derived_tables)
        
        schema = generator._get_related_table_schema("Campaigns_Leads", derived_tables["Campaigns_Leads"])
        
        field_names = schema.fieldNames()
        assert "_junction_id" in field_names
        assert "Campaigns_id" in field_names
        assert "Leads_id" in field_names


# =============================================================================
# DataReader Tests
# =============================================================================

class TestDataReader:
    """Tests for the DataReader class."""

    def test_read_records_with_pagination(self, mock_auth_client, derived_tables, mock_fields_response, mock_records_response):
        """Test reading records with pagination."""
        # Only need to mock the records API call since we pre-populate the fields cache
        mock_auth_client.make_request.return_value = mock_records_response
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        # Pre-populate the fields cache to avoid API calls for fields
        manager._fields_cache["Leads"] = mock_fields_response["fields"]
        
        schema_gen = SchemaGenerator(manager, derived_tables)
        reader = DataReader(mock_auth_client, manager, schema_gen, derived_tables, None)
        
        records = list(reader._read_records("Leads", None))
        
        assert len(records) == 2
        assert records[0]["id"] == "12345"
        assert records[1]["id"] == "12346"

    def test_read_table_returns_iterator_and_offset(self, mock_auth_client, derived_tables, mock_fields_response, mock_records_response):
        """Test read_table returns iterator and offset."""
        mock_auth_client.make_request.return_value = mock_records_response
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        # Pre-populate the fields cache
        manager._fields_cache["Leads"] = mock_fields_response["fields"]
        
        schema_gen = SchemaGenerator(manager, derived_tables)
        reader = DataReader(mock_auth_client, manager, schema_gen, derived_tables, None)
        
        iterator, offset = reader.read_table("Leads", {}, {})
        
        records = list(iterator)
        assert len(records) >= 0
        assert isinstance(offset, dict)

    def test_read_settings_table_users(self, mock_auth_client, derived_tables):
        """Test reading Users settings table."""
        mock_users_response = {
            "users": [
                {"id": "user1", "name": "Test User", "email": "test@example.com"},
            ],
            "info": {"more_records": False},
        }
        mock_auth_client.make_request.return_value = mock_users_response
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        schema_gen = SchemaGenerator(manager, derived_tables)
        reader = DataReader(mock_auth_client, manager, schema_gen, derived_tables, None)
        
        # _read_settings_table takes table_name and options
        records = list(reader._read_settings_table("Users", {}))
        
        assert len(records) == 1
        assert records[0]["id"] == "user1"

    def test_normalize_record_with_all_field_names(self, mock_auth_client, derived_tables, mock_fields_response):
        """Test that normalize_record works correctly with all arguments."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        schema_gen = SchemaGenerator(manager, derived_tables)
        reader = DataReader(mock_auth_client, manager, schema_gen, derived_tables, None)
        
        record = {
            "id": "123",
            "First_Name": "Test",
            "json_data": {"key": "value"},
        }
        json_fields = {"json_data"}
        all_field_names = ["id", "First_Name", "json_data"]
        
        normalized = reader._normalize_record(record, json_fields, all_field_names)
        
        assert normalized["id"] == "123"
        assert normalized["First_Name"] == "Test"
        assert normalized["json_data"] == '{"key": "value"}'

    def test_normalize_record_lookup_field(self, mock_auth_client, derived_tables):
        """Test that lookup fields are normalized to IDs."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        schema_gen = SchemaGenerator(manager, derived_tables)
        reader = DataReader(mock_auth_client, manager, schema_gen, derived_tables, None)
        
        record = {
            "id": "123",
            "Owner": {"id": "owner_id", "name": "John Doe"},
        }
        json_fields = set()
        all_field_names = ["id", "Owner"]
        
        normalized = reader._normalize_record(record, json_fields, all_field_names)
        
        assert normalized["id"] == "123"
        assert normalized["Owner"] == "owner_id"  # Should extract just the ID

    def test_read_deleted_records(self, mock_auth_client, derived_tables):
        """Test reading deleted records."""
        mock_deleted_response = {
            "data": [
                {"id": "del1", "deleted_time": "2024-06-01T10:00:00+00:00"},
                {"id": "del2", "deleted_time": "2024-06-02T10:00:00+00:00"},
            ],
            "info": {"more_records": False},
        }
        mock_auth_client.make_request.return_value = mock_deleted_response
        
        manager = MetadataManager(mock_auth_client, derived_tables)
        schema_gen = SchemaGenerator(manager, derived_tables)
        reader = DataReader(mock_auth_client, manager, schema_gen, derived_tables, None)
        
        # read_deletes takes table_name and options (with cursor_position)
        options = {"cursor_position": "2024-05-01T00:00:00+00:00"}
        records = list(reader.read_deletes("Leads", options))
        
        assert len(records) == 2
        assert records[0]["id"] == "del1"
        assert records[1]["id"] == "del2"

    def test_read_deletes_returns_empty_for_derived_tables(self, mock_auth_client, derived_tables):
        """Test that read_deletes returns empty for derived tables."""
        manager = MetadataManager(mock_auth_client, derived_tables)
        schema_gen = SchemaGenerator(manager, derived_tables)
        reader = DataReader(mock_auth_client, manager, schema_gen, derived_tables, None)
        
        options = {"cursor_position": "2024-05-01T00:00:00+00:00"}
        records = list(reader.read_deletes("Users", options))  # Derived table
        
        assert len(records) == 0


# =============================================================================
# LakeflowConnect Integration Tests
# =============================================================================

class TestLakeflowConnect:
    """Integration tests for the main LakeflowConnect class."""

    @patch('sources.zoho_crm._auth_client.requests.Session')
    @patch('sources.zoho_crm._auth_client.requests.post')
    def test_init_creates_all_components(self, mock_post, mock_session, valid_options):
        """Test LakeflowConnect initializes all helper classes."""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test", "expires_in": 3600}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        connector = LakeflowConnect(valid_options)
        
        assert connector._auth_client is not None
        assert connector._metadata_manager is not None
        assert connector._schema_generator is not None
        assert connector._data_reader is not None
        assert connector.initial_load_start_date == "2024-01-01T00:00:00Z"

    @patch('sources.zoho_crm._auth_client.requests.Session')
    @patch('sources.zoho_crm._auth_client.requests.post')
    def test_list_tables_delegates_to_metadata_manager(self, mock_post, mock_session, valid_options, mock_modules_response):
        """Test list_tables delegates to MetadataManager."""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test", "expires_in": 3600}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        connector = LakeflowConnect(valid_options)
        
        # Mock the metadata manager's make_request
        connector._auth_client.make_request = Mock(return_value=mock_modules_response)
        
        tables = connector.list_tables()
        
        assert isinstance(tables, list)
        assert "Leads" in tables
        assert "Users" in tables  # Derived table

    @patch('sources.zoho_crm._auth_client.requests.Session')
    @patch('sources.zoho_crm._auth_client.requests.post')
    def test_get_table_schema_delegates_to_schema_generator(self, mock_post, mock_session, valid_options, mock_modules_response, mock_fields_response):
        """Test get_table_schema delegates to SchemaGenerator."""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test", "expires_in": 3600}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        connector = LakeflowConnect(valid_options)
        connector._auth_client.make_request = Mock(side_effect=[mock_modules_response, mock_fields_response])
        
        schema = connector.get_table_schema("Leads", {})
        
        assert isinstance(schema, StructType)

    @patch('sources.zoho_crm._auth_client.requests.Session')
    @patch('sources.zoho_crm._auth_client.requests.post')
    def test_init_missing_credentials_raises_error(self, mock_post, mock_session):
        """Test LakeflowConnect raises error for missing credentials."""
        incomplete_options = {"client_id": "test"}
        
        with pytest.raises(ValueError) as exc_info:
            LakeflowConnect(incomplete_options)
        
        assert "Missing required connection options" in str(exc_info.value)

    @patch('sources.zoho_crm._auth_client.requests.Session')
    @patch('sources.zoho_crm._auth_client.requests.post')
    def test_derived_tables_constant(self, mock_post, mock_session, valid_options):
        """Test DERIVED_TABLES constant is properly defined."""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test", "expires_in": 3600}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        
        connector = LakeflowConnect(valid_options)
        
        # Check all expected derived tables
        assert "Users" in connector.DERIVED_TABLES
        assert "Roles" in connector.DERIVED_TABLES
        assert "Profiles" in connector.DERIVED_TABLES
        assert "Quoted_Items" in connector.DERIVED_TABLES
        assert "Campaigns_Leads" in connector.DERIVED_TABLES
        
        # Check structure
        assert connector.DERIVED_TABLES["Users"]["type"] == "settings"
        assert connector.DERIVED_TABLES["Quoted_Items"]["type"] == "subform"
        assert connector.DERIVED_TABLES["Campaigns_Leads"]["type"] == "related"


# =============================================================================
# Run tests with pytest
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
