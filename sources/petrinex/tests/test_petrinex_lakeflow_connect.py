"""
Integration tests for Petrinex LakeflowConnect connector

Requires PySpark and petrinex package to be installed.
"""
import os
import sys
import json
import pytest
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from petrinex import LakeflowConnect


@pytest.fixture
def connector():
    """Create a Petrinex connector instance for testing"""
    options = {}  # No auth required for Petrinex
    return LakeflowConnect(options)


@pytest.fixture
def table_config():
    """Load table configuration for testing"""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "configs",
        "dev_table_config.json"
    )
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    return config.get("volumetrics", {})


def test_list_tables(connector):
    """Test that list_tables returns expected tables"""
    tables = connector.list_tables()
    
    assert isinstance(tables, list), "list_tables should return a list"
    assert len(tables) > 0, "list_tables should return at least one table"
    assert "volumetrics" in tables, "volumetrics should be in the list of tables"
    
    print(f"✓ list_tables() returned: {tables}")


def test_get_table_schema(connector, table_config):
    """Test that get_table_schema returns a valid PySpark schema"""
    from pyspark.sql.types import StructType
    
    schema = connector.get_table_schema("volumetrics", table_config)
    
    assert isinstance(schema, StructType), "Schema should be a StructType"
    assert len(schema.fields) > 0, "Schema should have at least one field"
    
    # Check for key fields
    field_names = [f.name for f in schema.fields]
    required_fields = [
        "BA_ID", 
        "ProductionMonth", 
        "Product", 
        "file_updated_ts", 
        "ingestion_time"
    ]
    
    for field in required_fields:
        assert field in field_names, f"Schema should include '{field}' field"
    
    print(f"✓ Schema has {len(schema.fields)} fields")


def test_read_table_metadata(connector, table_config):
    """Test that read_table_metadata returns correct metadata"""
    metadata = connector.read_table_metadata("volumetrics", table_config)
    
    assert isinstance(metadata, dict), "Metadata should be a dictionary"
    
    # Check required metadata fields
    assert "primary_keys" in metadata, "Metadata should include primary_keys"
    assert "cursor_field" in metadata, "Metadata should include cursor_field"
    assert "sequence_by" in metadata, "Metadata should include sequence_by"
    assert "ingestion_type" in metadata, "Metadata should include ingestion_type"
    
    # Validate metadata values
    assert isinstance(metadata["primary_keys"], list), "primary_keys should be a list"
    assert len(metadata["primary_keys"]) > 0, "primary_keys should not be empty"
    assert metadata["cursor_field"] == "file_updated_ts", "cursor_field should be file_updated_ts"
    assert metadata["sequence_by"] == "ingestion_time", "sequence_by should be ingestion_time"
    assert metadata["ingestion_type"] == "cdc", "ingestion_type should be cdc"
    
    print(f"✓ Metadata: {metadata}")


def test_invalid_table_name(connector):
    """Test that invalid table names raise ValueError"""
    with pytest.raises(ValueError, match="not supported"):
        connector.list_tables()
        connector.get_table_schema("invalid_table", {})


def test_connector_initialization():
    """Test connector initialization with various options"""
    # Test with empty options
    connector1 = LakeflowConnect({})
    assert connector1 is not None
    
    # Test with jurisdiction option
    connector2 = LakeflowConnect({"jurisdiction": "AB"})
    assert connector2.jurisdiction == "AB"
    
    print("✓ Connector initialization works correctly")


def test_schema_field_types(connector, table_config):
    """Test that schema fields have appropriate types"""
    from pyspark.sql.types import StringType, TimestampType
    
    schema = connector.get_table_schema("volumetrics", table_config)
    
    # Find key fields and check their types
    field_types = {f.name: type(f.dataType).__name__ for f in schema.fields}
    
    # String fields
    assert field_types["BA_ID"] == "StringType"
    assert field_types["Product"] == "StringType"
    
    # Timestamp fields
    assert field_types["file_updated_ts"] == "TimestampType"
    assert field_types["ingestion_time"] == "TimestampType"
    
    print("✓ Schema field types are correct")


if __name__ == "__main__":
    print("\nRunning Petrinex connector integration tests...\n")
    print("Note: These tests require PySpark and petrinex package to be installed.\n")
    
    # Run with pytest
    pytest.main([__file__, "-v"])

