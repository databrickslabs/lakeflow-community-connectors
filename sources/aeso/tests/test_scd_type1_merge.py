#!/usr/bin/env python3
"""
Test AESO connector SCD Type 1 merge configuration.

Verifies that the connector is properly configured to handle late-arriving
data updates with SCD Type 1 behavior (latest value wins).
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from sources.aeso.aeso import LakeflowConnect


def test_metadata_configuration():
    """Test that metadata includes sequence_by for SCD Type 1 merges."""
    config = {
        "api_key": "test_key_for_metadata_only"
    }
    
    connector = LakeflowConnect(config)
    metadata = connector.read_table_metadata("pool_price", {})
    
    # Verify all required fields are present
    assert "primary_keys" in metadata, "Missing primary_keys"
    assert "cursor_field" in metadata, "Missing cursor_field"
    assert "sequence_by" in metadata, "Missing sequence_by field"
    assert "ingestion_type" in metadata, "Missing ingestion_type"
    
    # Verify correct values
    assert metadata["primary_keys"] == ["begin_datetime_utc"], \
        f"Expected primary_keys=['begin_datetime_utc'], got {metadata['primary_keys']}"
    
    assert metadata["cursor_field"] == "ingestion_time", \
        f"Expected cursor_field='ingestion_time', got {metadata['cursor_field']}"
    
    assert metadata["sequence_by"] == "ingestion_time", \
        f"Expected sequence_by='ingestion_time', got {metadata['sequence_by']}"
    
    assert metadata["ingestion_type"] == "cdc", \
        f"Expected ingestion_type='cdc', got {metadata['ingestion_type']}"
    
    print("✓ Metadata configuration test passed")


def test_schema_has_required_fields():
    """Test that schema includes both primary key and sequence fields."""
    config = {
        "api_key": "test_key_for_schema_only"
    }
    
    connector = LakeflowConnect(config)
    schema = connector.get_table_schema("pool_price", {})
    
    field_names = [field.name for field in schema.fields]
    
    # Verify required fields for merge
    assert "begin_datetime_utc" in field_names, \
        "Schema missing begin_datetime_utc (primary key)"
    
    assert "ingestion_time" in field_names, \
        "Schema missing ingestion_time (sequence field)"
    
    # Verify ingestion_time is not nullable (required for sequence_by)
    ingestion_time_field = next(f for f in schema.fields if f.name == "ingestion_time")
    assert not ingestion_time_field.nullable, \
        "ingestion_time should not be nullable (required for sequence_by)"
    
    print("✓ Schema validation test passed")


if __name__ == "__main__":
    print("Running AESO SCD Type 1 Merge Tests...")
    print("-" * 60)
    
    try:
        test_metadata_configuration()
        test_schema_has_required_fields()
        
        print("-" * 60)
        print("✓ All tests passed!")
        print()
        print("SCD Type 1 Configuration Summary:")
        print("  - Primary Key: begin_datetime_utc (identifies unique hour)")
        print("  - Cursor: ingestion_time (tracks progress)")
        print("  - Sequence By: ingestion_time (determines latest record)")
        print("  - Ingestion Type: CDC (change data capture)")
        print()
        print("This configuration ensures that when the same hour is")
        print("ingested multiple times (e.g., with updated forecasts),")
        print("the record with the latest ingestion_time will overwrite")
        print("the previous one (SCD Type 1 behavior).")
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

