"""
Test script for new Freshservice APIs (changes, agents, requesters, asset_types, 
service_catalog, conversations) and tickets API with include parameter.
"""

import pytest
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.test_utils import load_config
from sources.freshservice.freshservice import LakeflowConnect


def test_new_tables_in_list():
    """Test that all new tables are present in list_tables()"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    tables = connector.list_tables()
    
    # Check for new tables
    new_tables = ["changes", "agents", "requesters", "asset_types", "service_catalog", "conversations"]
    
    for table in new_tables:
        assert table in tables, f"New table '{table}' not found in list_tables()"
    
    print(f"✓ All {len(new_tables)} new tables found in list_tables()")
    print(f"  Total tables: {len(tables)}")


def test_new_tables_schemas():
    """Test that all new tables have valid schemas"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    new_tables = ["changes", "agents", "requesters", "asset_types", "service_catalog", "conversations"]
    
    for table_name in new_tables:
        schema = connector.get_table_schema(table_name, {})
        
        # Validate schema is StructType
        from pyspark.sql.types import StructType, IntegerType
        assert isinstance(schema, StructType), f"{table_name}: Schema should be StructType"
        
        # Check no IntegerType fields (should use LongType)
        for field in schema.fields:
            assert not isinstance(field.dataType, IntegerType), \
                f"{table_name}: Field '{field.name}' uses IntegerType, should use LongType"
        
        print(f"✓ {table_name}: Schema valid ({len(schema.fields)} fields)")


def test_new_tables_metadata():
    """Test that all new tables have valid metadata"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Test CDC table (changes)
    metadata = connector.read_table_metadata("changes", {})
    assert metadata["ingestion_type"] == "cdc", "changes should be CDC"
    assert metadata["cursor_field"] == "updated_at", "changes should use updated_at cursor"
    assert "id" in metadata["primary_keys"], "changes should have id as primary key"
    print("✓ changes: Metadata valid (CDC)")
    
    # Test Snapshot tables
    snapshot_tables = ["agents", "requesters", "asset_types", "service_catalog", "conversations"]
    for table_name in snapshot_tables:
        metadata = connector.read_table_metadata(table_name, {})
        assert metadata["ingestion_type"] == "snapshot", f"{table_name} should be snapshot"
        assert "id" in metadata["primary_keys"], f"{table_name} should have id as primary key"
        print(f"✓ {table_name}: Metadata valid (snapshot)")


def test_read_new_tables():
    """Test reading data from new tables (limited to 1 page)"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    # Test each new table
    new_tables = ["changes", "agents", "requesters", "asset_types", "service_catalog"]
    
    for table_name in new_tables:
        print(f"\nTesting {table_name}...")
        
        # Read with limited pages
        table_options = {"per_page": "10", "max_pages_per_batch": "1"}
        records_iter, next_offset = connector.read_table(table_name, {}, table_options)
        
        # Convert iterator to list
        records = list(records_iter)
        
        print(f"✓ {table_name}: Successfully read {len(records)} records")
        
        # If we got records, validate structure
        if len(records) > 0:
            first_record = records[0]
            assert "id" in first_record, f"{table_name}: Record should have 'id' field"
            print(f"  Sample record ID: {first_record.get('id')}")


def test_read_conversations():
    """Test reading conversations (nested resource)"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    print("\nTesting conversations (nested resource)...")
    
    # Read with very limited pages since this needs to iterate through tickets
    table_options = {"per_page": "5", "max_pages_per_batch": "1"}
    records_iter, next_offset = connector.read_table("conversations", {}, table_options)
    
    # Convert iterator to list
    records = list(records_iter)
    
    print(f"✓ conversations: Successfully read {len(records)} records")
    
    # If we got records, validate structure
    if len(records) > 0:
        first_record = records[0]
        assert "id" in first_record, "Conversation should have 'id' field"
        assert "ticket_id" in first_record, "Conversation should have 'ticket_id' field"
        print(f"  Sample: Conversation {first_record.get('id')} for ticket {first_record.get('ticket_id')}")


def test_tickets_with_include_parameter():
    """Test tickets API with include parameter"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    print("\nTesting tickets with include parameter...")
    
    # Test with include parameter
    table_options = {
        "per_page": "5",
        "max_pages_per_batch": "1",
        "include": "stats,requester"
    }
    
    records_iter, next_offset = connector.read_table("tickets", {}, table_options)
    records = list(records_iter)
    
    print(f"✓ tickets (with include): Successfully read {len(records)} records")
    
    # Check if records have additional fields from include
    if len(records) > 0:
        first_record = records[0]
        print(f"  Record keys: {list(first_record.keys())[:10]}...")
        
        # Check if stats or requester data is present
        has_stats = "stats" in first_record
        has_requester = "requester" in first_record
        
        if has_stats or has_requester:
            print(f"  ✓ Include parameter working: stats={has_stats}, requester={has_requester}")
        else:
            print(f"  Note: Include fields not found (may depend on API response)")


def test_tickets_without_include_parameter():
    """Test tickets API without include parameter (baseline)"""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    
    connector = LakeflowConnect(config)
    
    print("\nTesting tickets without include parameter (baseline)...")
    
    # Test without include parameter
    table_options = {
        "per_page": "5",
        "max_pages_per_batch": "1"
    }
    
    records_iter, next_offset = connector.read_table("tickets", {}, table_options)
    records = list(records_iter)
    
    print(f"✓ tickets (no include): Successfully read {len(records)} records")
    
    if len(records) > 0:
        first_record = records[0]
        print(f"  Sample ticket ID: {first_record.get('id')}")


if __name__ == "__main__":
    """Run tests when script is executed directly"""
    print("="*70)
    print("Testing New Freshservice APIs")
    print("="*70)
    
    try:
        # Run all tests
        test_new_tables_in_list()
        print()
        
        test_new_tables_schemas()
        print()
        
        test_new_tables_metadata()
        print()
        
        test_read_new_tables()
        print()
        
        test_read_conversations()
        print()
        
        test_tickets_with_include_parameter()
        print()
        
        test_tickets_without_include_parameter()
        print()
        
        print("="*70)
        print("✓ ALL TESTS PASSED!")
        print("="*70)
        
    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

