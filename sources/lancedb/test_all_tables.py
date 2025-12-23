#!/usr/bin/env python3
"""
Comprehensive test script for all LanceDB tables.

This script tests the connector against all tables in the LanceDB instance,
validating schema retrieval, metadata extraction, and data reading.
"""

import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from lancedb import LakeflowConnect


def test_all_tables():
    """Test connector functionality across all tables."""
    
    # Load configuration
    config_path = Path(__file__).parent / "configs" / "dev_config.json"
    with open(config_path) as f:
        config = json.load(f)
    
    print("=" * 80)
    print("LanceDB Connector - ALL TABLES COMPREHENSIVE TEST")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  Project: {config['project_name']}")
    print(f"  Region: {config['region']}")
    print(f"  API Key: {config['api_key'][:12]}...{config['api_key'][-10:]}")
    
    # Initialize connector
    try:
        connector = LakeflowConnect(config)
        print("\n✅ Connector initialized successfully!")
    except Exception as e:
        print(f"\n❌ Failed to initialize connector: {e}")
        return False
    
    # Get all tables
    print("\n" + "=" * 80)
    print("STEP 1: Discovering All Tables")
    print("=" * 80)
    
    try:
        all_tables = connector.list_tables()
        print(f"\n✅ Found {len(all_tables)} tables total:")
        for i, table in enumerate(all_tables, 1):
            print(f"  {i}. {table}")
        
        # Exclude specific tables
        exclude_tables = ["multivector-example-new"]
        tables = [t for t in all_tables if t not in exclude_tables]
        
        if len(tables) < len(all_tables):
            print(f"\n⚠️  Excluding {len(all_tables) - len(tables)} table(s): {', '.join(exclude_tables)}")
            print(f"📋 Testing {len(tables)} tables")
        
    except Exception as e:
        print(f"\n❌ Failed to list tables: {e}")
        return False
    
    # Test each table
    all_success = True
    results = []
    
    for table_idx, table_name in enumerate(tables, 1):
        print("\n" + "=" * 80)
        print(f"TESTING TABLE {table_idx}/{len(tables)}: '{table_name}'")
        print("=" * 80)
        
        table_result = {
            "name": table_name,
            "schema": None,
            "metadata": None,
            "records_read": 0,
            "sample_record": None,
            "success": True,
            "errors": []
        }
        
        # Test 1: Get Schema
        print(f"\n[1/3] Retrieving schema for '{table_name}'...")
        try:
            schema = connector.get_table_schema(table_name, {})
            table_result["schema"] = str(schema)
            print(f"✅ Schema: {schema}")
        except Exception as e:
            print(f"❌ Schema retrieval failed: {e}")
            table_result["success"] = False
            table_result["errors"].append(f"Schema: {e}")
            all_success = False
        
        # Test 2: Get Metadata
        print(f"\n[2/3] Retrieving metadata for '{table_name}'...")
        try:
            metadata = connector.read_table_metadata(table_name, {})
            table_result["metadata"] = metadata
            print(f"✅ Metadata:")
            print(f"    Primary Keys: {metadata.get('primary_keys', [])}")
            print(f"    Ingestion Type: {metadata.get('ingestion_type', 'N/A')}")
        except Exception as e:
            print(f"❌ Metadata retrieval failed: {e}")
            table_result["success"] = False
            table_result["errors"].append(f"Metadata: {e}")
            all_success = False
        
        # Test 3: Read Data
        print(f"\n[3/3] Reading data from '{table_name}'...")
        try:
            # Use default options with use_full_scan enabled
            table_options = {
                "batch_size": "10",
                "use_full_scan": "true"
            }
            
            records, offset = connector.read_table(table_name, {}, table_options)
            record_list = list(records)
            table_result["records_read"] = len(record_list)
            
            if record_list:
                table_result["sample_record"] = record_list[0]
                print(f"✅ Successfully read {len(record_list)} records!")
                print(f"\n   First record sample:")
                for key, value in list(record_list[0].items())[:5]:  # Show first 5 fields
                    if isinstance(value, list) and len(value) > 5:
                        print(f"     {key}: [{value[0]}, {value[1]}, ... ({len(value)} items)]")
                    else:
                        print(f"     {key}: {value}")
                if len(record_list[0]) > 5:
                    print(f"     ... ({len(record_list[0])} fields total)")
                
                print(f"\n   End offset: {offset}")
            else:
                print(f"⚠️  No records returned (table might be empty)")
                
        except Exception as e:
            print(f"❌ Data reading failed: {e}")
            table_result["success"] = False
            table_result["errors"].append(f"Data Reading: {e}")
            all_success = False
        
        results.append(table_result)
        
        # Status for this table
        if table_result["success"]:
            print(f"\n✅ Table '{table_name}' - ALL TESTS PASSED")
        else:
            print(f"\n❌ Table '{table_name}' - SOME TESTS FAILED")
    
    # Final Summary
    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    
    success_count = sum(1 for r in results if r["success"])
    total_records = sum(r["records_read"] for r in results)
    
    print(f"\n📊 Overall Results:")
    print(f"  Tables Tested: {len(results)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {len(results) - success_count}")
    print(f"  Total Records Read: {total_records}")
    
    print(f"\n📋 Detailed Results:")
    for result in results:
        status = "✅" if result["success"] else "❌"
        print(f"\n  {status} {result['name']}")
        print(f"     Schema: {'✓' if result['schema'] else '✗'}")
        print(f"     Metadata: {'✓' if result['metadata'] else '✗'}")
        print(f"     Records Read: {result['records_read']}")
        if result["errors"]:
            print(f"     Errors: {', '.join(result['errors'])}")
    
    # Final verdict
    print("\n" + "=" * 80)
    if all_success and total_records > 0:
        print("🎉 ALL TESTS PASSED - CONNECTOR FULLY OPERATIONAL!")
        print("=" * 80)
        return True
    elif success_count == len(results):
        print("✅ ALL TABLES ACCESSIBLE - Some tables may be empty")
        print("=" * 80)
        return True
    else:
        print("❌ SOME TESTS FAILED - Review errors above")
        print("=" * 80)
        return False


if __name__ == "__main__":
    success = test_all_tables()
    sys.exit(0 if success else 1)

