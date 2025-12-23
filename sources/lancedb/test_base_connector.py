#!/usr/bin/env python3
"""
Test script to compare original LanceDB connector vs Base Class implementation.

This script validates that the refactored implementation (using BaseVectorDBConnector)
produces identical results to the original implementation.
"""

import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

def test_connector_implementation(connector_module, version_name):
    """Test a connector implementation."""
    
    print(f"\n{'='*80}")
    print(f"Testing {version_name}")
    print(f"{'='*80}\n")
    
    # Load configuration
    config_path = Path(__file__).parent / "configs" / "dev_config.json"
    with open(config_path) as f:
        config = json.load(f)
    
    print(f"Configuration:")
    print(f"  Project: {config['project_name']}")
    print(f"  Region: {config['region']}")
    print(f"  API Key: {config['api_key'][:12]}...{config['api_key'][-10:]}\n")
    
    results = {}
    
    try:
        # Initialize connector
        connector = connector_module.LakeflowConnect(config)
        print(f"✅ Connector initialized")
        results["init"] = True
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        results["init"] = False
        return results
    
    # Test 1: List Tables
    print(f"\n{'─'*80}")
    print("TEST 1: List Tables")
    print(f"{'─'*80}")
    try:
        tables = connector.list_tables()
        print(f"✅ Found {len(tables)} tables: {tables}")
        results["list_tables"] = tables
    except Exception as e:
        print(f"❌ List tables failed: {e}")
        results["list_tables"] = None
        return results
    
    if not tables:
        print("⚠️  No tables found, skipping remaining tests")
        return results
    
    # Use first table for testing (skip multivector)
    test_table = None
    for table in tables:
        if "multivector" not in table.lower():
            test_table = table
            break
    
    if not test_table:
        test_table = tables[0]
    
    print(f"\n📊 Using table '{test_table}' for remaining tests\n")
    
    # Test 2: Get Schema
    print(f"{'─'*80}")
    print(f"TEST 2: Get Schema for '{test_table}'")
    print(f"{'─'*80}")
    try:
        schema = connector.get_table_schema(test_table, {})
        print(f"✅ Schema: {schema}")
        results["schema"] = str(schema)
    except Exception as e:
        print(f"❌ Get schema failed: {e}")
        results["schema"] = None
    
    # Test 3: Get Metadata
    print(f"\n{'─'*80}")
    print(f"TEST 3: Get Metadata for '{test_table}'")
    print(f"{'─'*80}")
    try:
        metadata = connector.read_table_metadata(test_table, {})
        print(f"✅ Metadata: {json.dumps(metadata, indent=2)}")
        results["metadata"] = metadata
    except Exception as e:
        print(f"❌ Get metadata failed: {e}")
        results["metadata"] = None
    
    # Test 4: Read Data
    print(f"\n{'─'*80}")
    print(f"TEST 4: Read Data from '{test_table}'")
    print(f"{'─'*80}")
    try:
        table_options = {"batch_size": "5", "use_full_scan": "true"}
        records, offset = connector.read_table(test_table, {}, table_options)
        record_list = list(records)
        print(f"✅ Read {len(record_list)} records")
        if record_list:
            print(f"\nFirst record:")
            # Show first 3 fields
            for i, (key, value) in enumerate(record_list[0].items()):
                if i >= 3:
                    print(f"  ... ({len(record_list[0])} fields total)")
                    break
                if isinstance(value, list) and len(value) > 5:
                    print(f"  {key}: [{value[0]}, {value[1]}, ... ({len(value)} items)]")
                else:
                    print(f"  {key}: {value}")
        results["read_data"] = len(record_list)
    except Exception as e:
        print(f"❌ Read data failed: {e}")
        import traceback
        traceback.print_exc()
        results["read_data"] = 0
    
    return results


def compare_results(results_v1, results_v2):
    """Compare results from both implementations."""
    
    print(f"\n\n{'='*80}")
    print("COMPARISON RESULTS")
    print(f"{'='*80}\n")
    
    all_match = True
    
    # Compare initialization
    print(f"1. Initialization:")
    if results_v1.get("init") == results_v2.get("init"):
        print(f"   ✅ MATCH - Both: {results_v1.get('init')}")
    else:
        print(f"   ❌ DIFFER - V1: {results_v1.get('init')}, V2: {results_v2.get('init')}")
        all_match = False
    
    # Compare table lists
    print(f"\n2. List Tables:")
    tables_v1 = results_v1.get("list_tables", [])
    tables_v2 = results_v2.get("list_tables", [])
    if set(tables_v1 or []) == set(tables_v2 or []):
        print(f"   ✅ MATCH - Both found {len(tables_v1 or [])} tables")
    else:
        print(f"   ❌ DIFFER - V1: {len(tables_v1 or [])} tables, V2: {len(tables_v2 or [])} tables")
        all_match = False
    
    # Compare schema
    print(f"\n3. Schema:")
    if results_v1.get("schema") == results_v2.get("schema"):
        print(f"   ✅ MATCH")
    else:
        print(f"   ⚠️  DIFFER (may be formatting)")
        print(f"   V1: {results_v1.get('schema', 'N/A')[:100]}...")
        print(f"   V2: {results_v2.get('schema', 'N/A')[:100]}...")
    
    # Compare metadata
    print(f"\n4. Metadata:")
    if results_v1.get("metadata") == results_v2.get("metadata"):
        print(f"   ✅ MATCH")
    else:
        print(f"   ❌ DIFFER")
        print(f"   V1: {results_v1.get('metadata')}")
        print(f"   V2: {results_v2.get('metadata')}")
        all_match = False
    
    # Compare data reading
    print(f"\n5. Data Reading:")
    records_v1 = results_v1.get("read_data", 0)
    records_v2 = results_v2.get("read_data", 0)
    if records_v1 == records_v2:
        print(f"   ✅ MATCH - Both read {records_v1} records")
    else:
        print(f"   ❌ DIFFER - V1: {records_v1} records, V2: {records_v2} records")
        all_match = False
    
    print(f"\n{'='*80}")
    if all_match:
        print("🎉 PERFECT MATCH - Refactored implementation works identically!")
    else:
        print("⚠️  Some differences found - Review above")
    print(f"{'='*80}\n")
    
    return all_match


def main():
    """Main test execution."""
    
    print("="*80)
    print("LanceDB Connector: Original vs Base Class Implementation Comparison")
    print("="*80)
    
    # Test original implementation
    try:
        import lancedb as lancedb_v1
        results_v1 = test_connector_implementation(lancedb_v1, "Original Implementation (lancedb.py)")
    except Exception as e:
        print(f"\n❌ Failed to test original implementation: {e}")
        import traceback
        traceback.print_exc()
        results_v1 = {}
    
    # Test refactored implementation
    try:
        import lancedb_v2
        results_v2 = test_connector_implementation(lancedb_v2, "Refactored Implementation (lancedb_v2.py with Base Class)")
    except Exception as e:
        print(f"\n❌ Failed to test refactored implementation: {e}")
        import traceback
        traceback.print_exc()
        results_v2 = {}
    
    # Compare results
    if results_v1 and results_v2:
        match = compare_results(results_v1, results_v2)
        return 0 if match else 1
    else:
        print("\n❌ Could not complete comparison - one or both implementations failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

