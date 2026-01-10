#!/usr/bin/env python3
"""
Local test script for MongoDB connector.
Tests that all documents are read from a collection.

Usage:
    python sources/mongodb/test_local.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sources.mongodb.mongodb import LakeflowConnect


def test_full_sync():
    """Test reading all documents from MongoDB collection"""
    
    # Configuration
    config = {
        "connection_uri": "mongodb+srv://ajdidonato7:ajpassword@cluster0.tbetho9.mongodb.net/sample_mflix?appName=Cluster0",
        "schema_sample_size": "100",
        "batch_size": "1000"
    }
    
    print("="*70)
    print("MongoDB Connector - Full Sync Test")
    print("="*70)
    
    try:
        # Initialize connector
        print("\n[1/4] Initializing connector...")
        connector = LakeflowConnect(config)
        print("✓ Connector initialized")
        
        # List tables
        print("\n[2/4] Listing collections...")
        tables = connector.list_tables()
        print(f"✓ Found {len(tables)} collections: {tables[:5]}...")
        
        # Test with movies collection
        table_name = "movies"
        table_options = {}
        
        # Get schema
        print(f"\n[3/4] Getting schema for '{table_name}'...")
        schema = connector.get_table_schema(table_name, table_options)
        print(f"✓ Schema has {len(schema.fields)} fields")
        
        # Read ALL documents
        print(f"\n[4/4] Reading ALL documents from '{table_name}'...")
        print("   (This may take a moment for large collections...)")
        
        start_offset = {}  # Empty offset = initial sync
        records, next_offset = connector.read_table(table_name, start_offset, table_options)
        
        print(f"\n✓ Read {len(records)} documents")
        print(f"   Next offset: {next_offset}")
        
        # Show sample of first few records
        print(f"\n   Sample of first 3 documents:")
        for i, record in enumerate(records[:3], 1):
            print(f"   {i}. _id: {record.get('_id')}, title: {record.get('title', 'N/A')[:50]}")
        
        print("\n" + "="*70)
        print("TEST RESULTS")
        print("="*70)
        print(f"Total documents read: {len(records)}")
        print(f"Expected documents: 21,350")
        
        if len(records) == 21350:
            print("✅ SUCCESS! All documents read correctly!")
        elif len(records) < 21350:
            print(f"⚠️  WARNING: Only {len(records)} documents read (missing {21350 - len(records)})")
        else:
            print(f"✅ SUCCESS! Read {len(records)} documents (may have changed since count)")
        
        print("="*70)
        
        return len(records)
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 0


if __name__ == "__main__":
    count = test_full_sync()
    sys.exit(0 if count > 20000 else 1)

