#!/usr/bin/env python3
"""
Test script to validate UiPath connection and retrieve queue items
"""
import json
import sys
from pathlib import Path

# Add sources to path
sys.path.insert(0, str(Path(__file__).parent))

from sources.uipathqueues.uipathqueues import LakeflowConnect


def main():
    print("=" * 80)
    print("UiPath Orchestrator Queue Items Connector - Connection Test")
    print("=" * 80)
    
    # Load configuration
    config_path = Path(__file__).parent / "sources/uipathqueues/configs/dev_config.json"
    table_config_path = Path(__file__).parent / "sources/uipathqueues/configs/dev_table_config.json"
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    with open(table_config_path, "r") as f:
        table_config = json.load(f)
    
    print("\n1. Loading Configuration...")
    print(f"   Organization: {config['organization_name']}")
    print(f"   Tenant: {config['tenant_name']}")
    print(f"   Folder ID: {config['folder_id']}")
    print(f"   Queue Definition ID: {table_config['queue_items']['queue_definition_id']}")
    
    # Initialize connector
    print("\n2. Initializing Connector...")
    try:
        connector = LakeflowConnect(config)
        print("   ✓ Connector initialized successfully")
    except Exception as e:
        print(f"   ✗ Failed to initialize connector: {e}")
        return
    
    # Test authentication
    print("\n3. Testing Authentication (OAuth Client Credentials)...")
    try:
        token = connector._get_access_token()
        print(f"   ✓ Authentication successful")
        print(f"   Token preview: {token[:20]}...{token[-20:]}")
    except Exception as e:
        print(f"   ✗ Authentication failed: {e}")
        return
    
    # List tables
    print("\n4. Listing Available Tables...")
    try:
        tables = connector.list_tables()
        print(f"   ✓ Available tables: {tables}")
    except Exception as e:
        print(f"   ✗ Failed to list tables: {e}")
        return
    
    # Get table schema
    print("\n5. Retrieving Table Schema...")
    try:
        schema = connector.get_table_schema("queue_items", table_config["queue_items"])
        print(f"   ✓ Schema retrieved successfully")
        print(f"   Fields: {len(schema.fields)} columns")
        print(f"   Primary fields: Id, QueueDefinitionId, Name, Status, Priority, Reference")
    except Exception as e:
        print(f"   ✗ Failed to get schema: {e}")
        return
    
    # Get table metadata
    print("\n6. Retrieving Table Metadata...")
    try:
        metadata = connector.read_table_metadata("queue_items", table_config["queue_items"])
        print(f"   ✓ Metadata retrieved successfully")
        print(f"   Primary Keys: {metadata['primary_keys']}")
        print(f"   Cursor Field: {metadata.get('cursor_field', 'N/A')}")
        print(f"   Ingestion Type: {metadata['ingestion_type']}")
    except Exception as e:
        print(f"   ✗ Failed to get metadata: {e}")
        return
    
    # List available queues
    print("\n7. Listing Available Queue Definitions...")
    try:
        import requests
        headers = connector._get_headers()
        url = f"{connector.api_base_url}/odata/QueueDefinitions"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            queues = response.json().get('value', [])
            print(f"   ✓ Found {len(queues)} queue(s) in folder {config['folder_id']}")
            for queue in queues[:10]:  # Show first 10
                print(f"      - ID: {queue.get('Id')}, Name: {queue.get('Name')}")
            
            # Verify our target queue exists
            target_id = int(table_config["queue_items"]["queue_definition_id"])
            target_queue = next((q for q in queues if q.get('Id') == target_id), None)
            if target_queue:
                print(f"\n   ✓ Target queue found!")
                print(f"      ID: {target_queue.get('Id')}")
                print(f"      Name: {target_queue.get('Name')}")
            else:
                print(f"\n   ⚠ Target queue ID {target_id} not found in this folder!")
                print(f"      Available queue IDs: {[q.get('Id') for q in queues]}")
        else:
            print(f"   ✗ Failed to list queues: {response.status_code} {response.text}")
    except Exception as e:
        print(f"   ✗ Failed to list queues: {e}")
        import traceback
        traceback.print_exc()
    
    # Read queue items
    print("\n8. Reading Queue Items (3-step export process)...")
    print(f"   Using Queue Definition ID: {table_config['queue_items']['queue_definition_id']}")
    print("   Step 1: Initiating export...")
    print("   Step 2: Polling export status...")
    print("   Step 3: Downloading and parsing CSV...")
    
    try:
        records_iterator, offset = connector.read_table(
            "queue_items", 
            start_offset={},
            table_options=table_config["queue_items"]
        )
        
        # Collect records
        records = list(records_iterator)
        
        print(f"\n   ✓ Export completed successfully!")
        print(f"   Total records retrieved: {len(records)}")
        print(f"   Next offset: {offset}")
        
        # Display records
        if records:
            print("\n" + "=" * 80)
            print("QUEUE ITEMS RETRIEVED")
            print("=" * 80)
            
            for i, record in enumerate(records, 1):
                print(f"\n--- Queue Item {i} ---")
                print(f"  ID: {record.get('Id')}")
                print(f"  Queue Definition ID: {record.get('QueueDefinitionId')}")
                print(f"  Name: {record.get('Name')}")
                print(f"  Reference: {record.get('Reference')}")
                print(f"  Priority: {record.get('Priority')}")
                print(f"  Status: {record.get('Status')}")
                print(f"  Creation Time: {record.get('CreationTime')}")
                print(f"  Last Modified: {record.get('LastModificationTime')}")
                
                # Show SpecificContent if present
                specific_content = record.get('SpecificContent')
                if specific_content and specific_content != '':
                    print(f"  Specific Content: {specific_content[:200]}...")
                
                # Show Robot if present
                robot = record.get('Robot')
                if robot:
                    print(f"  Robot: {robot}")
                
                # Show ReviewerUser if present
                reviewer = record.get('ReviewerUser')
                if reviewer:
                    print(f"  Reviewer: {reviewer}")
            
            print("\n" + "=" * 80)
        else:
            print("\n   ⚠ No records found in the queue")
        
        print("\n✓ CONNECTION TEST SUCCESSFUL!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n   ✗ Failed to read queue items: {e}")
        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    main()
