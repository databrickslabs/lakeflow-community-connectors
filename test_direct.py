#!/usr/bin/env python3
"""
Direct test script with hardcoded correct queue ID
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from sources.uipathqueues.uipathqueues import LakeflowConnect


def main():
    print("=" * 80)
    print("UiPath Queue Items - Direct Connection Test")
    print("=" * 80)
    
    # Configuration with correct values
    config = {
        "organization_name": "databwhnslig",
        "tenant_name": "DefaultTenant",
        "client_id": "5822280a-f26d-4284-8aa4-498c9348161d",
        "client_secret": "C0SPPwGjUf@PN8zQ5_pbKIenin07vMoOiqZTJ#z3cowl0^rj?2MnPV9cirsxhpkE",
        "folder_id": "434435",
        "scope": "OR.Queues OR.Execution"
    }
    
    table_options = {
        "queue_definition_id": "174358",  # Correct ID for loan_queue
        "expand": "Robot,ReviewerUser",
        "filter": ""
    }
    
    print(f"\nConfiguration:")
    print(f"  Organization: {config['organization_name']}")
    print(f"  Tenant: {config['tenant_name']}")
    print(f"  Folder ID: {config['folder_id']}")
    print(f"  Queue Definition ID: {table_options['queue_definition_id']}")
    
    # Initialize connector
    print("\n1. Initializing Connector...")
    connector = LakeflowConnect(config)
    print("   ✓ Connector initialized")
    
    # Test authentication
    print("\n2. Testing Authentication...")
    token = connector._get_access_token()
    print(f"   ✓ Authentication successful")
    
    # Read queue items
    print("\n3. Reading Queue Items from 'loan_queue' (ID: 174358)...")
    print("   - Initiating export...")
    print("   - Polling export status...")
    print("   - Downloading CSV...")
    
    try:
        # First, let's download the CSV and save it to inspect
        export_id = connector._initiate_export("174358", "", "Robot,ReviewerUser")
        connector._poll_export_status(export_id)
        download_url = connector._get_download_link(export_id)
        csv_content = connector._download_csv(download_url)
        
        # Save CSV for inspection
        with open("downloaded_queue_items.csv", "w") as f:
            f.write(csv_content)
        print(f"   ✓ CSV saved to downloaded_queue_items.csv ({len(csv_content)} bytes)")
        
        # Show first few lines
        lines = csv_content.split('\n')[:3]
        print(f"\n   CSV Header: {lines[0] if lines else 'N/A'}")
        
        # Now read through the connector
        records_iterator, offset = connector.read_table(
            "queue_items",
            start_offset={},
            table_options=table_options
        )
        
        records = list(records_iterator)
        
        print(f"\n   ✓ Export completed successfully!")
        print(f"   Total records retrieved: {len(records)}")
        
        if records:
            print("\n" + "=" * 80)
            print(f"RETRIEVED {len(records)} QUEUE ITEM(S)")
            print("=" * 80)
            
            for i, record in enumerate(records, 1):
                print(f"\n--- Queue Item #{i} ---")
                print(f"  ID: {record.get('Id')}")
                print(f"  Queue: {record.get('Name')}")
                print(f"  Reference: {record.get('Reference')}")
                print(f"  Priority: {record.get('Priority')}")
                print(f"  Status: {record.get('Status')}")
                print(f"  Created: {record.get('CreationTime')}")
                print(f"  Last Modified: {record.get('LastModificationTime')}")
                
                # Parse SpecificContent if it's JSON
                specific_content = record.get('SpecificContent')
                if specific_content and specific_content.strip():
                    print(f"  Specific Content (raw): {specific_content}")
                    try:
                        import json
                        content_dict = json.loads(specific_content)
                        print(f"  Specific Content (parsed):")
                        for key, value in content_dict.items():
                            print(f"    - {key}: {value}")
                    except:
                        pass
                
                robot = record.get('Robot')
                if robot:
                    print(f"  Robot: ID={robot.get('Id')}, Name={robot.get('Name')}, Type={robot.get('Type')}")
                
                reviewer = record.get('ReviewerUser')
                if reviewer:
                    print(f"  Reviewer: {reviewer.get('Username')} ({reviewer.get('Email')})")
            
            print("\n" + "=" * 80)
            print("✓ TEST SUCCESSFUL!")
            print("=" * 80)
        else:
            print("\n   ⚠ Queue is empty - no items found")
            print("   This may be normal if the queue hasn't been populated yet.")
        
    except Exception as e:
        print(f"\n   ✗ Failed to read queue items")
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
