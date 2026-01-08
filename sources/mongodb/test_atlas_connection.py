#!/usr/bin/env python3
"""
Test script to validate MongoDB Atlas SRV connection strings.
This script helps verify that the connector can properly connect to MongoDB Atlas.

Usage:
    python sources/mongodb/test_atlas_connection.py "mongodb+srv://user:pass@cluster.mongodb.net/database"
"""

import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError


def test_srv_connection(connection_uri: str) -> bool:
    """
    Test if a MongoDB Atlas SRV connection string works.
    
    Args:
        connection_uri: MongoDB connection string (should start with mongodb+srv://)
        
    Returns:
        True if connection successful, False otherwise
    """
    print(f"Testing connection to MongoDB Atlas...")
    print(f"Connection URI: {connection_uri[:30]}... (truncated for security)")
    
    # Check if it's an SRV URI
    if connection_uri.startswith("mongodb+srv://"):
        print("✓ Detected MongoDB Atlas SRV connection string")
    elif connection_uri.startswith("mongodb://"):
        print("⚠ Standard MongoDB URI detected (not SRV)")
    else:
        print("✗ Invalid MongoDB URI format")
        return False
    
    try:
        # Try to connect
        print("\n1. Initializing MongoClient...")
        client = MongoClient(
            connection_uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000
        )
        
        # Test connection
        print("2. Testing connection with ping command...")
        client.admin.command('ping')
        print("✓ Connection successful!")
        
        # Get topology type
        print("\n3. Checking deployment topology...")
        topology_type = client.topology_description.topology_type_name
        print(f"   Topology: {topology_type}")
        
        if topology_type in ['ReplicaSetWithPrimary', 'Sharded']:
            print("✓ Deployment supports Change Streams (CDC)")
        else:
            print("⚠ Deployment does not support Change Streams")
        
        # List databases
        print("\n4. Listing databases...")
        db_names = client.list_database_names()
        print(f"   Found {len(db_names)} databases:")
        for db_name in db_names[:5]:  # Show first 5
            print(f"   - {db_name}")
        if len(db_names) > 5:
            print(f"   ... and {len(db_names) - 5} more")
        
        # Test database access
        if db_names:
            test_db = db_names[0]
            print(f"\n5. Listing collections in '{test_db}'...")
            collections = client[test_db].list_collection_names()
            print(f"   Found {len(collections)} collections:")
            for coll_name in collections[:5]:  # Show first 5
                print(f"   - {coll_name}")
            if len(collections) > 5:
                print(f"   ... and {len(collections) - 5} more")
        
        print("\n" + "="*60)
        print("✓ All tests passed! MongoDB Atlas connection is working.")
        print("="*60)
        
        client.close()
        return True
        
    except ConfigurationError as e:
        print(f"\n✗ Configuration Error: {e}")
        if "dnspython" in str(e).lower():
            print("\n💡 Solution: Install dnspython package:")
            print("   pip install dnspython")
        return False
        
    except ConnectionFailure as e:
        print(f"\n✗ Connection Failed: {e}")
        print("\n💡 Possible solutions:")
        print("   1. Check network connectivity")
        print("   2. Verify credentials are correct")
        print("   3. Check MongoDB Atlas IP whitelist")
        print("   4. Ensure database user has proper permissions")
        return False
        
    except Exception as e:
        print(f"\n✗ Unexpected Error: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_atlas_connection.py <connection_uri>")
        print("\nExample:")
        print('  python sources/mongodb/test_atlas_connection.py "mongodb+srv://user:pass@cluster.mongodb.net/database"')
        sys.exit(1)
    
    connection_uri = sys.argv[1]
    success = test_srv_connection(connection_uri)
    sys.exit(0 if success else 1)

