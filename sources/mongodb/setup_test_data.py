#!/usr/bin/env python3
"""
Script to set up test data in MongoDB (local or Atlas) with proper error handling.

Usage:
    python sources/mongodb/setup_test_data.py "mongodb+srv://user:pass@cluster.mongodb.net/"
    python sources/mongodb/setup_test_data.py "mongodb://localhost:27017/"
"""

import sys
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import (
    ConnectionFailure,
    OperationFailure,
    ConfigurationError,
    ServerSelectionTimeoutError,
    DuplicateKeyError
)


def setup_test_data(connection_uri: str, database_name: str = "testdb") -> bool:
    """
    Set up test data in MongoDB with comprehensive error handling.
    
    Args:
        connection_uri: MongoDB connection string
        database_name: Database name to create test data in
        
    Returns:
        True if successful, False otherwise
    """
    print("="*70)
    print("MongoDB Test Data Setup")
    print("="*70)
    print(f"\nConnection URI: {connection_uri[:30]}... (truncated)")
    print(f"Target Database: {database_name}")
    
    client = None
    
    try:
        # Step 1: Connect to MongoDB
        print("\n[1/6] Connecting to MongoDB...")
        client = MongoClient(
            connection_uri,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=10000
        )
        
        # Test connection
        client.admin.command('ping')
        print("✓ Connected successfully!")
        
    except ConfigurationError as e:
        print(f"✗ Configuration Error: {e}")
        if "dnspython" in str(e).lower():
            print("\n💡 Solution: Install dnspython for SRV connection strings:")
            print("   pip install dnspython")
        return False
        
    except ServerSelectionTimeoutError as e:
        print(f"✗ Connection Timeout: {e}")
        print("\n💡 Possible solutions:")
        print("   1. Check network connectivity")
        print("   2. Verify MongoDB is running")
        print("   3. For Atlas: Check Network Access (IP whitelist)")
        print("   4. Verify connection string format")
        return False
        
    except ConnectionFailure as e:
        print(f"✗ Connection Failed: {e}")
        print("\n💡 Possible solutions:")
        print("   1. Verify credentials are correct")
        print("   2. Check authentication database")
        print("   3. Ensure user has proper permissions")
        return False
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    try:
        # Step 2: Access database
        print(f"\n[2/6] Accessing database '{database_name}'...")
        db = client[database_name]
        print(f"✓ Database accessed")
        
        # Step 3: Check/create collections and insert users
        print(f"\n[3/6] Creating 'users' collection...")
        users_collection = db['users']
        
        # Clear existing data (optional - comment out to keep existing data)
        existing_count = users_collection.count_documents({})
        if existing_count > 0:
            print(f"   Found {existing_count} existing documents")
            print(f"   Clearing existing data...")
            result = users_collection.delete_many({})
            print(f"   Deleted {result.deleted_count} documents")
        
        # Insert users
        users_data = [
            {
                "name": "Alice Johnson",
                "email": "alice@test.com",
                "age": 28,
                "active": True,
                "created_at": datetime.utcnow(),
                "tags": ["developer", "python"]
            },
            {
                "name": "Bob Smith",
                "email": "bob@test.com",
                "age": 35,
                "active": True,
                "created_at": datetime.utcnow(),
                "tags": ["manager", "agile"]
            },
            {
                "name": "Charlie Davis",
                "email": "charlie@test.com",
                "age": 42,
                "active": False,
                "created_at": datetime.utcnow(),
                "tags": ["architect", "cloud"]
            }
        ]
        
        result = users_collection.insert_many(users_data)
        print(f"✓ Inserted {len(result.inserted_ids)} users")
        print(f"   IDs: {[str(id) for id in result.inserted_ids]}")
        
    except OperationFailure as e:
        print(f"✗ Operation Failed: {e}")
        print("\n💡 Possible solutions:")
        print("   1. Check user permissions (needs write access)")
        print("   2. Verify database name is correct")
        print("   3. Check if user is read-only")
        if client:
            client.close()
        return False
        
    except DuplicateKeyError as e:
        print(f"✗ Duplicate Key Error: {e}")
        print("\n💡 Solution: Data might already exist. Try clearing the collection first.")
        if client:
            client.close()
        return False
        
    except Exception as e:
        print(f"✗ Unexpected error inserting users: {e}")
        if client:
            client.close()
        return False
    
    try:
        # Step 4: Create orders collection
        print(f"\n[4/6] Creating 'orders' collection...")
        orders_collection = db['orders']
        
        # Clear existing
        existing_count = orders_collection.count_documents({})
        if existing_count > 0:
            print(f"   Clearing {existing_count} existing documents...")
            orders_collection.delete_many({})
        
        orders_data = [
            {
                "order_id": "ORD-001",
                "user_id": 1,
                "total": 99.99,
                "status": "completed",
                "items": ["item1", "item2"],
                "created_at": datetime.utcnow()
            },
            {
                "order_id": "ORD-002",
                "user_id": 2,
                "total": 149.99,
                "status": "pending",
                "items": ["item3"],
                "created_at": datetime.utcnow()
            },
            {
                "order_id": "ORD-003",
                "user_id": 1,
                "total": 249.99,
                "status": "shipped",
                "items": ["item4", "item5", "item6"],
                "created_at": datetime.utcnow()
            }
        ]
        
        result = orders_collection.insert_many(orders_data)
        print(f"✓ Inserted {len(result.inserted_ids)} orders")
        
    except Exception as e:
        print(f"✗ Error inserting orders: {e}")
        if client:
            client.close()
        return False
    
    try:
        # Step 5: Create products collection
        print(f"\n[5/6] Creating 'products' collection...")
        products_collection = db['products']
        
        # Clear existing
        existing_count = products_collection.count_documents({})
        if existing_count > 0:
            print(f"   Clearing {existing_count} existing documents...")
            products_collection.delete_many({})
        
        products_data = [
            {
                "sku": "SKU-001",
                "name": "Widget",
                "price": 19.99,
                "stock": 100,
                "tags": ["new", "featured"],
                "created_at": datetime.utcnow()
            },
            {
                "sku": "SKU-002",
                "name": "Gadget",
                "price": 29.99,
                "stock": 50,
                "tags": ["sale"],
                "created_at": datetime.utcnow()
            },
            {
                "sku": "SKU-003",
                "name": "Doohickey",
                "price": 39.99,
                "stock": 75,
                "tags": ["popular"],
                "created_at": datetime.utcnow()
            }
        ]
        
        result = products_collection.insert_many(products_data)
        print(f"✓ Inserted {len(result.inserted_ids)} products")
        
    except Exception as e:
        print(f"✗ Error inserting products: {e}")
        if client:
            client.close()
        return False
    
    try:
        # Step 6: Verify data
        print(f"\n[6/6] Verifying inserted data...")
        
        users_count = users_collection.count_documents({})
        orders_count = orders_collection.count_documents({})
        products_count = products_collection.count_documents({})
        
        print(f"   Users: {users_count} documents")
        print(f"   Orders: {orders_count} documents")
        print(f"   Products: {products_count} documents")
        
        # Sample one document from each collection
        print(f"\n   Sample Documents:")
        
        sample_user = users_collection.find_one()
        if sample_user:
            print(f"   User: {sample_user.get('name')} ({sample_user.get('email')})")
        
        sample_order = orders_collection.find_one()
        if sample_order:
            print(f"   Order: {sample_order.get('order_id')} - ${sample_order.get('total')}")
        
        sample_product = products_collection.find_one()
        if sample_product:
            print(f"   Product: {sample_product.get('name')} - ${sample_product.get('price')}")
        
        print("\n" + "="*70)
        print("✓ SUCCESS! All test data inserted successfully!")
        print("="*70)
        print(f"\nYou can now run tests with database: {database_name}")
        print("\nNext steps:")
        print("1. Update sources/mongodb/configs/dev_config.json")
        print("2. Run: pytest sources/mongodb/test/test_mongodb_lakeflow_connect.py -v")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"✗ Error verifying data: {e}")
        if client:
            client.close()
        return False


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python setup_test_data.py <connection_uri> [database_name]")
        print("\nExamples:")
        print('  Local:  python sources/mongodb/setup_test_data.py "mongodb://localhost:27017/"')
        print('  Atlas:  python sources/mongodb/setup_test_data.py "mongodb+srv://user:pass@cluster.mongodb.net/"')
        print("\nOptional: specify database name (default: testdb)")
        print('  python sources/mongodb/setup_test_data.py "mongodb://localhost:27017/" mydb')
        sys.exit(1)
    
    connection_uri = sys.argv[1]
    database_name = sys.argv[2] if len(sys.argv) > 2 else "testdb"
    
    # Run setup
    success = setup_test_data(connection_uri, database_name)
    
    if not success:
        print("\n" + "="*70)
        print("✗ FAILED - Test data setup incomplete")
        print("="*70)
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()

