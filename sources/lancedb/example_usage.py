"""
Example usage of the LanceDB Lakeflow Connector.

This script demonstrates how to use the LanceDB connector to:
1. Connect to a LanceDB Cloud instance
2. List available tables
3. Retrieve table schemas
4. Read table metadata
5. Fetch data from tables with various configurations

This is for demonstration purposes only and requires valid LanceDB credentials.
"""

from sources.lancedb.lancedb import LakeflowConnect


def example_basic_connection():
    """
    Example 1: Basic connection and table listing.
    
    Demonstrates how to connect to LanceDB and discover available tables.
    """
    print("=" * 60)
    print("Example 1: Basic Connection and Table Listing")
    print("=" * 60)
    
    # Configure connection options
    options = {
        "api_key": "your-api-key-here",
        "project_name": "your-project-name",
        "region": "us-east-1"
    }
    
    # Initialize connector
    connector = LakeflowConnect(options)
    
    # List all tables
    tables = connector.list_tables()
    print(f"Found {len(tables)} tables:")
    for table in tables:
        print(f"  - {table}")
    
    print()


def example_get_schema():
    """
    Example 2: Retrieve table schema.
    
    Shows how to get the schema of a specific table.
    """
    print("=" * 60)
    print("Example 2: Retrieve Table Schema")
    print("=" * 60)
    
    options = {
        "api_key": "your-api-key-here",
        "project_name": "your-project-name",
        "region": "us-east-1"
    }
    
    connector = LakeflowConnect(options)
    
    # Get schema for a specific table
    table_name = "documents"  # Replace with your table name
    schema = connector.get_table_schema(table_name, {})
    
    print(f"Schema for table '{table_name}':")
    print(schema.simpleString())
    print()


def example_snapshot_read():
    """
    Example 3: Snapshot read (full table scan).
    
    Demonstrates reading all data from a table in snapshot mode.
    """
    print("=" * 60)
    print("Example 3: Snapshot Read (Full Table Scan)")
    print("=" * 60)
    
    options = {
        "api_key": "your-api-key-here",
        "project_name": "your-project-name",
        "region": "us-east-1"
    }
    
    connector = LakeflowConnect(options)
    
    table_name = "documents"
    table_options = {
        "batch_size": 1000
    }
    
    # Read table metadata
    metadata = connector.read_table_metadata(table_name, table_options)
    print(f"Table metadata: {metadata}")
    
    # Read data
    records_iterator, end_offset = connector.read_table(
        table_name=table_name,
        start_offset={},
        table_options=table_options
    )
    
    # Process records
    record_count = 0
    for record in records_iterator:
        record_count += 1
        if record_count <= 3:  # Print first 3 records
            print(f"Record {record_count}: {record}")
    
    print(f"Total records read: {record_count}")
    print(f"End offset: {end_offset}")
    print()


def example_incremental_append():
    """
    Example 4: Incremental append read.
    
    Shows how to read only new records using a cursor field.
    """
    print("=" * 60)
    print("Example 4: Incremental Append Read")
    print("=" * 60)
    
    options = {
        "api_key": "your-api-key-here",
        "project_name": "your-project-name",
        "region": "us-east-1"
    }
    
    connector = LakeflowConnect(options)
    
    table_name = "events"
    table_options = {
        "cursor_field": "created_at",
        "batch_size": 1000
    }
    
    # First sync - read from a starting point
    start_offset = {
        "cursor_value": "2024-01-01T00:00:00Z"
    }
    
    metadata = connector.read_table_metadata(table_name, table_options)
    print(f"Table metadata: {metadata}")
    print(f"Ingestion type: {metadata['ingestion_type']}")
    
    records_iterator, end_offset = connector.read_table(
        table_name=table_name,
        start_offset=start_offset,
        table_options=table_options
    )
    
    record_count = 0
    for record in records_iterator:
        record_count += 1
    
    print(f"Records read: {record_count}")
    print(f"End offset for next sync: {end_offset}")
    print()


def example_cdc_mode():
    """
    Example 5: CDC (Change Data Capture) mode.
    
    Demonstrates incremental reads with upserts based on primary keys.
    """
    print("=" * 60)
    print("Example 5: CDC Mode (Incremental Upserts)")
    print("=" * 60)
    
    options = {
        "api_key": "your-api-key-here",
        "project_name": "your-project-name",
        "region": "us-east-1"
    }
    
    connector = LakeflowConnect(options)
    
    table_name = "users"
    table_options = {
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
        "batch_size": 1000
    }
    
    metadata = connector.read_table_metadata(table_name, table_options)
    print(f"Table metadata: {metadata}")
    print(f"Primary keys: {metadata['primary_keys']}")
    print(f"Cursor field: {metadata.get('cursor_field', 'N/A')}")
    print(f"Ingestion type: {metadata['ingestion_type']}")
    
    # Read with cursor from last sync
    start_offset = {
        "cursor_value": "2024-11-01T00:00:00Z"
    }
    
    records_iterator, end_offset = connector.read_table(
        table_name=table_name,
        start_offset=start_offset,
        table_options=table_options
    )
    
    record_count = 0
    for record in records_iterator:
        record_count += 1
    
    print(f"Records read: {record_count}")
    print(f"End offset: {end_offset}")
    print()


def example_filtered_query():
    """
    Example 6: Filtered query with column selection.
    
    Shows how to read specific columns with filtering.
    """
    print("=" * 60)
    print("Example 6: Filtered Query with Column Selection")
    print("=" * 60)
    
    options = {
        "api_key": "your-api-key-here",
        "project_name": "your-project-name",
        "region": "us-east-1"
    }
    
    connector = LakeflowConnect(options)
    
    table_name = "logs"
    table_options = {
        "filter_expression": "severity = 'ERROR' AND timestamp > '2024-11-01'",
        "columns": ["timestamp", "message", "severity", "user_id"],
        "batch_size": 500
    }
    
    records_iterator, end_offset = connector.read_table(
        table_name=table_name,
        start_offset={},
        table_options=table_options
    )
    
    record_count = 0
    for record in records_iterator:
        record_count += 1
        if record_count <= 3:
            print(f"Record {record_count}: {record}")
    
    print(f"Total filtered records: {record_count}")
    print()


def main():
    """
    Main function to run all examples.
    
    Note: Replace placeholder credentials with actual values before running.
    """
    print("\n")
    print("=" * 60)
    print("LanceDB Lakeflow Connector - Usage Examples")
    print("=" * 60)
    print("\n")
    
    # Run examples (comment out as needed)
    try:
        example_basic_connection()
        example_get_schema()
        example_snapshot_read()
        example_incremental_append()
        example_cdc_mode()
        example_filtered_query()
        
        print("=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error running examples: {e}")
        print("\nPlease ensure:")
        print("1. You have valid LanceDB credentials")
        print("2. Your credentials are properly configured in the examples")
        print("3. You have network access to LanceDB Cloud")
        print("4. The table names used in examples exist in your project")


if __name__ == "__main__":
    main()

