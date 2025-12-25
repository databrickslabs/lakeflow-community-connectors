"""
Comprehensive test suite for LanceDB Lakeflow Connector.

This module provides thorough testing of the LanceDB connector including:
- Connection and authentication
- Table listing and schema retrieval
- Data reading with various configurations
- Error handling and edge cases
- Security validations
"""

from pathlib import Path

import pytest

from tests import test_suite  # pylint: disable=import-error
from tests.test_suite import LakeflowConnectTester  # pylint: disable=import-error
from tests.test_utils import load_config  # pylint: disable=import-error
from sources.lancedb.lancedb import (  # pylint: disable=import-error
    LakeflowConnect,
    LanceDBTableOptions
)


def test_lancedb_connector():
    """
    Test the LanceDB connector using the standard test suite.

    This test validates all core functionality including:
    - Connection initialization
    - Table discovery
    - Schema retrieval
    - Metadata reading
    - Data retrieval with pagination
    - Incremental reads (if configured)

    The test uses configuration from dev_config.json and expects
    a LanceDB Cloud instance to be accessible with valid credentials.
    """
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be
    # available
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"

    # Check if table config exists, otherwise use empty dict
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)

    # Load table config if it exists, otherwise use empty config
    if table_config_path.exists():
        table_config = load_config(table_config_path)
    else:
        table_config = {}

    # Set default table options for testing with full scan enabled
    # This allows the connector to use dummy vectors for non-vector queries
    if "use_full_scan" not in table_config:
        table_config["use_full_scan"] = "true"
    if "batch_size" not in table_config:
        table_config["batch_size"] = "10"

    # Create tester with the config
    tester = LakeflowConnectTester(config, table_config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (f"Test suite had failures: {
        report.failed_tests} failed, {
        report.error_tests} errors")


def test_connection_validation():
    """
    Test connection parameter validation.

    Ensures that the connector properly validates required parameters
    and raises appropriate errors for missing or invalid credentials.
    """
    # Test missing api_key
    with pytest.raises(ValueError, match="Missing required parameter: api_key"):
        LakeflowConnect({"project_name": "test", "region": "us-east-1"})

    # Test missing project_name
    with pytest.raises(ValueError, match="Missing required parameter: project_name"):
        LakeflowConnect({"api_key": "test-key", "region": "us-east-1"})

    # Test missing region
    with pytest.raises(ValueError, match="Missing required parameter: region"):
        LakeflowConnect({"api_key": "test-key", "project_name": "test"})


def test_identifier_sanitization():
    """
    Test that identifiers are properly sanitized to prevent injection attacks.

    Validates that table names, project names, and other identifiers
    reject malicious input patterns.
    """
    # Test invalid project name (SQL injection attempt)
    with pytest.raises(ValueError, match="Invalid identifier"):
        LakeflowConnect({
            "api_key": "test-key",
            "project_name": "test'; DROP TABLE users--",
            "region": "us-east-1"
        })

    # Test invalid region (URL injection attempt)
    with pytest.raises(ValueError, match="Invalid identifier"):
        LakeflowConnect({
            "api_key": "test-key",
            "project_name": "test",
            "region": "us-east-1/../../../etc/passwd"
        })


def test_column_name_validation():
    """
    Test that column names in table options are validated.

    Ensures that primary key and column specifications reject
    malicious input to prevent SQL injection.
    """
    # Valid column names should pass
    options = LanceDBTableOptions(primary_keys=["id", "user_id", "created_at"])
    assert options.primary_keys == ["id", "user_id", "created_at"]

    # Invalid column names should fail
    with pytest.raises(ValueError, match="Invalid column name"):
        LanceDBTableOptions(primary_keys=["id", "user_id; DROP TABLE users--"])

    # Invalid cursor field should fail
    with pytest.raises(ValueError, match="Invalid cursor field"):
        LanceDBTableOptions(cursor_field="created_at' OR '1'='1")


def test_batch_size_validation():
    """
    Test that batch size is properly validated.

    Ensures that batch_size parameter is within acceptable bounds
    to prevent resource exhaustion attacks.
    """
    # Valid batch size
    options = LanceDBTableOptions(batch_size=1000)
    assert options.batch_size == 1000

    # Batch size too small
    with pytest.raises(ValueError):
        LanceDBTableOptions(batch_size=0)

    # Batch size too large
    with pytest.raises(ValueError):
        LanceDBTableOptions(batch_size=100000)


def test_column_projection_api_support():
    """
    Test if LanceDB API actually supports column projection.
    
    This test verifies:
    1. API honors the 'columns' parameter
    2. Only requested columns are returned
    3. LanceDB-added columns (like _distance) are handled correctly
    
    If this test fails, it means the fallback post-fetch filtering is NEEDED.
    If this test passes, the fallback is just defensive programming.
    """
    import json
    
    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    connector = LakeflowConnect(config)
    
    # Get list of tables
    tables = connector.list_tables()
    
    # Skip if no tables available
    if not tables:
        pytest.skip("No tables available for testing")
    
    # Use first available table
    test_table = tables[0]
    
    # Get full schema to know available columns
    full_schema = connector.get_table_schema(test_table, {})
    all_columns = [field.name for field in full_schema.fields]
    
    # Select subset of columns (first 3 columns, or all if less than 3)
    if len(all_columns) >= 3:
        requested_columns = all_columns[:3]
    else:
        requested_columns = all_columns[:1]  # At least 1 column
    
    print(f"\n{'='*70}")
    print(f"Testing API Column Projection Support")
    print(f"{'='*70}")
    print(f"Table: {test_table}")
    print(f"Available columns: {all_columns}")
    print(f"Requested columns: {requested_columns}")
    
    # Read data with column projection
    table_options = {
        "use_full_scan": "true",
        "batch_size": "10",
        "columns": json.dumps(requested_columns)
    }
    
    records_iter, _ = connector.read_table(test_table, None, table_options)
    
    # Get first record
    try:
        first_record = next(records_iter)
    except StopIteration:
        pytest.skip(f"Table {test_table} has no data")
    
    returned_columns = set(first_record.keys())
    requested_columns_set = set(requested_columns)
    
    print(f"Returned columns: {sorted(returned_columns)}")
    
    # LanceDB may add system columns like _distance
    lancedb_system_columns = {'_distance', '_rowid'}
    extra_columns = returned_columns - requested_columns_set - lancedb_system_columns
    
    # Check if API honored the column projection
    if extra_columns:
        print(f"\n{'❌'*35}")
        print(f"RESULT: API did NOT honor column projection")
        print(f"{'❌'*35}")
        print(f"Unexpected columns returned: {extra_columns}")
        print(f"CONCLUSION: Fallback filtering IS NEEDED")
        print(f"{'='*70}\n")
        assert False, (
            f"LanceDB API returned unexpected columns: {extra_columns}. "
            f"This means the fallback post-fetch filtering logic is REQUIRED."
        )
    else:
        print(f"\n{'✅'*35}")
        print(f"RESULT: API honored column projection!")
        print(f"{'✅'*35}")
        print(f"Only requested columns (+ system columns) were returned")
        print(f"CONCLUSION: Fallback filtering is NOT needed")
        print(f"           (but good to keep for defensive programming)")
        print(f"{'='*70}\n")
        
        # Additional verification: ensure all requested columns are present
        missing_columns = requested_columns_set - returned_columns
        assert not missing_columns, (
            f"LanceDB API didn't return all requested columns. "
            f"Missing: {missing_columns}"
        )


def test_column_projection_performance():
    """
    Test that column projection actually reduces data transfer.
    
    Compares record sizes when:
    1. Reading all columns
    2. Reading subset of columns
    
    Expected: Subset should return fewer columns.
    """
    import json
    
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    connector = LakeflowConnect(config)
    
    tables = connector.list_tables()
    if not tables:
        pytest.skip("No tables available for testing")
    
    # Find a table with multiple columns
    test_table = None
    for table in tables:
        schema = connector.get_table_schema(table, {})
        if len(schema.fields) >= 3:
            test_table = table
            break
    
    if not test_table:
        pytest.skip("No suitable table with enough columns for testing")
    
    full_schema = connector.get_table_schema(test_table, {})
    all_columns = [field.name for field in full_schema.fields]
    
    # Select a small subset
    small_columns = all_columns[:2]
    
    print(f"\n{'='*70}")
    print(f"Testing Column Projection Performance Impact")
    print(f"{'='*70}")
    print(f"Table: {test_table}")
    print(f"Total columns available: {len(all_columns)}")
    print(f"Columns to request: {len(small_columns)} ({small_columns})")
    
    # Test 1: Read all columns
    all_cols_options = {
        "use_full_scan": "true",
        "batch_size": "5"
    }
    records_iter, _ = connector.read_table(test_table, None, all_cols_options)
    try:
        full_record = next(records_iter)
        full_record_size = len(full_record)
    except StopIteration:
        pytest.skip("Table has no data")
    
    # Test 2: Read subset of columns
    subset_options = {
        "use_full_scan": "true",
        "batch_size": "5",
        "columns": json.dumps(small_columns)
    }
    records_iter, _ = connector.read_table(test_table, None, subset_options)
    subset_record = next(records_iter)
    subset_record_size = len(subset_record)
    
    print(f"\nResults:")
    print(f"  All columns:    {full_record_size} fields")
    print(f"  Subset request: {subset_record_size} fields")
    
    # Account for system columns
    lancedb_system_columns = {'_distance', '_rowid'}
    max_expected = len(small_columns) + len(lancedb_system_columns)
    
    # Verify subset is smaller or equal to expected
    if subset_record_size <= max_expected:
        reduction = full_record_size - subset_record_size
        print(f"\n✅ Column projection working!")
        print(f"   Reduced by {reduction} columns ({reduction/full_record_size*100:.1f}%)")
        print(f"{'='*70}\n")
    else:
        print(f"\n❌ Column projection NOT working!")
        print(f"   Expected <= {max_expected} columns, got {subset_record_size}")
        print(f"{'='*70}\n")
        assert False, (
            f"Column projection didn't reduce data: "
            f"expected <= {max_expected}, got {subset_record_size}"
        )


def test_schema_matches_data():
    """
    Test that schema filtering works correctly to match actual data.

    When columns are specified, the schema should only include those columns
    to prevent NULL values in Spark for excluded columns.
    """
    import json

    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    config = load_config(config_path)
    connector = LakeflowConnect(config)

    tables = connector.list_tables()
    if not tables:
        pytest.skip("No tables available for testing")

    test_table = tables[0]

    print(f"\n{'='*70}")
    print(f"Testing Schema Matches Data")
    print(f"{'='*70}")
    print(f"Table: {test_table}")

    # Get full schema
    full_schema = connector.get_table_schema(test_table, {})
    all_columns = [field.name for field in full_schema.fields]
    print(f"Full schema: {len(all_columns)} columns - {all_columns}")

    # Select subset of columns (first 2)
    if len(all_columns) >= 2:
        requested_columns = all_columns[:2]
    else:
        pytest.skip("Not enough columns to test schema filtering")

    print(f"Requesting: {requested_columns}")

    # Get filtered schema
    filtered_options = {
        "columns": json.dumps(requested_columns)
    }
    filtered_schema = connector.get_table_schema(test_table, filtered_options)
    schema_columns = [field.name for field in filtered_schema.fields]
    print(f"Filtered schema: {len(schema_columns)} columns - {schema_columns}")

    # Verify schema is filtered
    assert len(schema_columns) == len(requested_columns), (
        f"Schema not filtered correctly: expected {len(requested_columns)} columns, "
        f"got {len(schema_columns)}"
    )

    for col in requested_columns:
        assert col in schema_columns, f"Requested column '{col}' not in schema"

    # Now verify data matches schema
    data_options = {
        "use_full_scan": "true",
        "batch_size": "1",
        "columns": json.dumps(requested_columns)
    }
    records_iter, _ = connector.read_table(test_table, None, data_options)
    records = list(records_iter)

    if not records:
        pytest.skip("No data available for testing")

    data_columns = set(records[0].keys())
    schema_columns_set = set(schema_columns)

    print(f"Data columns: {sorted(data_columns)}")

    # Allow for LanceDB system columns in data (_distance, _rowid)
    lancedb_system_columns = {'_distance', '_rowid'}
    data_columns_without_system = data_columns - lancedb_system_columns

    # Verify data columns match schema columns
    assert data_columns_without_system == schema_columns_set, (
        f"Data columns don't match schema: "
        f"schema={sorted(schema_columns_set)}, data={sorted(data_columns_without_system)}"
    )

    print(f"\n✅ Schema matches data perfectly!")
    print(f"   Schema columns: {sorted(schema_columns_set)}")
    print(f"   Data columns (excluding system): {sorted(data_columns_without_system)}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
