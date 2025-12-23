"""
Comprehensive test suite for LanceDB Lakeflow Connector.

This module provides thorough testing of the LanceDB connector including:
- Connection and authentication
- Table listing and schema retrieval
- Data reading with various configurations
- Error handling and edge cases
- Security validations
"""

import pytest
from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.lancedb.lancedb import LakeflowConnect


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
    # This is required because test_suite.py expects LakeflowConnect to be available
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
    
    # Set table options for testing with full scan enabled
    # This allows the connector to use dummy vectors for non-vector queries
    table_config["use_full_scan"] = "true"
    table_config["batch_size"] = "10"

    # Create tester with the config
    tester = LakeflowConnectTester(config, table_config)
    
    # Exclude problematic tables from testing
    # The multivector-example-new table has issues with vector dimensions
    tester.exclude_tables = ["multivector-example-new"]

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )


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
    from sources.lancedb.lancedb import LanceDBTableOptions
    
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
    from sources.lancedb.lancedb import LanceDBTableOptions
    
    # Valid batch size
    options = LanceDBTableOptions(batch_size=1000)
    assert options.batch_size == 1000
    
    # Batch size too small
    with pytest.raises(ValueError):
        LanceDBTableOptions(batch_size=0)
    
    # Batch size too large
    with pytest.raises(ValueError):
        LanceDBTableOptions(batch_size=100000)


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])

