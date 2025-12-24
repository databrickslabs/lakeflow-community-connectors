import pytest
from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.google_analytics_aggregated.google_analytics_aggregated import LakeflowConnect


def test_google_analytics_aggregated_connector():
    """Test the Google Analytics Aggregated connector
    
    Note: GA4 connector uses dynamic table names, so we test with specific
    table names from the config rather than relying on list_tables().
    """
    # Inject the LakeflowConnect class into test_suite module's namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Initialize connector
    connector = LakeflowConnect(config)
    
    # Test 1: Initialization
    print("\n" + "="*50)
    print("TEST: Initialization")
    print("="*50)
    assert connector is not None, "Connector should initialize successfully"
    print("✅ PASSED: Connector initialized")

    # Test 2: list_tables (returns empty for dynamic tables)
    print("\n" + "="*50)
    print("TEST: list_tables")
    print("="*50)
    tables = connector.list_tables()
    assert isinstance(tables, list), "list_tables should return a list"
    print(f"✅ PASSED: list_tables returned {len(tables)} tables (empty is expected for dynamic tables)")

    # For each table in the config, test the connector methods
    for table_name, table_options in table_config.items():
        print("\n" + "="*50)
        print(f"TESTING TABLE: {table_name}")
        print("="*50)
        
        # Test 3: get_table_schema
        print(f"\nTEST: get_table_schema for '{table_name}'")
        print("-"*50)
        try:
            schema = connector.get_table_schema(table_name, table_options)
            assert schema is not None, "Schema should not be None"
            assert hasattr(schema, 'fields'), "Schema should have fields"
            print(f"✅ PASSED: Schema has {len(schema.fields)} fields")
            for field in schema.fields[:5]:  # Print first 5 fields
                print(f"  - {field.name}: {field.dataType}")
        except Exception as e:
            print(f"❌ FAILED: {str(e)}")
            raise

        # Test 4: read_table_metadata
        print(f"\nTEST: read_table_metadata for '{table_name}'")
        print("-"*50)
        try:
            metadata = connector.read_table_metadata(table_name, table_options)
            assert isinstance(metadata, dict), "Metadata should be a dict"
            assert "ingestion_type" in metadata, "Metadata should include ingestion_type"
            print(f"✅ PASSED: Metadata retrieved")
            print(f"  - ingestion_type: {metadata.get('ingestion_type')}")
            print(f"  - primary_keys: {metadata.get('primary_keys')}")
            print(f"  - cursor_field: {metadata.get('cursor_field')}")
        except Exception as e:
            print(f"❌ FAILED: {str(e)}")
            raise

        # Test 5: read_table
        print(f"\nTEST: read_table for '{table_name}'")
        print("-"*50)
        try:
            records, next_offset = connector.read_table(table_name, {}, table_options)
            assert records is not None, "Records should not be None"
            
            # Collect some records
            record_list = []
            for i, record in enumerate(records):
                record_list.append(record)
                if i >= 4:  # Get first 5 records
                    break
            
            print(f"✅ PASSED: Retrieved {len(record_list)} records")
            if record_list:
                print(f"  Sample record keys: {list(record_list[0].keys())}")
            print(f"  Next offset: {next_offset}")
        except Exception as e:
            print(f"❌ FAILED: {str(e)}")
            raise

    # Test 6: Validation - Invalid dimension (should fail)
    print("\n" + "="*50)
    print("TEST: Validation - Invalid dimension")
    print("="*50)
    try:
        invalid_options = {
            "dimensions": '["date", "contry"]',  # Typo: contry instead of country
            "metrics": '["activeUsers"]'
        }
        schema = connector.get_table_schema("test_invalid", invalid_options)
        print("❌ FAILED: Invalid dimension should have been rejected")
        raise AssertionError("Invalid dimension was not caught by validation")
    except ValueError as e:
        error_msg = str(e)
        assert "Unknown dimensions" in error_msg, "Error should mention unknown dimensions"
        assert "contry" in error_msg, "Error should list the invalid dimension"
        print("✅ PASSED: Invalid dimension caught")
        print(f"  Error: {error_msg[:100]}...")
    except Exception as e:
        print(f"❌ FAILED: Unexpected error type: {type(e).__name__}")
        raise

    # Test 7: Validation - Invalid metric (should fail)
    print("\n" + "="*50)
    print("TEST: Validation - Invalid metric")
    print("="*50)
    try:
        invalid_options = {
            "dimensions": '["date"]',
            "metrics": '["activUsers"]'  # Typo: activUsers instead of activeUsers
        }
        schema = connector.get_table_schema("test_invalid", invalid_options)
        print("❌ FAILED: Invalid metric should have been rejected")
        raise AssertionError("Invalid metric was not caught by validation")
    except ValueError as e:
        error_msg = str(e)
        assert "Unknown metrics" in error_msg, "Error should mention unknown metrics"
        assert "activUsers" in error_msg, "Error should list the invalid metric"
        print("✅ PASSED: Invalid metric caught")
        print(f"  Error: {error_msg[:100]}...")
    except Exception as e:
        print(f"❌ FAILED: Unexpected error type: {type(e).__name__}")
        raise

    # Test 8: Validation - Multiple invalid fields (should fail)
    print("\n" + "="*50)
    print("TEST: Validation - Multiple invalid fields")
    print("="*50)
    try:
        invalid_options = {
            "dimensions": '["date", "contry", "deivce"]',  # Multiple typos
            "metrics": '["activUsers", "sesions"]'  # Multiple typos
        }
        schema = connector.get_table_schema("test_invalid", invalid_options)
        print("❌ FAILED: Multiple invalid fields should have been rejected")
        raise AssertionError("Multiple invalid fields were not caught by validation")
    except ValueError as e:
        error_msg = str(e)
        assert "Unknown dimensions" in error_msg, "Error should mention unknown dimensions"
        assert "Unknown metrics" in error_msg, "Error should mention unknown metrics"
        assert "contry" in error_msg and "deivce" in error_msg, "Error should list all invalid dimensions"
        assert "activUsers" in error_msg and "sesions" in error_msg, "Error should list all invalid metrics"
        print("✅ PASSED: Multiple invalid fields caught")
        print(f"  Error message includes both dimensions and metrics")
    except Exception as e:
        print(f"❌ FAILED: Unexpected error type: {type(e).__name__}")
        raise

    print("\n" + "="*50)
    print("ALL TESTS PASSED")
    print("="*50)
