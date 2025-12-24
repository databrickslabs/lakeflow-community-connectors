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

    # Test 9: Prebuilt report loading
    print("\n" + "="*50)
    print("TEST: Prebuilt report loading")
    print("="*50)
    prebuilt_reports = connector._load_prebuilt_reports()
    assert isinstance(prebuilt_reports, dict), "Prebuilt reports should be a dictionary"
    assert len(prebuilt_reports) >= 1, "Should have at least one prebuilt report"
    print(f"✅ PASSED: Loaded {len(prebuilt_reports)} prebuilt reports")
    print(f"  Available reports: {', '.join(sorted(prebuilt_reports.keys()))}")

    # Test 10: Using prebuilt report
    print("\n" + "="*50)
    print("TEST: Using prebuilt report (traffic_by_country)")
    print("="*50)
    
    prebuilt_table_options = {
        "prebuilt_report": "traffic_by_country"
    }
    
    try:
        schema = connector.get_table_schema("test_prebuilt", prebuilt_table_options)
        assert schema is not None, "Schema should not be None"
        assert len(schema.fields) > 0, "Schema should have fields"
        print(f"✅ PASSED: Schema generated from prebuilt report")
        print(f"  Fields ({len(schema.fields)}): {', '.join([f.name for f in schema.fields])}")
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        raise

    # Test 11: Prebuilt report with overrides
    print("\n" + "="*50)
    print("TEST: Prebuilt report with overrides")
    print("="*50)
    
    override_options = {
        "prebuilt_report": "traffic_by_country",
        "start_date": "7daysAgo",
        "lookback_days": "1"
    }
    
    resolved_options = connector._resolve_table_options(override_options)
    assert "dimensions" in resolved_options, "Should have dimensions from prebuilt"
    assert "metrics" in resolved_options, "Should have metrics from prebuilt"
    assert resolved_options["start_date"] == "7daysAgo", "Should override start_date"
    assert resolved_options["lookback_days"] == "1", "Should override lookback_days"
    
    print(f"✅ PASSED: Prebuilt report with overrides works correctly")
    print(f"  Base dimensions: {resolved_options['dimensions']}")
    print(f"  Overridden start_date: {resolved_options['start_date']}")

    # Test 12: Invalid prebuilt report name
    print("\n" + "="*50)
    print("TEST: Invalid prebuilt report name")
    print("="*50)
    
    invalid_prebuilt_options = {
        "prebuilt_report": "nonexistent_report"
    }
    
    try:
        connector._resolve_table_options(invalid_prebuilt_options)
        print(f"❌ FAILED: Should have raised ValueError for invalid report name")
        assert False
    except ValueError as e:
        error_msg = str(e)
        assert "not found" in error_msg, "Error should mention report not found"
        assert "Available prebuilt reports:" in error_msg, "Error should list available reports"
        print(f"✅ PASSED: Invalid report name raises clear error")
        print(f"  Error message (truncated): {error_msg[:80]}...")

    # Test 13: Prebuilt reports caching
    print("\n" + "="*50)
    print("TEST: Prebuilt reports caching")
    print("="*50)
    
    reports1 = connector._load_prebuilt_reports()
    reports2 = connector._load_prebuilt_reports()
    assert reports1 is reports2, "Should return cached object"
    print(f"✅ PASSED: Prebuilt reports are cached correctly")

    print("\n" + "="*50)
    print("ALL TESTS PASSED (INCLUDING PREBUILT REPORTS)")
    print("="*50)

