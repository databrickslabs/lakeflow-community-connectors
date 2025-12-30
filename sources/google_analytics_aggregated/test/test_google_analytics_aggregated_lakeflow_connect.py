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
    
    This test suite covers both single and multiple property scenarios.
    """
    # Inject the LakeflowConnect class into test_suite module's namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Initialize connector (property_id field is always included regardless of count)
    connector = LakeflowConnect(config)
    
    # Get property count for informational purposes
    property_ids = config.get("property_ids", [])
    property_count = len(property_ids)
    
    # Test 1: Initialization
    print("\n" + "="*50)
    print("TEST: Initialization")
    print("="*50)
    assert connector is not None, "Connector should initialize successfully"
    assert len(connector.property_ids) == len(property_ids), "Property IDs should match config"
    print(f"✅ PASSED: Connector initialized with {len(connector.property_ids)} properties")
    print(f"  Property IDs: {connector.property_ids}")

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
            
            field_names = [f.name for f in schema.fields]
            
            # property_id field is ALWAYS included for schema stability
            assert "property_id" in field_names, "Schema should always include 'property_id' field"
            assert field_names[0] == "property_id", "'property_id' should always be the first field"
            print(f"✅ PASSED: 'property_id' field present at position 0 (schema stability)")
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
            
            primary_keys = metadata.get('primary_keys', [])
            
            # property_id is ALWAYS the first primary key for schema stability
            assert "property_id" in primary_keys, "Primary keys should always include 'property_id'"
            assert primary_keys[0] == "property_id", "'property_id' should always be the first primary key"
            print(f"✅ PASSED: 'property_id' prepended to primary keys (schema stability)")
            print(f"✅ PASSED: Metadata retrieved")
            print(f"  - ingestion_type: {metadata.get('ingestion_type')}")
            print(f"  - primary_keys: {primary_keys}")
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
            
                # property_id field is ALWAYS included in records
                if record_list:
                    record_keys = list(record_list[0].keys())
                    assert "property_id" in record_keys, "Records should always include 'property_id' field"
                    # Verify property_id values are from the configured list
                    property_values = set(r.get("property_id") for r in record_list)
                    assert property_values.issubset(set(connector.property_ids)), \
                        f"Property ID values {property_values} should be from {connector.property_ids}"
                    print(f"✅ PASSED: Records contain 'property_id' field with values: {property_values}")
            
            print(f"✅ PASSED: Retrieved {len(record_list)} records")
            if record_list:
                print(f"  Sample record keys: {record_keys}")
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
        "prebuilt_report": "traffic_by_country",
        "primary_keys": ["date", "country"]
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
        "primary_keys": ["date", "country"],
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

    # Test 14: Using prebuilt report by table name (NEW APPROACH)
    print("\n" + "="*50)
    print("TEST: Using prebuilt report by table name")
    print("="*50)
    
    # When source_table = "traffic_by_country", it should work without table_options!
    empty_options = {}
    
    try:
        # Test list_tables returns prebuilt report names
        tables = connector.list_tables()
        assert "traffic_by_country" in tables, "list_tables should return prebuilt report names"
        print(f"✅ list_tables() returns prebuilt reports: {tables}")
        
        # Test get_table_schema works with just the table name
        schema = connector.get_table_schema("traffic_by_country", empty_options)
        assert schema is not None, "Schema should not be None"
        field_names = [f.name for f in schema.fields]
        assert "date" in field_names, "Should have date field"
        assert "country" in field_names, "Should have country field"
        
        # property_id field is ALWAYS included
        assert "property_id" in field_names, "Should always have 'property_id' field"
        assert field_names[0] == "property_id", "'property_id' should always be first"
        
        print(f"✅ get_table_schema() works with just table name")
        print(f"  Fields: {', '.join(field_names)}")
        
        # Test read_table_metadata works with just the table name
        metadata = connector.read_table_metadata("traffic_by_country", empty_options)
        assert metadata is not None, "Metadata should not be None"
        assert "primary_keys" in metadata, "Should have primary_keys"
        
        # property_id is always prepended
        expected_primary_keys = ["property_id", "date", "country"]
        assert metadata["primary_keys"] == expected_primary_keys, \
            f"Primary keys should always be: {expected_primary_keys}"
        
        print(f"✅ read_table_metadata() works with just table name")
        print(f"  Primary keys: {metadata['primary_keys']}")
        print(f"  Ingestion type: {metadata.get('ingestion_type')}")
        
        # Test read_table works with just the table name
        records, offset = connector.read_table("traffic_by_country", {}, empty_options)
        records_list = list(records)
        assert len(records_list) > 0, "Should return some records"
        
        # property_id field is ALWAYS included in records
        if records_list:
            assert "property_id" in records_list[0], "Records should always have 'property_id' field"
        
        print(f"✅ read_table() works with just table name")
        print(f"  Records returned: {len(records_list)}")
        
        print(f"\n✅ PASSED: Prebuilt reports work using just table name (no config needed!)")
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        raise

    # Test 15: Shadowing prebuilt report name with custom config
    print("\n" + "="*50)
    print("TEST: Shadowing prebuilt report name with custom config")
    print("="*50)
    
    # Use prebuilt name but provide custom dimensions (should override)
    shadow_options = {
        "dimensions": '["date", "city"]',  # Different from prebuilt!
        "metrics": '["sessions"]',
        "primary_keys": ["property_id", "date", "city"]  # Always include property_id
    }
    
    try:
        # Should use custom config, not prebuilt
        schema = connector.get_table_schema("traffic_by_country", shadow_options)
        field_names = [f.name for f in schema.fields]
        assert "city" in field_names, "Should use custom dimension (city)"
        assert "country" not in field_names, "Should NOT use prebuilt dimension (country)"
        print(f"✅ Custom dimensions override prebuilt report")
        print(f"  Fields: {', '.join(field_names)}")
        
        # Metadata should also use custom config
        metadata = connector.read_table_metadata("traffic_by_country", shadow_options)
        expected_shadow_keys = ["property_id", "date", "city"]
        assert metadata["primary_keys"] == expected_shadow_keys, \
            f"Should use custom primary_keys: {expected_shadow_keys}"
        print(f"✅ Custom primary_keys override prebuilt report")
        print(f"  Primary keys: {metadata['primary_keys']}")
        
        print(f"\n✅ PASSED: Can shadow prebuilt report names with explicit dimensions")
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        raise

    # Test 16: Single property mode (backward compatibility)
    print("\n" + "="*50)
    print("TEST: Single property mode")
    print("="*50)
    
    try:
        # Create a connector with single property
        single_property_config = config.copy()
        single_property_config["property_ids"] = [connector.property_ids[0]]  # Take first property only
        
        single_connector = LakeflowConnect(single_property_config)
        assert len(single_connector.property_ids) == 1, "Should have exactly 1 property"
        print(f"✅ Single property connector initialized: {single_connector.property_ids}")
        
        # Test schema ALWAYS includes 'property_id' field for schema stability
        schema = single_connector.get_table_schema("traffic_by_country", empty_options)
        field_names = [f.name for f in schema.fields]
        assert "property_id" in field_names, "Single property: Should ALWAYS have 'property_id' field for schema stability"
        assert field_names[0] == "property_id", "'property_id' should be first"
        print(f"✅ Single property: 'property_id' field present (schema stability)")
        
        # Test metadata ALWAYS includes 'property_id' in primary keys
        metadata = single_connector.read_table_metadata("traffic_by_country", empty_options)
        assert "property_id" in metadata["primary_keys"], "Single property: Should ALWAYS have 'property_id' in primary keys"
        assert metadata["primary_keys"] == ["property_id", "date", "country"], "Single property: property_id always prepended"
        print(f"✅ Single property: Primary keys = {metadata['primary_keys']}")
        
        # Test records ALWAYS include 'property_id' field
        records, _ = single_connector.read_table("traffic_by_country", {}, empty_options)
        records_list = list(records)
        if records_list:
            assert "property_id" in records_list[0], "Single property: Records should ALWAYS have 'property_id' field"
            print(f"✅ Single property: Records include 'property_id' field")
        
        print(f"\n✅ PASSED: Single property mode includes 'property_id' for schema stability")
    except Exception as e:
        print(f"❌ FAILED: {str(e)}")
        raise

    print("\n" + "="*50)
    print("ALL TESTS PASSED (INCLUDING MULTI-PROPERTY SUPPORT)")
    print("="*50)


