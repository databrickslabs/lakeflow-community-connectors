"""
Unit tests for pipeline_spec_validator.py
"""

import pytest
from databricks.labs.community_connector.pipeline_spec_validator import (
    validate_pipeline_spec,
    validate_and_report,
    PipelineSpecValidationError,
    VALID_SCD_TYPES,
)


class TestValidPipelineSpec:
    """Tests for valid pipeline specs."""

    def test_minimal_valid_spec(self):
        """Test minimal valid spec with required fields only."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {"table": {"source_table": "users"}}
            ],
        }
        warnings = validate_pipeline_spec(spec)
        assert not warnings

    def test_full_valid_spec(self):
        """Test fully populated valid spec."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "destination_catalog": "main",
                        "destination_schema": "raw",
                        "destination_table": "users_table",
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_2",
                            "primary_keys": ["id", "email"],
                        },
                    }
                }
            ],
        }
        warnings = validate_pipeline_spec(spec)
        assert not warnings

    def test_multiple_objects(self):
        """Test spec with multiple objects."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {"table": {"source_table": "users"}},
                {"table": {"source_table": "orders"}},
                {"table": {"source_table": "products"}},
            ],
        }
        warnings = validate_pipeline_spec(spec)
        assert not warnings

    def test_all_valid_scd_types(self):
        """Test all valid SCD types."""
        for scd_type in VALID_SCD_TYPES:
            spec = {
                "connection_name": "my_connection",
                "objects": [
                    {
                        "table": {
                            "source_table": "users",
                            "table_configuration": {"scd_type": scd_type},
                        }
                    }
                ],
            }
            warnings = validate_pipeline_spec(spec)
            assert not warnings


class TestMissingRequiredFields:
    """Tests for missing required fields."""

    def test_missing_connection_name(self):
        """Test error when connection_name is missing."""
        spec = {
            "objects": [{"table": {"source_table": "users"}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'connection_name' is required" in str(exc_info.value)

    def test_empty_connection_name(self):
        """Test error when connection_name is empty."""
        spec = {
            "connection_name": "",
            "objects": [{"table": {"source_table": "users"}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'connection_name' must be a non-empty string" in str(exc_info.value)

    def test_whitespace_connection_name(self):
        """Test error when connection_name is only whitespace."""
        spec = {
            "connection_name": "   ",
            "objects": [{"table": {"source_table": "users"}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'connection_name' must be a non-empty string" in str(exc_info.value)

    def test_missing_objects(self):
        """Test error when objects is missing."""
        spec = {
            "connection_name": "my_connection",
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'objects' is required" in str(exc_info.value)

    def test_empty_objects(self):
        """Test error when objects is empty."""
        spec = {
            "connection_name": "my_connection",
            "objects": [],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'objects' must contain at least one table" in str(exc_info.value)

    def test_missing_table_key(self):
        """Test error when table key is missing in object."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"not_table": {"source_table": "users"}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'table' is required" in str(exc_info.value)

    def test_missing_source_table(self):
        """Test error when source_table is missing."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"table": {"destination_table": "users"}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'source_table' is required" in str(exc_info.value)

    def test_empty_source_table(self):
        """Test error when source_table is empty."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"table": {"source_table": ""}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'source_table' must be a non-empty string" in str(exc_info.value)


class TestInvalidTypes:
    """Tests for invalid field types."""

    def test_spec_not_dict(self):
        """Test error when spec is not a dictionary."""
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec("not a dict")
        assert "must be a dictionary" in str(exc_info.value)

    def test_spec_is_list(self):
        """Test error when spec is a list."""
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec([{"connection_name": "test"}])
        assert "must be a dictionary" in str(exc_info.value)

    def test_objects_not_list(self):
        """Test error when objects is not a list."""
        spec = {
            "connection_name": "my_connection",
            "objects": "not a list",
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'objects' must be a list" in str(exc_info.value)

    def test_object_not_dict(self):
        """Test error when object in list is not a dict."""
        spec = {
            "connection_name": "my_connection",
            "objects": ["not a dict"],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "Each object must be a dictionary" in str(exc_info.value)

    def test_table_not_dict(self):
        """Test error when table is not a dict."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"table": "not a dict"}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'table' must be a dictionary" in str(exc_info.value)

    def test_connection_name_not_string(self):
        """Test error when connection_name is not a string."""
        spec = {
            "connection_name": 123,
            "objects": [{"table": {"source_table": "users"}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'connection_name' must be a non-empty string" in str(exc_info.value)

    def test_destination_catalog_not_string(self):
        """Test error when destination_catalog is not a string."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"table": {"source_table": "users", "destination_catalog": 123}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'destination_catalog' must be a string" in str(exc_info.value)

    def test_table_configuration_not_dict(self):
        """Test error when table_configuration is not a dict."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"table": {"source_table": "users", "table_configuration": "invalid"}}],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'table_configuration' must be a dictionary" in str(exc_info.value)


class TestInvalidScdType:
    """Tests for invalid scd_type values."""

    def test_invalid_scd_type(self):
        """Test error when scd_type is invalid."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {"scd_type": "INVALID_TYPE"},
                    }
                }
            ],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'scd_type' must be one of" in str(exc_info.value)
        assert "INVALID_TYPE" in str(exc_info.value)

    def test_scd_type_not_string(self):
        """Test error when scd_type is not a string."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {"scd_type": 1},
                    }
                }
            ],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'scd_type' must be a string" in str(exc_info.value)


class TestInvalidPrimaryKeys:
    """Tests for invalid primary_keys values."""

    def test_primary_keys_not_list(self):
        """Test error when primary_keys is not a list."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {"primary_keys": "id"},
                    }
                }
            ],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'primary_keys' must be a list" in str(exc_info.value)

    def test_primary_keys_element_not_string(self):
        """Test error when primary_keys element is not a string."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {"primary_keys": ["id", 123]},
                    }
                }
            ],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "'primary_keys[1]' must be a string" in str(exc_info.value)


class TestWarnings:
    """Tests for warning messages."""

    def test_unknown_top_level_keys(self):
        """Test warning for unknown top-level keys."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"table": {"source_table": "users"}}],
            "unknown_key": "value",
        }
        warnings = validate_pipeline_spec(spec)
        assert len(warnings) == 1
        assert "Unknown top-level keys" in warnings[0]
        assert "unknown_key" in warnings[0]

    def test_unknown_table_keys(self):
        """Test warning for unknown keys in table."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "unknown_field": "value",
                    }
                }
            ],
        }
        warnings = validate_pipeline_spec(spec)
        assert len(warnings) == 1
        assert "unknown_field" in warnings[0]

    def test_unknown_object_keys(self):
        """Test warning for unknown keys in object."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {"source_table": "users"},
                    "extra_key": "value",
                }
            ],
        }
        warnings = validate_pipeline_spec(spec)
        assert len(warnings) == 1
        assert "extra_key" in warnings[0]


class TestValidateAndReport:
    """Tests for validate_and_report convenience function."""

    def test_valid_spec_returns_none(self):
        """Test that valid spec returns None."""
        spec = {
            "connection_name": "my_connection",
            "objects": [{"table": {"source_table": "users"}}],
        }
        result = validate_and_report(spec)
        assert result is None

    def test_invalid_spec_returns_error_message(self):
        """Test that invalid spec returns error message."""
        spec = {"objects": []}  # missing connection_name
        result = validate_and_report(spec)
        assert result is not None
        assert "'connection_name' is required" in result


class TestErrorPaths:
    """Tests for error path reporting."""

    def test_error_path_for_nested_object(self):
        """Test that error paths are reported correctly."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {"table": {"source_table": "users"}},
                {"table": {"source_table": ""}},  # invalid at index 1
            ],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "objects[1]" in str(exc_info.value)

    def test_error_path_for_table_configuration(self):
        """Test error path for table_configuration issues."""
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {"scd_type": "INVALID"},
                    }
                }
            ],
        }
        with pytest.raises(PipelineSpecValidationError) as exc_info:
            validate_pipeline_spec(spec)
        assert "table_configuration" in str(exc_info.value)
        assert "scd_type" in str(exc_info.value)
