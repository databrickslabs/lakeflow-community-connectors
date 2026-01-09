"""
Utility functions for load-balanced deployment toolkit.

Adopted best practices from the upstream community-connector CLI tool:
1. Configuration merging with deep_merge
2. Placeholder resolution for paths
3. CSV validation with clear error messages
"""

from typing import Any, Optional
import csv


def deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge two dictionaries, with override taking precedence.

    Adopted from tools/community_connector/src/databricks/labs/community_connector/config.py

    Args:
        base: Base dictionary with default values.
        override: Dictionary with values that override base.

    Returns:
        Merged dictionary.

    Example:
        >>> base = {"pipeline": {"catalog": "main", "target": "bronze"}}
        >>> override = {"pipeline": {"target": "silver"}}
        >>> deep_merge(base, override)
        {'pipeline': {'catalog': 'main', 'target': 'silver'}}
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value

    return result


def replace_placeholder_in_value(value: Any, placeholder: str, replacement: str) -> Any:
    """
    Recursively replace a placeholder in a value (dict, list, or string).

    Adopted from tools/community_connector/src/databricks/labs/community_connector/cli.py

    Args:
        value: Value to process (dict, list, or string).
        placeholder: Placeholder string to replace (e.g., "{WORKSPACE_PATH}").
        replacement: Replacement string.

    Returns:
        Value with placeholder replaced.

    Example:
        >>> config = {"path": "/workspace/{USER}/data", "paths": ["{USER}/file1", "{USER}/file2"]}
        >>> replace_placeholder_in_value(config, "{USER}", "john")
        {'path': '/workspace/john/data', 'paths': ['john/file1', 'john/file2']}
    """
    if isinstance(value, dict):
        return {k: replace_placeholder_in_value(v, placeholder, replacement) for k, v in value.items()}
    elif isinstance(value, list):
        return [replace_placeholder_in_value(item, placeholder, replacement) for item in value]
    elif isinstance(value, str):
        return value.replace(placeholder, replacement)
    else:
        return value


class CSVValidationError(Exception):
    """Exception raised when CSV validation fails."""

    def __init__(self, message: str, row_number: Optional[int] = None, column: Optional[str] = None):
        self.message = message
        self.row_number = row_number
        self.column = column
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        parts = []
        if self.row_number is not None:
            parts.append(f"Row {self.row_number}")
        if self.column:
            parts.append(f"Column '{self.column}'")

        location = ", ".join(parts)
        if location:
            return f"CSV Validation Error ({location}): {self.message}"
        return f"CSV Validation Error: {self.message}"


def validate_csv_structure(csv_path: str, required_columns: list[str]) -> list[str]:
    """
    Validate CSV file structure and required columns.

    Inspired by tools/community_connector/src/databricks/labs/community_connector/pipeline_spec_validator.py

    Args:
        csv_path: Path to the CSV file to validate.
        required_columns: List of required column names.

    Returns:
        List of warning messages (empty if no warnings).

    Raises:
        CSVValidationError: If validation fails.
        FileNotFoundError: If CSV file not found.

    Example:
        >>> warnings = validate_csv_structure("tables.csv", ["pipeline_name", "table_name"])
        >>> if warnings:
        ...     for warning in warnings:
        ...         print(f"Warning: {warning}")
    """
    warnings = []

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Check if file has any columns
            if not reader.fieldnames:
                raise CSVValidationError("CSV file appears to be empty or has no header row")

            # Check for required columns
            missing_columns = set(required_columns) - set(reader.fieldnames)
            if missing_columns:
                raise CSVValidationError(
                    f"Missing required columns: {', '.join(sorted(missing_columns))}"
                )

            # Check for unknown columns (warnings, not errors)
            known_columns = {
                "pipeline_name", "table_name", "destination_table", "scd_type",
                "primary_keys", "table_configuration", "schedule", "weight"
            }
            unknown_columns = set(reader.fieldnames) - known_columns
            if unknown_columns:
                warnings.append(f"Unknown columns will be ignored: {', '.join(sorted(unknown_columns))}")

            # Validate each row
            row_count = 0
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
                row_count += 1

                # Check for empty required fields
                for col in required_columns:
                    value = row.get(col, "").strip()
                    if not value:
                        raise CSVValidationError(
                            f"Required field '{col}' is empty",
                            row_number=row_num,
                            column=col
                        )

                # Validate scd_type values
                if "scd_type" in row:
                    scd_type = row["scd_type"].strip()
                    valid_scd_types = {"SCD_TYPE_1", "SCD_TYPE_2", "APPEND_ONLY"}
                    if scd_type and scd_type not in valid_scd_types:
                        raise CSVValidationError(
                            f"Invalid scd_type '{scd_type}'. Must be one of: {', '.join(valid_scd_types)}",
                            row_number=row_num,
                            column="scd_type"
                        )

            if row_count == 0:
                raise CSVValidationError("CSV file has no data rows (only header)")

    except FileNotFoundError:
        raise
    except CSVValidationError:
        raise
    except Exception as e:
        raise CSVValidationError(f"Failed to parse CSV file: {e}")

    return warnings


def validate_and_report_csv(csv_path: str, required_columns: list[str]) -> Optional[str]:
    """
    Validate a CSV file and return an error message if invalid.

    This is a convenience function that catches exceptions and returns
    a formatted error message string.

    Args:
        csv_path: Path to the CSV file to validate.
        required_columns: List of required column names.

    Returns:
        Error message string if validation fails, None if valid.

    Example:
        >>> error = validate_and_report_csv("tables.csv", ["pipeline_name", "table_name"])
        >>> if error:
        ...     print(f"Validation failed: {error}")
        ...     sys.exit(1)
    """
    try:
        warnings = validate_csv_structure(csv_path, required_columns)
        # Log warnings but don't fail
        return None
    except CSVValidationError as e:
        return str(e)
    except FileNotFoundError:
        return f"CSV file not found: {csv_path}"
    except Exception as e:
        return f"Unexpected error validating CSV: {e}"


def format_error_with_context(error: Exception, context: str) -> str:
    """
    Format an error message with additional context.

    Inspired by upstream CLI error handling patterns.

    Args:
        error: The exception that was raised.
        context: Additional context about what was being done when the error occurred.

    Returns:
        Formatted error message.

    Example:
        >>> try:
        ...     # some operation
        ... except Exception as e:
        ...     print(format_error_with_context(e, "creating pipeline"))
    """
    error_msg = str(error)

    # Extract error code if available (Databricks SDK pattern)
    if hasattr(error, 'error_code'):
        error_msg = f"[{error.error_code}] {error_msg}"
    elif hasattr(error, 'message'):
        error_msg = error.message

    return f"Error {context}: {error_msg}"


# Example usage and tests
if __name__ == "__main__":
    # Test deep_merge
    print("Testing deep_merge:")
    base = {"pipeline": {"catalog": "main", "target": "bronze", "config": {"key1": "value1"}}}
    override = {"pipeline": {"target": "silver", "config": {"key2": "value2"}}}
    result = deep_merge(base, override)
    print(f"  Result: {result}")
    assert result == {"pipeline": {"catalog": "main", "target": "silver", "config": {"key1": "value1", "key2": "value2"}}}
    print("  ✓ Passed")

    # Test replace_placeholder_in_value
    print("\nTesting replace_placeholder_in_value:")
    config = {
        "path": "/workspace/{USER}/data",
        "paths": ["{USER}/file1", "{USER}/file2"],
        "nested": {"user_dir": "{USER}"}
    }
    result = replace_placeholder_in_value(config, "{USER}", "john")
    print(f"  Result: {result}")
    assert result["path"] == "/workspace/john/data"
    assert result["paths"] == ["john/file1", "john/file2"]
    assert result["nested"]["user_dir"] == "john"
    print("  ✓ Passed")

    # Test CSV validation error formatting
    print("\nTesting CSVValidationError:")
    try:
        raise CSVValidationError("Test error", row_number=5, column="table_name")
    except CSVValidationError as e:
        print(f"  Error message: {e}")
        assert "Row 5" in str(e)
        assert "table_name" in str(e)
        print("  ✓ Passed")

    print("\nAll tests passed!")
