"""
Pipeline spec validator for Community Connector CLI.

Validates the pipeline_spec structure according to the spec reference:
- connection_name (required): Unity Catalog connection name
- objects (required): List of tables to ingest, each containing:
  - table: Table configuration object:
    - source_table (required): Table name in the source system
    - destination_catalog (optional): Target catalog
    - destination_schema (optional): Target schema
    - destination_table (optional): Target table name
    - table_configuration (optional): Additional options:
      - scd_type: SCD_TYPE_1, SCD_TYPE_2, or APPEND_ONLY
      - primary_keys: List of columns
      - (other source-specific options)
"""

from typing import Optional

# Valid SCD types
VALID_SCD_TYPES = {"SCD_TYPE_1", "SCD_TYPE_2", "APPEND_ONLY"}


class PipelineSpecValidationError(Exception):
    """Exception raised when pipeline spec validation fails."""

    def __init__(self, message: str, path: str = ""):
        self.message = message
        self.path = path
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        if self.path:
            return f"Invalid pipeline spec at '{self.path}': {self.message}"
        return f"Invalid pipeline spec: {self.message}"


def validate_pipeline_spec(spec: dict) -> list[str]:
    """
    Validate a pipeline spec dictionary.

    Args:
        spec: The pipeline spec dictionary to validate.

    Returns:
        List of warning messages (empty if no warnings).

    Raises:
        PipelineSpecValidationError: If validation fails.
    """
    warnings = []

    if not isinstance(spec, dict):
        raise PipelineSpecValidationError("Pipeline spec must be a dictionary")

    # Validate connection_name (always required)
    if "connection_name" not in spec:
        raise PipelineSpecValidationError("'connection_name' is required")
    if not isinstance(spec["connection_name"], str) or not spec["connection_name"].strip():
        raise PipelineSpecValidationError("'connection_name' must be a non-empty string")

    # Validate objects (required)
    if "objects" not in spec:
        raise PipelineSpecValidationError("'objects' is required")

    objects = spec["objects"]
    if not isinstance(objects, list):
        raise PipelineSpecValidationError("'objects' must be a list", "objects")

    if len(objects) == 0:
        raise PipelineSpecValidationError("'objects' must contain at least one table", "objects")

    # Validate each object
    for i, obj in enumerate(objects):
        obj_path = f"objects[{i}]"
        obj_warnings = _validate_object(obj, obj_path)
        warnings.extend(obj_warnings)

    # Warn about unknown top-level keys
    known_keys = {"connection_name", "objects"}
    unknown_keys = set(spec.keys()) - known_keys
    if unknown_keys:
        warnings.append(f"Unknown top-level keys will be ignored: {unknown_keys}")

    return warnings


def _validate_object(obj: dict, path: str) -> list[str]:
    """
    Validate a single object in the objects list.

    Args:
        obj: The object dictionary to validate.
        path: Path for error messages.

    Returns:
        List of warning messages.

    Raises:
        PipelineSpecValidationError: If validation fails.
    """
    warnings = []

    if not isinstance(obj, dict):
        raise PipelineSpecValidationError("Each object must be a dictionary", path)

    # Must have 'table' key
    if "table" not in obj:
        raise PipelineSpecValidationError("'table' is required", path)

    table = obj["table"]
    table_path = f"{path}.table"

    if not isinstance(table, dict):
        raise PipelineSpecValidationError("'table' must be a dictionary", table_path)

    # source_table is required
    if "source_table" not in table:
        raise PipelineSpecValidationError("'source_table' is required", table_path)

    if not isinstance(table["source_table"], str) or not table["source_table"].strip():
        raise PipelineSpecValidationError(
            "'source_table' must be a non-empty string", f"{table_path}.source_table"
        )

    # Validate optional string fields
    optional_string_fields = ["destination_catalog", "destination_schema", "destination_table"]
    for field in optional_string_fields:
        if field in table:
            if not isinstance(table[field], str):
                raise PipelineSpecValidationError(
                    f"'{field}' must be a string", f"{table_path}.{field}"
                )

    # Validate table_configuration if present
    if "table_configuration" in table:
        config = table["table_configuration"]
        config_path = f"{table_path}.table_configuration"

        if not isinstance(config, dict):
            raise PipelineSpecValidationError(
                "'table_configuration' must be a dictionary", config_path
            )

        config_warnings = _validate_table_configuration(config, config_path)
        warnings.extend(config_warnings)

    # Warn about unknown keys in table
    known_table_keys = {
        "source_table",
        "destination_catalog",
        "destination_schema",
        "destination_table",
        "table_configuration",
    }
    unknown_table_keys = set(table.keys()) - known_table_keys
    if unknown_table_keys:
        warnings.append(f"Unknown keys in '{table_path}' will be ignored: {unknown_table_keys}")

    # Warn about unknown keys in object
    known_obj_keys = {"table"}
    unknown_obj_keys = set(obj.keys()) - known_obj_keys
    if unknown_obj_keys:
        warnings.append(f"Unknown keys in '{path}' will be ignored: {unknown_obj_keys}")

    return warnings


def _validate_table_configuration(config: dict, path: str) -> list[str]:
    """
    Validate table_configuration dictionary.

    Args:
        config: The table_configuration dictionary.
        path: Path for error messages.

    Returns:
        List of warning messages.

    Raises:
        PipelineSpecValidationError: If validation fails.
    """
    warnings = []

    # Validate scd_type if present
    if "scd_type" in config:
        scd_type = config["scd_type"]
        if not isinstance(scd_type, str):
            raise PipelineSpecValidationError("'scd_type' must be a string", f"{path}.scd_type")
        if scd_type not in VALID_SCD_TYPES:
            raise PipelineSpecValidationError(
                f"'scd_type' must be one of {VALID_SCD_TYPES}, got '{scd_type}'",
                f"{path}.scd_type",
            )

    # Validate primary_keys if present
    if "primary_keys" in config:
        pk = config["primary_keys"]
        if not isinstance(pk, list):
            raise PipelineSpecValidationError(
                "'primary_keys' must be a list", f"{path}.primary_keys"
            )
        for i, key in enumerate(pk):
            if not isinstance(key, str):
                raise PipelineSpecValidationError(
                    f"'primary_keys[{i}]' must be a string", f"{path}.primary_keys[{i}]"
                )

    return warnings


def validate_and_report(spec: dict) -> Optional[str]:
    """
    Validate a pipeline spec and return an error message if invalid.

    This is a convenience function that catches exceptions and returns
    a formatted error message string.

    Args:
        spec: The pipeline spec dictionary to validate.

    Returns:
        Error message string if validation fails, None if valid.
    """
    try:
        warnings = validate_pipeline_spec(spec)
        # Log warnings but don't fail
        return None
    except PipelineSpecValidationError as e:
        return str(e)
