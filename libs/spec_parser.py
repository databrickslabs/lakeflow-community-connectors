"""Parser for LakeFlow connector pipeline specifications."""

# pylint: disable=too-few-public-methods
from typing import List, Dict, Any, Optional
import json

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictStr,
    ValidationError,
    field_validator,
)


# Special table configuration options shared by all sources,
# These options will not get injected into the source.
SCD_TYPE = "scd_type"
PRIMARY_KEYS = "primary_keys"
SEQUENCE_BY = "sequence_by"
DELETION_SYNC = "deletion_sync"

# Valid SCD type values
SCD_TYPE_1 = "SCD_TYPE_1"
SCD_TYPE_2 = "SCD_TYPE_2"
APPEND_ONLY = "APPEND_ONLY"
VALID_SCD_TYPES = {SCD_TYPE_1, SCD_TYPE_2, APPEND_ONLY}


class TableSpec(BaseModel):
    """
    Specification for a single table.

    Currently only `table` objects are supported and they must include a
    `source_table` field.

    Optional destination fields:
    - destination_catalog: The catalog for the destination table
    - destination_schema: The schema for the destination table
    - destination_table: The name of the destination table

    The optional `table_configuration` is normalised so that:
    - it is always a mapping of string keys to string values
    - any nested structures (dicts / lists) are JSON-serialised

    """

    model_config = ConfigDict(extra="forbid")

    source_table: StrictStr
    destination_catalog: Optional[StrictStr] = None
    destination_schema: Optional[StrictStr] = None
    destination_table: Optional[StrictStr] = None
    table_configuration: Optional[Dict[str, StrictStr]] = None

    @field_validator("table_configuration", mode="before")
    @classmethod
    def normalize_table_configuration(cls, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
        """
        Ensure table_configuration is a mapping of string -> string.

        - Keys are coerced to strings.
        - Values that are dicts/lists are JSON-encoded.
        - Other values are stringified with str().
        """
        if v is None:
            return None

        if not isinstance(v, dict):
            raise ValueError("'table_configuration' must be a dictionary if provided")

        normalized: Dict[str, str] = {}
        for key, value in v.items():
            str_key = str(key)

            if isinstance(value, (dict, list)):
                normalized[str_key] = json.dumps(value)
            else:
                normalized[str_key] = str(value)

        return normalized


class ObjectSpec(BaseModel):
    """
    Wrapper for an object in the pipeline spec.

    Currently only the `table` key is supported.
    """

    model_config = ConfigDict(extra="forbid")

    table: TableSpec


class PipelineSpec(BaseModel):
    """
    Top-level pipeline specification.

    - `connection_name` is required and must be a non-empty string.
    - `objects` is required, must be a non-empty list, and each object
      must be a `table` with a `source_table` field.
    """

    model_config = ConfigDict(extra="forbid")

    connection_name: StrictStr
    objects: List[ObjectSpec]

    @field_validator("connection_name")
    @classmethod
    def connection_name_not_empty(cls, v: StrictStr) -> StrictStr:
        """Validate that connection_name is not empty or whitespace-only."""
        if not v.strip():
            raise ValueError("'connection_name' must be a non-empty string")
        return v

    @field_validator("objects")
    @classmethod
    def objects_must_not_be_empty(cls, v: List[ObjectSpec]) -> List[ObjectSpec]:
        """Validate that objects list is not empty."""
        if not v:
            raise ValueError("'objects' must be a non-empty list")
        return v


class SpecParser:
    """
    Parser for ingestion pipeline specifications using Pydantic models.

    Parses a JSON-like specification containing connection information and
    table configurations.

    Example:
        spec = {
            "connection_name": "my_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "my_table",
                    }
                }
            ]
        }
    """

    def __init__(self, spec: Dict[str, Any]):
        """
        Initialize the spec parser with a pipeline specification.

        Args:
            spec: A dictionary containing the pipeline specification with the
                  following structure:
                  - connection_name: The name of the connection (required string)
                  - objects: A non-empty list of table objects to be ingested
        """
        if not isinstance(spec, dict):
            raise ValueError("Spec must be a dictionary")

        try:
            self._model = PipelineSpec(**spec)
        except ValidationError as e:
            # Keep a simple ValueError surface compatible with previous behaviour
            raise ValueError(f"Invalid pipeline spec: {e}") from e

    def connection_name(self) -> str:
        """
        Return the connection name from the specification.

        Returns:
            The connection name as a string.
        """
        return self._model.connection_name

    def get_table_list(self) -> List[str]:
        """
        Return the list of source table names from the specification.

        Returns:
            A list of table names (strings).
        """
        return [obj.table.source_table for obj in self._model.objects]

    def get_table_configuration(self, table_name: str) -> Dict[str, Any]:
        """
        Return the configuration for a specific table.

        Excludes special keys: scd_type, primary_keys, sequence_by.
        Use dedicated methods to retrieve these values.

        Returns:
            A dictionary containing the table configuration without special keys.
        """
        special_keys = {SCD_TYPE, PRIMARY_KEYS, SEQUENCE_BY, DELETION_SYNC}
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                return {k: v for k, v in config.items() if k not in special_keys}
        return {}

    def get_scd_type(self, table_name: str) -> Optional[str]:
        """
        Return the SCD type for a specific table.

        Args:
            table_name: The name of the table.

        Returns:
            The SCD type as a string (normalized to uppercase), or None if not specified.
            Valid values: SCD_TYPE_1, SCD_TYPE_2, APPEND_ONLY (case insensitive).

        Raises:
            ValueError: If the SCD type is not one of the valid values.
        """
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                scd_type_value = config.get(SCD_TYPE)
                if scd_type_value is None:
                    return None

                # Normalize to uppercase for case-insensitive comparison
                normalized = scd_type_value.upper()

                if normalized not in VALID_SCD_TYPES:
                    raise ValueError(
                        f"Invalid SCD type '{scd_type_value}' for table '{table_name}'. "
                        f"Must be one of: {', '.join(sorted(VALID_SCD_TYPES))}"
                    )

                return normalized
        return None

    def get_primary_keys(self, table_name: str) -> Optional[List[str]]:
        """
        Return the primary keys for a specific table.

        Args:
            table_name: The name of the table.

        Returns:
            A list of primary key column names, or None if not specified.
            If the value was stored as a JSON string, it will be parsed.
        """
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                primary_keys_value = config.get(PRIMARY_KEYS)
                if primary_keys_value is None:
                    return None
                # If it's a JSON string (list was serialized), parse it
                if isinstance(primary_keys_value, str) and primary_keys_value.startswith("["):
                    return json.loads(primary_keys_value)
                # If it's a single string, return as a single-item list
                return (
                    [primary_keys_value]
                    if isinstance(primary_keys_value, str)
                    else primary_keys_value
                )
        return None

    def get_sequence_by(self, table_name: str) -> Optional[str]:
        """
        Return the sequence_by column for a specific table.

        Args:
            table_name: The name of the table.

        Returns:
            The sequence_by column name as a string, or None if not specified.
        """
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                return config.get(SEQUENCE_BY)
        return None

    def get_deletion_sync(self, table_name: str) -> bool:
        """
        Return whether deletion sync is enabled for a specific table.

        Args:
            table_name: The name of the table.

        Returns:
            True if deletion_sync is set to "true" (case-insensitive), False otherwise.
        """
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                config = obj.table.table_configuration or {}
                value = config.get(DELETION_SYNC)
                if value is None:
                    return False
                return value.lower() == "true"
        return False

    def get_full_destination_table_name(self, table_name: str) -> str:
        """
        Return the full destination table name for a specific table.

        Args:
            table_name: The name of the source table.

        Returns:
            The full destination table name in the format
            'destination_catalog.destination_schema.destination_table',
            or 'destination_catalog.destination_schema.table_name' if destination_table is
            not specified, or table_name if destination_catalog or destination_schema is
            not specified.

        Raises:
            ValueError: If the table_name is not found in the object list.
        """
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                catalog = obj.table.destination_catalog
                schema = obj.table.destination_schema
                table = obj.table.destination_table or table_name

                if catalog is None or schema is None:
                    return table
                return f"`{catalog}`.`{schema}`.`{table}`"

        raise ValueError(f"Table '{table_name}' not found in the pipeline spec")
