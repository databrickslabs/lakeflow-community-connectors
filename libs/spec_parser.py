from typing import List, Dict, Any, Optional
import json

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictStr,
    ValidationError,
    field_validator,
)


class TableSpec(BaseModel):
    """
    Specification for a single table.

    Currently only `table` objects are supported and they must include a
    `source_table` field.

    The optional `table_configuration` is normalised so that:
    - it is always a mapping of string keys to string values
    - any nested structures (dicts / lists) are JSON-serialised
    """

    model_config = ConfigDict(extra="forbid")

    source_table: StrictStr
    table_configuration: Optional[Dict[str, StrictStr]] = None

    @field_validator("table_configuration", mode="before")
    @classmethod
    def normalize_table_configuration(
        cls, v: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, str]]:
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
        if not v.strip():
            raise ValueError("'connection_name' must be a non-empty string")
        return v

    @field_validator("objects")
    @classmethod
    def objects_must_not_be_empty(cls, v: List[ObjectSpec]) -> List[ObjectSpec]:
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

        Returns:
            A dictionary containing the table configuration.
        """
        for obj in self._model.objects:
            if obj.table.source_table == table_name:
                return obj.table.table_configuration or {}
        return {}
