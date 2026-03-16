"""Terna Lakeflow community connector.

Ingests Italian electricity system data (load, generation, transmission)
from the Terna Public API. Uses OAuth 2.0 Client Credentials; optional
x_api_key for the Physical Foreign Flow (transmission) endpoint.
"""

import logging
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.terna.modules.load import (
    TotalLoadReader,
)
from databricks.labs.community_connector.sources.terna.terna_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.terna.utils import TernaApiClient

class TernaLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Terna Public API."""

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Terna connector.

        Expected options:
            - client_id: OAuth 2.0 Client Credentials application key (required).
            - client_secret: OAuth 2.0 Client Credentials secret (required).
            - base_url: Base URL for the API (default https://api.terna.it).
            - x_api_key: Optional API key for Physical Foreign Flow when that
              endpoint uses x-api-key instead of Bearer token.
        """
        super().__init__(options)
        self._client = TernaApiClient(options)
        self._total_load_reader = TotalLoadReader(self._client)

    def list_tables(self) -> list[str]:
        """List names of all tables supported by this connector."""
        return SUPPORTED_TABLES

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """Return the Spark schema for the given table."""
        self._client.validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """Return metadata (primary_keys, cursor_field, ingestion_type) for the table."""
        self._client.validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read records by date-range chunks; cursor is the last 'date' value (yyyy-mm-dd hh:mm:ss)."""
        self._client.validate_table(table_name)
        reader = {
            "total_load": self._total_load_reader.read,
            #"actual_generation": self._read_actual_generation,
            #"renewable_generation": self._read_renewable_generation,
            #"physical_foreign_flow": self._read_physical_foreign_flow,
        }[table_name]
        return reader(start_offset, table_options)
'''
    def _read_actual_generation(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read actual_generation in one date chunk. Optional table_options: type (primary source)."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        extra = {}
        type_opt = table_options.get("type")
        if type_opt:
            extra["type"] = type_opt.strip()

        records = self._read_table_chunk(
            "actual_generation",
            "/generation/v2.0/actual-generation",
            from_date,
            to_date,
            table_options,
            "actual_generation",
            extra_params=extra if extra else None,
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset

    def _read_renewable_generation(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read renewable_generation in one date chunk. Optional table_options: type."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        extra = {}
        type_opt = table_options.get("type")
        if type_opt:
            extra["type"] = type_opt.strip()

        records = self._read_table_chunk(
            "renewable_generation",
            "/generation/v2.0/renewable-generation",
            from_date,
            to_date,
            table_options,
            "renewable_generation",
            extra_params=extra if extra else None,
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset

    def _read_physical_foreign_flow(
        self,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read physical_foreign_flow in one date chunk."""
        chunk_result, range_end_opt = self._next_chunk(start_offset, table_options)
        if chunk_result is None:
            return iter([]), start_offset or {}

        from_date, to_date = chunk_result
        records = self._read_table_chunk(
            "physical_foreign_flow",
            "/transmission/v2.0/physical-foreign-flow",
            from_date,
            to_date,
            table_options,
            "physical_foreign_flow"
        )
        if not records:
            end_offset = {"cursor": self._format_cursor(to_date)}
        else:
            max_date = self._max_date_in_records(records)
            end_offset = {"cursor": max_date} if max_date else dict(start_offset or {})
        if range_end_opt is not None:
            end_offset["range_end"] = range_end_opt
        elif start_offset and "range_end" in start_offset:
            end_offset["range_end"] = start_offset["range_end"]
        return iter(records), end_offset
'''