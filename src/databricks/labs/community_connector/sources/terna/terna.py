"""Terna Lakeflow community connector.

Ingests Italian electricity system data (load, generation, transmission)
from the Terna Public API. Uses OAuth 2.0 Client Credentials; optional
x_api_key for the Physical Foreign Flow (transmission) endpoint.
"""

import logging
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.terna.modules.fees import (
    DailyPricesReader,
)
from databricks.labs.community_connector.sources.terna.modules.load import (
    MarketLoadReader,
    TotalLoadReader,
    PeakValleyLoadReader,
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
        self._market_load_reader = MarketLoadReader(self._client)
        self._daily_prices_reader = DailyPricesReader(self._client)
        self._peak_valley_load_reader = PeakValleyLoadReader(self._client)

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
            "market_load": self._market_load_reader.read,
            "daily_prices": self._daily_prices_reader.read,
            "peak_valley_load": self._peak_valley_load_reader.read,
        }[table_name]
        return reader(start_offset, table_options)
