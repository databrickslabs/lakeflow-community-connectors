"""
Base class for Zoho CRM table handlers.

Defines the interface that all table handlers must implement.
"""

from abc import ABC, abstractmethod
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.sources.zoho_crm.zoho_client import ZohoAPIClient


class TableHandler(ABC):
    """
    Abstract base class for handling different types of Zoho CRM tables.

    Each handler is responsible for:
    - Returning the table schema
    - Returning table metadata (primary keys, ingestion type, etc.)
    - Reading records from the table
    """

    def __init__(self, client: ZohoAPIClient) -> None:
        """
        Initialize the handler with an API client.

        Args:
            client: ZohoAPIClient instance for making API requests
        """
        self.client = client

    @abstractmethod
    def get_schema(self, table_name: str, config: dict) -> StructType:
        """
        Get the Spark schema for a table.

        Args:
            table_name: Name of the table
            config: Table configuration dictionary

        Returns:
            Spark StructType representing the table schema
        """

    @abstractmethod
    def get_metadata(self, table_name: str, config: dict) -> dict:
        """
        Get metadata for a table.

        Args:
            table_name: Name of the table
            config: Table configuration dictionary

        Returns:
            Dictionary with keys:
                - primary_keys: List of primary key column names
                - cursor_field: (optional) Field name for incremental loading
                - ingestion_type: "snapshot", "cdc", "cdc_with_deletes", or "append"
        """

    @abstractmethod
    def read(
        self,
        table_name: str,
        config: dict,
        start_offset: dict,
    ) -> tuple[Iterator[dict], dict]:
        """
        Read records from a table.

        Args:
            table_name: Name of the table
            config: Table configuration dictionary
            start_offset: Offset to start reading from

        Returns:
            Tuple of (records_iterator, next_offset)
        """
