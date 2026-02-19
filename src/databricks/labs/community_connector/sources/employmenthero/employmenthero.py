import random
from typing import Dict, List, Iterator, Optional, Tuple

from pydantic import BaseModel, PositiveInt, ConfigDict
from pyspark.sql.types import StructType, StructField, LongType, StringType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.employmenthero.employmenthero_client import EmploymentHeroAPIClient
from databricks.labs.community_connector.sources.employmenthero.employmenthero_schemas import (
    TABLE_SCHEMAS,
    TABLE_METADATA,
    SUPPORTED_TABLES,
)


class EmploymentHeroLakeflowConnect(LakeflowConnect):
    def __init__(self, options: Dict[str, str], client: Optional[EmploymentHeroAPIClient] = None) -> None:
        """
        Initialize source parameters. Options may include authentication or other configs.
        """
        self.options = options
        self.client = client or EmploymentHeroAPIClient(
            client_id=self.options.get("client_id"),
            client_secret=self.options.get("client_secret"),
            redirect_uri=self.options.get("redirect_uri"),
            authorization_code=self.options.get("authorization_code"),
        )

    def list_tables(self) -> List[str]:
        """
        Returns a list of available tables.
        """
        return SUPPORTED_TABLES.copy()

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        The schema is static and derived from the Employment Hero REST API documentation
        """
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Fetch the metadata of a table.
        """
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def _read_snapshot_table(
        self, table_name: str, table_options: Dict[str, str], endpoint_suffix: Optional[str] = None
    ) -> Tuple[Iterator[dict], dict]:
        """
        Reads a full snapshot of the specified table.
        """
        organisation_id = table_options.get("organisation_id")
        if not organisation_id:
            raise ValueError("table_options must contain 'organisation_id'")
        params = {}
        start_date = table_options.get("start_date")
        if start_date:
            params["start_date"] = start_date
        endpoint = f"/api/v1/organisations/{organisation_id}/{endpoint_suffix or table_name}"
        records = self.client.paginate(
            endpoint=endpoint,
            params=params,
            data_key="data",
            per_page=200,
        )
        return records, {}

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read data from a table and return an iterator of records along with the next offset.
        """
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(f"Unsupported table: {table_name!r}")

        table_medata = self.read_table_metadata(table_name, table_options)
        ingestion_type = table_medata.get("ingestion_type")
        endpoint_suffix = table_medata.get("endpoint_suffix")

        if ingestion_type == "snapshot":
            return self._read_snapshot_table(table_name, table_options, endpoint_suffix)

        raise NotImplementedError(
            f"Ingestion type '{ingestion_type}' for table '{table_name}' is not yet implemented."
        )
