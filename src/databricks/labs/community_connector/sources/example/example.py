import random
from typing import Dict, List, Iterator

from pydantic import BaseModel, PositiveInt, ConfigDict
from pyspark.sql.types import StructType, StructField, LongType, StringType
from databricks.labs.community_connector.interface import LakeflowConnect


class ExampleTableOptions(BaseModel):
    model_config = ConfigDict(extra="allow")

    total_available_rows: PositiveInt
    num_rows_per_batch: PositiveInt


# This is an example implementation of the LakeflowConnect class.
class ExampleLakeflowConnect(LakeflowConnect):
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize source parameters. Options may include authentication or other configs.
        """
        self.options = options
        self.tables = ["my_table", "your_table"]
        self.offset_id = 0
        self.offset_key = 1000

    def list_tables(self) -> List[str]:
        """
        Returns a list of available tables.
        """
        return self.tables

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        if table_name == "my_table":
            schema = StructType(
                [
                    StructField("id", LongType(), False),
                    StructField("name", StringType(), True),
                ]
            )
        elif table_name == "your_table":
            schema = StructType(
                [
                    StructField("key", StringType(), False),
                    StructField("value", StringType(), True),
                ]
            )
        else:
            raise ValueError(f"Unknown table: {table_name}")

        return schema

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Fetch the metadata of a table.
        """
        if table_name == "my_table":
            metadata = {"primary_keys": ["id"], "ingestion_type": "append"}
        elif table_name == "your_table":
            metadata = {"primary_keys": ["key"], "ingestion_type": "append"}
        else:
            raise ValueError(f"Unknown table: {table_name}")

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read data from a table and return an iterator of records along with the next offset.
        Pagination stops when the returned offset equals start_offset (no more data).
        """
        options = ExampleTableOptions(**table_options)

        initial_offset = self.offset_id if table_name == "my_table" else self.offset_key
        current_offset = (
            int(start_offset) if start_offset else initial_offset
        )
        max_offset = initial_offset + options.total_available_rows

        if current_offset >= max_offset:
            return iter([]), {"offset": current_offset}

        batch_end = min(current_offset + options.num_rows_per_batch, max_offset)
        data_iterator = self._read_helper(table_name, current_offset, batch_end)

        return data_iterator, {"offset": batch_end}

    def _read_helper(
        self,
        table_name: str,
        start: int,
        end: int,
    ) -> Iterator[dict]:
        if table_name not in self.tables:
            raise ValueError(f"Unknown table: {table_name}")

        for offset in range(start, end):
            if table_name == "my_table":
                yield {
                    "id": offset,
                    "name": f"Name_{random.randint(1000, 9999)}",
                }
            else:
                yield {
                    "key": str(offset),
                    "value": f"Value_{random.randint(1000, 9999)}",
                }
