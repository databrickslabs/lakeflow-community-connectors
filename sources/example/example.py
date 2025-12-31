import random
from typing import Dict, List, Iterator

from pydantic import BaseModel, PositiveInt, ConfigDict
from pyspark.sql.types import StructType, StructField, LongType, StringType


class ExampleTableOptions(BaseModel):
    model_config = ConfigDict(extra="allow")

    num_rows: PositiveInt


# This is an example implementation of the LakeflowConnect class.
# Test a change 123
class LakeflowConnect:
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

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
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

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict[str, str]:
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
        """
        options = ExampleTableOptions(**table_options)
        num_rows = options.num_rows

        # Call the helper function to get the iterator
        data_iterator = self._read_helper(table_name, start_offset, num_rows=num_rows)

        # Calculate the next offset based on how many rows were produced
        current_offset = (
            int(start_offset)
            if start_offset
            else (self.offset_id if table_name == "my_table" else self.offset_key)
        )
        next_offset = current_offset + num_rows

        return data_iterator, {"offset": next_offset}

    def _read_helper(
        self,
        table_name: str,
        start_offset: dict,
        num_rows: int = 10,
    ) -> Iterator[dict]:
        if table_name not in self.tables:
            raise ValueError(f"Unknown table: {table_name}")

        current_offset = (
            int(start_offset)
            if start_offset
            else (self.offset_id if table_name == "my_table" else self.offset_key)
        )

        for i in range(num_rows):
            if table_name == "my_table":
                record = {
                    "id": current_offset,
                    "name": f"Name_{random.randint(1000, 9999)}",
                }
            else:
                record = {
                    "key": str(current_offset),
                    "value": f"Value_{random.randint(1000, 9999)}",
                }
            current_offset += 1

            yield record
