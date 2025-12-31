from pyspark.sql.types import *
from pyspark.sql.datasource import (
    DataSource,
    SimpleDataSourceStreamReader,
    DataSourceReader,
)
from typing import Iterator
from sources.interface.lakeflow_connect import LakeflowConnect


METADATA_TABLE = "_lakeflow_metadata"
TABLE_NAME = "tableName"
TABLE_NAME_LIST = "tableNameList"


class LakeflowStreamReader(SimpleDataSourceStreamReader):
    """
    Implements a data source stream reader for Lakeflow Connect.
    Currently, only the simpleStreamReader is implemented, which uses a
    more generic protocol suitable for most data sources that support
    incremental loading.
    """

    def __init__(
        self,
        options: dict[str, str],
        schema: StructType,
        lakeflow_connect: LakeflowConnect,
    ):
        self.options = options
        self.lakeflow_connect = lakeflow_connect
        self.schema = schema

    def initialOffset(self):
        return {}

    def read(self, start: dict) -> (Iterator[tuple], dict):
        records, offset = self.lakeflow_connect.read_table(
            self.options["tableName"], start, self.options
        )
        rows = map(lambda x: parse_value(x, self.schema), records)
        return rows, offset

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[tuple]:
        # TODO: This does not ensure the records returned are identical across repeated calls.
        # For append-only tables, the data source must guarantee that reading from the same
        # start offset will always yield the same set of records.
        # For tables ingested as incremental CDC, it is only necessary that no new changes
        # are missed in the returned records.
        return self.read(start)[0]


class LakeflowBatchReader(DataSourceReader):
    def __init__(
        self,
        options: dict[str, str],
        schema: StructType,
        lakeflow_connect: LakeflowConnect,
    ):
        self.options = options
        self.schema = schema
        self.lakeflow_connect = lakeflow_connect
        self.table_name = options[TABLE_NAME]

    def read(self, partition):
        all_records = []
        if self.table_name == METADATA_TABLE:
            all_records = self._read_table_metadata()
        else:
            all_records, _ = self.lakeflow_connect.read_table(
                self.table_name, None, self.options
            )

        rows = map(lambda x: parse_value(x, self.schema), all_records)
        return iter(rows)

    def _read_table_metadata(self):
        table_name_list = self.options.get(TABLE_NAME_LIST, "")
        table_names = [o.strip() for o in table_name_list.split(",") if o.strip()]
        all_records = []
        for table in table_names:
            metadata = self.lakeflow_connect.read_table_metadata(table, self.options)
            all_records.append({"tableName": table, **metadata})
        return all_records


class LakeflowSource(DataSource):
    def __init__(self, options):
        self.options = options
        self.lakeflow_connect = LakeflowConnect(options)

    @classmethod
    def name(cls):
        return "lakeflow_connect"

    def schema(self):
        table = self.options["tableName"]
        if table == METADATA_TABLE:
            return StructType(
                [
                    StructField("tableName", StringType(), False),
                    StructField("primary_keys", ArrayType(StringType()), True),
                    StructField("cursor_field", StringType(), True),
                    StructField("ingestion_type", StringType(), True),
                ]
            )
        else:
            # Assuming the LakeflowConnect interface uses get_table_schema, not get_table_details
            return self.lakeflow_connect.get_table_schema(table, self.options)

    def reader(self, schema: StructType):
        return LakeflowBatchReader(self.options, schema, self.lakeflow_connect)

    def simpleStreamReader(self, schema: StructType):
        return LakeflowStreamReader(self.options, schema, self.lakeflow_connect)


spark.dataSource.register(LakeflowSource)
