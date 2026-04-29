from typing import Iterator
import json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamReader,
    InputPartition,
    SimpleDataSourceStreamReader,
    DataSourceReader,
)
from pyspark.sql.streaming.datasource import ReadAllAvailable, SupportsTriggerAvailableNow
from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartition,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.libs.utils import parse_value


# =============================================================================
# TEMPORARY WORKAROUND: Placeholder for merge script replacement
# =============================================================================
# Due to current Spark Declarative Pipeline (SDP) limitations, Python Data Source
# implementations cannot use module imports. The merge script (tools/scripts/
# merge_python_source.py) combines this file with source connector implementations
# into a single deployable file.
#
# The line below is replaced during merge:
#   - The marker `# __LAKEFLOW_CONNECT_IMPL__` is detected by the merge script
#   - `LakeflowConnect` is replaced with the actual implementation class name
#     (e.g., GithubLakeflowConnect, or the source's own LakeflowConnect class)
#
# This workaround will be removed once SDP supports proper module imports.
# =============================================================================
# fmt: off
LakeflowConnectImpl = LakeflowConnect  # __LAKEFLOW_CONNECT_IMPL__
# fmt: on

# Constant option or column names
METADATA_TABLE = "_lakeflow_metadata"
TABLE_NAME = "tableName"
TABLE_NAME_LIST = "tableNameList"
TABLE_CONFIGS = "tableConfigs"
IS_DELETE_FLOW = "isDeleteFlow"


# PySpark's DataSource API requires camelCase method names and inherits
# semantics from the parent class, so per-method docstrings are redundant.
# pylint: disable=invalid-name,missing-function-docstring
class LakeflowStreamReader(SimpleDataSourceStreamReader, SupportsTriggerAvailableNow):
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
        is_delete_flow = self.options.get(IS_DELETE_FLOW) == "true"
        # Strip delete flow options before passing to connector
        table_options = {
            k: v for k, v in self.options.items() if k != IS_DELETE_FLOW
        }

        if is_delete_flow:
            records, offset = self.lakeflow_connect.read_table_deletes(
                self.options[TABLE_NAME], start, table_options
            )
        else:
            records, offset = self.lakeflow_connect.read_table(
                self.options[TABLE_NAME], start, table_options
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

    def prepareForTriggerAvailableNow(self) -> None:
        # No need to do anything special here. Everything is handled in the __init__ method.
        pass


class LakeflowPartitionedStreamReader(DataSourceStreamReader, SupportsTriggerAvailableNow):
    """Proxy that bridges SupportsPartitionedStream to PySpark's DataSourceStreamReader.

    Used when a connector implements the SupportsPartitionedStream mixin to
    support partitioned streaming reads across Spark executors.
    """

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
        self.table_options = {k: v for k, v in options.items() if k != IS_DELETE_FLOW}

    def initialOffset(self):
        return {}

    def getDefaultReadLimit(self):
        # Admission control is the connector's responsibility (e.g. via
        # window_days, max_records_per_batch), not the engine's.  Always
        # ask the engine for ReadAllAvailable.
        return ReadAllAvailable()

    def latestOffset(self, start: dict, limit) -> dict:
        # We declared ReadAllAvailable via getDefaultReadLimit; the engine
        # must respect it.  Anything else means admission-control expectations
        # we do not support — fail loudly rather than silently ignore.
        if not isinstance(limit, ReadAllAvailable):
            raise ValueError(
                f"LakeflowPartitionedStreamReader only supports ReadAllAvailable; "
                f"got {type(limit).__name__}. Micro-batch sizing must be controlled "
                f"by the connector implementation (table_options), not the engine."
            )
        return self.lakeflow_connect.latest_offset(
            self.table_name, self.table_options, start
        )

    def partitions(self, start: dict, end: dict):
        partition_descs = self.lakeflow_connect.get_partitions(
            self.table_name, self.table_options, start, end
        )
        return [InputPartition(json.dumps(p)) for p in partition_descs]

    def read(self, partition: InputPartition):
        partition_desc = json.loads(partition.value)
        records = self.lakeflow_connect.read_partition(
            self.table_name, partition_desc, self.table_options
        )
        return map(lambda x: parse_value(x, self.schema), records)

    def prepareForTriggerAvailableNow(self) -> None:
        # No need to do anything special here. Everything is handled in the __init__ method.
        pass


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
        self._supports_partition = isinstance(lakeflow_connect, SupportsPartition)

    def partitions(self):
        if self._supports_partition and self.table_name != METADATA_TABLE:
            try:
                partition_descs = self.lakeflow_connect.get_partitions(
                    self.table_name, self.options
                )
                return [InputPartition(json.dumps(p)) for p in partition_descs]
            except Exception:
                self._supports_partition = False
        return [InputPartition(None)]

    def read(self, partition):
        if self.table_name == METADATA_TABLE:
            records = self._read_table_metadata()
        elif self._supports_partition and partition.value is not None:
            partition_desc = json.loads(partition.value)
            records = self.lakeflow_connect.read_partition(
                self.table_name, partition_desc, self.options
            )
        else:
            records, _ = self.lakeflow_connect.read_table(self.table_name, None, self.options)
        return map(lambda x: parse_value(x, self.schema), records)

    def _read_table_metadata(self):
        table_name_list = self.options.get(TABLE_NAME_LIST, "")
        table_names = [o.strip() for o in table_name_list.split(",") if o.strip()]
        all_records = []
        table_configs = json.loads(self.options.get(TABLE_CONFIGS, "{}"))
        for table in table_names:
            metadata = self.lakeflow_connect.read_table_metadata(
                table, table_configs.get(table, {})
            )
            all_records.append({TABLE_NAME: table, **metadata})
        return all_records


class LakeflowSource(DataSource):
    """
    PySpark DataSource implementation for Lakeflow Connect.
    """

    def __init__(self, options):
        self.options = options
        # TEMPORARY: LakeflowConnectImpl is replaced with the actual implementation
        # class during merge. See the placeholder comment at the top of this file.
        self.lakeflow_connect = LakeflowConnectImpl(options)  # pylint: disable=abstract-class-instantiated

    @classmethod
    def name(cls):
        return "lakeflow_connect"

    def schema(self):
        table = self.options[TABLE_NAME]
        if table == METADATA_TABLE:
            return StructType(
                [
                    StructField(TABLE_NAME, StringType(), False),
                    StructField("primary_keys", ArrayType(StringType()), True),
                    StructField("cursor_field", StringType(), True),
                    StructField("ingestion_type", StringType(), True),
                ]
            )
        else:
            return self.lakeflow_connect.get_table_schema(table, self.options)

    def reader(self, schema: StructType):
        return LakeflowBatchReader(self.options, schema, self.lakeflow_connect)

    def streamReader(self, schema: StructType):
        # Use the partitioned DataSourceStreamReader when the connector
        # implements SupportsPartitionedStream and the table opts in.
        # Otherwise, delegate to super() which raises PySparkNotImplementedError,
        # causing Spark to fall back to simpleStreamReader().
        if isinstance(self.lakeflow_connect, SupportsPartitionedStream):
            table = self.options[TABLE_NAME]
            if self.lakeflow_connect.is_partitioned(table):
                return LakeflowPartitionedStreamReader(self.options, schema, self.lakeflow_connect)
        return super().streamReader(schema)

    def simpleStreamReader(self, schema: StructType):
        return LakeflowStreamReader(self.options, schema, self.lakeflow_connect)
