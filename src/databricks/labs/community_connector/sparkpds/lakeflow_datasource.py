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
from pyspark.sql.streaming.datasource import (
    ReadAllAvailable,
    SupportsTriggerAvailableNow,
)
from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsNamespaces,
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
METADATA_TABLE = "_community_table_metadata"
NAMESPACES_TABLE = "_community_namespaces"
TABLES_TABLE = "_community_tables"
VIRTUAL_TABLES = (METADATA_TABLE, NAMESPACES_TABLE, TABLES_TABLE)
TABLE_NAME = "tableName"
TABLE_NAME_LIST = "tableNameList"
TABLE_CONFIGS = "tableConfigs"
IS_DELETE_FLOW = "isDeleteFlow"
NAMESPACE_PREFIX = "namespacePrefix"
NAMESPACE = "namespace"


def _decode_list_of_str_option(option_name: str, value: str | None) -> list[str] | None:
    """Decode and validate a JSON-encoded ``list[str]`` Spark option.

    Returns ``None`` if the option is absent; otherwise the parsed list.
    Raises ``ValueError`` with the offending value if the JSON is malformed
    or the decoded value is not a list of strings.
    """
    if value is None:
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"option '{option_name}' must be a JSON-encoded list[str]; "
            f"got non-JSON value: {value!r}"
        ) from e
    if not isinstance(decoded, list) or not all(isinstance(s, str) for s in decoded):
        raise ValueError(
            f"option '{option_name}' must be a JSON-encoded list[str]; "
            f"got: {decoded!r}"
        )
    return decoded


def _decode_dict_option(option_name: str, value: str | None) -> dict:
    """Decode and validate a JSON-encoded ``dict`` Spark option."""
    if value is None:
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"option '{option_name}' must be a JSON-encoded dict; "
            f"got non-JSON value: {value!r}"
        ) from e
    if not isinstance(decoded, dict):
        raise ValueError(
            f"option '{option_name}' must be a JSON-encoded dict; got: {decoded!r}"
        )
    return decoded


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
        if self._supports_partition and self.table_name not in VIRTUAL_TABLES:
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
        elif self.table_name == NAMESPACES_TABLE:
            records = self._read_namespaces()
        elif self.table_name == TABLES_TABLE:
            records = self._read_tables()
        elif self._supports_partition and partition.value is not None:
            partition_desc = json.loads(partition.value)
            records = self.lakeflow_connect.read_partition(
                self.table_name, partition_desc, self.options
            )
        else:
            records, _ = self.lakeflow_connect.read_table(self.table_name, None, self.options)
        return map(lambda x: parse_value(x, self.schema), records)

    def _read_table_metadata(self):
        table_names = _decode_list_of_str_option(
            TABLE_NAME_LIST, self.options.get(TABLE_NAME_LIST)
        ) or []
        table_configs = _decode_dict_option(
            TABLE_CONFIGS, self.options.get(TABLE_CONFIGS)
        )
        all_records = []
        # Preserve caller-supplied table order — caller controls it.
        for table in table_names:
            metadata = self.lakeflow_connect.read_table_metadata(
                table, table_configs.get(table, {})
            )
            all_records.append({TABLE_NAME: table, **metadata})
        return all_records

    def _read_namespaces(self):
        # Connectors without SupportsNamespaces are flat — no rows.
        if not isinstance(self.lakeflow_connect, SupportsNamespaces):
            return []
        prefix = _decode_list_of_str_option(
            NAMESPACE_PREFIX, self.options.get(NAMESPACE_PREFIX)
        )
        namespaces = self.lakeflow_connect.list_namespaces(prefix)
        # Sort framework-side for deterministic output regardless of
        # connector iteration order.
        return [{"namespace": ns} for ns in sorted(namespaces)]

    def _read_tables(self):
        namespace_supplied = NAMESPACE in self.options
        if isinstance(self.lakeflow_connect, SupportsNamespaces):
            if not namespace_supplied:
                raise ValueError(
                    f"option '{NAMESPACE}' is required when reading "
                    f"'{TABLES_TABLE}' against a connector that implements "
                    f"SupportsNamespaces. Pass a JSON-encoded list[str] "
                    f"(use '[]' for root-level tables; walk the tree via "
                    f"'{NAMESPACES_TABLE}' to enumerate every namespace)."
                )
            namespace = _decode_list_of_str_option(
                NAMESPACE, self.options[NAMESPACE]
            )
            tables = self.lakeflow_connect.list_tables_in_namespace(namespace)
            return [
                {"namespace": namespace, TABLE_NAME: tn}
                for tn in sorted(tables)
            ]
        # Flat connector path. Reject a stray `namespace` option — the
        # caller probably mistook this connector for namespace-aware and
        # silently ignoring the option would mask the bug.
        if namespace_supplied:
            raise ValueError(
                f"option '{NAMESPACE}' was supplied but the connector does "
                f"not implement SupportsNamespaces. Either omit the option "
                f"or use a namespace-aware connector."
            )
        return [
            {"namespace": [], TABLE_NAME: tn}
            for tn in sorted(self.lakeflow_connect.list_tables())
        ]


class LakeflowSource(DataSource):
    """
    PySpark DataSource base for Lakeflow Connect.

    Two ways the connector implementation is bound:

    - Per-source subclass (wheel / multi-file deployment): subclass and set
      ``_lakeflow_connect_cls``::

          class GmailDataSource(LakeflowSource):
              _lakeflow_connect_cls = GmailLakeflowConnect

          spark.dataSource.register(GmailDataSource)

    - Merged single-file deployment (SDP): ``_lakeflow_connect_cls`` is left
      ``None`` and the connector is taken from the module-level
      ``LakeflowConnectImpl`` placeholder, which the merge script substitutes
      with the actual implementation class.
    """

    # Per-source subclasses set this. Left ``None`` on the base so the merged
    # single-file path falls back to the ``LakeflowConnectImpl`` placeholder.
    _lakeflow_connect_cls = None

    # Spark format name. Defaults to "lakeflow_connect" because Unity Catalog
    # connection-option injection looks for that exact string. A per-source
    # subclass may override this with its source name once it no longer relies
    # on UC injection (see the commented override in each source's __init__.py).
    _format_name = "lakeflow_connect"

    def __init__(self, options):
        self.options = options
        table = options.get(TABLE_NAME)
        # Catch typos against the framework's reserved virtual-table namespace
        # early — falling through to the connector with an unknown
        # `_community_*` name yields a confusing per-connector error.
        if table and table.startswith("_community_") and table not in VIRTUAL_TABLES:
            raise ValueError(
                f"unknown framework virtual table '{table}'. Valid framework "
                f"virtual tables are: {', '.join(VIRTUAL_TABLES)}. "
                f"For a regular source table, use a name that does not start "
                f"with '_community_'."
            )
        # Per-source subclasses bind the implementation via _lakeflow_connect_cls.
        # The merged single-file path leaves it None and relies on the
        # LakeflowConnectImpl placeholder (substituted by the merge script).
        connect_cls = type(self)._lakeflow_connect_cls or LakeflowConnectImpl
        self.lakeflow_connect = connect_cls(options)  # pylint: disable=abstract-class-instantiated

    @classmethod
    def name(cls):
        return cls._format_name

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
        if table == NAMESPACES_TABLE:
            return StructType(
                [
                    StructField("namespace", ArrayType(StringType()), False),
                ]
            )
        if table == TABLES_TABLE:
            return StructType(
                [
                    StructField("namespace", ArrayType(StringType()), False),
                    StructField(TABLE_NAME, StringType(), False),
                ]
            )
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
