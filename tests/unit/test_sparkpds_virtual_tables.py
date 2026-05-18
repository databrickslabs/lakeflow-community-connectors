"""Unit tests for the namespace-aware virtual tables exposed by LakeflowSource.

These cover the in-process Python logic only (schema dispatch and the
``_read_namespaces`` / ``_read_tables`` helpers on ``LakeflowBatchReader``).
End-to-end coverage through ``spark.read.format('lakeflow_connect').load()``
requires a real Spark session and is intentionally out of scope here.
"""

import json
from typing import Iterator

import pytest
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsNamespaces,
    SupportsPartition,
)
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    NAMESPACE,
    NAMESPACE_PREFIX,
    NAMESPACES_TABLE,
    TABLE_NAME,
    TABLES_TABLE,
    LakeflowBatchReader,
    LakeflowSource,
)


class _FlatConnector(LakeflowConnect):
    """Connector without namespace support — exercises the fallback paths."""

    def list_tables(self) -> list[str]:
        return ["t1", "t2"]

    def get_table_schema(self, table_name, table_options):
        return StructType()

    def read_table_metadata(self, table_name, table_options):
        return {}

    def read_table(self, table_name, start_offset, table_options):
        return iter([]), {}


class _NamespacedConnector(LakeflowConnect, SupportsNamespaces):
    """Connector with a small two-level namespace hierarchy."""

    def list_tables(self) -> list[str]:
        return ["issues", "users"]

    def get_table_schema(self, table_name, table_options):
        return StructType()

    def read_table_metadata(self, table_name, table_options):
        return {}

    def read_table(self, table_name, start_offset, table_options):
        return iter([]), {}

    def list_namespaces(self, prefix=None):
        prefix = prefix or []
        if prefix == []:
            return [["orgA"], ["orgB"]]
        if prefix == ["orgA"]:
            return [["orgA", "repo1"], ["orgA", "repo2"]]
        return []

    _TABLES_BY_NAMESPACE = {
        (): ["global_settings"],
        ("orgA", "repo1"): ["issues"],
        ("orgA", "repo2"): ["issues"],
        ("orgB",): ["users"],
    }

    def list_tables_in_namespace(self, namespace):
        return list(self._TABLES_BY_NAMESPACE.get(tuple(namespace), []))


def _reader(connector: LakeflowConnect, table: str, **extra_opts) -> LakeflowBatchReader:
    options = {TABLE_NAME: table, **extra_opts}
    return LakeflowBatchReader(options, StructType(), connector)


def _source_schema(connector: LakeflowConnect, table: str) -> StructType:
    # Bypass LakeflowSource.__init__ — it instantiates LakeflowConnectImpl,
    # which in the source file is the abstract LakeflowConnect base.
    src = LakeflowSource.__new__(LakeflowSource)
    src.options = {TABLE_NAME: table}
    src.lakeflow_connect = connector
    return src.schema()


# ----- schemas -----


def test_community_namespaces_schema():
    schema = _source_schema(_NamespacedConnector({}), NAMESPACES_TABLE)
    assert schema == StructType(
        [StructField("namespace", ArrayType(StringType()), False)]
    )


def test_community_tables_schema():
    schema = _source_schema(_NamespacedConnector({}), TABLES_TABLE)
    assert schema == StructType(
        [
            StructField("namespace", ArrayType(StringType()), False),
            StructField("table_name", StringType(), False),
        ]
    )


# ----- _community_namespaces -----


def test_namespaces_empty_for_flat_connector():
    reader = _reader(_FlatConnector({}), NAMESPACES_TABLE)
    assert reader._read_namespaces() == []


def test_namespaces_root_no_prefix():
    reader = _reader(_NamespacedConnector({}), NAMESPACES_TABLE)
    assert reader._read_namespaces() == [
        {"namespace": ["orgA"]},
        {"namespace": ["orgB"]},
    ]


def test_namespaces_prefix_drills_in():
    reader = _reader(
        _NamespacedConnector({}),
        NAMESPACES_TABLE,
        **{NAMESPACE_PREFIX: json.dumps(["orgA"])},
    )
    assert reader._read_namespaces() == [
        {"namespace": ["orgA", "repo1"]},
        {"namespace": ["orgA", "repo2"]},
    ]


# ----- _community_tables -----


def test_tables_flat_connector_uses_list_tables_with_empty_namespace():
    """Flat connectors ignore the `namespace` option and report every table at root."""
    reader = _reader(_FlatConnector({}), TABLES_TABLE)
    assert reader._read_tables() == [
        {"namespace": [], "table_name": "t1"},
        {"namespace": [], "table_name": "t2"},
    ]


def test_tables_namespaced_requires_namespace_option():
    """Reading `_community_tables` without a namespace option must raise."""
    reader = _reader(_NamespacedConnector({}), TABLES_TABLE)
    with pytest.raises(ValueError, match="'namespace' is required"):
        reader._read_tables()


def test_tables_namespaced_root_only_with_empty_namespace():
    """An explicit empty-list namespace selects only root-level tables."""
    reader = _reader(
        _NamespacedConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: json.dumps([])},
    )
    assert reader._read_tables() == [
        {"namespace": [], "table_name": "global_settings"},
    ]


def test_tables_namespaced_specific_namespace():
    reader = _reader(
        _NamespacedConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: json.dumps(["orgA", "repo1"])},
    )
    assert reader._read_tables() == [
        {"namespace": ["orgA", "repo1"], "table_name": "issues"},
    ]


def test_tables_namespaced_unknown_namespace_returns_empty():
    reader = _reader(
        _NamespacedConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: json.dumps(["does", "not", "exist"])},
    )
    assert reader._read_tables() == []


# ----- partitions fast-path skips virtual tables -----


def test_partitions_skips_fanout_for_virtual_tables():
    """A connector with SupportsPartition must not be asked to partition the new virtual tables."""

    class _PartitionedNamespaced(_NamespacedConnector, SupportsPartition):
        def get_partitions(self, table_name, table_options):
            raise AssertionError(
                f"get_partitions called for virtual table '{table_name}'"
            )

        def read_partition(
            self, table_name, partition, table_options
        ) -> Iterator[dict]:
            return iter([])

    connector = _PartitionedNamespaced({})
    for table in (NAMESPACES_TABLE, TABLES_TABLE):
        reader = _reader(connector, table)
        parts = reader.partitions()
        assert len(parts) == 1
        assert parts[0].value is None
