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
    METADATA_TABLE,
    NAMESPACE,
    NAMESPACE_PREFIX,
    NAMESPACES_TABLE,
    TABLE_CONFIGS,
    TABLE_NAME,
    TABLE_NAME_LIST,
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
    """Connector with a small two-level namespace hierarchy.

    Returns from a dict (insertion-ordered) and at one point from a set so
    the test exercises the framework-side sorting guarantee.
    """

    def list_tables(self) -> list[str]:
        # Framework must never call this path for a SupportsNamespaces
        # connector — `_community_tables` routes through
        # `list_tables_in_namespace`. Raise loudly if that assumption
        # breaks.
        raise NotImplementedError(
            "list_tables should not be called on a SupportsNamespaces connector"
        )

    def get_table_schema(self, table_name, table_options):
        return StructType()

    def read_table_metadata(self, table_name, table_options):
        return {}

    def read_table(self, table_name, start_offset, table_options):
        return iter([]), {}

    def list_namespaces(self, prefix=None):
        prefix = prefix or []
        if prefix == []:
            # Return out of sorted order so the framework's sort is
            # actually exercised by the test below.
            return [["orgB"], ["orgA"]]
        if prefix == ["orgA"]:
            return [["orgA", "repo2"], ["orgA", "repo1"]]
        return []

    _TABLES_BY_NAMESPACE = {
        (): ["global_settings"],
        ("orgA", "repo1"): ["issues"],
        ("orgA", "repo2"): ["issues"],
        ("orgB",): ["users", "settings"],  # multi-table namespace exercises sort
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


def test_tables_namespaced_sorts_table_names():
    """Table names within a namespace are sorted framework-side."""
    reader = _reader(
        _NamespacedConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: json.dumps(["orgB"])},
    )
    # Fixture returns ["users", "settings"]; framework sorts → ["settings", "users"].
    assert reader._read_tables() == [
        {"namespace": ["orgB"], "table_name": "settings"},
        {"namespace": ["orgB"], "table_name": "users"},
    ]


def test_tables_flat_connector_with_namespace_option_raises():
    """A flat connector should not silently ignore a supplied namespace option."""
    reader = _reader(
        _FlatConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: json.dumps([])},
    )
    with pytest.raises(ValueError, match="does not implement SupportsNamespaces"):
        reader._read_tables()


# ----- malformed option values -----


def test_namespace_prefix_non_json_raises():
    reader = _reader(
        _NamespacedConnector({}),
        NAMESPACES_TABLE,
        **{NAMESPACE_PREFIX: "orgA"},  # bare string, not JSON-encoded
    )
    with pytest.raises(ValueError, match="JSON-encoded list"):
        reader._read_namespaces()


def test_namespace_prefix_wrong_type_raises():
    """`'"orgA"'` is valid JSON but parses to a string, not a list[str]."""
    reader = _reader(
        _NamespacedConnector({}),
        NAMESPACES_TABLE,
        **{NAMESPACE_PREFIX: json.dumps("orgA")},  # parses to "orgA", not ["orgA"]
    )
    with pytest.raises(ValueError, match="JSON-encoded list"):
        reader._read_namespaces()


def test_namespace_non_json_raises():
    reader = _reader(
        _NamespacedConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: "orgA"},
    )
    with pytest.raises(ValueError, match="JSON-encoded list"):
        reader._read_tables()


def test_namespace_wrong_type_raises():
    reader = _reader(
        _NamespacedConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: json.dumps({"not": "a list"})},
    )
    with pytest.raises(ValueError, match="JSON-encoded list"):
        reader._read_tables()


def test_namespace_list_of_non_strings_raises():
    reader = _reader(
        _NamespacedConnector({}),
        TABLES_TABLE,
        **{NAMESPACE: json.dumps(["org", 42])},
    )
    with pytest.raises(ValueError, match="JSON-encoded list"):
        reader._read_tables()


def test_table_name_list_wrong_type_raises():
    """`tableNameList` accepts only a JSON list of strings (not CSV)."""
    reader = _reader(
        _FlatConnector({}),
        METADATA_TABLE,
        **{TABLE_NAME_LIST: "t1,t2"},  # the old comma-separated format
    )
    with pytest.raises(ValueError, match="JSON-encoded list"):
        reader._read_table_metadata()


def test_table_configs_wrong_type_raises():
    reader = _reader(
        _FlatConnector({}),
        METADATA_TABLE,
        **{
            TABLE_NAME_LIST: json.dumps(["t1"]),
            TABLE_CONFIGS: json.dumps(["not", "a", "dict"]),
        },
    )
    with pytest.raises(ValueError, match="JSON-encoded dict"):
        reader._read_table_metadata()


# ----- unknown `_community_*` tableName -----


def test_unknown_community_table_raises_in_source():
    """LakeflowSource rejects typos against the reserved `_community_*` namespace."""
    # Bypass LakeflowConnectImpl(options) (it's the abstract base) by
    # invoking __init__ directly and only asserting the early guard fires
    # before that line.
    with pytest.raises(ValueError, match="unknown framework virtual table"):
        LakeflowSource({TABLE_NAME: "_community_tabls"})


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
