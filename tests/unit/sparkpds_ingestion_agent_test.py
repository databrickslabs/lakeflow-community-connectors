"""Unit tests for the ingestion-agent dispatcher on ``lakeflow_connect``.

Exercises :class:`IngestionAgentSource` / :class:`IngestionAgentReader`
directly and end-to-end through :class:`LakeflowSource`, without
spinning up a SparkSession.

Coverage:

- the five built-in operations against ExampleLakeflowConnect;
- the AgentOperation plug-in pattern for source-specific operations;
- the built-in subclassing pattern (override ``produce`` to customise);
- input validation (missing operation, missing required option);
- error containment for metadata operations (init failure → error row,
  not a Spark exception);
- end-to-end dispatch through LakeflowSource.
"""

from __future__ import annotations

from typing import Mapping

import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import (
    AgentOperation,
    LakeflowConnect,
    SupportsIngestionAgent,
)
from databricks.labs.community_connector.libs.simulated_source.api import reset_api
from databricks.labs.community_connector.sources.example.example import (
    ExampleLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
    DEFAULT_PREVIEW_LIMIT,
    IngestionAgentSource,
    ListObjectsOp,
    OP_GET_OBJECT_METADATA,
    OP_LIST_OBJECTS,
    OP_LIST_OPERATIONS,
    OP_PREVIEW_TABLE,
    OP_VALIDATE_CONNECTION,
)
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    LakeflowSource,
)


_CREDS = {
    "username": "ingestion-agent-test",
    "password": "ingestion-agent-test-pw",
}


@pytest.fixture(autouse=True)
def _reset_example_api():
    reset_api(_CREDS["username"], _CREDS["password"])
    yield


def _creds_only(options: Mapping[str, str]) -> dict:
    return {k: options[k] for k in ("username", "password") if k in options}


class _ExampleIngestionAgentSource(IngestionAgentSource):
    """IngestionAgentSource bound to ExampleLakeflowConnect.

    Mirrors what the registry wrapper does at runtime.
    """

    def _build_connector(self):
        return ExampleLakeflowConnect(_creds_only(self.options))


def _make_source(operation: str, **extra: str) -> IngestionAgentSource:
    return _ExampleIngestionAgentSource(
        {"operation": operation, **_CREDS, **extra}
    )


def _collect(source: IngestionAgentSource) -> list:
    schema = source.schema()
    reader = source.reader(schema)
    return list(reader.read(next(iter(reader.partitions()))))


# ---------------------------------------------------------------------------
# Built-in: list_objects
# ---------------------------------------------------------------------------

def test_list_objects_defaults_to_flat_table_list():
    rows = _collect(_make_source(OP_LIST_OBJECTS))
    names = [r["name"] for r in rows]
    assert "events" in names
    assert "orders" in names
    types = {r["type"] for r in rows}
    assert types == {"table"}
    for row in rows:
        assert row["full_path"] == row["name"]
        # Default success rows carry _meta.status == "ok".
        assert row["_meta"]["status"] == "ok"


def test_list_objects_search_filters_by_regex():
    rows = _collect(_make_source(OP_LIST_OBJECTS, search="^orders$"))
    assert [r["name"] for r in rows] == ["orders"]


def test_list_objects_returns_empty_when_parent_unknown():
    # The default implementation has no hierarchy — any non-empty parent
    # is treated as "no children" rather than an error.
    rows = _collect(_make_source(OP_LIST_OBJECTS, parent="some/path"))
    assert rows == []


# ---------------------------------------------------------------------------
# Built-in: get_object_metadata
# ---------------------------------------------------------------------------

def test_get_object_metadata_flattens_read_table_metadata():
    rows = _collect(_make_source(OP_GET_OBJECT_METADATA, name="orders"))
    by_key = {r["key"]: r["value"] for r in rows}
    # The example connector reports orders as cdc_with_deletes.
    assert by_key["ingestion_type"] == "cdc_with_deletes"
    assert by_key["cursor_column"] == "updated_at"
    # primary_key value gets stringified (JSON list) since the schema column
    # is a string.
    assert "id" in by_key["primary_key"]


def test_get_object_metadata_filters_by_key():
    rows = _collect(
        _make_source(
            OP_GET_OBJECT_METADATA, name="orders", metadataKey="ingestion_type"
        )
    )
    assert len(rows) == 1
    assert rows[0]["key"] == "ingestion_type"


def test_get_object_metadata_unknown_key_returns_warning_row():
    rows = _collect(
        _make_source(
            OP_GET_OBJECT_METADATA, name="orders", metadataKey="does_not_exist"
        )
    )
    assert len(rows) == 1
    assert rows[0]["key"] == "does_not_exist"
    assert rows[0]["value"] is None
    assert rows[0]["_meta"]["status"] == "warning"
    assert rows[0]["_meta"]["code"] == "metadata_key_not_found"


def test_get_object_metadata_missing_name_returns_error_row():
    rows = _collect(_make_source(OP_GET_OBJECT_METADATA))
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "name" in rows[0]["_meta"]["message"]


# ---------------------------------------------------------------------------
# Built-in: preview_table (data-kind)
# ---------------------------------------------------------------------------

def test_preview_table_returns_rows_capped_by_limit():
    source = _make_source(OP_PREVIEW_TABLE, tableName="orders", limit="2")
    schema = source.schema()
    # The schema is the source's natural table schema — no _meta column.
    assert "_meta" not in [f.name for f in schema.fields]
    rows = _collect(source)
    assert 0 < len(rows) <= 2


def test_preview_table_default_limit():
    source = _make_source(OP_PREVIEW_TABLE, tableName="products")
    rows = _collect(source)
    assert len(rows) <= DEFAULT_PREVIEW_LIMIT


def test_preview_table_missing_table_name_raises():
    source = _make_source(OP_PREVIEW_TABLE)
    # preview_table is data-bearing: errors propagate, not error rows.
    with pytest.raises(ValueError, match="tableName"):
        source.schema()


# ---------------------------------------------------------------------------
# Built-in: validate_connection
# ---------------------------------------------------------------------------

def test_validate_connection_ok_with_default_health_check():
    rows = _collect(_make_source(OP_VALIDATE_CONNECTION))
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "ok"


class _FailingConnector(LakeflowConnect):
    """LakeflowConnect whose __init__ always raises — exercises init-failure path."""

    def __init__(self, options):
        raise RuntimeError("bad creds")

    def list_tables(self):
        return []

    def get_table_schema(self, table_name, table_options):
        raise NotImplementedError

    def read_table_metadata(self, table_name, table_options):
        raise NotImplementedError

    def read_table(self, table_name, start_offset, table_options):
        raise NotImplementedError


class _FailingSource(IngestionAgentSource):
    def _build_connector(self):
        return _FailingConnector(self.options)


def test_validate_connection_reports_init_failure_as_error_row():
    source = _FailingSource({"operation": OP_VALIDATE_CONNECTION})
    rows = list(source.reader(source.schema()).read(None))
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "bad creds" in rows[0]["_meta"]["message"]


# ---------------------------------------------------------------------------
# Built-in: list_operations
# ---------------------------------------------------------------------------

def test_list_operations_returns_builtin_catalog():
    rows = _collect(_make_source(OP_LIST_OPERATIONS))
    names = {r["name"] for r in rows}
    assert {
        OP_LIST_OBJECTS,
        OP_PREVIEW_TABLE,
        OP_GET_OBJECT_METADATA,
        OP_VALIDATE_CONNECTION,
        OP_LIST_OPERATIONS,
    }.issubset(names)


# ---------------------------------------------------------------------------
# Customisation pattern 1: subclass a built-in to swap behaviour.
# ---------------------------------------------------------------------------

class _NamespacedListObjectsOp(ListObjectsOp):
    """Subclass that returns a fake catalog/schema/table hierarchy."""

    def produce(self, connector, *, parent, search):
        del connector, search
        if parent is None:
            return [{"name": "public", "type": "schema", "full_path": "public"}]
        if parent == "public":
            return [{"name": "orders", "type": "table", "full_path": "public.orders"}]
        return []


class _NamespacedConnector(ExampleLakeflowConnect, SupportsIngestionAgent):
    def agent_operations(self):
        return {ListObjectsOp.name: _NamespacedListObjectsOp()}


class _NamespacedSource(IngestionAgentSource):
    def _build_connector(self):
        return _NamespacedConnector(_creds_only(self.options))


def test_builtin_subclass_replaces_default_at_root():
    source = _NamespacedSource({"operation": OP_LIST_OBJECTS, **_CREDS})
    rows = _collect(source)
    assert [r["name"] for r in rows] == ["public"]
    assert rows[0]["type"] == "schema"
    assert rows[0]["_meta"]["status"] == "ok"


def test_builtin_subclass_handles_parent_drill_down():
    source = _NamespacedSource(
        {"operation": OP_LIST_OBJECTS, "parent": "public", **_CREDS}
    )
    rows = _collect(source)
    assert len(rows) == 1
    assert rows[0]["name"] == "orders"
    assert rows[0]["full_path"] == "public.orders"


def test_builtin_subclass_returns_empty_for_unknown_parent():
    source = _NamespacedSource(
        {"operation": OP_LIST_OBJECTS, "parent": "other", **_CREDS}
    )
    rows = _collect(source)
    assert rows == []


# ---------------------------------------------------------------------------
# Customisation pattern 2: add new source-prefixed operations.
# ---------------------------------------------------------------------------

class _DescribeTableOp(AgentOperation):
    """Describe a table's columns. Required option: tableName."""

    name = "example.describe_table"
    description = (
        "Source-specific: describe a table's columns. Required: tableName. "
        "Returns rows of (column_name, data_type, nullable)."
    )
    kind = "metadata"
    schema = StructType(
        [
            StructField("column_name", StringType(), False),
            StructField("data_type", StringType(), False),
            StructField("nullable", BooleanType(), False),
        ]
    )

    def pull(self, connector, options):
        table_name = options["tableName"]
        table_schema = connector.get_table_schema(table_name, options)
        for field in table_schema.fields:
            yield {
                "column_name": field.name,
                "data_type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }


class _RowCountOp(AgentOperation):
    """Data-kind op returning a single (count) row — bypasses _meta."""

    name = "example.row_count"
    description = "Return the number of records (data-kind, no _meta)."
    kind = "data"
    schema = StructType([StructField("count", IntegerType(), False)])

    def pull(self, connector, options):
        del options
        yield {"count": len(connector.list_tables())}


class _ExampleWithCustomOps(ExampleLakeflowConnect, SupportsIngestionAgent):
    def agent_operations(self):
        return {
            _DescribeTableOp.name: _DescribeTableOp(),
            _RowCountOp.name: _RowCountOp(),
        }


class _CustomOpsSource(IngestionAgentSource):
    def _build_connector(self):
        return _ExampleWithCustomOps(_creds_only(self.options))


def _make_custom_source(operation: str, **extra: str) -> IngestionAgentSource:
    return _CustomOpsSource({"operation": operation, **_CREDS, **extra})


def test_source_specific_op_metadata_kind():
    source = _make_custom_source(_DescribeTableOp.name, tableName="orders")
    schema = source.schema()
    # Metadata-kind ops auto-acquire a _meta column.
    assert [f.name for f in schema.fields] == [
        "column_name",
        "data_type",
        "nullable",
        "_meta",
    ]
    rows = _collect(source)
    column_names = [r["column_name"] for r in rows]
    assert "order_id" in column_names
    assert rows[0]["_meta"]["status"] == "ok"


def test_source_specific_op_data_kind_skips_meta():
    source = _make_custom_source(_RowCountOp.name)
    schema = source.schema()
    # Data-kind ops keep the natural schema, no _meta column appended.
    assert [f.name for f in schema.fields] == ["count"]
    rows = _collect(source)
    assert len(rows) == 1
    assert rows[0]["count"] > 0


def test_source_specific_op_metadata_error_becomes_error_row():
    source = _make_custom_source(_DescribeTableOp.name)  # missing tableName
    rows = _collect(source)
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    # KeyError on the missing option propagates as an error row.
    assert "tableName" in rows[0]["_meta"]["message"]


def test_list_operations_includes_source_plug_ins():
    rows = _collect(_make_custom_source("list_operations"))
    names = {r["name"] for r in rows}
    # Built-ins still present.
    assert {
        "list_objects",
        "preview_table",
        "get_object_metadata",
        "validate_connection",
        "list_operations",
    }.issubset(names)
    # Source plug-ins listed with their description.
    assert _DescribeTableOp.name in names
    assert _RowCountOp.name in names
    descriptions = {r["name"]: r["description"] for r in rows}
    assert "Required: tableName" in descriptions[_DescribeTableOp.name]


def test_agent_operations_rejects_non_operation_values():
    """An entry that isn't an AgentOperation raises a TypeError at lookup time."""

    class _BadConnector(ExampleLakeflowConnect, SupportsIngestionAgent):
        def agent_operations(self):
            return {"example.bogus": "not an AgentOperation"}

    class _BadSource(IngestionAgentSource):
        def _build_connector(self):
            return _BadConnector(_creds_only(self.options))

    with pytest.raises(TypeError, match="AgentOperation"):
        _BadSource({"operation": "example.bogus", **_CREDS})


# ---------------------------------------------------------------------------
# AgentOperation __init_subclass__ validation
# ---------------------------------------------------------------------------

def test_agent_operation_rejects_empty_name():
    with pytest.raises(TypeError, match="non-empty `name`"):
        class _NoName(AgentOperation):
            description = "x"
            kind = "metadata"
            schema = StructType([])

            def pull(self, connector, options):
                return iter([])


def test_agent_operation_rejects_invalid_kind():
    with pytest.raises(TypeError, match=r"kind must be one of"):
        class _BadKind(AgentOperation):
            name = "x"
            description = "x"
            kind = "metdata"  # typo
            schema = StructType([])

            def pull(self, connector, options):
                return iter([])


# ---------------------------------------------------------------------------
# Misc input validation
# ---------------------------------------------------------------------------

def test_missing_operation_raises_on_init():
    with pytest.raises(ValueError, match="operation"):
        _ExampleIngestionAgentSource(dict(_CREDS))


def test_unknown_operation_returns_error_row():
    source = _make_source("unsupported_op")
    with pytest.raises(ValueError, match="Unknown ingestion-agent operation"):
        source.schema()


# ---------------------------------------------------------------------------
# Integration through LakeflowSource: the agent surface is reachable via the
# same `lakeflow_connect` format that serves table reads. Setting `operation`
# routes through the agent dispatcher; omitting it keeps the regular path.
# ---------------------------------------------------------------------------

class _ExampleLakeflowSource(LakeflowSource):
    """LakeflowSource wired to ExampleLakeflowConnect for end-to-end tests."""

    def _build_table_connector(self, options):
        return ExampleLakeflowConnect(options)

    def _build_agent_source(self, options):
        return _ExampleIngestionAgentSource(options)


def test_lakeflow_source_routes_to_agent_when_operation_set():
    source = _ExampleLakeflowSource(
        {"operation": OP_LIST_OPERATIONS, **_CREDS}
    )
    schema = source.schema()
    # Agent dispatch is in effect — schema is the list_operations shape.
    assert {f.name for f in schema.fields} == {"name", "description", "_meta"}
    rows = list(source.reader(schema).read(None))
    names = {r["name"] for r in rows}
    assert {
        OP_LIST_OBJECTS,
        OP_GET_OBJECT_METADATA,
        OP_PREVIEW_TABLE,
        OP_VALIDATE_CONNECTION,
        OP_LIST_OPERATIONS,
    }.issubset(names)


def test_lakeflow_source_table_mode_unchanged_when_no_operation():
    # Without `operation`, the source behaves as before — schema() requires
    # `tableName`. We don't assert on data here; the existing source-specific
    # test suites cover the table read path.
    source = _ExampleLakeflowSource({"tableName": "orders", **_CREDS})
    schema = source.schema()
    # The table's natural schema has columns; absence of `_meta` confirms we
    # did NOT go through the agent dispatcher.
    assert "_meta" not in {f.name for f in schema.fields}


def test_lakeflow_source_agent_mode_blocks_streaming():
    source = _ExampleLakeflowSource(
        {"operation": OP_VALIDATE_CONNECTION, **_CREDS}
    )
    schema = source.schema()
    with pytest.raises(NotImplementedError, match="streaming"):
        source.streamReader(schema)
    with pytest.raises(NotImplementedError, match="streaming"):
        source.simpleStreamReader(schema)
