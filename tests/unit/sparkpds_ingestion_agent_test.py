"""Unit tests for the ingestion-agent dispatcher.

Exercises :class:`IngestionAgentDispatcher` / :class:`IngestionAgentReader`
directly, without spinning up a SparkSession. Wiring the dispatcher
into :class:`LakeflowSource` (so callers can reach it through
``spark.read.format("lakeflow_connect").option("operation", ...)``) is
a follow-up PR — see the module docstring on
``sparkpds.ingestion_agent_datasource``.

Coverage:

- the five built-in operations against ExampleLakeflowConnect;
- the AgentOperation plug-in pattern for source-specific operations;
- the built-in subclassing pattern (override ``produce`` to customise);
- input validation (missing operation, missing required option);
- init-failure error containment (metadata-kind → error row,
  data-kind → re-raise, connector-optional → still runs).
"""

from __future__ import annotations

import json
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
    AgentError,
    AgentOperation,
    ErrorCode,
    LakeflowConnect,
    Parameter,
    SupportsIngestionAgent,
)
from databricks.labs.community_connector.libs.simulated_source.api import reset_api
from databricks.labs.community_connector.sources.example.example import (
    ExampleLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
    IngestionAgentDispatcher,
    ListObjectsOp,
    OP_GET_OBJECT_METADATA,
    OP_LIST_OBJECTS,
    OP_LIST_OPERATIONS,
    OP_READ_TABLE,
    OP_VALIDATE_CONNECTION,
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


def _dispatcher(
    operation: str,
    connector=None,
    init_error: Exception | None = None,
    **extra: str,
) -> IngestionAgentDispatcher:
    options = {"operation": operation, **_CREDS, **extra}
    if connector is None and init_error is None:
        connector = ExampleLakeflowConnect(_creds_only(options))
    return IngestionAgentDispatcher(
        options=options, connector=connector, init_error=init_error
    )


def _collect(dispatcher: IngestionAgentDispatcher) -> list:
    schema = dispatcher.schema()
    reader = dispatcher.reader(schema)
    return list(reader.read(next(iter(reader.partitions()))))


# ---------------------------------------------------------------------------
# Built-in: list_objects
# ---------------------------------------------------------------------------

def test_list_objects_defaults_to_flat_table_list():
    rows = _collect(_dispatcher(OP_LIST_OBJECTS))
    names = [r["name"] for r in rows]
    assert "events" in names
    assert "orders" in names
    types = {r["type"] for r in rows}
    assert types == {"table"}
    for row in rows:
        assert row["full_path"] == row["name"]
        assert row["_meta"]["status"] == "ok"


def test_list_objects_search_filters_by_regex():
    rows = _collect(_dispatcher(OP_LIST_OBJECTS, search="^orders$"))
    assert [r["name"] for r in rows] == ["orders"]


def test_list_objects_returns_empty_when_path_unknown():
    rows = _collect(_dispatcher(OP_LIST_OBJECTS, path="some/path"))
    assert rows == []


# ---------------------------------------------------------------------------
# Built-in: get_object_metadata
# ---------------------------------------------------------------------------

def test_get_object_metadata_flattens_read_table_metadata():
    rows = _collect(_dispatcher(OP_GET_OBJECT_METADATA, name="orders"))
    by_key = {r["key"]: r["value"] for r in rows}
    assert by_key["ingestion_type"] == "cdc_with_deletes"
    assert by_key["cursor_column"] == "updated_at"
    assert "id" in by_key["primary_key"]


def test_get_object_metadata_filters_by_key():
    rows = _collect(
        _dispatcher(OP_GET_OBJECT_METADATA, name="orders", metadataKey="ingestion_type")
    )
    assert len(rows) == 1
    assert rows[0]["key"] == "ingestion_type"


def test_get_object_metadata_filter_is_case_insensitive():
    rows = _collect(
        _dispatcher(OP_GET_OBJECT_METADATA, name="orders", metadataKey="INGESTION_TYPE")
    )
    assert len(rows) == 1
    assert rows[0]["key"] == "ingestion_type"


def test_get_object_metadata_unknown_key_returns_empty():
    """Framework filters silently; an unknown key just yields no rows."""
    rows = _collect(
        _dispatcher(OP_GET_OBJECT_METADATA, name="orders", metadataKey="does_not_exist")
    )
    assert rows == []


def test_get_object_metadata_missing_name_returns_error_row():
    rows = _collect(_dispatcher(OP_GET_OBJECT_METADATA))
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "name" in rows[0]["_meta"]["message"]


# ---------------------------------------------------------------------------
# Built-in: read_table (data-kind)
# ---------------------------------------------------------------------------

def test_read_table_returns_rows_with_natural_schema():
    dispatcher = _dispatcher(OP_READ_TABLE, tableName="orders")
    schema = dispatcher.schema()
    # Data-kind: source's natural schema, no _meta column.
    assert "_meta" not in [f.name for f in schema.fields]
    rows = _collect(dispatcher)
    assert len(rows) > 0


def test_read_table_missing_table_name_raises():
    from databricks.labs.community_connector.interface import AgentError

    dispatcher = _dispatcher(OP_READ_TABLE)
    with pytest.raises(AgentError, match="tableName") as exc_info:
        dispatcher.schema()
    assert exc_info.value.code == "bad_request"


# ---------------------------------------------------------------------------
# Built-in: validate_connection
# ---------------------------------------------------------------------------

def test_validate_connection_ok_with_default_health_check():
    rows = _collect(_dispatcher(OP_VALIDATE_CONNECTION))
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "ok"


def test_validate_connection_reports_init_failure_as_error_row():
    init_error = RuntimeError("bad creds")
    dispatcher = _dispatcher(
        OP_VALIDATE_CONNECTION, connector=None, init_error=init_error
    )
    rows = _collect(dispatcher)
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "bad creds" in rows[0]["_meta"]["message"]


# ---------------------------------------------------------------------------
# Built-in: list_operations (connector-optional)
# ---------------------------------------------------------------------------

def test_list_operations_returns_builtin_catalog():
    rows = _collect(_dispatcher(OP_LIST_OPERATIONS))
    names = {r["name"] for r in rows}
    assert {
        OP_LIST_OBJECTS,
        OP_READ_TABLE,
        OP_GET_OBJECT_METADATA,
        OP_VALIDATE_CONNECTION,
        OP_LIST_OPERATIONS,
    }.issubset(names)


def test_list_operations_works_when_connector_failed_to_build():
    """Connector-optional ops still run with a None connector."""
    dispatcher = _dispatcher(
        OP_LIST_OPERATIONS,
        connector=None,
        init_error=RuntimeError("bad creds"),
    )
    rows = _collect(dispatcher)
    names = {r["name"] for r in rows}
    # Built-ins are still listed.
    assert OP_LIST_OBJECTS in names
    assert OP_LIST_OPERATIONS in names


def test_list_operations_rows_carry_planning_metadata():
    """Every row exposes kind + schema/params JSON for planning."""
    rows = _collect(_dispatcher(OP_LIST_OPERATIONS))
    by_name = {r["name"]: r for r in rows}

    list_objects = by_name[OP_LIST_OBJECTS]
    assert list_objects["kind"] == "metadata"
    # Schema is exposed as JSON; round-trippable.
    schema_json = json.loads(list_objects["result_schema_json"])
    assert any(f["name"] == "name" for f in schema_json["fields"])
    # Parameters declare path and search.
    params = json.loads(list_objects["parameters_json"])
    param_names = {p["name"] for p in params}
    assert {"path", "search"} == param_names

    read_table = by_name[OP_READ_TABLE]
    # read_table is data-kind; its schema is dynamic, so result_schema_json is null.
    assert read_table["kind"] == "data"
    assert read_table["result_schema_json"] is None
    read_table_params = json.loads(read_table["parameters_json"])
    table_name_param = next(
        p for p in read_table_params if p["name"] == "tableName"
    )
    assert table_name_param["required"] is True
    # No `limit` option on the shared interface — Spark's .limit() pushes down.
    assert "limit" not in {p["name"] for p in read_table_params}


# ---------------------------------------------------------------------------
# Canonical error codes
# ---------------------------------------------------------------------------

def test_missing_required_param_yields_bad_request_code():
    """Framework rejects metadata-op missing required param with bad_request."""
    rows = _collect(_dispatcher(OP_GET_OBJECT_METADATA))  # missing `name`
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert rows[0]["_meta"]["code"] == ErrorCode.BAD_REQUEST


def test_unknown_exception_in_pull_yields_internal_error_code():
    class _BoomOp(AgentOperation):
        name = "example.boom"
        description = "Always raises a bare exception."
        kind = "metadata"
        schema = StructType([])

        def pull(self, connector, options):
            raise RuntimeError("kaboom")

    class _BoomConnector(ExampleLakeflowConnect, SupportsIngestionAgent):
        def agent_operations(self):
            return {_BoomOp.name: _BoomOp()}

    options = {"operation": _BoomOp.name, **_CREDS}
    dispatcher = IngestionAgentDispatcher(
        options=options,
        connector=_BoomConnector(_creds_only(options)),
    )
    rows = _collect(dispatcher)
    assert rows[0]["_meta"]["code"] == ErrorCode.INTERNAL_ERROR
    assert "kaboom" in rows[0]["_meta"]["message"]


def test_agent_error_code_propagates_into_meta():
    class _AuthFailingOp(AgentOperation):
        name = "example.auth_fail"
        description = "Raises AgentError(AUTH_FAILED)."
        kind = "metadata"
        schema = StructType([])

        def pull(self, connector, options):
            raise AgentError(ErrorCode.AUTH_FAILED, "creds expired")

    class _AuthConnector(ExampleLakeflowConnect, SupportsIngestionAgent):
        def agent_operations(self):
            return {_AuthFailingOp.name: _AuthFailingOp()}

    options = {"operation": _AuthFailingOp.name, **_CREDS}
    dispatcher = IngestionAgentDispatcher(
        options=options,
        connector=_AuthConnector(_creds_only(options)),
    )
    rows = _collect(dispatcher)
    assert rows[0]["_meta"]["status"] == "error"
    assert rows[0]["_meta"]["code"] == ErrorCode.AUTH_FAILED
    assert "creds expired" in rows[0]["_meta"]["message"]


def test_metadata_op_returns_error_row_when_connector_failed():
    """Metadata-kind ops that require a connector → error row on init failure."""
    dispatcher = _dispatcher(
        OP_LIST_OBJECTS,
        connector=None,
        init_error=RuntimeError("bad creds"),
    )
    rows = _collect(dispatcher)
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "bad creds" in rows[0]["_meta"]["message"]


def test_data_op_raises_when_connector_failed():
    """Data-kind ops → exception propagates."""
    init_error = RuntimeError("bad creds")
    dispatcher = _dispatcher(
        OP_READ_TABLE,
        tableName="orders",
        connector=None,
        init_error=init_error,
    )
    with pytest.raises(RuntimeError, match="bad creds"):
        dispatcher.schema()


def test_validate_connection_init_failure_uses_connection_failed_code():
    """Init failure on validate_connection → connection_failed (not internal_error)."""
    dispatcher = _dispatcher(
        OP_VALIDATE_CONNECTION,
        connector=None,
        init_error=RuntimeError("bad creds"),
    )
    rows = _collect(dispatcher)
    assert len(rows) == 1
    assert rows[0]["_meta"]["code"] == ErrorCode.CONNECTION_FAILED


# ---------------------------------------------------------------------------
# Customisation pattern 1: subclass a built-in to swap behaviour.
# ---------------------------------------------------------------------------

class _NamespacedListObjectsOp(ListObjectsOp):
    """Subclass that returns a fake catalog/schema/table hierarchy."""

    def produce(self, connector, *, path, search):
        del connector, search
        if path is None:
            return [{"name": "public", "type": "schema", "full_path": "public"}]
        if path == "public":
            return [{"name": "orders", "type": "table", "full_path": "public.orders"}]
        return []


class _NamespacedConnector(ExampleLakeflowConnect, SupportsIngestionAgent):
    def agent_operations(self):
        return {ListObjectsOp.name: _NamespacedListObjectsOp()}


def _namespaced_dispatcher(operation: str, **extra: str) -> IngestionAgentDispatcher:
    options = {"operation": operation, **_CREDS, **extra}
    return IngestionAgentDispatcher(
        options=options,
        connector=_NamespacedConnector(_creds_only(options)),
    )


def test_builtin_subclass_replaces_default_at_root():
    rows = _collect(_namespaced_dispatcher(OP_LIST_OBJECTS))
    assert [r["name"] for r in rows] == ["public"]
    assert rows[0]["type"] == "schema"
    assert rows[0]["_meta"]["status"] == "ok"


def test_builtin_subclass_handles_path_drill_down():
    rows = _collect(_namespaced_dispatcher(OP_LIST_OBJECTS, path="public"))
    assert len(rows) == 1
    assert rows[0]["name"] == "orders"
    assert rows[0]["full_path"] == "public.orders"


def test_builtin_subclass_returns_empty_for_unknown_path():
    rows = _collect(_namespaced_dispatcher(OP_LIST_OBJECTS, path="other"))
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


def _custom_ops_dispatcher(operation: str, **extra: str) -> IngestionAgentDispatcher:
    options = {"operation": operation, **_CREDS, **extra}
    return IngestionAgentDispatcher(
        options=options,
        connector=_ExampleWithCustomOps(_creds_only(options)),
    )


def test_source_specific_op_metadata_kind():
    dispatcher = _custom_ops_dispatcher(_DescribeTableOp.name, tableName="orders")
    schema = dispatcher.schema()
    assert [f.name for f in schema.fields] == [
        "column_name",
        "data_type",
        "nullable",
        "_meta",
    ]
    rows = _collect(dispatcher)
    column_names = [r["column_name"] for r in rows]
    assert "order_id" in column_names
    assert rows[0]["_meta"]["status"] == "ok"


def test_source_specific_op_data_kind_skips_meta():
    dispatcher = _custom_ops_dispatcher(_RowCountOp.name)
    schema = dispatcher.schema()
    assert [f.name for f in schema.fields] == ["count"]
    rows = _collect(dispatcher)
    assert len(rows) == 1
    assert rows[0]["count"] > 0


def test_source_specific_op_metadata_error_becomes_error_row():
    dispatcher = _custom_ops_dispatcher(_DescribeTableOp.name)  # missing tableName
    rows = _collect(dispatcher)
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "tableName" in rows[0]["_meta"]["message"]


def test_list_operations_includes_source_plug_ins():
    rows = _collect(_custom_ops_dispatcher("list_operations"))
    names = {r["name"] for r in rows}
    assert {
        "list_objects",
        "read_table",
        "get_object_metadata",
        "validate_connection",
        "list_operations",
    }.issubset(names)
    assert _DescribeTableOp.name in names
    assert _RowCountOp.name in names
    descriptions = {r["name"]: r["description"] for r in rows}
    assert "Required: tableName" in descriptions[_DescribeTableOp.name]


def test_agent_operations_rejects_non_operation_values():
    """An entry that isn't an AgentOperation raises a TypeError at lookup time."""

    class _BadConnector(ExampleLakeflowConnect, SupportsIngestionAgent):
        def agent_operations(self):
            return {"example.bogus": "not an AgentOperation"}

    with pytest.raises(TypeError, match="AgentOperation"):
        IngestionAgentDispatcher(
            options={"operation": "example.bogus", **_CREDS},
            connector=_BadConnector(_creds_only(_CREDS)),
        )


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
        IngestionAgentDispatcher(
            options=dict(_CREDS),
            connector=ExampleLakeflowConnect(_creds_only(_CREDS)),
        )


def test_unknown_operation_returns_error_row():
    dispatcher = _dispatcher("unsupported_op")
    with pytest.raises(ValueError, match="Unknown ingestion-agent operation"):
        dispatcher.schema()
