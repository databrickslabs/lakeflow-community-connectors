"""Unit tests for the ingestion_agent Spark Python Data Source.

These exercise IngestionAgentSource / IngestionAgentReader directly,
without spinning up a SparkSession. They cover:

- the five built-in operations against ExampleLakeflowConnect (which
  is backed by the in-process simulated REST API);
- the optional ``SupportsIngestionAgent`` mixin override path;
- input validation (missing operation, missing required option);
- error containment for metadata operations (init failure → error row,
  not a Spark exception).
"""

from __future__ import annotations

from typing import Iterable, Mapping, Optional

import pytest

from databricks.labs.community_connector.interface import (
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
    OP_GET_OBJECT_METADATA,
    OP_LIST_OBJECTS,
    OP_LIST_OPERATIONS,
    OP_PREVIEW_TABLE,
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


def _make_source(operation: str, **extra: str) -> IngestionAgentSource:
    """Build an IngestionAgentSource bound to ExampleLakeflowConnect.

    A small subclass overrides ``_build_connector`` so the source uses the
    concrete connector class instead of the placeholder LakeflowConnectImpl
    alias. This is the same pattern the registry uses at runtime.
    """

    class _ExampleIngestionAgentSource(IngestionAgentSource):
        def _build_connector(self):
            return ExampleLakeflowConnect(_creds_only(self.options))

    options = {"operation": operation, **_CREDS, **extra}
    return _ExampleIngestionAgentSource(options)


def _creds_only(options: Mapping[str, str]) -> dict:
    return {k: options[k] for k in ("username", "password") if k in options}


def _collect(source: IngestionAgentSource) -> list:
    schema = source.schema()
    reader = source.reader(schema)
    rows = list(reader.read(next(iter(reader.partitions()))))
    return rows


# ---------------------------------------------------------------------------
# list_objects
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
# get_object_metadata
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
    source = _make_source(OP_GET_OBJECT_METADATA)
    rows = _collect(source)
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "name" in rows[0]["_meta"]["message"]


# ---------------------------------------------------------------------------
# preview_table
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
# validate_connection
# ---------------------------------------------------------------------------

def test_validate_connection_ok_with_default_health_check():
    rows = _collect(_make_source(OP_VALIDATE_CONNECTION))
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "ok"


def test_validate_connection_reports_init_failure_as_error_row():
    # Build a connector class whose __init__ raises so we can verify the
    # error is converted to a structured row.
    class FailingConnector(LakeflowConnect):
        def __init__(self, options):  # noqa: D401 - matches base signature
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
            return FailingConnector(self.options)

    source = _FailingSource({"operation": OP_VALIDATE_CONNECTION})
    rows = list(
        source.reader(source.schema()).read(None)
    )
    assert len(rows) == 1
    assert rows[0]["_meta"]["status"] == "error"
    assert "bad creds" in rows[0]["_meta"]["message"]


# ---------------------------------------------------------------------------
# list_operations
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
# SupportsIngestionAgent override path
# ---------------------------------------------------------------------------

class _MixinExample(ExampleLakeflowConnect, SupportsIngestionAgent):
    """Example connector with a richer ``list_objects`` and a custom op."""

    def list_objects(
        self,
        parent: Optional[str] = None,
        search: Optional[str] = None,
    ) -> Iterable[Mapping[str, str]]:
        del search
        if parent is None:
            return [
                {"name": "public", "type": "schema", "full_path": "public"},
            ]
        if parent == "public":
            return [
                {"name": "orders", "type": "table", "full_path": "public.orders"},
            ]
        return NotImplemented  # fall back to default for any other parent


def test_mixin_list_objects_override_wins():
    class _Source(IngestionAgentSource):
        def _build_connector(self):
            return _MixinExample(_creds_only(self.options))

    source = _Source({"operation": OP_LIST_OBJECTS, **_CREDS})
    rows = _collect(source)
    assert [r["name"] for r in rows] == ["public"]
    assert rows[0]["type"] == "schema"

    source_child = _Source(
        {"operation": OP_LIST_OBJECTS, "parent": "public", **_CREDS}
    )
    rows = _collect(source_child)
    assert len(rows) == 1
    assert rows[0]["name"] == "orders"
    assert rows[0]["type"] == "table"
    assert rows[0]["full_path"] == "public.orders"
    assert rows[0]["_meta"]["status"] == "ok"


def test_mixin_notimplemented_falls_back_to_default():
    class _Source(IngestionAgentSource):
        def _build_connector(self):
            return _MixinExample(_creds_only(self.options))

    # `parent="other"` triggers the NotImplemented path in our mixin, so the
    # default flat listing kicks in. The default ignores non-empty parents
    # (no hierarchy), so the result is empty.
    source = _Source({"operation": OP_LIST_OBJECTS, "parent": "other", **_CREDS})
    rows = _collect(source)
    assert rows == []


# ---------------------------------------------------------------------------
# Misc input validation
# ---------------------------------------------------------------------------

def test_missing_operation_raises_on_init():
    class _Source(IngestionAgentSource):
        def _build_connector(self):
            return ExampleLakeflowConnect(_creds_only(self.options))

    with pytest.raises(ValueError, match="operation"):
        _Source(dict(_CREDS))


def test_unknown_operation_returns_error_row():
    class _Source(IngestionAgentSource):
        def _build_connector(self):
            return ExampleLakeflowConnect(_creds_only(self.options))

    source = _Source({"operation": "unsupported_op", **_CREDS})
    with pytest.raises(ValueError, match="Unknown ingestion_agent operation"):
        source.schema()
