"""Spark Python Data Source implementing the ingestion-agent read API.

See ``experimental/yong-li/docs/ingestion-agent-api-design.md`` for the
specification this module implements. Briefly:

    spark.read.format("ingestion_agent")
        .option("databricks.connection", "...")  # forwarded as-is
        .option("operation", "list_objects")
        .option(...)
        .load()
        .collect()

A single format / single invocation shape, with the work dispatched on
the ``operation`` option. Defaults for every operation derive from the
existing :class:`LakeflowConnect` surface — every connector gains the
agent API automatically. Sources that need richer responses
(hierarchical listing, source-native sampling, extra metadata, custom
operations) implement the optional
:class:`SupportsIngestionAgent` mixin.
"""

from __future__ import annotations

import inspect
import itertools
import json
import re
from typing import Any, Iterable, Iterator, Mapping, Optional, Tuple

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsIngestionAgent,
)
from databricks.labs.community_connector.libs.utils import parse_value


# =============================================================================
# Merge-script placeholder (mirrors lakeflow_datasource.py). When this file
# is bundled into a single deployable via tools/scripts/merge_python_source.py
# the marker line below is rewritten to point at the concrete LakeflowConnect
# implementation class. In the package-installation path the registry rebinds
# this through a subclass instead.
# =============================================================================
# fmt: off
LakeflowConnectImpl = LakeflowConnect  # __LAKEFLOW_CONNECT_IMPL__
# fmt: on


# ---------------------------------------------------------------------------
# Reserved options
# ---------------------------------------------------------------------------

OPERATION = "operation"
PARENT = "parent"
SEARCH = "search"
PATH = "path"
NAME = "name"
METADATA_KEY = "metadataKey"
TABLE_NAME = "tableName"
CATALOG_NAME = "catalogName"
SCHEMA_NAME = "schemaName"
LIMIT = "limit"

DEFAULT_PREVIEW_LIMIT = 100


# ---------------------------------------------------------------------------
# Built-in operations
# ---------------------------------------------------------------------------

OP_LIST_OBJECTS = "list_objects"
OP_PREVIEW_TABLE = "preview_table"
OP_GET_OBJECT_METADATA = "get_object_metadata"
OP_VALIDATE_CONNECTION = "validate_connection"
OP_LIST_OPERATIONS = "list_operations"


_BUILTIN_OPERATION_DESCRIPTIONS: dict[str, str] = {
    OP_LIST_OBJECTS: (
        "Hierarchical listing of objects. "
        "Optional: parent (path to list under), search (regex on names). "
        "Returns rows of (name, type, full_path)."
    ),
    OP_PREVIEW_TABLE: (
        "Sample a tabular object. "
        f"Required: {TABLE_NAME}. "
        f"Optional: {CATALOG_NAME}, {SCHEMA_NAME}, {LIMIT} "
        f"(default {DEFAULT_PREVIEW_LIMIT}). "
        "Returns the table's natural schema and rows."
    ),
    OP_GET_OBJECT_METADATA: (
        "Per-object metadata as (key, value) rows. "
        f"Required: {NAME}. Optional: {PATH}, {METADATA_KEY}."
    ),
    OP_VALIDATE_CONNECTION: (
        "Connection-level health check. Returns one row with _meta only."
    ),
    OP_LIST_OPERATIONS: (
        "List operations supported by this connection."
    ),
}


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

_META_STRUCT = StructType(
    [
        StructField("status", StringType(), False),
        StructField("code", StringType(), True),
        StructField("message", StringType(), True),
    ]
)


def _meta_field(nullable: bool) -> StructField:
    return StructField("_meta", _META_STRUCT, nullable)


_LIST_OBJECTS_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("full_path", StringType(), True),
        _meta_field(nullable=True),
    ]
)

_GET_OBJECT_METADATA_SCHEMA = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        _meta_field(nullable=True),
    ]
)

_VALIDATE_CONNECTION_SCHEMA = StructType([_meta_field(nullable=False)])

_LIST_OPERATIONS_SCHEMA = StructType(
    [
        StructField("name", StringType(), False),
        StructField("description", StringType(), True),
        _meta_field(nullable=True),
    ]
)


_STATIC_SCHEMAS: dict[str, StructType] = {
    OP_LIST_OBJECTS: _LIST_OBJECTS_SCHEMA,
    OP_GET_OBJECT_METADATA: _GET_OBJECT_METADATA_SCHEMA,
    OP_VALIDATE_CONNECTION: _VALIDATE_CONNECTION_SCHEMA,
    OP_LIST_OPERATIONS: _LIST_OPERATIONS_SCHEMA,
}


def _meta(
    status: str = "ok",
    code: Optional[str] = None,
    message: Optional[str] = None,
) -> dict:
    return {"status": status, "code": code, "message": message}


def _error_str(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------

# pylint: disable=invalid-name,missing-function-docstring
class IngestionAgentSource(DataSource):
    """DataSource registered as ``ingestion_agent``.

    Lifecycle:

    - ``__init__`` validates the ``operation`` option and tries to
      build the underlying :class:`LakeflowConnect`. Init failure is
      tolerated for ``validate_connection`` (it becomes the error row
      reported back to the agent) and ``list_operations`` (we still
      return the built-in catalog).
    - ``schema()`` returns the operation's static schema, except for
      ``preview_table`` where the schema is the table's natural
      ``get_table_schema`` result.
    - ``reader()`` returns an :class:`IngestionAgentReader` that
      dispatches to the relevant operation handler on read.
    """

    def __init__(self, options: Mapping[str, str]) -> None:
        self.options = dict(options)
        self.operation = self.options.get(OPERATION)
        if not self.operation:
            raise ValueError(
                f"ingestion_agent source requires an '{OPERATION}' option."
            )
        self._connector_init_error: Optional[BaseException] = None
        self.lakeflow_connect: Optional[LakeflowConnect] = None
        try:
            self.lakeflow_connect = self._build_connector()
        except Exception as exc:  # pylint: disable=broad-except
            # validate_connection and list_operations tolerate init failure —
            # they convert it into structured output. Every other operation
            # genuinely needs the connector, so propagate.
            if self.operation not in (OP_VALIDATE_CONNECTION, OP_LIST_OPERATIONS):
                raise
            self._connector_init_error = exc

    def _build_connector(self) -> LakeflowConnect:
        # The merge script and the registry both retarget this — see the
        # module-level placeholder and the registry subclass.
        return LakeflowConnectImpl(  # pylint: disable=abstract-class-instantiated
            _connector_options(self.options)
        )

    @classmethod
    def name(cls) -> str:
        return "ingestion_agent"

    def schema(self):
        if self.operation in _STATIC_SCHEMAS:
            return _STATIC_SCHEMAS[self.operation]
        if self.operation == OP_PREVIEW_TABLE:
            return self._preview_table_schema()
        # Custom (source-prefixed) operation.
        schema = _custom_operation_schema(self.lakeflow_connect, self.operation)
        if schema is not None:
            return schema
        raise ValueError(f"Unknown ingestion_agent operation: {self.operation}")

    def _preview_table_schema(self) -> StructType:
        table_name = _required_option(self.options, TABLE_NAME, OP_PREVIEW_TABLE)
        table_options = _table_options(self.options)
        return self.lakeflow_connect.get_table_schema(table_name, table_options)

    def reader(self, schema: StructType):
        return IngestionAgentReader(
            options=self.options,
            schema=schema,
            operation=self.operation,
            lakeflow_connect=self.lakeflow_connect,
            connector_init_error=self._connector_init_error,
        )


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class IngestionAgentReader(DataSourceReader):
    def __init__(
        self,
        options: Mapping[str, str],
        schema: StructType,
        operation: str,
        lakeflow_connect: Optional[LakeflowConnect],
        connector_init_error: Optional[BaseException],
    ) -> None:
        self.options = dict(options)
        self.schema = schema
        self.operation = operation
        self.lakeflow_connect = lakeflow_connect
        self.connector_init_error = connector_init_error

    def partitions(self):
        # All ingestion-agent operations are small, single-partition reads.
        return [InputPartition(None)]

    def read(self, _partition):
        if self.operation == OP_PREVIEW_TABLE:
            # preview_table is data-bearing — Spark exceptions are the
            # error model. The records are raw dicts; parse against the
            # source's natural schema, no _meta injection.
            for record in self._preview_table():
                yield parse_value(record, self.schema)
            return

        try:
            rows = self._dispatch_metadata_operation()
        except Exception as exc:  # pylint: disable=broad-except
            rows = [self._error_row(exc)]
        for row in rows:
            yield parse_value(_with_default_meta(row), self.schema)

    # ----- metadata operation dispatch ------------------------------------

    def _dispatch_metadata_operation(self) -> Iterable[dict]:
        if self.operation == OP_LIST_OBJECTS:
            return self._list_objects()
        if self.operation == OP_GET_OBJECT_METADATA:
            return self._get_object_metadata()
        if self.operation == OP_VALIDATE_CONNECTION:
            return self._validate_connection()
        if self.operation == OP_LIST_OPERATIONS:
            return self._list_operations()
        # Custom (source-prefixed) operation.
        rows = _invoke_custom_operation(
            self.lakeflow_connect, self.operation, self.options
        )
        if rows is None:
            raise ValueError(f"Unknown ingestion_agent operation: {self.operation}")
        return rows

    # ----- list_objects ---------------------------------------------------

    def _list_objects(self) -> Iterable[dict]:
        parent = self.options.get(PARENT) or None
        search = self.options.get(SEARCH) or None
        override = _mixin_override(self.lakeflow_connect, "list_objects")
        if override is not None:
            rows = override(parent=parent, search=search)
            if rows is not NotImplemented:
                return _ensure_dicts(rows)
        return _default_list_objects(self.lakeflow_connect, parent, search)

    # ----- get_object_metadata --------------------------------------------

    def _get_object_metadata(self) -> Iterable[dict]:
        name = _required_option(self.options, NAME, OP_GET_OBJECT_METADATA)
        path = self.options.get(PATH) or None
        metadata_key = self.options.get(METADATA_KEY) or None
        override = _mixin_override(self.lakeflow_connect, "get_object_metadata")
        if override is not None:
            rows = override(path=path, name=name, metadata_key=metadata_key)
            if rows is not NotImplemented:
                return _ensure_dicts(rows)
        return _default_get_object_metadata(
            self.lakeflow_connect,
            table_name=name,
            table_options=_table_options(self.options),
            metadata_key=metadata_key,
        )

    # ----- preview_table --------------------------------------------------

    def _preview_table(self) -> Iterator[Mapping[str, Any]]:
        table_name = _required_option(self.options, TABLE_NAME, OP_PREVIEW_TABLE)
        limit = _int_option(self.options, LIMIT, DEFAULT_PREVIEW_LIMIT)
        table_options = _table_options(self.options)

        override = _mixin_override(self.lakeflow_connect, "preview_table")
        records: Iterable[Mapping[str, Any]]
        if override is not None:
            candidate = override(
                table_name=table_name,
                limit=limit,
                table_options=table_options,
            )
            if candidate is not NotImplemented:
                records = candidate
            else:
                records = _default_preview_records(
                    self.lakeflow_connect, table_name, table_options, limit
                )
        else:
            records = _default_preview_records(
                self.lakeflow_connect, table_name, table_options, limit
            )

        return itertools.islice(records, limit)

    # ----- validate_connection --------------------------------------------

    def _validate_connection(self) -> Iterable[dict]:
        if self.connector_init_error is not None:
            return [
                {
                    "_meta": _meta(
                        status="error",
                        code=type(self.connector_init_error).__name__,
                        message=_error_str(self.connector_init_error),
                    )
                }
            ]
        override = _mixin_override(self.lakeflow_connect, "validate_connection")
        if override is not None:
            result = override()
            if result is not NotImplemented:
                return [{"_meta": _normalize_meta(result)}]
        # Default health check: list_tables() round-trip.
        try:
            self.lakeflow_connect.list_tables()
        except Exception as exc:  # pylint: disable=broad-except
            return [
                {
                    "_meta": _meta(
                        status="error",
                        code=type(exc).__name__,
                        message=_error_str(exc),
                    )
                }
            ]
        return [{"_meta": _meta(status="ok")}]

    # ----- list_operations ------------------------------------------------

    def _list_operations(self) -> Iterable[dict]:
        for op_name, description in _BUILTIN_OPERATION_DESCRIPTIONS.items():
            yield {"name": op_name, "description": description}
        custom_ops = _custom_operations(self.lakeflow_connect)
        for op_name, handler in sorted(custom_ops.items()):
            doc = inspect.getdoc(handler) or ""
            yield {"name": op_name, "description": doc}

    # ----- error row ------------------------------------------------------

    def _error_row(self, exc: BaseException) -> dict:
        return {
            "_meta": _meta(
                status="error",
                code=type(exc).__name__,
                message=_error_str(exc),
            )
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Reserved option keys are addressed to the agent layer, not the underlying
# connector. They are stripped from the dict passed to LakeflowConnect.__init__
# and to table_options for table-targeting operations.
_RESERVED_OPTIONS = frozenset(
    {OPERATION, PARENT, SEARCH, PATH, NAME, METADATA_KEY, LIMIT}
)
# Table-targeting operations receive these as separate arguments. They stay
# inside the connector-init options dict (so credentials etc. survive) but
# are stripped from table_options to keep the connector's existing contract.
_TABLE_OPTION_RESERVED = frozenset(
    {OPERATION, PARENT, SEARCH, PATH, NAME, METADATA_KEY, LIMIT, TABLE_NAME}
)


def _connector_options(options: Mapping[str, str]) -> dict:
    return {k: v for k, v in options.items() if k not in _RESERVED_OPTIONS}


def _table_options(options: Mapping[str, str]) -> dict:
    return {k: v for k, v in options.items() if k not in _TABLE_OPTION_RESERVED}


def _required_option(
    options: Mapping[str, str], key: str, operation: str
) -> str:
    value = options.get(key)
    if not value:
        raise ValueError(
            f"Operation '{operation}' requires option '{key}'."
        )
    return value


def _int_option(options: Mapping[str, str], key: str, default: int) -> int:
    raw = options.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Option '{key}' must be an integer, got {raw!r}.") from exc


def _mixin_override(
    connector: Optional[LakeflowConnect], method_name: str
):
    if not isinstance(connector, SupportsIngestionAgent):
        return None
    method = getattr(connector, method_name, None)
    # The mixin's stub returns NotImplemented when not overridden — but
    # the method itself still exists. We always return the bound method
    # and let the caller observe NotImplemented at call time.
    return method


def _ensure_dicts(rows: Iterable[Any]) -> Iterator[dict]:
    for row in rows:
        if isinstance(row, Mapping):
            yield dict(row)
        else:
            raise TypeError(
                f"Ingestion-agent operation returned a non-mapping row of "
                f"type {type(row).__name__}: {row!r}"
            )


def _with_default_meta(row: Mapping[str, Any]) -> dict:
    out = dict(row)
    out.setdefault("_meta", _meta(status="ok"))
    return out


def _normalize_meta(value: Any) -> dict:
    if value is None:
        return _meta(status="ok")
    if isinstance(value, Mapping):
        return {
            "status": str(value.get("status", "ok")),
            "code": _str_or_none(value.get("code")),
            "message": _str_or_none(value.get("message")),
        }
    raise TypeError(
        f"validate_connection override must return a mapping, got {type(value).__name__}."
    )


def _str_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


# ---------------------------------------------------------------------------
# Default implementations
# ---------------------------------------------------------------------------

def _default_list_objects(
    connector: LakeflowConnect,
    parent: Optional[str],
    search: Optional[str],
) -> Iterator[dict]:
    """Flat listing of ``list_tables()`` — every table at the source root."""
    if parent:
        # The default connector has no hierarchy. A non-empty parent that
        # isn't the source root is treated as "no children" rather than an
        # error so the agent can probe paths without crashing.
        return iter([])
    pattern = re.compile(search) if search else None
    return (
        {"name": name, "type": "table", "full_path": name}
        for name in connector.list_tables()
        if pattern is None or pattern.search(name)
    )


def _default_get_object_metadata(
    connector: LakeflowConnect,
    table_name: str,
    table_options: Mapping[str, str],
    metadata_key: Optional[str],
) -> Iterator[dict]:
    """Flatten ``read_table_metadata`` into ``(key, value)`` rows.

    Maps the connector's metadata keys to the standardised ingestion-agent
    keys (``primary_key`` from ``primary_keys``, ``cursor_column`` from
    ``cursor_field``) and preserves the others verbatim.
    """
    raw = connector.read_table_metadata(table_name, table_options)

    standardized: list[Tuple[str, Any]] = []
    if "primary_keys" in raw:
        standardized.append(("primary_key", raw["primary_keys"]))
    if "cursor_field" in raw:
        standardized.append(("cursor_column", raw["cursor_field"]))
    if "ingestion_type" in raw:
        standardized.append(("ingestion_type", raw["ingestion_type"]))
    seen = {"primary_keys", "cursor_field", "ingestion_type"}
    for key, value in raw.items():
        if key in seen:
            continue
        standardized.append((key, value))

    if metadata_key is not None:
        standardized = [(k, v) for k, v in standardized if k == metadata_key]
        if not standardized:
            return iter([
                {
                    "key": metadata_key,
                    "value": None,
                    "_meta": _meta(
                        status="warning",
                        code="metadata_key_not_found",
                        message=f"Metadata key '{metadata_key}' is not "
                        f"available for '{table_name}'.",
                    ),
                }
            ])

    return ({"key": k, "value": _to_str_value(v)} for k, v in standardized)


def _default_preview_records(
    connector: LakeflowConnect,
    table_name: str,
    table_options: Mapping[str, str],
    limit: int,
) -> Iterator[Mapping[str, Any]]:
    """Pull records from ``read_table`` and bail once ``limit`` is reached."""
    del limit  # The reader slices the iterator; we just need to start the read.
    records, _offset = connector.read_table(table_name, None, dict(table_options))
    return records


def _to_str_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, dict, set)):
        try:
            return json.dumps(list(value) if isinstance(value, set) else value)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


# ---------------------------------------------------------------------------
# Custom operation hookup
# ---------------------------------------------------------------------------

def _custom_operations(
    connector: Optional[LakeflowConnect],
) -> Mapping[str, Any]:
    if not isinstance(connector, SupportsIngestionAgent):
        return {}
    try:
        ops = connector.custom_operations()
    except Exception:  # pylint: disable=broad-except
        return {}
    return ops or {}


def _custom_operation_schema(
    connector: Optional[LakeflowConnect], operation: str
) -> Optional[StructType]:
    """Schema for a custom operation.

    Convention: a custom operation handler may return a tuple
    ``(StructType, rows)`` for dynamic schemas, or just ``rows`` whose
    first row gives the columns. To keep ``schema()`` cheap we let the
    handler optionally expose a ``schema`` attribute / method.
    """
    handler = _custom_operations(connector).get(operation)
    if handler is None:
        return None
    schema_attr = getattr(handler, "schema", None)
    if isinstance(schema_attr, StructType):
        return _ensure_meta_field(schema_attr)
    if callable(schema_attr):
        result = schema_attr()
        if isinstance(result, StructType):
            return _ensure_meta_field(result)
    # Fall back to a generic (key, value) row shape so callers can still
    # consume the operation. Authors that need a richer schema should
    # provide one via the handler's ``schema`` attribute.
    return _GET_OBJECT_METADATA_SCHEMA


def _ensure_meta_field(schema: StructType) -> StructType:
    if any(f.name == "_meta" for f in schema.fields):
        return schema
    return StructType(list(schema.fields) + [_meta_field(nullable=True)])


def _invoke_custom_operation(
    connector: Optional[LakeflowConnect],
    operation: str,
    options: Mapping[str, str],
) -> Optional[Iterable[Mapping[str, Any]]]:
    handler = _custom_operations(connector).get(operation)
    if handler is None:
        return None
    request_options = {k: v for k, v in options.items() if k != OPERATION}
    result = handler(request_options)
    if isinstance(result, tuple) and len(result) == 2 and isinstance(
        result[0], StructType
    ):
        return _ensure_dicts(result[1])
    return _ensure_dicts(result)
