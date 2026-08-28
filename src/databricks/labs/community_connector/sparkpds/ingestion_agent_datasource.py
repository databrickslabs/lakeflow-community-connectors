"""Ingestion-agent operation dispatcher.

Implements the read-side agent surface as a plain dispatcher class.
Callers construct :class:`IngestionAgentDispatcher` directly with a
:class:`LakeflowConnect` instance and an options dict, then ask for
``schema()`` / ``reader(schema)``.

Wiring this dispatcher into :class:`LakeflowSource` — so it's reachable
via ``spark.read.format("lakeflow_connect").option("operation", ...)``
— is a follow-up PR. That change also needs the merge script
(``tools/scripts/merge_python_source.py``) to start bundling this
module + ``interface/supports_ingestion_agent.py`` +
``interface/agent_protocol.py`` into the ``_generated_*`` deployables.
Until then this module is standalone scaffolding: importable from the
package, exercised by unit tests, but not yet on the Spark format
dispatch path.

Operations are first-class :class:`AgentOperation` objects. Five
built-ins (``list_objects``, ``preview_table``, ``get_object_metadata``,
``validate_connection``, ``list_operations``) ship with the framework
and derive their behaviour from the existing :class:`LakeflowConnect`
surface — every connector gains the agent surface automatically.

Source customisation goes through
:meth:`SupportsIngestionAgent.agent_operations`:

- **To customise a built-in**, subclass the relevant built-in class
  (e.g. :class:`ListObjectsOp`) and override ``produce`` — not
  ``pull``. The framework owns dispatch, schema, kind, and name; the
  ``produce`` override hook receives typed keyword arguments.

- **To add a new source-prefixed operation**, subclass
  :class:`AgentOperation` directly and implement ``pull``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable, Iterator, Mapping, Optional, Tuple

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.datasource import DataSourceReader, InputPartition

from databricks.labs.community_connector.interface import (
    AgentError,
    AgentOperation,
    ErrorCode,
    LakeflowConnect,
    Parameter,
    SupportsIngestionAgent,
)
from databricks.labs.community_connector.libs.utils import parse_value


# ---------------------------------------------------------------------------
# Reserved options
# ---------------------------------------------------------------------------

OPERATION = "operation"
SEARCH = "search"
PATH = "path"
NAME = "name"
METADATA_KEY = "metadataKey"
TABLE_NAME = "tableName"
CATALOG_NAME = "catalogName"
SCHEMA_NAME = "schemaName"

# Built-in operation names exposed for callers and tests.
OP_LIST_OBJECTS = "list_objects"
OP_READ_TABLE = "read_table"
OP_GET_OBJECT_METADATA = "get_object_metadata"
OP_VALIDATE_CONNECTION = "validate_connection"
OP_LIST_OPERATIONS = "list_operations"

# Operation kinds.
KIND_METADATA = "metadata"
KIND_DATA = "data"

# Operations that can dispatch even when the connector failed to build
# (e.g. bad creds). Today this is only `list_operations` — it can yield
# the framework's built-in catalog without touching the connector.
_CONNECTOR_OPTIONAL_OPS = frozenset({OP_LIST_OPERATIONS})


def _requires_connector(op: AgentOperation) -> bool:
    return op.name not in _CONNECTOR_OPTIONAL_OPS

# Reserved keys the agent layer owns. Stripped from the dict passed into
# LakeflowConnect.__init__ so the connector can't accidentally interpret an
# agent option as one of its own. The user's request options keep these
# keys; ops read them via the AgentOperation.pull options dict.
_AGENT_RESERVED_KEYS = frozenset(
    {OPERATION, SEARCH, PATH, NAME, METADATA_KEY}
)


def _agent_options(options: Mapping[str, str]) -> dict:
    """Options visible to ``AgentOperation.pull`` / ``resolve_schema``.

    Strips only the framework-owned ``operation`` key.
    """
    return {k: v for k, v in options.items() if k != OPERATION}


def _connector_options(options: Mapping[str, str]) -> dict:
    """Options passed to ``LakeflowConnect.__init__`` and to read-side calls
    (``read_table``, ``get_table_schema``, ``read_table_metadata``).

    Strips ``operation`` plus the agent-reserved keys so the connector sees
    only its own option namespace.
    """
    return {k: v for k, v in options.items() if k not in _AGENT_RESERVED_KEYS}


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


_LIST_OBJECTS_BASE_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("full_path", StringType(), True),
    ]
)

_GET_OBJECT_METADATA_BASE_SCHEMA = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

_VALIDATE_CONNECTION_BASE_SCHEMA = StructType([])

_LIST_OPERATIONS_BASE_SCHEMA = StructType(
    [
        StructField("name", StringType(), False),
        StructField("description", StringType(), True),
        StructField("kind", StringType(), True),
        StructField("result_schema_json", StringType(), True),
        StructField("parameters_json", StringType(), True),
    ]
)


def _meta(
    status: str = "ok",
    code: Optional[str] = None,
    message: Optional[str] = None,
) -> dict:
    return {"status": status, "code": code, "message": message}


def _error_str(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# Built-in AgentOperation subclasses.
#
# Each fixes ``name``, ``description``, ``kind``, ``schema`` and ``pull``.
# ``pull`` parses the request options into typed kwargs and delegates to
# ``produce`` — the single override point for sources that want a richer
# implementation. ``produce`` is the only method a source customising a
# built-in should override.
# ---------------------------------------------------------------------------

class ListObjectsOp(AgentOperation):
    """Built-in: hierarchical listing of objects under an optional parent."""

    name = OP_LIST_OBJECTS
    description = (
        "Hierarchical listing of objects. Returns rows of (name, type, full_path). "
        "`type` is source-defined; conventional values are `schema` / `table` / "
        "`view` / `synonym`."
    )
    kind = KIND_METADATA
    schema = _LIST_OBJECTS_BASE_SCHEMA
    parameters = (
        Parameter(
            name=PATH,
            description="Parent path to list under. Empty / missing = top level. "
            "Format is source-defined.",
        ),
        Parameter(
            name=SEARCH,
            description="Regex filter applied to result names.",
        ),
    )

    def pull(self, connector, options):
        path = options.get(PATH) or None
        search = options.get(SEARCH) or None
        return self.produce(connector, path=path, search=search)

    def produce(
        self,
        connector: LakeflowConnect,
        *,
        path: Optional[str],
        search: Optional[str],
    ) -> Iterable[Mapping[str, Any]]:
        """Yield rows of ``(name, type, full_path)``.

        Default flat listing derived from ``list_tables``. Override to
        return hierarchical results or to bind to a source-native
        listing API.
        """
        return _default_list_objects(connector, path, search)


class GetObjectMetadataOp(AgentOperation):
    """Built-in: per-object metadata as ``(key, value)`` rows.

    Conventional keys (sources are free to add more, but using these
    for the shared concerns keeps results consistent across sources):

    - ``primary_key`` — comma-joined ordered column list of the PK.
    - ``table_size`` — source-reported size in bytes (string-encoded).
    - ``index_columns`` — JSON array of ordered column lists, one per
      index, e.g. ``[["id"], ["first_name", "last_name"]]``.

    The framework applies the ``metadataKey`` filter case-insensitively
    after ``produce`` emits, so sources don't need to handle it.
    """

    name = OP_GET_OBJECT_METADATA
    description = "Per-object metadata as (key, value) rows."
    kind = KIND_METADATA
    schema = _GET_OBJECT_METADATA_BASE_SCHEMA
    parameters = (
        Parameter(name=NAME, required=True, description="Object name to describe."),
        Parameter(
            name=PATH,
            description="Parent path of the target object (source-defined).",
        ),
        Parameter(
            name=METADATA_KEY,
            description=(
                "Case-insensitive filter; if set, only rows whose `key` "
                "matches are returned."
            ),
        ),
    )

    def pull(self, connector, options):
        name = options[NAME]  # framework validated required.
        path = options.get(PATH) or None
        rows = self.produce(
            connector,
            path=path,
            name=name,
            table_options=_connector_options(options),
        )
        key_filter = options.get(METADATA_KEY)
        if key_filter:
            needle = key_filter.lower()
            rows = (r for r in rows if str(r.get("key", "")).lower() == needle)
        return rows

    def produce(
        self,
        connector: LakeflowConnect,
        *,
        path: Optional[str],
        name: str,
        table_options: Mapping[str, str],
    ) -> Iterable[Mapping[str, Any]]:
        """Yield ``(key, value)`` rows describing the named object.

        Default flattens ``read_table_metadata`` and maps connector
        keys to the standard ingestion-agent vocabulary
        (``primary_key`` from ``primary_keys``, ``cursor_column`` from
        ``cursor_field``).
        """
        del path
        return _default_get_object_metadata(
            connector,
            table_name=name,
            table_options=table_options,
        )


class ReadTableOp(AgentOperation):
    """Built-in: read a tabular object in its native schema.

    Data-kind — rows pass through with the table's natural schema (no
    ``_meta`` column) and errors surface as Spark exceptions. The
    operation does not enforce a row cap of its own; callers wanting
    a sample chain ``.limit(N)`` on the resulting DataFrame.

    Override :meth:`produce` to wire to a source-native read path.
    """

    name = OP_READ_TABLE
    description = "Read a tabular object. Returns the table's natural schema and rows."
    kind = KIND_DATA
    parameters = (
        Parameter(name=TABLE_NAME, required=True, description="Target object name."),
        Parameter(
            name=SCHEMA_NAME,
            description="Schema within the parent path (source-defined).",
        ),
        Parameter(
            name=CATALOG_NAME,
            description=(
                "Top-level catalog (source-defined). Two-level sources should "
                "reject this rather than silently dropping it."
            ),
        ),
    )

    def resolve_schema(self, connector, options):
        table_name = options[TABLE_NAME]
        return connector.get_table_schema(table_name, _connector_options(options))

    def pull(self, connector, options):
        table_name = options[TABLE_NAME]
        return self.produce(
            connector,
            table_name=table_name,
            table_options=_connector_options(options),
        )

    def produce(
        self,
        connector: LakeflowConnect,
        *,
        table_name: str,
        table_options: Mapping[str, str],
    ) -> Iterable[Mapping[str, Any]]:
        """Yield records from ``table_name``.

        Default reads through ``LakeflowConnect.read_table``. Override
        to bind to a source-native scan.
        """
        return _default_read_records(connector, table_name, table_options)


class ValidateConnectionOp(AgentOperation):
    """Built-in: connection-level health check.

    Returns one row with the standard ``_meta`` struct. Override
    :meth:`produce` to use a source-native ping; the default attempts
    ``connector.list_tables()`` and reports any raised exception.

    Sources reporting a failed health check should raise
    ``AgentError(ErrorCode.CONNECTION_FAILED, ...)`` rather than the
    generic ``internal_error`` so consumers can dispatch on a dedicated
    code.
    """

    name = OP_VALIDATE_CONNECTION
    description = (
        "Connection-level health check. Returns one row with _meta only."
    )
    kind = KIND_METADATA
    schema = _VALIDATE_CONNECTION_BASE_SCHEMA

    def pull(self, connector, options):
        del options
        return [{"_meta": _normalize_meta(self.produce(connector))}]

    def produce(self, connector: LakeflowConnect) -> Mapping[str, Optional[str]]:
        """Return ``{status, code, message}`` for the connection.

        Default: try ``list_tables``; report exceptions as
        ``connection_failed``.
        """
        try:
            connector.list_tables()
        except Exception as exc:  # pylint: disable=broad-except
            return _meta(
                status="error",
                code=ErrorCode.CONNECTION_FAILED,
                message=_error_str(exc),
            )
        return _meta(status="ok")


class ListOperationsOp(AgentOperation):
    """Built-in: list operations supported by this connection.

    Framework-owned. Runs even when the connector failed to build —
    returns the framework's built-in catalog, minus any source-defined
    operations the connector would have contributed. Each row carries
    the schema and parameter metadata an agent needs to plan a call.
    """

    name = OP_LIST_OPERATIONS
    description = (
        "List operations supported by this connection, with their "
        "parameter declarations and result schemas."
    )
    kind = KIND_METADATA
    schema = _LIST_OPERATIONS_BASE_SCHEMA

    def pull(self, connector, options):
        del options
        for op in _resolve_operation_catalog(connector).values():
            yield {
                "name": op.name,
                "description": op.description,
                "kind": op.kind,
                "result_schema_json": op.schema.json() if op.schema is not None else None,
                "parameters_json": _parameters_to_json(getattr(op, "parameters", ())),
            }


# Ordered map of built-in operations. Source-defined operations with the same
# name replace these (see :func:`_resolve_operation_catalog`).
_BUILTIN_OPERATIONS: dict[str, AgentOperation] = {
    op.name: op
    for op in (
        ListObjectsOp(),
        ReadTableOp(),
        GetObjectMetadataOp(),
        ValidateConnectionOp(),
        ListOperationsOp(),
    )
}


# ---------------------------------------------------------------------------
# Dispatcher (plain class, not a DataSource)
# ---------------------------------------------------------------------------

# pylint: disable=invalid-name,missing-function-docstring
class IngestionAgentDispatcher:
    """Dispatches ingestion-agent operations onto a pre-built connector.

    Constructed by :class:`LakeflowSource` when the ``operation`` option
    is set. The connector is built by ``LakeflowSource`` and passed in
    — the dispatcher never instantiates it itself.

    - ``schema()`` resolves the operation and asks it for its schema.
      Metadata-kind ops automatically include a ``_meta`` column;
      data-kind ops return the schema unchanged.
    - ``reader()`` returns an :class:`IngestionAgentReader` that
      applies ``_meta`` defaults and converts errors to a single
      error row (metadata kind) or lets them propagate (data kind).

    Init-error policy: when the connector failed to build, ``connector``
    is ``None`` and ``init_error`` holds the exception. The reader uses
    the operation's :attr:`AgentOperation.requires_connector` flag —
    ops that don't need a connector (e.g. ``list_operations``) still
    run; metadata-kind ops that do need one emit an error row;
    data-kind ops re-raise.
    """

    def __init__(
        self,
        options: Mapping[str, str],
        connector: Optional[LakeflowConnect],
        init_error: Optional[BaseException] = None,
    ) -> None:
        self.options = dict(options)
        self.connector = connector
        self.init_error = init_error
        self.operation_name = self.options.get(OPERATION)
        if not self.operation_name:
            raise ValueError(
                f"ingestion-agent dispatch requires an '{OPERATION}' option."
            )
        self.operation = _resolve_operation(connector, self.operation_name)

    def schema(self) -> StructType:
        if self.operation is None:
            raise ValueError(
                f"Unknown ingestion-agent operation: {self.operation_name}"
            )
        # If the connector failed to init and the op needs it, we can't
        # safely call resolve_schema (it may dereference the connector).
        # Fall back to the op's static `schema` attribute and rely on the
        # reader to emit an error row. Data-kind ops in this state can't
        # produce a valid schema at all, so we raise the init error.
        if self.init_error is not None and _requires_connector(self.operation):
            if self.operation.kind == KIND_DATA:
                raise self.init_error
            base = self.operation.schema or StructType([])
        else:
            # Data-kind ops compute their schema from request options
            # (e.g. preview_table needs `tableName`). Validate required
            # parameters now so resolve_schema doesn't trip over missing
            # keys. Metadata-kind ops use a static schema; their parameter
            # validation runs at read time and surfaces as an error row.
            if self.operation.kind == KIND_DATA:
                _validate_required_parameters(self.operation, self.options)
            base = self.operation.resolve_schema(self.connector, self.options)
        if self.operation.kind == KIND_METADATA:
            # Metadata-kind ops emit a single error row carrying only _meta
            # when pull() raises. Relax data-column nullability so that
            # error row validates against the schema.
            return _ensure_meta_field(_relax_nullability(base))
        return base

    def reader(self, schema: StructType) -> "IngestionAgentReader":
        return IngestionAgentReader(
            options=self.options,
            schema=schema,
            operation=self.operation,
            operation_name=self.operation_name,
            connector=self.connector,
            init_error=self.init_error,
        )


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class IngestionAgentReader(DataSourceReader):
    def __init__(
        self,
        options: Mapping[str, str],
        schema: StructType,
        operation: Optional[AgentOperation],
        operation_name: str,
        connector: Optional[LakeflowConnect],
        init_error: Optional[BaseException],
    ) -> None:
        self.options = dict(options)
        self.schema = schema
        self.operation = operation
        self.operation_name = operation_name
        self.connector = connector
        self.init_error = init_error

    def partitions(self):
        # All ingestion-agent operations are small, single-partition reads.
        return [InputPartition(None)]

    def read(self, _partition):
        if self.operation is None:
            yield from self._yield_error_rows(
                ValueError(
                    f"Unknown ingestion-agent operation: {self.operation_name}"
                )
            )
            return

        # If the connector failed and this op needs it, route by kind:
        # metadata-kind → error row, data-kind → re-raise. validate_connection
        # treats the init failure as the health-check answer and uses the
        # canonical CONNECTION_FAILED code.
        if self.init_error is not None and _requires_connector(self.operation):
            if self.operation.kind == KIND_DATA:
                raise self.init_error
            if self.operation.name == OP_VALIDATE_CONNECTION:
                yield from self._yield_error_rows(
                    self.init_error, code=ErrorCode.CONNECTION_FAILED
                )
            else:
                yield from self._yield_error_rows(self.init_error)
            return

        request_options = _agent_options(self.options)

        if self.operation.kind == KIND_DATA:
            # Data-kind ops surface exceptions as Spark exceptions and don't
            # carry a _meta column.
            for record in self.operation.pull(self.connector, request_options):
                yield parse_value(record, self.schema)
            return

        # Metadata-kind: framework wraps pull() exceptions into a single
        # error row and setdefaults _meta on every row to {status: ok}.
        try:
            _validate_required_parameters(self.operation, request_options)
            rows = self.operation.pull(self.connector, request_options)
            # Materialise so generator-time errors are caught and converted to
            # a single error row instead of escaping mid-iteration. Acceptable
            # because all metadata ops produce small results.
            rows = list(rows) if not isinstance(rows, list) else rows
        except Exception as exc:  # pylint: disable=broad-except
            rows = [self._error_row(exc)]
        for row in rows:
            yield parse_value(_with_default_meta(row), self.schema)

    def _yield_error_rows(
        self, exc: BaseException, code: Optional[str] = None
    ):
        yield parse_value(
            _with_default_meta(self._error_row(exc, code=code)), self.schema
        )

    def _error_row(
        self, exc: BaseException, code: Optional[str] = None
    ) -> dict:
        if code is None:
            if isinstance(exc, AgentError):
                code = exc.code
            elif isinstance(exc, ValueError):
                # Framework-detected input errors (legacy code paths still raise
                # ValueError for missing options).
                code = ErrorCode.BAD_REQUEST
            else:
                code = ErrorCode.INTERNAL_ERROR
        if isinstance(exc, (AgentError, ValueError)):
            message = str(exc)
        else:
            message = _error_str(exc)
        return {"_meta": _meta(status="error", code=code, message=message)}


# ---------------------------------------------------------------------------
# Operation catalog
# ---------------------------------------------------------------------------

def _source_operations(
    connector: Optional[LakeflowConnect],
) -> Mapping[str, AgentOperation]:
    if not isinstance(connector, SupportsIngestionAgent):
        return {}
    try:
        ops = connector.agent_operations()
    except Exception:  # pylint: disable=broad-except
        return {}
    if not ops:
        return {}
    out: dict[str, AgentOperation] = {}
    for op_name, op in ops.items():
        if not isinstance(op, AgentOperation):
            raise TypeError(
                f"agent_operations()[{op_name!r}] must be an AgentOperation "
                f"instance, got {type(op).__name__}."
            )
        out[op_name] = op
    return out


def _resolve_operation_catalog(
    connector: Optional[LakeflowConnect],
) -> Mapping[str, AgentOperation]:
    """Built-ins plus source-defined operations.

    Source-defined entries replace built-ins with the same name. Ordering:
    built-ins first (canonical order), then any new source-defined
    operations (sorted by name for determinism).
    """
    source_ops = _source_operations(connector)
    catalog: dict[str, AgentOperation] = {}
    for name, op in _BUILTIN_OPERATIONS.items():
        catalog[name] = source_ops.get(name, op)
    extras = {n: o for n, o in source_ops.items() if n not in _BUILTIN_OPERATIONS}
    for name in sorted(extras):
        catalog[name] = extras[name]
    return catalog


def _resolve_operation(
    connector: Optional[LakeflowConnect], operation_name: str
) -> Optional[AgentOperation]:
    return _resolve_operation_catalog(connector).get(operation_name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _required_option(
    options: Mapping[str, str], key: str, operation: str
) -> str:
    value = options.get(key)
    if not value:
        raise AgentError(
            ErrorCode.BAD_REQUEST,
            f"Operation '{operation}' requires option '{key}'.",
        )
    return value


def _validate_required_parameters(
    op: AgentOperation, options: Mapping[str, str]
) -> None:
    """Reject calls missing a required :class:`Parameter`.

    Raises :class:`AgentError` with ``bad_request`` when a required
    parameter is absent. Called by the dispatcher before
    ``resolve_schema`` (data-kind) and before ``pull`` (metadata-kind),
    so ops can rely on declared required inputs being present.
    """
    for param in getattr(op, "parameters", ()):
        if param.required and not options.get(param.name):
            raise AgentError(
                ErrorCode.BAD_REQUEST,
                f"Operation '{op.name}' requires option '{param.name}'.",
            )


def _parameters_to_json(params: Iterable[Parameter]) -> str:
    """JSON-serialise a tuple of :class:`Parameter` declarations.

    Surfaced by ``list_operations.parameters_json`` so agents can plan
    calls. Fields with ``None`` default / enum are kept in the JSON for
    a stable schema.
    """
    return json.dumps(
        [
            {
                "name": p.name,
                "type": p.type,
                "description": p.description,
                "required": p.required,
                "default": p.default,
                "enum": list(p.enum) if p.enum else None,
            }
            for p in params
        ]
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
        f"validate_connection produce() must return a mapping, "
        f"got {type(value).__name__}."
    )


def _str_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _ensure_meta_field(schema: StructType) -> StructType:
    if any(f.name == "_meta" for f in schema.fields):
        return schema
    return StructType(list(schema.fields) + [_meta_field(nullable=True)])


def _relax_nullability(schema: StructType) -> StructType:
    """Return a copy of ``schema`` with every non-``_meta`` field nullable.

    Used for metadata-kind operations so the framework's error row (which
    carries only ``_meta``) parses cleanly even when the operation
    declared its data columns as non-nullable.
    """
    return StructType(
        [
            StructField(f.name, f.dataType, True, f.metadata)
            if f.name != "_meta"
            else f
            for f in schema.fields
        ]
    )


# ---------------------------------------------------------------------------
# Default implementations of the built-in operations
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
) -> Iterator[dict]:
    """Flatten ``read_table_metadata`` into ``(key, value)`` rows.

    Maps the connector's metadata keys to the standardised ingestion-agent
    keys (``primary_key`` from ``primary_keys``, ``cursor_column`` from
    ``cursor_field``) and preserves the others verbatim. The framework
    applies the ``metadataKey`` filter case-insensitively above this.
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

    return ({"key": k, "value": _to_str_value(v)} for k, v in standardized)


def _default_read_records(
    connector: LakeflowConnect,
    table_name: str,
    table_options: Mapping[str, str],
) -> Iterator[Mapping[str, Any]]:
    """Pull all records from ``connector.read_table`` and yield them."""
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
