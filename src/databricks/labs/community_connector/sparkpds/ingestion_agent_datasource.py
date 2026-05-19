"""Ingestion-agent operation dispatcher for the ``lakeflow_connect`` format.

Used by :class:`LakeflowSource` when callers set the ``operation``
option. The agent operation surface lives under the same
``lakeflow_connect`` format as regular table reads — there is no
separate format string.

Operations are first-class :class:`AgentOperation` objects. Five
built-ins (``list_objects``, ``preview_table``, ``get_object_metadata``,
``validate_connection``, ``list_operations``) ship with the framework
and derive their behaviour from the existing :class:`LakeflowConnect`
surface — every connector gains the agent surface automatically.

Source customisation is exclusively through
:meth:`SupportsIngestionAgent.agent_operations`:

- **To customise a built-in**, subclass the relevant built-in class
  (e.g. :class:`ListObjectsOp`) and override ``produce`` — not
  ``pull``. The framework owns dispatch, schema, kind, and name; the
  ``produce`` override hook receives typed keyword arguments.

- **To add a new source-prefixed operation**, subclass
  :class:`AgentOperation` directly and implement ``pull``.

The classes :class:`IngestionAgentSource` / :class:`IngestionAgentReader`
are exposed for unit testing; they are not registered as a separate
Spark format.
"""

from __future__ import annotations

import itertools
import json
import re
from typing import Any, Iterable, Iterator, Mapping, Optional, Tuple

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

from databricks.labs.community_connector.interface import (
    AgentOperation,
    LakeflowConnect,
    SupportsIngestionAgent,
)
from databricks.labs.community_connector.libs.utils import parse_value


# =============================================================================
# Merge-script placeholder. Same shape as ``lakeflow_datasource.py``. The merge
# script does not currently process this module, so the placeholder is only a
# safety net for direct ``IngestionAgentSource`` instantiation; the registry
# wrapper overrides ``_build_connector`` to bind to the concrete LakeflowConnect
# class at registration time.
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

# Built-in operation names exposed for callers and tests.
OP_LIST_OBJECTS = "list_objects"
OP_PREVIEW_TABLE = "preview_table"
OP_GET_OBJECT_METADATA = "get_object_metadata"
OP_VALIDATE_CONNECTION = "validate_connection"
OP_LIST_OPERATIONS = "list_operations"

# Operation kinds.
KIND_METADATA = "metadata"
KIND_DATA = "data"


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
# Each built-in fixes ``name``, ``description``, ``kind``, ``schema`` and
# ``pull``. ``pull`` parses the request options into typed kwargs and
# delegates to ``produce``, which is the override hook for sources that
# want a richer implementation. ``produce`` is the only method a source
# customising a built-in should override — everything else is part of the
# contract callers rely on.
# ---------------------------------------------------------------------------

class ListObjectsOp(AgentOperation):
    """Built-in: hierarchical listing of objects under an optional parent.

    Subclass and override :meth:`produce` to return a source-native or
    hierarchical listing. Do not override ``pull``, ``schema``, ``kind``,
    or ``name`` — those are the framework contract.
    """

    name = OP_LIST_OBJECTS
    description = (
        "Hierarchical listing of objects. "
        "Optional: parent (path to list under), search (regex on names). "
        "Returns rows of (name, type, full_path)."
    )
    kind = KIND_METADATA
    schema = _LIST_OBJECTS_BASE_SCHEMA

    def pull(self, connector, options):
        parent = options.get(PARENT) or None
        search = options.get(SEARCH) or None
        return self.produce(connector, parent=parent, search=search)

    def produce(
        self,
        connector: LakeflowConnect,
        *,
        parent: Optional[str],
        search: Optional[str],
    ) -> Iterable[Mapping[str, Any]]:
        """Yield rows of ``(name, type, full_path)``.

        Default flat listing derived from ``list_tables``. Override to
        return hierarchical results or to bind to a source-native
        listing API.

        Args:
            connector: The :class:`LakeflowConnect` instance.
            parent: Path identifying the parent to list under. ``None``
                lists from the source root.
            search: Regex filter applied to names; ``None`` for no filter.

        Yields:
            Row dicts with keys ``name``, ``type``, ``full_path`` (and
            optionally ``_meta`` for per-row warnings).
        """
        return _default_list_objects(connector, parent, search)


class GetObjectMetadataOp(AgentOperation):
    """Built-in: per-object metadata as ``(key, value)`` rows."""

    name = OP_GET_OBJECT_METADATA
    description = (
        f"Per-object metadata as (key, value) rows. "
        f"Required: {NAME}. Optional: {PATH}, {METADATA_KEY}."
    )
    kind = KIND_METADATA
    schema = _GET_OBJECT_METADATA_BASE_SCHEMA

    def pull(self, connector, options):
        name = _required_option(options, NAME, OP_GET_OBJECT_METADATA)
        path = options.get(PATH) or None
        metadata_key = options.get(METADATA_KEY) or None
        return self.produce(
            connector,
            path=path,
            name=name,
            metadata_key=metadata_key,
            table_options=_table_options(options),
        )

    def produce(
        self,
        connector: LakeflowConnect,
        *,
        path: Optional[str],
        name: str,
        metadata_key: Optional[str],
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
            metadata_key=metadata_key,
        )


class PreviewTableOp(AgentOperation):
    """Built-in: sample-read a tabular object.

    Data-kind operation — rows pass through with the table's natural
    schema (no ``_meta`` column) and errors surface as Spark exceptions.
    Override :meth:`produce` to wire to a source-native sampling API
    that's cheaper than reading + slicing.
    """

    name = OP_PREVIEW_TABLE
    description = (
        f"Sample a tabular object. "
        f"Required: {TABLE_NAME}. "
        f"Optional: {CATALOG_NAME}, {SCHEMA_NAME}, {LIMIT} "
        f"(default {DEFAULT_PREVIEW_LIMIT}). "
        "Returns the table's natural schema and rows."
    )
    kind = KIND_DATA

    def resolve_schema(self, connector, options):
        table_name = _required_option(options, TABLE_NAME, OP_PREVIEW_TABLE)
        return connector.get_table_schema(table_name, _table_options(options))

    def pull(self, connector, options):
        table_name = _required_option(options, TABLE_NAME, OP_PREVIEW_TABLE)
        limit = _int_option(options, LIMIT, DEFAULT_PREVIEW_LIMIT)
        table_options = _table_options(options)
        records = self.produce(
            connector,
            table_name=table_name,
            limit=limit,
            table_options=table_options,
        )
        # Framework-side cap. Defends against an override that ignores `limit`.
        return itertools.islice(records, limit)

    def produce(
        self,
        connector: LakeflowConnect,
        *,
        table_name: str,
        limit: int,
        table_options: Mapping[str, str],
    ) -> Iterable[Mapping[str, Any]]:
        """Yield up to ``limit`` records from ``table_name``.

        Default consumes ``read_table`` and lets the framework slice.
        Override to use a source-native LIMIT clause when available.

        Args:
            connector: The :class:`LakeflowConnect` instance.
            table_name: Table to sample.
            limit: Maximum number of records the framework expects.
            table_options: Connector-level options for the read
                (reserved agent keys already stripped).
        """
        del limit  # Framework slices; default doesn't need it.
        return _default_preview_records(connector, table_name, table_options)


class ValidateConnectionOp(AgentOperation):
    """Built-in: connection-level health check.

    Returns one row with the standard ``_meta`` struct. Override
    :meth:`produce` to use a source-native ping; the default attempts
    ``connector.list_tables()`` and reports any raised exception.
    """

    name = OP_VALIDATE_CONNECTION
    description = (
        "Connection-level health check. Returns one row with _meta only."
    )
    kind = KIND_METADATA
    schema = _VALIDATE_CONNECTION_BASE_SCHEMA

    def pull(self, connector, options):
        del options
        result = self.produce(connector)
        return [{"_meta": _normalize_meta(result)}]

    def produce(
        self,
        connector: LakeflowConnect,
    ) -> Mapping[str, Optional[str]]:
        """Return ``{status, code, message}`` for the connection.

        Default: try ``list_tables``; report exceptions as ``error``.
        """
        try:
            connector.list_tables()
        except Exception as exc:  # pylint: disable=broad-except
            return _meta(
                status="error",
                code=type(exc).__name__,
                message=_error_str(exc),
            )
        return _meta(status="ok")


class ListOperationsOp(AgentOperation):
    """Built-in: list operations supported by this connection.

    Framework-owned. There is normally no reason for a source to
    override this — the catalog is computed from the framework's
    built-ins plus the source's ``agent_operations`` map.
    """

    name = OP_LIST_OPERATIONS
    description = "List operations supported by this connection."
    kind = KIND_METADATA
    schema = _LIST_OPERATIONS_BASE_SCHEMA

    def pull(self, connector, options):
        del options
        for op in _resolve_operation_catalog(connector).values():
            yield {"name": op.name, "description": op.description}


# Ordered map of built-in operations. Source-defined operations with the same
# name replace these (see :func:`_resolve_operation_catalog`).
_BUILTIN_OPERATIONS: dict[str, AgentOperation] = {
    op.name: op
    for op in (
        ListObjectsOp(),
        PreviewTableOp(),
        GetObjectMetadataOp(),
        ValidateConnectionOp(),
        ListOperationsOp(),
    )
}


# ---------------------------------------------------------------------------
# DataSource — internal dispatcher reached via LakeflowSource. Not a
# registered Spark format on its own.
# ---------------------------------------------------------------------------

# pylint: disable=invalid-name,missing-function-docstring
class IngestionAgentSource(DataSource):
    """Internal dispatcher for the ingestion-agent operation surface.

    Constructed by :class:`LakeflowSource` when the ``operation`` option
    is set. Lifecycle:

    - ``__init__`` validates the ``operation`` option and tries to build
      the underlying :class:`LakeflowConnect`. Init failure is tolerated
      for ``validate_connection`` (it becomes the error row reported
      back to the agent) and ``list_operations`` (we still return the
      built-in catalog). For every other operation, init errors
      propagate as Spark exceptions.
    - ``schema()`` resolves the operation and asks it for its schema.
      Metadata-kind ops automatically include a ``_meta`` column;
      data-kind ops return the schema unchanged.
    - ``reader()`` returns an :class:`IngestionAgentReader` that
      dispatches to the operation on read, applying ``_meta`` defaults
      and converting errors to a single error row (metadata kind) or
      letting them propagate (data kind).
    """

    def __init__(self, options: Mapping[str, str]) -> None:
        self.options = dict(options)
        self.operation_name = self.options.get(OPERATION)
        if not self.operation_name:
            raise ValueError(
                f"ingestion-agent dispatch requires an '{OPERATION}' option."
            )
        self._connector_init_error: Optional[BaseException] = None
        self.lakeflow_connect: Optional[LakeflowConnect] = None
        try:
            self.lakeflow_connect = self._build_connector()
        except Exception as exc:  # pylint: disable=broad-except
            # validate_connection and list_operations tolerate init failure —
            # they convert it into structured output. Every other operation
            # genuinely needs the connector, so propagate.
            if self.operation_name not in (
                OP_VALIDATE_CONNECTION,
                OP_LIST_OPERATIONS,
            ):
                raise
            self._connector_init_error = exc
        self.operation = _resolve_operation(
            self.lakeflow_connect, self.operation_name
        )

    def _build_connector(self) -> LakeflowConnect:
        # Both the merge script and the registry wrapper retarget this;
        # see the placeholder at the top of the module.
        return LakeflowConnectImpl(  # pylint: disable=abstract-class-instantiated
            _connector_options(self.options)
        )

    def schema(self):
        if self.operation is None:
            raise ValueError(
                f"Unknown ingestion-agent operation: {self.operation_name}"
            )
        base = self.operation.resolve_schema(self.lakeflow_connect, self.options)
        if self.operation.kind == KIND_METADATA:
            # Metadata-kind ops emit a single error row carrying only _meta
            # when pull() raises. Relax data-column nullability so that
            # error row validates against the schema.
            return _ensure_meta_field(_relax_nullability(base))
        return base

    def reader(self, schema: StructType):
        return IngestionAgentReader(
            options=self.options,
            schema=schema,
            operation=self.operation,
            operation_name=self.operation_name,
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
        operation: Optional[AgentOperation],
        operation_name: str,
        lakeflow_connect: Optional[LakeflowConnect],
        connector_init_error: Optional[BaseException],
    ) -> None:
        self.options = dict(options)
        self.schema = schema
        self.operation = operation
        self.operation_name = operation_name
        self.lakeflow_connect = lakeflow_connect
        self.connector_init_error = connector_init_error

    def partitions(self):
        # All ingestion-agent operations are small, single-partition reads.
        return [InputPartition(None)]

    def read(self, _partition):
        if self.operation is None:
            # Unknown operation — surface as a metadata error row.
            yield from self._yield_error_rows(
                ValueError(
                    f"Unknown ingestion-agent operation: {self.operation_name}"
                )
            )
            return

        kind = self.operation.kind

        # Special-case: validate_connection on a failed connector init.
        # The op can't run without a connector, so we emit its result directly.
        if (
            self.connector_init_error is not None
            and self.operation.name == OP_VALIDATE_CONNECTION
        ):
            yield from self._yield_error_rows(self.connector_init_error)
            return

        if kind == KIND_DATA:
            # Data-kind ops surface exceptions as Spark exceptions and don't
            # carry a _meta column.
            request_options = _request_options(self.options)
            for record in self.operation.pull(
                self.lakeflow_connect, request_options
            ):
                yield parse_value(record, self.schema)
            return

        # Metadata-kind: framework wraps pull() exceptions into a single
        # error row and setdefaults _meta on every row to {status: ok}.
        try:
            rows = self.operation.pull(
                self.lakeflow_connect, _request_options(self.options)
            )
            # Materialize so generator-time errors are caught and converted to
            # a single error row instead of escaping mid-iteration. Acceptable
            # because all metadata ops produce small results.
            rows = list(rows) if not isinstance(rows, list) else rows
        except Exception as exc:  # pylint: disable=broad-except
            rows = [self._error_row(exc)]
        for row in rows:
            yield parse_value(_with_default_meta(row), self.schema)

    def _yield_error_rows(self, exc: BaseException):
        yield parse_value(_with_default_meta(self._error_row(exc)), self.schema)

    def _error_row(self, exc: BaseException) -> dict:
        return {
            "_meta": _meta(
                status="error",
                code=type(exc).__name__,
                message=_error_str(exc),
            )
        }


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


def _request_options(options: Mapping[str, str]) -> dict:
    """Options passed to ``AgentOperation.pull`` / ``resolve_schema``.

    We drop the framework-only ``operation`` key but keep everything else,
    including credentials, so source-defined ops can read whatever they need.
    """
    return {k: v for k, v in options.items() if k != OPERATION}


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
) -> Iterator[Mapping[str, Any]]:
    """Pull records from ``read_table``; the caller slices to the limit."""
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
