"""Mixin + AgentOperation for the ingestion-agent Spark source.

Three ergonomically distinct ways to customise, in order of weight:

1. **Decorate a method** with :func:`agent_operation`. The fastest
   path for the typical case: a stateless operation with a static
   schema. The framework auto-discovers decorated methods — no
   ``agent_operations()`` override needed::

       class MyConnector(LakeflowConnect, SupportsIngestionAgent):
           @agent_operation(
               name="salesforce.describe_sobject",
               description="Describe an SObject's fields. Required: sobject.",
               schema=StructType([
                   StructField("field", StringType(), False),
                   StructField("type", StringType(), True),
                   StructField("custom", BooleanType(), True),
               ]),
           )
           def describe_sobject(self, options):
               for f in self.client.describe(options["sobject"]):
                   yield {"field": f["name"], "type": f["type"],
                          "custom": f["custom"]}

2. **Subclass :class:`AgentOperation`** and register the instance via
   :meth:`agent_operations`. Use this when the op has its own state or
   needs a dynamic ``resolve_schema`` (e.g. a schema that depends on a
   target table the request names). The class still slots into the
   same dispatch path — no framework edits.

3. **Override the five per-method hooks** on
   :class:`SupportsIngestionAgent` (``list_objects``,
   ``get_object_metadata``, ``preview_table``,
   ``validate_connection``) when you just want a richer version of an
   existing built-in operation. Return ``NotImplemented`` to fall back
   to the framework's default behaviour derived from
   :class:`LakeflowConnect`.

The framework owns ``_meta`` and error containment for
``kind="metadata"`` operations and lists every operation through
``list_operations`` automatically.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

from pyspark.sql.types import StructType


_DECORATOR_ATTR = "__agent_operation_spec__"


@dataclass(frozen=True)
class _AgentOperationSpec:
    name: str
    description: str
    schema: Optional[StructType]
    kind: str


def agent_operation(
    *,
    name: str,
    description: str = "",
    schema: Optional[StructType] = None,
    kind: str = "metadata",
) -> Callable[[Callable], Callable]:
    """Decorate a method to expose it as an :class:`AgentOperation`.

    The decorated method takes ``options`` (the request options dict
    with the reserved ``operation`` key stripped) and returns rows.
    The connector instance is bound as ``self`` — call any other
    ``LakeflowConnect`` method on it normally.

    Example::

        class MyConnector(LakeflowConnect, SupportsIngestionAgent):
            @agent_operation(
                name="example.row_count",
                description="Return number of rows for a table.",
                schema=StructType([StructField("count", LongType(), False)]),
            )
            def row_count(self, options):
                yield {"count": self.count(options["tableName"])}

    The framework auto-discovers every decorated method on a
    :class:`SupportsIngestionAgent` subclass. To register a class-based
    :class:`AgentOperation` alongside, override
    :meth:`SupportsIngestionAgent.agent_operations` and merge with
    ``super().agent_operations()``.

    Args:
        name: Operation name. Source-prefix it (e.g.
            ``"salesforce.describe_sobject"``) to keep the operation
            namespace unambiguous.
        description: One-line text shown by ``list_operations``;
            agent planners read this to decide when to call the op.
        schema: Static result schema. For ``kind="metadata"``,
            ``_meta`` is appended automatically. Leave ``None`` and
            use a subclass-style :class:`AgentOperation` with
            ``resolve_schema`` when the schema must be computed.
        kind: ``"metadata"`` (framework appends ``_meta`` + converts
            ``pull`` errors to a single error row) or ``"data"``
            (rows pass through unchanged; errors surface as Spark
            exceptions).
    """

    def wrap(fn):
        spec = _AgentOperationSpec(
            name=name, description=description, schema=schema, kind=kind
        )
        setattr(fn, _DECORATOR_ATTR, spec)
        return fn

    return wrap


class AgentOperation(ABC):
    """A single ingestion-agent operation.

    Subclass and implement :meth:`pull`. Set :attr:`name` (the value
    callers pass as ``operation``), :attr:`description` (shown in
    ``list_operations``), :attr:`kind` (``"metadata"`` to auto-append
    ``_meta`` and convert errors into a row; ``"data"`` to pass the
    rows through unchanged), and either :attr:`schema` (a class-level
    :class:`StructType`) or override :meth:`resolve_schema` for
    dynamic schemas.
    """

    #: Operation name. Use snake_case; prefix with the source name for
    #: source-specific operations (e.g. ``"salesforce.describe_sobject"``).
    name: str = ""

    #: One-paragraph human-readable description. Surfaced by
    #: ``list_operations`` and read by the ingestion agent's planner.
    description: str = ""

    #: ``"metadata"`` (framework adds ``_meta`` + converts errors into
    #: a row) or ``"data"`` (rows pass through unchanged; errors
    #: surface as Spark exceptions).
    kind: str = "metadata"

    #: Static schema. Leave unset and override :meth:`resolve_schema`
    #: for schemas that depend on the connector or request options.
    schema: Optional[StructType] = None

    def resolve_schema(
        self,
        connector: Any,
        options: Mapping[str, str],
    ) -> StructType:
        """Return the result schema. Defaults to :attr:`schema`.

        Override when the schema depends on the connector or request
        options — for example a JDBC-style ``describe`` whose columns
        come from the target system.
        """
        del connector, options
        if self.schema is None:
            raise NotImplementedError(
                f"AgentOperation '{self.name}' must set a class-level "
                f"`schema` or override `resolve_schema()`."
            )
        return self.schema

    @abstractmethod
    def pull(
        self,
        connector: Any,
        options: Mapping[str, str],
    ) -> Iterable[Mapping[str, Any]]:
        """Yield rows for the result.

        Args:
            connector: The :class:`LakeflowConnect` instance for this
                request. Use it to talk to the source.
            options: Request options (excluding the reserved
                ``operation`` key).

        Yields:
            Rows as mappings (dicts). For ``kind="metadata"`` ops the
            framework setdefaults ``_meta`` to ``ok`` if absent; a row
            may set its own ``_meta`` to report a per-row warning.
        """


class SupportsIngestionAgent:
    """Optional mixin for connectors that customise ingestion-agent ops.

    All hooks are optional. The mixin is purely a marker for the
    framework — implementing it signals "I provide at least one
    customisation."

    See the module docstring for the two extension paths.
    """

    # ------------------------------------------------------------------
    # Path 1: per-method overrides for the built-in five operations.
    # Return NotImplemented to fall back to the default behaviour
    # derived from LakeflowConnect.
    # ------------------------------------------------------------------

    def list_objects(
        self,
        parent: Optional[str] = None,
        search: Optional[str] = None,
    ) -> Iterable[Mapping[str, str]]:
        """Yield rows of ``(name, type, full_path)``.

        ``type`` is one of ``catalog`` / ``schema`` / ``table`` /
        ``view`` / ``folder`` / ``file``. Rows may include a ``_meta``
        mapping for per-row warnings.

        Return ``NotImplemented`` (the sentinel) to fall back to the
        default flat listing derived from ``list_tables``.
        """
        del parent, search
        return NotImplemented

    def get_object_metadata(
        self,
        path: Optional[str],
        name: str,
        metadata_key: Optional[str] = None,
    ) -> Iterable[Mapping[str, str]]:
        """Yield ``(key, value)`` rows describing the object.

        Return ``NotImplemented`` to fall back to the default
        flattening of ``read_table_metadata``.
        """
        del path, name, metadata_key
        return NotImplemented

    def preview_table(
        self,
        table_name: str,
        limit: int,
        table_options: Mapping[str, str],
    ) -> Iterable[Mapping[str, object]]:
        """Yield up to ``limit`` records from ``table_name``.

        Return ``NotImplemented`` to fall back to ``read_table``
        truncated client-side.
        """
        del table_name, limit, table_options
        return NotImplemented

    def validate_connection(self) -> Mapping[str, Optional[str]]:
        """Return ``{status, code, message}`` for the connection.

        Return ``NotImplemented`` to fall back to calling
        ``list_tables`` and reporting any raised exception.
        """
        return NotImplemented

    # ------------------------------------------------------------------
    # Path 2: contribute / replace whole operations as first-class
    # objects. This is the path for adding *new* source-specific
    # operations.
    # ------------------------------------------------------------------

    def agent_operations(self) -> Mapping[str, AgentOperation]:
        """Return ``{name: AgentOperation}`` to plug in to the source.

        The default implementation auto-discovers every
        :func:`agent_operation`-decorated method on the class and wraps
        each one in an :class:`AgentOperation`. Override and call
        ``super().agent_operations()`` to also register class-based
        :class:`AgentOperation` instances::

            def agent_operations(self):
                ops = dict(super().agent_operations())
                ops[StatefulOp.name] = StatefulOp(self.client)
                return ops

        Each returned operation is dispatched by ``ingestion_agent``
        when callers pass its ``name`` as the ``operation`` option.
        Entries whose name matches a built-in (``list_objects``,
        ``preview_table``, …) take precedence over the built-in.

        Source-prefix new operations (e.g.
        ``"salesforce.describe_sobject"``) to keep the namespace
        unambiguous.
        """
        return _discover_decorated_operations(self)


def _discover_decorated_operations(
    owner: SupportsIngestionAgent,
) -> Mapping[str, AgentOperation]:
    """Build AgentOperation instances from @agent_operation methods on ``owner``.

    Walks ``type(owner)``'s attributes, collects every callable carrying an
    ``__agent_operation_spec__`` annotation, and returns one
    :class:`AgentOperation` per spec, with ``pull`` bound to ``owner``.
    """
    ops: dict[str, AgentOperation] = {}
    for attr_name in dir(type(owner)):
        attr = getattr(type(owner), attr_name, None)
        spec: Optional[_AgentOperationSpec] = getattr(attr, _DECORATOR_ATTR, None)
        if spec is None:
            continue
        ops[spec.name] = _DecoratedAgentOperation(owner=owner, method=attr, spec=spec)
    return ops


class _DecoratedAgentOperation(AgentOperation):
    """AgentOperation backed by an @agent_operation-decorated method.

    ``pull(connector, options)`` ignores ``connector`` — the source instance
    is captured as ``self.owner``, so the decorated method just receives
    ``self`` and ``options`` like any normal method.
    """

    def __init__(
        self,
        owner: SupportsIngestionAgent,
        method: Callable,
        spec: _AgentOperationSpec,
    ) -> None:
        self.owner = owner
        self._method = method
        self.name = spec.name
        self.description = spec.description
        self.kind = spec.kind
        self.schema = spec.schema

    def pull(
        self,
        connector: Any,
        options: Mapping[str, str],
    ) -> Iterable[Mapping[str, Any]]:
        del connector  # Source instance is bound through ``self.owner``.
        return self._method(self.owner, options)
