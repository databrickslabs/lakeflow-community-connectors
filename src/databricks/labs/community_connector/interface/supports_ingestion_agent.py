"""AgentOperation + SupportsIngestionAgent mixin.

The ingestion-agent surface dispatches read-only operations through the
``lakeflow_connect`` Spark Python Data Source format. Every connector
gains five built-in operations automatically (``list_objects``,
``preview_table``, ``get_object_metadata``, ``validate_connection``,
``list_operations``), with defaults derived from the existing
:class:`LakeflowConnect` surface.

Sources customise the agent surface in two ways, both routed through
:meth:`SupportsIngestionAgent.agent_operations`:

1. **Subclass a built-in operation to swap in a richer implementation.**
   The built-in operation classes live in
   :mod:`databricks.labs.community_connector.sparkpds.ingestion_agent_datasource`.
   Each one exposes a ``produce`` override hook with typed keyword
   arguments; the framework owns ``pull``, the schema, the kind, and
   the operation's name — those are the contract every connector must
   honour::

       from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import ListObjectsOp

       class PostgresListObjectsOp(ListObjectsOp):
           def produce(self, connector, *, parent, search):
               if parent is None:
                   return [{"name": db, "type": "catalog", "full_path": db}
                           for db in connector.list_databases()]
               ...

       class PostgresConnector(LakeflowConnect, SupportsIngestionAgent):
           def agent_operations(self):
               return {ListObjectsOp.name: PostgresListObjectsOp()}

2. **Add a brand-new source-specific operation.** Subclass
   :class:`AgentOperation` directly and register the instance with a
   source-prefixed name::

       class DescribeSObjectOp(AgentOperation):
           name = "salesforce.describe_sobject"
           description = "Describe an SObject's fields. Required: sobject."
           kind = "metadata"
           schema = StructType([...])

           def pull(self, connector, options):
               for f in connector.client.describe(options["sobject"]):
                   yield {"field": f["name"], ...}

       class SalesforceConnector(LakeflowConnect, SupportsIngestionAgent):
           def agent_operations(self):
               return {DescribeSObjectOp.name: DescribeSObjectOp()}

The framework owns ``_meta`` and error containment for
``kind="metadata"`` operations and lists every operation through
``list_operations`` automatically.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, Optional

from pyspark.sql.types import StructType


_VALID_KINDS = frozenset({"metadata", "data"})


class AgentOperation(ABC):
    """A single ingestion-agent operation.

    Subclass and implement :meth:`pull`. Set :attr:`name` (the value
    callers pass as ``operation``), :attr:`description` (shown by
    ``list_operations``), :attr:`kind` (``"metadata"`` to auto-append
    ``_meta`` and convert errors into a row; ``"data"`` to pass rows
    through unchanged), and either :attr:`schema` (a class-level
    :class:`StructType`) or override :meth:`resolve_schema` for
    schemas that depend on the connector or request options.

    Built-in operations expose a separate ``produce`` override hook
    instead — see the module docstring.
    """

    #: Operation name. Use snake_case; prefix with the source name for
    #: source-specific operations (e.g. ``"salesforce.describe_sobject"``).
    name: str = ""

    #: One-line human-readable description. Surfaced by
    #: ``list_operations`` and read by the ingestion agent's planner.
    description: str = ""

    #: ``"metadata"`` (framework adds ``_meta`` + converts errors into
    #: a row) or ``"data"`` (rows pass through unchanged; errors
    #: surface as Spark exceptions).
    kind: str = "metadata"

    #: Static schema. Leave unset and override :meth:`resolve_schema`
    #: for schemas that depend on the connector or request options.
    schema: Optional[StructType] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Skip abstract intermediate subclasses; they may legitimately
        # leave attributes for concrete classes to fill in.
        if getattr(cls, "__abstractmethods__", None):
            return
        if not isinstance(cls.name, str) or not cls.name:
            raise TypeError(
                f"AgentOperation subclass {cls.__name__} must set a "
                f"non-empty class-level `name`."
            )
        if cls.kind not in _VALID_KINDS:
            raise TypeError(
                f"AgentOperation subclass {cls.__name__} has kind="
                f"{cls.kind!r}; must be one of {sorted(_VALID_KINDS)}."
            )

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
            options: Request options (the reserved ``operation`` key
                is stripped before the framework passes the dict in).

        Yields:
            Rows as mappings (dicts). For ``kind="metadata"`` ops the
            framework setdefaults ``_meta`` to ``ok`` if absent; a row
            may set its own ``_meta`` to report a per-row warning.
        """


class SupportsIngestionAgent:
    """Optional mixin for connectors that customise ingestion-agent ops.

    Implementing the mixin is a signal to the framework that the
    connector contributes its own :class:`AgentOperation` instances via
    :meth:`agent_operations`. The mixin has no abstract methods — every
    built-in operation has a working default derived from
    :class:`LakeflowConnect`, so a connector that needs no
    customisation does not need to implement this mixin at all.

    See the module docstring for the two extension patterns
    (subclassing a built-in, or adding a new source-prefixed op).
    """

    def agent_operations(self) -> Mapping[str, AgentOperation]:
        """Return ``{name: AgentOperation}`` to plug in to the source.

        Each returned operation is dispatched when callers pass its
        ``name`` as the ``operation`` option. Entries whose name
        matches a built-in (``list_objects``, ``preview_table``, …)
        replace the framework default *for this connector*; the rest
        of the catalog (other built-ins + the source's other entries)
        is unaffected.

        When replacing a built-in, subclass the framework's
        ``<Builtin>Op`` class (which fixes the schema and dispatch
        contract) and override ``produce`` — not ``pull``. Source-
        prefix new operations (e.g.
        ``"salesforce.describe_sobject"``) to keep the namespace
        unambiguous.
        """
        return {}
