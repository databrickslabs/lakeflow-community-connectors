"""Mixin + AgentOperation for the ingestion-agent operation surface.

Two extension paths, picked depending on what the source needs:

1. **Subclass :class:`AgentOperation`** and register the instance via
   :meth:`SupportsIngestionAgent.agent_operations`. This is how every
   source-specific operation is added. One class + one map entry::

       class DescribeSObjectOp(AgentOperation):
           name = "salesforce.describe_sobject"
           description = "Describe an SObject's fields. Required: sobject."
           kind = "metadata"
           schema = StructType([
               StructField("field", StringType(), False),
               StructField("type", StringType(), True),
               StructField("custom", BooleanType(), True),
           ])

           def pull(self, connector, options):
               for f in connector.client.describe(options["sobject"]):
                   yield {"field": f["name"], "type": f["type"],
                          "custom": f["custom"]}

       class SalesforceConnector(LakeflowConnect, SupportsIngestionAgent):
           def agent_operations(self):
               return {DescribeSObjectOp.name: DescribeSObjectOp()}

2. **Override the four per-method hooks** on
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
from typing import Any, Iterable, Mapping, Optional

from pyspark.sql.types import StructType


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
    # Path 1: per-method overrides for the built-in four operations.
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

        Each returned operation is dispatched when callers pass its
        ``name`` as the ``operation`` option. Entries whose name
        matches a built-in (``list_objects``, ``preview_table``, …)
        take precedence over the built-in for this source.

        Source-prefix new operations (e.g.
        ``"salesforce.describe_sobject"``) to keep the namespace
        unambiguous.
        """
        return {}
