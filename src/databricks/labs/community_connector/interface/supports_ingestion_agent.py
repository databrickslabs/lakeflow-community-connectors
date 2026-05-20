"""AgentOperation + SupportsIngestionAgent mixin.

Operations the ingestion agent dispatches through the
``lakeflow_connect`` Spark source format. Every connector gets five
built-ins for free; sources contribute new operations or replace
built-ins via :meth:`SupportsIngestionAgent.agent_operations`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, Optional, Tuple

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.agent_protocol import Parameter


_VALID_KINDS = frozenset({"metadata", "data"})


class AgentOperation(ABC):
    """One operation the ingestion agent can dispatch.

    Set :attr:`name`, :attr:`description`, :attr:`kind`, and either
    :attr:`schema` or :meth:`resolve_schema`; implement :meth:`pull`.

    ``kind = "metadata"`` auto-appends a ``_meta`` column and converts
    ``pull`` exceptions into a single error row. ``kind = "data"``
    passes rows through unchanged and lets exceptions propagate.
    """

    name: str = ""
    description: str = ""
    kind: str = "metadata"
    schema: Optional[StructType] = None
    #: Whether the operation needs a live :class:`LakeflowConnect`
    #: instance. Defaults to ``True``. Set ``False`` on ops that should
    #: still run when the connector failed to construct
    #: (e.g. discovery-style ops like ``list_operations``).
    requires_connector: bool = True
    #: Typed declaration of accepted input options. Exposed via
    #: ``list_operations.parameters_json`` so agents can plan calls.
    #: The framework validates required parameters before invoking
    #: ``pull``; undeclared options pass through untouched.
    parameters: Tuple[Parameter, ...] = ()
    #: Operation version. Bump on breaking changes to the op's input or
    #: output contract. Surfaced through ``list_operations.version``.
    version: str = "1.0.0"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, "__abstractmethods__", None):
            return
        if not isinstance(cls.name, str) or not cls.name:
            raise TypeError(f"{cls.__name__} must set a non-empty `name`.")
        if cls.kind not in _VALID_KINDS:
            raise TypeError(
                f"{cls.__name__}.kind must be one of "
                f"{sorted(_VALID_KINDS)}, got {cls.kind!r}."
            )

    def resolve_schema(
        self, connector: Any, options: Mapping[str, str]
    ) -> StructType:
        """Return the result schema. Override for option-dependent schemas."""
        del connector, options
        if self.schema is None:
            raise NotImplementedError(
                f"{type(self).__name__} must set `schema` or override "
                f"`resolve_schema`."
            )
        return self.schema

    @abstractmethod
    def pull(
        self, connector: Any, options: Mapping[str, str]
    ) -> Iterable[Mapping[str, Any]]:
        """Yield result rows as dicts."""


class SupportsIngestionAgent:
    """Connector mixin: contribute :class:`AgentOperation` instances."""

    def agent_operations(self) -> Mapping[str, AgentOperation]:
        """Return ``{name: AgentOperation}``.

        Entries whose name matches a built-in replace the framework
        default for this connector.
        """
        return {}
