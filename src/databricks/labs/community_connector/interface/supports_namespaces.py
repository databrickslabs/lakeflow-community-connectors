from abc import ABC, abstractmethod


class SupportsNamespaces(ABC):
    """Mixin for connectors whose tables live under hierarchical namespaces.

    A namespace is a path of zero or more string segments (e.g. ``["org",
    "repo"]`` for GitHub, ``["tenant", "project"]`` for Azure DevOps).
    Connectors with a flat catalog do not need this mixin — the framework
    falls back to :meth:`LakeflowConnect.list_tables` and reports each table
    with an empty namespace.

    Must be used together with :class:`LakeflowConnect`.

    Usage::

        class MyConnector(LakeflowConnect, SupportsNamespaces):
            ...
    """

    @abstractmethod
    def list_namespaces(
        self,
        prefix: list[str] | None = None,
    ) -> list[list[str]]:
        """Return the immediate child namespaces under ``prefix``.

        This method returns one level of children only. Callers that need to
        walk the full tree do so by recursing on each returned child. An
        empty return value means ``prefix`` has no further namespace children
        — the caller is expected to enumerate tables there via
        :meth:`list_tables_in_namespaces`.

        Args:
            prefix: A namespace path under which to list children. ``None`` or
                an empty list lists the root-level namespaces.
        Returns:
            A list of full namespace paths (each path includes the prefix).
        """

    @abstractmethod
    def list_tables_in_namespaces(
        self,
        namespaces: list[list[str]] | None = None,
    ) -> list[tuple[list[str], str]]:
        """Return ``(namespace, table_name)`` pairs for the given namespaces.

        Args:
            namespaces: A list of namespace paths to enumerate tables for.
                ``None`` means "list tables across all namespaces the
                connector can enumerate". An empty list means "no namespaces"
                and must return an empty result.
        Returns:
            A list of ``(namespace, table_name)`` tuples. ``namespace`` is the
            full path (a list of strings); ``table_name`` is the table
            identifier within that namespace.
        """
