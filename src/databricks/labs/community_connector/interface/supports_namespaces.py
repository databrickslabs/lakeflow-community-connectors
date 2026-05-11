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
        :meth:`list_tables_in_namespace`.

        Args:
            prefix: A namespace path under which to list children. ``None`` or
                an empty list lists the root-level namespaces.
        Returns:
            A list of full namespace paths (each path includes the prefix).
        """

    @abstractmethod
    def list_tables_in_namespace(
        self,
        namespace: list[str] | None = None,
    ) -> list[tuple[list[str], str]]:
        """Return ``(namespace, table_name)`` pairs for one namespace path.

        Args:
            namespace: The namespace path to enumerate tables for.
                - ``None`` means "list tables across every namespace the
                  connector can enumerate".
                - An empty list ``[]`` means "list tables at the root"
                  (tables that live outside any namespace).
                - A non-empty list lists tables under exactly that one
                  namespace path.
        Returns:
            A list of ``(namespace, table_name)`` tuples. ``namespace`` is the
            full path (a list of strings); ``table_name`` is the table
            identifier within that namespace.
        """
