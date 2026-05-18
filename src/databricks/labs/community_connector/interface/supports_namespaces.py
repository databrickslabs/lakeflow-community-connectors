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
        namespace: list[str],
    ) -> list[str]:
        """Return the table names that live directly under ``namespace``.

        Callers that want every table across the whole catalog walk the
        namespace tree via :meth:`list_namespaces` and call this method
        once per leaf — there is no "list everything" shortcut.

        Args:
            namespace: The namespace path. An empty list ``[]`` selects
                root-level tables (those that live outside any namespace).
        Returns:
            A list of table names. The full ``(namespace, table_name)``
            row exposed on ``_community_tables`` is reconstructed by the
            framework from the namespace the caller already supplied, so
            the connector does not need to echo it back.
        """
