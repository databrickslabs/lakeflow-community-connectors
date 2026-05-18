from abc import ABC, abstractmethod


class SupportsNamespaces(ABC):
    """Mixin for connectors whose tables live under hierarchical namespaces.

    A namespace is a path of zero or more string segments (e.g. ``["org",
    "repo"]`` for GitHub, ``["tenant", "project"]`` for Azure DevOps).
    Connectors with a flat catalog do not need this mixin — the framework
    falls back to :meth:`LakeflowConnect.list_tables` and reports each table
    with an empty namespace.

    Output ordering on the ``_community_namespaces`` and ``_community_tables``
    Spark virtual tables is normalized by the framework via ``sorted(...)``,
    so connector implementations of :meth:`list_namespaces` and
    :meth:`list_tables_in_namespace` are free to return their results in
    any order (including from a :class:`set` or generator).

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

        This method returns one level of *namespace* children only — it
        never returns tables. A namespace can hold tables and child
        namespaces independently; callers must always call
        :meth:`list_tables_in_namespace` for every namespace they care
        about, regardless of whether :meth:`list_namespaces` returned
        children for it. An empty return value just means there are no
        further child namespaces under ``prefix``.

        Walk the full tree by recursing on each returned child.

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
