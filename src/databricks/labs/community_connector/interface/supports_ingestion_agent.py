"""Optional mixin for richer ingestion-agent responses.

The ``ingestion_agent`` Spark source format works for every
:class:`LakeflowConnect` automatically — defaults derive from
``list_tables``, ``read_table_metadata``, ``get_table_schema`` and
``read_table``.

Implement this mixin to provide:

- hierarchical listing (catalogs / schemas / folders), not just a flat
  table list;
- metadata keys beyond the standard
  ``primary_key`` / ``cursor_column`` / ``ingestion_type`` set;
- a cheaper or source-native sample read for ``preview_table``;
- a source-native connection health check;
- additional, source-prefixed operations exposed through the same
  format (e.g. ``salesforce.describe_sobject``).

All methods are optional; the defaults in
:mod:`databricks.labs.community_connector.sparkpds.ingestion_agent_datasource`
kick in when an override is not provided.
"""

from __future__ import annotations

from typing import Callable, Iterable, Mapping, Optional


class SupportsIngestionAgent:
    """Optional mixin for connectors that customise ingestion-agent operations.

    Usage::

        class MyConnector(LakeflowConnect, SupportsIngestionAgent):
            def list_objects(self, parent=None, search=None):
                ...
    """

    def list_objects(
        self,
        parent: Optional[str] = None,
        search: Optional[str] = None,
    ) -> Iterable[Mapping[str, str]]:
        """Yield rows describing objects under ``parent``.

        Each row is a mapping with the keys ``name``, ``type``, and
        ``full_path``. ``type`` is one of ``catalog``, ``schema``,
        ``table``, ``view``, ``folder``, ``file``. May also include a
        ``_meta`` mapping (``status`` / ``code`` / ``message``) on rows
        the source could not introspect.

        Args:
            parent: Path of the parent object (e.g. ``my_catalog`` or
                ``/folder/sub``). ``None`` lists from the source root.
            search: Regex filter applied to result names.

        Returning ``NotImplemented`` (the sentinel, not ``raise``) lets
        the default flat ``list_tables``-based implementation handle the
        call.
        """
        return NotImplemented

    def get_object_metadata(
        self,
        path: Optional[str],
        name: str,
        metadata_key: Optional[str] = None,
    ) -> Iterable[Mapping[str, str]]:
        """Yield ``(key, value)`` rows of metadata for an object.

        Suggested keys: ``primary_key``, ``cursor_column``,
        ``ingestion_type``, ``table_size``, ``partition_columns``,
        ``last_modified_at``, ``size_bytes``, ``mime_type``,
        ``format_hint``.

        Args:
            path: Path of the parent (catalog/schema/folder), or
                ``None`` for sources without a hierarchy.
            name: Object name within ``path``.
            metadata_key: If set, yield only this single key.

        Returning ``NotImplemented`` falls back to flattening
        ``read_table_metadata`` into rows.
        """
        return NotImplemented

    def preview_table(
        self,
        table_name: str,
        limit: int,
        table_options: Mapping[str, str],
    ) -> Iterable[Mapping[str, object]]:
        """Yield up to ``limit`` records from ``table_name``.

        Implement this when the source can sample more efficiently than
        a full ``read_table`` call (e.g. via a server-side LIMIT
        clause).

        Returning ``NotImplemented`` falls back to consuming
        ``read_table`` and truncating client-side.
        """
        return NotImplemented

    def validate_connection(self) -> Mapping[str, Optional[str]]:
        """Run a connection-level health check.

        Return a mapping with ``status`` (``ok`` / ``warning`` /
        ``error``), and optional ``code`` and ``message``.

        Returning ``NotImplemented`` falls back to calling
        ``list_tables`` and reporting any raised exception.
        """
        return NotImplemented

    def custom_operations(self) -> Mapping[str, Callable]:
        """Return source-prefixed operation handlers.

        Keys are operation names (snake_case, prefixed with the source
        name to avoid collisions, e.g. ``salesforce.describe_sobject``).
        Each handler receives the request options dict (minus
        ``operation``) and returns either:

        - an iterable of dict rows (when the operation has a static
          schema declared via ``custom_operation_schemas``); or
        - a tuple ``(schema, rows_iterable)`` for dynamically schema'd
          operations.

        Operation descriptions used by ``list_operations`` come from
        the handler's docstring.

        The default returns an empty mapping.
        """
        return {}
