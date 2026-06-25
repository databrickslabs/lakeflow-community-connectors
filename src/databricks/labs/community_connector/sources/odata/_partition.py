"""Partitioned-read support for the OData connector.

``PartitionMixin`` implements the ``SupportsPartitionedStream``
interface: it discovers the top-level entity set's primary keys for a
contained table, bin-packs them across a fixed number of partitions,
and exposes a ``read_partition`` that walks only the assigned subset.
Spark distributes partitions across executors; each executor runs the
existing serial chain-walk inside its own partition.

Activation policy (``is_partitioned``):

* Contained paths only (depth >= 2). Flat tables aren't usefully
  partitionable without prior knowledge of the keyspace distribution.
* ``expand_contained=false`` (the N+1 model). With
  ``expand_contained=true`` the whole table is one HTTP — no fan-out
  to parallelise.
* Delta-tracking is off. The server-driven delta link is stateful
  and can't be split across executors.
* For *streaming* reads (``latest_offset`` path), additionally the
  cursor must live on the top-level entity (level 0). At other
  cursor levels there's no cheap way to fence the micro-batch
  upfront — without a fence Spark would re-emit rows on every
  trigger. Other configurations fall back to ``simpleStreamReader``,
  which preserves the existing serial offset semantics.

Per-call shape:

* Batch reads land via ``LakeflowBatchReader``, which calls
  ``get_partitions(table, options)`` (no offsets). For contained
  snapshot reads, this yields ``num_partitions`` descriptors each
  holding a slice of top-level parents.
* Streaming reads land via ``LakeflowPartitionedStreamReader`` when
  ``is_partitioned`` is True; ``get_partitions(table, options, start,
  end)`` carries the cursor bounds into the descriptor so each
  executor filters ``cursor gt start_cursor AND cursor le
  end_cursor``. ``latest_offset`` probes the top-level entity for the
  current max cursor — one extra HTTP per micro-batch.

Each partition descriptor is JSON-serialisable (primitive keys
only).
"""

from typing import Iterator, Sequence

from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.odata._contained import (
    _ancestor_pk_order_by,
    combine_filters,
    parse_contained_path,
    resolve_segment_filters,
)


_DEFAULT_NUM_PARTITIONS = 4
_OPT_NUM_PARTITIONS = "num_partitions"


class PartitionMixin(SupportsPartitionedStream):
    """Mixes ``SupportsPartitionedStream`` into ``ODataLakeflowConnect``.

    Only methods specific to partitioning live here. Discovery of
    top-level parent keys, leaf walking, and FK tagging are delegated
    back into the rest of the connector via duck-typed ``self.*``
    calls (same pattern as ``ContainedNavMixin``).
    """

    # ------------------------------------------------------------------
    # SupportsPartitionedStream interface
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        """Opt this table into the partitioned read path.

        Streaming + partitioning has a stricter precondition than batch
        + partitioning because the connector has to fence each micro-
        batch's cursor window upfront in ``latest_offset`` — there's no
        way to communicate "max cursor observed" back from executors.
        For batch reads any contained N+1 path is partitionable; for
        streaming reads we additionally require the cursor to live on
        the top-level entity so a single probe can compute the fence.

        The framework calls this without table_options, so we read
        them from ``self.options`` — safe because each
        ``LakeflowSource`` instance carries one table's options.
        """
        if parse_contained_path(table_name) is None:
            return False
        opts = getattr(self, "options", {}) or {}
        if opts.get("expand_contained", "false").strip().lower() == "true":
            return False
        if self._delta_setting(opts) != "disabled":
            return False
        # If a cursor is set, it must live at the top level for the
        # streaming fence probe to make sense. Snapshot reads (no
        # cursor_field) clear this trivially.
        cursor_field = opts.get("cursor_field")
        if cursor_field:
            segments = parse_contained_path(table_name) or [table_name]
            namespace = opts.get("namespace")
            if self._find_cursor_level(segments, namespace, cursor_field) != 0:
                return False
        return True

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Probe the top-level entity for the current max cursor value.

        Each micro-batch reads ``(start_cursor, end_cursor]`` — the
        ``end`` returned here becomes ``start`` of the next batch when
        Spark commits, so cursor progression is monotonic.

        For snapshot streams (no ``cursor_field``) the offset is a
        wall-clock epoch — there's no source-side notion of "what's
        new" without a cursor, so each trigger reads a full snapshot
        and Spark commits the new epoch.
        """
        opts = table_options or {}
        cursor_field = opts.get("cursor_field")
        if not cursor_field:
            return {"snapshot_id": _wall_clock_ns()}
        segments = parse_contained_path(table_name) or [table_name]
        namespace = opts.get("namespace")
        max_cursor = self._probe_top_level_max_cursor(segments[0], namespace, cursor_field)
        prior = (start_offset or {}).get("cursor")
        if max_cursor is None:
            # Empty top set or all-null cursor column. Keep the prior
            # value so Spark sees no progress and skips the batch.
            return {"cursor": prior} if prior is not None else {}
        return {"cursor": max_cursor}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Return partition descriptors covering this read.

        Two invocation shapes (PySpark dispatches both into the same
        method):

        * ``get_partitions(table, options)`` — batch path. Returns
          partitions over the full top-level set.
        * ``get_partitions(table, options, start, end)`` — streaming
          path. Returns partitions filtered to the cursor window
          ``(start.cursor, end.cursor]``.

        For tables ``is_partitioned`` rejects, the framework still
        invokes this on the batch path; in that case we hand back a
        single empty descriptor so ``read_partition`` falls through
        to the existing serial ``read_table`` semantics.
        """
        if parse_contained_path(table_name) is None:
            # Flat table — let the existing serial path handle it.
            return [{}]
        opts = table_options or {}
        if opts.get("expand_contained", "false").strip().lower() == "true":
            return [{}]
        if self._delta_setting(opts) != "disabled":
            return [{}]
        if start_offset == end_offset and start_offset is not None:
            # Streaming: no new data — no work to partition.
            return []
        segments = parse_contained_path(table_name) or [table_name]
        namespace = opts.get("namespace")
        cursor_field = opts.get("cursor_field")
        # ``cursor_lower`` is "what we've already read up to" — used
        # by read_partition as ``cursor gt cursor_lower``. ``end`` is
        # the previously-probed fence; we stamp it onto each row's
        # cursor column so the next batch's ``cursor_lower`` matches.
        cursor_lower = (start_offset or {}).get("cursor")
        top_rows = self._discover_top_parent_rows(
            segments, namespace, opts, cursor_field, cursor_lower
        )
        if not top_rows:
            return []
        num_partitions = max(1, int(opts.get(_OPT_NUM_PARTITIONS, _DEFAULT_NUM_PARTITIONS)))
        return _bin_pack(top_rows, num_partitions, cursor_lower)

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Walk one partition's slice of top-level parents.

        An empty descriptor ``{}`` (returned by ``get_partitions`` for
        unsupported configurations) falls back to the existing serial
        ``read_table`` — same shape as the simple-reader path. With a
        ``top_parent_rows`` key, the chain enumeration starts from
        that subset instead of fetching the whole level-0 set.
        """
        opts = table_options or {}
        if not partition or "top_parent_rows" not in partition:
            # Single-partition fallback: defer to read_table which
            # returns (iter, offset). Drop the offset; partitioned
            # mode commits offsets via latest_offset, not per-read.
            records, _ = self.read_table(table_name, None, opts)
            return records
        segments = parse_contained_path(table_name) or [table_name]
        cursor_field = opts.get("cursor_field")
        top_parent_rows = partition["top_parent_rows"]
        cursor_lower = partition.get("cursor_lower")
        return self._iter_partition_rows(
            segments, opts, top_parent_rows, cursor_field, cursor_lower
        )

    # ------------------------------------------------------------------
    # Per-table helpers (called by the methods above)
    # ------------------------------------------------------------------

    def _probe_top_level_max_cursor(
        self,
        top_set: str,
        namespace: str | None,
        cursor_field: str,
    ):
        """One HTTP probe: ``$top=1&$orderby=<cursor> desc`` → max value.

        Returns ``None`` when the top set is empty or every row has a
        null cursor. The caller decides whether that means "no new
        data" or "first call against an empty source."
        """
        # Use the existing URL builder + page-fetch plumbing so OAuth,
        # extra_headers, retries, etc. all carry through unchanged.
        et = self._entity_type_for(top_set, namespace)
        own_fields = self._own_fields_for_et(et)
        if not any(f.name == cursor_field for f in own_fields):
            return None
        opts = {
            "page_size": "1",
            "select": cursor_field,
        }
        url = self._build_url(top_set, opts, order_by=f"{cursor_field} desc")
        for row in self._fetch_pages(url):
            value = row.get(cursor_field)
            if value is not None:
                return value
        return None

    def _discover_top_parent_rows(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str],
        cursor_field: str | None,
        cursor_lower,
    ) -> list[dict]:
        """Fetch the rows of the top-level entity set that bound this
        read. Returns level-0 PK dicts (plus the cursor column when a
        cursor is present, so executors can stamp rows without re-
        fetching). Apply the cursor filter at the top set when the
        cursor lives at level 0; otherwise no filter is applied here
        and per-leaf filtering picks up the slack in the executor."""
        top_set = segments[0]
        ancestor_et = self._entity_type_for(top_set, namespace)
        ancestor_pks = self._own_primary_keys_for_et(ancestor_et)
        select_cols = list(ancestor_pks)
        cursor_extra: str | None = None
        # Default to PK-only ordering so server skiptoken pagination
        # is stable when there's no cursor at the top set (or the
        # cursor lives deeper). See ``_ancestor_pk_order_by`` for the
        # skiptoken-safety argument.
        order_by: str | None = _ancestor_pk_order_by(ancestor_pks)
        if cursor_field:
            own_fields = self._own_fields_for_et(ancestor_et)
            if any(f.name == cursor_field for f in own_fields):
                if cursor_field not in select_cols:
                    select_cols.append(cursor_field)
                if cursor_lower is not None:
                    cursor_extra = self._cursor_filter(cursor_field, cursor_lower)
                terms = [f"{cursor_field} asc"]
                terms.extend(f"{pk} asc" for pk in ancestor_pks if pk != cursor_field)
                order_by = ",".join(terms)
        # AND the level-0 segment filter (``filter_at_<top>``) with any
        # cursor filter. Without this the partition pre-fetch returns
        # every parent, then per-partition leaf fetches issue one
        # request per parent — even though the user explicitly told us
        # which parents to walk. The leaf filter then matches inside
        # every unfiltered parent, surfacing rows that should have been
        # excluded by the top filter.
        segment_filters = resolve_segment_filters(table_options, segments)
        extra_filter = combine_filters(cursor_extra, segment_filters.get(0))
        opts = {
            "page_size": table_options.get("page_size", "1000"),
            "select": ",".join(select_cols),
        }
        url = self._build_url(top_set, opts, extra_filter=extra_filter, order_by=order_by)
        return list(self._fetch_pages(url))

    def _iter_partition_rows(
        self,
        segments: list[str],
        table_options: dict[str, str],
        top_parent_rows: list[dict],
        cursor_field: str | None,
        cursor_lower,
    ) -> Iterator[dict]:
        """Stream leaf rows for one partition's slice of parents.

        Dispatch mirrors ``read_table``'s contained branches: cursor on
        a non-leaf ancestor → ancestor-cursor walk with stamped cursor
        values; cursor on the leaf → per-leaf cursor filter; no
        cursor → full snapshot per chain.
        """
        namespace = (table_options or {}).get("namespace")
        fk_columns = self._resolve_fk_columns(segments, namespace)
        segment_filters = resolve_segment_filters(table_options, segments)
        leaf_seg_filter = segment_filters.get(len(segments) - 1)
        leaf_order_by = self._leaf_pk_order_by(segments, namespace)
        if not cursor_field:
            for chain in self._iter_parent_key_chains(
                segments, namespace, table_options, top_parent_rows=top_parent_rows
            ):
                url = self._build_contained_url(
                    segments,
                    chain,
                    table_options,
                    extra_filter=leaf_seg_filter,
                    order_by=leaf_order_by,
                )
                for row in self._fetch_pages(url):
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    yield row
            return
        cursor_level = self._find_cursor_level(segments, namespace, cursor_field)
        if cursor_level == -1:
            raise ValueError(
                f"cursor_field {cursor_field!r} is not a property on "
                f"the contained path or any of its ancestors."
            )
        # Partition activation requires cursor at level 0 for the
        # streaming probe to make sense; this branch is the only one
        # reached in practice. We still go through the with-cursor
        # iterator so the cursor column is stamped onto leaf rows.
        chains_iter = self._iter_parent_chains_with_cursor(
            segments,
            namespace,
            table_options,
            cursor_level,
            cursor_field,
            cursor_lower,
            top_parent_rows=top_parent_rows,
        )
        for chain, ancestor_cursor in chains_iter:
            url = self._build_contained_url(
                segments, chain, table_options, extra_filter=leaf_seg_filter, order_by=leaf_order_by
            )
            for row in self._fetch_pages(url):
                self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                if cursor_level == len(segments) - 1:
                    # Leaf-cursor mode: filter per row by ``cursor gt
                    # cursor_lower``. (Server-side filter would be
                    # cheaper, but partition activation gates this to
                    # cursor_level==0; this branch exists for
                    # completeness only.)
                    rec = row.get(cursor_field)
                    if cursor_lower is not None and rec is not None and rec <= cursor_lower:
                        continue
                else:
                    row[cursor_field] = ancestor_cursor
                yield row


def _bin_pack(rows: list[dict], num_partitions: int, cursor_lower) -> list[dict]:
    """Split ``rows`` into ``num_partitions`` partition descriptors.

    Round-robin assignment with each partition carrying a contiguous
    slice — keeps cursor ordering stable within a partition (the
    ``_discover_top_parent_rows`` caller sorts by cursor when one is
    set). Empty bins are dropped so the framework doesn't spawn
    no-op executors.
    """
    if not rows:
        return []
    bin_size = max(1, (len(rows) + num_partitions - 1) // num_partitions)
    partitions: list[dict] = []
    for i in range(0, len(rows), bin_size):
        partitions.append(
            {
                "top_parent_rows": rows[i : i + bin_size],
                "cursor_lower": cursor_lower,
            }
        )
    return partitions


def _wall_clock_ns() -> int:
    """Wall-clock nanoseconds for snapshot-stream offset progression.

    Imported lazily here so ``_partition.py`` has no module-level
    import-time work — the connector is forked + re-imported per
    PySpark Python Data Source ``.load()`` call, and import cost is
    on the hot path.
    """
    import time  # pylint: disable=import-outside-toplevel

    return time.time_ns()
