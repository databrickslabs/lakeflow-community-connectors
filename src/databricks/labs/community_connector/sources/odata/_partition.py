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
* ``expand_contained`` resolves to the N+1 model — explicit ``false``,
  or ``auto`` (the default) whose behavioural preflight fell back to
  N+1. With ``expand_contained=true`` (or an ``auto`` preflight that
  verified ``$expand``) the whole table is one HTTP — no fan-out to
  parallelise, so it is not partitioned.
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
  end)`` seeds each descriptor with ``cursor_lower`` (= start's
  cursor, floored by any configured ``cursor_lookback_seconds``) so
  each executor filters ``cursor gt cursor_lower``. There is NO upper
  fence filter: rows landing past ``end`` mid-batch are emitted now
  and re-read next batch — duplicate-safe. ``latest_offset`` probes
  the top-level entity for the current max cursor (over the same
  ``filter_at_<top>``-restricted population the read walks) — one
  extra HTTP per micro-batch.

Each partition descriptor is JSON-serialisable (primitive keys
only).
"""

from typing import Iterator, Sequence

import requests

from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.odata._contained import (
    CONTAINED_PATH_SEP,
    DEFAULT_PAGE_SIZE,
    _ancestor_pk_order_by,
    _is_vanished_error,
    _log_vanished_parent,
    combine_filters,
    resolve_segment_filters,
    validate_page_size,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    DELETED_COL as _DELETED_COL,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    SEQUENCE_COL as _SEQUENCE_COL,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    cursor_at_or_before_for_refilter as _cursor_at_or_before_for_refilter,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    cursor_le as _cursor_le,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    jsonify_complex_values as _jsonify_complex_values,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    max_or as _max_or,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    pad_row_to_fields,
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
        if self._table_segments(table_name) is None:
            return False
        opts = getattr(self, "options", {}) or {}
        # Fail fast at stream setup: a partitionable table never routes
        # through read_table, so its option validation must run here. This
        # includes ``contained_fetch`` — it has no other parse on the
        # partition path (``expand_contained`` is parsed just below), so a
        # typo'd value would otherwise be silently accepted, the one enum
        # still silent where the round-33 dispatch fix made the rest loud.
        validate_page_size(opts)
        _parse_num_partitions(opts)
        self._contained_fetch_batch_size(opts)
        # Reset any per-table shared-cache verdict pinned non-``auto`` here too:
        # a partitionable table streams through the partition path (this →
        # get_partitions → read_partition), never read_table, so without this
        # the reset would never fire for it and a later switch back to ``auto``
        # would reuse a stale verdict. Table-scoped + idempotent (see
        # ``_purge_nonauto_table_verdicts``); a no-op under ``auto``.
        self._purge_nonauto_table_verdicts(table_name, opts)
        if self._expand_contained_mode(opts) == "true":
            return False
        if self._delta_setting(opts) != "disabled":
            return False
        # If a cursor is set, it must live at the top level for the
        # streaming fence probe to make sense. Snapshot reads (no
        # cursor_field) clear this trivially.
        cursor_field = opts.get("cursor_field")
        if cursor_field:
            segments = self._table_segments(table_name) or [table_name]
            namespace = opts.get("namespace")
            if self._find_cursor_level(segments, namespace, cursor_field) != 0:
                return False
        # ``expand_contained=auto`` follows its RESOLVED shape: the preflight
        # verdict decides (single-$expand read → no fan-out to parallelise →
        # not partitioned; N+1 fallback → partitionable). Checked LAST so the
        # probe only runs for tables every cheap gate above already admitted;
        # the instance cache dedupes it across is_partitioned/get_partitions
        # within one setup. A transient preflight failure resolves to the N+1
        # (partitioned) shape for this stream — correct, just parallel.
        return not self._expand_read_active(table_name, opts)

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
        # Honour the user's ``pagination=`` for the fence probe's page walk —
        # get_partitions/read_partition set this at entry too, but this method
        # can run first (or alone) on a freshly-recreated driver instance, and
        # the probe walking under a stale/default mode can misread a
        # link-omitting server's short page as the whole top set.
        self._pagination = self._parse_pagination(opts)
        segments = self._table_segments(table_name) or [table_name]
        namespace = opts.get("namespace")
        max_cursor = self._probe_top_level_max_cursor(segments, namespace, cursor_field, opts)
        prior = (start_offset or {}).get("cursor")
        if max_cursor is None:
            # Empty top set or all-null cursor column. Keep the prior
            # value so Spark sees no progress and skips the batch.
            return {"cursor": prior} if prior is not None else {}
        # Never regress the committed fence (replica lag, deletion of the
        # max row): the docstring's monotonic-progression promise is what
        # lets ``cursor gt fence`` be the sole dedup boundary.
        return {"cursor": _max_or(prior, max_cursor)}

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
        if self._table_segments(table_name) is None:
            # Flat table — let the existing serial path handle it.
            return [{}]
        opts = table_options or {}
        validate_page_size(opts)
        num_partitions = _parse_num_partitions(opts)
        # Validate ``contained_fetch`` here too (not just ``is_partitioned``):
        # the batch reader can reach ``get_partitions`` without a prior
        # ``is_partitioned`` call, and a typo'd value should fail the same way
        # on every partition entry point. Inert on the read itself (partition
        # walks are always plain N+1), but consistency > silence.
        self._contained_fetch_batch_size(opts)
        # Reset any per-table shared-cache verdict pinned non-``auto`` on the
        # partition path too (called every microbatch for a partitioned stream,
        # which never reaches read_table's reset). Table-scoped + idempotent;
        # a no-op under ``auto``.
        self._purge_nonauto_table_verdicts(table_name, opts)
        if self._expand_contained_mode(opts) == "true":
            return [{}]
        if self._delta_setting(opts) != "disabled":
            return [{}]
        if start_offset == end_offset and start_offset is not None:
            # Streaming: no new data — no work to partition.
            return []
        # ``expand_contained=auto`` on the BATCH invocation (no offsets)
        # follows its resolved shape: expand verified → a single empty
        # descriptor defers to the serial ``read_table`` (which re-uses the
        # cached verdict); preflight fail → partitionable N+1 below. The
        # STREAMING invocation never re-probes: ``is_partitioned`` already
        # resolved the shape at stream setup, and a divergent verdict here
        # (e.g. a transient flip) would pair the partitioned reader with a
        # ``[{}]`` descriptor — an uncapped serial read per microbatch.
        if (
            start_offset is None
            and end_offset is None
            and self._expand_read_active(table_name, opts)
        ):
            return [{}]
        segments = self._table_segments(table_name) or [table_name]
        namespace = opts.get("namespace")
        cursor_field = opts.get("cursor_field")
        self._pagination = self._parse_pagination(opts)
        if cursor_field or self._pagination != "nextlink":
            # Cursor-based read, or client-driven pagination (which needs a
            # $top to size pages): default page_size so a $top is sent.
            # Snapshot + nextlink leaves it unset → no $top.
            opts = {**opts, "page_size": opts.get("page_size", DEFAULT_PAGE_SIZE)}
        # ``cursor_lower`` is "what we've already read up to" — used by
        # read_partition as ``cursor gt cursor_lower``. There is NO upper
        # fence filter: rows landing above the previously-probed fence are
        # read now AND re-read next batch (``latest_offset`` commits the
        # probed fence, not the rows' max) — duplicate-safe by design, and
        # each row keeps its REAL cursor value. ``end_offset`` is consumed
        # only by the ``start == end`` quiescence check below.
        cursor_lower = (start_offset or {}).get("cursor")
        if cursor_field:
            # Mirror the serial reads: floor the READ boundary by the
            # configured lookback so rows that land at-or-below the
            # committed fence after the fence was probed (the fence is
            # taken BEFORE discovery, so that race window is real) are
            # re-scanned. Only the read floor moves — ``latest_offset``
            # still commits the true probed max, and re-read rows are
            # duplicate-safe. ``auto`` resolves to 0 on this path (no
            # walk-duration history rides the partitioned offset), so
            # overlap here requires an explicit ``cursor_lookback_seconds``.
            self._cursor_lookback = self._parse_cursor_lookback(opts)
            self._cursor_lookback_factor = self._parse_cursor_lookback_factor(opts)
            self._cursor_lookback_max_seconds = self._parse_cursor_lookback_ceiling(opts)
            self._active_lookback_seconds = self._resolve_active_lookback(start_offset)
            cursor_lower = self._apply_cursor_lookback(cursor_lower)
        top_rows = self._discover_top_parent_rows(
            segments, namespace, opts, cursor_field, cursor_lower
        )
        streaming = start_offset is not None or end_offset is not None
        if (
            streaming
            and cursor_field
            and any(
                f.name == cursor_field
                for f in self._own_fields_for_et(self._entity_type_for(segments[0], namespace))
            )
        ):
            # Null-cursor top parents are UNSUPPORTED on the partitioned
            # STREAMING path: once a fence is committed every batch's
            # ``cursor gt`` discovery filter excludes them SERVER-SIDE —
            # their subtrees' future changes would be dropped silently, with
            # no error and no log (the serial ancestor-cursor path raises on
            # the same configuration). The unfenced FIRST batch still sees
            # them in ``top_rows``; every fenced batch runs a one-request
            # ``eq null`` probe instead, so a null-cursor parent INSERTED
            # mid-stream is caught too, not just pre-existing ones. The
            # BATCH invocation (no offsets) is exempt: it re-discovers
            # unfenced every run, so null-cursor parents are always visible
            # and always read correctly there.
            nulls = [r for r in top_rows if r.get(cursor_field) is None]
            if nulls:
                self._raise_null_cursor_parents(table_name, segments, cursor_field, len(nulls))
            if cursor_lower is not None and self._null_cursor_parents_exist(
                segments, namespace, opts, cursor_field
            ):
                self._raise_null_cursor_parents(table_name, segments, cursor_field, None)
        if not top_rows:
            return []
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
        segments = self._table_segments(table_name) or [table_name]
        cursor_field = opts.get("cursor_field")
        self._pagination = self._parse_pagination(opts)
        # Parse THIS table's exclusion list before tagging rows — this entry
        # point never routes through read_table's reset, so without it a
        # stale exclusion from another table on a shared instance would
        # drop this table's FK columns (declared non-nullable → hard parse
        # failure downstream).
        self._set_excluded_ancestor_columns(opts)
        if cursor_field or self._pagination != "nextlink":
            # Cursor-based read, or client-driven pagination (needs a $top
            # to size pages): default page_size so a $top is sent.
            # Snapshot + nextlink leaves it unset → no $top.
            opts = {**opts, "page_size": opts.get("page_size", DEFAULT_PAGE_SIZE)}
        top_parent_rows = partition["top_parent_rows"]
        cursor_lower = partition.get("cursor_lower")
        # Same emit-boundary treatment as read_table: pad each row to the
        # declared schema (so a server that omits a null-valued non-nullable
        # property doesn't hard-fail the framework parser) then JSON-render
        # structured values, with primary keys exempt (a missing KEY is a
        # broken response that must stay loud — see pad_row_to_fields).
        # ``get_table_schema`` rebuilds from memoized parts — no I/O after
        # the first call.
        field_names = tuple(f.name for f in self.get_table_schema(table_name, opts).fields)
        # The delta synthetics are exempt too, mirroring read_table — today a
        # partitioned schema never declares them (delta != disabled forces the
        # serial fallback), so this only keeps the emit-boundary rule uniform.
        never_pad = frozenset(self._primary_keys_for(table_name, opts.get("namespace")) or ()) | {
            _DELETED_COL,
            _SEQUENCE_COL,
        }

        def _emit(row):
            return _jsonify_complex_values(pad_row_to_fields(row, field_names, never_pad))

        return map(
            _emit,
            self._iter_partition_rows(segments, opts, top_parent_rows, cursor_field, cursor_lower),
        )

    # ------------------------------------------------------------------
    # Per-table helpers (called by the methods above)
    # ------------------------------------------------------------------

    def _probe_top_level_max_cursor(
        self,
        segments: list[str],
        namespace: str | None,
        cursor_field: str,
        table_options: dict[str, str],
    ):
        """One HTTP probe: ``$top=1&$orderby=<cursor> desc`` → max value.

        The probe ANDs in the level-0 segment filter (``filter_at_<top>``)
        so the fence is the max over the SAME row population the discovery
        fetch reads. An unfiltered probe can fence past the filtered
        population's max (a fresher row OUTSIDE the filter); any filtered-in
        row that later lands with a cursor at or below that fence would sit
        behind the next batch's ``cursor gt fence`` forever. It also filters
        ``<cursor> ne null`` so a backend that sorts nulls FIRST under
        ``desc`` doesn't hand the ``$top=1`` probe a single null row and
        silently stall the stream; a backend that rejects null comparisons
        with a 400 gets one retry without that guard (keeping the
        population-defining segment filter).

        Returns ``None`` when the (filtered) top set is empty or the probed
        row's cursor is null. The caller decides whether that means "no new
        data" or "first call against an empty source."
        """
        top_set = segments[0]
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
        order_by = f"{cursor_field} desc"
        seg_filter = resolve_segment_filters(table_options or {}, segments).get(0)

        def _first_value(extra_filter, use_order_by=order_by):
            url = self._build_url(top_set, opts, extra_filter=extra_filter, order_by=use_order_by)
            for row in self._fetch_pages(url):
                value = row.get(cursor_field)
                if value is not None:
                    return value
            return None

        def _probe_max():
            try:
                return _first_value(combine_filters(seg_filter, f"{cursor_field} ne null"))
            except requests.HTTPError as exc:
                if exc.response is None or exc.response.status_code != 400:
                    raise
                return _first_value(seg_filter)

        value = _probe_max()
        if value is not None:
            self._verify_fence_orderby_desc(
                top_set, namespace, cursor_field, seg_filter, value, _first_value, _probe_max
            )
        return value

    def _verify_fence_orderby_desc(
        self, top_set, namespace, cursor_field, seg_filter, probed_max, first_value, probe_max
    ) -> None:
        """One-time behavioural check that the fence probe's ``$orderby …
        desc`` is actually honoured: ask for any row with ``cursor gt
        <probed max>`` — a row coming back means the probe returned a
        NON-max row, which unchecked would pin the fence at a stale value
        and silently stall the stream forever (``latest_offset`` returns
        ``end == start``, every trigger skipped, data accumulating unread).
        The adjacent nulls-first quirk already has its ``ne null`` guard;
        this closes the other way the ``$top=1`` probe can lie.

        A contradiction alone is NOT proof of a desc-ignoring server: on a
        busy source a row inserted in the one-RTT window between the probe
        and the check produces the same observation on a fully compliant
        server — and partitioned streaming targets exactly the busy
        sources, so raising on first contradiction would fail triggers
        spuriously at a rate scaling with write load. Disambiguate by
        RE-PROBING with ``desc``: an honoring server's re-probe returns a
        value at or above the check-found row (desc demonstrably surfaces
        the new max — the first probe was merely stale), which records the
        PASS; only a re-probe still BELOW the check-found row proves the
        server is ignoring ``$orderby`` → raise actionably. An empty
        re-probe (rows vanished mid-check) is inconclusive: no verdict, no
        raise, re-checked next trigger.

        The PASS verdict rides the shared process/file capability cache
        (15-min TTL, same layer as ``batch_ok``) so the extra request is
        paid once per table per TTL, not per trigger; a proven FAIL raises
        every time — a desc-ignoring server cannot stream partitioned, and
        silence here is the stall."""
        shared_key = f"{namespace}:{top_set}" if namespace else top_set
        if self._cached_capability("fence_desc_ok", table_name=shared_key):
            return
        edm_type = self._edm_types_for_et(self._entity_type_for(top_set, namespace)).get(
            cursor_field
        )
        above = first_value(
            combine_filters(seg_filter, self._cursor_filter(cursor_field, probed_max, edm_type)),
            use_order_by=None,
        )
        if above is None:
            self._store_capability("fence_desc_ok", True, table_name=shared_key)
            return
        reprobed = probe_max()
        if reprobed is not None and _cursor_le(above, reprobed):
            # desc demonstrably works — the first probe raced an insert.
            # The fence stays at the ORIGINAL probed max (monotonic and
            # valid; the fresher rows land next trigger).
            self._store_capability("fence_desc_ok", True, table_name=shared_key)
            return
        if reprobed is None:
            return  # rows vanished mid-check — inconclusive, re-check next trigger
        # ``above`` was read BEFORE the re-probe. On a fully compliant server
        # the row that exceeded ``probed_max`` can be DELETED in the interim
        # (an insert-then-delete race, not just an insert), leaving
        # ``reprobed < above`` with no desc-ignore at all — the re-probe simply
        # surfaces the true post-delete max. The genuine desc-ignore signature
        # is a row that STILL persists strictly above what desc surfaces NOW;
        # re-check that against ``reprobed`` before accusing the server. If it
        # has vanished, treat as inconclusive (like the empty-re-probe case)
        # and re-check next trigger — collapsing the single insert-then-delete
        # race into no-verdict rather than a fatal, misleading raise.
        still_above = first_value(
            combine_filters(seg_filter, self._cursor_filter(cursor_field, reprobed, edm_type)),
            use_order_by=None,
        )
        if still_above is None:
            return
        raise ValueError(
            f"The partitioned-stream fence probe on {top_set!r} is unreliable: "
            f"$orderby={cursor_field} desc&$top=1 returned {reprobed!r}, the server "
            f"still has rows with {cursor_field} gt {reprobed!r} (e.g. {still_above!r}) — "
            f"it is ignoring $orderby. A mis-probed fence pins the stream's watermark "
            f"and silently stalls it with data pending. Fix the server's ordering "
            f"support, or read this table serially (drop num_partitions / use a "
            f"non-partitioned configuration)."
        )

    def _raise_null_cursor_parents(
        self, table_name: str, segments: list[str], cursor_field: str, count: int | None
    ) -> None:
        """The shared refusal for null-cursor top parents on the partitioned
        streaming path — from the first batch's in-discovery check (exact
        ``count``) or a fenced batch's probe (``count=None``)."""
        found = f"{count} top-level parent(s) have" if count else "a top-level parent has"
        raise ValueError(
            f"Partitioned read of {table_name!r}: {found} a null "
            f"{cursor_field!r}. Null-cursor parents are invisible to the "
            f"partitioned fence filter, so their contained rows would be "
            f"silently dropped. Exclude them server-side (e.g. "
            f'filter_at_{segments[0]}="{cursor_field} ne null"), fix the '
            f"data, or read serially (num_partitions unset)."
        )

    def _null_cursor_parents_exist(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> bool:
        """One ``$top=1`` probe for null-cursor top parents. Fenced batches
        need it because their ``cursor gt fence`` discovery filter hides
        null-cursor rows server-side — without the probe the round-29 guard
        is dead after batch 1 and a parent inserted with a null cursor is
        silently invisible forever. Best-effort: a server that rejects the
        ``eq null`` filter — or any transport/transient failure — keeps the
        (first-batch-only) guard behavior rather than failing the batch
        (the same fail-open discipline as the capability probes)."""
        segment_filters = resolve_segment_filters(table_options, segments)
        extra = combine_filters(f"{cursor_field} eq null", segment_filters.get(0))
        pks = self._own_primary_keys_for_et(self._entity_type_for(segments[0], namespace))
        url = self._build_url(
            segments[0],
            {"select": ",".join(pks), "page_size": "1"},
            extra_filter=extra,
        )
        try:
            return next(iter(self._fetch_pages(url)), None) is not None
        except (requests.RequestException, RuntimeError):
            return False

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
                    cursor_extra = self._cursor_filter(
                        cursor_field,
                        cursor_lower,
                        edm_type=self._edm_types_for_et(ancestor_et).get(cursor_field),
                    )
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
        opts = {"select": ",".join(select_cols)}
        # Propagate the user's ``page_size`` only when set; with no
        # ``page_size`` no ``$top`` is sent (see ``_format_query_params``).
        if table_options.get("page_size"):
            opts["page_size"] = table_options["page_size"]
        url = self._build_url(top_set, opts, extra_filter=extra_filter, order_by=order_by)
        return list(self._fetch_pages(url, self._edm_types_for_et(ancestor_et)))

    def _leaf_pages_tolerating_vanished(self, url: str, leaf_types, chain: list) -> Iterator[dict]:
        """One chain's leaf pages, skipping the chain when its parent
        vanished (404/410) between planning and this task's walk. The
        partition descriptor is frozen at planning, so without the skip a
        parent deleted mid-batch fails every Spark task retry
        deterministically and kills the streaming query (the serial walks
        re-enumerate each trigger and self-heal without this)."""
        try:
            yield from self._fetch_pages(url, leaf_types)
        except requests.HTTPError as exc:
            if not _is_vanished_error(exc):
                raise
            _log_vanished_parent(chain, exc)

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
        # Leaf-collection types so keyset-seek boundaries render typed
        # (guid bare / ISO-looking string quoted) — see odata_literal_typed.
        leaf_types = self._edm_types_for_level(segments, len(segments) - 1, namespace)
        if not cursor_field:
            for chain in self._iter_parent_key_chains(
                segments,
                namespace,
                table_options,
                top_parent_rows=top_parent_rows,
                tolerate_vanished=True,
            ):
                url = self._build_contained_url(
                    segments,
                    chain,
                    table_options,
                    extra_filter=leaf_seg_filter,
                    order_by=leaf_order_by,
                )
                for row in self._leaf_pages_tolerating_vanished(url, leaf_types, chain):
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    yield row
            return
        cursor_level = self._find_cursor_level(segments, namespace, cursor_field)
        if cursor_level == -1:
            raise ValueError(
                f"cursor_field {cursor_field!r} is not a property on "
                f"the contained path or any of its ancestors."
            )
        # STREAMING partition activation requires cursor at level 0, but the
        # BATCH reader plans partitions without consulting ``is_partitioned``
        # — so the leaf-cursor branch below IS reached on partitioned batch
        # reads and must match the serial paths row-for-row. In particular
        # the ``cursor_nulls`` policy: every other path applies
        # ``skip_null`` from ``_make_cursor_resolver``; without it here a
        # partitioned batch read emits null-cursor rows the user configured
        # ``cursor_nulls=ignore`` to drop — and nondeterministically so,
        # since the framework silently falls back to the (correct) serial
        # read on any planning exception.
        skip_null = False
        if cursor_level == len(segments) - 1:
            skip_null, _effective = self._make_cursor_resolver(
                CONTAINED_PATH_SEP.join(segments), namespace, cursor_field, table_options
            )
        chains_iter = self._iter_parent_chains_with_cursor(
            segments,
            namespace,
            table_options,
            cursor_level,
            cursor_field,
            cursor_lower,
            top_parent_rows=top_parent_rows,
            tolerate_vanished=True,
        )
        for chain, ancestor_cursor in chains_iter:
            url = self._build_contained_url(
                segments, chain, table_options, extra_filter=leaf_seg_filter, order_by=leaf_order_by
            )
            for row in self._leaf_pages_tolerating_vanished(url, leaf_types, chain):
                self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                if cursor_level == len(segments) - 1:
                    # Leaf-cursor mode: filter per row by ``cursor gt
                    # cursor_lower`` — chronological comparison,
                    # never lexical (``.5Z`` vs ``Z`` renderings invert
                    # under string order).
                    rec = row.get(cursor_field)
                    if skip_null and rec is None:
                        continue
                    if (
                        cursor_lower is not None
                        and rec is not None
                        and _cursor_at_or_before_for_refilter(rec, cursor_lower)
                    ):
                        continue
                else:
                    row[cursor_field] = ancestor_cursor
                yield row


def _parse_num_partitions(opts: dict) -> int:
    """Curated parse of ``num_partitions`` (default 4).

    Mirrors ``validate_page_size``: garbage must fail fast with a clear
    error instead of riding into a bare ``int()`` — on the batch path the
    framework swallows planner exceptions and silently degrades to a
    serial read, so an uncurated ``ValueError`` would cost the user their
    parallelism with no hint why. Called from ``is_partitioned`` (stream
    setup, where a raise still surfaces) and ``get_partitions``."""
    raw = opts.get(_OPT_NUM_PARTITIONS)
    if raw is None:
        return _DEFAULT_NUM_PARTITIONS
    text = str(raw).strip()
    if not text.isdigit() or int(text) < 1:
        raise ValueError(
            f"num_partitions={raw!r} is not a positive integer. Use a value "
            f">= 1, or unset it for the default ({_DEFAULT_NUM_PARTITIONS})."
        )
    return int(text)


def _bin_pack(rows: list[dict], num_partitions: int, cursor_lower) -> list[dict]:
    """Split ``rows`` into ``min(num_partitions, len(rows))`` partition
    descriptors.

    Each partition carries a contiguous slice — keeps cursor ordering
    stable within a partition (the ``_discover_top_parent_rows`` caller
    sorts by cursor when one is set). Balanced ``divmod`` sizing (the
    first ``len(rows) % bins`` slices carry one extra row) delivers every
    partition the user asked for: a uniform ``ceil(n/p)`` slice width
    yields only ``ceil(n / ceil(n/p))`` bins — fewer than requested for
    many inputs (n=9, p=4 → 3 bins of 3) — silently costing parallelism.
    Bins are non-empty by construction, so no no-op executors spawn.
    """
    if not rows:
        return []
    bins = min(num_partitions, len(rows))
    base, extra = divmod(len(rows), bins)
    partitions: list[dict] = []
    start = 0
    for i in range(bins):
        size = base + (1 if i < extra else 0)
        partitions.append(
            {
                "top_parent_rows": rows[start : start + size],
                "cursor_lower": cursor_lower,
            }
        )
        start += size
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
