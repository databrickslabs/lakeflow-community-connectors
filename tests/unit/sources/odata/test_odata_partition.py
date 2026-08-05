"""OData connector unit tests — partition group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json

import pytest
import responses

from tests.unit.sources.odata._odata_test_helpers import *  # noqa: F401,F403


def test_pg_is_continuation_recognizes_any_casing():
    """Server continuations use arbitrary casing (Microsoft stacks emit
    $skipToken). Missing one would let the $top injection append onto an
    opaque token URL — the §11.2.5.7 hazard the guard exists to avoid."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _pg_is_continuation,
    )

    assert _pg_is_continuation("https://svc/Coll?$skipToken=abc") is True
    assert _pg_is_continuation("https://svc/Coll?%24skipToken=abc") is True
    assert _pg_is_continuation("https://svc/Coll?$SKIP=5") is True
    assert _pg_is_continuation("https://svc/Coll?$top=5") is False


def test_compute_dynamic_tops():
    """``compute_dynamic_tops`` distributes ``page_size`` across all
    levels with triangular weights so the cross-product fits in the
    budget. Top gets the largest share; minimum per level is 5."""
    from databricks.labs.community_connector.sources.odata._contained import (
        compute_dynamic_tops,
    )

    assert compute_dynamic_tops(1000, 1) == [1000]
    # 100 × 10 = 1000 (exactly fits)
    assert compute_dynamic_tops(1000, 2) == [100, 10]
    # 34 × 5 × 5 = 850. Bottom clamps to MIN, remaining 200-budget split
    # across the upper two levels: 200^(2/3) ≈ 34, 200^(1/3) ≈ 5.
    assert compute_dynamic_tops(1000, 3) == [34, 5, 5]
    # 8 × 5 × 5 × 5 = 1000. Bottom three clamp to MIN, top gets the
    # remaining 1000 / 125 = 8.
    assert compute_dynamic_tops(1000, 4) == [8, 5, 5, 5]
    # Cross-product never exceeds page_size when it's mathematically
    # possible (i.e. MIN ** N <= page_size).
    for n in (2, 3, 4):
        tops = compute_dynamic_tops(1000, n)
        product = 1
        for t in tops:
            product *= t
        assert product <= 1000, f"N={n} product={product} exceeds budget"
        assert all(t >= 5 for t in tops)
    # Small budget: every level clamps to minimum (5**3 = 125 > 10).
    assert compute_dynamic_tops(10, 3) == [5, 5, 5]


def test_compute_expand_tops_for_root():
    """A continuation rooted below level 0 budgets ``page_size`` across only its
    own collection levels (root..leaf); the fixed-key ancestors above take no
    share. Entries below the root are placeholders (never read)."""
    from databricks.labs.community_connector.sources.odata._contained import (
        compute_expand_tops_for_root,
    )

    # root_level=0 over a 4-segment chain == the full distribution.
    assert compute_expand_tops_for_root(1000, 4, 0) == [8, 5, 5, 5]
    # The xmla_demo case: a 4-segment chain, continuation rooted at level 2
    # (Instances(k)/Projects(k)/WorkPackageDetails?...$expand=WorkPackagesStepDetails).
    # Only levels 2,3 are collections → [100, 10] there, not the [5, 5] the
    # whole-chain distribution would force. Levels 0,1 are placeholders.
    assert compute_expand_tops_for_root(1000, 4, 2) == [0, 0, 100, 10]
    # Continuation rooted at the leaf level gets the entire budget.
    assert compute_expand_tops_for_root(1000, 4, 3) == [0, 0, 0, 1000]


# ---------------------------------------------------------------------------
# Partitioning (SupportsPartitionedStream)
# ---------------------------------------------------------------------------


@responses.activate
def test_partition_is_partitioned_rejects_flat_table():
    """Flat tables aren't partitioned — we'd be partitioning a single
    keyspace without distribution info."""
    _mock_nested_metadata()
    c = _make()
    assert c.is_partitioned("Parents") is False


@responses.activate
def test_partition_is_partitioned_rejects_expand_contained():
    """expand_contained does the whole table in one HTTP — no fan-out."""
    _mock_nested_metadata()
    c = _make({"expand_contained": "true"})
    assert c.is_partitioned("Parents__Children") is False


@responses.activate
def test_partition_is_partitioned_accepts_contained_snapshot():
    """Contained N+1 snapshot reads are the prime partition target."""
    _mock_nested_metadata()
    c = _make()
    assert c.is_partitioned("Parents__Children") is True


@responses.activate
def test_partition_get_partitions_bin_packs_contained_snapshot():
    """Snapshot batch path: top-level rows are bin-packed across
    ``num_partitions`` descriptors, each carrying its slice of parents."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": i} for i in range(1, 9)]},
    )
    c = _make()
    parts = c.get_partitions("Parents__Children", {"num_partitions": "4"})
    assert len(parts) == 4
    # Slices contiguous and exhaustive.
    flat = [row for p in parts for row in p["top_parent_rows"]]
    assert [r["Id"] for r in flat] == list(range(1, 9))


@responses.activate
def test_partition_get_partitions_applies_filter_at_top():
    """``filter_at_<top>`` (or its lowercased form from the framework)
    is applied to the partition pre-fetch so we don't bin-pack — and
    later walk — parents the user explicitly excluded."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 5}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$select": "Id",
                    "$filter": "Id eq 5",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    c = _make()
    parts = c.get_partitions(
        "Parents__Children",
        {"num_partitions": "4", "filter_at_Parents": "Id eq 5"},
    )
    flat = [row for p in parts for row in p["top_parent_rows"]]
    assert [r["Id"] for r in flat] == [5]


@responses.activate
def test_partition_read_partition_applies_filter_at_leaf():
    """``filter_at_<leaf>`` is applied at the leaf URL inside the
    partitioned walk, not just in the non-partitioned snapshot path."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1000", "$filter": "Label eq 'a'", "$orderby": "Id asc"}
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": None}
    rows = list(
        c.read_partition("Parents__Children", partition, {"filter_at_Children": "Label eq 'a'"})
    )
    assert [r["Id"] for r in rows] == [11]


@responses.activate
def test_partition_read_partition_walks_only_assigned_parents():
    """Executor never fetches level-0 leaves outside its partition.
    Parents(99)/Children is deliberately unregistered — if the
    partition walker over-fetches the test fails."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 99}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": None}
    rows = list(c.read_partition("Parents__Children", partition, {}))
    assert len(rows) == 1
    assert rows[0]["Id"] == 11
    # Verify no Parents(99)/Children call was made.
    leaf_urls = [c.request.url for c in responses.calls]
    assert not any("Parents(99)" in u for u in leaf_urls)


@responses.activate
def test_partition_empty_descriptor_falls_back_to_read_table():
    """get_partitions returns ``[{}]`` for flat tables; read_partition
    on that descriptor must produce the same rows as serial read_table."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "Name": "x"}]},
        match_querystring=False,
    )
    c = _make()
    rows = list(c.read_partition("Customers", {}, {}))
    # ``ModifiedAt`` padded to None (declared column the mock omitted).
    assert rows == [{"Id": 1, "Name": "x", "ModifiedAt": None}]


@responses.activate
def test_partition_latest_offset_probes_top_level_max_cursor():
    """In streaming mode the fence comes from a single
    ``?$top=1&$orderby=<cursor> desc`` probe against the top set."""
    _mock_nested_metadata()

    # Filter-aware: the round-45 desc self-check asks for ``Name gt 'z'``
    # and must see an empty set on this compliant server.
    def _parents(request):
        from urllib.parse import unquote as _unq

        if "gt" in _unq(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 9, "Name": "z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    offset = c.latest_offset(
        "Parents__Children",
        {"cursor_field": "Name"},
        None,
    )
    assert _drop_lb(offset) == {"cursor": "z"}


@responses.activate
def test_partition_latest_offset_snapshot_returns_wall_clock():
    """Without a cursor_field, snapshot streams advance via wall-clock
    epoch so Spark sees fresh end != start and triggers each batch."""
    _mock_nested_metadata()
    c = _make()
    offset = c.latest_offset("Parents__Children", {}, None)
    assert "snapshot_id" in offset
    assert isinstance(offset["snapshot_id"], int)


@responses.activate
def test_partition_get_partitions_empty_when_offsets_equal():
    """Streaming: when start_offset == end_offset Spark expects an
    empty partition list — no work to do."""
    _mock_nested_metadata()
    c = _make()
    parts = c.get_partitions(
        "Parents__Children",
        {"cursor_field": "Name"},
        {"cursor": "z"},
        {"cursor": "z"},
    )
    assert parts == []


@responses.activate
def test_partition_fence_probe_scopes_to_top_level_filter():
    """The ``latest_offset`` fence must be the max over the SAME population
    the read walks — ``filter_at_<top>`` rows only, non-null cursors first.
    An unfiltered probe fences past the filtered population's max (a fresher
    row OUTSIDE the filter), permanently skipping any filtered-in row that
    later lands at-or-below that fence (``cursor gt fence`` excludes it on
    every subsequent batch). The matcher below IS the assertion: a probe
    without this exact ``$filter`` finds no registered response."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 5, "Name": "2024-05-01T00:00:00Z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1",
                    "$select": "Name",
                    "$filter": "(Id eq 5) and (Name ne null)",
                    "$orderby": "Name desc",
                }
            )
        ],
    )
    # The round-45 desc self-check keeps the population filter and asks for
    # anything above the probed max (typed literal — Edm.String quotes).
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": []},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1",
                    "$select": "Name",
                    "$filter": "(Id eq 5) and (Name gt '2024-05-01T00:00:00Z')",
                }
            )
        ],
    )
    c = _make()
    offset = c.latest_offset(
        "Parents__Children",
        {"cursor_field": "Name", "filter_at_Parents": "Id eq 5"},
        None,
    )
    assert offset == {"cursor": "2024-05-01T00:00:00Z"}


@responses.activate
def test_partition_fence_probe_retries_without_null_guard_on_400():
    """A backend that rejects the ``ne null`` comparison (400) gets one
    retry without the null guard, so the hardening never breaks a stream
    that worked before it existed."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"error": {"message": "null comparison not supported"}},
        status=400,
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1",
                    "$select": "Name",
                    "$filter": "Name ne null",
                    "$orderby": "Name desc",
                }
            )
        ],
    )
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 9, "Name": "z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1", "$select": "Name", "$orderby": "Name desc"}
            )
        ],
    )
    # Round-45 desc self-check: nothing above the probed max.
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": []},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1", "$select": "Name", "$filter": "Name gt 'z'"}
            )
        ],
    )
    c = _make()
    offset = c.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)
    assert offset == {"cursor": "z"}


@responses.activate
def test_partition_latest_offset_never_regresses_fence():
    """Replica lag / deletion of the max row must not move the committed
    fence backwards: the docstring's monotonic-progression promise is what
    makes ``cursor gt fence`` a safe dedup boundary."""
    _mock_nested_metadata()

    def _parents(request):
        from urllib.parse import unquote as _unq

        # Round-45 desc self-check: nothing above the probed max.
        if "gt" in _unq(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "2024-01-01T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    offset = c.latest_offset(
        "Parents__Children",
        {"cursor_field": "Name"},
        {"cursor": "2024-06-01T00:00:00Z"},
    )
    assert offset == {"cursor": "2024-06-01T00:00:00Z"}


@responses.activate
def test_partition_lookback_floors_read_boundary_not_fence():
    """``cursor_lookback_seconds`` must floor the partitioned READ boundary
    (discovery filter + descriptor ``cursor_lower``) — it was silently
    ignored on this path, leaving the probe→discovery race with no overlap
    protection. The committed fence itself is never floored."""
    from urllib.parse import unquote

    _mock_nested_metadata()

    def _parents(request):
        # Filter-aware: the fenced-batch ``eq null`` guard probe must see
        # no null-cursor parents (a filter-blind mock would feed the probe
        # the discovery rows and false-positive the guard).
        if "eq null" in unquote(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "2024-05-01T00:05:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    parts = c.get_partitions(
        "Parents__Children",
        {"cursor_field": "Name", "cursor_lookback_seconds": "600"},
        {"cursor": "2024-05-01T00:10:00Z"},
        {"cursor": "2024-05-01T00:20:00Z"},
    )
    # Descriptors carry the FLOORED boundary so every executor re-scans
    # the overlap window.
    assert parts
    assert all(p["cursor_lower"] == "2024-05-01T00:00:00Z" for p in parts)
    # And the discovery fetch used the floored boundary on the wire —
    # QUOTED: the cursor is declared Edm.String, and typed rendering
    # (round 41) quotes string literals even when they look like ISO
    # timestamps (the bare sniff form 400s strict servers).
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any("Name gt '2024-05-01T00:00:00Z'" in u for u in urls)


@responses.activate
def test_partition_num_partitions_garbage_rejected():
    """Garbage ``num_partitions`` must fail fast with a curated error —
    a bare ``int()`` crash is swallowed by the batch planner, which then
    silently degrades to a serial read."""
    _mock_nested_metadata()
    c = _make({"num_partitions": "abc"})
    with pytest.raises(ValueError, match="num_partitions"):
        c.is_partitioned("Parents__Children")
    c2 = _make()
    with pytest.raises(ValueError, match="num_partitions"):
        c2.get_partitions("Parents__Children", {"num_partitions": "0"})


@responses.activate
def test_partition_leaf_cursor_refilter_is_chronological_not_lexical():
    """The leaf-level client-side re-filter must compare cursor text
    chronologically: ``…00.5Z`` is NEWER than a ``…00Z`` boundary but
    lexically smaller (``.`` < ``Z``), so the old raw ``<=`` silently
    dropped exactly the newest rows."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "new", "ModifiedAt": "2024-01-01T23:00:00.5Z"},
                {"Id": 12, "Label": "old", "ModifiedAt": "2024-01-01T22:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": "2024-01-01T23:00:00Z"}
    rows = list(c.read_partition("Parents__Children", partition, {"cursor_field": "ModifiedAt"}))
    assert [r["Id"] for r in rows] == [11]


@responses.activate
def test_partition_read_partition_resets_stale_ancestor_exclusions():
    """``read_partition`` never routes through ``read_table``'s
    ``exclude_ancestor_columns`` reset, so a stale exclusion from another
    table on a shared instance would silently strip this table's FK
    columns (declared non-nullable → hard parse failure downstream)."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    c._excluded_ancestor_columns = frozenset({"Parents_Id"})  # stale, another table's
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": None}
    rows = list(c.read_partition("Parents__Children", partition, {}))
    assert rows and rows[0]["Parents_Id"] == 1


@responses.activate
def test_partitioned_pin_false_resets_shared_verdict_via_partition_path():
    """The reset contract must hold on the PARTITION path too. A partitionable
    contained snapshot pinned ``expand_contained=false`` streams through
    ``is_partitioned`` / ``get_partitions`` (never ``read_table``), so those
    must purge the per-table shared-cache verdict — otherwise a later switch
    back to ``auto`` would reuse a stale verdict without re-probing."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    tree = {"value": [{"Id": 1, "Mids": [{"Id": 10, "Leaves": [{"Id": 1001}]}]}]}
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda r: (200, {}, json.dumps(tree))
    )

    # auto snapshot: the preflight records expand_ok=True in the shared cache.
    c_auto = _make({"expand_contained": "auto"})
    assert c_auto._expand_read_active(PROBE_TABLE, {"expand_contained": "auto"}) is True
    assert c_auto._cached_capability("expand_ok", table_name=PROBE_TABLE) is True

    # Pinned false, partitionable snapshot: is_partitioned purges the verdict
    # (it would otherwise never be reset — this path skips read_table).
    c_false = _make({"expand_contained": "false"})
    assert c_false.is_partitioned(PROBE_TABLE) is True
    assert c_false._cached_capability("expand_ok", table_name=PROBE_TABLE) is None

    # And get_partitions on the pinned-false path resets it too (idempotent).
    c_auto._store_capability("expand_ok", True, table_name=PROBE_TABLE)  # re-seed
    c_false2 = _make({"expand_contained": "false"})
    c_false2.get_partitions(PROBE_TABLE, {"expand_contained": "false"})
    assert c_false2._cached_capability("expand_ok", table_name=PROBE_TABLE) is None

    # Switching back to auto now genuinely re-probes (nothing cached).
    n_before = sum(1 for c in responses.calls if "$expand" in unquote(c.request.url))
    c_reauto = _make({"expand_contained": "auto"})
    assert c_reauto.is_partitioned(PROBE_TABLE) is False  # verified → expand shape
    assert sum(1 for c in responses.calls if "$expand" in unquote(c.request.url)) > n_before


def test_pg_keyset_filter_typed_literals():
    """Unit shape of the typed seek: guid boundary bare, ISO-looking string
    boundary quoted; untyped columns keep the value sniff."""
    from databricks.labs.community_connector.sources.odata._contained import _pg_keyset_filter

    row = {"g": _GUID, "s": "2024-01-01"}
    types = {"g": "Edm.Guid", "s": "Edm.String"}
    seek = _pg_keyset_filter(["g", "s"], row, types)
    assert f"g gt {_GUID}" in seek
    assert "s gt '2024-01-01'" in seek
    assert f"g eq {_GUID}" in seek
    # Untyped fallback preserves the pre-round-27 sniff behavior.
    sniffed = _pg_keyset_filter(["g", "s"], row)
    assert f"g gt '{_GUID}'" in sniffed
    assert "s gt 2024-01-01" in sniffed


def test_pg_filter_percent24_spelling_folded():
    """A server-issued continuation can carry ``%24filter=`` instead of
    ``$filter=``. The filter readers must see it and the writers must FOLD
    it into the one ``$filter`` param — two filter params make the server
    pick one arbitrarily (or 400)."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _pg_base_filter,
        _pg_keyset_seek_url,
        _pg_with_extra_filter,
    )

    url = "https://x/E?%24filter=a eq 1&%24top=5"
    assert _pg_base_filter(url) == "a eq 1"
    out = _pg_with_extra_filter(url, "b gt 2")
    assert "%24filter" not in out
    assert "$filter=(a eq 1) and (b gt 2)" in out
    seek_url = _pg_keyset_seek_url(url, _pg_base_filter(url), "k gt 3")
    assert "%24filter" not in seek_url
    assert seek_url.count("$filter=") == 1
    assert "$filter=(a eq 1) and (k gt 3)" in seek_url


@responses.activate
def test_partition_walks_keyset_seek_guid_boundaries_bare():
    """Round-28: the partition path's discovery AND per-partition leaf fetches
    build keyset seeks too — both must render guid PK boundaries bare."""
    from urllib.parse import unquote

    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)

    def _accounts_cb(request):
        url = unquote(request.url)
        if f"AccountId gt {_GUID}" in url:
            return (200, {}, json.dumps({"value": []}))
        if "AccountId gt" in url:  # quoted — keep returning the page
            return (200, {}, json.dumps({"value": [{"AccountId": _GUID}]}))
        return (200, {}, json.dumps({"value": [{"AccountId": _GUID}]}))

    def _contacts_cb(request):
        url = unquote(request.url)
        if f"ContactId gt {_GUID2}" in url:
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps({"value": [{"ContactId": _GUID2, "ModifiedAt": "2020-06-01T00:00:00Z"}]}),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Accounts", callback=_accounts_cb)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Accounts({_GUID})/Contacts", callback=_contacts_cb
    )
    c = _make()
    opts = {
        "expand_contained": "false",
        "pagination": "keyset",
        "num_partitions": "2",
    }
    parts = c.get_partitions("Accounts__Contacts", opts)
    rows = []
    for part in parts:
        rows.extend(c.read_partition("Accounts__Contacts", part, opts))
    assert [r["ContactId"] for r in rows] == [_GUID2]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any(f"AccountId gt {_GUID}" in u for u in urls), "discovery never seeked"
    assert any(f"ContactId gt {_GUID2}" in u for u in urls), "leaf fetch never seeked"
    assert not any(f"gt '{_GUID}'" in u or f"gt '{_GUID2}'" in u for u in urls)


@responses.activate
def test_partition_discovery_rejects_null_cursor_parents():
    """Round-28: null-cursor top parents are visible only to the UNFENCED
    first discovery — every fenced batch's ``cursor gt`` filter hides them
    server-side and their subtrees' changes drop silently. Discovery must
    refuse loudly instead (the serial ancestor path raises on the same
    configuration)."""
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)
    responses.get(
        f"{SERVICE_URL}Accounts",
        json={
            "value": [
                {"AccountId": _GUID, "Name": "2020-06-01T00:00:00Z"},
                {"AccountId": _GUID2, "Name": None},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(ValueError, match="null"):
        c.get_partitions(
            "Accounts__Contacts",
            {"cursor_field": "Name", "expand_contained": "false", "num_partitions": "2"},
            {},
            {"cursor": "2020-06-01T00:00:00Z"},
        )


@responses.activate
def test_partition_null_cursor_parents_allowed_on_batch_invocation():
    """The null-cursor rejection is a STREAMING-fence hazard: the batch
    invocation re-discovers unfenced every run, so null-cursor parents are
    always visible and must keep working (round-28 guard was over-broad)."""
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)
    responses.get(
        f"{SERVICE_URL}Accounts",
        json={
            "value": [
                {"AccountId": _GUID, "Name": "2020-06-01T00:00:00Z"},
                {"AccountId": _GUID2, "Name": None},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    parts = c.get_partitions(
        "Accounts__Contacts",
        {"cursor_field": "Name", "expand_contained": "false", "num_partitions": "2"},
    )
    assert parts and all("top_parent_rows" in p for p in parts)


# ---------------------------------------------------------------------------
# Round-33 fixes: vanished-parent tolerance, per-batch null-cursor probe,
# dispatch-validated shape options, delta_ok reset path
# ---------------------------------------------------------------------------


@responses.activate
def test_partitioned_read_skips_vanished_parent():
    """The partition descriptor is frozen at planning, so a parent deleted
    mid-batch previously 404'd every Spark task retry and killed the
    streaming query. The vanished chain is now skipped with a warning and
    the surviving parents' rows still emit."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"error": "gone"}, status=404)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "ok"}]},
    )
    c = _make()
    rows = list(
        c.read_partition(
            "Parents__Children",
            {"top_parent_rows": [{"Id": 1}, {"Id": 2}], "cursor_lower": None},
            {},
        )
    )
    assert [r["Id"] for r in rows] == [21]
    assert rows[0]["Parents_Id"] == 2


@responses.activate
def test_partitioned_stream_null_cursor_probe_catches_late_arrivals():
    """The round-29 null-cursor guard inspected discovery rows only — dead
    after batch 1, because the fence filter hides null-cursor parents
    server-side. Fenced batches now run a one-request ``eq null`` probe, so
    a parent INSERTED with a null cursor mid-stream raises instead of its
    subtree being silently dropped forever."""
    from urllib.parse import unquote as _unq

    _mock_nested_metadata()

    def _parents(request):
        if "eq null" in _unq(request.url):
            # The mid-stream-inserted null-cursor parent.
            return (200, {}, '{"value": [{"Id": 9}]}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "2024-05-02T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    with pytest.raises(ValueError, match="null 'Name'"):
        c.get_partitions(
            "Parents__Children",
            {"cursor_field": "Name"},
            {"cursor": "2024-05-01T00:00:00Z"},
            {"cursor": "2024-05-02T00:00:00Z"},
        )


@responses.activate
def test_partitioned_contained_fetch_garbage_rejected():
    """`contained_fetch` garbage was silently accepted on the partitioned
    streaming path — validation lived only in read_table's dispatch, which a
    partitioned stream never routes through. is_partitioned now parses it."""
    _mock_nested_metadata()
    c = _make({"num_partitions": "2", "contained_fetch": "garbadge"})
    with pytest.raises(ValueError, match="contained_fetch"):
        c.is_partitioned("Parents__Children")


@responses.activate
def test_partitioned_batch_leaf_cursor_respects_cursor_nulls_ignore():
    """The partitioned BATCH path (LakeflowBatchReader plans partitions
    without consulting is_partitioned, so leaf-cursor tables DO reach it)
    must apply the cursor_nulls policy like every other path — it used to
    emit null-cursor rows the user configured ``ignore`` to drop, and
    nondeterministically so (the framework silently falls back to the
    correct serial read on any planning exception)."""
    _mock_nested_metadata()
    children = [
        {"Id": 101, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
        {"Id": 102, "Label": "b", "ModifiedAt": None},  # ignore must drop it
    ]
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 1}]})),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": children})),
    )
    opts = {
        "cursor_field": "ModifiedAt",  # LEAF-level cursor
        "cursor_nulls": "ignore",
        "pagination": "nextlink",
        "expand_contained": "false",
        "contained_fetch": "single",
        "cursor_probe": "false",
    }
    c = _make()
    serial_rows, _ = c.read_table("Parents__Children", None, opts)
    serial_ids = sorted(r["Id"] for r in serial_rows)
    parts = c.get_partitions("Parents__Children", opts)
    part_ids = sorted(
        r["Id"] for p in parts for r in c.read_partition("Parents__Children", p, opts)
    )
    assert serial_ids == part_ids == [101]


def test_pg_orderby_keys_plus_encoded_spaces():
    """'+' is a legal space encoding in query strings: 'Id+asc' must parse
    to key 'Id' and 'Name+desc' must trip the desc guard — not produce the
    bogus key 'Id+asc' that seeks on a nonexistent column."""
    from databricks.labs.community_connector.sources.odata._contained import _pg_orderby_keys

    assert _pg_orderby_keys("https://x/S?$orderby=Id+asc") == ["Id"]
    assert _pg_orderby_keys("https://x/S?$orderby=Name+desc") == []
    assert _pg_orderby_keys("https://x/S?$orderby=A+asc,B+asc") == ["A", "B"]
