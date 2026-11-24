"""OData connector unit tests — expand group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import re

import pytest
import requests
import responses
from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect

from tests.unit.sources.odata._odata_test_helpers import (
    _EXPAND_AUTO_OPTS,
    PROBE_TABLE,
    R39_FLIP_METADATA,
    R42_KEYLESS_MID_METADATA,
    R43_CI_COLLATION_METADATA,
    SERVICE_URL,
    _drop_lb,
    _expand_auto_roots_callback,
    _expand_inner_park_batch1,
    _expand_l0_page1,
    _expand_l0_park_batch1,
    _expand_urls,
    _make,
    _mock_nested_metadata,
    _mock_probe_metadata,
    _run_flip_preflight,
    _switch_opts,
    _switch_tree,
)


@responses.activate
def test_expand_cursor_lookback_floors_read_filter_not_offset():
    """``cursor_lookback_seconds`` floors the read filter by the overlap
    window (so a non-atomic walk re-scans mid-walk arrivals) but commits the
    TRUE max watermark, not the floored value."""
    from urllib.parse import unquote

    _mock_nested_metadata()
    captured: list[str] = []

    def _parents(req):
        captured.append(unquote(req.url))
        if "Id gt" in unquote(req.url):  # top-level auto drain probe
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-02T00:00:00Z"},
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",  # 1h overlap
        },
    )
    rows = list(records)
    # Read filter floored by 1h behind the committed 2024-01-02T00:00:00Z.
    assert any("ModifiedAt gt 2024-01-01T23:00:00" in u for u in captured), captured
    # Committed offset is the TRUE max emitted, NOT the floored read value.
    assert _drop_lb(offset) == {"cursor": "2024-01-03T00:00:00Z"}
    assert [r["Id"] for r in rows] == [11, 12]


@responses.activate
def test_expand_cursor_lookback_idles_on_no_progress_instead_of_raising():
    """Quiescent re-read: the floored filter re-returns the overlap rows
    (cursor <= committed) but no row exceeds the watermark. With lookback
    this never raises the no-progress error (which is what the plain
    ``cursor gt`` path would do): under default-on dedup the FIRST such
    batch delivers the overlap rows once (they enter ``lb_seen`` tracking
    — real offset progress), and every later quiescent trigger idles
    (empty, offset unchanged)."""
    from urllib.parse import unquote

    _mock_nested_metadata()

    def _parents(req):
        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    start = {"cursor": "2024-01-02T00:00:00Z"}  # == the only child's cursor
    records, offset = c.read_table(
        "Parents__Children",
        start,
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",
        },
    )
    # First quiescent batch: the overlap row enters dedup tracking and is
    # delivered once (lb_seen delta = offset progress) — still no raise.
    assert [r["Id"] for r in records] == [11]
    assert _drop_lb(offset) == start  # cursor did not advance
    assert len(offset["lb_seen"]) == 1
    # Second quiescent batch: tracked and unchanged — idles.
    records2, offset2 = c.read_table(
        "Parents__Children",
        offset,
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",
        },
    )
    assert list(records2) == []
    assert offset2 == offset  # idled, no advance, no RuntimeError


@responses.activate
def test_expand_midpage_park_resumes_by_row_key_not_position():
    """The expand drainer's mid-page park must carry the last processed
    row's ORDER KEY, not a positional skip: on a cursor-ordered top page,
    updating an already-emitted row moves it to the tail of the re-fetched
    page and shifts an UNREAD row into the skipped prefix — its whole
    subtree lost behind the watermark under a positional resume."""
    _mock_nested_metadata()
    # Mutable source: parents with ISO ``Name`` as the level-0 cursor, one
    # inline child each.
    state = [
        {"Id": 10, "Name": "2024-01-01T00:00:00Z", "kid": 101},
        {"Id": 20, "Name": "2024-01-02T00:00:00Z", "kid": 201},
        {"Id": 30, "Name": "2024-01-03T00:00:00Z", "kid": 301},
        {"Id": 40, "Name": "2024-01-04T00:00:00Z", "kid": 401},
    ]

    def parents_cb(_req):
        rows = [
            {
                "Id": p["Id"],
                "Name": p["Name"],
                "Children": [{"Id": p["kid"], "Label": "x", "ModifiedAt": p["Name"]}],
            }
            for p in sorted(state, key=lambda p: (p["Name"], p["Id"]))
        ]
        return (200, {}, json.dumps({"value": rows}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=parents_cb)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",
        "pagination": "nextlink",
    }
    recs1, offset1 = c.read_table("Parents__Children", {}, opts)
    # Batch 1: children of parents 10 and 20 emitted; mid-page park.
    assert sorted(r["Id"] for r in recs1) == [101, 201]
    assert offset1["pending_fetches"][0]["boundary"] == ["2024-01-02T00:00:00Z", 20]
    # Parent 10 is updated between batches → moves to the TAIL of the
    # cursor-ordered page; parent 30 shifts into the old positional prefix.
    state[0]["Name"] = "2024-01-05T00:00:00Z"
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    got = [r["Id"] for r in recs2]
    if offset2.get("pending_fetches"):
        recs3, _ = c.read_table("Parents__Children", offset2, opts)
        got += [r["Id"] for r in recs3]
    # Key-based resume: parents 30, 40, and the updated 10 all emit (across
    # the remaining capped batches). The positional skip=2 resume lost
    # parent 30's subtree entirely.
    assert sorted(got) == [101, 301, 401]


@responses.activate
def test_expand_parked_continuation_for_deleted_parent_drops_subtree():
    """A parked ``pending_fetches`` continuation is an entity-scoped URL;
    if its parent is deleted between batches the URL 404s FOREVER —
    re-raising turned the checkpoint into a permanently failing stream
    only a full refresh could recover. The resume must instead confirm
    the parent is gone (the from-scratch rebuild 404s too) and drop the
    subtree, duplicate-safe."""
    _mock_nested_metadata()
    c = _make()
    opts, offset1 = _expand_inner_park_batch1()
    # Parent 1 deleted: BOTH the parked continuation and any rebuilt
    # collection URL under it now 404.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda _r: (404, {}, json.dumps({"error": {"message": "not found"}})),
    )
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    # No raise; the dead subtree is dropped and the walk completes.
    assert list(recs2) == []
    assert "pending_fetches" not in offset2


@responses.activate
def test_expand_stale_inner_continuation_rebuilds_from_scratch():
    """The SAME 404/410 can mean the server continuation went stale
    (expired ``$skiptoken``) while the parent still exists — dropping the
    item there would silently lose the rest of the collection. The
    resume rebuilds the collection URL from the parked chain and re-reads
    it from scratch: bounded duplicates, never loss."""
    _mock_nested_metadata()
    c = _make()
    opts, offset1 = _expand_inner_park_batch1()

    def children_cb(req):
        if "skiptoken" in req.url:
            return (410, {}, json.dumps({"error": {"message": "token expired"}}))
        # The rebuilt from-scratch URL: the collection's remaining row.
        return (200, {}, json.dumps({"value": [{"Id": 13, "Label": "d"}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=children_cb)
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    rows = list(recs2)
    # The rebuilt read recovered the collection's remaining row, tagged
    # with the PARKED chain's parent.
    assert [(r["Parents_Id"], r["Id"]) for r in rows] == [(1, 13)]
    assert "pending_fetches" not in offset2


@responses.activate
def test_expand_stale_top_level_continuation_rebuilds_from_scratch():
    """A parked LEVEL-0 continuation (the top collection's $skiptoken) can
    expire exactly like an inner one — 410 is the spec-sanctioned signal.
    Re-raising made the checkpoint a permanently failing stream; the
    recovery must rebuild the top-level seed URL from the stashed
    options/watermark and re-read the collection (bounded duplicates)."""
    _mock_nested_metadata()

    state = {"seed_calls": 0}

    def parents_cb(req):
        if "skiptoken" in req.url:
            return (410, {}, json.dumps({"error": {"message": "token expired"}}))
        state["seed_calls"] += 1
        if state["seed_calls"] == 1:  # batch 1's seed fetch
            return (200, {}, json.dumps(_expand_l0_page1()))
        # batch 2's REBUILT seed: the collection's remaining page.
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 2,
                            "Name": "2024-01-02T00:00:00Z",
                            "Children": [{"Id": 21, "Label": "b"}],
                        }
                    ]
                }
            ),
        )

    c = _make()
    opts, offset1 = _expand_l0_park_batch1(c, parents_cb)
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    # The rebuilt seed re-read the top collection; the remaining parent's
    # child is recovered, and the stream is healthy again.
    assert [(r["Parents_Id"], r["Id"]) for r in recs2] == [(2, 21)]
    assert "pending_fetches" not in offset2


@responses.activate
def test_expand_top_level_collection_truly_gone_still_raises():
    """When the REBUILT top-level seed also 404s, the whole collection is
    gone — that's a config/service error, not row churn, and it must
    surface loudly rather than silently dropping the table."""
    _mock_nested_metadata()
    state = {"first": True}

    def parents_cb(_req):
        if state["first"]:
            state["first"] = False
            return (200, {}, json.dumps(_expand_l0_page1()))
        return (404, {}, json.dumps({"error": {"message": "gone"}}))

    c = _make()
    opts, offset1 = _expand_l0_park_batch1(c, parents_cb)
    with pytest.raises(requests.HTTPError):
        records, _ = c.read_table("Parents__Children", offset1, opts)
        list(records)


@responses.activate
def test_expand_parked_queue_is_depth_bounded_not_width():
    """The parked ``pending_fetches`` frontier is bounded by the contained
    path DEPTH, not by the top page's fan-out WIDTH. A wide top page over an
    inner-paging server (here 6 parents, each with a server-paged Children
    collection) would, under a breadth-first drain, park one continuation per
    parent — O(width), a multi-MB offset at scale. The depth-first stack
    machine drains each parent's subtree before the next, so at any park the
    frontier is just the current top-page position plus the O(depth) path
    being walked — never one-per-parent. Every child still arrives exactly
    once across the capped batches."""
    _mock_nested_metadata()
    parents = []
    for i in range(1, 7):
        parents.append(
            {
                "Id": i,
                "Name": f"2024-01-0{i}T00:00:00Z",
                "Children": [{"Id": i * 100 + 1, "Label": "inline"}],
                "Children@odata.nextLink": f"{SERVICE_URL}Parents({i})/Children?$skiptoken=k{i}",
            }
        )
        responses.get(
            f"{SERVICE_URL}Parents({i})/Children",
            json={"value": [{"Id": i * 100 + 2, "Label": "paged"}]},
            match_querystring=False,
        )
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents}, match_querystring=False)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        # LOW cap so the CAP (not any ceiling) drives parking every batch.
        "max_records_per_batch": "2",
        "pagination": "nextlink",
    }
    # Parents__Children has 2 contained segments; the frontier is O(depth):
    # the top-page resume item plus at most one in-flight inner continuation.
    depth_bound = 2 + 2  # len(segments) + a small constant
    got: list[int] = []
    offset: dict = {}
    parked = False
    for _ in range(50):
        records, offset = c.read_table("Parents__Children", offset, opts)
        got.extend(r["Id"] for r in records)
        pending = offset.get("pending_fetches")
        if not pending:
            break
        parked = True
        # The load-bearing assertion: the frontier stays DEPTH-bounded even
        # though the fan-out (6 parents) is far wider.
        assert len(pending) <= depth_bound, f"offset grew to {len(pending)} — width leaked in"
    else:
        raise AssertionError("expand queue never drained")
    # The cap must actually park for this test to prove anything.
    assert parked, "never parked — cap too high, test vacuous"
    # Every inline and every paged child arrived exactly once.
    assert sorted(got) == sorted(
        [i * 100 + 1 for i in range(1, 7)] + [i * 100 + 2 for i in range(1, 7)]
    )


@responses.activate
def test_expand_nextlink_parks_mid_inline_from_start_with_boundary():
    """``pagination=nextlink`` has no churn-safe client seek, so a mid-way
    INLINE collection parks as a FROM-START refetch (``$skip=0``, never a
    positional seek into the page) plus the chronological ``boundary`` of the
    last processed row; resume elides the already-emitted prefix client-side.
    Two properties are load-bearing: (1) the cap parks IMMEDIATELY — batch 1
    emits cap rows, not the whole in-flight inline collection's subtree; and
    (2) deleting an already-emitted row between batches cannot shift an
    unread row out of the refetch (a server-honoured ``$skip=2`` would drop
    it silently — the boundary resume must not)."""
    _mock_nested_metadata()
    children = [{"Id": 100 + i, "Label": f"c{i}"} for i in range(1, 7)]
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 1, "Name": "2024-01-01T00:00:00Z", "Children": children}]},
        match_querystring=False,
    )
    # The from-start refetch of the parked inline collection. Mutable so a
    # later batch observes between-batch churn (c1 deleted).
    live: list[dict] = list(children)
    responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(SERVICE_URL)}Parents\(1\)/Children.*"),
        callback=lambda req: (200, {}, json.dumps({"value": list(live)})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",
        "pagination": "nextlink",
    }
    records, offset = c.read_table("Parents__Children", {}, opts)
    got = [r["Id"] for r in records]
    # (1) Immediate park at the cap: 2 rows, not all 6 inline children.
    assert got == [101, 102]
    pending = offset.get("pending_fetches")
    assert pending
    inline_item = next(p for p in pending if p["level"] == 1)
    plain_url = inline_item["url"].replace("%24", "$")
    assert "$skip=0" in plain_url, f"expected from-start refetch, got {plain_url}"
    assert "$skip=2" not in plain_url, f"positional seek leaked into park: {plain_url}"
    assert inline_item["boundary"] is not None
    # (2) Churn: an ALREADY-EMITTED child vanishes between batches.
    del live[0]  # c1 == Id 101, emitted in batch 1
    for _ in range(10):
        records, offset = c.read_table("Parents__Children", offset, opts)
        got.extend(r["Id"] for r in records)
        if not offset.get("pending_fetches"):
            break
    else:
        raise AssertionError("expand queue never drained")
    # No loss (103-106 all arrive despite the deletion) and no duplicates.
    assert sorted(got) == [101, 102, 103, 104, 105, 106]


@responses.activate
def test_expand_three_level_parked_offset_stays_depth_bounded():
    """O(depth), not O(width), on a genuinely DEEP path. Parents__Children__
    Notes with a fan-out (2 parents × 2 children) far wider than its depth
    (3 segments); each child's Notes are server-paged. A breadth-first drain
    would accumulate a continuation per child (O(width)); the depth-first
    stack machine walks one subtree at a time so the parked frontier is the
    O(depth) path only. In nextlink mode a mid-way inline collection parks
    as a FROM-START refetch (positional $skip resume would be churn-unsafe)
    whose boundary elides the already-processed prefix on resume — so the
    cap parks immediately at any depth. Every note arrives exactly once."""
    _mock_nested_metadata()
    parents = []
    for p in (1, 2):
        children = []
        for cidx in (1, 2):
            cid = p * 10 + cidx
            notes_link = f"{SERVICE_URL}Parents({p})/Children({cid})/Notes?$skiptoken=n"
            children.append(
                {
                    "Id": cid,
                    "Label": f"c{cid}",
                    "Notes": [],  # notes deferred behind the nextLink
                    "Notes@odata.nextLink": notes_link,
                }
            )
            responses.get(
                f"{SERVICE_URL}Parents({p})/Children({cid})/Notes",
                json={
                    "value": [
                        {"Id": cid * 100 + 1, "Text": "a"},
                        {"Id": cid * 100 + 2, "Text": "b"},
                    ]
                },
                match_querystring=False,
            )
        # The from-start refetch a mid-inline nextlink park issues on resume
        # (boundary elides already-processed children client-side).
        responses.get(
            f"{SERVICE_URL}Parents({p})/Children",
            json={"value": children},
            match_querystring=False,
        )
        parents.append({"Id": p, "Name": f"2024-01-0{p}T00:00:00Z", "Children": children})
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents}, match_querystring=False)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",  # LOW: force parking across batches
        "pagination": "nextlink",
    }
    depth_bound = 3 + 2  # len(segments) + small constant
    got: list[int] = []
    offset: dict = {}
    parked = False
    for _ in range(50):
        records, offset = c.read_table("Parents__Children__Notes", offset, opts)
        got.extend(r["Id"] for r in records)
        pending = offset.get("pending_fetches")
        if not pending:
            break
        parked = True
        assert len(pending) <= depth_bound, f"offset grew to {len(pending)} — width leaked in"
    else:
        raise AssertionError("expand queue never drained")
    assert parked, "never parked — cap too high, test vacuous"
    # 2 parents × 2 children × 2 notes = 8 leaves, each exactly once.
    expected = [
        cid * 100 + n for p in (1, 2) for cidx in (1, 2) for cid in [p * 10 + cidx] for n in (1, 2)
    ]
    assert sorted(got) == sorted(expected)


@responses.activate
def test_expand_deep_single_collection_pages_with_bounded_offset():
    """The DEPTH case (a single parent whose inner collection pages across
    many server pages) must still page via ``$top``/``@odata.nextLink`` with
    an O(1) parked offset and bounded per-batch memory — the property the
    width-bounding redesign must NOT regress. One parent, Children paged
    across three server pages, cap=1: each batch emits ~one page and parks a
    single continuation; every child arrives exactly once."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Name": "2024-01-01T00:00:00Z",
                    "Children": [{"Id": 101, "Label": "p1"}],
                    "Children@odata.nextLink": f"{SERVICE_URL}Parents(1)/Children?$skiptoken=p2",
                }
            ]
        },
        match_querystring=False,
    )

    def children_cb(req):
        if "skiptoken=p2" in req.url:
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [{"Id": 102, "Label": "p2"}],
                        "@odata.nextLink": f"{SERVICE_URL}Parents(1)/Children?$skiptoken=p3",
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 103, "Label": "p3"}]}))  # last page

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=children_cb)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "1",
        "pagination": "nextlink",
    }
    got: list[int] = []
    offset: dict = {}
    max_pending = 0
    for _ in range(20):
        records, offset = c.read_table("Parents__Children", offset, opts)
        got.extend(r["Id"] for r in records)
        pending = offset.get("pending_fetches") or []
        max_pending = max(max_pending, len(pending))
        if not pending:
            break
    else:
        raise AssertionError("deep collection never drained")
    # O(1) offset — the parked frontier is one in-flight continuation, never
    # grows with the collection length.
    assert max_pending <= 2
    assert sorted(got) == [101, 102, 103]


def test_expand_verdict_key_is_namespace_qualified():
    """The same contained path string can resolve to differently-shaped
    types in two namespaces of one service — mirroring
    ``_cursor_probe_shared_key``, the ``expand_ok`` verdict key must be
    namespace-qualified so one namespace's pass can't skip the other's
    preflight (and get baked into its offset)."""
    c = _make()
    assert c._expand_shared_key("Customers__Addresses", {"namespace": "Sales"}) == (
        "Sales:Customers__Addresses"
    )
    assert c._expand_shared_key("Customers__Addresses", {}) == "Customers__Addresses"
    c._seed_capability_caches(
        "Customers__Addresses", {"namespace": "Sales"}, {"cursor": "x", "expand_ok": True}
    )
    _, off_hr = c._with_capabilities(
        ([], {"cursor": "y"}), {"namespace": "HR"}, "Customers__Addresses"
    )
    assert "expand_ok" not in off_hr
    _, off_sales = c._with_capabilities(
        ([], {"cursor": "y"}), {"namespace": "Sales"}, "Customers__Addresses"
    )
    assert off_sales.get("expand_ok") is True


@responses.activate
def test_expand_contained_auto_uses_expand_when_supported():
    """``auto`` preflights the real nested-$expand URL; a conclusive pass
    (inline children at every level) runs the expand read and persists
    ``expand_ok``, which a recreated reader uses to skip the preflight."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=lambda request: (200, {}, json.dumps(tree)),
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert offset.get("expand_ok") is True
    # Expand read: never a per-parent keyed GET (no N+1 ancestor walk).
    assert not any("Roots(" in call.request.url for call in responses.calls)
    # Exactly two $expand GETs: the preflight probe + the actual read.
    n_expand = sum(1 for call in responses.calls if "$expand" in unquote(call.request.url))
    assert n_expand == 2
    # The preflight probe pins the top-level $top to 1 (small subtree).
    probe_urls = [
        unquote(c_.request.url) for c_ in responses.calls if "$top=1&" in unquote(c_.request.url)
    ]
    assert probe_urls  # probe present

    # A RECREATED reader seeded from the offset skips the preflight entirely.
    n_before = len(responses.calls)
    c2 = _make()
    recs2, _ = c2.read_table(PROBE_TABLE, offset, dict(_EXPAND_AUTO_OPTS))
    list(recs2)
    new_roots = [call for call in responses.calls[n_before:] if "/Roots?" in call.request.url]
    assert len(new_roots) == 1  # just the read — no second probe


@responses.activate
def test_expand_contained_auto_falls_back_when_expand_ignored():
    """A server that accepts the $expand URL but returns rows WITHOUT the
    inline child collections would silently drop every deep row. The preflight
    cross-checks direct navigation, sees the children exist, records the
    definitive fail (``expand_ok=false``) and falls back to the N+1 walk."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=_expand_auto_roots_callback()
    )
    # Serves both the preflight's direct-nav cross-check and the N+1 walk.
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    # Round-30: the FAIL never rides the checkpoint (offsets are immortal —
    # a baked-in false would skip the preflight even after the server is
    # fixed). It lives in the TTL'd shared cache instead, like
    # cursor_probe_ok.
    assert "expand_ok" not in offset
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is False
    # Fallback hydrated via per-parent GETs.
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_expand_contained_auto_definitive_4xx_falls_back_and_persists():
    """A hard 4xx on the expand URL is a definitive verdict: fall back to N+1
    and persist ``expand_ok=false`` so the next microbatch skips the probe."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(
            expand_body={"error": "expand not supported"}, expand_status=400
        ),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    # Round-30: the fail is persisted in the TTL'd shared cache, never the
    # checkpoint — offsets are immortal, and a baked-in false would skip the
    # preflight even after the server is fixed.
    assert "expand_ok" not in offset
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is False
    # A recreated reader consults the shared cache and never retries $expand.
    n_before = len(responses.calls)
    c2 = _make()
    list(c2.read_table(PROBE_TABLE, offset, dict(_EXPAND_AUTO_OPTS))[0])
    assert not any("$expand" in unquote(call.request.url) for call in responses.calls[n_before:])
    # Once the cached fail expires (TTL / process restart), a fresh reader
    # RE-PROBES — exactly the recovery a fixed server needs.
    from databricks.labs.community_connector.sources.odata.odata import _clear_capability_cache

    _clear_capability_cache()
    n_before = len(responses.calls)
    c3 = _make()
    list(c3.read_table(PROBE_TABLE, offset, dict(_EXPAND_AUTO_OPTS))[0])
    assert any("$expand" in unquote(call.request.url) for call in responses.calls[n_before:])


@responses.activate
def test_expand_contained_auto_transient_failure_not_persisted():
    """A transient failure (503) on the expand preflight degrades THIS batch to
    the N+1 walk but records NO verdict — the next batch re-probes instead of
    pinning the stream to the fallback."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(expand_body={"detail": "busy"}, expand_status=503),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert "expand_ok" not in offset
    # Transient: the per-table memo dict may exist but must hold no verdict.
    assert not c.__dict__.get("_expand_supported")
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is None


@responses.activate
def test_expand_contained_default_is_auto():
    """With ``expand_contained`` UNSET, contained reads default to ``auto``:
    the preflight runs and a verified server is read via nested-$expand."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda request: (200, {}, json.dumps(tree))
    )
    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},  # no expand_contained
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset.get("expand_ok") is True
    assert not any("Roots(" in call.request.url for call in responses.calls)  # no N+1 walk


def test_expand_verdict_seed_and_merge_are_table_scoped():
    """A resume offset's ``expand_ok`` belongs to ITS table only. Seeding
    table A's verdict must not ride into table B's returned offset on a
    multi-table instance — baked in there it persists in B's checkpoint and
    skips B's own preflight forever, though B's (deeper) path may verify
    differently. That's the silent-deep-row-loss direction the preflight
    exists to prevent."""
    c = _make()
    c._seed_capability_caches("Roots__Mids__Leaves", None, {"cursor": "x", "expand_ok": True})
    merged_other = c._merge_capability_caches({"cursor": "y"}, "Other__Deep__Path")
    assert "expand_ok" not in merged_other
    merged_own = c._merge_capability_caches({"cursor": "y"}, "Roots__Mids__Leaves")
    assert merged_own.get("expand_ok") is True


@responses.activate
def test_expand_preflight_not_short_circuited_by_another_tables_verdict():
    """A verdict memoized for one table must not answer for another: with
    the instance memo pre-poisoned by a DIFFERENT table's ``False``, this
    table's ``auto`` preflight still runs, verifies expand, and reads via
    ``$expand`` (no N+1 walk) — and both tables' verdicts coexist in the
    per-table memo."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda request: (200, {}, json.dumps(tree))
    )
    c = _make()
    c.__dict__["_expand_supported"] = {"Some__Other__Table": False}
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset.get("expand_ok") is True
    assert not any("Roots(" in call.request.url for call in responses.calls)  # no N+1 walk
    assert c.__dict__["_expand_supported"] == {"Some__Other__Table": False, PROBE_TABLE: True}


@responses.activate
def test_expand_contained_auto_inconclusive_falls_back_to_n1():
    """An INCONCLUSIVE preflight must resolve to the N+1 shape, not expand.

    The trap: a server that silently ignores ``$expand`` whose first sampled
    branch is genuinely childless reads as inconclusive forever — assuming the
    expand shape there would silently drop every OTHER branch's rows on every
    batch. The safe resolution is N+1 for this batch (always correct), record
    nothing, re-probe next batch."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"

    def _roots_cb(request):
        from urllib.parse import unquote

        # $expand ignored by the server: rows come back with NO inline Mids —
        # for the probe AND for any read. Two parents; the first is childless.
        _ = unquote(request.url)
        return (200, {}, json.dumps({"value": [{"Id": 1}, {"Id": 2}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Roots", callback=_roots_cb)
    # Preflight cross-check on the FIRST parent: genuinely childless → the
    # probe cannot tell "ignored $expand" from "no children" → inconclusive.
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": []})
    # The second parent HAS children — only the N+1 walk can see them.
    responses.get(f"{SERVICE_URL}Roots(2)/Mids", json={"value": [{"Id": 20}]})
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids(20)/Leaves",
        json={"value": [{"Id": 2001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    # N+1 fallback found the second parent's leaf — the expand shape would
    # have silently emitted nothing.
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(2, 20, 2001)]
    # Inconclusive: nothing recorded, nothing persisted — re-probed next batch.
    assert "expand_ok" not in offset
    # Transient: the per-table memo dict may exist but must hold no verdict.
    assert not c.__dict__.get("_expand_supported")
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is None


@responses.activate
@pytest.mark.parametrize("second_mode", ["true", "auto"])
def test_expand_contained_switch_false_to_expand_resumes_from_watermark(second_mode):
    """Batch 1 reads N+1 (``expand_contained=false``) and commits a watermark;
    switching to ``true`` (or ``auto``) resumes from that same ``cursor`` key —
    the expand read filters ``gt <watermark>`` and picks up exactly the new
    rows, no re-ingest of batch 1's rows, no error."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(
            expand_body=_switch_tree(1002, "2020-07-01T00:00:00Z")
        ),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    # Batch 1: N+1 walk.
    c1 = _make()
    recs1, offset1 = c1.read_table(PROBE_TABLE, {"cursor": since}, _switch_opts("false"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs1] == [(1, 10, 1001)]
    assert offset1["cursor"] == "2020-06-01T00:00:00Z"
    assert not _expand_urls()  # pure N+1 so far

    # Batch 2: switched mode, resumed from batch 1's checkpoint.
    c2 = _make()
    recs2, offset2 = c2.read_table(PROBE_TABLE, offset1, _switch_opts(second_mode))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs2] == [(1, 10, 1002)]
    assert offset2["cursor"] == "2020-07-01T00:00:00Z"
    # The expand read resumed from the SHARED watermark, not from scratch.
    assert any("gt 2020-06-01T00:00:00Z" in u for u in _expand_urls())
    # No stale N+1 resume state rides forward.
    for stale in ("parent_idx", "parent_keys", "chain_next_link", "truncated_chain_cursor"):
        assert stale not in offset2


@responses.activate
def test_expand_contained_switch_true_to_false_resumes_from_watermark():
    """The reverse switch: batch 1 reads via $expand and commits a watermark;
    ``expand_contained=false`` resumes from it — the N+1 leaf walk filters
    ``gt <watermark>`` and no $expand request is ever issued again."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(
            expand_body=_switch_tree(1001, "2020-06-01T00:00:00Z")
        ),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1002, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
        match_querystring=False,
    )
    # Batch 1: explicit expand read.
    c1 = _make()
    recs1, offset1 = c1.read_table(PROBE_TABLE, {"cursor": since}, _switch_opts("true"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs1] == [(1, 10, 1001)]
    assert offset1["cursor"] == "2020-06-01T00:00:00Z"
    n_expand_batch1 = len(_expand_urls())
    assert n_expand_batch1 >= 1

    # Batch 2: N+1, resumed from the expand checkpoint.
    c2 = _make()
    recs2, offset2 = c2.read_table(PROBE_TABLE, offset1, _switch_opts("false"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs2] == [(1, 10, 1002)]
    assert offset2["cursor"] == "2020-07-01T00:00:00Z"
    assert len(_expand_urls()) == n_expand_batch1  # no $expand after the switch
    # The leaf walk filtered from the shared watermark.
    from urllib.parse import unquote

    leaf_urls = [
        unquote(c.request.url) for c in responses.calls if "Mids(10)/Leaves" in c.request.url
    ]
    assert any("gt 2020-06-01T00:00:00Z" in u for u in leaf_urls)


@responses.activate
def test_expand_truncation_offset_switch_to_false_ignores_pending_fetches():
    """MID-FLIGHT switch: the expand read truncated (parked ``pending_fetches``
    + ``running_max_cursor``, watermark held). Switching to ``false`` must
    ignore the parked expand state, re-read from the HELD watermark (re-emitted
    rows are MERGE-deduped downstream — never loss), and drop the stale expand
    keys from the outgoing offset so they can't resurrect on a later switch
    back."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    truncated = {
        "cursor": since,  # watermark held while the chain was in flight
        "running_max_cursor": "2020-06-05T00:00:00Z",
        "pending_fetches": [
            {
                "url": f"{SERVICE_URL}Roots?$marker=stale",
                "level": 0,
                "chain": [],
                "cur_val": None,
                "skip": 0,
            }
        ],
    }
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, dict(truncated), _switch_opts("false"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # The parked expand work queue was never resumed...
    assert not any("marker=stale" in c_.request.url for c_ in responses.calls)
    # ...and neither expand key leaks into the N+1 checkpoint.
    assert "pending_fetches" not in offset
    assert "running_max_cursor" not in offset
    # Read floor came from the held watermark, not the in-flight running max.
    from urllib.parse import unquote

    leaf_urls = [unquote(c_.request.url) for c_ in responses.calls if "Leaves" in c_.request.url]
    assert any(f"gt {since}" in u for u in leaf_urls)


@responses.activate
def test_expand_contained_auto_pin_unpin_lifecycle_across_stream():
    """Full verdict lifecycle over three microbatches of one stream:
    ``auto`` records ``expand_ok`` (offset + shared cache) → pinning ``false``
    reads N+1, scrubs the flag from the checkpoint AND purges the shared cache
    → re-selecting ``auto`` re-runs the preflight from scratch. Rows flow
    correctly at every step."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"

    # Since-aware $expand body: a real server honors the ``gt <since>``
    # cursor filter, so once microbatch 2 advances the watermark to
    # 2020-07-01 the expand read must serve a NEWER leaf — an ignored
    # filter returning only stale rows now (correctly) trips the
    # no-progress guard, since completion cursors are floored at ``since``
    # instead of regressing.
    def _roots_cb(request):
        url = unquote(request.url)
        if "$expand" not in url:
            return (200, {}, json.dumps({"value": [{"Id": 1}]}))
        if "gt 2020-07-01" in url:
            body = _switch_tree(1003, "2020-08-01T00:00:00Z")
        else:
            body = _switch_tree(1001, "2020-06-01T00:00:00Z")
        return (200, {}, json.dumps(body))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Roots", callback=_roots_cb)
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1002, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
        match_querystring=False,
    )
    # Microbatch 1 — auto: preflight + expand read, verdict recorded twice.
    c1 = _make()
    recs1, off1 = c1.read_table(PROBE_TABLE, {"cursor": since}, _switch_opts("auto"))
    assert [(r["Id"]) for r in recs1] == [1001]
    assert off1.get("expand_ok") is True
    assert c1._cached_capability("expand_ok", table_name=PROBE_TABLE) is True

    # Microbatch 2 — pinned false: N+1 read; the switch scrubs the checkpoint
    # flag and purges the shared cache entry.
    c2 = _make()
    n_expand_before = len(_expand_urls())
    recs2, off2 = c2.read_table(PROBE_TABLE, off1, _switch_opts("false"))
    assert [(r["Id"]) for r in recs2] == [1002]
    assert off2["cursor"] == "2020-07-01T00:00:00Z"
    assert "expand_ok" not in off2
    assert len(_expand_urls()) == n_expand_before  # pinned false never expands
    assert c2._cached_capability("expand_ok", table_name=PROBE_TABLE) is None

    # Microbatch 3 — back to auto: nothing recorded anywhere → the preflight
    # RE-RUNS (probe + read = two more $expand GETs), then re-records.
    c3 = _make()
    recs3, off3 = c3.read_table(PROBE_TABLE, off2, _switch_opts("auto"))
    list(recs3)
    assert len(_expand_urls()) == n_expand_before + 2
    assert off3.get("expand_ok") is True


def test_expand_contained_nonauto_scrubs_expand_ok():
    """An explicit non-``auto`` ``expand_contained`` scrubs the recorded
    ``expand_ok`` verdict, so re-selecting ``auto`` re-runs the preflight;
    ``auto`` — explicit or the unset default — keeps it."""
    c = _make()
    off = {"cursor": "x", "expand_ok": True}
    assert c._scrub_nonauto_verdicts(off, {"expand_contained": "false"}) == {"cursor": "x"}
    assert c._scrub_nonauto_verdicts(off, {"expand_contained": "true"}) == {"cursor": "x"}
    assert c._scrub_nonauto_verdicts(off, {}) == off  # unset default is auto → kept
    assert c._scrub_nonauto_verdicts(off, {"expand_contained": "auto"}) == off


@responses.activate
def test_expand_all_children_deferred_drain_without_dropping_queue():
    """A server that defers EVERY inner collection behind
    ``<Nav>@odata.nextLink`` (nothing inline) must still drain fully across
    capped batches, and the parked ``pending_fetches`` must never be silently
    dropped by the idle shortcut in ``_read_contained_expand`` (round-26: an
    empty-``emitted`` batch that echoes ``start_offset`` discards the queue
    and livelocks at zero rows). Depth-first drains each deferred child in the
    batch it's discovered, so the queue makes progress every batch and stays
    depth-bounded; every paged child arrives exactly once. (The specific
    "park before the first emit" trigger the old ceiling produced is no longer
    reachable under depth-first — the drainer emits as it descends — but the
    idle-shortcut guard remains as defense and this exercises the drain it
    protects.)"""
    _mock_nested_metadata()
    parents = []
    for i in range(1, 7):
        parents.append(
            {
                "Id": i,
                "Name": f"2024-01-0{i}T00:00:00Z",
                "Children": [],  # nothing inline — all children deferred
                "Children@odata.nextLink": f"{SERVICE_URL}Parents({i})/Children?$skiptoken=k{i}",
            }
        )
        responses.get(
            f"{SERVICE_URL}Parents({i})/Children",
            json={"value": [{"Id": i * 100 + 2, "Label": "paged"}]},
            match_querystring=False,
        )
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents}, match_querystring=False)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",  # LOW: park across batches
        "pagination": "nextlink",
    }
    got: list[int] = []
    offset: dict = {}
    parked = False
    for _ in range(50):
        records, offset = c.read_table("Parents__Children", offset, opts)
        rows = [r["Id"] for r in records]
        got.extend(rows)
        pending = offset.get("pending_fetches")
        if not pending:
            break
        parked = True
        # Progress every batch (queue never dropped/stalled) and depth-bounded.
        assert rows, "batch parked a queue but emitted nothing — possible livelock"
        assert len(pending) <= 4
    else:
        raise AssertionError("expand queue never drained")
    assert parked, "never parked — cap too high, test vacuous"
    assert sorted(got) == sorted(i * 100 + 2 for i in range(1, 7))


@responses.activate
def test_inner_next_link_service_root_relative_resolves_against_root():
    """A per-property ``<Nav>@odata.nextLink`` may be SERVICE-ROOT-relative
    (Hexagon SCApi, SAP Gateway). Resolving it with a plain ``urljoin``
    against the deep continuation URL doubles the ancestor path
    (``Roots(1)/Roots(1)/…`` → 404 + a rebuild-recovery full re-read); it
    must route through ``_resolve_next_link`` like top-level links."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    responses.get(
        f"{SERVICE_URL}Roots",
        json={
            "value": [
                {
                    "Id": 1,
                    "Mids": [],
                    "Mids@odata.nextLink": f"{SERVICE_URL}Roots(1)/Mids?$skiptoken=m",
                }
            ]
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {
                    "Id": 10,
                    "Leaves": [],
                    # service-root-relative — restates the path from the root
                    "Leaves@odata.nextLink": "Roots(1)/Mids(10)/Leaves?$skiptoken=z",
                }
            ]
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table(PROBE_TABLE, {}, {"expand_contained": "true", "pagination": "nextlink"})
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert not any("Roots(1)/Roots(1)" in u for u in urls), "ancestor path doubled"


def test_expand_level_types_stash_bounds():
    """The per-level type stash used by the expand queue drains: in-range
    levels return their map, out-of-range/absent stash returns None (sniff
    fallback), never raises."""
    c = _make()
    assert c._expand_level_types(0) is None  # no expand read yet
    c._expand_types_per_level = [{"a": "Edm.Guid"}, {}]
    assert c._expand_level_types(0) == {"a": "Edm.Guid"}
    assert c._expand_level_types(1) == {}
    assert c._expand_level_types(2) is None
    assert c._expand_level_types(-1) is None


def test_expand_ok_offset_carries_pass_only():
    """The checkpoint is immortal, so only the PASS may ride it: a memoized
    fail must stay out of the outgoing offset, and a poisoned checkpoint's
    ``expand_ok: false`` must not seed the memo (the preflight re-runs)."""
    c = _make()
    key = c._expand_shared_key("Roots__Mids__Leaves", None)
    c.__dict__["_expand_supported"] = {key: False}
    assert "expand_ok" not in c._merge_capability_caches({"cursor": "y"}, key)
    c.__dict__["_expand_supported"] = {key: True}
    assert c._merge_capability_caches({"cursor": "y"}, key)["expand_ok"] is True
    # Seed side: a false from an old (pre-fix) checkpoint is ignored.
    c2 = _make()
    c2._seed_capability_caches("Roots__Mids__Leaves", None, {"cursor": "x", "expand_ok": False})
    assert not c2.__dict__.get("_expand_supported")
    c2._seed_capability_caches("Roots__Mids__Leaves", None, {"cursor": "x", "expand_ok": True})
    assert c2.__dict__["_expand_supported"] == {key: True}


@responses.activate
def test_expand_url_user_select_lands_on_leaf_clause_not_top():
    """The ``select`` option is LEAF-scoped (docs, schema derivation, and the
    N+1 path all agree) — in expand mode it must ride the innermost
    ``$expand(...)`` clause, not the top segment's URL, where a leaf-only
    column 400s the server and a cross-level name silently mis-projects."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children"], {"select": "Id,Label", "page_size": "100"})
    top, expand_clause = url.split("$expand=", 1)
    assert "$select" not in top
    assert "$select=Id,Label" in expand_clause


@responses.activate
def test_expand_url_user_select_merges_with_cursor_select_at_leaf():
    """When a cursor projection targets the leaf level too, the user's leaf
    ``select`` merges with it — deduped, user's order first."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(
        ["Parents", "Children"],
        {"select": "Id,Label"},
        cursor_level=1,
        cursor_filter="ModifiedAt gt 2020-01-01T00:00:00Z",
        cursor_order="ModifiedAt asc,Id asc",
        cursor_select="ModifiedAt,Label",
    )
    expand_clause = url.split("$expand=", 1)[1]
    assert "$select=Id,Label,ModifiedAt" in expand_clause


@responses.activate
def test_probe_preflight_rendering_flip_is_a_pass_not_condemnation():
    """The preflight's final newest-leaf comparison is SAME-INSTANT, not raw
    text: a load balancer rendering one instant as …00Z on one backend and
    …00.000Z on the other must not produce definitive mis-ordering evidence
    (false cursor_probe_ok=false under auto; a spurious raise under strict
    nested-expand). Both flip directions now verify as a conclusive pass."""
    problem, conclusive, race = _run_flip_preflight("Z", ".000Z")
    assert problem is None and conclusive and not race
    responses.reset()
    problem, conclusive, race = _run_flip_preflight(".000Z", "Z")
    assert problem is None and conclusive and not race


def test_probe_preflight_all_race_scan_declines_probe(monkeypatch):
    """A verdict-less scan containing RACE skips (discriminating samples that
    returned newer-than-reference) must decline the probe for the batch —
    engaging it unverified could hide a mis-ordering server behind concurrent
    writes — while still recording nothing."""
    c = _make()
    monkeypatch.setattr(
        ODataLakeflowConnect,
        "_run_cursor_probe_preflight",
        lambda self, *a, **k: (None, False, True),
    )
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (False, False)
    # A genuinely non-discriminating scan (no races) still engages unverified.
    c2 = _make()
    monkeypatch.setattr(
        ODataLakeflowConnect,
        "_run_cursor_probe_preflight",
        lambda self, *a, **k: (None, False, False),
    )
    supported, conclusive = c2._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (True, False)


@responses.activate
def test_expand_preflight_annotation_deferral_verified_not_condemned():
    """A server that defers inner collections behind <Nav>@odata.nextLink
    (inline [] + annotation) used to read as 'children exist but $expand
    omitted them' — a definitive expand_ok=false pinning N+1 forever on a
    server the read fully supports. Annotation presence is containment
    evidence: the preflight now verifies through it and passes."""
    _mock_nested_metadata()

    def _parents_cb(req):
        from urllib.parse import unquote

        if "$expand=" in unquote(req.url):
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [
                            {
                                "Id": 1,
                                "Name": "p",
                                "Children": [],
                                "Children@odata.nextLink": (f"{SERVICE_URL}Parents(1)/Children"),
                            }
                        ]
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 1, "Name": "p"}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 11, "Label": "x"}]})),
    )
    c = _make()
    ok, definitive = c._run_expand_preflight("Parents__Children", ["Parents", "Children"], {}, None)
    assert (ok, definitive) == (True, True)


@responses.activate
def test_expand_flatten_absent_child_property_fetched_directly():
    """A spec-violating partial-expansion server that wholly OMITS the
    expanded property for some parents (no inline list, no annotation) used
    to have those subtrees silently dropped — absent is NOT verified-empty.
    The flatten now fetches the collection directly for such parents."""
    _mock_nested_metadata()

    def _parents_cb(req):
        from urllib.parse import unquote

        if "$expand=" in unquote(req.url):
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [
                            {"Id": 1, "Name": "a", "Children": [{"Id": 11, "Label": "x"}]},
                            {"Id": 2, "Name": "b"},  # property wholly absent
                        ]
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 1}, {"Id": 2}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(2)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 22, "Label": "y"}]})),
    )
    c = _make()
    rows, _ = c.read_table(
        "Parents__Children", None, {"expand_contained": "true", "pagination": "nextlink"}
    )
    assert {r["Id"] for r in rows} == {11, 22}


@responses.activate
def test_probe_preflight_transient_fetch_is_no_verdict_not_definitive():
    """A retry-exhausted transient (503) on the probe-shaped $expand fetch
    is NOT capability evidence — it used to return the definitive 'error'
    status, pinning a false cursor_probe_ok=false for the cache TTL under
    auto and raising a misleading capability error under strict. It now
    routes to the no-verdict path: auto degrades this batch and records
    NOTHING; strict raises the accurate 'before reaching a verdict'."""
    from urllib.parse import unquote

    def _mock_all():
        responses.get(f"{SERVICE_URL}$metadata", body=R39_FLIP_METADATA, status=200)
        responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]}, match_querystring=False)

        def _mids_cb(req):
            if "$expand=" in unquote(req.url):
                return (503, {}, "busy")  # probe-shaped fetch: throttled
            return (200, {}, json.dumps({"value": [{"Id": 7}]}))

        responses.add_callback(responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_cb)
        responses.get(
            f"{SERVICE_URL}Roots(1)/Mids(7)/Leaves",
            json={
                "value": [
                    {"RecordLastModified": "2024-05-02T00:00:00Z"},
                    {"RecordLastModified": "2024-05-01T00:00:00Z"},
                ]
            },
            match_querystring=False,
        )

    _mock_all()
    c = _make({"max_retries": "0"})
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (False, False)
    # Nothing recorded anywhere — the next batch re-probes.
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is None
    responses.reset()
    _mock_all()
    c2 = _make({"max_retries": "0"})
    with pytest.raises(ValueError, match="before reaching a verdict"):
        c2._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=True
        )


@responses.activate
def test_probe_preflight_keyless_parent_returns_three_tuple():
    """The keyless-leaf-parent early return was the one preflight exit the
    round-39 3-tuple migration missed: both consumers unpack
    ``problem, conclusive, race`` from the cached result, so the stale
    2-tuple crashed a probe-engaged read over a keyless parent with an
    undiagnosable unpack ValueError instead of reaching the walk's
    actionable "no primary key declared in $metadata" error."""
    responses.get(f"{SERVICE_URL}$metadata", body=R42_KEYLESS_MID_METADATA, status=200)
    c = _make({"token": "t"})
    result = c._run_cursor_probe_preflight(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified"
    )
    assert result == (None, False, False)
    # The consumer path must survive it too: inconclusive, race-free →
    # engage-unverified semantics, same as any non-discriminating scan.
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (True, False)


@responses.activate
def test_expand_park_boundary_survives_ci_collation():
    """A case-insensitive server orders parents 'a1' < 'B2'; Python ordinal
    order says the opposite. The expand drainer's park-boundary skip used to
    trust ordinal order, classifying the unwalked 'B2' as "already walked"
    on resume and silently dropping its whole subtree. The resume now
    anchors on the parked row's identity (exact for every key type) and
    only trusts client-side order where it's provable."""
    responses.get(f"{SERVICE_URL}$metadata", body=R43_CI_COLLATION_METADATA, status=200)
    page = {
        "value": [
            {"Id": "a1", "Children": [{"Cid": 1}]},
            {"Id": "B2", "Children": [{"Cid": 2}]},
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents", callback=lambda _r: (200, {}, json.dumps(page))
    )
    opts = {"expand_contained": "true", "max_records_per_batch": "1", "pagination": "nextlink"}
    emitted = []
    offset = {}
    for _ in range(6):
        recs, offset = _make().read_table("Parents__Children", offset, opts)
        emitted.extend(list(recs))
        if not offset or offset.get("snapshot_done"):
            break
    assert sorted(r["Cid"] for r in emitted) == [1, 2]
