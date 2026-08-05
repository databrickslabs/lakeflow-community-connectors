"""OData connector unit tests — batch group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import logging
import re

import pytest
import responses

from tests.unit.sources.odata._odata_test_helpers import *  # noqa: F401,F403


@responses.activate
def test_batch_mode_flat_cursor_drains_fully_despite_explicit_cap():
    """Batch reader (``start_offset=None``) with an explicit cap reads the
    whole table. The offset is discarded, so the cap is force-disabled —
    honouring it could only truncate-and-lose — and rows stream lazily.
    Three distinct-cursor rows that the *streaming* path with ``cap=1``
    would truncate then trim to empty (and raise) all come through here,
    with the terminal ``{}`` offset."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-01-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-01-03T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", None, {"cursor_field": "ModifiedAt", "max_records_per_batch": "1"}
    )
    assert [r["Id"] for r in records] == [1, 2, 3]
    assert _drop_lb(offset) == {}


@responses.activate
def test_batch_mode_contained_cursor_streams_lazily_per_parent():
    """The batch-mode contained cursor read yields lazily: consuming only
    the first parent's leaf row must not have fetched the second parent's
    leaf collection. This is the property that bounds peak memory to one
    page instead of materialising the whole result set (which the
    streaming walk's ``emitted`` list does). Draining the rest then
    reaches parent 2."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    fetched: list[str] = []

    def _leaf(request):
        fetched.append(request.url)
        n = "1" if "Parents(1)" in request.url else "2"
        return (
            200,
            {},
            '{"value": [{"Id": 1' + n + ', "Label": "x", '
            '"ModifiedAt": "2024-01-0' + n + 'T00:00:00Z"}]}',
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_leaf)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(2)/Children", callback=_leaf)
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "1"},
    )
    it = iter(records)
    first = next(it)
    assert first["Id"] == 11
    # Lazy: only parent 1's leaf fetched so far; parent 2 untouched.
    assert any("Parents(1)/Children" in u for u in fetched)
    assert not any("Parents(2)/Children" in u for u in fetched)
    # Draining the rest reaches parent 2 — full coverage, uncapped.
    rest = [r["Id"] for r in it]
    assert rest == [12]
    assert any("Parents(2)/Children" in u for u in fetched)
    assert _drop_lb(offset) == {}


@responses.activate
def test_batch_mode_expand_streams_lazily_and_uncapped():
    """``expand_contained=true`` under the batch reader streams flattened
    leaf rows one $expand response at a time and ignores an explicit cap
    (offset discarded → a cap could only truncate-and-lose). All leaf
    rows across the inline cross-product come through with a ``{}``
    offset."""
    _mock_nested_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Children": [
                        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
                        {"Id": 13, "Label": "c", "ModifiedAt": "2024-01-03T00:00:00Z"},
                    ],
                }
            ]
        },
        match_querystring=False,
    )
    # Short top-level page → the drainer probes once more to confirm exhaustion.
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    # Short, link-less inline Children page → the inner drainer probes past the
    # last inline child to confirm exhaustion (mirrors the top-level auto drain).
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        None,
        {"expand_contained": "true", "max_records_per_batch": "1"},
    )
    assert [r["Id"] for r in records] == [11, 12, 13]
    assert _drop_lb(offset) == {}


@responses.activate
def test_batch_probe_missing_substatus_is_definitive_fail():
    """A 2xx ``$batch`` envelope whose sub-response omits ``status`` is a
    malformed envelope, not a pass: the old ``.get("status", 0)`` read it
    as ``0 < 400`` and minted a definitive ``batch_ok=True`` from garbage
    (every hydrate then pays a doomed ``$batch`` POST before degrading to
    per-op GETs). Same discipline as the id-less envelope: definitive
    FAIL, hydrate goes straight to plain GETs."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={"responses": [{"id": "0", "body": {"value": []}}]},  # no status
    )
    c = _make()
    assert c._verify_batch_support(["Roots"], {}) is False
    assert c.__dict__["_batch_supported"] is False  # pinned definitively


@responses.activate
def test_capped_walk_resume_survives_parent_delete():
    """The truncation checkpoint parks the truncated parent's KEY CHAIN, not
    just its position: a parent deleted below the park shifts every
    successor left one slot, and a positional resume then skips the parked
    parent forever — its unread tail excluded by ``cursor gt <watermark>``
    on every later batch (permanent loss; beyond lookback during a capped
    bootstrap). The key-based resume re-finds the parked parent."""
    _mock_nested_metadata()
    parents_state = [{"Id": 10}, {"Id": 20}, {"Id": 30}]
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents",
        callback=lambda _r: (200, {}, json.dumps({"value": parents_state})),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(10)/Children",
        callback=_churn_children_cb([{"Id": 101, "ModifiedAt": "2024-01-01T00:00:00Z"}]),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(20)/Children",
        callback=_churn_children_cb(
            [
                {"Id": 201, "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 202, "ModifiedAt": "2024-01-02T00:00:00Z"},
                {"Id": 203, "ModifiedAt": "2024-01-03T00:00:00Z"},
            ]
        ),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(30)/Children",
        callback=_churn_children_cb([{"Id": 301, "ModifiedAt": "2024-02-01T00:00:00Z"}]),
    )
    c = _make()
    recs1, offset1 = c.read_table("Parents__Children", {}, _churn_walk_opts())
    # Batch 1: parent 10 in full + parent 20 trimmed at the c2 boundary.
    assert [r["Id"] for r in recs1] == [101, 201, 202]
    assert offset1["parent_keys"] == [{"Id": 20}]
    # Parent 10 is deleted between batches — every survivor shifts left.
    parents_state[:] = [{"Id": 20}, {"Id": 30}]
    recs2, offset2 = c.read_table("Parents__Children", offset1, _churn_walk_opts())
    # Batch 2 must resume AT parent 20 (its unread tail), then walk 30.
    # The positional resume skipped 20 entirely and lost row 203.
    assert [r["Id"] for r in recs2] == [203, 301]
    assert _drop_lb(offset2) == {"cursor": "2024-02-01T00:00:00Z"}


@responses.activate
def test_capped_walk_parked_link_follows_parent_keys_not_position():
    """A parent inserted below the park shifts the enumeration right; a
    positional resume then applies the parked mid-collection continuation
    link to the WRONG parent — its rows FK-tagged with that parent's keys
    (corrupt ancestor attribution). The key-based resume applies the link
    only to the parent that parked it. (The inserted parent's own rows are
    the documented mid-walk-arrival class — recovered via
    ``cursor_lookback`` on a later cycle, never mis-tagged.)"""
    _mock_nested_metadata()
    parents_state = [{"Id": 10}, {"Id": 20}, {"Id": 30}]
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents",
        callback=lambda _r: (200, {}, json.dumps({"value": parents_state})),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(10)/Children",
        callback=_churn_children_cb([{"Id": 101, "ModifiedAt": "2024-01-01T00:00:00Z"}]),
    )
    token_page = {"value": [{"Id": 203, "ModifiedAt": "2024-01-03T00:00:00Z"}]}

    def p20_cb(req):
        if "$skiptoken=t1" in req.url:
            return (200, {}, json.dumps(token_page))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"Id": 201, "ModifiedAt": "2024-01-01T00:00:00Z"},
                        {"Id": 202, "ModifiedAt": "2024-01-02T00:00:00Z"},
                    ],
                    "@odata.nextLink": f"{SERVICE_URL}Parents(20)/Children?$skiptoken=t1",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(20)/Children", callback=p20_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(30)/Children",
        callback=_churn_children_cb([{"Id": 301, "ModifiedAt": "2024-02-01T00:00:00Z"}]),
    )
    # Parents(15)/Children deliberately unregistered: any fetch of the
    # inserted parent (e.g. the parked link misapplied to it under the old
    # positional resume) fails the test via ConnectionError.
    c = _make()
    recs1, offset1 = c.read_table("Parents__Children", {}, _churn_walk_opts())
    # Batch 1: parent 10 (1 row) + parent 20 page 1 (2 rows) = cap; the
    # page's nextLink is the checkpoint.
    assert [r["Id"] for r in recs1] == [101, 201, 202]
    assert offset1["parent_keys"] == [{"Id": 20}]
    assert offset1["chain_next_link"].endswith("$skiptoken=t1")
    # Parent 15 is inserted below the park between batches.
    parents_state[:] = [{"Id": 10}, {"Id": 15}, {"Id": 20}, {"Id": 30}]
    recs2, _ = c.read_table("Parents__Children", offset1, _churn_walk_opts())
    # The link's rows must be tagged with parent 20 — the parent that
    # parked it — never with the inserted parent occupying its old slot.
    assert [(r["Parents_Id"], r["Id"]) for r in recs2] == [(20, 203), (30, 301)]


@responses.activate
def test_capped_walk_legacy_positional_offset_still_resumes():
    """Offsets written before ``parent_keys`` existed carry only
    ``parent_idx`` — they must keep resuming positionally (stable parent
    set), so an upgrade mid-stream doesn't strand a parked checkpoint."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 10}, {"Id": 20}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(10)/Children",
        callback=_churn_children_cb(
            [
                {"Id": 101, "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 102, "ModifiedAt": "2024-01-02T00:00:00Z"},
            ]
        ),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(20)/Children",
        callback=_churn_children_cb([{"Id": 201, "ModifiedAt": "2024-02-01T00:00:00Z"}]),
    )
    c = _make()
    legacy = {"parent_idx": 0, "truncated_chain_cursor": "2024-01-01T00:00:00Z"}
    recs, offset = c.read_table("Parents__Children", legacy, _churn_walk_opts())
    # Positional resume: parent at index 0 re-read from cursor gt c1.
    assert [r["Id"] for r in recs] == [102, 201]
    assert _drop_lb(offset) == {"cursor": "2024-02-01T00:00:00Z"}


@responses.activate
def test_capped_walk_watermark_survives_empty_resume_completion():
    """A truncated batch's max cursor must survive a resume that completes
    EMPTY: without running_max the checkpoint clear fell back to the old
    watermark and the stream re-read the same rows forever (period-2
    duplicate loop on a static source)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})

    def children_cb(req):
        from urllib.parse import unquote

        url = unquote(req.url)
        rows = [
            {"Id": 11, "ModifiedAt": "2024-02-01T00:00:00Z"},
            {"Id": 12, "ModifiedAt": "2024-03-01T00:00:00Z"},
            {"Id": 13, "ModifiedAt": "2024-04-01T00:00:00Z"},
        ]
        m = re.findall(r"ModifiedAt gt (\S+?)[)&]", url + "&")
        if m:
            floor = max(m)
            rows = [r for r in rows if r["ModifiedAt"] > floor]
        return (200, {}, json.dumps({"value": rows}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=children_cb)
    c = _make()
    # Default pagination=auto: the cap fires inside the one full-collection
    # page and the synthesized keyset seek becomes the parked link.
    opts = {"cursor_field": "ModifiedAt", "max_records_per_batch": "3"}
    start = {"cursor": "2024-01-01T00:00:00Z"}
    recs1, offset1 = c.read_table("Parents__Children", start, opts)
    assert [r["Id"] for r in recs1] == [11, 12, 13]
    assert offset1.get("running_max") == "2024-04-01T00:00:00Z"
    # Resume: the parked seek returns nothing — the clear must FOLD the
    # accumulated max into the committed cursor, not fall back to the old
    # watermark (which replays the same three rows forever).
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    assert list(recs2) == []
    assert _drop_lb(offset2) == {"cursor": "2024-04-01T00:00:00Z"}


@responses.activate
def test_batch_subrequest_urls_are_percent_encoded():
    """Sub-request URLs ride inside the JSON ``$batch`` envelope and never
    pass through ``requests``' URL preparation — they must be pre-encoded
    the way requests would encode a plain GET (spaces → %20): a strict
    OData v4 server may reject a sub-request URL carrying raw spaces."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responder = _batch_responder(
        [
            ("Parents(1)/Children", {"value": [{"Id": 11, "Label": "a"}]}),
            ("Parents", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    assert [r["Id"] for r in recs] == [11]
    assert all(" " not in u for u in responder.seen), responder.seen
    # The leaf hydrate carries a stable $orderby — its space arrives as %20.
    assert any("%20" in u for u in responder.seen if "Children" in u)


@responses.activate
def test_batch_subresponse_transient_error_falls_back_to_plain_get():
    """A 2xx ``$batch`` envelope carrying one FAILED sub-response (a throttled
    leaf-parent, status 500) must not silently skip that parent's rows —
    ``rows = []`` with no error would be permanent loss on a cursor walk (the
    watermark advances past the failed parent). The drain re-issues the failed
    part as a plain GET and every row still arrives."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            url = r["url"]
            if "Parents(1)/Children" in url:
                out.append(
                    {"id": r["id"], "status": 200, "body": {"value": [{"Id": 11, "Label": "a"}]}}
                )
            elif "Parents(2)/Children" in url:
                out.append(
                    {"id": r["id"], "status": 500, "body": {"error": {"message": "throttled"}}}
                )
            else:  # capability preflight
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    # Plain-GET recovery target for the failed part.
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "b"}]},
        match_querystring=False,
    )

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    rows = sorted((r["Parents_Id"], r["Id"]) for r in recs)
    assert rows == [(1, 11), (2, 21)]  # nothing silently skipped
    assert any(
        call.request.method == "GET" and "Parents(2)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_batch_subresponse_hard_error_raises_instead_of_silent_skip():
    """A hard 4xx sub-response is re-issued as a plain GET, which raises with
    the server's actual error body — a failed part must surface, never quietly
    drop its parent's rows."""
    import requests as _requests

    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            if "Children" in r["url"]:
                out.append(
                    {"id": r["id"], "status": 400, "body": {"error": {"message": "bad filter"}}}
                )
            else:  # capability preflight
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    # The plain-GET re-issue hits the same 400 and raises with the body.
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"error": {"message": "bad filter"}},
        status=400,
        match_querystring=False,
    )

    c = _make()
    with pytest.raises(_requests.exceptions.HTTPError, match="bad filter"):
        list(c.read_table("Parents__Children", {}, {"expand_contained": "false"})[0])


@responses.activate
def test_batch_too_many_parts_shrinks_and_records_size():
    """When the server rejects a ``$batch`` with "too many parts", the connector
    shrinks the chunk size by 25% and retries until it fits, hydrates every
    leaf-parent, and records the discovered size in the offset (``batch_size_ok``)."""
    _mock_nested_metadata()
    parents = [{"Id": i} for i in range(1, 6)]  # 5 leaf-parents
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents})
    responder = _too_many_parts_responder(
        [(f"Parents({i})/Children", {"value": [{"Id": i * 10 + 1}]}) for i in range(1, 6)]
        + [("Parents", {"value": [{"Id": 1}]})],  # 1-part preflight (accepted)
        max_parts=2,
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})  # default contained_fetch=batch (1000)
    assert sorted(r["Id"] for r in recs) == [11, 21, 31, 41, 51]
    # Server rejected the oversized batch at least once, then every accepted
    # hydrate POST fit within the shrunk cap (<= 2 parts).
    assert responder.rejections[0] >= 1
    assert all(n <= 2 for n in responder.accepted)
    # The working size was discovered and recorded on the instance for reuse.
    # (The snapshot offset is built lazily before the generator runs, so the
    # persisted ``batch_size_ok`` is exercised by the cursor path below.)
    assert c.__dict__["_batch_size_cap"] == 2


@responses.activate
def test_batch_too_many_parts_falls_back_to_single_gets():
    """A server that rejects *any* multi-part ``$batch`` drives the cap down to 1
    and falls back to a plain per-leaf-parent GET — every row still arrives."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    responder = _too_many_parts_responder([("Parents", {"value": [{"Id": 1}]})], max_parts=1)
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)
    # Plain-GET fall-back targets.
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11}]},
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21}]},
        match_querystring=False,
    )

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})
    assert sorted(r["Id"] for r in recs) == [11, 21]
    # Fell back to per-parent GETs for the leaf collections.
    assert any(
        call.request.method == "GET" and "Parents(1)/Children" in call.request.url
        for call in responses.calls
    )
    assert c.__dict__["_batch_size_cap"] == 1  # give-up sentinel
    # The plain-GET fall-back re-adds a $top (the $batch-shaped URL carries
    # none) so the client-driven drain under the default pagination=auto can
    # page a server that page-limits while omitting @odata.nextLink.
    assert all(
        "$top=" in call.request.url
        for call in responses.calls
        if call.request.method == "GET" and "/Children" in call.request.url
    )


@responses.activate
def test_batch_size_ok_seeded_from_offset_avoids_oversized_batch():
    """``batch_size_ok`` in the resume offset seeds the cap, so the connector
    chunks at that size from the first round — no oversized batch is attempted."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}, {"Id": 3}]})
    responder = _too_many_parts_responder(
        [(f"Parents({i})/Children", {"value": [{"Id": i * 10 + 1}]}) for i in range(1, 4)]
        + [("Parents", {"value": [{"Id": 1}]})],
        max_parts=2,
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    # Snapshot read seeds capability caches from start_offset.
    recs, _ = c.read_table("Parents__Children", {"batch_size_ok": 2}, {})
    assert sorted(r["Id"] for r in recs) == [11, 21, 31]
    # Never overflowed (chunked at the seeded cap from the start): no rejection.
    assert responder.rejections[0] == 0
    # Accepted POSTs: the 1-part capability preflight + two hydrate rounds (2 + 1).
    assert sorted(responder.accepted) == [1, 1, 2]


@responses.activate
def test_batch_too_many_parts_persists_size_in_cursor_offset():
    """The **eager** cursor-incremental ``$batch`` walk (``cursor_probe=batch``)
    discovers the working size on a "too many parts" rejection and records it in
    the resume offset (``batch_size_ok``) so the next microbatch reuses it."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}, {"Id": 11}, {"Id": 12}]},
    )
    responder = _too_many_parts_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            (
                "Mids(11)/Leaves",
                {"value": [{"Id": 1101, "RecordLastModified": "2020-06-02T00:00:00Z"}]},
            ),
            (
                "Mids(12)/Leaves",
                {"value": [{"Id": 1201, "RecordLastModified": "2020-06-03T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # capability preflight
        ],
        max_parts=2,
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    assert sorted(r["Id"] for r in recs) == [1001, 1101, 1201]
    assert responder.rejections[0] >= 1
    assert all(n <= 2 for n in responder.accepted)
    # Eager walk → cap discovered before the offset is finalized → persisted.
    assert offset.get("batch_size_ok") == 2


@responses.activate
def test_batch_too_many_parts_converges_below_100_cap():
    """The retry budget lets the 1000-op default shrink below a ~100-part server
    cap and keep batching (rather than giving up): the recorded size settles
    between 1 and 100, every accepted batch fits the cap, and all rows arrive."""
    import re

    _mock_nested_metadata()
    n = 1000
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": i} for i in range(1, n + 1)]})

    accepted: list[int] = []
    rejections = [0]

    def cb(request):
        reqs = json.loads(request.body)["requests"]
        if len(reqs) > 100:  # server caps a batch at 100 parts
            rejections[0] += 1
            return (
                400,
                {"Content-Type": "application/json"},
                json.dumps({"error": {"message": "OData batch message contains too many parts"}}),
            )
        accepted.append(len(reqs))
        out = []
        for r in reqs:
            m = re.search(r"Parents\((\d+)\)/Children", r["url"])
            rows = [{"Id": int(m.group(1)) * 1000 + 1}] if m else []
            out.append({"id": r["id"], "status": 200, "body": {"value": rows}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=cb)

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})  # default batch (1000)
    assert sorted(r["Id"] for r in recs) == sorted(i * 1000 + 1 for i in range(1, n + 1))
    assert rejections[0] >= 1
    # Converged below the cap and kept batching — NOT the give-up sentinel (1).
    assert 1 < c.__dict__["_batch_size_cap"] <= 100
    assert all(s <= 100 for s in accepted)


@responses.activate
def test_batch_overflow_detects_exceeds_maximum_message():
    """The shrink trigger matches phrasing variants, not just "too many parts":
    a server that rejects with "$batch exceeds the maximum of 100 operations"
    (the live Hexagon Smart API wording) still shrinks instead of hard-failing."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": i} for i in range(1, 6)]})
    responder = _too_many_parts_responder(
        [(f"Parents({i})/Children", {"value": [{"Id": i * 10 + 1}]}) for i in range(1, 6)]
        + [("Parents", {"value": [{"Id": 1}]})],
        max_parts=2,
        message="$batch exceeds the maximum of 100 operations",
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})
    assert sorted(r["Id"] for r in recs) == [11, 21, 31, 41, 51]
    assert responder.rejections[0] >= 1  # the message was recognized → shrank
    assert all(n <= 2 for n in responder.accepted)
    assert c.__dict__["_batch_size_cap"] == 2


@responses.activate
def test_batch_preflight_transient_failure_not_persisted():
    """A transient failure of the ``$batch`` capability preflight (e.g. a 503)
    degrades THIS batch to the plain N+1 walk but records NO verdict — the next
    read re-probes, instead of persisting ``batch_ok=False`` and permanently
    pinning the stream to the slow path on a momentary blip. (Contrast the 405
    tests, where the definitive rejection IS persisted.)"""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "busy"}, status=503)
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    opts = {
        "cursor_field": "RecordLastModified",
        "cursor_probe": "batch",
        "pagination": "nextlink",
    }
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, opts)
    # Degraded to the plain N+1 walk for this batch — rows still correct.
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    # Transient → nothing cached on the instance, nothing persisted.
    assert "batch_ok" not in offset
    assert "_batch_supported" not in c.__dict__
    # The next read re-probes: a second preflight POST goes out.
    list(c.read_table(PROBE_TABLE, {"cursor": since}, opts)[0])
    posts = [call for call in responses.calls if call.request.method == "POST"]
    assert len(posts) == 2


@responses.activate
def test_batch_walk_cap_on_final_chunk_resume_clears_checkpoint():
    """The ``$batch`` walk's cap can fire exactly on its FINAL chunk (dirty
    parents an exact multiple of the chunk size). The truncated offset parks
    ``parent_idx`` == the total chain count, so the resumed batch has no
    re-entry work and emits nothing — it must CLEAR the checkpoint (offset back
    to the plain watermark) rather than echo it back forever, which would
    freeze the walk and silently skip all future changes under those parents."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}, {"Id": 11}]})
    responder = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            (
                "Mids(11)/Leaves",
                {"value": [{"Id": 1101, "RecordLastModified": "2020-06-02T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    opts = {
        "cursor_field": "RecordLastModified",
        "cursor_probe": "batch:2",  # chunk size == the number of leaf-parents
        "max_records_per_batch": "1",  # cap fires as the final chunk drains
        "pagination": "nextlink",
    }
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, opts)
    assert sorted(r["Id"] for r in recs) == [1001, 1101]  # chunk-aligned overshoot
    assert offset["parent_idx"] == 2  # truncated at the (final) chunk boundary
    assert offset["cursor"] == since  # watermark held while "in flight"

    # Resume: every chain is skipped and nothing is left to emit — the parked
    # checkpoint is cleared so the walk terminates instead of parking forever.
    recs2, offset2 = c.read_table(PROBE_TABLE, offset, opts)
    assert list(recs2) == []
    assert "parent_idx" not in offset2
    # The clear folds the truncated cycle's running_max into the committed
    # cursor — batch 1's progress is never lost (no period-2 re-read loop).
    assert offset2["cursor"] == "2020-06-02T00:00:00Z"


@responses.activate
def test_max_records_per_batch_curated_validation():
    """``max_records_per_batch`` caps EMITTED rows — 0/negative would park
    (or livelock) forever without emitting, and a non-numeric value crashed
    with a bare int() traceback. Both get a curated error now."""
    _mock_metadata()
    c = _make()
    for bad in ("0", "-3", "abc"):
        with pytest.raises(ValueError, match="max_records_per_batch"):
            c.read_table(
                "Customers",
                {"cursor": "2020-01-01T00:00:00Z"},
                {"cursor_field": "ModifiedAt", "max_records_per_batch": bad},
            )


@responses.activate
def test_batch_envelope_corrupt_200_retried_once():
    """The ``$batch`` envelope is the LARGEST response the connector ever
    receives — the exact truncated-200 shape ``_fetch_page_payload`` retries
    for on plain GETs. One corrupt envelope must re-POST (GET-only
    sub-requests, safe), not kill the whole read."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    good = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
        ]
    )
    state = {"hydrates": 0}

    def _cb(request):
        body = request.body.decode() if isinstance(request.body, bytes) else request.body
        if "Leaves" in body:
            state["hydrates"] += 1
            if state["hydrates"] == 1:
                return (200, {"Content-Type": "application/json"}, '{"responses": [{"id"')
        return good(request)

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since, "batch_ok": True},  # verdict seeded → no preflight POST
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    assert [r["Id"] for r in recs] == [1001]
    assert state["hydrates"] == 2  # corrupt once, re-POSTed once


@responses.activate
def test_batch_envelope_corrupt_200_twice_raises_actionable():
    """Twice-corrupt envelope: raise with the URL and a truncated body
    excerpt instead of a bare JSONDecodeError."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.add_callback(
        responses.POST,
        f"{SERVICE_URL}$batch",
        callback=lambda request: (200, {"Content-Type": "application/json"}, "{trunc"),
    )
    c = _make()
    with pytest.raises(RuntimeError, match="malformed JSON body twice"):
        recs, _ = c.read_table(
            PROBE_TABLE,
            {"cursor": "2020-01-01T00:00:00Z", "batch_ok": True},
            {
                "cursor_field": "RecordLastModified",
                "cursor_probe": "batch",
                "pagination": "nextlink",
            },
        )
        list(recs)


@responses.activate
def test_batch_subresponse_string_body_reissued_not_dropped():
    """A 2xx $batch sub-response whose `body` is a JSON STRING (spec-legal for
    non-JSON media) previously drained to rows=[] — silently dropping that
    parent's whole collection and letting the cursor walk advance past it. It's
    now re-issued as a plain GET so every row still arrives."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            if "Parents(1)/Children" in r["url"]:
                out.append(
                    {"id": r["id"], "status": 200, "body": {"value": [{"Id": 11, "Label": "a"}]}}
                )
            elif "Parents(2)/Children" in r["url"]:
                # Body serialized as a STRING rather than an inline object.
                out.append(
                    {"id": r["id"], "status": 200, "body": '{"value":[{"Id":21,"Label":"b"}]}'}
                )
            else:  # capability preflight
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "b"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    rows = sorted((r["Parents_Id"], r["Id"]) for r in recs)
    assert rows == [(1, 11), (2, 21)]  # parent 2 not silently dropped
    assert any(
        call.request.method == "GET" and "Parents(2)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_batch_subresponse_error_body_under_200_reissued():
    """A 200-status $batch sub-response whose body is an OData error envelope
    ({"error": {...}}, no "value") is a dict, so round 34's non-dict gate let
    it through — draining to rows=[] and silently dropping that parent. It's
    now re-issued as a plain GET."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            elif "Parents(2)/Children" in r["url"]:
                # 200 status but an error envelope body (spec-violating, but
                # seen in the wild) — must not drain to [].
                out.append({"id": r["id"], "status": 200, "body": {"error": {"message": "x"}}})
            else:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 22}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    ids = sorted(r["Id"] for r in recs)
    assert ids == [11, 22]  # parent 2 recovered via plain GET, not dropped


@responses.activate
def test_batch_empty_collection_dict_body_not_reissued():
    """A genuine empty-collection 200 ({"value": []}) is a drainable dict and
    must NOT be re-issued — only error/non-dict bodies are."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            elif "Parents(2)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": []}})
            else:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    assert sorted(r["Id"] for r in recs) == [11]
    # No plain-GET re-issue happened for parent 2 (empty is legit).
    assert not any(
        call.request.method == "GET" and "Parents(2)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_batch_subresponse_dict_without_value_reissued():
    """A 200 $batch sub-response whose dict body has NEITHER "value" NOR
    "error" (an OData-v2-style {"d": …} gateway shape, a JSON proxy page)
    drained to rows=[] — a silent parent skip the cursor walk then advances
    past. Every sub-request is a collection GET, so a drainable body is
    exactly one whose "value" is a LIST; anything else is re-issued."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            else:
                # v2-gateway shape: success status, no "value", no "error".
                out.append(
                    {"id": r["id"], "status": 200, "body": {"d": {"results": [{"Id": 999}]}}}
                )
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 22}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    ids = sorted(r["Id"] for r in recs)
    assert ids == [11, 22]  # parent 2 recovered via plain GET, not dropped


@responses.activate
def test_batch_subresponse_null_value_reissued():
    """{"value": null} (non-list value) used to crash the drain loudly
    (TypeError iterating None); it is now re-issued as a plain GET like every
    other undrainable body."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            else:
                out.append({"id": r["id"], "status": 200, "body": {"value": None}})
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 22}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    assert sorted(r["Id"] for r in recs) == [11, 22]


@responses.activate
def test_batch_probe_requires_id_echo():
    """A 2xx $batch envelope whose sub-response lacks the echoed ``id`` is a
    server ``_post_batch`` can never consume (it keys sub-responses by id and
    hard-raises without one, and that raise is not _BatchTooManyParts — so
    nothing would ever degrade to plain GETs). The probe used to pass it on
    status alone, pinning a definitive-but-unusable ``batch_ok: true``."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={"responses": [{"status": 200, "body": {"value": []}}]},
    )
    c = _make()
    assert c._verify_batch_support(["Roots"], {}) is False
    # Definitive fail — recorded, not retried every batch.
    assert c._cached_capability("batch_ok") is False


def test_batch_relative_normalizes_same_origin_spelling():
    """An absolute same-origin continuation spelled with the default port or
    a different host case must still resolve service-relative — the raw
    prefix match missed it and kept the service root's own path segments,
    404ing the sub-request (self-healing via the plain-GET re-issue, but one
    wasted round-trip plus an alarming warning per continuation)."""
    c = _make()
    assert (
        c._batch_relative("https://example.com:443/odata/Orders?$skiptoken=5")
        == "Orders?$skiptoken=5"
    )
    assert c._batch_relative("https://EXAMPLE.com/odata/Orders") == "Orders"
    # Off-origin (or unparseable-port) absolute URLs keep the legacy
    # path-only fallback; the same-origin guard upstream handles policy.
    assert c._batch_relative("https://other.example.com/odata/Orders") == "odata/Orders"
    assert c._batch_relative("https://example.com:banana/odata/Orders") == "odata/Orders"


@responses.activate
def test_capped_walk_vanished_string_key_park_resets_and_recovers(caplog):
    """String parent keys + the parked parent deleted between batches: the
    resume seek can no longer trust ordinal order to prove the remaining
    parents already-walked, and completing the batch would fold running_max
    past them (permanent sub-max loss — the pre-fix behavior). The seek now
    exhausts, resets the walk via a truncated no-park offset (positional
    restart, floor kept), and the next batch recovers the unwalked subtree."""
    responses.get(f"{SERVICE_URL}$metadata", body=R43_CI_COLLATION_METADATA, status=200)
    parents = {"n": 0}

    def _parents_cb(_req):
        parents["n"] += 1
        if parents["n"] == 1:  # batch 1: both parents, server CI order
            return (200, {}, json.dumps({"value": [{"Id": "a1"}, {"Id": "B2"}]}))
        return (200, {}, json.dumps({"value": [{"Id": "B2"}]}))  # a1 deleted

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.get(
        f"{SERVICE_URL}Parents('a1')/Children",
        json={
            "value": [
                {"Cid": 1, "ModifiedAt": "2024-06-01T00:00:00Z"},
                {"Cid": 2, "ModifiedAt": "2024-06-02T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents('B2')/Children",
        json={"value": [{"Cid": 3, "ModifiedAt": "2024-03-01T00:00:00Z"}]},
        match_querystring=False,
    )
    opts = {
        "cursor_field": "ModifiedAt",
        "max_records_per_batch": "1",
        "pagination": "nextlink",
        "cursor_probe": "false",
        # Pin the N+1 walk (no expand preflight — its probe request would
        # advance the parents callback's batch counter).
        "expand_contained": "false",
    }
    # Batch 1: cap parks parent 'a1' (boundary-trimmed, key chain parked).
    recs, off1 = _make().read_table("Parents__Children", {}, opts)
    assert [r["Cid"] for r in recs] == [1]
    assert off1.get("parent_keys") == [{"Id": "a1"}]
    # Batch 2: 'a1' vanished. The seek exhausts and RESETS (no cursor fold,
    # no park) instead of completing past the unwalked 'B2'.
    with caplog.at_level(logging.WARNING):
        recs, off2 = _make().read_table("Parents__Children", off1, opts)
    assert list(recs) == []
    assert "was not re-found" in caplog.text
    assert "parent_keys" not in off2
    assert "cursor" not in off2  # floor (None) kept — running_max NOT folded
    # Round 44: the reset offset must not stamp an inert explicit
    # ``truncated_chain_cursor: None`` (cosmetic wart; resume reads .get()).
    assert "truncated_chain_cursor" not in off2
    # Batch 3: full re-walk recovers B2's sub-max child.
    recs, off3 = _make().read_table("Parents__Children", off2, opts)
    assert [r["Cid"] for r in recs] == [3]
    # Completion folds the accumulated running_max (>= batch 1's rows).
    assert _drop_lb(off3)["cursor"] == "2024-06-01T00:00:00Z"


@responses.activate
def test_batch_probe_duplicate_ids_last_wins():
    """Duplicate ``id`` echoes resolve LAST-wins in ``_post_batch``'s by-id
    dict; the probe must judge the same sub-response the hydrate would
    consume, not pass on the first one."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={
            "responses": [
                {"id": "0", "status": 200, "body": {"value": []}},
                {"id": "0", "status": 404},
            ]
        },
    )
    assert _make()._verify_batch_support(["Roots"], {}) is False
