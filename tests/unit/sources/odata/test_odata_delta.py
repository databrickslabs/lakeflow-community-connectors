"""OData connector unit tests — delta group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import os

import pytest
import requests
import responses

from tests.unit.sources.odata._odata_test_helpers import (
    _COLLISION_MD,
    _GUID,
    DELTA_LINK_V1,
    DELTA_LINK_V2,
    METADATA_XML,
    NONNULL_METADATA_XML,
    PROBE_TABLE,
    SERVICE_URL,
    STREAM_METADATA_XML,
    _delta_bootstrap_body,
    _drop_lb,
    _make,
    _mock_metadata,
    _mock_probe_metadata,
)


@responses.activate
def test_delta_metadata_returns_cdc_with_synthetic_sequence_cursor():
    """When delta is active for a table, the connector advertises
    ``ingestion_type=cdc`` with the synthetic ``_lc_sequence`` cursor.
    Primary keys still come from the entity type's CSDL ``<Key>`` —
    apply_changes uses them as the MERGE key at the destination."""
    _mock_metadata()
    c = _make()
    meta = c.read_table_metadata("Customers", {"delta_tracking": "enabled"})
    assert meta == {
        "primary_keys": ["Id"],
        "cursor_field": "_lc_sequence",
        "ingestion_type": "cdc",
    }


@responses.activate
def test_delta_schema_appends_deleted_and_sequence_columns():
    """The destination needs the synthetic columns in the Spark schema
    so Delta accepts the emitted records. ``_deleted`` carries the
    in-band tombstone signal; ``_lc_sequence`` is apply_changes'
    sequence_by column."""
    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {"delta_tracking": "enabled"})
    names = [f.name for f in schema.fields]
    assert names == ["Id", "Name", "ModifiedAt", "_deleted", "_lc_sequence"]
    deleted_field = schema.fields[3]
    sequence_field = schema.fields[4]
    assert type(deleted_field.dataType).__name__ == "BooleanType"
    assert type(sequence_field.dataType).__name__ == "StringType"
    assert deleted_field.nullable is False
    assert sequence_field.nullable is False


@responses.activate
def test_delta_enabled_with_cursor_field_raises():
    """``delta_tracking=enabled`` and ``cursor_field`` are mutually
    exclusive — the server-driven delta stream provides its own
    sequencing, layering cursor filtering on top would over-constrain
    the read."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="mutually exclusive"):
        c.read_table_metadata(
            "Customers",
            {"delta_tracking": "enabled", "cursor_field": "ModifiedAt"},
        )


@responses.activate
def test_delta_invalid_setting_raises():
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="auto, enabled, disabled"):
        c.read_table_metadata("Customers", {"delta_tracking": "sometimes"})


@responses.activate
def test_delta_disabled_default_sends_no_prefer_header():
    """Default ``delta_tracking=disabled`` means existing snapshot /
    cursor pipelines see zero behavior change and zero extra HTTP cost.
    No ``Prefer`` header is sent on any request."""
    _mock_metadata()
    captured_headers = []

    def _callback(request):
        captured_headers.append(dict(request.headers))
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    c.read_table("Customers", None, {})
    assert all("Prefer" not in h for h in captured_headers)


@responses.activate
def test_delta_auto_probe_positive_routes_through_delta_path():
    """``delta_tracking=auto`` probes once. If the server returns
    ``Preference-Applied: odata.track-changes``, the connector marks
    the table delta-capable and reads via the delta path."""
    _mock_metadata()
    call_count = {"n": 0}

    def _callback(request):
        call_count["n"] += 1
        # Probe call ($top=1) — return Preference-Applied to acknowledge.
        if call_count["n"] == 1:
            assert request.headers.get("Prefer") == "odata.track-changes"
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(_delta_bootstrap_body([])),
            )
        # Bootstrap call (after probe) — same header, but tests above
        # only care that the read path was reached.
        return (
            200,
            {"Preference-Applied": "odata.track-changes"},
            json.dumps(
                _delta_bootstrap_body(
                    [{"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"}]
                )
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    records, offset = c.read_table("Customers", None, {"delta_tracking": "auto"})
    rows = list(records)
    # Bootstrap row plus synthetic columns.
    assert len(rows) == 1
    assert rows[0]["Id"] == 1
    assert rows[0]["_deleted"] is False
    assert "_lc_sequence" in rows[0]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_auto_probe_silent_ignore_falls_back():
    """Some servers accept the ``Prefer`` request, return data, but
    don't echo ``Preference-Applied``. The connector treats that as
    "not supported" and falls back to snapshot — silently, so the
    auto path stays usable without extra config."""
    _mock_metadata()
    call_count = {"n": 0}

    def _callback(request):
        call_count["n"] += 1
        # Probe: no Preference-Applied → probe says "not supported".
        # Snapshot follow-up: returns regular data.
        return (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, offset = c.read_table("Customers", None, {"delta_tracking": "auto"})
    rows = list(records)
    assert rows == [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]
    # Empty offset = snapshot mode. No delta_link in there.
    assert _drop_lb(offset) == {}


@responses.activate
def test_delta_auto_probe_transient_failure_records_nothing():
    """A transient failure during the ``delta_tracking=auto`` probe degrades
    that call to the snapshot/cursor path but caches NO verdict — the same
    definitive-only discipline as the other capability probes. Pinning
    ``False`` for the instance's lifetime on a momentary 503 would keep a
    delta-capable stream on the wrong path until the reader is recreated."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"error": "down"}, status=503)
    c = _make({"max_retries": "0", "retry_max_delay_seconds": "0"})
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is False
    assert not c._delta_capable  # transient → no verdict cached
    # The server recovers: the SAME instance re-probes and gets the verdict.
    responses.reset()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": []},
        headers={"Preference-Applied": "odata.track-changes"},
    )
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is True
    assert list(c._delta_capable.values()) == [True]  # definitive → cached


@responses.activate
def test_delta_auto_probe_408_is_transient_not_a_verdict():
    """A 408 sits outside the retry set, so ``_http_get`` RETURNS it rather
    than raising after the budget — the probe must classify it as transient
    (no verdict cached), not as a definitive "server doesn't acknowledge"."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"error": "timeout"}, status=408)
    c = _make()
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is False
    assert not c._delta_capable  # transient → no verdict cached, re-probes


@responses.activate
def test_delta_auto_probe_400_falls_back():
    """Servers can outright reject the ``Prefer`` header with 4xx. The
    probe surfaces False and the connector falls back to snapshot."""
    _mock_metadata()
    call_count = {"n": 0}

    def _callback(request):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Probe rejected.
            return (400, {}, '{"error": "Bad prefer"}')
        return (200, {}, '{"value": [{"Id": 7, "Name": "G", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, _ = c.read_table("Customers", None, {"delta_tracking": "auto"})
    assert [r["Id"] for r in list(records)] == [7]


@responses.activate
def test_delta_enabled_without_preference_applied_raises():
    """``delta_tracking=enabled`` is the user's positive assertion that
    the server supports it. If the bootstrap response is missing the
    ``Preference-Applied`` header, surface a clear error pointing at
    ``delta_tracking=disabled``."""
    _mock_metadata()
    # No Preference-Applied in the response → connector raises.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=_delta_bootstrap_body([]),
        status=200,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="Preference-Applied"):
        records, _ = c.read_table("Customers", None, {"delta_tracking": "enabled"})
        list(records)


@responses.activate
def test_delta_bootstrap_emits_full_snapshot_with_deleted_false():
    """Initial bootstrap call emits all current rows with
    ``_deleted=False`` and a monotonic ``_lc_sequence``. Offset is the
    server's first delta link."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=_delta_bootstrap_body(
            [
                {"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 2, "Name": "B", "ModifiedAt": "2024-02-01T00:00:00Z"},
            ]
        ),
        headers={"Preference-Applied": "odata.track-changes"},
    )
    c = _make()
    records, offset = c.read_table("Customers", None, {"delta_tracking": "enabled"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    assert all(r["_deleted"] is False for r in rows)
    # Sequences are strictly increasing per emit.
    seqs = [r["_lc_sequence"] for r in rows]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == 2
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_resume_emits_changes_and_removes_via_in_band_deleted_flag():
    """Resume call (offset has ``delta_link``) walks that URL. Regular
    entries become ``_deleted=False`` records, ``@removed`` entries
    become ``_deleted=True`` records carrying only the primary key."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "@odata.context": f"{SERVICE_URL}$metadata#Customers/$delta",
            "value": [
                {"Id": 5, "Name": "E", "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"@removed": {"reason": "deleted"}, "Id": 2},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert len(rows) == 2
    change, tombstone = rows
    assert change["Id"] == 5
    assert change["Name"] == "E"
    assert change["_deleted"] is False
    # Non-key columns ride the tombstone as EXPLICIT NULLs — the framework
    # parser rejects an ABSENT non-nullable column but accepts a null one.
    assert tombstone == {
        "Id": 2,
        "Name": None,
        "ModifiedAt": None,
        "_deleted": True,
        "_lc_sequence": tombstone["_lc_sequence"],
    }
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_resume_walks_nextlink_chain_to_captured_deltalink():
    """The delta response itself can paginate via ``@odata.nextLink``.
    The terminal page carries the new ``@odata.deltaLink`` — the
    connector follows the chain to completion before returning."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 10, "Name": "Ten", "ModifiedAt": "x"}],
            "@odata.nextLink": next_link,
        },
    )
    responses.add(
        responses.GET,
        next_link,
        json={
            "value": [{"Id": 11, "Name": "Eleven", "ModifiedAt": "y"}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [10, 11]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_no_op_response_preserves_prior_delta_link():
    """Graph-rotation guard: even when the server mints a fresh
    deltaLink on every response, an empty change set means "no
    progress" — the connector hands the prior link back so the
    framework sees ``end_offset == start_offset`` and AvailableNow can
    terminate."""
    _mock_metadata()
    # Server returns no records AND a rotated deltaLink. Without the
    # rotation guard the offset would advance and the framework would
    # commit forever.
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    assert list(records) == []
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_410_triggers_full_rebootstrap():
    """The server can expire a delta token (410 Gone). The connector
    re-bootstraps automatically: emits the fresh snapshot as
    ``_deleted=False`` rows and returns a brand-new delta link."""
    _mock_metadata()
    # First call: 410 on the stored delta link.
    responses.add(responses.GET, DELTA_LINK_V1, status=410)
    # Re-bootstrap: fresh snapshot via Prefer.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=_delta_bootstrap_body(
            [{"Id": 99, "Name": "Reborn", "ModifiedAt": "x"}],
            delta_link=DELTA_LINK_V2,
        ),
        headers={"Preference-Applied": "odata.track-changes"},
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [99]
    assert all(r["_deleted"] is False for r in rows)
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_sparse_entity_raises_runtimeerror():
    """OData v4 §11.4 lets the server return only the changed
    properties on an update. Applying that as-is would write NULLs over
    good values at the destination — silent corruption. The connector
    refuses sparse responses with an actionable error."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                # Missing "Name" and "ModifiedAt" — schema declares them.
                {"Id": 5},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="sparse entity"):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": DELTA_LINK_V1},
            {"delta_tracking": "enabled"},
        )
        list(records)


@responses.activate
def test_delta_sparse_check_runs_on_every_entity_not_just_the_first():
    """Mixed payloads are the norm for real delta services: full entities
    for creates, changed-properties-only for updates. A full-bodied create
    at the head of the batch must not wave the sparse update behind it
    through to a NULL-writing MERGE — the guard runs per entity."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                # Full entity first (a create) — the old first-entry-only
                # sampling stopped checking here.
                {"Id": 5, "Name": "E", "ModifiedAt": "2024-01-01T00:00:00Z"},
                # Sparse update behind it — missing ModifiedAt.
                {"Id": 6, "Name": "F"},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="sparse entity"):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": DELTA_LINK_V1},
            {"delta_tracking": "enabled"},
        )
        list(records)


@responses.activate
def test_delta_page_decode_retries_corrupt_200_body():
    """Delta pages get the same corrupt-200-body retry as cursor/snapshot
    pages (``_fetch_page_payload``): a truncated JSON body under load is
    retried with a fresh GET instead of hard-failing the stream."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        body='{"value": [{"Id": 99, "Name": "trunc',  # cut mid-serialization
        status=200,
        content_type="application/json",
    )
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 99, "Name": "ok", "ModifiedAt": "2024-01-01T00:00:00Z"}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [99]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_sparse_check_honors_select():
    """When the user restricts the projection via ``$select``, only the
    selected fields are expected in every delta entry. Returning only
    those (and nothing else) is no longer "sparse"."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                # Only Id + Name, matching the select clause exactly.
                {"Id": 5, "Name": "E"},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, _ = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled", "select": "Id,Name"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [5]
    # No exception — schema only requires Id + Name (+ synthetic columns).


@responses.activate
def test_delta_max_records_caps_at_page_boundary_and_stashes_next_link():
    """A long catch-up after a paused pipeline can return more rows than
    ``max_records_per_batch``. The connector caps at the **page boundary**
    (stops following ``@odata.nextLink``) and stashes the unfollowed link as
    the resume point. The cap must NOT truncate mid-page: the stashed link
    points at the NEXT page, so any rows dropped from the current page would
    never be re-fetched — permanent loss during bootstrap. The cap therefore
    overshoots by up to one server page instead."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                {"Id": 1, "Name": "A", "ModifiedAt": "x"},
                {"Id": 2, "Name": "B", "ModifiedAt": "x"},
                {"Id": 3, "Name": "C", "ModifiedAt": "x"},
            ],
            "@odata.nextLink": next_link,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled", "max_records_per_batch": "2"},
    )
    rows = list(records)
    # The whole cap-hit page is emitted (bounded overshoot, never loss).
    assert [r["Id"] for r in rows] == [1, 2, 3]
    # Offset carries both prior delta_link (fallback) AND next_link
    # (preferred resume point) — pagination stopped at the page boundary.
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1, "next_link": next_link}


@responses.activate
def test_delta_resume_via_next_link_continues_pagination():
    """After a cap-hit batch the next call's offset has ``next_link``.
    The connector resumes from that URL directly, no fresh ``Prefer``
    header, no probe."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    captured_headers = []

    def _callback(request):
        captured_headers.append(dict(request.headers))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": 3, "Name": "C", "ModifiedAt": "x"}],
                    "@odata.deltaLink": DELTA_LINK_V2,
                }
            ),
        )

    responses.add_callback(responses.GET, next_link, callback=_callback)
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"next_link": next_link, "delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [3]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}
    # Resume must not re-send the bootstrap-only Prefer header.
    assert all("Prefer" not in h for h in captured_headers)


@responses.activate
def test_delta_dispatch_recognizes_delta_link_offset_without_enabled_flag():
    """A pipeline started with ``delta_tracking=enabled`` checkpoints a
    delta-shaped offset; if the next run loses that table option (config
    drift, partial rollout) the dispatch must still take the delta path
    based on the offset shape alone — losing the offset shape and
    treating it as a fresh snapshot would re-fetch the whole table."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    # No delta_tracking option set, but the offset carries a delta_link.
    records, offset = c.read_table("Customers", {"delta_link": DELTA_LINK_V1}, {})
    assert list(records) == []
    # Rotation guard: prior link preserved on no-op.
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_walk_guard_stops_self_referential_link():
    """The delta walk guards against a self-referential @odata.nextLink: the
    server points the continuation back at the same URL. The self-loop is
    detected before re-fetching, and — since the broken chain produced
    records with no advanced change cursor — the no-progress guard raises
    rather than emitting the same records against the same offset forever
    (round-30: previously this returned rows + the unchanged prior link,
    which was byte-for-byte the infinite-churn shape)."""
    _mock_metadata()
    calls = []

    def cb(req):
        calls.append(req.url)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": 10, "Name": "x", "ModifiedAt": "t"}],
                    "@odata.nextLink": DELTA_LINK_V1,
                }
            ),
        )

    responses.add_callback(responses.GET, DELTA_LINK_V1, callback=cb)
    c = _make()
    with pytest.raises(RuntimeError, match="no terminal @odata.deltaLink"):
        records, _ = c.read_table(
            "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
        )
        list(records)
    assert len(calls) == 1  # self-loop detected before re-fetching


@responses.activate
def test_pin_false_on_one_table_leaves_sibling_table_verdict_intact():
    """The snapshot purge is table-scoped: pinning ``expand_contained=false`` on
    one contained table must not evict a SIBLING table's cached ``expand_ok``
    (the drop of a per-table verdict touches only its own key)."""
    _mock_probe_metadata()
    c = _make()
    c._store_capability("expand_ok", True, table_name="Roots__Mids__Leaves")
    c._store_capability("expand_ok", True, table_name="Roots__Mids")
    # Read the two-segment sibling pinned false → purges only its own entry.
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    list(
        c.read_table("Roots__Mids", {}, {"pagination": "nextlink", "expand_contained": "false"})[0]
    )
    assert c._cached_capability("expand_ok", table_name="Roots__Mids") is None
    assert c._cached_capability("expand_ok", table_name="Roots__Mids__Leaves") is True


@responses.activate
def test_capability_cache_shares_batch_verdict_across_instances():
    """``batch_ok`` (and the discovered ``batch_size_ok`` cap) reach a fresh
    instance through the process cache — the capability POST runs once per
    process, not once per framework-recreated reader."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={"responses": [{"id": "0", "status": 200, "body": {"value": []}}]},
    )
    c1 = _make()
    assert c1._verify_batch_support(["Roots"], {}) is True
    c1._shrink_batch_cap(100)  # discovered cap must travel with the verdict
    discovered_cap = c1.__dict__["_batch_size_cap"]
    n_posts = sum(1 for call in responses.calls if call.request.method == "POST")
    assert n_posts == 1

    c2 = _make()
    assert c2._verify_batch_support(["Roots"], {}) is True
    assert c2.__dict__["_batch_size_cap"] == discovered_cap
    assert sum(1 for call in responses.calls if call.request.method == "POST") == n_posts


@responses.activate
def test_capability_cache_definitive_false_survives_process_cache_clear():
    """A definitive fail is shared too, and the on-disk JSON mirror covers a
    fresh process (simulated by clearing BOTH process-memory dicts — the verdict
    cache and its mtime memo — while leaving the disk file intact): the fresh
    'process' loads the verdict from the file instead of re-probing."""
    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_CACHE,
        _CAPABILITY_DISK_MTIME,
    )

    responses.post(f"{SERVICE_URL}$batch", json={"error": "no batch"}, status=405)
    c1 = _make()
    assert c1._verify_batch_support(["Roots"], {}) is False
    assert sum(1 for call in responses.calls if call.request.method == "POST") == 1

    # Fresh process = empty verdict cache AND empty mtime memo (a real fork
    # inherits both via copy-on-write; a brand-new process has neither). The
    # disk file is untouched, so the reload rehydrates the verdict from it.
    _CAPABILITY_CACHE.clear()
    _CAPABILITY_DISK_MTIME.clear()
    c2 = _make()
    assert c2._verify_batch_support(["Roots"], {}) is False
    assert sum(1 for call in responses.calls if call.request.method == "POST") == 1


def test_capability_cache_disk_merge_unions_per_table_maps():
    """The disk merge must union BOTH per-table maps (``expand_ok`` AND
    ``cursor_probe_ok``) table-by-table, process verdicts winning. A plain
    ``setdefault`` would shadow a sibling worker's whole on-disk map as soon as
    this process holds ANY table's verdict — re-probing exactly what the merge
    exists to prevent."""
    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_DISK_MTIME,
        _capability_cache_flush,
    )

    c = _make()
    # This process already holds table-A verdicts for both per-table maps.
    c._store_capability("cursor_probe_ok", False, table_name="A__Path")
    c._store_capability("expand_ok", False, table_name="A__Tbl")
    # A sibling worker's on-disk state: table A plus its own table-B verdicts.
    _capability_cache_flush(
        c.service_url,
        json.dumps(
            {
                "cursor_probe_ok": {"A__Path": True, "B__Path": True},
                "expand_ok": {"A__Tbl": True, "B__Tbl": True},
                "batch_ok": True,
            }
        ),
    )
    _CAPABILITY_DISK_MTIME.clear()  # force the next load to re-merge the file
    # The sibling's table-B verdicts merged in; table-A keeps the process value.
    assert c._cached_capability("cursor_probe_ok", table_name="B__Path") is True
    assert c._cached_capability("cursor_probe_ok", table_name="A__Path") is False
    assert c._cached_capability("expand_ok", table_name="B__Tbl") is True
    assert c._cached_capability("expand_ok", table_name="A__Tbl") is False
    assert c._cached_capability("batch_ok") is True


def test_capability_cache_concurrent_access_is_thread_safe(monkeypatch):
    """The shared process cache is read-modify-written and serialized from
    multiple threads: concurrent streaming queries on one driver share
    ``_CAPABILITY_CACHE`` by ``service_url``, and ``json.dump`` / the load-merge
    iterate that live dict. Under ``_CAPABILITY_LOCK`` that's safe; without it a
    mutation landing mid-iteration trips "dictionary changed size during
    iteration".

    On the standard GIL build the C JSON encoder holds the GIL across a dict
    encode, so the race can't surface with default settings. To exercise the real
    hazard here (and to stand in for a free-threaded interpreter, PEP 703), force
    the *pure-Python* JSON encoder — whose ``for k, v in dct.items()`` yields the
    GIL between elements — and drop the thread-switch interval so a switch lands
    mid-encode. With the lock this stays green; remove the lock and it reliably
    raises "dictionary changed size during iteration"."""
    import json as _json
    import sys
    import threading

    from databricks.labs.community_connector.sources.odata.odata import _capability_cache_drop

    monkeypatch.setattr(_json.encoder, "c_make_encoder", None)  # force pure-Python dump

    c = _make()
    errors: list = []

    def worker(base: int) -> None:
        try:
            for i in range(400):
                tbl = f"T{base}_{i % 16}"
                c._store_capability("expand_ok", True, table_name=tbl)
                c._cached_capability("expand_ok", table_name=tbl)
                c._store_capability("batch_ok", True)  # server-wide churn
                if i % 4 == 0:
                    _capability_cache_drop(c.service_url, {"expand_ok"}, table_name=tbl)
        except Exception as exc:  # RuntimeError from an unlocked race, etc.
            errors.append(exc)

    prev_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)  # switch aggressively so a mutation lands mid-encode
    try:
        threads = [threading.Thread(target=worker, args=(b,)) for b in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(prev_interval)
    assert not errors, errors


def test_purge_nonauto_table_verdicts_is_table_scoped_and_mode_gated():
    """``_purge_nonauto_table_verdicts`` drops the per-table ``expand_ok`` /
    ``cursor_probe_ok`` only when the governing option is non-``auto``, and only
    for the named table."""
    c = _make()
    c._store_capability("expand_ok", True, table_name="Roots__Mids__Leaves")
    c._store_capability("expand_ok", True, table_name="Roots__Mids")

    # auto (unset) → no purge.
    c._purge_nonauto_table_verdicts("Roots__Mids__Leaves", {"cursor_probe": "false"})
    assert c._cached_capability("expand_ok", table_name="Roots__Mids__Leaves") is True

    # pinned false → drops only this table's entry.
    c._purge_nonauto_table_verdicts("Roots__Mids__Leaves", {"expand_contained": "false"})
    assert c._cached_capability("expand_ok", table_name="Roots__Mids__Leaves") is None
    assert c._cached_capability("expand_ok", table_name="Roots__Mids") is True


@responses.activate
def test_nonauto_clears_recorded_preflight_verdicts():
    """A non-``auto`` option scrubs its recorded preflight verdict from the
    outgoing offset, so re-selecting ``auto`` later re-runs the preflight:
    ``cursor_probe`` non-auto drops ``cursor_probe_ok``; ``contained_fetch``
    non-auto drops the ``$batch`` verdicts (``batch_ok`` / ``batch_size_ok``)."""
    _mock_probe_metadata()
    c = _make()

    # contained_fetch=single (non-auto): a previously-recorded $batch verdict in
    # the incoming offset is not carried forward.
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    _, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z", "batch_ok": True, "batch_size_ok": 200},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "false",  # non-auto → drops cursor_probe_ok (absent here)
            "contained_fetch": "single",  # non-auto → drops batch_ok / batch_size_ok
            "pagination": "nextlink",
        },
    )
    assert "batch_ok" not in offset
    assert "batch_size_ok" not in offset


@responses.activate
def test_capability_verdicts_thread_through_offset():
    """The OR / $batch capability verdicts ride the resume offset so a reader
    the framework recreates each microbatch skips re-probing. Seed-from-offset,
    seeded-verdict-skips-the-probe, merge-into-offset, and never-overwrite."""
    _mock_probe_metadata()
    c = _make()
    # Seed instance caches from a prior batch's offset.
    c._seed_capability_caches(
        PROBE_TABLE, None, {"cursor": "x", "or_filter_ok": False, "batch_ok": True}
    )
    assert c.__dict__["_or_filter_ok"] is False
    assert c.__dict__["_batch_supported"] is True
    # A seeded OR verdict is returned WITHOUT issuing a probe (cached short-circuit).
    assert c._verify_or_filter_support("https://x/Coll", ["a", "b"], {"a": 1, "b": 2}) is False
    assert not responses.calls  # no network for the seeded verdict
    # Merge threads the verdicts back into a fresh offset...
    merged = c._merge_capability_caches({"cursor": "y"})
    assert merged == {"cursor": "y", "or_filter_ok": False, "batch_ok": True}
    # ...but never overwrites a value a read path already wrote.
    assert c._merge_capability_caches({"batch_ok": True, "or_filter_ok": True}) == {
        "batch_ok": True,
        "or_filter_ok": True,
    }
    # Single-key $orderby never builds an OR → never probed (short-circuits True).
    c.__dict__.pop("_or_filter_ok", None)
    assert c._verify_or_filter_support("https://x/Coll", ["a"], {"a": 1}) is True
    assert not responses.calls


# ---------------------------------------------------------------------------
# Round-29 fixes: delta $top removal + maxpagesize, entity-reference
# tombstones, next_link-410 fallback, delta no-progress, partition batch
# null tolerance, select validation, connection-int validation
# ---------------------------------------------------------------------------


@responses.activate
def test_delta_bootstrap_sends_no_top_and_maps_page_size_to_maxpagesize():
    """OData $top is a TOTAL-RESULT limit (§11.2.5.3): sent on a delta
    bootstrap it ends change tracking at page_size rows and silently drops
    the rest of the table forever. The bootstrap must carry NO $top; an
    explicit page_size rides Prefer: odata.maxpagesize instead."""
    _mock_metadata()
    seen = {}

    def _cb(request):
        seen["url"] = request.url
        seen["prefer"] = request.headers.get("Prefer", "")
        return (
            200,
            {"Preference-Applied": "odata.track-changes"},
            json.dumps(_delta_bootstrap_body([{"Id": 1, "Name": "A", "ModifiedAt": "x"}])),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table(
        "Customers", None, {"delta_tracking": "enabled", "page_size": "500"}
    )
    assert [r["Id"] for r in list(records)] == [1]
    assert "$top" not in seen["url"] and "%24top" not in seen["url"]
    assert "odata.track-changes" in seen["prefer"]
    assert "odata.maxpagesize=500" in seen["prefer"]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_bootstrap_default_pagination_sends_no_top():
    """Even under the default pagination=auto (which injects a client-paging
    page_size for other reads), the delta bootstrap must carry no $top and
    no maxpagesize (the user asked for nothing)."""
    _mock_metadata()
    seen = {}

    def _cb(request):
        seen["url"] = request.url
        seen["prefer"] = request.headers.get("Prefer", "")
        return (
            200,
            {"Preference-Applied": "odata.track-changes"},
            json.dumps(_delta_bootstrap_body([{"Id": 1, "Name": "A", "ModifiedAt": "x"}])),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, _ = c.read_table("Customers", None, {"delta_tracking": "enabled"})
    list(records)
    assert "$top" not in seen["url"] and "%24top" not in seen["url"]
    assert "maxpagesize" not in seen["prefer"]


@responses.activate
def test_delta_tombstone_key_parsed_from_entity_reference():
    """A spec-shaped tombstone carries its key only in @odata.id — the
    connector must parse it (typed: int PK coerced so it MERGE-matches the
    upserts), not emit a keyless no-op tombstone."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                {"@removed": {"reason": "deleted"}, "@odata.id": f"{SERVICE_URL}Customers(2)"},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
    )
    (tomb,) = list(records)
    assert tomb["Id"] == 2 and isinstance(tomb["Id"], int)
    assert tomb["_deleted"] is True
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_v40_deleted_entity_context_is_tombstone_not_sparse_error():
    """A v4.0-format deleted entry ($deletedEntity context + id, no
    @removed) must become a tombstone — pre-fix it was misread as a regular
    entity and tripped the sparse-entity guard with a misleading
    'partial updates' error."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                {
                    "@odata.context": f"{SERVICE_URL}$metadata#Customers/$deletedEntity",
                    "id": "Customers(3)",
                    "reason": "deleted",
                },
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, _ = c.read_table(
        "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
    )
    (tomb,) = list(records)
    assert tomb["Id"] == 3
    assert tomb["_deleted"] is True


@responses.activate
def test_delta_tombstone_without_resolvable_key_raises():
    """A tombstone with neither inline keys nor a parsable entity reference
    would MERGE against nothing — the deletion silently lost. Raise."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"@removed": {"reason": "deleted"}}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="resolvable primary key"):
        records, _ = c.read_table(
            "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
        )
        list(records)


def test_tombstone_keys_from_id_shapes():
    """Unit coverage of the entity-reference parser: composite named keys,
    quoted-string un-escaping, bare guids, absolute URLs, and non-matching
    shapes returning None."""
    c = _make()
    types = {"OrderID": "Edm.Int32", "Lang": "Edm.String", "G": "Edm.Guid"}
    assert c._tombstone_keys_from_id(
        "Orders(OrderID=1,Lang='en''x')", ["OrderID", "Lang"], types
    ) == {"OrderID": 1, "Lang": "en'x"}
    assert c._tombstone_keys_from_id(f"https://x/svc/Accounts({_GUID})?x=1", ["G"], types) == {
        "G": _GUID
    }
    assert c._tombstone_keys_from_id("Customers('A,B')", ["Id"], {}) == {"Id": "A,B"}
    assert c._tombstone_keys_from_id("Customers", ["Id"], {}) is None
    assert c._tombstone_keys_from_id("Orders(OrderID=1)", ["OrderID", "Lang"], types) is None
    assert c._tombstone_keys_from_id("Customers(7)", ["A", "B"], {}) is None


@responses.activate
def test_delta_next_link_410_falls_back_to_retained_delta_link():
    """A 410 on the parked mid-pagination next_link must replay the retained
    prior delta_link (changes-since window) — not re-bootstrap the whole
    entity set."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    responses.add(responses.GET, next_link, status=410)
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 9, "Name": "N", "ModifiedAt": "z"}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"next_link": next_link, "delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    assert [r["Id"] for r in list(records)] == [9]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}
    # The plain entity-set bootstrap GET never happened.
    assert not any(call.request.url.rstrip("/").endswith("Customers") for call in responses.calls)


@responses.activate
def test_delta_same_link_with_records_raises_no_progress():
    """Change records + the SAME deltaLink as the prior batch would re-read
    that change set forever — raise like the cursor paths do."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 4, "Name": "D", "ModifiedAt": "w"}],
            "@odata.deltaLink": DELTA_LINK_V1,  # did not advance
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="SAME @odata.deltaLink"):
        records, _ = c.read_table(
            "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
        )
        list(records)


# ---------------------------------------------------------------------------
# Round-30 fixes: per-user cache hardening, verdict reset paths, pass-only
# expand_ok, root-wins typing, Edm.Stream delta exclusion
# ---------------------------------------------------------------------------


def test_cache_paths_are_per_user_and_reader_checks_ownership(monkeypatch):
    """Both tempdir caches previously sat at predictable world-writable paths
    keyed only by service_url — the pickle one feeds pickle.load (arbitrary
    code execution if pre-planted by another local user), the JSON one could
    force an unverified $expand read. Paths now embed the owner tag, and the
    readers refuse foreign-owned files."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    tag = odata_mod._cache_owner_tag()
    assert f"_{tag}_" in odata_mod._metadata_cache_path(SERVICE_URL)
    assert f"_{tag}_" in odata_mod._capability_cache_path(SERVICE_URL)

    # Wiring: a file the ownership check rejects is never unpickled.
    c = _make()
    path = odata_mod._metadata_cache_path(SERVICE_URL)
    import pickle as _pickle
    from xml.etree import ElementTree as _ET

    with open(path, "wb") as fh:
        _pickle.dump((METADATA_XML, _ET.fromstring(METADATA_XML)), fh)
    try:
        monkeypatch.setattr(odata_mod, "_cache_file_owned_by_us", lambda p: False)
        assert c._read_metadata_file_cache() is None
        monkeypatch.setattr(odata_mod, "_cache_file_owned_by_us", lambda p: True)
        assert c._read_metadata_file_cache() is not None
    finally:
        import os as _os

        _os.remove(path)


@responses.activate
def test_delta_stream_property_not_expected_in_payload():
    """Edm.Stream values are media references the JSON payload never carries
    (§11.2.4): the sparse-entity guard must not demand them — pre-fix every
    healthy entity on a stream-bearing type failed delta with a misleading
    'partial updates' error. A genuinely sparse entity still raises."""
    responses.get(f"{SERVICE_URL}$metadata", body=STREAM_METADATA_XML, status=200)
    delta_link = f"{SERVICE_URL}Docs?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={
            "value": [{"Id": 1, "Name": "ok"}],  # no Content — always absent
            "@odata.deltaLink": f"{SERVICE_URL}Docs?$deltatoken=t2",
        },
    )
    c = _make()
    records, _ = c.read_table("Docs", {"delta_link": delta_link}, {"delta_tracking": "enabled"})
    (row,) = list(records)
    assert row["Id"] == 1 and row["_deleted"] is False

    responses.add(
        responses.GET,
        f"{SERVICE_URL}Docs?$deltatoken=t2",
        json={
            "value": [{"Id": 2}],  # missing Name — genuinely sparse
            "@odata.deltaLink": f"{SERVICE_URL}Docs?$deltatoken=t3",
        },
    )
    with pytest.raises(RuntimeError, match="missing"):
        records, _ = c.read_table(
            "Docs",
            {"delta_link": f"{SERVICE_URL}Docs?$deltatoken=t2"},
            {"delta_tracking": "enabled"},
        )
        list(records)


def test_capability_flush_writes_private_file():
    """Wiring: the capability mirror goes through the hardened writer, so the
    published file carries owner-only permissions (pre-fix: umask default)."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    path = odata_mod._capability_cache_path(SERVICE_URL)
    odata_mod._capability_cache_flush(SERVICE_URL, "{}")
    try:
        assert (os.stat(path).st_mode & 0o777) == 0o600
    finally:
        os.remove(path)


def test_cache_ownership_check_uses_lstat(tmp_path):
    """The ownership check previously followed symlinks (os.stat): a foreign
    symlink pointing at a victim-owned file passed. lstat judges the link
    itself — pinned via a dangling symlink, where stat raises (False) but
    lstat sees our own link (True; the subsequent open just misses)."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    dangling = tmp_path / "dangling"
    dangling.symlink_to(tmp_path / "nonexistent-target")
    assert odata_mod._cache_file_owned_by_us(str(dangling)) is True


@responses.activate
def test_delta_tombstone_parses_against_non_nullable_schema():
    """A tombstone carries only its keys, but the framework parser rejects
    a non-nullable column that is ABSENT (while accepting an explicit
    null) — pre-fix the first delete on any schema with a
    Nullable="false" non-key property killed the batch. Tombstones are
    now padded with explicit NULLs for every remaining schema column."""
    from databricks.labs.community_connector.libs.utils import parse_value

    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_METADATA_XML, status=200)
    delta_link = f"{SERVICE_URL}Customers?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={
            "value": [{"Id": 2, "@removed": {"reason": "deleted"}}],
            "@odata.deltaLink": f"{SERVICE_URL}Customers?$deltatoken=t2",
        },
    )
    c = _make()
    schema = c.get_table_schema("Customers", {"delta_tracking": "enabled"})
    records, _ = c.read_table(
        "Customers", {"delta_link": delta_link}, {"delta_tracking": "enabled"}
    )
    (tombstone,) = list(records)
    assert tombstone["Name"] is None and tombstone["ModifiedAt"] is None
    # The framework must accept the padded record against the declared
    # (non-nullable Name) schema — this is the actual pre-fix crash site.
    parsed = parse_value(tombstone, schema)
    assert parsed["Id"] == 2 and parsed["_deleted"] is True


@responses.activate
def test_delta_auto_verdict_shared_across_instances():
    """Schema inference and the streaming read run in different forked
    workers; an instance-only probe verdict lets a flapping server desync
    the declared schema from the emitted rows. The definitive verdict now
    rides the process/file capability cache: a second instance resolves
    delta WITHOUT re-probing (no probe mock is registered for it — an
    attempted probe would come back as no-verdict and resolve False)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": []},
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c1 = _make()
    assert c1._delta_active_for("Customers", {"delta_tracking": "auto"}) is True
    responses.reset()  # no $metadata, no probe endpoint from here on
    c2 = _make()
    c2._metadata = c1._metadata  # metadata via cache; probe is the question
    assert c2._delta_active_for("Customers", {"delta_tracking": "auto"}) is True


@responses.activate
def test_delta_ok_purged_on_explicit_setting():
    """`delta_ok` previously had no reset path — asymmetric with
    `expand_ok`/`cursor_probe_ok`, whose explicit non-auto values purge the
    shared cache entry so a later switch back to auto re-probes."""
    _mock_metadata()
    c = _make()
    c._store_capability("delta_ok", False, table_name="Customers")
    assert c._delta_active_for("Customers", {"delta_tracking": "disabled"}) is False
    assert c._cached_capability("delta_ok", table_name="Customers") is None
    c._store_capability("delta_ok", False, table_name="Customers")
    assert c._delta_active_for("Customers", {"delta_tracking": "enabled"}) is True
    assert c._cached_capability("delta_ok", table_name="Customers") is None
    # auto keeps (and uses) the verdict.
    c._store_capability("delta_ok", True, table_name="Customers")
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is True
    assert c._cached_capability("delta_ok", table_name="Customers") is True


@responses.activate
def test_delta_auto_fallback_stamps_delta_ok_into_offset():
    """A definitive negative delta probe under ``delta_tracking=auto`` pins
    the fallback decision into the outgoing offset (``delta_ok: false``), so
    the stream's read shape can't flip later (see the offset-wins test
    below)."""
    _mock_metadata()
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Customers",
        callback=lambda _r: (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}'),
    )
    c = _make()
    records, offset = c.read_table("Customers", {}, {"delta_tracking": "auto"})
    list(records)
    assert offset.get("delta_ok") is False


@responses.activate
def test_delta_auto_offset_verdict_wins_over_flapping_server():
    """An offset carrying ``delta_ok: false`` keeps the stream on the
    fallback path even when the server NOW acknowledges the delta probe
    (Preference-Applied flap after the 15-min shared cache expired). Without
    the pin, the read would flip onto the delta path mid-stream and emit
    ``_deleted``/``_lc_sequence`` columns the setup-frozen schema never
    declared — the framework parser drops them silently and a tombstone
    MERGEs as a live all-null row."""
    _mock_metadata()
    prefer_seen = {"n": 0}

    def _cb(request):
        if request.headers.get("Prefer"):
            prefer_seen["n"] += 1
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(_delta_bootstrap_body([{"Id": 9, "Name": "Z", "ModifiedAt": "y"}])),
            )
        return (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table("Customers", {"delta_ok": False}, {"delta_tracking": "auto"})
    rows = list(records)
    # Snapshot fallback held: no probe ran, no synthetic columns emitted.
    assert prefer_seen["n"] == 0
    assert rows and all("_deleted" not in r and "_lc_sequence" not in r for r in rows)
    assert offset.get("delta_ok") is False


@responses.activate
def test_delta_ok_scrubbed_on_explicit_setting():
    """Explicit ``delta_tracking`` (or the disabled default) scrubs a
    persisted ``delta_ok`` from the outgoing offset — the same non-auto
    reset discipline as every other verdict, so returning to ``auto``
    re-probes."""
    _mock_metadata()
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Customers",
        callback=lambda _r: (
            200,
            {},
            '{"value": [{"Id": 2, "Name": "B", "ModifiedAt": "2024-05-01T00:00:00Z"}]}',
        ),
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-01-01T00:00:00Z", "delta_ok": False},
        {"cursor_field": "ModifiedAt"},
    )
    list(records)
    assert "delta_ok" not in offset


@responses.activate
def test_delta_page_with_both_links_continues_to_next_page():
    """A spec-violating page carrying BOTH @odata.nextLink and
    @odata.deltaLink must follow the continuation (stopping at the deltaLink
    silently dropped every trailing page) while retaining the deltaLink."""
    _mock_metadata()
    page2_url = f"{SERVICE_URL}Customers?$skiptoken=p2"
    call_n = {"n": 0}

    def _cb(request):
        call_n["n"] += 1
        if "skiptoken=p2" in request.url:
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(
                    _delta_bootstrap_body(
                        [{"Id": 2, "Name": "B", "ModifiedAt": "y"}],
                        delta_link=f"{SERVICE_URL}Customers?$deltatoken=tok-final",
                    )
                ),
            )
        body = _delta_bootstrap_body(
            [{"Id": 1, "Name": "A", "ModifiedAt": "x"}], next_link=page2_url
        )
        # Both links on page 1 (delta_link default + explicit next_link).
        return (200, {"Preference-Applied": "odata.track-changes"}, json.dumps(body))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]  # page 2 delivered
    # The terminal page's deltaLink wins.
    assert offset["delta_link"].endswith("tok-final")


@responses.activate
def test_delta_relationship_link_entries_skipped_not_sparse_error():
    """v4.01 relationship-change entries ($link / $deletedLink contexts,
    carrying source/relationship/target) are valid delta shapes — they used
    to fall into the regular-entity branch and die in the sparse-entity
    guard with a misleading diagnosis. They're skipped (nav properties are
    never ingested on this path)."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [
            {"Id": 1, "Name": "A", "ModifiedAt": "x"},
            {
                "@odata.context": f"{SERVICE_URL}$metadata#Customers/$deletedLink",
                "source": "Customers(1)",
                "relationship": "Orders",
                "target": "Orders(9)",
            },
            {
                "@odata.context": f"{SERVICE_URL}$metadata#Customers/$link",
                "source": "Customers(1)",
                "relationship": "Orders",
                "target": "Orders(10)",
            },
        ]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]  # link entries skipped, no raise


@responses.activate
def test_delta_cross_set_tombstone_refuses_wrong_key():
    """A tombstone whose entity reference names a DIFFERENT entity set
    (…/Suppliers(77) in a Customers feed) must not delete Customers 77 —
    the reference is treated as unresolvable and the loud keyless-tombstone
    raise fires. A container-qualified same-set reference still resolves."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [{"@removed": {"reason": "deleted"}, "@odata.id": f"{SERVICE_URL}Suppliers(77)"}]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="no resolvable primary key"):
        records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
        list(records)
    # Dotted (container-qualified) same-set form still resolves.
    keys = c._tombstone_keys_from_id(
        "Container.Customers(5)", ["Id"], {"Id": "Edm.Int32"}, entity_set="Customers"
    )
    assert keys == {"Id": 5}


@responses.activate
def test_delta_property_scoped_annotations_stripped():
    """Prop@odata.type-style property-scoped control info must not survive
    into emitted records."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [{"Id": 1, "Name": "A", "Name@odata.type": "#String", "ModifiedAt": "x"}]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    (row,) = list(records)
    assert "Name@odata.type" not in row and row["Name"] == "A"


@responses.activate
def test_delta_removed_with_link_context_still_tombstones():
    """A contradictory entry carrying BOTH @removed and a $deletedLink
    context takes the TOMBSTONE branch (keys resolve-or-raise — loud, never
    a silently dropped delete)."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [
            {
                "@removed": {"reason": "deleted"},
                "@odata.context": f"{SERVICE_URL}$metadata#Customers/$deletedLink",
                "@odata.id": f"{SERVICE_URL}Customers(5)",
            }
        ]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    (row,) = list(records)
    assert row["Id"] == 5 and row["_deleted"] is True


# ---------------------------------------------------------------------------
# Round 42 — fallback-shape delta pin, keyless-parent preflight 3-tuple,
# $batch probe id echo, _batch_relative origin normalization, header-name
# validation, curated origin errors, delta_ok in the no-progress strip
# ---------------------------------------------------------------------------


@responses.activate
def test_delta_auto_snapshot_shaped_offset_pins_fallback_without_stamp():
    """A NON-empty fallback-shaped offset (``snapshot_done``, no ``delta_ok``
    — the first batch's probe was transient, so nothing was stamped) must pin
    the fallback by SHAPE: it proves earlier batches ran the cursor/snapshot
    shape against the setup-frozen schema. Without the pin, a later batch's
    re-probe (recovered transient / Preference-Applied flap) flips the stream
    ONTO the delta path mid-stream — emitting ``_deleted``/``_lc_sequence``
    columns the frozen schema never declared (a tombstone then MERGEs as a
    live row) and committing a sticky ``delta_link`` offset."""
    _mock_metadata()
    prefer_seen = {"n": 0}

    def _cb(request):
        if request.headers.get("Prefer"):
            prefer_seen["n"] += 1
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(_delta_bootstrap_body([{"Id": 9, "Name": "Z", "ModifiedAt": "y"}])),
            )
        return (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table("Customers", {"snapshot_done": True}, {"delta_tracking": "auto"})
    rows = list(records)
    assert prefer_seen["n"] == 0  # no delta probe ran at all
    assert rows == []  # the quiesced snapshot stays quiesced
    assert "delta_link" not in offset and "next_link" not in offset
    assert offset.get("snapshot_done") is True
    # The pin is persisted so framework-recreated instances inherit it.
    assert offset.get("delta_ok") is False


@responses.activate
def test_delta_stored_link_non_410_4xx_curated():
    """Real gateways answer 404/400 (not the spec's 410) for an expired
    delta token; the checkpoint pins the link, so a bare HTTPError re-raised
    forever was an undiagnosable dead-end. Now a curated error names the
    cause and the full-refresh remedy — and does NOT auto-rebootstrap."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"error": {"message": "token not found"}},
        status=404,
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="full refresh"):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": f"{SERVICE_URL}Customers?$deltatoken=expired"},
            {"delta_tracking": "enabled"},
        )
        list(records)


@responses.activate
def test_delta_fresh_midwalk_link_404_not_blamed_on_stored_token():
    """A fresh @odata.nextLink minted by THIS walk's response that 404s must
    keep the bare HTTPError — the round-45 curation claimed the STORED
    token expired and prescribed a full refresh, the wrong remedy for a
    link that isn't persisted anywhere."""
    _mock_metadata()

    def _customers(request):
        if "$deltatoken=stored" in request.url:
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}],
                        "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=fresh2",
                    }
                ),
            )
        return (404, {}, '{"error": {"message": "page gone"}}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make()
    with pytest.raises(requests.HTTPError):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": f"{SERVICE_URL}Customers?$deltatoken=stored"},
            {"delta_tracking": "enabled"},
        )
        list(records)


# ---------------------------------------------------------------------------
# Round 47 — capability cache LRU cap (memory + disk-inode bound)
# ---------------------------------------------------------------------------


def test_capability_cache_caps_entries_and_sweeps_disk():
    """`_CAPABILITY_CACHE` used to grow one dict entry + one mtime-memo entry
    + one tempdir file per distinct service_url for the driver's whole
    lifetime (its sibling _METADATA_CACHE was capped, this one wasn't). It's
    now bounded: creating > cap distinct services evicts oldest-created
    entries and deletes their on-disk mirrors."""

    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_CACHE,
        _CAPABILITY_CACHE_MAX_SERVICES,
        _CAPABILITY_DISK_MTIME,
        _capability_cache_path,
        _capability_cache_store,
        _clear_capability_cache,
    )

    _clear_capability_cache()
    n = _CAPABILITY_CACHE_MAX_SERVICES + 50
    paths = []
    for i in range(n):
        svc = f"https://cap-r47-{i}.example.com/odata/"
        paths.append(_capability_cache_path(svc))
        _capability_cache_store(svc, "batch_ok", True)
    try:
        # In-memory dict and mtime memo are both bounded at the cap.
        assert len(_CAPABILITY_CACHE) == _CAPABILITY_CACHE_MAX_SERVICES
        assert len(_CAPABILITY_DISK_MTIME) <= _CAPABILITY_CACHE_MAX_SERVICES
        # The most-recent cap services survive; the oldest are evicted.
        assert f"https://cap-r47-{n - 1}.example.com/odata/" in _CAPABILITY_CACHE
        assert "https://cap-r47-0.example.com/odata/" not in _CAPABILITY_CACHE
        # Evicted services' disk mirrors are deleted, not left as dead inodes.
        assert not os.path.exists(paths[0])
        assert os.path.exists(paths[-1])
        # Total on-disk mirror count is bounded at the cap too.
        assert sum(os.path.exists(p) for p in paths) == _CAPABILITY_CACHE_MAX_SERVICES
    finally:
        _clear_capability_cache()


def test_capability_cache_cap_survives_concurrent_first_touch():
    """Entry creation now happens under _CAPABILITY_LOCK (the eviction
    iterates the shared dict); concurrent first-touches of many services
    must not raise or blow past the cap."""
    import sys
    import threading

    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_CACHE,
        _CAPABILITY_CACHE_MAX_SERVICES,
        _capability_cache_store,
        _clear_capability_cache,
    )

    _clear_capability_cache()
    errors = []
    prev = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:

        def _hammer(tid):
            try:
                for i in range(200):
                    _capability_cache_store(
                        f"https://race-r47-{tid}-{i}.example.com/odata/", "batch_ok", True
                    )
            except Exception as exc:  # pragma: no cover - pre-fix would surface here
                errors.append(repr(exc))

        threads = [threading.Thread(target=_hammer, args=(t,)) for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(prev)
    assert errors == []
    assert len(_CAPABILITY_CACHE) == _CAPABILITY_CACHE_MAX_SERVICES
    _clear_capability_cache()


def test_capability_flush_skips_memo_for_evicted_service():
    """A store whose service was concurrently evicted must NOT re-plant an
    orphaned mtime memo: that would leak the memo AND make the next load
    short-circuit past the on-disk verdict (a redundant re-probe). The memo
    write is now gated on live cache membership."""
    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_DISK_MTIME,
        _capability_cache_flush,
        _capability_cache_path,
        _clear_capability_cache,
    )

    _clear_capability_cache()
    svc = "https://flush-r48.example.com/odata/"
    path = _capability_cache_path(svc)
    # Service NOT in the cache (simulating an eviction between load and flush).
    _capability_cache_flush(svc, json.dumps({"batch_ok": True}))
    try:
        assert os.path.exists(path)  # the file is still written (disk authoritative)
        assert path not in _CAPABILITY_DISK_MTIME  # but no orphaned memo planted
    finally:
        _clear_capability_cache()


@responses.activate
def test_delta_synthetic_column_collision_raises():
    """A source column literally named _deleted / _lc_sequence (legal: the
    OData v4 ABNF allows a leading underscore) collides with the delta
    synthetics. Under delta_tracking the schema must NOT silently emit
    duplicate columns / overwrite the source value — it raises a curated
    reserved-name error instead."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="reserved delta synthetic"):
        c.get_table_schema("Widgets", {"delta_tracking": "enabled"})


@responses.activate
def test_delta_collision_table_still_readable_without_delta():
    """The same table without delta tracking must schema/read fine — the
    guard only fires when the synthetics would actually be stamped."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    schema = c.get_table_schema("Widgets", {})
    names = [f.name for f in schema.fields]
    assert names.count("_deleted") == 1 and names.count("_lc_sequence") == 1


def test_capability_load_skips_memo_for_evicted_service():
    """Symmetric to the round-48 flush gate: if a sibling load evicts this
    service during load()'s lock-free file read, the merge must NOT re-plant
    an orphaned mtime memo — otherwise the next load short-circuits past the
    on-disk verdicts and returns an empty entry (silent verdict loss)."""
    from databricks.labs.community_connector.sources.odata import odata as _od

    _od._clear_capability_cache()
    _od._CAPABILITY_CACHE.clear()
    _od._CAPABILITY_DISK_MTIME.clear()
    svc = "https://memo-load-r49.example.com/odata/"
    path = _od._capability_cache_path(svc)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"batch_ok": True, "expand_ok": {"Orders": True}}))

    real_json_load = json.load
    fired = {"done": False}

    def _racing_load(fh, *a, **k):
        # Runs inside load()'s lock-free read window: simulate a sibling
        # load(other) that trips cap-eviction and pops THIS service + its memo.
        if not fired["done"]:
            fired["done"] = True
            with _od._CAPABILITY_LOCK:
                _od._CAPABILITY_CACHE.pop(svc, None)
                _od._CAPABILITY_DISK_MTIME.pop(path, None)
        return real_json_load(fh, *a, **k)

    json.load = _racing_load
    try:
        _od._capability_cache_load(svc)
    finally:
        json.load = real_json_load
    # No orphaned memo left behind for the evicted service...
    assert path not in _od._CAPABILITY_DISK_MTIME
    # ...so a fresh load re-reads and merges the persisted verdicts.
    entry2 = _od._capability_cache_load(svc)
    assert entry2.get("batch_ok") is True
    assert entry2.get("expand_ok") == {"Orders": True}
    _od._clear_capability_cache()
