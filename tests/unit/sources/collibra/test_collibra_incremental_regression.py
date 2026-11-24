"""Regression tests for the client-side incremental engine
(`_incremental_from_iter`), covering two data-loss defects found in PR #254
review (yyoli-db):

1. **assets, ID-ordered silent loss** — `/assets` sorts by `ID` (LAST_MODIFIED
   is rejected 400), so records do NOT arrive in `lastModifiedOn` order. A
   count-based batch cap would advance the watermark to the max cursor of an
   arbitrary ID-ordered slice, permanently skipping every not-yet-emitted asset
   modified at/before it. Fix: the assets path (`cursor_sorted=False`) ignores
   `max_records_per_batch` and drains the full collection per run.

2. **attributes/responsibilities tie-group split** — these are server-sorted
   `LAST_MODIFIED ASC`, but a hard cap that lands mid-way through a group of
   records sharing one `lastModifiedOn` would advance the watermark to that
   value and the next run's strict `> since` filter would skip the rest of the
   group. Fix: `max_records` is a *soft* cap — keep draining the boundary tie
   group before stopping.

These exercise the pure incremental logic directly (no network, no simulator).
"""

from databricks.labs.community_connector.sources.collibra.collibra import (
    CollibraLakeflowConnect,
)

# A cursor value comfortably below any record's lastModifiedOn used here, so the
# init-time upper cap never filters test records (we raise _init_ts after init).
_FAR_FUTURE = 10**15


def _connector() -> CollibraLakeflowConnect:
    conn = CollibraLakeflowConnect(
        {"org": "simulator", "access_token": "fake-token"}
    )
    # Neutralize the init-time upper cap so tests control emission purely via
    # the sort-order / cap logic under test.
    conn._init_ts = _FAR_FUTURE
    return conn


def _rec(rec_id: str, last_modified: int) -> dict:
    return {"id": rec_id, "name": rec_id, "lastModifiedOn": last_modified}


def _drain(result):
    records, end_offset = result
    return list(records), end_offset


# --------------------------------------------------------------------------- #
# Bug 1: assets are ID-ordered — count-based truncation must NOT drop records
# --------------------------------------------------------------------------- #

def test_assets_id_ordered_batch_does_not_silently_drop_records():
    """Records arrive in ID order (lastModifiedOn out of order). With a small
    ``max_records_per_batch`` the assets path must still emit ALL of them in one
    drain — never truncating on the count and advancing the watermark past
    un-emitted records."""
    conn = _connector()
    # lastModifiedOn deliberately NOT ascending (ID-sorted arrival). A hard cap
    # of 2 would have stopped after [500, 100] and set the watermark to 500,
    # permanently skipping the 300/200/400 records (all <= 500).
    arrival = [
        _rec("a1", 500),
        _rec("a2", 100),
        _rec("a3", 300),
        _rec("a4", 200),
        _rec("a5", 400),
    ]
    records, end_offset = _drain(
        conn._incremental_from_iter(
            iter(arrival),
            start_offset={},
            table_options={"max_records_per_batch": "2"},
            cursor_sorted=False,
        )
    )

    # All five emitted despite the cap of 2 (cap ignored for ID-ordered path).
    assert [r["id"] for r in records] == ["a1", "a2", "a3", "a4", "a5"]
    # Watermark is the TRUE global max over the whole scan, not a partial slice.
    assert end_offset == {"cursor": 500}

    # And a resume from that watermark emits nothing new (nothing was dropped).
    records2, end2 = _drain(
        conn._incremental_from_iter(
            iter(arrival),
            start_offset={"cursor": 500},
            table_options={"max_records_per_batch": "2"},
            cursor_sorted=False,
        )
    )
    assert records2 == []
    assert end2 == {"cursor": 500}


# --------------------------------------------------------------------------- #
# Bug 2: cursor-sorted tables must not split a lastModifiedOn tie group at the cap
# --------------------------------------------------------------------------- #

def test_cursor_sorted_soft_cap_does_not_split_tie_group():
    """LAST_MODIFIED-ASC records with a tie group straddling the cap: the soft
    cap must keep draining the boundary group so its members aren't skipped on
    the next strict ``> since`` resume."""
    conn = _connector()
    # Sorted ascending; three records share lastModifiedOn=200. Cap of 2 lands
    # inside that group. A hard cap would emit [100, 200] and set watermark 200,
    # then the next run's `> 200` filter would skip the other two 200s forever.
    arrival = [
        _rec("b1", 100),
        _rec("b2", 200),
        _rec("b3", 200),
        _rec("b4", 200),
        _rec("b5", 300),
    ]
    records, end_offset = _drain(
        conn._incremental_from_iter(
            iter(arrival),
            start_offset={},
            table_options={"max_records_per_batch": "2"},
            cursor_sorted=True,
        )
    )

    # Whole tie group (all three 200s) drained past the soft cap; 300 excluded
    # (strictly greater cursor ends the batch).
    assert [r["id"] for r in records] == ["b1", "b2", "b3", "b4"]
    assert end_offset == {"cursor": 200}

    # Resume: the strict `> 200` filter now correctly emits only the 300 record,
    # with no loss of the tie-group members.
    records2, end2 = _drain(
        conn._incremental_from_iter(
            iter(arrival),
            start_offset={"cursor": 200},
            table_options={"max_records_per_batch": "2"},
            cursor_sorted=True,
        )
    )
    assert [r["id"] for r in records2] == ["b5"]
    assert end2 == {"cursor": 300}


def test_cursor_sorted_hard_boundary_when_no_tie():
    """When the cap lands on a clean boundary (no tie), the soft cap stops
    promptly — it doesn't over-drain into the next distinct cursor value."""
    conn = _connector()
    arrival = [
        _rec("c1", 100),
        _rec("c2", 200),
        _rec("c3", 300),
        _rec("c4", 400),
    ]
    records, end_offset = _drain(
        conn._incremental_from_iter(
            iter(arrival),
            start_offset={},
            table_options={"max_records_per_batch": "2"},
            cursor_sorted=True,
        )
    )
    # Cap 2, all distinct cursors: emit exactly [100, 200]; 300 ends the group.
    assert [r["id"] for r in records] == ["c1", "c2"]
    assert end_offset == {"cursor": 200}
