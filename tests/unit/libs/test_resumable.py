"""Unit tests for the reusable resumable-CDC helper (``libs.resumable``).

Exercise the state machine directly, independent of any connector: fresh-pass
drain, page-granular cap + resume, watermark-advances-only-on-completion, a
frozen snapshot bound across resumes, convergence when caught up, and the
incremental window. Cursors are compared with plain ordering, so the helper is
type-agnostic — covered here with both int and ISO-string cursors.
"""

from databricks.labs.community_connector.libs.resumable import (
    RESUME_TOKEN,
    SNAPSHOT_TS,
    WATERMARK,
    resumable_cdc_read,
)


def _paginator(pages):
    """Build a ``paginate(resume_token)`` over ``pages``.

    ``pages`` is a list of ``(batch, next_token)``; ``next_token`` is the token
    to resume *after* that page (``None`` on the last page). ``paginate`` starts
    at the page whose incoming token matches ``resume_token`` (``""`` = first),
    and records the tokens it was called with on ``paginate.calls``.
    """
    incoming = [""]
    for _, next_token in pages[:-1]:
        incoming.append(next_token)
    calls = []

    def paginate(resume_token):
        calls.append(resume_token)
        start = incoming.index(resume_token) if resume_token in incoming else 0
        for i in range(start, len(pages)):
            yield pages[i]

    paginate.calls = calls
    return paginate


def _rec(rec_id, ts):
    return {"id": rec_id, "ts": ts}


def _run(paginate, start_offset, snapshot_ts, max_records):
    records, end_offset = resumable_cdc_read(
        start_offset=start_offset,
        snapshot_ts=snapshot_ts,
        paginate=paginate,
        cursor_of=lambda r: r.get("ts"),
        shape=lambda r: r,
        max_records=max_records,
    )
    return [r["id"] for r in records], end_offset


# --------------------------------------------------------------------------- #
# Fresh pass
# --------------------------------------------------------------------------- #


def test_fresh_pass_no_cap_drains_all_and_advances_watermark():
    pages = [
        ([_rec("a", 10), _rec("b", 20)], "t1"),
        ([_rec("c", 30)], None),
    ]
    ids, off = _run(_paginator(pages), None, snapshot_ts=100, max_records=None)
    assert ids == ["a", "b", "c"]
    # Whole space scanned under one snapshot -> watermark jumps to the bound.
    assert off == {WATERMARK: 100}


def test_empty_source_completes_and_advances_to_snapshot():
    ids, off = _run(_paginator([([], None)]), None, snapshot_ts=100, max_records=None)
    assert ids == []
    assert off == {WATERMARK: 100}


# --------------------------------------------------------------------------- #
# Page-granular cap + resume
# --------------------------------------------------------------------------- #


def test_cap_stops_mid_pass_and_persists_resume_token():
    pages = [
        ([_rec("a", 10), _rec("b", 10)], "t1"),
        ([_rec("c", 10), _rec("d", 10)], "t2"),
        ([_rec("e", 10), _rec("f", 10)], None),
    ]
    paginate = _paginator(pages)

    ids1, off1 = _run(paginate, None, snapshot_ts=100, max_records=2)
    assert ids1 == ["a", "b"]
    assert off1[RESUME_TOKEN] == "t1"
    assert off1[SNAPSHOT_TS] == 100
    # Watermark not advanced mid-pass (nothing proven complete yet).
    assert off1.get(WATERMARK) is None

    ids2, off2 = _run(paginate, off1, snapshot_ts=999, max_records=2)
    assert ids2 == ["c", "d"]
    assert off2[RESUME_TOKEN] == "t2"

    ids3, off3 = _run(paginate, off2, snapshot_ts=999, max_records=2)
    assert ids3 == ["e", "f"]
    # Last page's cap hit but next_token is None -> pass completes, token cleared.
    assert RESUME_TOKEN not in off3
    assert off3 == {WATERMARK: 100}

    allids = ids1 + ids2 + ids3
    assert allids == ["a", "b", "c", "d", "e", "f"]
    assert len(allids) == len(set(allids))


def test_snapshot_ts_frozen_across_resume():
    """Once a pass starts, its snapshot bound is kept across resumes even if the
    connector's live snapshot_ts moves on."""
    pages = [
        ([_rec("a", 10), _rec("b", 10)], "t1"),
        ([_rec("c", 10)], None),
    ]
    paginate = _paginator(pages)
    _, off1 = _run(paginate, None, snapshot_ts=100, max_records=2)
    assert off1[SNAPSHOT_TS] == 100
    # Resume with a *different* live snapshot_ts; the frozen 100 must win.
    _, off2 = _run(paginate, off1, snapshot_ts=555, max_records=2)
    assert off2 == {WATERMARK: 100}


def test_cap_on_last_page_completes_without_false_resume():
    pages = [([_rec("a", 10), _rec("b", 10), _rec("c", 10)], None)]
    ids, off = _run(_paginator(pages), None, snapshot_ts=100, max_records=2)
    assert ids == ["a", "b", "c"]  # last page drained despite cap
    assert off == {WATERMARK: 100}


# --------------------------------------------------------------------------- #
# Incremental window + convergence
# --------------------------------------------------------------------------- #


def test_incremental_window_excludes_committed_and_future():
    pages = [
        (
            [
                _rec("old", 50),  # <= watermark -> excluded
                _rec("keep1", 150),
                _rec("future", 250),  # > snapshot_ts -> excluded
                _rec("keep2", 200),
            ],
            None,
        )
    ]
    ids, off = _run(
        _paginator(pages), {WATERMARK: 100}, snapshot_ts=200, max_records=None
    )
    assert ids == ["keep1", "keep2"]
    assert off == {WATERMARK: 200}


def test_caught_up_converges_without_paging():
    paginate = _paginator([([_rec("a", 10)], None)])
    ids, off = _run(paginate, {WATERMARK: 100}, snapshot_ts=100, max_records=None)
    assert ids == []
    assert off == {WATERMARK: 100}
    # Already caught up: the source must not be paged at all.
    assert paginate.calls == []


def test_record_without_cursor_is_emitted_and_does_not_move_watermark():
    pages = [([_rec("a", 20), {"id": "nocursor"}], None)]
    ids, off = _run(_paginator(pages), None, snapshot_ts=100, max_records=None)
    assert set(ids) == {"a", "nocursor"}
    # Pass completed -> watermark advances to the snapshot bound regardless.
    assert off == {WATERMARK: 100}


# --------------------------------------------------------------------------- #
# Type-agnostic: ISO-8601 string cursors (Purview-style)
# --------------------------------------------------------------------------- #


def test_iso_string_cursors_are_supported():
    pages = [
        (
            [
                _rec("a", "2026-01-01T00:00:00+00:00"),  # <= watermark -> excluded
                _rec("b", "2026-05-01T00:00:00+00:00"),
            ],
            None,
        )
    ]
    ids, off = _run(
        _paginator(pages),
        {WATERMARK: "2026-01-01T00:00:00+00:00"},
        snapshot_ts="2026-12-31T23:59:59+00:00",
        max_records=None,
    )
    assert ids == ["b"]
    assert off == {WATERMARK: "2026-12-31T23:59:59+00:00"}
