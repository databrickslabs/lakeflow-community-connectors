"""Unit tests targeting the review findings on PR #249.

These exercise the specific edge cases the maintainer flagged, which the
simulator corpus (one small audience) does not cover. They mock the HTTP
layer, so no network or simulator corpus is involved.

- Finding 1: ``members`` auto-discovery must seed from the GLOBAL oldest
  cursor across every audience, not the first audience's oldest.
- Finding 2: unsorted endpoints (``reports``/``lists``) must not resume
  mid-window from the max cursor seen (would drop server-unordered rows);
  they drain the whole window regardless of ``max_records_per_batch``.
- Finding 3: the ``window_drained`` signal must key off a short page (window
  actually exhausted), not off the ``max_records`` cap.
- Finding 4: cursor comparisons/arithmetic must be timestamp-aware
  (``Z`` vs ``+00:00``), not raw string compares.
- Finding 5: a same-second cursor cluster larger than one page must not
  stall — when the cap trips and the resume cursor can't advance past
  ``since``, the window is re-drained uncapped so no row is lost.
- Finding 6: the oldest-cursor probe on unsorted endpoints must paginate
  fully, so a global-oldest row on a later page seeds ``since`` correctly.
"""

from unittest.mock import Mock, patch

from databricks.labs.community_connector.sources.mailchimp.mailchimp import (
    MailchimpLakeflowConnect,
)
from databricks.labs.community_connector.sources.mailchimp.mailchimp_schemas import (
    TABLE_CONFIG,
)


def _connector() -> MailchimpLakeflowConnect:
    return MailchimpLakeflowConnect({"api_key": "0123456789abcdef0123456789abcde-us1"})


def _resp(payload: dict, status: int = 200) -> Mock:
    resp = Mock()
    resp.status_code = status
    resp.json.return_value = payload
    resp.text = ""
    return resp


def _paged_request(rows_by_path: dict):
    """Build a fake ``_request_with_retry`` that pages ``rows_by_path``.

    Honours offset/count and always reports ``total_items``. Each path maps to
    (resource_key, rows).
    """

    def fake(path, params=None):
        params = params or {}
        if path not in rows_by_path:
            raise AssertionError(f"unexpected path {path}")
        key, rows = rows_by_path[path]
        offset = int(params.get("offset", 0))
        count = int(params.get("count", 1000))
        page = rows[offset : offset + count]
        return _resp({key: page, "total_items": len(rows)})

    return fake


# ---------------------------------------------------------------------------
# Finding 1 — members auto-discovery takes the global oldest cursor
# ---------------------------------------------------------------------------


def test_members_peek_takes_global_oldest_across_audiences():
    """Audience B holds an older member than audience A; the probe must pick B."""
    conn = _connector()
    rows_by_path = {
        "/lists": ("lists", [{"id": "A"}, {"id": "B"}]),
        # ascending-sort probe (count=1) returns each audience's oldest member
        "/lists/A/members": ("members", [{"last_changed": "2026-05-01T00:00:00+00:00"}]),
        "/lists/B/members": ("members", [{"last_changed": "2026-01-01T00:00:00+00:00"}]),
    }
    with patch.object(conn, "_request_with_retry", side_effect=_paged_request(rows_by_path)):
        oldest = conn._peek_oldest_cursor("members", TABLE_CONFIG["members"])

    # Global minimum (audience B), not the first audience probed (A).
    assert oldest == "2026-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Finding 2 — unsorted endpoints never resume mid-window from max cursor
# ---------------------------------------------------------------------------


def test_unsorted_endpoint_drains_window_ignoring_max_records():
    """reports (unsorted) must drain the full window, never truncate at the cap.

    3 rows in server-default order (oldest listed LAST). With ``max_records=2``,
    the pre-fix code would truncate to 2 and resume from the max cursor seen,
    dropping the oldest row forever. The fix drains all 3 and advances to
    window_end.
    """
    conn = _connector()
    conn._init_ts = "2030-01-01T00:00:00+00:00"  # far future: window not capped early
    rows = [
        {"id": "r1", "send_time": "2026-01-02T00:00:00+00:00"},
        {"id": "r2", "send_time": "2026-01-03T00:00:00+00:00"},
        {"id": "r3", "send_time": "2026-01-01T00:00:00+00:00"},  # oldest, listed last
    ]
    with patch.object(
        conn, "_request_with_retry", side_effect=_paged_request({"/reports": ("reports", rows)})
    ):
        it, end_offset = conn._read_window(
            "reports",
            {"cursor": "2026-01-01T00:00:00+00:00"},
            {"max_records_per_batch": "2", "window_seconds": "864000", "lookback_seconds": "0"},
        )
        out = list(it)

    assert len(out) == 3  # nothing dropped
    expected_end = conn._window_end("2026-01-01T00:00:00+00:00", 864000)
    assert end_offset == {"cursor": expected_end}  # advanced cleanly to window end


# ---------------------------------------------------------------------------
# Finding 3 — window_drained keys off a short page, not the max_records cap
# ---------------------------------------------------------------------------


def test_sorted_window_between_maxrecords_and_count_is_treated_as_drained():
    """campaigns (sorted) with rows between max_records and count -> drained.

    5 rows, ``max_records=3``, page size 1000. The single page (5 < 1000) means
    the window is exhausted. Pre-fix, ``len(records) < max_records`` was
    ``5 < 3`` = False, so it wrongly resumed from max_cursor and never reached
    window_end. The fix advances to window_end.
    """
    conn = _connector()
    conn._init_ts = "2030-01-01T00:00:00+00:00"
    rows = [
        {"id": f"c{i}", "create_time": f"2026-01-0{i}T00:00:00+00:00"} for i in range(1, 6)
    ]
    with patch.object(
        conn,
        "_request_with_retry",
        side_effect=_paged_request({"/campaigns": ("campaigns", rows)}),
    ):
        it, end_offset = conn._read_window(
            "campaigns",
            {"cursor": "2026-01-01T00:00:00+00:00"},
            {"max_records_per_batch": "3", "window_seconds": "864000", "lookback_seconds": "0"},
        )
        out = list(it)

    assert len(out) == 5
    expected_end = conn._window_end("2026-01-01T00:00:00+00:00", 864000)
    assert end_offset == {"cursor": expected_end}


def test_paginate_exhausted_flag():
    """Directly verify the drained signal: short page -> True, cap hit -> False."""
    conn = _connector()
    rows = [{"id": f"c{i}"} for i in range(3)]

    # Short page ends the loop (count=2 pages: [0,1] then [2]) -> exhausted True.
    with patch.object(
        conn, "_request_with_retry", side_effect=_paged_request({"/x": ("items", rows)})
    ):
        recs, exhausted = conn._paginate("/x", "items", {"count": 2}, max_records=10)
    assert len(recs) == 3 and exhausted is True

    # Full page hits the cap first (count=2, max_records=2) -> exhausted False.
    with patch.object(
        conn, "_request_with_retry", side_effect=_paged_request({"/x": ("items", rows)})
    ):
        recs, exhausted = conn._paginate("/x", "items", {"count": 2}, max_records=2)
    assert len(recs) == 2 and exhausted is False


# ---------------------------------------------------------------------------
# Finding 4 — cursor comparison / arithmetic is timestamp-aware (Z vs +00:00)
# ---------------------------------------------------------------------------


def test_init_ts_short_circuit_handles_z_suffix():
    """A ``Z``-suffixed cursor equal to _init_ts must short-circuit (no read).

    String compare would treat ``...Z`` > ``...+00:00`` and wrongly proceed to
    the API; timestamp-aware compare treats them equal -> no read.
    """
    conn = _connector()
    conn._init_ts = "2026-01-01T00:00:00+00:00"

    def boom(path, params=None):
        raise AssertionError("should not hit the API when already caught up")

    with patch.object(conn, "_request_with_retry", side_effect=boom):
        it, end_offset = conn._read_window(
            "campaigns", {"cursor": "2026-01-01T00:00:00Z"}, {}
        )
        out = list(it)

    assert out == []
    assert end_offset == {"cursor": "2026-01-01T00:00:00Z"}


def test_window_end_normalizes_mixed_offsets():
    """``_window_end`` parses both notations and caps at _init_ts correctly."""
    conn = _connector()
    conn._init_ts = "2026-01-10T00:00:00+00:00"

    # since with Z suffix; a 1-day window lands before the cap.
    assert conn._window_end("2026-01-01T00:00:00Z", 86400) == "2026-01-02T00:00:00+00:00"
    # A window that would exceed _init_ts is capped at it.
    assert conn._window_end("2026-01-09T12:00:00Z", 86400) == "2026-01-10T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Finding 5 — same-second cursor cluster larger than a page re-drains uncapped
# ---------------------------------------------------------------------------


def test_same_second_cluster_larger_than_cap_drains_all_rows():
    """1500 sorted rows sharing one cursor-second, cap=1000 -> all 1500 ingest.

    The capped page would resume from ``_max_cursor`` == ``since`` (every row
    shares the same ``create_time``), so an offset/time-cursor resume can't
    advance: the next call re-fetches the same first page and the 500 rows past
    it are lost forever. The drain-on-stall fix re-drains the window uncapped so
    all 1500 come through in one read and the cursor advances to window_end.
    """
    conn = _connector()
    conn._init_ts = "2030-01-01T00:00:00+00:00"
    rows = [
        {"id": f"c{i}", "create_time": "2026-03-01T00:00:00+00:00"} for i in range(1500)
    ]
    with patch.object(
        conn,
        "_request_with_retry",
        side_effect=_paged_request({"/campaigns": ("campaigns", rows)}),
    ):
        it, end_offset = conn._read_window(
            "campaigns",
            {"cursor": "2026-03-01T00:00:00+00:00"},
            {
                "max_records_per_batch": "1000",
                "window_seconds": "864000",
                "lookback_seconds": "0",
            },
        )
        out = list(it)

    assert len(out) == 1500  # nothing dropped despite the cap
    expected_end = conn._window_end("2026-03-01T00:00:00+00:00", 864000)
    assert end_offset == {"cursor": expected_end}  # advanced cleanly, no stall


# ---------------------------------------------------------------------------
# Finding 6 — oldest-cursor probe on unsorted endpoints paginates fully
# ---------------------------------------------------------------------------


def test_unsorted_probe_paginates_to_global_oldest():
    """1200 reports, global-oldest at index 1100 -> probe must find it.

    Unsorted endpoints have no ascending sort, so the probe fetches in
    server-default order. A single-page probe (count=1000) would only see the
    first page and miss the oldest row on page 2; draining all pages returns the
    true global minimum.
    """
    conn = _connector()
    rows = [
        {"id": f"r{i}", "send_time": f"2026-06-{(i % 27) + 1:02d}T00:00:00+00:00"}
        for i in range(1200)
    ]
    rows[1100]["send_time"] = "2020-01-01T00:00:00+00:00"  # global oldest, page 2
    with patch.object(
        conn,
        "_request_with_retry",
        side_effect=_paged_request({"/reports": ("reports", rows)}),
    ):
        oldest = conn._peek_oldest_cursor("reports", TABLE_CONFIG["reports"])

    assert oldest == "2020-01-01T00:00:00+00:00"
