"""Cursor semantics shared by the v2 incremental and delete streams.

Covers the reviewer's findings: ties and same-second writes must neither be dropped
nor skipped, a batch must never return rows with an unchanged offset, and the settle
margin holds back recent changes until a later trigger.  The fakes stand in for the
HTTP client because there is no Pipedrive account (see fakes.py).
"""

import logging

import pytest

from databricks.labs.community_connector.sources.pipedrive.pipedrive import (
    PipedriveLakeflowConnect,
)
from databricks.labs.community_connector.sources.pipedrive.pipedrive_utils import (
    PipedriveAPIError,
    PipedriveClient,
)
from tests.unit.sources.pipedrive.fakes import V2Fake, drain

START = "2024-01-01T00:00:00Z"
DEALS = "/api/v2/deals"
ARCHIVED = "/api/v2/deals/archived"


def _ts(seconds_from_base):
    hours, rest = divmod(seconds_from_base, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"2024-06-01T{hours:02d}:{minutes:02d}:{seconds:02d}Z"


def _deal(deal_id, update_time, status="open", archived=False):
    return {
        "id": deal_id,
        "update_time": update_time,
        "add_time": update_time,
        "status": status,
        "is_deleted": status == "deleted",
        "is_archived": archived,
    }


def _connector(client, init_time="2024-07-01T00:00:00Z"):
    connector = PipedriveLakeflowConnect({"api_token": "fake", "start_date": START})
    connector._client = client  # pylint: disable=protected-access
    connector._init_time = init_time  # pylint: disable=protected-access
    return connector


SETTLE_OFF = {"lookback_seconds": "0"}


def test_ties_split_across_batches_are_all_emitted_once():
    """Reviewer scenario A: 25 rows share one update_time, batch 7, page 3."""
    rows = [_deal(i + 1, _ts(100) if i < 25 else _ts(200 + i)) for i in range(60)]
    client = V2Fake({DEALS: rows, ARCHIVED: []})
    options = {**SETTLE_OFF, "max_records_per_batch": "7", "page_size": "3"}

    emitted, _ = drain(lambda off: _connector(client).read_table("deals", off, options), None)

    ids = [r["id"] for r in emitted]
    assert sorted(ids) == list(range(1, 61))
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("batch", ["1", "2", "5"])
def test_one_tie_group_larger_than_a_batch_terminates(batch):
    rows = [_deal(i, _ts(500)) for i in range(1, 12)]
    client = V2Fake({DEALS: rows, ARCHIVED: []})
    emitted, _ = drain(
        lambda off: _connector(client).read_table(
            "deals", off, {**SETTLE_OFF, "max_records_per_batch": batch}
        ),
        None,
    )
    assert sorted(r["id"] for r in emitted) == list(range(1, 12))


def test_same_second_row_after_drain_is_emitted_on_next_trigger():
    """Reviewer scenario (adv_stall): a deal written in the cursor's second."""
    rows = [_deal(1, _ts(9 * 3600)), _deal(2, _ts(10 * 3600))]
    client = V2Fake({DEALS: rows, ARCHIVED: []})
    emitted, offset = drain(
        lambda off: _connector(client, "2024-06-01T11:00:00Z").read_table("deals", off, SETTLE_OFF),
        None,
    )
    assert [r["id"] for r in emitted] == [1, 2]

    rows.append(_deal(3, _ts(10 * 3600)))  # same second as the cursor
    emitted, offset2 = drain(
        lambda off: _connector(client, "2024-06-01T12:00:00Z").read_table("deals", off, SETTLE_OFF),
        offset,
    )
    assert [r["id"] for r in emitted] == [3]
    assert offset2 == {"cursor": _ts(10 * 3600), "ids": [2, 3]}

    rows.append(_deal(4, _ts(13 * 3600)))  # a later, unrelated write
    emitted, _ = drain(
        lambda off: _connector(client, "2024-06-01T14:00:00Z").read_table("deals", off, SETTLE_OFF),
        offset2,
    )
    assert [r["id"] for r in emitted] == [4]


def test_settle_margin_holds_back_rows_newer_than_init_minus_margin():
    """Rows newer than init minus the settle margin wait for the next trigger."""
    old = _deal(1, "2024-06-01T11:58:00Z")
    recent = _deal(2, "2024-06-01T11:59:30Z")  # within 60 s of init 12:00:00
    client = V2Fake({DEALS: [old, recent], ARCHIVED: []})

    first, offset = drain(
        lambda off: _connector(client, "2024-06-01T12:00:00Z").read_table("deals", off, {}), None
    )
    assert [r["id"] for r in first] == [1]

    second, _ = drain(
        lambda off: _connector(client, "2024-06-01T13:00:00Z").read_table("deals", off, {}),
        offset,
    )
    assert [r["id"] for r in second] == [2]


def test_live_deals_path_drops_rows_the_server_reports_as_deleted():
    """A server that ignores ``status=open,won,lost`` must not leak deleted rows."""
    rows = [_deal(1, _ts(100)), _deal(2, _ts(200), status="deleted")]
    client = V2Fake({DEALS: rows, ARCHIVED: []}, ignore_status=True)
    emitted, _ = drain(
        lambda off: _connector(client).read_table("deals", off, SETTLE_OFF), None
    )
    assert [r["id"] for r in emitted] == [1]


def test_non_advancing_next_cursor_is_an_error_not_an_endless_loop(monkeypatch):
    client = PipedriveClient("token", "https://example.invalid")
    pages = iter([
        {"data": [{"id": 1}], "additional_data": {"next_cursor": "c1"}},
        {"data": [{"id": 2}], "additional_data": {"next_cursor": "c1"}},
    ])
    monkeypatch.setattr(client, "get_json", lambda path, params: next(pages))
    with pytest.raises(PipedriveAPIError, match="did not advance"):
        list(client.iter_v2_pages("/api/v2/deals", {"limit": 1}))


class _SkippedChildren:
    """iter_v2_pages yields one live deal; every per-deal flow call is a 404 (deal gone)."""

    def iter_v2_pages(self, path, params):  # pylint: disable=unused-argument
        yield [{"id": 7, "update_time": _ts(100)}]

    def iter_v1_pages(self, path, params):  # pylint: disable=unused-argument
        raise PipedriveAPIError("Deal not found", status_code=404, is_json=True)


def test_deal_flow_batch_with_every_child_call_skipped_logs_a_warning(caplog):
    connector = _connector(_SkippedChildren(), "2024-06-02T00:00:00Z")
    with caplog.at_level(logging.WARNING):
        rows, _ = connector.read_table(
            "deal_flow", {"cursor": "2024-06-01T00:00:00Z"}, {"lookback_seconds": "0"}
        )
        assert list(rows) == []
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("all 1 per-deal flow calls" in m for m in messages)
