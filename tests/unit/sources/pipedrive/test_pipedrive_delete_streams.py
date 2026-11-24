"""Delete streams for deals / deals_archived: archive and unarchive moves, the
client-side status guard, and per-trigger offsets.

Why a fake client: there is no Pipedrive account, so these tests drive the
connector's own read logic through an in-memory stand-in for the HTTP client.
The simulator suite covers the wire format; this file covers the sequencing.
"""

from databricks.labs.community_connector.sources.pipedrive.pipedrive import (
    PipedriveLakeflowConnect,
)

START = "2024-01-01T00:00:00Z"
INIT = "2024-06-01T00:00:00Z"
LIVE_PATH = "/api/v2/deals"
ARCHIVED_PATH = "/api/v2/deals/archived"


def _deal(deal_id, update_time, *, archived=False, deleted=False):
    return {
        "id": deal_id,
        "update_time": update_time,
        "status": "deleted" if deleted else "open",
        "is_deleted": deleted,
        "is_archived": archived,
    }


def _status_matches(row, status):
    wanted = set(status.split(","))
    if row["is_deleted"]:
        return "deleted" in wanted
    return bool(wanted & {"open", "won", "lost"})


class FakeDealLists:
    """Stand-in for ``PipedriveClient.iter_v2_pages`` over the two deal lists.

    The mock is needed because there is no live account.  ``honor_status=False``
    models a server that ignores the ``status`` query parameter and returns
    every row in the list.
    """

    def __init__(self, lists, honor_status=True, page_size=2):
        self.lists = lists
        self.honor_status = honor_status
        self.page_size = page_size

    def iter_v2_pages(self, path, params):
        rows = list(self.lists.get(path, []))
        if params.get("updated_since"):
            rows = [r for r in rows if r["update_time"] >= params["updated_since"]]
        if params.get("updated_until"):
            rows = [r for r in rows if r["update_time"] <= params["updated_until"]]
        if params.get("status") and self.honor_status:
            rows = [r for r in rows if _status_matches(r, params["status"])]
        rows.sort(key=lambda r: (r["update_time"], r["id"]))
        size = min(self.page_size, int(params.get("limit", self.page_size)))
        for i in range(0, len(rows), size):
            yield rows[i : i + size]


def _connector(lists, *, honor_status=True, init_time=INIT):
    connector = PipedriveLakeflowConnect({"api_token": "fake", "start_date": START})
    connector._client = FakeDealLists(lists, honor_status)  # pylint: disable=protected-access
    connector._init_time = init_time  # pylint: disable=protected-access
    return connector


def _drain(connector, table, offset=None, options=None, max_batches=100):
    """Run the delete stream to convergence; returns (rows, final offset)."""
    offset = offset or {}
    rows = []
    for _ in range(max_batches):
        records, end = connector.read_table_deletes(table, offset, options or {})
        batch = list(records)
        rows.extend(batch)
        if end == offset:
            return rows, end
        offset = end
    raise AssertionError(f"{table} delete stream did not converge")


def _ids(rows):
    return {r["id"] for r in rows}


def _lists():
    return {
        LIVE_PATH: [
            _deal(1, "2024-02-01T00:00:00Z"),
            _deal(3, "2024-03-01T00:00:00Z", deleted=True),
            _deal(4, "2024-03-10T00:00:00Z"),  # unarchived back into live deals
        ],
        ARCHIVED_PATH: [
            _deal(2, "2024-03-05T00:00:00Z", archived=True),
            _deal(5, "2024-02-15T00:00:00Z", archived=True, deleted=True),
        ],
    }


def test_deals_tombstones_cover_soft_deletes_and_archived_moves():
    connector = _connector(_lists())
    rows, _ = _drain(connector, "deals")

    # 3 is soft-deleted in the live list; 2 and 5 were archived.  1 and 4 are
    # live and must not be tombstoned.
    assert _ids(rows) == {2, 3, 5}
    archived_move = next(r for r in rows if r["id"] == 2)
    assert archived_move == {"id": 2, "update_time": "2024-03-05T00:00:00Z"}


def test_deals_archived_tombstones_cover_deleted_and_unarchived_moves():
    connector = _connector(_lists())
    rows, _ = _drain(connector, "deals_archived")

    # 5 is soft-deleted in the archived list.  4 was unarchived and so re-enters
    # the live list, which tombstones it here.  2 is still archived and must not
    # be tombstoned.
    ids = _ids(rows)
    assert 5 in ids
    assert 4 in ids
    assert 2 not in ids


def test_deletes_drop_live_rows_when_server_ignores_status_filter():
    """A server that ignores ``status=deleted`` must not turn live rows into deletes."""
    connector = _connector(_lists(), honor_status=False)

    deals_rows, _ = _drain(connector, "deals")
    archived_rows, _ = _drain(connector, "deals_archived")

    # Without the guard, the unfiltered /deals list would tombstone 1 and 4.
    assert 1 not in _ids(deals_rows)
    assert 4 not in _ids(deals_rows)
    assert _ids(deals_rows) == {2, 3, 5}
    # Without the guard, the unfiltered /deals/archived list would tombstone
    # the still-archived 2 from the deleted-source phase.
    assert 2 not in _ids(archived_rows)
    assert 5 in _ids(archived_rows)


def test_delete_offsets_carry_across_triggers():
    """The second trigger picks up moves after the first trigger's init time."""
    lists = _lists()
    first = _connector(lists)
    _, offset = _drain(first, "deals")
    assert offset["cursor"] == "2024-03-05T00:00:00Z"
    assert set(offset) <= {"cursor", "ids"}

    # Deal 1 is archived after the first trigger's init time.
    lists[LIVE_PATH] = [d for d in lists[LIVE_PATH] if d["id"] != 1]
    lists[ARCHIVED_PATH].append(_deal(1, "2024-06-05T00:00:00Z", archived=True))
    second = _connector(lists, init_time="2024-07-01T00:00:00Z")
    rows, _ = _drain(second, "deals", offset=offset)

    assert 1 in _ids(rows)


def test_delete_stream_batches_terminate_with_complete_union():
    """One row per batch: the walk terminates and no tombstone is lost."""
    connector = _connector(_lists())
    options = {"max_records_per_batch": "1", "page_size": "1"}
    offset, rows = {}, []
    for _ in range(100):
        records, end = connector.read_table_deletes("deals", offset, options)
        batch = list(records)
        assert len(batch) <= 1
        rows.extend(batch)
        if end == offset:
            break
        offset = end
    else:
        raise AssertionError("deals delete stream did not converge")
    assert _ids(rows) == {2, 3, 5}
