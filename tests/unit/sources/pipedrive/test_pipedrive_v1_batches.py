"""v1 keyset walks: notes, leads, and the leads/archived delete stream.

The fakes honour ``sort``, ``updated_since`` and pagination (see fakes.py).  A fake
is needed because there is no Pipedrive account.
"""

from datetime import datetime, timedelta

import pytest

from databricks.labs.community_connector.sources.pipedrive.pipedrive import (
    PipedriveLakeflowConnect,
)
from tests.unit.sources.pipedrive.fakes import V1Fake, drain

START = "2024-01-01T00:00:00Z"
INIT = "2024-06-01T00:00:00Z"


def _connector(client, init_time=INIT):
    connector = PipedriveLakeflowConnect({"api_token": "fake", "start_date": START})
    connector._client = client  # pylint: disable=protected-access
    connector._init_time = init_time  # pylint: disable=protected-access
    return connector


def _note(note_id, update_time):
    return {"id": note_id, "update_time": update_time, "add_time": update_time, "content": "x"}


def _hour(i, base=datetime(2024, 2, 1)):
    return (base + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")


def _lead(i, when):
    return {"id": f"uuid-{i:05d}", "add_time": when, "update_time": when}


# Settle 0 so the upper bound is the init time itself, as in the unit scenarios.
SETTLE = {"lookback_seconds": "0"}


def test_notes_with_ties_split_across_pages_are_all_emitted_once():
    """Ties at the end of the range span pages; every row arrives exactly once."""
    notes = [_note(i, _hour(i)) for i in range(1, 11)]
    tied = "2024-02-03 00:00:00"
    notes += [_note(i, tied) for i in range(11, 20)]  # nine rows tied at the end
    client = V1Fake({"/v1/notes": notes})
    options = {**SETTLE, "max_records_per_batch": "3", "page_size": "2"}

    rows, _ = drain(lambda off: _connector(client).read_table("notes", off, options), None)

    ids = [r["id"] for r in rows]
    assert sorted(ids) == list(range(1, 20))
    assert len(ids) == len(set(ids))


def test_notes_batches_never_exceed_the_cap():
    notes = [_note(i, _hour(i)) for i in range(1, 30)]
    client = V1Fake({"/v1/notes": notes})
    options = {**SETTLE, "max_records_per_batch": "4", "page_size": "3"}
    offset, sizes = {}, []
    connector = _connector(client)
    for _ in range(200):
        records, end = connector.read_table("notes", offset, options)
        batch = list(records)
        sizes.append(len(batch))
        if end == offset:
            break
        offset = end
    assert max(sizes) <= 4
    assert sum(sizes) == 29


def test_same_second_row_after_drain_is_emitted_on_next_trigger():
    """A note written in the cursor's second, after the first trigger drained."""
    notes = [_note(1, "2024-02-01 09:00:00"), _note(2, "2024-02-01 10:00:00")]
    client = V1Fake({"/v1/notes": notes})
    rows, offset = drain(lambda off: _connector(client).read_table("notes", off, SETTLE), None)
    assert [r["id"] for r in rows] == [1, 2]
    assert offset == {"cursor": "2024-02-01T10:00:00Z", "ids": [2]}

    notes.append(_note(3, "2024-02-01 10:00:00"))  # same second as the cursor
    rows, offset2 = drain(
        lambda off: _connector(client).read_table("notes", off, SETTLE), offset
    )
    assert [r["id"] for r in rows] == [3]
    assert offset2 == {"cursor": "2024-02-01T10:00:00Z", "ids": [2, 3]}


def test_row_removed_mid_walk_does_not_skip_any_other_lead():
    """Reviewer scenario: leads archived while a multi-batch walk is in progress."""
    leads = [_lead(i, _hour(i)) for i in range(1200)]
    state = {"fired": False, "calls": 0}

    def archive_mid_walk(fake, path, params):  # pylint: disable=unused-argument
        # Archive two leads (one already read, one not yet) after the first batch.
        if path == "/v1/leads":
            state["calls"] += 1
        if path == "/v1/leads" and state["calls"] == 2 and not state["fired"]:
            state["fired"] = True
            fake.lists[path] = [r for r in fake.lists[path] if r["id"] not in
                                {leads[10]["id"], leads[1100]["id"]}]

    client = V1Fake({"/v1/leads": leads, "/v1/leads/archived": []}, on_request=archive_mid_walk)
    options = {**SETTLE, "max_records_per_batch": "300", "page_size": "300"}
    rows, _ = drain(lambda off: _connector(client).read_table("leads", off, options), None)

    emitted = [r["id"] for r in rows]
    live_at_end = {r["id"] for r in client.lists["/v1/leads"]}
    assert state["fired"]
    assert live_at_end <= set(emitted)
    assert len(emitted) == len(set(emitted))


def test_notes_removed_mid_walk_do_not_skip_others():
    notes = [_note(i, _hour(i)) for i in range(1, 60)]
    state = {"fired": False, "calls": 0}

    def delete_mid_walk(fake, path, params):  # pylint: disable=unused-argument
        if path == "/v1/notes":
            state["calls"] += 1
        if path == "/v1/notes" and state["calls"] == 2 and not state["fired"]:
            state["fired"] = True
            fake.lists[path] = [r for r in fake.lists[path] if r["id"] != 5]

    client = V1Fake({"/v1/notes": notes}, on_request=delete_mid_walk)
    options = {**SETTLE, "max_records_per_batch": "7", "page_size": "7"}
    rows, _ = drain(lambda off: _connector(client).read_table("notes", off, options), None)
    emitted = {r["id"] for r in rows}
    assert {r["id"] for r in client.lists["/v1/notes"]} <= emitted


def test_second_trigger_picks_up_rows_after_the_first_init_time():
    """Reviewer scenario: rows past the first trigger's init time arrive in trigger two."""
    leads = [_lead(i, _hour(i)) for i in range(1200)]
    lists = {"/v1/leads": leads, "/v1/leads/archived": []}
    state = {"fired": False, "calls": 0}

    def archive_mid_walk(fake, path, params):  # pylint: disable=unused-argument
        if path == "/v1/leads":
            state["calls"] += 1
        if path == "/v1/leads" and state["calls"] == 2 and not state["fired"]:
            state["fired"] = True
            fake.lists[path] = [r for r in fake.lists[path] if r["id"] != leads[10]["id"]]

    options = {**SETTLE, "max_records_per_batch": "300", "page_size": "300"}
    client = V1Fake(lists, on_request=archive_mid_walk)
    first_rows, cursor = drain(
        lambda off: _connector(client, "2024-03-01T00:00:00Z").read_table("leads", off, options),
        None,
    )
    second_rows, _ = drain(
        lambda off: _connector(client, "2024-04-01T00:00:00Z").read_table("leads", off, options),
        cursor,
    )
    live = {r["id"] for r in client.lists["/v1/leads"]}
    emitted = {r["id"] for r in first_rows} | {r["id"] for r in second_rows}
    assert live <= emitted


def test_leads_archived_ignoring_updated_since_still_terminates_and_tombstones():
    """Reviewer scenario: /v1/leads/archived ignores updated_since."""
    archived = [
        {"id": "a-old", "update_time": "2024-01-10 00:00:00"},
        {"id": "b-mid", "update_time": "2024-02-02 00:00:00"},
        {"id": "c-new", "update_time": "2024-02-03 00:00:00"},
        {"id": "d-after-init", "update_time": "2024-06-02 00:00:00"},
    ]
    client = V1Fake(
        {"/v1/leads": [], "/v1/leads/archived": archived},
        ignore_since=["/v1/leads/archived"],
    )
    connector = _connector(client)
    options = {**SETTLE, "max_records_per_batch": "1"}

    rows, offset = drain(
        lambda off: connector.read_table_deletes("leads", off, options),
        {"cursor": "2024-02-01T00:00:00Z"},
    )
    assert sorted(r["id"] for r in rows) == ["b-mid", "c-new"]
    assert all(set(r) == {"id", "update_time"} for r in rows)
    assert offset == {"cursor": "2024-02-03T00:00:00Z", "ids": ["c-new"]}
    archived_requests = [p for path, p in client.requests if path == "/v1/leads/archived"]
    assert archived_requests and all(p["sort"] == "update_time ASC, id ASC" for p in archived_requests)

    # A stream that is fully caught up still terminates on the ignored filter.
    rows, _ = drain(
        lambda off: connector.read_table_deletes("leads", off, options), offset
    )
    assert rows == []


def test_leads_are_read_in_update_time_order_and_archived_snapshot_by_add_time():
    leads = [_lead(i, _hour(i)) for i in range(3)]
    client = V1Fake({"/v1/leads": leads, "/v1/leads/archived": leads})
    connector = _connector(client)
    drain(lambda off: connector.read_table("leads", off, SETTLE), None)
    list(connector.read_table("leads_archived", {}, {})[0])
    sorts = {path: params["sort"] for path, params in client.requests}
    assert sorts["/v1/leads"] == "update_time ASC, id ASC"
    assert sorts["/v1/leads/archived"] == "add_time ASC, id ASC"


def test_leads_read_table_is_cdc_with_deletes_and_resumable():
    lists = {"/v1/leads": [_lead(i, _hour(i)) for i in range(5)]}
    connector = _connector(V1Fake(lists))
    rows, _ = drain(
        lambda off: connector.read_table("leads", off, {**SETTLE, "max_records_per_batch": "2"}),
        None,
    )
    assert [r["id"] for r in rows] == [f"uuid-{i:05d}" for i in range(5)]
    assert connector.read_table_metadata("leads", {})["ingestion_type"] == "cdc_with_deletes"


@pytest.mark.parametrize("table", ["notes", "leads"])
def test_settle_margin_holds_back_recent_rows(table):
    """Rows newer than init minus settle wait for the next trigger."""
    when_old = "2024-06-01 10:00:00"
    when_recent = "2024-06-01 11:59:30"  # within 5 min of init 12:00:00
    rows = [_note(1, when_old)] if table == "notes" else [_lead(1, when_old)]
    rows.append(_note(2, when_recent) if table == "notes" else _lead(2, when_recent))
    client = V1Fake({f"/v1/{table}": rows})
    init = "2024-06-01T12:00:00Z"

    first, offset = drain(
        lambda off: _connector(client, init).read_table(table, off, {}), None
    )
    assert [r["id"] for r in first] == [1 if table == "notes" else "uuid-00001"]

    second, _ = drain(
        lambda off: _connector(client, "2024-06-01T13:00:00Z").read_table(table, off, {}),
        offset,
    )
    assert [r["id"] for r in second] == [2 if table == "notes" else "uuid-00002"]


@pytest.mark.parametrize("change", ["delete", "edit"])
def test_row_changed_between_pages_inside_one_batch_skips_nothing(change):
    """A visited note leaves its place between two page requests of the same batch
    (page size below the batch cap); every live note must still be read."""
    notes = [_note(i, _hour(i)) for i in range(1, 301)]
    state = {"calls": 0}

    def change_mid_batch(fake, path, params):  # pylint: disable=unused-argument
        if path != "/v1/notes":
            return
        state["calls"] += 1
        if state["calls"] == 3:  # third page of the first batch
            if change == "delete":
                fake.lists[path] = [r for r in fake.lists[path] if r["id"] != 20]
            else:
                for row in fake.lists[path]:
                    if row["id"] == 20:
                        row["update_time"] = _hour(250)

    client = V1Fake({"/v1/notes": notes}, on_request=change_mid_batch)
    options = {**SETTLE, "max_records_per_batch": "5000", "page_size": "50"}
    rows, _ = drain(lambda off: _connector(client).read_table("notes", off, options), None)
    assert state["calls"] >= 3
    assert {r["id"] for r in client.lists["/v1/notes"]} <= {r["id"] for r in rows}


def test_edit_of_a_read_row_inside_a_tie_group_between_batches_skips_nothing():
    """Four notes share one second; after the first is read it is edited, which shrinks
    the tie group. Resuming must still read the other three."""
    notes = [_note(i, "2024-02-03 00:00:00") for i in range(1, 5)]
    client = V1Fake({"/v1/notes": notes})
    options = {**SETTLE, "max_records_per_batch": "1", "page_size": "1"}
    first, offset = _connector(client).read_table("notes", None, options)
    first = [r["id"] for r in first]
    notes[0]["update_time"] = "2024-05-01 00:00:00"
    rest, _ = drain(lambda off: _connector(client).read_table("notes", off, options), offset)
    assert first == [1] and {2, 3, 4} <= {r["id"] for r in rest}
