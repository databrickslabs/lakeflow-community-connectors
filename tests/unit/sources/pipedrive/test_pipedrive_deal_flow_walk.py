"""deal_flow parent walk: deals edited / archived / unarchived mid-walk.

Regression for review finding R1-01: the parent query used to be capped at
``updated_until=<init time>``, so a deal edited (or archived) after the trigger
started but before the walk reached it fell out of every remaining page and
phase, and its window entries were lost for good.
"""

from databricks.labs.community_connector.sources.pipedrive.pipedrive import (
    PipedriveLakeflowConnect,
)
from databricks.labs.community_connector.sources.pipedrive.pipedrive_utils import (
    normalize_ts,
    now_utc,
)

LOWER = "2024-01-01T00:00:00Z"
UNTIL = "2024-06-01T00:00:00Z"


class _FakeClient:
    """In-memory /deals, /deals/archived and /deals/{id}/flow with mutation hooks.

    v2 pages hold one deal each and are evaluated lazily, so a mutation made
    while one deal's flow is fetched is visible to the next page request.
    """

    def __init__(self, deals, flows, hooks):
        self.deals = deals  # id -> {"id", "update_time", "archived"}
        self.flows = flows  # id -> [entry dict]
        self.hooks = hooks  # id -> callable(client), run once on first flow fetch
        self.flow_calls = []

    def iter_v2_pages(self, path, params):
        """One deal per page, filtered and sorted like the v2 list endpoints."""
        archived = path.endswith("/archived")
        since = params["updated_since"]
        until = params.get("updated_until")
        emitted = set()
        while True:
            candidates = sorted(
                (
                    d
                    for d in self.deals.values()
                    if d["archived"] == archived
                    and d["update_time"] >= since
                    and (until is None or d["update_time"] <= until)
                    and d["id"] not in emitted
                ),
                key=lambda d: (d["update_time"], d["id"]),
            )
            if not candidates:
                return
            deal = candidates[0]
            emitted.add(deal["id"])
            yield [{"id": deal["id"], "update_time": deal["update_time"]}]

    def iter_v1_pages(self, path, params):  # pylint: disable=unused-argument
        """A deal's flow as one page; runs the deal's mutation hook first."""
        deal_id = int(path.split("/")[-2])
        self.flow_calls.append(deal_id)
        hook = self.hooks.pop(deal_id, None)
        if hook:
            hook(self)
        yield [
            {"object": "dealChange", "timestamp": e["log_time"], "data": dict(e)}
            for e in self.flows.get(deal_id, [])
        ]


def _entry(entry_id, deal_id, log_time):
    return {"id": entry_id, "item_id": deal_id, "log_time": log_time}


def _run_to_completion(connector, table_options, max_batches=100):
    offset = {"cursor": LOWER}
    rows = []
    for _ in range(max_batches):
        records, end = connector.read_table("deal_flow", offset, table_options)
        rows.extend(records)
        if end == offset:
            return rows, end
        offset = end
        if offset == {"cursor": UNTIL}:
            # One more call must be a no-op (stream caught up to init time).
            records, again = connector.read_table("deal_flow", offset, table_options)
            assert not list(records) and again == offset
            return rows, offset
    raise AssertionError("deal_flow did not converge")


def test_deals_changed_mid_walk_keep_their_window_entries():
    """Edited, archived and unarchived deals are all still visited."""
    deals = {
        1: {"id": 1, "update_time": "2024-02-01T00:00:00Z", "archived": False},
        2: {"id": 2, "update_time": "2024-03-01T00:00:00Z", "archived": False},
        3: {"id": 3, "update_time": "2024-04-01T00:00:00Z", "archived": False},
        4: {"id": 4, "update_time": "2024-01-15T00:00:00Z", "archived": True},
        5: {"id": 5, "update_time": "2024-02-15T00:00:00Z", "archived": True},
    }
    flows = {
        1: [_entry(101, 1, "2024-02-01 00:00:00")],
        2: [_entry(201, 2, "2024-03-01 00:00:00")],
        3: [_entry(301, 3, "2024-04-01 00:00:00")],
        4: [_entry(401, 4, "2024-01-15 00:00:00")],
        5: [_entry(501, 5, "2024-02-15 00:00:00")],
    }

    def edit_2_and_archive_3(client):
        # Edited after ``until``, before the walk reaches it.
        client.deals[2]["update_time"] = "2024-07-01T00:00:00Z"
        client.flows[2].append(_entry(202, 2, "2024-07-01 00:00:00"))
        # Archived mid-walk: leaves /deals with a bumped update_time.
        client.deals[3].update(archived=True, update_time="2024-07-02T00:00:00Z")

    def unarchive_5(client):
        # Unarchived while /deals/archived is walked: re-enters /deals,
        # which phase 0 already finished.
        client.deals[5].update(archived=False, update_time=normalize_ts(now_utc()))

    def edit_1_after_processing(client):
        # Already processed: moves to the end of the sort and is revisited.
        client.deals[1]["update_time"] = "2024-07-03T00:00:00Z"

    hooks = {1: edit_2_and_archive_3, 4: unarchive_5}
    client = _FakeClient(deals, flows, hooks)
    # Deal 1's own edit happens once deal 3 (archived) is fetched in phase 1.
    hooks[3] = edit_1_after_processing

    connector = PipedriveLakeflowConnect(
        {"api_token": "fake", "start_date": LOWER}
    )
    connector._client = client  # pylint: disable=protected-access
    connector._init_ts = UNTIL  # pylint: disable=protected-access

    rows, final = _run_to_completion(
        connector, {"max_records_per_batch": "1", "lookback_seconds": "0"}
    )

    assert final == {"cursor": UNTIL}
    ids = {r["id"] for r in rows}
    # Every entry in (LOWER, UNTIL] is emitted; 202 is past ``until``.
    assert ids == {101, 201, 301, 401, 501}
    # Re-visits are allowed (deal_flow upserts on id); nothing is lost.
    assert set(client.flow_calls) == {1, 2, 3, 4, 5}


def test_deal_flow_is_upserted_on_id():
    """Revisited deals re-emit rows, so deal_flow must upsert on ``id``."""
    connector = PipedriveLakeflowConnect({"api_token": "fake"})
    metadata = connector.read_table_metadata("deal_flow", {})
    assert metadata["ingestion_type"] == "cdc"
    assert metadata["primary_keys"] == ["id"]
    assert metadata["cursor_field"] == "log_time"
