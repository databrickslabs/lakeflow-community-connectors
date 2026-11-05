"""Large activity bootstraps resume only at complete HubSpot page boundaries."""

import copy
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from tests.unit.sources.hubspot.test_hubspot_activity_consumer import ActivityAPI, consumer

UPPER = "2026-01-03T00:00:00.000Z"
LATER_INIT = "2026-01-05T00:00:00.000Z"
CDC_TIME = "2026-01-04T00:00:00.000Z"


class BootstrapAPI(ActivityAPI):
    def __init__(self, table="emails"):
        super().__init__(table)
        self.object_afters = []
        self.search_calls = 0
        self.pages = {
            None: ([self.parent("1"), self.parent("2")], "opaque-a"),
            "opaque-a": ([self.parent("3")], "opaque-b"),
            "opaque-b": ([self.parent("4")], None),
        }

    def parent(self, identity="1", updated_at=None):
        parent = super().parent()
        parent["id"] = identity
        if updated_at is not None:
            parent["updatedAt"] = updated_at
            parent["properties"] = {**parent["properties"], "hs_lastmodifieddate": updated_at}
        return parent

    def get(self, url, **kwargs):
        if urlparse(url).path != f"/crm/objects/2026-09/{self.table}":
            return super().get(url, **kwargs)
        self.requests.append(("GET", url, kwargs))
        after = parse_qs(urlparse(url).query).get("after", [None])[0]
        self.object_afters.append(after)
        records, next_after = self.pages[after]
        payload = {"results": copy.deepcopy(records)}
        if next_after is not None:
            payload["paging"] = {"next": {"after": next_after}}
        return Mock(status_code=200, json=lambda: payload)

    def post(self, url, **kwargs):
        if not url.endswith("/search"):
            return super().post(url, **kwargs)
        self.requests.append(("POST", url, kwargs))
        self.search_calls += 1
        payload = {"results": [self.parent("5", CDC_TIME)]}
        return Mock(status_code=200, json=lambda: copy.deepcopy(payload))


def _reader(generated, init_ts, table="emails"):
    conn, _, stream = consumer(table, generated)
    conn._init_ts = init_ts
    stream.options["max_records_per_batch"] = "1"
    return conn, stream


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("table", ["calls", "emails", "tasks", "notes"])
def test_bootstrap_resumes_replays_and_transitions_to_cdc(generated, table):
    api = BootstrapAPI(table)
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, first = _reader(generated, UPPER, table)
        rows, end1 = first.read({})
        assert [row.id for row in rows] == ["1", "2"]
        assert end1 == {"activity_bootstrap": {
            "upper": UPPER, "after": "opaque-a", "complete": False,
        }}

        # Recreate the connector each time: only the persisted boundary/cursor
        # may define the continuation, not the new connector's initialization time.
        _, second = _reader(generated, LATER_INIT, table)
        rows, end2 = second.read(copy.deepcopy(end1))
        assert [row.id for row in rows] == ["3"]
        assert end2 == {"activity_bootstrap": {
            "upper": UPPER, "after": "opaque-b", "complete": False,
        }}

        _, third = _reader(generated, LATER_INIT, table)
        rows, end3 = third.read(copy.deepcopy(end2))
        assert [row.id for row in rows] == ["4"]
        assert end3 == {
            "activity_bootstrap": {"upper": UPPER, "after": None, "complete": True},
            "updatedAt": UPPER,
        }

        # Spark replay reads the exact recorded page range, including across a
        # new connector whose own frozen timestamp differs.
        assert [row.id for row in first.readBetweenOffsets({}, end1)] == ["1", "2"]
        assert [row.id for row in second.readBetweenOffsets(end1, end2)] == ["3"]
        assert list(third.readBetweenOffsets(end3, end3)) == []

        _, cdc = _reader(generated, LATER_INIT, table)
        rows, end4 = cdc.read(copy.deepcopy(end3))
        assert [row.id for row in rows] == ["5"]
        assert end4 == {"updatedAt": CDC_TIME}

    assert api.object_afters[:3] == [None, "opaque-a", "opaque-b"]
    assert api.search_calls == 1


@pytest.mark.parametrize("generated", [False, True])
def test_bootstrap_replay_fails_if_recorded_page_range_changes(generated):
    api = BootstrapAPI()
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, stream = _reader(generated, UPPER)
        rows, end = stream.read({})
        assert len(list(rows)) == 2
        api.pages[None] = ([api.parent("1"), api.parent("2")], "changed-cursor")
        with pytest.raises(RuntimeError, match="replay|offset|range"):
            list(stream.readBetweenOffsets({}, end))


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("state", [
    None,
    {},
    {"upper": UPPER, "after": None, "complete": False},
    {"upper": UPPER, "after": True, "complete": False},
    {"upper": "not-a-time", "after": "opaque-a", "complete": False},
    {"upper": UPPER, "after": "opaque-a", "complete": "false"},
])
def test_malformed_bootstrap_offset_fails_closed(generated, state):
    api = BootstrapAPI()
    start = {"activity_bootstrap": state}
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _ = _reader(generated, LATER_INIT)
        with pytest.raises(RuntimeError, match="bootstrap|offset"):
            conn.read_table("emails", start, {"max_records_per_batch": "1"})
    assert start == {"activity_bootstrap": state}


@pytest.mark.parametrize("generated", [False, True])
def test_repeated_bootstrap_cursor_fails_before_checkpoint(generated):
    api = BootstrapAPI()
    api.pages["opaque-a"] = ([api.parent("3")], "opaque-a")
    start = {"activity_bootstrap": {
        "upper": UPPER, "after": "opaque-a", "complete": False,
    }}
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _ = _reader(generated, LATER_INIT)
        with pytest.raises(RuntimeError, match="cursor"):
            conn.read_table("emails", start, {"max_records_per_batch": "1"})
    assert start["activity_bootstrap"]["after"] == "opaque-a"
