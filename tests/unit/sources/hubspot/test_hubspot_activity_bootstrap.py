"""Large activity bootstraps resume only at complete HubSpot page boundaries."""

import copy
import hashlib
import json
from unittest.mock import Mock, patch
from urllib.parse import urlparse

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
        after = kwargs.get("params", {}).get("after")
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


def _digest(records):
    members = sorted(json.dumps(
        [str(record["id"]), "updatedAt" in record, record.get("updatedAt")],
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ) for record in records)
    payload = json.dumps(members, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _cursor_hash(value):
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _state(table, upper, after, complete, records, cursor_hashes, pages):
    return {
        "version": 1,
        "table": table,
        "upper": upper,
        "after": after,
        "complete": complete,
        "range_count": len(records),
        "range_digest": _digest(records),
        "cursor_hashes": cursor_hashes,
        "pages": pages,
    }


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("table", ["calls", "emails", "tasks", "notes"])
def test_bootstrap_resumes_replays_and_transitions_to_cdc(generated, table):
    api = BootstrapAPI(table)
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, first = _reader(generated, UPPER, table)
        rows, end1 = first.read({})
        assert [row.id for row in rows] == ["1", "2"]
        hash_a = _cursor_hash("opaque-a")
        hash_b = _cursor_hash("opaque-b")
        assert end1 == {"activity_bootstrap": _state(
            table, UPPER, "opaque-a", False, api.pages[None][0], [hash_a], 1,
        )}

        # Recreate the connector each time: only the persisted boundary/cursor
        # may define the continuation, not the new connector's initialization time.
        _, second = _reader(generated, LATER_INIT, table)
        rows, end2 = second.read(copy.deepcopy(end1))
        assert [row.id for row in rows] == ["3"]
        assert end2 == {"activity_bootstrap": _state(
            table, UPPER, "opaque-b", False, api.pages["opaque-a"][0], [hash_a, hash_b], 2,
        )}

        _, third = _reader(generated, LATER_INIT, table)
        rows, end3 = third.read(copy.deepcopy(end2))
        assert [row.id for row in rows] == ["4"]
        assert end3 == {
            "activity_bootstrap": _state(
                table, UPPER, None, True, api.pages["opaque-b"][0], [hash_a, hash_b], 3,
            ),
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
def test_bootstrap_replay_fails_if_membership_changes_with_same_cursor(generated):
    api = BootstrapAPI()
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, stream = _reader(generated, UPPER)
        rows, end = stream.read({})
        assert [row.id for row in rows] == ["1", "2"]
        api.pages[None] = ([api.parent("9"), api.parent("10")], "opaque-a")
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
    start = {"activity_bootstrap": _state(
        "emails", UPPER, "opaque-a", False, [], [_cursor_hash("opaque-a")], 1,
    )}
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _ = _reader(generated, LATER_INIT)
        with pytest.raises(RuntimeError, match="cursor"):
            conn.read_table("emails", start, {"max_records_per_batch": "1"})
    assert start["activity_bootstrap"]["after"] == "opaque-a"


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("field,value", [
    ("version", 2), ("version", True), ("table", "calls"),
])
def test_bootstrap_offset_is_versioned_and_table_bound(generated, field, value):
    api = BootstrapAPI()
    state = _state(
        "emails", UPPER, "opaque-a", False, [], [_cursor_hash("opaque-a")], 1,
    )
    state[field] = value
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _ = _reader(generated, LATER_INIT)
        with pytest.raises(RuntimeError, match="version|table|bootstrap"):
            conn.read_table("emails", {"activity_bootstrap": state}, {"max_records_per_batch": "1"})


@pytest.mark.parametrize("generated", [False, True])
def test_bootstrap_hard_page_limit_fails_before_request(generated):
    api = BootstrapAPI()
    state = _state(
        "emails", UPPER, "opaque-a", False, [], [_cursor_hash("opaque-a")], 100_000,
    )
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _ = _reader(generated, LATER_INIT)
        calls_before = len(api.object_afters)
        with pytest.raises(RuntimeError, match="page limit"):
            conn.read_table("emails", {"activity_bootstrap": state}, {"max_records_per_batch": "1"})
    assert len(api.object_afters) == calls_before


@pytest.mark.parametrize("generated", [False, True])
def test_completed_bootstrap_requires_at_least_one_scanned_page(generated):
    api = BootstrapAPI()
    state = _state("emails", UPPER, None, True, [], [], 0)
    offset = {"activity_bootstrap": state, "updatedAt": UPPER}
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _ = _reader(generated, LATER_INIT)
        with pytest.raises(RuntimeError, match="pagination|bootstrap|page"):
            conn.read_table("emails", offset, {"max_records_per_batch": "1"})


@pytest.mark.parametrize("generated", [False, True])
def test_bootstrap_replay_distinguishes_missing_and_empty_version(generated):
    api = BootstrapAPI()
    records = [api.parent("1"), api.parent("2")]
    records[0].pop("updatedAt")
    api.pages[None] = (records, "opaque-a")
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, stream = _reader(generated, UPPER)
        _, end = stream.read({})
        replay = copy.deepcopy(records)
        replay[0]["updatedAt"] = ""
        api.pages[None] = (replay, "opaque-a")
        with pytest.raises(RuntimeError, match="replay|range|offset"):
            list(stream.readBetweenOffsets({}, end))


@pytest.mark.parametrize("generated", [False, True])
def test_bootstrap_cursor_cycle_across_microbatches_fails_closed(generated):
    api = BootstrapAPI()
    api.pages["opaque-b"] = ([api.parent("4")], "opaque-a")
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, first = _reader(generated, UPPER)
        _, end1 = first.read({})
        _, second = _reader(generated, LATER_INIT)
        _, end2 = second.read(copy.deepcopy(end1))
        _, third = _reader(generated, LATER_INIT)
        with pytest.raises(RuntimeError, match="cursor|pagination"):
            third.read(copy.deepcopy(end2))


@pytest.mark.parametrize("generated", [False, True])
def test_future_completed_bootstrap_boundary_is_rejected(generated):
    api = BootstrapAPI()
    future = "2099-01-01T00:00:00.000Z"
    poisoned = {
        "activity_bootstrap": _state("emails", future, None, True, [], [], 0),
        "updatedAt": future,
    }
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, stream = _reader(generated, LATER_INIT)
        with pytest.raises(RuntimeError, match="bootstrap|boundary|future"):
            stream.read(copy.deepcopy(poisoned))


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("config_style", ["flat", "tableConfigs"])
def test_full_refresh_activity_batch_reader_drains_every_page(generated, config_style):
    from pyspark.sql.datasource import InputPartition
    from tests.unit.sources.hubspot.test_hubspot_table_options import _source_class

    api = BootstrapAPI()
    activity_options = {
        "association_sync_mode": "full_refresh",
        "max_records_per_batch": "1",
    }
    options = {"access_token": "offline", "tableName": "emails"}
    if config_style == "flat":
        options.update(activity_options)
    else:
        options["tableConfigs"] = json.dumps({"emails": activity_options})

    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post), \
            patch("time.sleep"):
        source = _source_class(generated)(options)
        rows = list(source.reader(source.schema()).read(InputPartition(None)))

    id_index = source.schema().fieldNames().index("id")
    assert [row[id_index] for row in rows] == ["1", "2", "3", "4"]
    assert api.object_afters == [None, "opaque-a", "opaque-b"]


@pytest.mark.parametrize("generated", [False, True])
def test_object_cursor_is_encoded_as_a_query_parameter(generated):
    api = BootstrapAPI()
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _, _ = consumer("emails", generated)
    response = Mock(status_code=200, json=lambda: {"results": []})
    malicious = "opaque&properties=private_field#fragment"
    with patch.object(conn._http, "get", return_value=response) as get:
        conn._fetch_full_refresh_batch("emails", ["public_field"], [], malicious)
    url = get.call_args.args[0]
    assert "?" not in url and malicious not in url
    assert get.call_args.kwargs["params"] == {
        "limit": 100,
        "archived": "false",
        "after": malicious,
        "properties": "public_field",
    }


@pytest.mark.parametrize("generated", [False, True])
def test_oversized_object_cursor_is_rejected(generated):
    api = BootstrapAPI()
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _, _ = consumer("emails", generated)
    with pytest.raises(RuntimeError, match="cursor"):
        conn._fetch_full_refresh_batch("emails", [], [], "x" * 1_000_000)
