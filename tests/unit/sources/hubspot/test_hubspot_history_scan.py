"""Persisted history scans: replay, restart, >10k ties, and fail-closed offsets."""

import json
import runpy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from databricks.labs.community_connector.sources.hubspot import hubspot_property_history as hph
from databricks.labs.community_connector.sources.hubspot import hubspot as source_module
from tests.unit.sources.hubspot.test_hubspot_custom_objects import _connector_class

UPPER = "2026-02-01T12:00:00.000Z"
PARENT_TIME = "2026-02-01T11:59:00.000Z"
OLD_HISTORY = "2020-01-01T00:00:00.000Z"


def reader(generated=False, upper=UPPER):
    conn = _connector_class(generated)({"access_token": "offline"})
    history = conn._history
    history._init_ts = upper
    history._init_dt = hph._parse_ts(upper)
    history._sleep = lambda _: None
    return history


def stream_reader(conn, options, generated):
    from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowStreamReader
    if not generated:
        return LakeflowStreamReader(options, conn._history.get_table_schema(), conn)
    registry = Mock()
    path = Path(source_module.__file__).with_name("_generated_hubspot_python_source.py")
    runpy.run_path(str(path))["register_lakeflow_source"](SimpleNamespace(dataSource=registry))
    source = registry.register.call_args.args[0]({**options, "access_token": "offline"})
    source.lakeflow_connect = conn
    return source.simpleStreamReader(conn._history.get_table_schema())


class Search:
    def __init__(self, count):
        self.parents = [{"id": str(i), "updatedAt": PARENT_TIME} for i in range(1, count + 1)]
        self.bodies = []
        self.fail_batch = False

    def post(self, url, **kwargs):
        body = kwargs["json"]
        if url.endswith("/search"):
            self.bodies.append(body)
            # The API applies numeric ID ordering and the requested predicates.
            filters = body["filterGroups"][0]["filters"]
            def matches(parent):
                for f in filters:
                    value = int(parent["id"]) if f["propertyName"] == "hs_object_id" else hph._to_ms(hph._parse_ts(parent["updatedAt"]))
                    wanted = int(f["value"])
                    if f["operator"] == "GT" and value <= wanted:
                        return False
                    if f["operator"] == "GTE" and value < wanted:
                        return False
                    if f["operator"] == "LTE" and value > wanted:
                        return False
                return True
            parents = [p for p in self.parents if matches(p)]
            page = parents[:body["limit"]]
            paging = {"next": {"after": "200"}} if len(parents) > len(page) else {}
            return Mock(status_code=200, json=lambda: {"results": page, "paging": paging})
        if self.fail_batch:
            return Mock(status_code=503, text="synthetic failure")
        return Mock(status_code=200, json=lambda: {"status": "COMPLETE", "results": [
            {"id": p["id"], "propertiesWithHistory": {"amount": [
                {"value": "1", "timestamp": OLD_HISTORY},
                {"value": "2", "timestamp": PARENT_TIME},
            ]}} for p in body["inputs"]
        ]})


@pytest.mark.parametrize("generated", [False, True])
def test_late_indexed_parent_emits_old_history_and_advances_scan(generated):
    fake = Search(1)
    start = {"updatedAt": PARENT_TIME}
    with patch("requests.post", side_effect=fake.post), patch("requests.get", return_value=Mock(status_code=200, json=lambda: {"results": []})):
        rows, end = reader(generated).read_table("deals_property_history", start, {"history_properties": "amount"})
        rows = list(rows)
    assert {row["timestamp"] for row in rows} == {OLD_HISTORY, PARENT_TIME}
    assert end["updatedAt"] == UPPER and end != start
    assert end["history_scan"]["complete"] is True


@pytest.mark.parametrize("generated", [False, True])
def test_scan_restart_and_more_than_ten_thousand_tied_parents(generated):
    fake = Search(10_005)
    offset = {}
    seen = []
    opts = {"history_properties": "amount", "max_records_per_batch": "350"}
    with patch("requests.post", side_effect=fake.post), patch("requests.get", return_value=Mock(status_code=200, json=lambda: {"results": []})):
        for _ in range(100):
            # Restart the connector for every call to exercise persisted state.
            rows, end = reader(generated).read_table("deals_property_history", json.loads(json.dumps(offset)), opts)
            rows = list(rows)
            assert not rows or end != offset
            seen.extend(r["object_id"] for r in rows if r["timestamp"] == OLD_HISTORY)
            if end == offset:
                break
            offset = end
        else:
            pytest.fail("scan did not terminate")
    assert seen == [str(i) for i in range(1, 10_006)]
    assert offset["updatedAt"] == UPPER
    assert offset["history_scan"]["complete"] is True
    assert all("after" not in body for body in fake.bodies)
    assert all(body["sorts"] == [{"propertyName": "hs_object_id", "direction": "ASCENDING"}] for body in fake.bodies)


@pytest.mark.parametrize("generated", [False, True])
def test_failed_history_chunk_does_not_mutate_checkpoint(generated):
    fake = Search(201)
    opts = {"history_properties": "amount", "max_records_per_batch": "1"}
    with patch("requests.post", side_effect=fake.post), patch("requests.get", return_value=Mock(status_code=200, json=lambda: {"results": []})):
        _, offset = reader(generated).read_table("deals_property_history", {}, opts)
        original = json.dumps(offset, sort_keys=True)
        assert "history_scan" in offset
        fake.fail_batch = True
        with pytest.raises(RuntimeError):
            reader(generated).read_table("deals_property_history", offset, opts)
    assert json.dumps(offset, sort_keys=True) == original


@pytest.mark.parametrize("generated", [False, True])
def test_parent_order_violation_rejected_before_checkpoint(generated):
    fake = Search(2)
    fake.parents.reverse()
    with patch("requests.post", side_effect=fake.post), patch("requests.get", return_value=Mock(status_code=200, json=lambda: {"results": []})):
        with pytest.raises(RuntimeError, match="order|identity"):
            reader(generated).read_table("deals_property_history", {}, {"history_properties": "amount"})


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("planned_cap", ["1", "10000"])
def test_stream_recovery_honors_planned_end_despite_new_cap_and_clock(generated, planned_cap):
    fake = Search(250)
    conn = _connector_class(generated)({"access_token": "offline"})
    conn._history._init_ts = UPPER
    conn._history._init_dt = hph._parse_ts(UPPER)
    conn._history._sleep = lambda _: None
    opts = {"tableName": "deals_property_history", "history_properties": "amount",
            "max_records_per_batch": planned_cap}
    stream = stream_reader(conn, opts, generated)
    with patch("requests.post", side_effect=fake.post), patch("requests.get", return_value=Mock(status_code=200, json=lambda: {"results": []})):
        planned, end = stream.read({})
        expected = list(planned)
        # Executor reconstruction has a new clock and a much smaller cap.
        conn2 = _connector_class(generated)({"access_token": "offline"})
        conn2._history._init_ts = "2026-02-02T00:00:00.000Z"
        conn2._history._init_dt = hph._parse_ts(conn2._history._init_ts)
        conn2._history._sleep = lambda _: None
        recovery = stream_reader(conn2, {**opts, "max_records_per_batch": "1"}, generated)
        actual = list(recovery.readBetweenOffsets({}, json.loads(json.dumps(end))))
    assert actual == expected
    assert len(actual) == (100 if planned_cap == "1" else 500)
    lte_times = [f["value"] for body in fake.bodies for f in body["filterGroups"][0]["filters"]
                 if f["operator"] == "LTE" and f["propertyName"] != "hs_object_id"]
    assert set(lte_times) == {str(hph._to_ms(hph._parse_ts(UPPER)))}


@pytest.mark.parametrize("generated", [False, True])
def test_scan_resumes_frozen_bounds_and_explicit_history_floor(generated):
    fake = Search(120)
    opts = {"history_properties": "amount", "history_since": PARENT_TIME,
            "max_records_per_batch": "1"}
    with patch("requests.post", side_effect=fake.post), patch("requests.get", return_value=Mock(status_code=200, json=lambda: {"results": []})):
        rows, start = reader(generated).read_table("deals_property_history", {}, opts)
        assert {r["timestamp"] for r in rows} == {PARENT_TIME}
        rows, end = reader(generated, "2026-02-02T00:00:00.000Z").read_table(
            "deals_property_history", start, opts)
        assert {r["object_id"] for r in rows} == {str(i) for i in range(51, 101)}
        assert end["history_scan"]["upper"] == UPPER
        filters = fake.bodies[-1]["filterGroups"][0]["filters"]
        assert {f["value"] for f in filters if f["operator"] == "GTE"} == {
            str(hph._to_ms(hph._parse_ts(PARENT_TIME)))}


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("change", [
    {"version": 2}, {"version": True}, {"after_id": "oops"},
    {"complete": "false"}, {"upper": "bad"}, {"lower": "2030-01-01T00:00:00Z"},
])
def test_malformed_scan_fails_without_source_reads(generated, change):
    scan = {"version": 1, "lower": None, "upper": UPPER, "after_id": "50", "complete": False,
            "parent_count": 1, "parent_digest": "0" * 64}
    offset = {"history_scan": {**scan, **change}}
    with patch("requests.post") as post:
        with pytest.raises(RuntimeError, match="history"):
            reader(generated).read_table("deals_property_history", offset, {"history_properties": "amount"})
    post.assert_not_called()


@pytest.mark.parametrize("generated", [False, True])
def test_future_since_and_empty_histories_terminate_with_valid_offsets(generated):
    fake = Search(2)
    history = reader(generated)
    opts = {"history_properties": "amount", "history_since": "2027-01-01T00:00:00Z"}
    with patch("requests.post", side_effect=fake.post) as post:
        rows, end = history.read_table("deals_property_history", {}, opts)
        assert list(rows) == [] and end["history_scan"]["complete"] is True
        assert history.read_table("deals_property_history", end, opts)[1] == end
    post.assert_not_called()
    def post_empty_history(url, **kwargs):
        if url.endswith("/search"):
            return fake.post(url, **kwargs)
        return Mock(status_code=200, json=lambda: {"status": "COMPLETE", "results": [
            {"id": item["id"], "propertiesWithHistory": {}} for item in kwargs["json"]["inputs"]
        ]})
    with patch("requests.post", side_effect=post_empty_history):
        rows, end = history.read_table("deals_property_history", {}, {"history_properties": "amount"})
        assert list(rows) == [] and end["history_scan"]["after_id"] == "2"
        assert history.read_table("deals_property_history", end, {})[1] == end


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("payload", [
    {"results": None}, {"results": [], "paging": []},
    {"results": [], "paging": {"next": {"after": "x"}}},
    {"results": [{"id": "1", "updatedAt": "2030-01-01T00:00:00Z"}]},
    {"results": [{"id": "1", "updatedAt": "bad"}]},
    {"results": [{"id": "x", "updatedAt": PARENT_TIME}]},
])
def test_invalid_search_pages_fail_without_checkpoint(generated, payload):
    start = {"updatedAt": PARENT_TIME}
    with patch("requests.post", return_value=Mock(status_code=200, json=lambda: payload)):
        with pytest.raises(RuntimeError, match="HubSpot history"):
            reader(generated).read_table("deals_property_history", start, {"history_properties": "amount"})
    assert start == {"updatedAt": PARENT_TIME}


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("parent_count", [1, 2])
def test_recovery_fails_if_planned_parent_disappears(generated, parent_count):
    fake = Search(parent_count)
    opts = {"history_properties": "amount"}
    with patch("requests.post", side_effect=fake.post):
        planned, end = reader(generated).read_table("deals_property_history", {}, opts)
        assert len(list(planned)) == parent_count * 2
        fake.parents = [parent for parent in fake.parents if parent["id"] != "1"]
        with pytest.raises(RuntimeError, match="recovery.*parent|parent.*recovery"):
            list(reader(generated).read_between_offsets("deals_property_history", {}, end, opts))
