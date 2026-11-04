"""Meetings must reconcile from complete active snapshots, never archive paging."""
import copy
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from tests.unit.sources.hubspot.test_hubspot_activity_consumer import ActivityAPI, consumer


class SnapshotAPI(ActivityAPI):
    def __init__(self, table="meetings"):
        super().__init__(table)
        self.pages = [{"results": [self.parent()], "paging": {"next": {"after": "1"}}},
                      {"results": [{**self.parent(), "id": "2"}]}]
        self.active_calls = 0

    def get(self, url, **kwargs):
        if urlparse(url).path != f"/crm/objects/2026-09/{self.table}":
            return super().get(url, **kwargs)
        query = parse_qs(urlparse(url).query)
        assert query["archived"] == ["false"], "meetings cannot list archives"
        self.active_calls += 1
        assert self.active_calls < 6, "unbounded active pagination"
        index = int(query.get("after", ["0"])[0])
        return Mock(status_code=200, json=lambda: copy.deepcopy(self.pages[index]))


@pytest.mark.parametrize("generated", [False, True])
def test_meetings_default_snapshot_ignores_old_offset_and_cap(generated):
    api = SnapshotAPI()
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post), patch("time.sleep"):
        conn, _, _ = consumer("meetings", generated)
        assert conn.read_table_metadata("meetings", {})["ingestion_type"] == "snapshot"
        saved = {"updatedAt": "2099-01-01T00:00:00.000Z"}
        for options in ({}, {"association_sync_mode": "incremental", "max_records_per_batch": "1"}):
            rows, _ = conn.read_table("meetings", saved, options)
            assert {r["id"] for r in rows} == {"1", "2"}
        assert list(conn.read_table_deletes("meetings", saved, {})[0]) == []
        assert saved == {"updatedAt": "2099-01-01T00:00:00.000Z"}


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("case", ["empty_continuation", "cycle", "malformed_record", "duplicate_id", "archived_record"])
def test_incomplete_active_snapshot_never_publishes(generated, case):
    api = SnapshotAPI()
    if case == "empty_continuation":
        api.pages[0]["results"] = []
    elif case == "cycle":
        api.pages[1]["paging"] = {"next": {"after": "1"}}
    elif case == "malformed_record":
        api.pages[1]["results"] = [None]
    elif case == "duplicate_id":
        api.pages[1]["results"][0]["id"] = "1"
    else:
        api.pages[1]["results"][0]["archived"] = True
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post), patch("time.sleep"):
        conn, _, _ = consumer("meetings", generated)
        start = {}
        with pytest.raises(RuntimeError, match="snapshot|pagination"):
            conn.read_table("meetings", start, {"association_sync_mode": "full_refresh"})
        assert start == {}


@pytest.mark.parametrize("generated", [False, True])
def test_each_snapshot_returns_current_set_including_empty(generated):
    api = SnapshotAPI()
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post), patch("time.sleep"):
        conn, _, _ = consumer("meetings", generated)
        rows, offset = conn.read_table("meetings", {}, {})
        assert {r["id"] for r in rows} == {"1", "2"}
        api.active_calls = 0
        api.pages = [{"results": [{**api.parent(), "id": "2"}]}]
        rows, offset = conn.read_table("meetings", offset, {})
        assert [r["id"] for r in rows] == ["2"]
        api.pages = [{"results": []}]
        rows, _ = conn.read_table("meetings", offset, {})
        assert list(rows) == []


@pytest.mark.parametrize("generated", [False, True])
def test_meeting_strategy_does_not_change_other_objects_or_history(generated):
    api = SnapshotAPI()
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _, _ = consumer("meetings", generated)
        for table in ("calls", "emails", "tasks", "notes", "deals"):
            assert conn.read_table_metadata(table, {})["ingestion_type"] == "cdc_with_deletes"
            assert conn.read_table_metadata(table, {"association_sync_mode": "full_refresh"})["ingestion_type"] == "snapshot"
            assert conn.read_table_metadata(table, {})["ingestion_type"] == "cdc_with_deletes"
        assert conn.read_table_metadata("deals_property_history", {})["ingestion_type"] == "cdc"
        assert conn.read_table_metadata("meetings", {"association_sync_mode": "incremental"})["ingestion_type"] == "snapshot"


@pytest.mark.parametrize("generated", [False, True])
def test_numeric_zero_cursor_is_followed_not_treated_as_end(generated):
    api = SnapshotAPI()
    calls = []

    def get(url, **kwargs):
        if urlparse(url).path != "/crm/objects/2026-09/meetings":
            return api.get(url, **kwargs)
        calls.append(url)
        payload = ({"results": [api.parent()], "paging": {"next": {"after": 0}}}
                   if len(calls) == 1 else {"results": [{**api.parent(), "id": "2"}]})
        return Mock(status_code=200, json=lambda: copy.deepcopy(payload))

    with patch("requests.get", side_effect=get), patch("requests.post", side_effect=api.post), patch("time.sleep"):
        conn, _, _ = consumer("meetings", generated)
        rows, _ = conn.read_table("meetings", {}, {})
        assert {r["id"] for r in rows} == {"1", "2"}
        assert parse_qs(urlparse(calls[1]).query)["after"] == ["0"]


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("envelope", [
    {"status": "error", "errors": [{"category": "MISSING_SCOPES"}], "numErrors": 1},
    {"errors": []}, {"numErrors": 0}, {"category": "VALIDATION_ERROR"},
    {"status": "PENDING"},
])
@pytest.mark.parametrize("nonempty", [False, True])
def test_error_envelope_never_becomes_authoritative_snapshot(generated, envelope, nonempty):
    api = SnapshotAPI()

    def get(url, **kwargs):
        if urlparse(url).path != "/crm/objects/2026-09/meetings":
            return api.get(url, **kwargs)
        payload = {"results": [api.parent()] if nonempty else [], **envelope}
        return Mock(status_code=200, json=lambda: copy.deepcopy(payload))

    with patch("requests.get", side_effect=get), patch("requests.post", side_effect=api.post), patch("time.sleep"):
        conn, _, _ = consumer("meetings", generated)
        start = {}
        with pytest.raises(RuntimeError, match="snapshot|object.*error|response"):
            conn.read_table("meetings", start, {})
        assert start == {}


@pytest.mark.parametrize("generated", [False, True])
def test_incremental_search_forwards_numeric_zero_cursor(generated):
    api = ActivityAPI("calls")
    bodies = []

    def post(url, **kwargs):
        if not url.endswith("/search"):
            return api.post(url, **kwargs)
        body = kwargs["json"]
        bodies.append(copy.deepcopy(body))
        assert len(bodies) <= 2, "incremental cursor zero was lost"
        payload = ({"results": [api.parent()], "paging": {"next": {"after": 0}}}
                   if "after" not in body else
                   {"results": [{**api.parent(), "id": "2", "updatedAt": "2026-01-03T00:00:00.000Z"}]})
        return Mock(status_code=200, json=lambda: copy.deepcopy(payload))

    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=post), patch("time.sleep"):
        conn, _, _ = consumer("calls", generated)
        start = {"updatedAt": "2025-01-01T00:00:00.000Z"}
        rows, _ = conn.read_table("calls", start, {})
        assert {r["id"] for r in rows} == {"1", "2"}
        assert bodies[1]["after"] == 0
        assert start == {"updatedAt": "2025-01-01T00:00:00.000Z"}
