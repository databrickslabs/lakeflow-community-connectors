"""Archive reconciliation must not checkpoint past unordered, unscanned pages."""

import copy
from urllib.parse import parse_qs, urlparse
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import _make_type_verifier

from databricks.labs.community_connector.sparkpds.lakeflow_datasource import IS_DELETE_FLOW
from tests.unit.sources.hubspot.test_hubspot_activity_consumer import ActivityAPI, FIELDS, consumer

UPPER = "2026-03-04T00:00:00.000Z"
START = {"updatedAt": "2026-02-28T00:00:00.000Z"}


class ArchiveAPI(ActivityAPI):
    def __init__(self, table):
        super().__init__(table)
        self.pages = [self.archived("1", "2026-03-03T00:00:00Z"),
                      self.archived("2", "2026-03-01T00:00:00Z"),
                      self.archived("3", "2026-03-05T00:00:00Z")]
        self.archive_calls = 0
        self.fail_page = None
        self.cycle = False

    def archived(self, identity, stamp):
        return {**self.parent(), "id": identity, "archived": True, "archivedAt": stamp}

    def get(self, url, **kwargs):
        if urlparse(url).path != f"/crm/objects/2026-09/{self.table}":
            return super().get(url, **kwargs)
        query = parse_qs(urlparse(url).query)
        assert query["archived"] == ["true"]
        index = int(query.get("after", ["0"])[0])
        self.archive_calls += 1
        assert self.archive_calls <= 6, "unbounded archive pagination"
        if index == self.fail_page:
            return Mock(status_code=503, text="synthetic later page failure")
        data = {"results": [self.pages[index]]}
        if index < len(self.pages) - 1:
            data["paging"] = {"next": {"after": "1" if self.cycle else str(index + 1)}}
        return Mock(status_code=200, json=lambda: copy.deepcopy(data))


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("table", [table for table in FIELDS if table != "meetings"])
def test_activity_archives_drain_pages_and_respect_frozen_window(table, generated):
    api = ArchiveAPI(table)
    with patch("requests.get", side_effect=api.get), patch("time.sleep", return_value=None):
        conn, schema, stream = consumer(table, generated)
        conn._init_ts = UPPER
        assert conn.read_table_metadata(table, {})["ingestion_type"] == "cdc_with_deletes"
        stream.options.update({IS_DELETE_FLOW: "true", "max_records_per_batch": "1"})
        rows, end = stream.read(dict(START))
        rows = list(rows)
        assert {row.id for row in rows} == {"1", "2"}
        assert all(row.archived is True for row in rows)
        assert {row.updatedAt for row in rows} == {
            "2026-03-03T00:00:00.000Z", "2026-03-01T00:00:00.000Z"}
        for row in rows:
            _make_type_verifier(schema)(row)
        assert end == {"updatedAt": UPPER}
        calls = api.archive_calls
        rows, repeat = stream.read(end)
        assert list(rows) == [] and repeat == end and api.archive_calls == calls


@pytest.mark.parametrize("generated", [False, True])
def test_archive_failure_after_first_page_does_not_checkpoint(generated):
    api = ArchiveAPI("calls")
    api.fail_page = 1
    with patch("requests.get", side_effect=api.get), patch("time.sleep", return_value=None):
        conn, _, _ = consumer("calls", generated)
        conn._init_ts = UPPER
        start = dict(START)
        with pytest.raises(Exception, match="HubSpot"):
            conn.read_table_deletes("calls", start, {})
    assert start == START


@pytest.mark.parametrize("generated", [False, True])
def test_archive_cursor_cycle_fails_closed(generated):
    api = ArchiveAPI("calls")
    api.cycle = True
    with patch("requests.get", side_effect=api.get), patch("time.sleep", return_value=None):
        conn, _, _ = consumer("calls", generated)
        conn._init_ts = UPPER
        with pytest.raises(RuntimeError, match="cursor|pagination"):
            conn.read_table_deletes("calls", dict(START), {})


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("field,value", [("archived", False), ("archivedAt", None),
                                        ("archivedAt", "2026-03-01T00:00:00"),
                                        ("id", True), ("id", " ")])
def test_malformed_archive_fails_before_checkpoint(generated, field, value):
    api = ArchiveAPI("calls")
    api.pages[1][field] = value
    with patch("requests.get", side_effect=api.get), patch("time.sleep", return_value=None):
        conn, _, _ = consumer("calls", generated)
        conn._init_ts = UPPER
        start = dict(START)
        with pytest.raises((RuntimeError, ValueError)):
            conn.read_table_deletes("calls", start, {})
    assert start == START


@pytest.mark.parametrize("generated", [False, True])
def test_conflicting_duplicate_archive_fails_before_checkpoint(generated):
    api = ArchiveAPI("calls")
    api.pages[1]["id"] = "1"
    with patch("requests.get", side_effect=api.get), patch("time.sleep", return_value=None):
        conn, _, _ = consumer("calls", generated)
        conn._init_ts = UPPER
        with pytest.raises(RuntimeError, match="Conflicting duplicate"):
            conn.read_table_deletes("calls", dict(START), {})
