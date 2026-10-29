"""Unit tests for Asset Framework / Event Frame read resilience.

These cover defensive handling the simulator harness does not exercise:
- ``_read_assetdatabases`` skips an asset server the credential cannot see
  (401/403) instead of failing the whole read, and ``_read_event_frames``
  reports the skip count, and
- ``_read_event_frames`` fails on a server error (5xx) so the append offset
  does not move past an unread window, and treats 404 as end-of-pages.
"""

import logging

import pytest
import requests

from databricks.labs.community_connector.sources.osipi.osipi import OsipiLakeflowConnect

OPTIONS = {
    "pi_base_url": "https://simulator-pi.example.com",
    "access_token": "fake-token",
}


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    resp = requests.Response()
    resp.status_code = status_code
    return requests.exceptions.HTTPError("boom", response=resp)


@pytest.mark.parametrize("status", [401, 403])
def test_assetdatabases_skips_on_access_denied(monkeypatch, status):
    """A 401/403 on an asset server yields an empty list, not an exception."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(
        connector._client,
        "get_json",
        lambda path, params=None: (_ for _ in ()).throw(_http_error(status)),
    )

    assert connector._read_assetdatabases("S1") == []


def test_assetdatabases_reraises_other_errors(monkeypatch):
    """A non-auth error (e.g. 500) is not swallowed."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(
        connector._client,
        "get_json",
        lambda path, params=None: (_ for _ in ()).throw(_http_error(500)),
    )

    with pytest.raises(requests.exceptions.HTTPError):
        connector._read_assetdatabases("S1")


def test_event_frames_raises_on_db_5xx(monkeypatch):
    """A 5xx fails the read so the framework retries the same window.

    Skipping the database instead would advance the append offset past its
    event frames, which a later run would never re-read.
    """
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_read_assetservers", lambda: [{"WebId": "S1", "Name": "srv1"}])
    monkeypatch.setattr(
        connector,
        "_read_assetdatabases",
        lambda _srv: [{"WebId": "D1", "Name": "db1"}, {"WebId": "D2", "Name": "db2"}],
    )

    def fake_get_json(path, params=None):
        if "D1" in path:
            raise _http_error(503)
        return {"Items": [{"WebId": "EF2", "Name": "ef2"}]}

    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    with pytest.raises(requests.exceptions.HTTPError):
        connector._read_event_frames({"offset": "2026-01-01T00:00:00Z"}, {})


def test_event_frames_reports_access_denied_server_skips(monkeypatch, caplog):
    """A 401/403 server is skipped with a count warning; other servers ingest."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(
        connector,
        "_read_assetservers",
        lambda: [{"WebId": "S1", "Name": "srv1"}, {"WebId": "S2", "Name": "srv2"}],
    )

    def fake_get_json(path, params=None):
        if path == "/piwebapi/assetservers/S1/assetdatabases":
            raise _http_error(403)
        if path == "/piwebapi/assetservers/S2/assetdatabases":
            return {"Items": [{"WebId": "D2", "Name": "db2"}]}
        return {"Items": [{"WebId": "EF2", "Name": "ef2"}]}

    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    with caplog.at_level(logging.WARNING):
        records, offset = connector._read_event_frames({"offset": "2026-01-01T00:00:00Z"}, {})
        rows = list(records)

    assert [r["event_frame_webid"] for r in rows] == ["EF2"]
    assert offset["offset"] != "2026-01-01T00:00:00Z"
    summary = [r.getMessage() for r in caplog.records if "pi_event_frames: skipped" in r.getMessage()]
    assert summary and "skipped 1 of 2 asset servers" in summary[0]


def test_event_frames_treats_404_as_end_of_pages(monkeypatch):
    """A 404 on an asset database breaks pagination without raising."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_read_assetservers", lambda: [{"WebId": "S1", "Name": "srv1"}])
    monkeypatch.setattr(
        connector, "_read_assetdatabases", lambda _srv: [{"WebId": "D1", "Name": "db1"}]
    )
    monkeypatch.setattr(
        connector._client,
        "get_json",
        lambda path, params=None: (_ for _ in ()).throw(_http_error(404)),
    )

    records, _offset = connector._read_event_frames({}, {})

    assert list(records) == []
