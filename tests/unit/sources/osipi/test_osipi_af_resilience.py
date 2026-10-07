"""Unit tests for Asset Framework / Event Frame read resilience.

These cover defensive handling the simulator harness does not exercise:
- ``_read_assetdatabases`` skips an asset server the credential cannot see
  (401/403) instead of failing the whole read, and
- ``_read_event_frames`` skips an asset database that returns a server error
  (5xx), and treats 404 as end-of-pages, instead of aborting ingestion.
"""

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


def test_event_frames_skips_db_on_5xx_and_continues(monkeypatch):
    """A 5xx on one asset database is skipped; other databases still ingest."""
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

    records, _offset = connector._read_event_frames({}, {})
    rows = list(records)

    assert [r["event_frame_webid"] for r in rows] == ["EF2"]


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
