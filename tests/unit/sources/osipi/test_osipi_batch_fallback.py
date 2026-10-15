"""Unit tests for the PI Web API batch-403 resilience fixes.

These cover behaviour that the simulator harness does not exercise:
- the ``X-Requested-With`` header that keeps POST /piwebapi/batch working when
  CSRF defense is enabled, and
- the graceful fallback to per-tag GET requests when a batch call is refused.
"""

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


def test_csrf_header_present_on_session():
    """Every request must advertise itself as an XHR so CSRF defense allows /batch."""
    connector = OsipiLakeflowConnect(OPTIONS)
    assert connector._client.session.headers.get("X-Requested-With") == "XMLHttpRequest"


def test_current_value_falls_back_to_individual_gets_on_batch_failure(monkeypatch):
    """When batch is refused, _read_current_value degrades to per-tag GETs."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])

    def fail_batch(_reqs):
        raise _http_error(403)

    gets = []

    def fake_get_json(path, params=None):
        gets.append(path)
        return {"Timestamp": "2026-09-23T00:00:00Z", "Value": 1.5, "Good": True}

    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)
    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    rows = connector._read_current_value({})

    assert [r["tag_webid"] for r in rows] == ["W1", "W2"]
    assert gets == [
        "/piwebapi/streams/W1/value",
        "/piwebapi/streams/W2/value",
    ]
    assert all(r["value"] == 1.5 for r in rows)


def test_value_at_time_falls_back_to_individual_gets_on_batch_failure(monkeypatch):
    """Same graceful fallback for _read_value_at_time."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1"])
    monkeypatch.setattr(
        connector._client, "batch_execute", lambda _reqs: (_ for _ in ()).throw(_http_error(403))
    )
    monkeypatch.setattr(
        connector._client,
        "get_json",
        lambda path, params=None: {"Timestamp": "2026-09-23T00:00:00Z", "Value": 2.0},
    )

    rows = connector._read_value_at_time({})

    assert len(rows) == 1
    assert rows[0]["tag_webid"] == "W1"
    assert rows[0]["value"] == 2.0


def test_individual_get_fallback_skips_404_tags(monkeypatch):
    """A 404 for one tag is skipped, not fatal, during the per-tag fallback."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "MISSING", "W3"])
    monkeypatch.setattr(
        connector._client, "batch_execute", lambda _reqs: (_ for _ in ()).throw(_http_error(403))
    )

    def fake_get_json(path, params=None):
        if "MISSING" in path:
            raise _http_error(404)
        return {"Timestamp": "2026-09-23T00:00:00Z", "Value": 3.0}

    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    rows = connector._read_current_value({})

    assert [r["tag_webid"] for r in rows] == ["W1", "W3"]


def test_current_value_uses_batch_when_it_succeeds(monkeypatch):
    """The fast path stays batch when the server accepts it (no per-tag GETs)."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])

    def ok_batch(_reqs):
        return [
            ("0", {"Status": 200, "Content": {"Timestamp": "2026-09-23T00:00:00Z", "Value": 9.0}}),
            ("1", {"Status": 200, "Content": {"Timestamp": "2026-09-23T00:00:00Z", "Value": 8.0}}),
        ]

    def should_not_call(*_args, **_kwargs):
        raise AssertionError("get_json should not be called when batch succeeds")

    monkeypatch.setattr(connector._client, "batch_execute", ok_batch)
    monkeypatch.setattr(connector._client, "get_json", should_not_call)

    rows = connector._read_current_value({})

    assert [r["value"] for r in rows] == [9.0, 8.0]
