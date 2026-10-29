"""Unit tests for the PI Web API batch-403 resilience fixes.

These cover behaviour that the simulator harness does not exercise:
- the ``X-Requested-With`` header that keeps POST /piwebapi/batch working when
  CSRF defense is enabled,
- the graceful fallback to per-tag GET requests when a batch call is refused
  (403/405 only; other errors raise), remembered across groups, and
- the zero-width window guard in ``_read_interpolated``.
"""

import logging

import pytest
import requests

from databricks.labs.community_connector.sources.osipi.osipi import OsipiLakeflowConnect
from databricks.labs.community_connector.sources.osipi.osipi_utils import parse_ts

OPTIONS = {
    "pi_base_url": "https://simulator-pi.example.com",
    "access_token": "fake-token",
}


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    resp = requests.Response()
    resp.status_code = status_code
    return requests.exceptions.HTTPError("boom", response=resp)


def _refuse_batch(status_code: int = 403):
    calls = []

    def fail_batch(reqs):
        calls.append(reqs)
        raise _http_error(status_code)

    return fail_batch, calls


def _should_not_call(*_args, **_kwargs):
    raise AssertionError("PI Web API should not be called")


def test_csrf_header_present_on_session():
    """Every request must advertise itself as an XHR so CSRF defense allows /batch."""
    connector = OsipiLakeflowConnect(OPTIONS)
    assert connector._client.session.headers.get("X-Requested-With") == "XMLHttpRequest"


@pytest.mark.parametrize("status", [403, 405])
def test_current_value_falls_back_to_individual_gets_on_batch_refusal(monkeypatch, status):
    """When batch is refused, _read_current_value degrades to per-tag GETs."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])
    fail_batch, _calls = _refuse_batch(status)

    gets = []

    def fake_get_json(path, params=None):
        gets.append((path, params))
        return {"Timestamp": "2026-09-23T00:00:00Z", "Value": 1.5, "Good": True}

    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)
    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    rows = connector._read_current_value({})

    assert [r["tag_webid"] for r in rows] == ["W1", "W2"]
    assert gets == [
        ("/piwebapi/streams/W1/value", {}),
        ("/piwebapi/streams/W2/value", {}),
    ]
    assert all(r["value"] == 1.5 for r in rows)


def test_value_at_time_fallback_forwards_time_param(monkeypatch):
    """The per-tag fallback must send the same ``time`` param the batch would have."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1"])
    fail_batch, calls = _refuse_batch()
    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)

    gets = []

    def fake_get_json(path, params=None):
        gets.append((path, params))
        return {"Timestamp": "2026-01-01T00:00:00Z", "Value": 2.0}

    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    rows = connector._read_value_at_time({"time": "2026-01-01T00:00:00Z"})

    assert calls[0][0]["Parameters"] == {"time": "2026-01-01T00:00:00Z"}
    assert gets == [("/piwebapi/streams/W1/value", {"time": "2026-01-01T00:00:00Z"})]
    assert len(rows) == 1
    assert rows[0]["tag_webid"] == "W1"
    assert rows[0]["value"] == 2.0


def test_individual_get_fallback_skips_404_tags(monkeypatch):
    """A 404 for one tag is skipped, not fatal, during the per-tag fallback."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "MISSING", "W3"])
    fail_batch, _calls = _refuse_batch()
    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)

    def fake_get_json(path, params=None):
        if "MISSING" in path:
            raise _http_error(404)
        return {"Timestamp": "2026-09-23T00:00:00Z", "Value": 3.0}

    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    rows = connector._read_current_value({})

    assert [r["tag_webid"] for r in rows] == ["W1", "W3"]


@pytest.mark.parametrize("status", [401, 429, 500, 503])
def test_batch_errors_other_than_refusal_raise(monkeypatch, status):
    """429/5xx/401 must not fan out to one GET per tag; they fail the read."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])
    fail_batch, _calls = _refuse_batch(status)
    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)
    monkeypatch.setattr(connector._client, "get_json", _should_not_call)

    with pytest.raises(requests.exceptions.HTTPError):
        connector._read_current_value({})
    assert connector._client.batch_refused is False


def test_batch_refusal_is_remembered_and_logged_once(monkeypatch, caplog):
    """After one refusal, later groups (and reads) skip the batch POST entirely."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2", "W3"])
    fail_batch, calls = _refuse_batch()
    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)
    monkeypatch.setattr(
        connector._client,
        "get_json",
        lambda path, params=None: {"Timestamp": "2026-09-23T00:00:00Z", "Value": 1.0},
    )

    with caplog.at_level(logging.WARNING):
        rows = connector._read_current_value({"tags_per_request": "1"})
        rows += connector._read_value_at_time({"tags_per_request": "1"})

    assert len(rows) == 6
    assert len(calls) == 1
    assert connector._client.batch_refused is True
    refusals = [r for r in caplog.records if "refused POST /piwebapi/batch" in r.getMessage()]
    assert len(refusals) == 1
    assert "403" in refusals[0].getMessage()


def test_current_value_uses_batch_when_it_succeeds(monkeypatch):
    """The fast path stays batch when the server accepts it (no per-tag GETs)."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])

    def ok_batch(_reqs):
        return [
            ("0", {"Status": 200, "Content": {"Timestamp": "2026-09-23T00:00:00Z", "Value": 9.0}}),
            ("1", {"Status": 200, "Content": {"Timestamp": "2026-09-23T00:00:00Z", "Value": 8.0}}),
        ]

    monkeypatch.setattr(connector._client, "batch_execute", ok_batch)
    monkeypatch.setattr(connector._client, "get_json", _should_not_call)

    rows = connector._read_current_value({})

    assert [r["value"] for r in rows] == [9.0, 8.0]


def test_interpolated_falls_back_to_individual_gets_on_batch_refusal(monkeypatch):
    """Interpolated per-tag path: batch 403 -> per-tag GETs with the window params."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])
    fail_batch, _calls = _refuse_batch()
    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)

    gets = []

    def fake_get_json(path, params=None):
        if path == "/piwebapi/streamsets/interpolated":
            # Server without streamsets: fall through to the per-tag path.
            raise _http_error(404)
        gets.append((path, params))
        if "W2" in path:
            raise _http_error(404)
        return {
            "Items": [
                {"Timestamp": "2026-01-01T00:00:00Z", "Value": 1.0},
                {"Timestamp": "2026-01-01T00:10:00Z", "Value": 2.0},
                {"Value": 99.0},  # no Timestamp: dropped
            ]
        }

    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    records, offset = connector._read_interpolated(
        {},
        {
            "startTime": "2026-01-01T00:00:00Z",
            "endTime": "2026-01-01T01:00:00Z",
            "interval": "10m",
        },
    )
    rows = list(records)

    expected_params = {
        "startTime": "2026-01-01T00:00:00Z",
        "endTime": "2026-01-01T01:00:00Z",
        "interval": "10m",
        "maxCount": "1000",
    }
    assert gets == [
        ("/piwebapi/streams/W1/interpolated", expected_params),
        ("/piwebapi/streams/W2/interpolated", expected_params),
    ]
    assert [(r["tag_webid"], r["value"]) for r in rows] == [("W1", 1.0), ("W1", 2.0)]
    assert offset == {"offset": "2026-01-01T01:00:00Z"}


def test_timeseries_batch_path_falls_back_on_refusal(monkeypatch):
    """Recorded reads on the batch path also degrade to per-tag GETs."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])
    fail_batch, _calls = _refuse_batch()
    monkeypatch.setattr(connector._client, "batch_execute", fail_batch)

    gets = []

    def fake_get_json(path, params=None):
        gets.append(path)
        return {"Items": [{"Timestamp": "2026-01-01T00:00:00Z", "Value": 5.0}]}

    monkeypatch.setattr(connector._client, "get_json", fake_get_json)

    records, _offset = connector._read_timeseries(
        {},
        {
            "prefer_streamset": "false",
            "startTime": "2026-01-01T00:00:00Z",
            "endTime": "2026-01-01T01:00:00Z",
        },
    )
    rows = list(records)

    assert gets == ["/piwebapi/streams/W1/recorded", "/piwebapi/streams/W2/recorded"]
    assert [r["tag_webid"] for r in rows] == ["W1", "W2"]


def test_interpolated_zero_width_window_holds_offset_without_calling_server(monkeypatch):
    """Caught up (start == init_time): empty rows, same offset, no HTTP."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1", "W2"])
    monkeypatch.setattr(connector._client, "get_json", _should_not_call)
    monkeypatch.setattr(connector._client, "batch_execute", _should_not_call)

    start = "2026-01-01T00:00:00Z"
    connector._init_time = parse_ts(start)
    start_offset = {"offset": start}

    records, offset = connector._read_interpolated(start_offset, {})

    assert list(records) == []
    assert offset == start_offset


def test_interpolated_inverted_window_holds_offset_without_calling_server(monkeypatch):
    """start after end (endTime option earlier than the offset): same treatment."""
    connector = OsipiLakeflowConnect(OPTIONS)
    monkeypatch.setattr(connector, "_resolve_tag_webids", lambda opts: ["W1"])
    monkeypatch.setattr(connector._client, "get_json", _should_not_call)
    monkeypatch.setattr(connector._client, "batch_execute", _should_not_call)

    start_offset = {"offset": "2026-06-01T00:00:00Z"}

    records, offset = connector._read_interpolated(
        start_offset, {"endTime": "2026-01-01T00:00:00Z"}
    )

    assert list(records) == []
    assert offset == start_offset
