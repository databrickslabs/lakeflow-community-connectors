import gzip
import io
import json
import zipfile
from unittest.mock import Mock, patch

import pytest
from requests.exceptions import RequestException

from databricks.labs.community_connector.sources.amplitude.amplitude import (
    AmplitudeLakeflowConnect,
)
from databricks.labs.community_connector.sources.amplitude.amplitude_schemas import (
    INITIAL_BACKOFF,
    MAX_RETRIES,
)


def _connector() -> AmplitudeLakeflowConnect:
    return AmplitudeLakeflowConnect(
        {
            "api_key": "test-api-key",
            "secret_key": "test-secret-key",
        }
    )


def _mock_response(status_code: int, text: str = "", headers: dict | None = None) -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    return resp


def _make_export_zip(rows: list[dict]) -> bytes:
    gz_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode="wb") as gz:
        for row in rows:
            gz.write((json.dumps(row) + "\n").encode("utf-8"))
    gz_bytes = gz_buffer.getvalue()

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("events_0.json.gz", gz_bytes)
    return zip_buffer.getvalue()


def test_parse_export_body_reads_zip_of_gzip_ndjson():
    rows = [{"uuid": "u1"}, {"uuid": "u2"}]
    content = _make_export_zip(rows)

    parsed = AmplitudeLakeflowConnect._parse_export_body(content)

    assert parsed == rows


def test_request_with_retry_retries_network_error_then_succeeds():
    connector = _connector()
    ok = _mock_response(200)

    with (
        patch(
            "databricks.labs.community_connector.sources.amplitude.amplitude.requests.get",
            side_effect=[RequestException("temporary network issue"), ok],
        ) as mocked_get,
        patch("databricks.labs.community_connector.sources.amplitude.amplitude.time.sleep") as mocked_sleep,
    ):
        resp = connector._request_with_retry("/api/2/events/list")

    assert resp is ok
    assert mocked_get.call_count == 2
    mocked_sleep.assert_called_once_with(INITIAL_BACKOFF)


def test_request_with_retry_respects_retry_after_header():
    connector = _connector()
    throttled = _mock_response(429, headers={"Retry-After": "3"})
    ok = _mock_response(200)

    with (
        patch(
            "databricks.labs.community_connector.sources.amplitude.amplitude.requests.get",
            side_effect=[throttled, ok],
        ),
        patch("databricks.labs.community_connector.sources.amplitude.amplitude.time.sleep") as mocked_sleep,
    ):
        connector._request_with_retry("/api/2/events/list")

    mocked_sleep.assert_called_once_with(3.0)


def test_request_with_retry_raises_after_repeated_network_errors():
    connector = _connector()

    with patch(
        "databricks.labs.community_connector.sources.amplitude.amplitude.requests.get",
        side_effect=RequestException("network down"),
    ):
        with pytest.raises(RuntimeError, match="failed after"):
            connector._request_with_retry("/api/2/events/list")


def test_read_events_advances_cursor_by_window():
    connector = _connector()
    connector._init_ts = "2026-06-20T12:00:00+00:00"
    resp_404 = _mock_response(404)

    with patch.object(connector, "_request_with_retry", return_value=resp_404) as mocked_request:
        records, offset = connector._read_events(
            {"cursor": "2026-06-20T00:00:00+00:00"},
            {"window_hours": "6"},
        )

    assert list(records) == []
    assert offset == {"cursor": "2026-06-20T06:00:00+00:00"}
    mocked_request.assert_called_once_with(
        "/api/2/export",
        params={"start": "20260620T00", "end": "20260620T06"},
        stream=True,
        timeout=120,
    )


def test_read_events_rejects_non_positive_window():
    connector = _connector()
    connector._init_ts = "2026-06-20T12:00:00+00:00"

    with pytest.raises(ValueError, match="window_hours"):
        connector._read_events({}, {"window_hours": "0"})


def test_flatten_user_counts_coerces_strings_and_invalid_values():
    rows = AmplitudeLakeflowConnect._flatten_user_counts(
        {
            "data": {
                "series": [["42", "oops", True]],
                "seriesMeta": ["US"],
                "xValues": ["2026-06-01", "2026-06-02", "2026-06-03"],
            }
        }
    )

    assert rows == [
        {"date": "2026-06-01", "count": 42, "segment": "US"},
        {"date": "2026-06-02", "count": None, "segment": "US"},
        {"date": "2026-06-03", "count": 1, "segment": "US"},
    ]


def test_flatten_session_length_coerces_strings_and_invalid_values():
    rows = AmplitudeLakeflowConnect._flatten_session_length(
        {
            "data": {
                "series": [["12.5", "oops", 2]],
                "xValues": ["2026-06-01", "2026-06-02", "2026-06-03"],
            }
        }
    )

    assert rows == [
        {"date": "2026-06-01", "length": 12.5},
        {"date": "2026-06-02", "length": None},
        {"date": "2026-06-03", "length": 2.0},
    ]


def test_read_events_404_at_init_ts_cap_does_not_advance_cursor():
    """When the current window reaches the _init_ts cap, the cursor returned
    must equal init_ts, not init_ts + window.  A 404 in that last window must
    still cap the offset correctly so the connector terminates."""
    connector = _connector()
    connector._init_ts = "2026-06-20T06:00:00+00:00"
    resp_404 = _mock_response(404)

    with patch.object(connector, "_request_with_retry", return_value=resp_404):
        records, offset = connector._read_events(
            {"cursor": "2026-06-20T04:00:00+00:00"},
            {"window_hours": "6"},  # would reach 10:00, but cap is 06:00
        )

    assert list(records) == []
    # cursor must be capped at init_ts, not at 04:00 + 6h = 10:00
    assert offset == {"cursor": "2026-06-20T06:00:00+00:00"}


def test_read_events_404_then_200_returns_records_on_second_call():
    """Consecutive 404 windows followed by a 200 must return records correctly
    on the call that gets the 200, and advance the cursor past all empty windows."""
    connector = _connector()
    connector._init_ts = "2026-06-20T12:00:00+00:00"

    event = {"uuid": "e1", "event_type": "page_view"}
    resp_404 = _mock_response(404)
    resp_200 = _mock_response(200)
    resp_200.content = _make_export_zip([event])

    with patch.object(
        connector, "_request_with_retry", side_effect=[resp_404, resp_200]
    ):
        # First call — empty window
        records1, offset1 = connector._read_events(
            {"cursor": "2026-06-20T00:00:00+00:00"}, {"window_hours": "2"}
        )
        assert list(records1) == []
        assert offset1 == {"cursor": "2026-06-20T02:00:00+00:00"}

        # Second call — window has data
        records2, offset2 = connector._read_events(offset1, {"window_hours": "2"})
        assert list(records2) == [event]
        assert offset2 == {"cursor": "2026-06-20T04:00:00+00:00"}


def test_read_events_cursor_at_init_ts_returns_empty_without_api_call():
    """Once the cursor equals init_ts the connector is caught up.  It must
    signal termination immediately without making any HTTP request."""
    connector = _connector()
    connector._init_ts = "2026-06-20T06:00:00+00:00"

    with patch.object(connector, "_request_with_retry") as mocked_request:
        records, offset = connector._read_events(
            {"cursor": "2026-06-20T06:00:00+00:00"}, {}
        )

    assert list(records) == []
    assert offset == {"cursor": "2026-06-20T06:00:00+00:00"}
    mocked_request.assert_not_called()


def test_retry_count_matches_configured_max_attempts():
    connector = _connector()
    unavailable = _mock_response(503)

    with (
        patch(
            "databricks.labs.community_connector.sources.amplitude.amplitude.requests.get",
            return_value=unavailable,
        ) as mocked_get,
        patch("databricks.labs.community_connector.sources.amplitude.amplitude.time.sleep"),
    ):
        resp = connector._request_with_retry("/api/2/events/list")

    assert resp is unavailable
    assert mocked_get.call_count == MAX_RETRIES
