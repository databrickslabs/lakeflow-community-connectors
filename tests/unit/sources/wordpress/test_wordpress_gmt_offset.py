"""Unit tests for the WordPress connector's gmt_offset handling.

WordPress compares its ``after`` / ``before`` / ``modified_*`` date filters
against the site-local columns, while the connector tracks its cursor from the
UTC ``_gmt`` fields. These tests cover the offset fetch, the UTC->local bound
conversion, and that ``read_partition`` shifts the query bounds accordingly.
Offline: no simulator, no network.
"""

import pytest
import requests

from databricks.labs.community_connector.sources.wordpress import wordpress as wp_module
from databricks.labs.community_connector.sources.wordpress.wordpress import (
    WordPressLakeflowConnect,
)
from databricks.labs.community_connector.sources.wordpress.wordpress_utils import (
    fetch_gmt_offset_seconds,
    to_local_query_bound,
)

CONFIG = {
    "base_url": "https://example.com",
    "username": "user",
    "application_password": "abcd efgh ijkl mnop",
}


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, raise_json=False):
        self.status_code = status_code
        self._payload = payload
        self._raise_json = raise_json

    def json(self):
        if self._raise_json:
            raise ValueError("no json body")
        return self._payload


class _FakeSession:
    def __init__(self, response=None, exc=None):
        self._response = response
        self._exc = exc
        self.calls = []

    def get(self, url, timeout=None, params=None):
        self.calls.append(url)
        if self._exc is not None:
            raise self._exc
        return self._response


# --------------------------------------------------------------------------- #
# to_local_query_bound
# --------------------------------------------------------------------------- #


def test_to_local_query_bound_positive_offset_shifts_forward_and_drops_z():
    # Arrange / Act
    result = to_local_query_bound("2026-06-01T12:00:00Z", 5 * 3600)
    # Assert
    assert result == "2026-06-01T17:00:00"


def test_to_local_query_bound_negative_offset_shifts_back():
    assert to_local_query_bound("2026-06-01T12:00:00Z", -8 * 3600) == "2026-06-01T04:00:00"


def test_to_local_query_bound_fractional_offset():
    assert to_local_query_bound("2026-06-01T12:00:00Z", int(5.5 * 3600)) == "2026-06-01T17:30:00"


def test_to_local_query_bound_rejects_invalid_timestamp():
    with pytest.raises(ValueError):
        to_local_query_bound("not-a-timestamp", 0)


# --------------------------------------------------------------------------- #
# fetch_gmt_offset_seconds
# --------------------------------------------------------------------------- #


def test_fetch_gmt_offset_seconds_reads_fractional_hours():
    session = _FakeSession(_FakeResponse(200, {"gmt_offset": 5.5}))
    assert fetch_gmt_offset_seconds(session, "https://example.com/wp-json/") == int(
        round(5.5 * 3600)
    )


def test_fetch_gmt_offset_seconds_zero_when_field_missing():
    session = _FakeSession(_FakeResponse(200, {"name": "site without offset"}))
    assert fetch_gmt_offset_seconds(session, "https://example.com/wp-json/") == 0


def test_fetch_gmt_offset_seconds_zero_on_non_200():
    # 404 is non-retriable, so request_with_retry returns it immediately.
    session = _FakeSession(_FakeResponse(404, None))
    assert fetch_gmt_offset_seconds(session, "https://example.com/wp-json/") == 0


def test_fetch_gmt_offset_seconds_zero_on_request_exception():
    session = _FakeSession(exc=requests.ConnectionError("boom"))
    assert fetch_gmt_offset_seconds(session, "https://example.com/wp-json/") == 0


def test_fetch_gmt_offset_seconds_zero_on_unparseable_body():
    session = _FakeSession(_FakeResponse(200, None, raise_json=True))
    assert fetch_gmt_offset_seconds(session, "https://example.com/wp-json/") == 0


# --------------------------------------------------------------------------- #
# connector wiring
# --------------------------------------------------------------------------- #


def test_connector_fetches_offset_at_init(monkeypatch):
    captured = {}

    def fake_fetch(session, url, timeout=30):
        captured["url"] = url
        return 3 * 3600

    monkeypatch.setattr(wp_module, "fetch_gmt_offset_seconds", fake_fetch)
    conn = WordPressLakeflowConnect(CONFIG)

    assert conn._gmt_offset_seconds == 3 * 3600
    assert captured["url"] == "https://example.com/wp-json/"


def test_read_partition_shifts_bounds_when_offset_nonzero(monkeypatch):
    monkeypatch.setattr(wp_module, "fetch_gmt_offset_seconds", lambda *a, **k: 3 * 3600)
    captured = {}

    def fake_paginate(session, url, params, per_page=None):
        captured["params"] = params
        return iter(())

    monkeypatch.setattr(wp_module, "paginate", fake_paginate)
    conn = WordPressLakeflowConnect(CONFIG)

    partition = {"since": "2026-06-01T00:00:00Z", "until": "2026-06-02T00:00:00Z"}
    list(conn.read_partition("posts", partition, {}))

    # +3h and no ``Z`` (site-local, timezone-naive) on both bounds; the upper
    # bound is until + 1s before the shift.
    assert captured["params"]["modified_after"] == "2026-06-01T03:00:00"
    assert captured["params"]["modified_before"] == "2026-06-02T03:00:01"


def test_read_partition_keeps_utc_bounds_when_offset_zero(monkeypatch):
    monkeypatch.setattr(wp_module, "fetch_gmt_offset_seconds", lambda *a, **k: 0)
    captured = {}

    def fake_paginate(session, url, params, per_page=None):
        captured["params"] = params
        return iter(())

    monkeypatch.setattr(wp_module, "paginate", fake_paginate)
    conn = WordPressLakeflowConnect(CONFIG)

    partition = {"since": "2026-06-01T00:00:00Z", "until": "2026-06-02T00:00:00Z"}
    list(conn.read_partition("comments", partition, {}))

    # UTC site: exact ``...Z`` bounds preserved (unchanged from prior behavior).
    assert captured["params"]["after"] == "2026-06-01T00:00:00Z"
    assert captured["params"]["before"] == "2026-06-02T00:00:01Z"


# --------------------------------------------------------------------------- #
# lookback_seconds gating (only cdc tables, never append comments)
# --------------------------------------------------------------------------- #


def _connector_offset_zero(monkeypatch):
    monkeypatch.setattr(wp_module, "fetch_gmt_offset_seconds", lambda *a, **k: 0)
    return WordPressLakeflowConnect(CONFIG)


def test_lookback_applied_to_cdc_posts(monkeypatch):
    conn = _connector_offset_zero(monkeypatch)
    parts = conn.get_partitions(
        "posts",
        {"lookback_seconds": "3600", "num_partitions": "1"},
        start_offset={"cursor": "2026-06-01T12:00:00Z"},
        end_offset={"cursor": "2026-06-02T12:00:00Z"},
    )
    # cdc + merge on PK => lookback widens the lower bound by 1h.
    assert parts[0]["since"] == "2026-06-01T11:00:00Z"


def test_lookback_not_applied_to_append_comments(monkeypatch):
    conn = _connector_offset_zero(monkeypatch)
    parts = conn.get_partitions(
        "comments",
        {"lookback_seconds": "3600", "num_partitions": "1"},
        start_offset={"cursor": "2026-06-01T12:00:00Z"},
        end_offset={"cursor": "2026-06-02T12:00:00Z"},
    )
    # append + no merge => lookback would duplicate rows, so it is skipped.
    assert parts[0]["since"] == "2026-06-01T12:00:00Z"


# --------------------------------------------------------------------------- #
# fail-fast validation of table options
# --------------------------------------------------------------------------- #


def test_resolve_start_rejects_invalid_timestamp(monkeypatch):
    conn = _connector_offset_zero(monkeypatch)
    with pytest.raises(ValueError):
        conn.get_partitions("posts", {"start_timestamp": "2026-13-01"})


def test_int_option_rejects_malformed_value(monkeypatch):
    conn = _connector_offset_zero(monkeypatch)
    with pytest.raises(ValueError):
        conn.get_partitions("posts", {"num_partitions": "eight"})
