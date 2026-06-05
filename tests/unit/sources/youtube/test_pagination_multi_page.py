"""
Multi-page pagination: page-per-call token chaining and drain-all tail calls.

Complements test_pagination_tail_call (single-page tail) and the harness
test_read_terminates (simulator uses pagination_style: none).
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import YouTubeLakeflowConnect

_OPTIONS = {"api_key": "test-key"}


def _ok_response(items: list, next_token: str | None = None) -> MagicMock:
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.json.return_value = {"items": items, "nextPageToken": next_token}
    return resp


def _channel_item(channel_id: str) -> dict:
    return {
        "id": channel_id,
        "snippet": {"title": channel_id},
        "statistics": {},
        "contentDetails": {},
    }


def _playlist_item(item_id: str) -> dict:
    return {
        "id": item_id,
        "snippet": {"title": item_id, "playlistId": "PL1"},
        "contentDetails": {"videoId": f"vid_{item_id}"},
    }


def _video_item(video_id: str) -> dict:
    return {
        "id": video_id,
        "snippet": {"title": video_id},
        "statistics": {},
        "contentDetails": {},
    }


class _MockConnector(YouTubeLakeflowConnect):
    def __init__(self):
        super().__init__(_OPTIONS)
        self._request = MagicMock()  # type: ignore[method-assign]


def test_channels_multi_page_chains_tokens_then_tail_is_empty():
    """Framework: page 1 → page 2 → tail; two API calls, no duplicate page 1."""
    conn = _MockConnector()

    def fake_request(path, params=None):
        assert path == "channels"
        token = (params or {}).get("pageToken")
        if token is None:
            return _ok_response([_channel_item("UC_page1")], "tok2")
        if token == "tok2":
            return _ok_response([_channel_item("UC_page2")], None)
        raise AssertionError(f"unexpected pageToken {token!r}")

    conn._request.side_effect = fake_request

    r1, o1 = conn.read_table("channels", {}, {"channel_ids": "UCx"})
    page1 = list(r1)
    assert [r["id"] for r in page1] == ["UC_page1"]
    assert o1 == {"pageToken": "tok2"}

    r2, o2 = conn.read_table("channels", o1, {"channel_ids": "UCx"})
    page2 = list(r2)
    assert [r["id"] for r in page2] == ["UC_page2"]
    assert o2 == {"pageToken": None}

    r3, o3 = conn.read_table("channels", o2, {"channel_ids": "UCx"})
    assert list(r3) == []
    assert o3 == {"pageToken": None}
    assert conn._request.call_count == 2


def test_activities_multi_page_chains_tokens_then_tail_is_empty():
    conn = _MockConnector()

    def fake_request(path, params=None):
        assert path == "activities"
        token = (params or {}).get("pageToken")
        if token is None:
            return _ok_response([{"id": "act1", "snippet": {}}], "tokB")
        if token == "tokB":
            return _ok_response([{"id": "act2", "snippet": {}}], None)
        raise AssertionError(f"unexpected pageToken {token!r}")

    conn._request.side_effect = fake_request

    _, o1 = conn.read_table("activities", {}, {"channel_id": "UCx"})
    _, o2 = conn.read_table("activities", o1, {"channel_id": "UCx"})
    tail, o3 = conn.read_table("activities", o2, {"channel_id": "UCx"})

    assert list(tail) == []
    assert o3 == {"pageToken": None}
    assert conn._request.call_count == 2


@pytest.mark.parametrize(
    "table_name,table_options,path,setup",
    [
        (
            "playlist_items",
            {"playlist_id": "PL1", "max_pages": "10"},
            "playlistItems",
            "playlist",
        ),
        (
            "videos",
            {"chart": "mostPopular", "region_code": "US", "max_pages": "10"},
            "videos",
            "videos",
        ),
    ],
)
def test_drain_all_multi_page_then_tail_is_empty(table_name, table_options, path, setup):
    """Drain-all tables: one read fetches all pages; tail call adds no rows or requests."""
    conn = _MockConnector()
    calls: list[str | None] = []

    def fake_request(req_path, params=None):
        assert req_path == path
        token = (params or {}).get("pageToken")
        calls.append(token)
        if token is None:
            if setup == "playlist":
                return _ok_response([_playlist_item("pi1"), _playlist_item("pi2")], "tok2")
            return _ok_response([_video_item("vid1")], "tok2")
        if token == "tok2":
            if setup == "playlist":
                return _ok_response([_playlist_item("pi3")], None)
            return _ok_response([_video_item("vid2")], None)
        raise AssertionError(f"unexpected pageToken {token!r}")

    conn._request.side_effect = fake_request

    r1, end = conn.read_table(table_name, {}, table_options)
    first = list(r1)
    assert len(first) == 3 if setup == "playlist" else 2
    assert end == {"pageToken": None}
    assert calls == [None, "tok2"]

    r2, end2 = conn.read_table(table_name, end, table_options)
    assert list(r2) == []
    assert end2 == {"pageToken": None}
    assert conn._request.call_count == 2
