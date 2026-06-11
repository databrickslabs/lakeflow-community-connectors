"""Multi-page snapshot reads: all API pages drain inside one read_table call."""
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


@pytest.mark.parametrize(
    "table_name,table_options,path",
    [
        ("channels", {"channel_ids": "UCx"}, "channels"),
        ("activities", {"channel_id": "UCx"}, "activities"),
        (
            "playlist_items",
            {"playlist_id": "PL1", "max_pages": "10"},
            "playlistItems",
        ),
        (
            "videos",
            {"chart": "mostPopular", "region_code": "US", "max_pages": "10"},
            "videos",
        ),
    ],
)
def test_snapshot_drains_all_pages_in_one_call_then_tail_is_empty(
    table_name, table_options, path
):
    """One read_table drains API pages and terminates with offset=None."""
    conn = _MockConnector()
    calls: list[str | None] = []

    def fake_request(req_path, params=None):
        assert req_path == path
        token = (params or {}).get("pageToken")
        calls.append(token)
        if token is None:
            if path == "playlistItems":
                return _ok_response([_playlist_item("pi1"), _playlist_item("pi2")], "tok2")
            if path == "videos":
                return _ok_response([_video_item("vid1")], "tok2")
            return _ok_response(
                [_channel_item("page1") if path == "channels" else {"id": "act1", "snippet": {}}],
                "tok2",
            )
        if token == "tok2":
            if path == "playlistItems":
                return _ok_response([_playlist_item("pi3")], None)
            if path == "videos":
                return _ok_response([_video_item("vid2")], None)
            return _ok_response(
                [_channel_item("page2") if path == "channels" else {"id": "act2", "snippet": {}}],
                None,
            )
        raise AssertionError(f"unexpected pageToken {token!r}")

    conn._request.side_effect = fake_request

    r1, end = conn.read_table(table_name, {}, table_options)
    first = list(r1)
    assert len(first) == 3 if path == "playlistItems" else 2
    assert end is None
    assert calls == [None, "tok2"]
    assert conn._request.call_count == 2


def test_videos_video_ids_chunks_requests_in_batches_of_50():
    """video_ids path should chunk >50 IDs into multiple videos.list calls."""
    conn = _MockConnector()
    seen_ids: list[list[str]] = []

    def fake_request(req_path, params=None):
        assert req_path == "videos"
        ids = ((params or {}).get("id") or "").split(",")
        seen_ids.append(ids)
        return _ok_response([_video_item(video_id) for video_id in ids], None)

    conn._request.side_effect = fake_request
    raw_ids = ",".join(f"vid{i}" for i in range(1, 121))

    rows, end = conn.read_table("videos", {}, {"video_ids": raw_ids})
    records = list(rows)
    assert end is None
    assert len(seen_ids) == 3
    assert [len(batch) for batch in seen_ids] == [50, 50, 20]
    assert len(records) == 120
    assert conn._request.call_count == 3
