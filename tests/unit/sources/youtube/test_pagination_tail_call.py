"""Snapshot reads should terminate with offset=None in one call."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import YouTubeLakeflowConnect

_OPTIONS = {"client_id": "x", "client_secret": "y", "refresh_token": "z"}


def _channel_item(channel_id: str = "UCtest") -> dict:
    return {
        "id": channel_id,
        "snippet": {"title": "T", "description": "D", "publishedAt": "2024-01-01T00:00:00Z"},
        "statistics": {"viewCount": "1"},
        "contentDetails": {},
    }


def _api_response(items: list, next_token: str | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.ok = True
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"items": items, "nextPageToken": next_token}
    return resp


@pytest.fixture
def mock_connector():
    class MockConnector(YouTubeLakeflowConnect):
        def __init__(self):
            super().__init__(_OPTIONS)
            self._request = MagicMock()  # type: ignore[method-assign]

    conn = MockConnector()
    conn._request.return_value = _api_response([_channel_item()], next_token=None)
    return conn


@pytest.mark.parametrize(
    "table_name,table_options",
    [
        ("channels", {"channel_ids": "UCtest"}),
        ("playlists", {"channel_id": "UCtest"}),
        ("playlist_items", {"playlist_id": "PLtest"}),
        ("videos", {"video_ids": "vid123"}),
        ("search", {"q": "test query"}),
        ("activities", {"channel_id": "UCtest"}),
        ("subscriptions", {"channel_id": "UCtest"}),
        ("video_categories", {"region_code": "US"}),
        ("comment_threads", {"video_id": "vid123"}),
    ],
)
def test_snapshot_read_returns_none_offset(mock_connector, table_name, table_options):
    """Snapshot reads return None offset so framework terminates immediately."""
    records_iter, end_offset = mock_connector.read_table(table_name, {}, table_options)
    first = list(records_iter)
    assert len(first) > 0
    assert end_offset is None
    assert mock_connector._request.call_count == 1
