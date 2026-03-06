"""
Local test: ensure search connector never returns duplicate (search_query, result_index).
Mocks the API so it runs without network/quota. Simulates single read and pipeline-style
double call (with offset) to verify no duplicates.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import YouTubeLakeflowConnect


def _make_search_item(idx: int, video_id: str = None) -> dict:
    vid = video_id or f"video_{idx}"
    return {
        "kind": "youtube#searchResult",
        "id": {"kind": "youtube#video", "videoId": vid},
        "snippet": {
            "publishedAt": "2024-01-01T00:00:00Z",
            "channelId": "UCtest",
            "title": f"Title {idx}",
            "description": f"Desc {idx}",
            "channelTitle": "Channel",
        },
    }


def _make_response(items: list, next_token: str = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"items": items, "nextPageToken": next_token}
    return resp


@pytest.fixture
def mock_connector():
    """Connector with mocked _request returning fake search pages (no network)."""
    # Need minimal init: pass options that skip real auth for _request mock
    options = {"client_id": "x", "client_secret": "y", "refresh_token": "z"}

    class MockSearchConnector(YouTubeLakeflowConnect):
        def __init__(self):
            super().__init__(options)

        def _request(self, method: str, path: str, params: dict = None):
            if path != "search" or (params or {}).get("q") != "databricks":
                return super()._request(method, path, params)
            page_token = (params or {}).get("pageToken")
            if page_token is None:
                # Page 1: 50 items
                items = [_make_search_item(i) for i in range(50)]
                return _make_response(items, next_token="token2")
            if page_token == "token2":
                # Page 2: 50 items
                items = [_make_search_item(i) for i in range(50, 100)]
                return _make_response(items, next_token="token3")
            if page_token == "token3":
                # Page 3: 25 items, no more pages
                items = [_make_search_item(i) for i in range(100, 125)]
                return _make_response(items, next_token=None)
            return _make_response([], None)

    return MockSearchConnector()


def test_search_single_call_no_duplicate_keys(mock_connector):
    """One read_table('search') must not contain duplicate (search_query, result_index)."""
    table_options = {"q": "databricks", "type": "video", "max_pages": "10"}
    records_iter, offset = mock_connector.read_table("search", {}, table_options)
    records = list(records_iter)
    keys = [(r.get("search_query"), r.get("result_index")) for r in records]
    assert len(records) == 125, f"Expected 125 records (50+50+25), got {len(records)}"
    unique_keys = set(keys)
    assert len(unique_keys) == len(keys), (
        f"Duplicate (search_query, result_index) in single response: "
        f"total={len(keys)} unique={len(unique_keys)}. "
        f"Example duplicates: {[k for k in keys if keys.count(k) > 1][:5]}"
    )


def test_search_second_call_returns_empty_so_no_duplicates_across_calls(mock_connector):
    """Pipeline: first call returns data, second call with returned offset must return 0 records."""
    table_options = {"q": "databricks", "type": "video", "max_pages": "10"}
    all_keys = []
    start_offset = {}
    for _ in range(3):  # At most 3 calls; second and third should return empty
        records_iter, next_offset = mock_connector.read_table("search", start_offset, table_options)
        records = list(records_iter)
        for r in records:
            all_keys.append((r.get("search_query"), r.get("result_index")))
        if not records:
            break
        start_offset = next_offset
    unique_keys = set(all_keys)
    assert len(unique_keys) == len(all_keys), (
        f"Duplicate keys across pipeline calls: total={len(all_keys)} unique={len(unique_keys)}. "
        f"Duplicates: {[k for k in all_keys if all_keys.count(k) > 1][:10]}"
    )
    assert len(records) == 0, "Second call should return 0 records when offset has pageToken=None"
    assert len(all_keys) == 125, "Only first call should have returned 125 records"
