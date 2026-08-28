"""YouTube API error messages surfaced as ValueError."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import (
    YouTubeLakeflowConnect,
    _raise_for_youtube_api_error,
)

_OPTIONS = {"api_key": "test-key"}


def _error_response(status: int, reason: str, message: str) -> MagicMock:
    resp = MagicMock()
    resp.ok = status < 400
    resp.status_code = status
    resp.reason = "Error"
    resp.text = message
    resp.json.return_value = {
        "error": {
            "code": status,
            "message": message,
            "errors": [{"reason": reason, "message": message}],
        }
    }
    return resp


def test_raise_quota_exceeded_403():
    resp = _error_response(403, "quotaExceeded", "The request cannot be completed because you have exceeded your quota.")
    with pytest.raises(ValueError, match="quota exceeded"):
        _raise_for_youtube_api_error(resp, "search")


def test_raise_401_with_oauth_hint():
    resp = _error_response(401, "authError", "Invalid Credentials")
    with pytest.raises(ValueError, match="401 Unauthorized"):
        _raise_for_youtube_api_error(resp, "channels")


def test_raise_429_after_retries():
    resp = _error_response(429, "rateLimitExceeded", "Rate Limit Exceeded")
    with pytest.raises(ValueError, match="429 Too Many Requests"):
        _raise_for_youtube_api_error(resp, "channels")


def test_comment_threads_403_includes_detail():
    resp = _error_response(403, "commentsDisabled", "Comments disabled")
    with pytest.raises(ValueError, match="video_id=vid123"):
        _raise_for_youtube_api_error(resp, "commentThreads", detail="video_id=vid123")


def test_read_channels_surfaces_quota_error():
    conn = YouTubeLakeflowConnect(_OPTIONS)
    bad = _error_response(403, "quotaExceeded", "quota exceeded")

    with patch.object(conn, "_request", return_value=bad):
        with pytest.raises(ValueError, match="quota exceeded"):
            list(conn.read_table("channels", {}, {"channel_ids": "UCx"})[0])
