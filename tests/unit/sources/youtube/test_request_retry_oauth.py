"""Retry-After and OAuth token error handling."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import (
    YouTubeLakeflowConnect,
    _retry_wait_seconds,
)

_OPTIONS = {"client_id": "x", "client_secret": "y", "refresh_token": "z"}


def test_retry_wait_seconds_honors_retry_after():
    resp = MagicMock()
    resp.headers = {"Retry-After": "42"}
    assert _retry_wait_seconds(resp, 1.0) == 42.0


def test_retry_wait_seconds_falls_back_to_backoff():
    resp = MagicMock()
    resp.headers = {}
    assert _retry_wait_seconds(resp, 8.0) == 8.0


def test_get_access_token_invalid_grant_raises_value_error():
    conn = YouTubeLakeflowConnect(_OPTIONS)
    bad = MagicMock()
    bad.status_code = 400
    bad.json.return_value = {"error": "invalid_grant", "error_description": "Bad Request"}
    with patch.object(conn._session, "post", return_value=bad):
        with pytest.raises(ValueError, match="invalid_grant"):
            conn._get_access_token()


def test_request_sleeps_retry_after_on_429():
    conn = YouTubeLakeflowConnect(_OPTIONS)
    ok = MagicMock(status_code=200, headers={})
    rate_limited = MagicMock(status_code=429, headers={"Retry-After": "3"})

    with patch.object(conn, "_get_access_token", return_value="token"):
        with patch.object(conn._session, "get", side_effect=[rate_limited, ok]):
            with patch("databricks.labs.community_connector.sources.youtube.youtube.time.sleep") as sleep:
                resp = conn._request("channels", {"id": "UCx"})
    assert resp.status_code == 200
    sleep.assert_called_once_with(3.0)
