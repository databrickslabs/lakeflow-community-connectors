"""Connection auth option validation."""
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import YouTubeLakeflowConnect


def test_rejects_api_key_and_oauth_together():
    with pytest.raises(ValueError, match="not both"):
        YouTubeLakeflowConnect({
            "api_key": "key",
            "access_token": "tok",
        })


def test_accepts_api_key_only():
    conn = YouTubeLakeflowConnect({"api_key": "key"})
    assert conn._api_key == "key"


def test_accepts_access_token_only():
    conn = YouTubeLakeflowConnect({"access_token": "opaque-token"})
    assert conn._injected_access_token == "opaque-token"


def test_accepts_refresh_token_oauth():
    conn = YouTubeLakeflowConnect({
        "client_id": "cid",
        "client_secret": "csec",
        "refresh_token": "rtok",
    })
    assert conn._client_id == "cid"
    assert conn._refresh_token == "rtok"


def test_refresh_token_requires_client_credentials():
    with pytest.raises(ValueError, match="client_id"):
        YouTubeLakeflowConnect({"refresh_token": "rtok"})


def test_missing_auth_raises_with_community_hint():
    with pytest.raises(ValueError, match="access_token") as info:
        YouTubeLakeflowConnect({})
    assert "COMMUNITY" in str(info.value)
