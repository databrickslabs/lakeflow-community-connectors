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
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rtok",
        })


def test_accepts_api_key_only():
    conn = YouTubeLakeflowConnect({"api_key": "key"})
    assert conn._api_key == "key"


def test_accepts_oauth_only():
    conn = YouTubeLakeflowConnect({
        "client_id": "cid",
        "client_secret": "csec",
        "refresh_token": "rtok",
    })
    assert conn._client_id == "cid"
