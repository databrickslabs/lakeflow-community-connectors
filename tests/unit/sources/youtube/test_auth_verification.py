"""
Auth verification test for youtube connector.

Verifies that stored credentials (client_id, client_secret, refresh_token) can be used to:
1. Obtain an access token via Google OAuth2 token endpoint.
2. Optionally call YouTube Data API v3 (channels?part=id&mine=true) to confirm the token works.
   If the API returns 403 (e.g. YouTube API not enabled on the project), the test is skipped.

No connector implementation is required; this test validates token exchange only.
Run with: pytest tests/unit/sources/youtube/test_auth_verification.py -v
"""

import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

# Ensure project root is on path so "tests" is importable when run from repo root or CI.
_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.unit.sources.test_utils import load_config


# Google OAuth2 token endpoint (from connector_spec.yaml)
TOKEN_URL = "https://oauth2.googleapis.com/token"
# Minimal YouTube Data API v3 check: channels for the authenticated user
YOUTUBE_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels?part=id&mine=true"


def _load_dev_config():
    """Load dev config from the standard path."""
    config_dir = Path(__file__).parent / "configs"
    config_path = config_dir / "dev_config.json"
    if not config_path.exists():
        return None
    return load_config(config_path)


def _is_placeholder(value: str) -> bool:
    """Return True if the value looks like a placeholder or is empty."""
    if not value or not isinstance(value, str):
        return True
    placeholders = ["YOUR_", "REPLACE_", "dummy", "xxx", "***"]
    return any(p in value for p in placeholders)


def _exchange_refresh_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """Exchange refresh_token for access_token via Google OAuth2 token endpoint."""
    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }).encode()
    req = urllib.request.Request(TOKEN_URL, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _call_youtube_channels_or_skip(access_token: str) -> dict:
    """Call YouTube Data API v3 channels; skip with message if 403 (API not enabled)."""
    try:
        req = urllib.request.Request(YOUTUBE_CHANNELS_URL, method="GET")
        req.add_header("Authorization", f"Bearer {access_token}")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 403:
            pytest.skip(
                "YouTube Data API v3 returned 403. Ensure the OAuth consent includes "
                "https://www.googleapis.com/auth/youtube.readonly and the Google Cloud "
                "project has the YouTube Data API v3 enabled. CI can pass without it."
            )
        raise


class TestYoutubeAuthVerification:
    """Auth verification tests for youtube (token exchange + optional API check)."""

    @pytest.fixture(autouse=True)
    def _require_credentials(self):
        """Skip entire class if dev_config is missing or credentials are placeholders."""
        config = _load_dev_config()
        if config is None:
            pytest.skip(
                "No dev_config.json found at tests/unit/sources/youtube/configs/dev_config.json"
            )
        for key in ["client_id", "client_secret", "refresh_token"]:
            val = config.get(key, "")
            if _is_placeholder(str(val)):
                pytest.skip(
                    f"Missing or placeholder '{key}' in dev_config.json. "
                    "Add real credentials (and run authenticate script for refresh_token) to run auth verification."
                )
        self._config = config

    def test_token_exchange(self):
        """Verify refresh_token can be exchanged for an access_token."""
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]
        refresh_token = self._config["refresh_token"]

        token_data = _exchange_refresh_token(client_id, client_secret, refresh_token)

        assert "access_token" in token_data, (
            f"Token response missing access_token: {token_data.get('error', token_data)}"
        )
        assert token_data.get("token_type", "").lower() == "bearer"

    def test_access_token_works_with_youtube_api(self):
        """Verify the obtained access token works with a minimal YouTube Data API v3 call."""
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]
        refresh_token = self._config["refresh_token"]

        token_data = _exchange_refresh_token(client_id, client_secret, refresh_token)
        access_token = token_data["access_token"]

        data = _call_youtube_channels_or_skip(access_token)
        # Response has "items" (list of channels) or empty if no channel
        assert "items" in data, f"YouTube channels response missing items: {data}"
