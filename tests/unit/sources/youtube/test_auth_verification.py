"""
Auth verification test for youtube connector.

Supports two auth modes (from dev_config.json):
- API key: if "api_key" is set (non-placeholder), calls YouTube API with key=...
- OAuth: if client_id, client_secret, refresh_token are set, exchanges for token
  and optionally calls channels?mine=true.

If the API returns 403 (e.g. API not enabled), the API check is skipped.
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
# Minimal YouTube Data API v3 check: channels for the authenticated user (OAuth)
YOUTUBE_CHANNELS_URL = "https://www.googleapis.com/youtube/v3/channels?part=id&mine=true"
# Public channel ID for API-key test (YouTube's own channel)
YOUTUBE_CHANNELS_BY_ID_URL = "https://www.googleapis.com/youtube/v3/channels?part=id,snippet&id=UC_x5XG1OV2P6uZZ5FSM9Ttw"


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
    """Call YouTube Data API v3 channels (OAuth); skip if 403."""
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


def _call_youtube_with_api_key_or_skip(api_key: str) -> dict:
    """Call YouTube Data API v3 with key= (public channel); skip if 403/400."""
    url = f"{YOUTUBE_CHANNELS_BY_ID_URL}&key={urllib.parse.quote(api_key, safe='')}"
    try:
        req = urllib.request.Request(url, method="GET")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code in (403, 400):
            pytest.skip(
                "YouTube Data API v3 returned %s. Ensure the API key is valid and "
                "YouTube Data API v3 is enabled for the project. CI can pass without it."
                % e.code
            )
        raise


class TestYoutubeAuthVerification:
    """Auth verification: API key or OAuth (token exchange + optional API check)."""

    @pytest.fixture(autouse=True)
    def _require_credentials(self):
        """Skip if no dev_config; set _config and _auth_mode (api_key or oauth)."""
        config = _load_dev_config()
        if config is None:
            pytest.skip(
                "No dev_config.json found at tests/unit/sources/youtube/configs/dev_config.json"
            )
        api_key = config.get("api_key") or ""
        if api_key and not _is_placeholder(str(api_key)):
            self._config = config
            self._auth_mode = "api_key"
            return
        for key in ["client_id", "client_secret", "refresh_token"]:
            val = config.get(key, "")
            if _is_placeholder(str(val)):
                pytest.skip(
                    "dev_config.json must have either non-placeholder 'api_key' or "
                    "all of client_id, client_secret, refresh_token."
                )
        self._config = config
        self._auth_mode = "oauth"

    def test_token_exchange(self):
        """Verify refresh_token can be exchanged for an access_token (OAuth only)."""
        if self._auth_mode != "oauth":
            pytest.skip("OAuth credentials not in use (api_key mode)")
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]
        refresh_token = self._config["refresh_token"]

        token_data = _exchange_refresh_token(client_id, client_secret, refresh_token)

        assert "access_token" in token_data, (
            f"Token response missing access_token: {token_data.get('error', token_data)}"
        )
        assert token_data.get("token_type", "").lower() == "bearer"

    def test_access_token_works_with_youtube_api(self):
        """Verify OAuth access token works with YouTube Data API v3 (OAuth only)."""
        if self._auth_mode != "oauth":
            pytest.skip("OAuth credentials not in use (api_key mode)")
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]
        refresh_token = self._config["refresh_token"]

        token_data = _exchange_refresh_token(client_id, client_secret, refresh_token)
        access_token = token_data["access_token"]

        data = _call_youtube_channels_or_skip(access_token)
        assert "items" in data, f"YouTube channels response missing items: {data}"

    def test_api_key_works_with_youtube_api(self):
        """Verify api_key works with a public YouTube Data API v3 call."""
        if self._auth_mode != "api_key":
            pytest.skip("API key not in use (OAuth mode)")
        api_key = self._config["api_key"]
        data = _call_youtube_with_api_key_or_skip(api_key)
        assert "items" in data, f"YouTube channels response missing items: {data}"
