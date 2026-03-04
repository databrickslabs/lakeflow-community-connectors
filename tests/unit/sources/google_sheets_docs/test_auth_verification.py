"""
Auth verification test for google_sheets_docs connector.

Verifies that stored credentials (client_id, client_secret, refresh_token) can be used to:
1. Obtain an access token via Google OAuth2 token endpoint.
2. Optionally call a minimal API (Drive or Sheets) to confirm the token works.

No connector implementation is required; this test validates token exchange only.
Run with: pytest tests/unit/sources/google_sheets_docs/test_auth_verification.py -v
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest


def _call_drive_about_or_skip(access_token: str) -> dict:
    """Call Drive API about; skip with message if 403 (scope/API not enabled)."""
    try:
        return _call_drive_about(access_token)
    except urllib.error.HTTPError as e:
        if e.code == 403:
            pytest.skip(
                "Drive API returned 403. Ensure the OAuth consent includes "
                "https://www.googleapis.com/auth/drive.readonly and the Google Cloud "
                "project has the Drive API enabled."
            )
        raise

from tests.unit.sources.test_utils import load_config


# Google OAuth2 token endpoint (from connector_spec)
TOKEN_URL = "https://oauth2.googleapis.com/token"
# Minimal API check: Drive about returns current user (no extra scopes beyond drive.readonly)
DRIVE_ABOUT_URL = "https://www.googleapis.com/drive/v3/about?fields=user"


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


def _call_drive_about(access_token: str) -> dict:
    """Call Drive API about endpoint to verify token works."""
    req = urllib.request.Request(DRIVE_ABOUT_URL, method="GET")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


class TestGoogleSheetsDocsAuthVerification:
    """Auth verification tests for google_sheets_docs (token exchange + optional API check)."""

    @pytest.fixture(autouse=True)
    def _require_credentials(self):
        """Skip entire class if dev_config is missing or credentials are placeholders."""
        config = _load_dev_config()
        if config is None:
            pytest.skip("No dev_config.json found at tests/unit/sources/google_sheets_docs/configs/")
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

    def test_access_token_works_with_drive_api(self):
        """Verify the obtained access token works with a minimal Drive API call."""
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]
        refresh_token = self._config["refresh_token"]

        token_data = _exchange_refresh_token(client_id, client_secret, refresh_token)
        access_token = token_data["access_token"]

        about = _call_drive_about_or_skip(access_token)
        assert "user" in about, f"Drive about response missing user: {about}"
        # user has displayName, emailAddress, etc.
        user = about["user"]
        assert "emailAddress" in user or "displayName" in user, (
            f"Drive user object unexpected: {user}"
        )
