"""
Auth verification test for Notion connector.
Run this script to verify your credentials are correctly configured.

Usage:
    python tests/unit/sources/notion/auth_test.py

The Notion API uses Bearer token auth. The api_key field in dev_config.json
must be a valid Notion Internal Integration Token.
The token is validated by calling GET /v1/users/me.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

import pathlib
import requests

from tests.unit.sources.test_utils import load_config

CONFIG_PATH = pathlib.Path(__file__).parent / "configs" / "dev_config.json"

NOTION_VERSION = "2026-03-11"
BASE_URL = "https://api.notion.com"
VERIFY_ENDPOINT = "/v1/users/me"


def _is_placeholder(value: str) -> bool:
    """Return True if the value looks like a placeholder or is empty."""
    if not value or not isinstance(value, str):
        return True
    placeholders = ["YOUR_", "REPLACE_", "dummy", "xxx", "***", "<"]
    return any(p in value for p in placeholders)


def test_auth():
    """Verify that credentials in dev_config.json are valid by calling GET /v1/users/me."""
    if not CONFIG_PATH.exists():
        print("ERROR: dev_config.json not found.")
        print(f"   Expected at: {CONFIG_PATH}")
        return False

    config = load_config(CONFIG_PATH)

    api_key = config.get("api_key", "")
    if _is_placeholder(api_key):
        print("ERROR: 'api_key' in dev_config.json is missing or looks like a placeholder.")
        print("   Set it to your Notion Internal Integration Token.")
        return False

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": NOTION_VERSION,
    }

    url = f"{BASE_URL}{VERIFY_ENDPOINT}"
    response = requests.get(url, headers=headers, timeout=10)

    if response.status_code == 200:
        data = response.json()
        bot_name = data.get("name", "<unknown>")
        bot_type = data.get("type", "bot")
        print(f"Authentication successful! Connected to Notion.")
        print(f"   Bot name : {bot_name}")
        print(f"   Bot type : {bot_type}")
        return True
    elif response.status_code == 401:
        print("Authentication failed: Invalid credentials (HTTP 401).")
        print("   Check your api_key in tests/unit/sources/notion/configs/dev_config.json.")
        print(f"   Response: {response.text}")
        return False
    elif response.status_code == 403:
        print("Authorization failed: Insufficient permissions (HTTP 403).")
        print("   Ensure the integration has 'Read user information' capability enabled.")
        print(f"   Response: {response.text}")
        return False
    else:
        print(f"Unexpected response: HTTP {response.status_code}")
        print(f"   Body: {response.text}")
        return False


if __name__ == "__main__":
    success = test_auth()
    sys.exit(0 if success else 1)
