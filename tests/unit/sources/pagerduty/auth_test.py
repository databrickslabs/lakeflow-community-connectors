"""
Auth verification test for PagerDuty connector.
Run this script to verify your credentials are correctly configured.

Usage:
    python tests/unit/sources/pagerduty/auth_test.py
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

from pathlib import Path
from tests.unit.sources.test_utils import load_config
import requests


def test_auth():
    """Verify that credentials in dev_config.json are valid by making a simple API call."""
    config_path = Path(__file__).parent / "configs" / "dev_config.json"
    config = load_config(config_path)

    api_key = config["api_key"]

    headers = {
        "Authorization": f"Token token={api_key}",
        "Accept": "application/vnd.pagerduty+json;version=2",
    }

    response = requests.get(
        "https://api.pagerduty.com/users",
        headers=headers,
        params={"limit": 1},
        timeout=10,
    )

    if response.status_code == 200:
        data = response.json()
        users = data.get("users", [])
        sample = users[0].get("name", "<unknown>") if users else "<no users returned>"
        print(f"Authentication successful! Connected to PagerDuty.")
        print(f"   Sample user name: {sample}")
        return True
    elif response.status_code == 401:
        print("Authentication failed: Invalid credentials (HTTP 401).")
        print("   Check your api_key in tests/unit/sources/pagerduty/configs/dev_config.json")
        return False
    elif response.status_code == 403:
        print("Authorization failed: Insufficient permissions (HTTP 403).")
        print("   Ensure your API token has the required scopes/permissions.")
        return False
    else:
        print(f"Unexpected response: HTTP {response.status_code}")
        print(f"   Body: {response.text}")
        return False


if __name__ == "__main__":
    success = test_auth()
    sys.exit(0 if success else 1)
