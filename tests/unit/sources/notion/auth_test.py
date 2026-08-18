"""
Auth verification test for Notion connector.
Run this script to verify your credentials are correctly configured.

Usage:
    # via env var (inline JSON):
    CONNECTOR_TEST_CONFIG_JSON='{"api_token":"secret_..."}' \\
        python tests/unit/sources/notion/auth_test.py

    # via env var (path to a JSON file at any location):
    CONNECTOR_TEST_CONFIG_PATH=~/secrets/notion.json \\
        python tests/unit/sources/notion/auth_test.py
"""
import sys
import os

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
sys.path.insert(0, project_root)

from tests.unit.sources.test_utils import load_config
import requests


def test_auth():
    """Verify supplied credentials are valid by making a simple API call."""
    config = load_config()  # honors CONNECTOR_TEST_CONFIG_JSON / _PATH env vars

    # Build auth headers from config - Notion uses Bearer token
    api_token = config.get("api_token")
    if not api_token:
        print("Error: 'api_token' not found in credentials.")
        print("   Please provide credentials via CONNECTOR_TEST_CONFIG_JSON or CONNECTOR_TEST_CONFIG_PATH.")
        return False

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Notion-Version": "2025-09-03",
        "Content-Type": "application/json",
    }

    # Use the /users endpoint to verify authentication
    # This is a simple read-only endpoint that returns workspace users
    response = requests.get(
        "https://api.notion.com/v1/users",
        headers=headers,
        timeout=10
    )

    if response.status_code == 200:
        data = response.json()
        user_count = len(data.get("results", []))
        print(f"Authentication successful! Connected to Notion.")
        print(f"   Found {user_count} user(s) in workspace.")
        if data.get("results"):
            first_user = data["results"][0]
            print(f"   First user: {first_user.get('name', 'N/A')} ({first_user.get('type', 'N/A')})")
        return True
    elif response.status_code == 401:
        print(f"Authentication failed: Invalid credentials (HTTP 401).")
        print(f"   Check the api_token supplied via CONNECTOR_TEST_CONFIG_JSON / "
              f"CONNECTOR_TEST_CONFIG_PATH.")
        return False
    elif response.status_code == 403:
        print(f"Authorization failed: Insufficient permissions (HTTP 403).")
        print(f"   Ensure your Notion integration has the required permissions.")
        return False
    else:
        print(f"Unexpected response: HTTP {response.status_code}")
        print(f"   Body: {response.text}")
        return False


if __name__ == "__main__":
    success = test_auth()
    sys.exit(0 if success else 1)