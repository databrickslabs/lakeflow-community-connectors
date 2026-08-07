"""
Auth verification test for the Fin.ai (Intercom) connector.
Run this script to verify your credentials are correctly configured.

Usage:
    # via env var (inline JSON):
    CONNECTOR_TEST_CONFIG_JSON='{"access_token":"...","region":"us"}' \\
        python tests/unit/sources/fin_ai/auth_test.py

    # via env var (path to a JSON file at any location):
    CONNECTOR_TEST_CONFIG_PATH=~/secrets/fin_ai.json \\
        python tests/unit/sources/fin_ai/auth_test.py

This reuses the connector's own auth constants (``BASE_URLS``,
``INTERCOM_VERSION``, ``DEFAULT_TIMEOUT``) from ``fin_ai_schemas`` so the
verification call matches exactly how ``FinAiLakeflowConnect`` authenticates
in production. It calls ``GET /me`` — the lightest-weight authenticated
Intercom endpoint (identifies the admin owning the access token) — and never
prints the token itself.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import requests

from tests.unit.sources.test_utils import load_config
from databricks.labs.community_connector.sources.fin_ai.fin_ai_schemas import (
    BASE_URLS,
    DEFAULT_TIMEOUT,
    INTERCOM_VERSION,
)


def test_auth() -> bool:
    """Verify supplied Fin.ai (Intercom) credentials are valid."""
    try:
        config = load_config()  # honors CONNECTOR_TEST_CONFIG_JSON / _PATH env vars
    except RuntimeError:
        # No credentials configured (e.g. the default offline simulate run).
        # There is nothing to verify against, so skip rather than error.
        try:
            import pytest

            pytest.skip("No credentials configured; live auth check skipped")
        except ImportError:
            print("No credentials configured; skipping auth check.")
            return True

    access_token = config.get("access_token")
    if not access_token:
        print("Config is missing required field 'access_token'.")
        return False

    region = (config.get("region") or "us").lower()
    if region not in BASE_URLS:
        print(f"Unsupported region {region!r}; expected one of {sorted(BASE_URLS)}")
        return False
    base_url = BASE_URLS[region]

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Intercom-Version": INTERCOM_VERSION,
    }

    response = requests.get(
        f"{base_url}/me",
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    if response.status_code == 200:
        body = response.json()
        print(f"Authentication successful! Connected to Fin.ai (Intercom) [{region}].")
        print(
            f"   Authenticated as admin: {body.get('name') or body.get('email') or body.get('id')} "
            f"(app_id={body.get('app', {}).get('id_code', 'n/a')})"
        )
        return True
    elif response.status_code == 401:
        print("Authentication failed: Invalid/expired access token (HTTP 401).")
        try:
            print(f"   API error: {response.json()}")
        except ValueError:
            print(f"   Body: {response.text}")
        print(
            "   Check the 'access_token' supplied via CONNECTOR_TEST_CONFIG_JSON / "
            "CONNECTOR_TEST_CONFIG_PATH."
        )
        return False
    elif response.status_code == 403:
        print("Authorization failed: Insufficient permissions/scope (HTTP 403).")
        try:
            print(f"   API error: {response.json()}")
        except ValueError:
            print(f"   Body: {response.text}")
        print("   Ensure the private app token has the required read scopes.")
        return False
    elif response.status_code in (301, 302, 307, 308):
        print(f"Unexpected redirect (HTTP {response.status_code}) — likely wrong 'region'.")
        print(f"   Location: {response.headers.get('Location', 'n/a')}")
        return False
    else:
        print(f"Unexpected response: HTTP {response.status_code}")
        print(f"   Body: {response.text}")
        return False


if __name__ == "__main__":
    success = test_auth()
    sys.exit(0 if success else 1)
