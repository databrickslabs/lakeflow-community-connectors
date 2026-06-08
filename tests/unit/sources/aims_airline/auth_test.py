"""
Auth verification test for AIMS Airline connector.
Run this script to verify your credentials are correctly configured.

Usage:
    python tests/unit/sources/aims_airline/auth_test.py
"""
import re
import sys
from pathlib import Path

# Add project root to path when running script directly
_project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from tests.unit.sources.test_utils import load_config


def test_auth():
    """Verify that credentials in dev_config.json are valid.

    The connector spec defines a single connection parameter: access_key (string, required, secret).
    This test validates that:
    1. The config loads successfully
    2. access_key is present and non-empty
    3. access_key has a valid format (32 hex characters, typical for API keys)
    """
    config_path = Path(__file__).parent / "configs" / "dev_config.json"
    assert config_path.exists(), (
        f"Config file not found: {config_path}. "
        "Run the authenticate script to collect credentials first."
    )

    config = load_config(config_path)

    assert "access_key" in config, (
        "'access_key' is missing from dev_config.json. "
        "Add access_key to tests/unit/sources/aims_airline/configs/dev_config.json"
    )

    access_key = config["access_key"]
    assert access_key and isinstance(access_key, str), (
        "'access_key' must be a non-empty string"
    )

    access_key = access_key.strip()
    assert access_key, "'access_key' cannot be empty or whitespace"

    assert re.match(r"^[a-fA-F0-9]{32}$", access_key), (
        "'access_key' should be 32 hexadecimal characters. "
        "Current format does not match expected pattern."
    )

    print("Authentication successful! Credentials loaded and validated.")
    print("  access_key: present, non-empty, valid format (32 hex chars)")


def _run_standalone():
    """Run test when executed as script; exits with code 0 on success, 1 on failure."""
    try:
        test_auth()
        return 0
    except AssertionError as e:
        print(f"Authentication failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(_run_standalone())
