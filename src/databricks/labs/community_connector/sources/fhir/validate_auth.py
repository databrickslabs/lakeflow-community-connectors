"""Validate FHIR credentials by attempting a real token exchange.

Run this after authenticate.py to confirm credentials work before setting
up a pipeline. Exits 0 on success, 1 on failure.

Usage:
    python validate_auth.py --config path/to/dev_config.json
    python validate_auth.py --config path/to/dev_config.json --resource Patient
"""

import argparse
import json
import sys
from pathlib import Path

import requests

from databricks.labs.community_connector.sources.fhir.fhir_utils import (
    SmartAuthClient, FhirHttpClient, discover_token_url,
)
from databricks.labs.community_connector.sources.fhir.fhir_constants import TOKEN_TIMEOUT


def validate(config: dict) -> None:
    """Validate FHIR credentials by performing a token exchange and metadata fetch."""
    auth_type = config.get("auth_type", "none")
    base_url = config.get("base_url", "").rstrip("/")

    if not base_url:
        _fail("'base_url' is required in config.")

    print(f"  base_url   : {base_url}")
    print(f"  auth_type  : {auth_type}")

    # Step 1: Resolve token URL (or skip for none)
    token_url = config.get("token_url", "").strip()
    if auth_type != "none" and not token_url:
        print("\n[1/3] Discovering token endpoint via .well-known/smart-configuration ...")
        try:
            token_url = discover_token_url(base_url)
            print(f"  token_url  : {token_url}  (auto-discovered)")
        except RuntimeError as exc:
            _fail(f"Token endpoint discovery failed:\n  {exc}")
    elif token_url:
        print(f"  token_url  : {token_url}")

    # Step 2: Obtain an access token
    if auth_type != "none":
        print(f"\n[2/3] Requesting access token ({auth_type}) ...")
        auth_client = SmartAuthClient(
            token_url=token_url,
            client_id=config.get("client_id", ""),
            auth_type=auth_type,
            private_key_pem=config.get("private_key_pem", ""),
            client_secret=config.get("client_secret", ""),
            scope=config.get("scope", ""),
            kid=config.get("kid", ""),
            private_key_algorithm=config.get("private_key_algorithm", "RS384"),
        )
        try:
            token = auth_client.get_token()
        except (RuntimeError, ValueError) as exc:
            _fail(f"Token request failed:\n  {exc}")
        print(f"  access token obtained  ({len(token)} chars)")
    else:
        print("\n[2/3] auth_type=none — skipping token request.")
        auth_client = SmartAuthClient(
            token_url="", client_id="", auth_type="none",
        )

    # Step 3: Hit the FHIR server metadata endpoint to confirm connectivity
    print("\n[3/3] Confirming FHIR server connectivity (GET /metadata) ...")
    client = FhirHttpClient(base_url=base_url, auth_client=auth_client)
    resp = client.get("metadata")
    if resp.status_code != 200:
        _fail(
            f"GET {base_url}/metadata returned HTTP {resp.status_code}.\n"
            f"  Response: {resp.text[:300]}"
        )
    body = resp.json()
    fhir_version = body.get("fhirVersion", "unknown")
    software = body.get("software", {}).get("name", "unknown")
    print(f"  FHIR version : {fhir_version}")
    print(f"  Server       : {software}")

    print("\n✓ Credentials validated successfully. The connector can reach the FHIR server.\n")


def _fail(message: str) -> None:
    print(f"\n✗ Validation failed: {message}\n", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    """CLI entry point for credential validation."""
    parser = argparse.ArgumentParser(
        description="Validate FHIR connector credentials against a real FHIR server."
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to dev_config.json produced by authenticate.py",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        _fail(f"Config file not found: {config_path}")

    with open(config_path) as f:
        config = json.load(f)

    print(f"\nValidating FHIR credentials from: {config_path}\n")
    validate(config)


if __name__ == "__main__":
    main()
