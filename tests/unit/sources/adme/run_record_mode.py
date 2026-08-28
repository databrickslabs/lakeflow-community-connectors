#!/usr/bin/env python3
"""Run record-mode tests for the ADME connector against a live ADME instance.

This script is designed to be run on a Databricks cluster that has
Managed Identity access to an ADME (Azure Data Manager for Energy) instance.

Usage (on a Databricks cluster terminal or notebook %sh cell):
    cd /path/to/lakeflow-community-connectors
    pip install -e ".[dev]" -e "src/databricks/labs/community_connector/sources/adme[dev,azure-identity]"

    # Option A: Using environment variables
    export CONNECTOR_TEST_MODE=record
    export CONNECTOR_TEST_CONFIG_PATH=tests/unit/sources/adme/configs/dev_config.json
    PYTHONPATH=src pytest tests/unit/sources/adme/ -q

    # Option B: Using this convenience script (auto-generates config)
    python tests/unit/sources/adme/run_record_mode.py

After a successful run, these files will be created locally:
    tests/unit/sources/adme/cassettes/TestADMEConnector.json
    tests/unit/sources/adme/cassettes/TestADMEConnector.json.validation.json
    tests/unit/sources/adme/cassettes/TestADMEConnector.json.coverage.json

The self-review check B11/B12 will pass once these files exist locally.
These files are gitignored and not committed to the repository.

Configuration (dev_config.json):
    {
        "auth_mode": "managed_identity",
        "instance_url": "https://admesbxscusins1.energy.azure.com",
        "data_partition_id": "opendes",
        "tenant_id": "<your-azure-tenant-id>",
        "adme_api_client_id": "e37a6c70-7cbc-4593-80fc-01c1f20203f7",
        "managed_identity_client_id": "<your-MI-client-id-or-omit-for-system-assigned>",
        "kind_query_rock_and_fluid": "osdu:wks:work-product-component--RockSampleAnalysis:*"
    }
"""

import json
import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parents[4]
CONFIG_DIR = SCRIPT_DIR / "configs"
CONFIG_PATH = CONFIG_DIR / "dev_config.json"

DEFAULT_CONFIG = {
    "auth_mode": "managed_identity",
    "instance_url": "https://admesbxscusins1.energy.azure.com",
    "data_partition_id": "opendes",
    "tenant_id": "72f988bf-86f1-41af-91ab-2d7cd011db47",
    "adme_api_client_id": "e37a6c70-7cbc-4593-80fc-01c1f20203f7",
    "kind_query_rock_and_fluid": "osdu:wks:work-product-component--RockSampleAnalysis:*",
}


def main():
    if not CONFIG_PATH.exists():
        print(f"No config found at {CONFIG_PATH}")
        print("Creating default config for admesbxscusins1/opendes with MI auth...")
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        print(f"  Written to: {CONFIG_PATH}")
        print("  Edit tenant_id / managed_identity_client_id if needed, then re-run.")
        print()

    env = os.environ.copy()
    env["CONNECTOR_TEST_MODE"] = "record"
    env["CONNECTOR_TEST_CONFIG_PATH"] = str(CONFIG_PATH)
    env["PYTHONPATH"] = str(REPO_ROOT / "src")

    cmd = [
        sys.executable, "-m", "pytest",
        str(SCRIPT_DIR / "test_adme_lakeflow_connect.py"),
        "-v", "--tb=short",
    ]

    print(f"Running: CONNECTOR_TEST_MODE=record pytest ...")
    print(f"Config: {CONFIG_PATH}")
    print()

    result = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT))

    cassette_dir = SCRIPT_DIR / "cassettes"
    validation = cassette_dir / "TestADMEConnector.json.validation.json"
    coverage = cassette_dir / "TestADMEConnector.json.coverage.json"

    print()
    print("=" * 60)
    if validation.exists() and coverage.exists():
        with open(validation) as f:
            v = json.load(f)
        with open(coverage) as f:
            c = json.load(f)
        s = v.get("summary", {})
        print("RECORD MODE COMPLETE")
        print(f"  Validation: {s.get('endpoints_validated', '?')}/{s.get('endpoints_total', '?')} endpoints validated")
        print(f"  Mismatched: {s.get('endpoints_mismatched', 0)}")
        print(f"  Coverage: {len(c.get('endpoints', []))} endpoints hit")
        print()
        print("Self-review B11/B12 will now PASS.")
    else:
        print("RECORD MODE FAILED — cassette artifacts not generated.")
        print(f"  Expected: {validation}")
        print(f"  Expected: {coverage}")

    print("=" * 60)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
