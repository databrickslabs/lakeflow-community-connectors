"""Live (integration) tests for the OSI PI connector.

Hackathon requirement: each connector must include tests that run the generic
Lakeflow Connect test suite against a live source environment.

This test is designed to:
- Run the shared generic suite (list_tables/schema/metadata/read_table)
- Skip cleanly when no live credentials are available (local dev)

Credentials can be provided either via local JSON (gitignored) or environment vars.

Supported env vars:
- OSIPI_PI_BASE_URL (required)
- OSIPI_ACCESS_TOKEN (optional if using OIDC client credentials)
- OSIPI_WORKSPACE_HOST (optional, for OIDC)
- OSIPI_CLIENT_ID (optional, for OIDC)
- OSIPI_CLIENT_SECRET (optional, for OIDC)
- OSIPI_VERIFY_SSL (optional, default true)

Notes:
- We intentionally do NOT require UC Connections here; the generic test suite
  operates by instantiating the connector class directly.
- If you want to validate UC Connections wiring, use the Databricks notebook
  examples in the connector's examples directory.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import pytest

from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config
from databricks.labs.community_connector.sources.osipi.osipi import LakeflowConnect


def _is_blank(value: object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _env(name: str) -> str | None:
    v = os.environ.get(name)
    return v if (v is not None and v.strip() != "") else None


def _load_live_init_options() -> Dict[str, Any] | None:
    """Load live connection options from env or local (gitignored) JSON."""

    # 1) Preferred for CI: environment variables
    pi_base_url = _env("OSIPI_PI_BASE_URL")
    if pi_base_url:
        opts: Dict[str, Any] = {
            "pi_base_url": pi_base_url,
            "verify_ssl": _env("OSIPI_VERIFY_SSL") or "true",
        }

        access_token = _env("OSIPI_ACCESS_TOKEN")
        if access_token:
            opts["access_token"] = access_token

        # Optional OIDC client creds (alternative to OSIPI_ACCESS_TOKEN)
        workspace_host = _env("OSIPI_WORKSPACE_HOST")
        client_id = _env("OSIPI_CLIENT_ID")
        client_secret = _env("OSIPI_CLIENT_SECRET")
        if workspace_host:
            opts["workspace_host"] = workspace_host
        if client_id:
            opts["client_id"] = client_id
        if client_secret:
            opts["client_secret"] = client_secret

        return opts

    # 2) Local dev fallback: dev_config.json (gitignored) or dev_config.local.json
    config_dir = Path(__file__).parent / "configs"
    for fname in ("dev_config.json", "dev_config.local.json"):
        p = config_dir / fname
        if p.exists():
            cfg = load_config(p)
            if not isinstance(cfg, dict):
                raise ValueError(f"{p} must be a JSON object")
            if _is_blank(cfg.get("pi_base_url")) and _is_blank(cfg.get("pi_web_api_url")):
                return None
            return cfg

    return None


def _load_table_config() -> Dict[str, Dict[str, Any]]:
    config_dir = Path(__file__).parent / "configs"
    table_config_path = config_dir / "dev_table_config.json"
    return load_config(table_config_path)


@pytest.mark.integration
def test_osipi_connector_generic_suite_live():
    """Run the shared LakeflowConnect generic suite against a live OSI PI environment."""

    init_options = _load_live_init_options()
    if not init_options:
        pytest.skip(
            "Missing live OSI PI config. Set OSIPI_PI_BASE_URL (+ auth), or create "
            "tests/unit/sources/osipi/configs/dev_config.local.json (gitignored)."
        )

    # Inject into the shared test_suite namespace so LakeflowConnectTester can instantiate it.
    test_suite.LakeflowConnect = LakeflowConnect

    table_config = _load_table_config()

    tester = LakeflowConnectTester(init_options, table_config)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
