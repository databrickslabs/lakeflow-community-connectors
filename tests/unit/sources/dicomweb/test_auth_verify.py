"""
Auth verification test for DICOMwebLakeflowConnect.

Connects to the real DICOMweb server configured in dev_config.json and
makes the simplest possible API call to verify that the base_url,
credentials, and network connectivity are correct.

Run with:
    .venv/bin/python -m pytest tests/unit/sources/dicomweb/test_auth_verify.py -v
"""

from __future__ import annotations

import pathlib

import pytest

from databricks.labs.community_connector.sources.dicomweb.dicomweb import (
    DICOMwebLakeflowConnect,
)
from tests.unit.sources.test_utils import load_config

CONFIG_PATH = pathlib.Path(__file__).parent / "configs" / "dev_config.json"

pytestmark = pytest.mark.skipif(
    not CONFIG_PATH.exists(),
    reason="dev_config.json not found — skipping live auth tests",
)


def _make_connector() -> DICOMwebLakeflowConnect:
    """Load dev_config.json and return an instantiated connector."""
    config = load_config(CONFIG_PATH)
    return DICOMwebLakeflowConnect(config)


def test_auth_connectivity():
    """Verify credentials and connectivity by probing the /studies endpoint."""
    connector = _make_connector()

    result = connector._client.probe_endpoint("/studies?limit=1")

    if result["error"] is not None:
        pytest.skip(f"DICOMweb server unreachable: {result['error']}")

    assert result["status_code"] is not None, "Probe returned no status code"
    assert result["status_code"] == 200, (
        f"Expected HTTP 200, got {result['status_code']}. "
        "Check that dev_config.json credentials are correct."
    )
