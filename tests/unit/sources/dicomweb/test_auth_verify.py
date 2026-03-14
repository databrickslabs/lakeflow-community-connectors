"""
Auth verification test for DICOMwebLakeflowConnect.

Connects to the real DICOMweb server configured in dev_config.json and
makes the simplest possible API call (query 1 study) to verify that
the base_url, credentials, and network connectivity are correct.

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
    """Verify credentials and connectivity by querying a single study."""
    connector = _make_connector()

    # Simplest API call: fetch 1 study from the server
    results = connector._client.query_studies("19000101-99991231", limit=1, offset=0)

    assert isinstance(results, list), f"Expected list, got {type(results).__name__}"
    assert len(results) > 0, (
        "Server returned no studies. "
        "Check that the DICOMweb server is reachable and dev_config.json credentials are correct."
    )
