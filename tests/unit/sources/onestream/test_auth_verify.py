"""
Auth verification test for OneStreamLakeflowConnect.

Makes a single cheap read-only call (SELECT 1) against a live OneStream
tenant to verify base_url, application_name, token, and connectivity.

Credentials come from CONNECTOR_TEST_CONFIG_JSON or CONNECTOR_TEST_CONFIG_PATH;
the test is skipped when neither is set.

Run with:
    CONNECTOR_TEST_CONFIG_PATH=/path/to/config.json \
        .venv/bin/python -m pytest tests/unit/sources/onestream/test_auth_verify.py -v
"""

from __future__ import annotations

import os

import pytest

from databricks.labs.community_connector.sources.onestream.onestream import (
    OneStreamLakeflowConnect,
)
from tests.unit.sources.test_utils import load_config

pytestmark = pytest.mark.skipif(
    not (
        os.environ.get("CONNECTOR_TEST_CONFIG_JSON", "").strip()
        or os.environ.get("CONNECTOR_TEST_CONFIG_PATH", "").strip()
    ),
    reason="No live credentials (CONNECTOR_TEST_CONFIG_JSON/PATH) - skipping live auth test",
)


def test_auth_connectivity():
    """Verify credentials and connectivity with a trivial read-only query."""
    connector = OneStreamLakeflowConnect(load_config())

    rows = connector._client.run_sql("SELECT 1 AS one")

    assert rows, "Expected at least one row from SELECT 1"
