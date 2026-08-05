"""Tests for KlaviyoLakeflowConnect.

Default mode runs against the in-process source simulator (no creds,
no network) — pulled from
``src/databricks/labs/community_connector/source_simulator/specs/klaviyo/``.

Live-mode runs against the real Klaviyo API via env vars:
``CONNECTOR_TEST_MODE=record CONNECTOR_TEST_CONFIG_JSON='{"api_key":...,
"api_revision":...}' pytest tests/unit/sources/klaviyo/``
"""

from databricks.labs.community_connector.sources.klaviyo.klaviyo import (
    KlaviyoLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestKlaviyoConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = KlaviyoLakeflowConnect
    simulator_source = "klaviyo"
    # Stand-in credentials for simulate / replay mode. The simulator
    # never validates these — it just needs the connector to be
    # constructible without requiring a real Klaviyo private key.
    replay_config = {
        "api_key": "pk_simulator_fake_key",
        "api_revision": "2024-10-15",
    }
