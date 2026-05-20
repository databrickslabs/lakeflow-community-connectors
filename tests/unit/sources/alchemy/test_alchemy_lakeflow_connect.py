"""Tests for AlchemyLakeflowConnect.

Default mode runs against the in-process source simulator (no creds,
no network) — pulled from
``src/databricks/labs/community_connector/source_simulator/specs/alchemy/``.

Live-mode runs against the real Alchemy API via env vars:
``CONNECTOR_TEST_MODE=record CONNECTOR_TEST_CONFIG_JSON='{"api_key":...,
"network":...}' pytest tests/unit/sources/alchemy/``
"""

from databricks.labs.community_connector.sources.alchemy.alchemy import (
    AlchemyLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAlchemyConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = AlchemyLakeflowConnect
    simulator_source = "alchemy"
    # Stand-in credentials for simulate / replay mode. The simulator
    # never validates these — it just needs the connector to be
    # constructible without requiring a real Alchemy key.
    replay_config = {
        "api_key": "simulator-fake-key",
        "network": "eth-mainnet",
    }
