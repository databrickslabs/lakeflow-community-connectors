"""Tests for EnergyQuantifiedLakeflowConnect.

Default mode runs against the in-process source simulator (no creds,
no network) — pulled from
``src/databricks/labs/community_connector/source_simulator/specs/energy_quantified/``.

The connector implements both ``LakeflowConnect`` and
``SupportsPartitionedStream`` — four of its six tables (``timeseries``,
``periods``, ``ohlc``, ``srmc``) split a date range across partitions,
while ``curves`` (paginated snapshot) and ``instances`` (cursor walk
backward) stay on the single-driver path.

Live-mode runs against the real Energy Quantified API via env vars::

    CONNECTOR_TEST_MODE=live \
      CONNECTOR_TEST_CONFIG_JSON='{"api_key":"..."}' \
      pytest tests/unit/sources/energy_quantified/
"""

from databricks.labs.community_connector.sources.energy_quantified.energy_quantified import (
    EnergyQuantifiedLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestEnergyQuantifiedConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = EnergyQuantifiedLakeflowConnect
    simulator_source = "energy_quantified"
    # Stand-in credentials for simulate / replay mode. The simulator
    # never validates these — the connector just needs an api_key
    # value to construct successfully.
    replay_config = {"api_key": "simulator-fake-api-key"}
