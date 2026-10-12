"""Databricks' shared connector contracts against a sealed jss0 simulator.

The standalone package does not vendor Databricks' test harness. This module
is collected when copied into the upstream repository, where that harness is
available. The local archive protocol tests remain independently runnable.
"""

import pytest

pytest.importorskip("tests.unit.sources.test_suite")

from databricks.labs.community_connector.sources.bluesky.bluesky import BlueskyLakeflowConnect
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestBlueskyConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    """Exercise discovery, schema, read, retry, and stream offset contracts."""

    connector_class = BlueskyLakeflowConnect
    simulator_source = "bluesky"
    replay_config = {
        "endpoint": "https://jetstream.us-east.bsky.network",
        "api_key": "simulator-only-token",
        "starting_cursor": "0",
        "max_events_per_refresh": "3",
    }
    sample_records = 3
