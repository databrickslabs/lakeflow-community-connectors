"""Simulate-mode tests for the Egnyte connector.

Runs offline against ``source_simulator/specs/egnyte/`` (endpoints.yaml +
corpus/). No credentials, no network. See the module docstring on
``egnyte.py`` for why four of the nine tables stream through
``SupportsPartitionedStream`` while the rest use the single-driver
``read_table`` path.
"""

from databricks.labs.community_connector.sources.egnyte.egnyte import (
    EgnyteLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestEgnyteConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = EgnyteLakeflowConnect
    # Simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/egnyte/``. No live credentials required.
    simulator_source = "egnyte"
    # The synthesized corpus holds 5 records per table; 20 leaves headroom
    # for the tables that emit more rows than corpus records (``folders``
    # yields the walked root plus each child).
    sample_records = 20

    # ``__init__`` needs the tenant ``domain`` plus either a pre-issued
    # ``access_token`` or the client_id/client_secret/refresh_token trio.
    # The pre-issued token is used here so no OAuth exchange is needed;
    # the simulator never validates any of these values.
    # ``min_request_interval`` defaults to 0.5s (Egnyte allows 2 calls/sec
    # per token) — zeroed here so the offline suite isn't paced by a
    # rate limiter that has nothing to protect.
    replay_config = {
        "domain": "simulator",
        "access_token": "simulator-fake-token",
        "min_request_interval": "0",
    }

    # ``members`` is only returned by ``GET /pubapi/v2/groups/{id}``, so the
    # column stays NULL unless the per-group fan-out is enabled. Turn it on
    # so the groups schema is fully exercised.
    table_configs = {
        "groups": {"include_members": "true"},
    }
