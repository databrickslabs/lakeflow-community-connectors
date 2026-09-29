"""Simulate-mode tests for the Maxio (SaaSOptics REST API v1.0) connector.

The simulator spec lives at
``source_simulator/specs/maxio/endpoints.yaml`` with a synthesized corpus
under ``corpus/``. The corpus is bootstrapped from the connector's
``TABLE_SCHEMAS`` via
``source_simulator.tools.corpus_from_schema.write_corpus_from_schemas``.

No live credentials are required: ``replay_config`` declares stand-in
values whose shape matches the connector's required options. The
simulator never validates them.
"""

from databricks.labs.community_connector.sources.maxio.maxio import (
    MaxioLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMaxioConnector(LakeflowConnectTests):
    connector_class = MaxioLakeflowConnect
    simulator_source = "maxio"
    replay_config = {
        "server_subdomain": "simulator",
        "account_name": "sim_account",
        "api_key": "simulator-fake-key",
    }
