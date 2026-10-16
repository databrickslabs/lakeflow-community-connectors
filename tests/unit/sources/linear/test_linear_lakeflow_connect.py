from databricks.labs.community_connector.sources.linear.linear import (
    LinearLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestLinearConnector(LakeflowConnectTests):
    connector_class = LinearLakeflowConnect
    # Simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/linear/``. The single GraphQL endpoint is
    # served by a custom handler (handlers/graphql.py). No credentials or
    # network are required.
    simulator_source = "linear"
    # Personal API key path — the simulator never validates auth, so any
    # right-shaped string works. (The OAuth trio is the alternative auth.)
    replay_config = {"api_key": "lin_api_simulator-fake-key"}
    sample_records = 5
