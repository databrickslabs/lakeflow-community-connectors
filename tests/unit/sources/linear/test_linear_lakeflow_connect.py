from databricks.labs.community_connector.sources.linear.linear import LinearLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestLinearConnector(LakeflowConnectTests):
    connector_class = LinearLakeflowConnect
    # Use simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/linear/``. Linear is a single GraphQL
    # endpoint served by a custom handler that parses the request body,
    # routes to the issues/projects corpus, applies the updatedAt window,
    # and renders Relay pages. The handler also injects future-dated clones
    # so ``test_read_terminates`` doubles as an init-time cap check — an
    # uncapped connector would leak them and fail to converge.
    simulator_source = "linear"
    # Personal API key sent verbatim in the Authorization header. The
    # simulator never validates it, so any string of the right shape works.
    replay_config = {"api_key": "simulator-fake-key"}
