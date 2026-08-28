from databricks.labs.community_connector.sources.amplitude.amplitude import (
    AmplitudeLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAmplitudeConnector(LakeflowConnectTests):
    connector_class = AmplitudeLakeflowConnect
    simulator_source = "amplitude"
    # HTTP Basic auth (api_key:secret_key); the simulator never validates
    # these, so any non-empty strings of the right shape work.
    replay_config = {
        "api_key": "simulator-fake-api-key",
        "secret_key": "simulator-fake-secret-key",
    }
