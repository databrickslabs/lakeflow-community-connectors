from databricks.labs.community_connector.sources.osipi.osipi import OsipiLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestOsipiConnector(LakeflowConnectTests):
    connector_class = OsipiLakeflowConnect
    simulator_source = "osipi"
    replay_config = {
        "pi_base_url": "https://simulator-pi.example.com",
        "access_token": "simulator-fake-access-token",
    }
