from databricks.labs.community_connector.sources.appsflyer.appsflyer import AppsflyerLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAppsflyerConnector(LakeflowConnectTests):
    connector_class = AppsflyerLakeflowConnect
    simulator_source = "appsflyer"
    replay_config = {
        "api_token": "simulator-fake-token",
        "base_url": "https://simulator.example.com",
    }
