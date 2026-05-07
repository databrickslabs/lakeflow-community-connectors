from databricks.labs.community_connector.sources.palantir.palantir import PalantirLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestPalantirConnector(LakeflowConnectTests):
    connector_class = PalantirLakeflowConnect
    simulator_source = "palantir"
    replay_config = {
        "token": "simulator-fake-token",
        "hostname": "simulator.palantirfoundry.com",
        "ontology_api_name": "ontology-simulator",
    }
