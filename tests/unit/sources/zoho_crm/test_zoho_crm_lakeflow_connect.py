from databricks.labs.community_connector.sources.zoho_crm.zoho_crm import ZohoCRMLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestZohoCrmConnector(LakeflowConnectTests):
    connector_class = ZohoCRMLakeflowConnect
    simulator_source = "zoho_crm"
    replay_config = {
        "client_id": "00000000-0000-0000-0000-000000000000",
        "client_secret": "simulator-fake-secret",
        "refresh_token": "simulator-fake-refresh-token",
        "base_url": "https://simulator.example.com",
    }
