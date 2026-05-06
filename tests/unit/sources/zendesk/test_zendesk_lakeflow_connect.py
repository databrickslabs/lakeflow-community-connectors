from databricks.labs.community_connector.sources.zendesk.zendesk import ZendeskLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestZendeskConnector(LakeflowConnectTests):
    connector_class = ZendeskLakeflowConnect
    simulator_source = "zendesk"
    replay_config = {
        "subdomain": "simulator",
        "email": "simulator@example.com",
        "api_token": "simulator-fake-token",
    }
