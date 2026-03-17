from databricks.labs.community_connector.sources.zendesk.zendesk import ZendeskLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestZendeskConnector(LakeflowConnectTests):
    connector_class = ZendeskLakeflowConnect
