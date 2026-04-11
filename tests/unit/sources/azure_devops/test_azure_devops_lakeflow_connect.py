from databricks.labs.community_connector.sources.azure_devops.azure_devops import LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAzureDevopsConnector(LakeflowConnectTests):
    connector_class = LakeflowConnect
