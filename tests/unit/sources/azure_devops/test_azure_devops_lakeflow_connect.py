from databricks.labs.community_connector.sources.azure_devops.azure_devops import (
    AzureDevopsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAzureDevopsConnector(LakeflowConnectTests):
    connector_class = AzureDevopsLakeflowConnect
    simulator_source = "azure_devops"
    replay_config = {
        "organization": "simulator-org",
        "project": "simulator-project",
        "personal_access_token": "simulator-fake-pat",
    }
