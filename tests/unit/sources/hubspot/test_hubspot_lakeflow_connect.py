from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect
from tests.unit.sources.hubspot.hubspot_test_utils import LakeflowConnectWriteTestUtils
from tests.unit.sources.test_suite import LakeflowConnectTests
from tests.unit.sources.test_write_back_suite import LakeflowConnectWriteBackTests


class TestHubspotConnector(LakeflowConnectWriteBackTests, LakeflowConnectTests):
    connector_class = HubspotLakeflowConnect
    simulator_source = "hubspot"
    test_utils_class = LakeflowConnectWriteTestUtils
    replay_config = {"access_token": "simulator-fake-access-token"}
