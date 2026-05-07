from databricks.labs.community_connector.sources.qualtrics.qualtrics import QualtricsLakeflowConnect
from tests.unit.sources.qualtrics.qualtrics_test_utils import LakeflowConnectWriteTestUtils
from tests.unit.sources.test_suite import LakeflowConnectTests
from tests.unit.sources.test_write_back_suite import LakeflowConnectWriteBackTests


class TestQualtricsConnector(LakeflowConnectWriteBackTests, LakeflowConnectTests):
    connector_class = QualtricsLakeflowConnect
    test_utils_class = LakeflowConnectWriteTestUtils
    simulator_source = "qualtrics"
    replay_config = {
        "api_token": "simulator-fake-token",
        "datacenter_id": "iad1",
    }
    # Simulator corpus dates don't overlap the connector's first-call
    # window for these tables — fixture limitation, not a connector bug.
    allow_empty_first_read = frozenset({"survey_definitions", "survey_responses"})
