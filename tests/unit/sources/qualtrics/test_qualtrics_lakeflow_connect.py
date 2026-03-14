from databricks.labs.community_connector.sources.qualtrics.qualtrics import QualtricsLakeflowConnect
from tests.unit.sources.qualtrics.qualtrics_test_utils import LakeflowConnectWriteTestUtils
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestQualtricsConnector(LakeflowConnectTests):
    connector_class = QualtricsLakeflowConnect
    test_utils_class = LakeflowConnectWriteTestUtils
