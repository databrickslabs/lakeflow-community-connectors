from databricks.labs.community_connector.sources.snyk.snyk_mock_api import reset_mock_api
from databricks.labs.community_connector.sources.snyk.snyk import SnykLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSnykConnector(LakeflowConnectTests):
    connector_class = SnykLakeflowConnect
    sample_records = 50

    @classmethod
    def setup_class(cls):
        cls.config = cls._load_config()
        reset_mock_api()
        super().setup_class()
