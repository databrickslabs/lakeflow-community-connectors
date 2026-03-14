from databricks.labs.community_connector.libs.simulated_source.api import reset_api
from databricks.labs.community_connector.sources.example.example import ExampleLakeflowConnect
from tests.unit.sources.example.example_test_utils import LakeflowConnectWriteTestUtils
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestExampleConnector(LakeflowConnectTests):
    connector_class = ExampleLakeflowConnect
    test_utils_class = LakeflowConnectWriteTestUtils
    sample_records = 100

    @classmethod
    def setup_class(cls):
        # Reset the simulated API before creating the connector so that
        # both self.connector and any fresh instances share the same data.
        cls.config = cls._load_config()
        reset_api(cls.config["username"], cls.config["password"])
        super().setup_class()
