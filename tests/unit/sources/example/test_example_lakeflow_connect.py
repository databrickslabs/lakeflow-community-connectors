from databricks.labs.community_connector.libs.simulated_source.api import reset_api
from databricks.labs.community_connector.sources.example.example import ExampleLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestExampleConnector(LakeflowConnectTests):
    connector_class = ExampleLakeflowConnect
    sample_records = 100

    @classmethod
    def setup_class(cls):
        super().setup_class()
        reset_api(cls.config["username"], cls.config["password"])
