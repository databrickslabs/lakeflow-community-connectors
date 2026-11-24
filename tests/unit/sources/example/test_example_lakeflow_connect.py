from databricks.labs.community_connector.libs.simulated_source.api import reset_api
from databricks.labs.community_connector.sources.example.example import ExampleLakeflowConnect
from tests.unit.sources.example.example_test_utils import LakeflowConnectWriteTestUtils
from tests.unit.sources.test_suite import LakeflowConnectTests
from tests.unit.sources.test_write_back_suite import LakeflowConnectWriteBackTests


class TestExampleConnector(LakeflowConnectWriteBackTests, LakeflowConnectTests):
    connector_class = ExampleLakeflowConnect
    test_utils_class = LakeflowConnectWriteTestUtils
    sample_records = 100
    # The example connector wraps an in-process simulated source — no real
    # creds required at any point. These two values key the in-memory
    # API store seeded by ``reset_api`` below.
    replay_config = {
        "username": "simulator-user",
        "password": "simulator-fake-password",
    }

    @classmethod
    def setup_class(cls):
        # Reset the simulated API before creating the connector so that
        # both self.connector and any fresh instances share the same data.
        cls.config = cls._load_config()
        reset_api(cls.config["username"], cls.config["password"])
        super().setup_class()
