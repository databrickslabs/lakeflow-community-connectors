from databricks.labs.community_connector.sources.qualys_was.qualys_was import (
    QualysWasLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestQualysWasConnector(LakeflowConnectTests):
    connector_class = QualysWasLakeflowConnect
    simulator_source = "qualys_was"
    replay_config = {
        "username": "simulator-user",
        "password": "simulator-pass",
        "base_url": "https://qualysapi.qualys.com",
    }
