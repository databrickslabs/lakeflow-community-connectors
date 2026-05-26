from databricks.labs.community_connector.sources.qualys_vmdr.qualys_vmdr import (
    QualysVmdrLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestQualysVmdrConnector(LakeflowConnectTests):
    connector_class = QualysVmdrLakeflowConnect
    simulator_source = "qualys_vmdr"
    replay_config = {
        "username": "simulator-user",
        "password": "simulator-pass",
        "base_url": "https://qualysapi.qualys.com",
    }
