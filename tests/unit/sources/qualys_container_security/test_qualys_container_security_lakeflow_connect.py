from databricks.labs.community_connector.sources.qualys_container_security.qualys_container_security import (
    QualysContainerSecurityLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestQualysContainerSecurityConnector(LakeflowConnectTests):
    connector_class = QualysContainerSecurityLakeflowConnect
    simulator_source = "qualys_container_security"
    replay_config = {
        "token": "simulator-bearer-token",
        "base_url": "https://qualysapi.qualys.com",
    }
