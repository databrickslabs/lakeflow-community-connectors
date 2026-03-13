from databricks.labs.community_connector.sources.osipi.osipi import OsipiLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestOsipiConnector(LakeflowConnectTests):
    connector_class = OsipiLakeflowConnect
