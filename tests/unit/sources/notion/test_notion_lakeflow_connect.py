from databricks.labs.community_connector.sources.notion.notion import NotionLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestNotionConnector(LakeflowConnectTests):
    connector_class = NotionLakeflowConnect
    sample_records = 50
