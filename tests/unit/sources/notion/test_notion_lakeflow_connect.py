"""Unit tests for the Notion Lakeflow Connect connector."""

from databricks.labs.community_connector.sources.notion.notion import NotionLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestNotionConnector(LakeflowConnectTests):
    """Test suite for the Notion connector."""

    connector_class = NotionLakeflowConnect
    simulator_source = "notion"
    replay_config = {
        "api_token": "simulator-fake-token",
    }