"""Notion source connector."""

from databricks.labs.community_connector.sources.notion.notion import NotionLakeflowConnect

from databricks.labs.community_connector.sparkpds import LakeflowSource


class NotionDataSource(LakeflowSource):
    _lakeflow_connect_cls = NotionLakeflowConnect


__all__ = ["NotionLakeflowConnect", "NotionDataSource"]