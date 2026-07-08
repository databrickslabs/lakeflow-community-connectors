"""Zendesk source connector."""

from databricks.labs.community_connector.sources.zendesk.zendesk import ZendeskLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class ZendeskDataSource(LakeflowSource):
    _lakeflow_connect_cls = ZendeskLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "zendesk"


__all__ = ["ZendeskLakeflowConnect",
    "ZendeskDataSource",
]
