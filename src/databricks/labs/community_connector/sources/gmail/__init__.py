"""Gmail source connector."""

from databricks.labs.community_connector.sources.gmail.gmail import GmailLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class GmailDataSource(LakeflowSource):
    _lakeflow_connect_cls = GmailLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "gmail"


__all__ = ["GmailLakeflowConnect",
    "GmailDataSource",
]
