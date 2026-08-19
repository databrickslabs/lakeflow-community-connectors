"""WordPress source connector."""

from databricks.labs.community_connector.sources.wordpress.wordpress import (
    WordPressLakeflowConnect,
)

from databricks.labs.community_connector.sparkpds import LakeflowSource


class WordPressDataSource(LakeflowSource):
    _lakeflow_connect_cls = WordPressLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "wordpress"


__all__ = [
    "WordPressLakeflowConnect",
    "WordPressDataSource",
]
