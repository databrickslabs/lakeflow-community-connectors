"""OneStream source connector."""

from databricks.labs.community_connector.sources.onestream.onestream import OneStreamLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class OneStreamDataSource(LakeflowSource):
    _lakeflow_connect_cls = OneStreamLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "onestream"


__all__ = [
    "OneStreamLakeflowConnect",
    "OneStreamDataSource",
]
