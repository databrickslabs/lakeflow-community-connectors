"""actiTIME source connector."""

from databricks.labs.community_connector.sources.actitime.actitime import (
    ActitimeLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class ActitimeDataSource(LakeflowSource):
    _lakeflow_connect_cls = ActitimeLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "actitime"


__all__ = ["ActitimeLakeflowConnect",
    "ActitimeDataSource",
]
