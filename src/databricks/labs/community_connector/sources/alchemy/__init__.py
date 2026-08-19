"""Alchemy source connector."""

from databricks.labs.community_connector.sources.alchemy.alchemy import (
    AlchemyLakeflowConnect,
)

from databricks.labs.community_connector.sparkpds import LakeflowSource


class AlchemyDataSource(LakeflowSource):
    _lakeflow_connect_cls = AlchemyLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "alchemy"


__all__ = [
    "AlchemyLakeflowConnect",
    "AlchemyDataSource",
]
