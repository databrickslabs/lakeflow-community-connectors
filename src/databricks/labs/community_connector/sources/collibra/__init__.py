"""Collibra Data Intelligence Platform source connector."""

from databricks.labs.community_connector.sources.collibra.collibra import (
    CollibraLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class CollibraDataSource(LakeflowSource):
    _lakeflow_connect_cls = CollibraLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "collibra"


__all__ = [
    "CollibraLakeflowConnect",
    "CollibraDataSource",
]
