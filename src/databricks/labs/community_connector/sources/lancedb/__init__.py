"""LanceDB Cloud source connector."""

from databricks.labs.community_connector.sources.lancedb.lancedb import (
    LancedbLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class LancedbDataSource(LakeflowSource):
    _lakeflow_connect_cls = LancedbLakeflowConnect


__all__ = [
    "LancedbLakeflowConnect",
    "LancedbDataSource",
]
