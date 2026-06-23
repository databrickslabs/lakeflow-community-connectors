"""Tabs Platform billing API source connector."""

from databricks.labs.community_connector.sources.tabs.tabs import (
    TabsLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class TabsDataSource(LakeflowSource):
    _lakeflow_connect_cls = TabsLakeflowConnect


__all__ = [
    "TabsLakeflowConnect",
    "TabsDataSource",
]
