"""Rootly source connector."""

from databricks.labs.community_connector.sources.rootly.rootly import RootlyLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class RootlyDataSource(LakeflowSource):
    _lakeflow_connect_cls = RootlyLakeflowConnect


__all__ = ["RootlyLakeflowConnect", "RootlyDataSource"]
