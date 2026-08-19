"""Xquik source connector."""

from databricks.labs.community_connector.sources.xquik.xquik import XquikLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class XquikDataSource(LakeflowSource):
    """Register Xquik through the shared Lakeflow source format."""

    _lakeflow_connect_cls = XquikLakeflowConnect


__all__ = ["XquikLakeflowConnect", "XquikDataSource"]
