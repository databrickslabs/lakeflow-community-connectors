"""Expose the Bluesky connector classes for Lakeflow source discovery."""

from databricks.labs.community_connector.sources.bluesky.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class BlueskyDataSource(LakeflowSource):
    """Register the Bluesky connector with Lakeflow's Spark data source API."""

    _lakeflow_connect_cls = BlueskyLakeflowConnect


__all__ = ["BlueskyDataSource", "BlueskyLakeflowConnect"]
