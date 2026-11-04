"""Register the Bluesky connector with Lakeflow's Spark data source API."""

from databricks.labs.community_connector.sources.bluesky.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class BlueskyDataSource(LakeflowSource):
    """Register the Bluesky connector with Lakeflow's Spark data source API.

    Attributes
    ----------
    _lakeflow_connect_cls : type
        Connector instantiated from Lakeflow's option mapping. Its constructor
        validates the mapping before any planning or archive read.
    """

    _lakeflow_connect_cls = BlueskyLakeflowConnect
