"""OData v4 community connector package — re-exports
``ODataLakeflowConnect`` so callers can ``from
databricks.labs.community_connector.sources.odata import
ODataLakeflowConnect`` without reaching into the implementation module."""

from databricks.labs.community_connector.sources.odata.odata import (
    ODataLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class ODataDataSource(LakeflowSource):
    _lakeflow_connect_cls = ODataLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "odata"


__all__ = ["ODataLakeflowConnect", "ODataDataSource"]
