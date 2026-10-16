"""QuickBooks Online source connector."""

from databricks.labs.community_connector.sources.quickbooks.quickbooks import (
    QuickBooksLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class QuickBooksDataSource(LakeflowSource):
    """Spark Python Data Source wrapper for QuickBooks Online."""

    _lakeflow_connect_cls = QuickBooksLakeflowConnect


__all__ = ["QuickBooksDataSource", "QuickBooksLakeflowConnect"]
