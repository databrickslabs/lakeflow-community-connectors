"""Gmail source connector.

Usage with Spark::

    from databricks.labs.community_connector.sources.gmail import GmailDataSource

    spark.dataSource.register(GmailDataSource)
    df = (
        spark.read.format("lakeflow_connect")
        .option("databricks.connection", "<uc_connection_name>")
        .option("operation", "list_operations")
        .load()
    )
"""

from databricks.labs.community_connector.sources.gmail.gmail import GmailLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class GmailDataSource(LakeflowSource):
    _lakeflow_connect_cls = GmailLakeflowConnect


__all__ = ["GmailLakeflowConnect", "GmailDataSource"]
