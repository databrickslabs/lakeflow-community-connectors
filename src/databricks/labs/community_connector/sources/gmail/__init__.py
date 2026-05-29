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
    # Keep the legacy format name so notebooks/pipelines that hardcode
    # ``spark.read.format("lakeflow_connect")`` (matching the merged-file
    # deployment from ``_generated_gmail_python_source.py``) continue to
    # work when the gmail wheel is installed directly.
    _format_name = "lakeflow_connect"


__all__ = ["GmailLakeflowConnect", "GmailDataSource"]
