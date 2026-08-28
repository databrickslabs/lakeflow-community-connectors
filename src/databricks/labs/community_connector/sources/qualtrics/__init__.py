from databricks.labs.community_connector.sources.qualtrics.qualtrics import (
    QualtricsLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class QualtricsDataSource(LakeflowSource):
    _lakeflow_connect_cls = QualtricsLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "qualtrics"


__all__ = ["QualtricsLakeflowConnect", "QualtricsDataSource"]
