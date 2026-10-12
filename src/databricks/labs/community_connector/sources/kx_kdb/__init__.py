"""KX KDB source connector."""

from databricks.labs.community_connector.sources.kx_kdb.kx_kdb import KxKdbLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class KxKdbDataSource(LakeflowSource):
    """Spark data source binding for the KX KDB connector."""

    _lakeflow_connect_cls = KxKdbLakeflowConnect


__all__ = ["KxKdbDataSource", "KxKdbLakeflowConnect"]
