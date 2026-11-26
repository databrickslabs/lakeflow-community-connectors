"""Google Campaign Manager 360 source connector."""

from databricks.labs.community_connector.sources.google_cm360.google_cm360 import (
    GoogleCm360LakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class GoogleCm360DataSource(LakeflowSource):
    """Spark Python data source backed by the CM360 connector."""

    _lakeflow_connect_cls = GoogleCm360LakeflowConnect


__all__ = ["GoogleCm360LakeflowConnect", "GoogleCm360DataSource"]
