"""Google Workspace Reports API User Activity connector."""

from databricks.labs.community_connector.sources.google_user_activity.google_user_activity import (
    GoogleUserActivityLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class GoogleUserActivityDataSource(LakeflowSource):
    """Google User Activity connector implementing LakeflowSource."""

    _lakeflow_connect_cls = GoogleUserActivityLakeflowConnect


__all__ = [
    "GoogleUserActivityLakeflowConnect",
    "GoogleUserActivityDataSource",
]
