"""AppsFlyer source connector."""

from databricks.labs.community_connector.sources.appsflyer.appsflyer import AppsflyerLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class AppsflyerDataSource(LakeflowSource):
    _lakeflow_connect_cls = AppsflyerLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "appsflyer"


__all__ = ["AppsflyerLakeflowConnect",
    "AppsflyerDataSource",
]
