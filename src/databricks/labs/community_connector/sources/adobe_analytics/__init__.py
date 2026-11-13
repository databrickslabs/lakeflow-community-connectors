"""Adobe Analytics source connector."""

from databricks.labs.community_connector.sources.adobe_analytics.adobe_analytics import (
    AdobeAnalyticsLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class AdobeAnalyticsDataSource(LakeflowSource):
    _lakeflow_connect_cls = AdobeAnalyticsLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "adobe_analytics"


__all__ = [
    "AdobeAnalyticsLakeflowConnect",
    "AdobeAnalyticsDataSource",
]
