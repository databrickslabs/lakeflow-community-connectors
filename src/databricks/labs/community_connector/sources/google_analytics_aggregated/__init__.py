"""Google Analytics Aggregated Data source connector."""

from databricks.labs.community_connector.sources.\
    google_analytics_aggregated.google_analytics_aggregated import (
        GoogleAnalyticsAggregatedLakeflowConnect,
    )


from databricks.labs.community_connector.sparkpds import LakeflowSource


class GoogleAnalyticsAggregatedDataSource(LakeflowSource):
    _lakeflow_connect_cls = GoogleAnalyticsAggregatedLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "google_analytics_aggregated"


__all__ = ["GoogleAnalyticsAggregatedLakeflowConnect",
    "GoogleAnalyticsAggregatedDataSource",
]
