"""Google Analytics Aggregated Data source connector."""

from databricks.labs.community_connector.sources.\
    google_analytics_aggregated.google_analytics_aggregated import (
        GoogleAnalyticsAggregatedLakeflowConnect,
    )

__all__ = ["GoogleAnalyticsAggregatedLakeflowConnect"]
