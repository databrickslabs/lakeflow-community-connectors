from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "google_analytics_aggregated"

# =============================================================================
# GOOGLE ANALYTICS AGGREGATED DATA INGESTION PIPELINE CONFIGURATION
# =============================================================================
#
# IMPORTANT NOTE:
# Each report must have a UNIQUE source_table name to avoid internal view name
# collisions in the ingestion pipeline. Use descriptive names for each report.
#
# =============================================================================
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of custom reports to ingest
#     └── table
#         ├── source_table (required): Unique name for this report (any name is accepted)
#         ├── destination_catalog (optional): Target catalog (defaults to pipeline's default)
#         ├── destination_schema (optional): Target schema (defaults to pipeline's default)
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         └── table_configuration (required): Report definition with dimensions and metrics
#             ├── dimensions (required): JSON array of dimension names, e.g., '["date", "country"]'
#             ├── metrics (required): JSON array of metric names, e.g., '["activeUsers", "sessions"]'
#             └── primary_keys (required): List of dimension names that form the composite key
#                                          Must match all dimensions in your report
#             ├── start_date (optional): Initial date range start (default: "30daysAgo")
#             ├── lookback_days (optional): Days to look back on incremental reads (default: 3)
#             ├── page_size (optional): Records per API request (default: 10000, max: 100000)
#             ├── dimension_filter (optional): JSON filter object for dimensions
#             ├── metric_filter (optional): JSON filter object for metrics
#             ├── scd_type (optional): "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
# =============================================================================

# Define your reports
# Each report needs a UNIQUE source_table name to avoid view name collisions
reports = [
    # Example 1: Daily traffic by country
    {
        "table": {
            "source_table": "traffic_by_country",
            "table_configuration": {
                "dimensions": '["date", "country"]',
                "metrics": '["activeUsers", "sessions", "screenPageViews"]',
                "primary_keys": ["date", "country"],
                "start_date": "30daysAgo",
                "lookback_days": 3,
            },
        }
    },
    # Example 2: User engagement metrics by device category
    {
        "table": {
            "source_table": "engagement_by_device",
            "table_configuration": {
                "dimensions": '["date", "deviceCategory"]',
                "metrics": '["activeUsers", "engagementRate", "averageSessionDuration"]',
                "primary_keys": ["date", "deviceCategory"],
                "start_date": "90daysAgo",
                "lookback_days": 3,
                "page_size": 5000,
            },
        }
    },
    # Example 3: Traffic sources with filters
    {
        "table": {
            "source_table": "web_traffic_sources",
            "table_configuration": {
                "dimensions": '["date", "platform", "browser"]',
                "metrics": '["sessions"]',
                "primary_keys": ["date", "platform", "browser"],
                "start_date": "7daysAgo",
                "lookback_days": 3,
                "dimension_filter": '{"filter": {"fieldName": "platform", "stringFilter": {"matchType": "EXACT", "value": "web"}}}',
            },
        }
    },
    # Example 4: Snapshot report (no date dimension)
    {
        "table": {
            "source_table": "all_time_by_country", 
            "table_configuration": {
                "dimensions": '["country"]',
                "metrics": '["totalUsers", "sessions"]',
                "primary_keys": ["country"],
                "start_date": "2020-01-01",  # Use YYYY-MM-DD format for specific dates
                "scd_type": "SCD_TYPE_1",  # Full refresh each time
            },
        }
    },
]

# Build the final pipeline spec
pipeline_spec = {
    "connection_name": "ga4_test",
    "objects": reports,
}

# =============================================================================
# AVAILABLE DIMENSIONS (Common Examples)
# =============================================================================
# - date: Date in YYYYMMDD format
# - country, city, region: Geographic dimensions
# - deviceCategory, operatingSystem, browser: Device/platform dimensions
# - sessionSource, sessionMedium, sessionCampaignName: Traffic source dimensions
# - pagePath, pageTitle: Content dimensions
# - eventName: Event dimensions
#
# For full list, see: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
# Or use the Metadata API: GET https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}/metadata
# =============================================================================

# =============================================================================
# AVAILABLE METRICS (Common Examples)
# =============================================================================
# - activeUsers, totalUsers: User counts
# - sessions, screenPageViews: Session and page view counts
# - engagementRate, averageSessionDuration: Engagement metrics
# - conversions, eventCount: Event and conversion metrics
# - bounceRate, sessionConversionRate: Conversion rates
#
# For full list, see: https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
# Or use the Metadata API: GET https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}/metadata
# =============================================================================

# Dynamically import and register the LakeFlow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
