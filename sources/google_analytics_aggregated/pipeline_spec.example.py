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
# TWO WAYS TO DEFINE REPORTS:
#
# 1. PREBUILT REPORTS (Recommended for common use cases)
#    - Use predefined report configurations from prebuilt_reports.json
#    - Simply specify: "prebuilt_report": "report_name"
#    - Available reports: traffic_by_country, engagement_by_device, 
#                         traffic_sources, page_performance, user_demographics
#    - Can override any settings (start_date, lookback_days, filters, etc.)
#
# 2. CUSTOM REPORTS (For specific needs)
#    - Manually specify dimensions, metrics, and primary_keys
#    - Full control over report configuration
#    - Required fields: dimensions, metrics, primary_keys
#
# =============================================================================
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of reports to ingest (prebuilt or custom)
#     └── table
#         ├── source_table (required): Unique name for this report
#         ├── destination_catalog (optional): Target catalog
#         ├── destination_schema (optional): Target schema
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         └── table_configuration (required): Report definition
#             
#             FOR PREBUILT REPORTS:
#             ├── prebuilt_report (required): Name of prebuilt report
#             ├── start_date (optional): Override default "30daysAgo"
#             ├── lookback_days (optional): Override default 3
#             ├── dimension_filter (optional): Add filters
#             ├── metric_filter (optional): Add filters
#             ├── page_size (optional): Override default 10000
#             ├── scd_type (optional): Override default "SCD_TYPE_1"
#             
#             FOR CUSTOM REPORTS:
#             ├── dimensions (required): JSON array e.g., '["date", "country"]'
#             ├── metrics (required): JSON array e.g., '["activeUsers", "sessions"]'
#             ├── primary_keys (required): List matching dimensions
#                                          TODO: This is redundant but required due to
#                                          architectural limitation. See connector code.
#             ├── start_date (optional): Initial date range start (default: "30daysAgo")
#             ├── lookback_days (optional): Days to look back (default: 3)
#             ├── page_size (optional): Records per request (default: 10000, max: 100000)
#             ├── dimension_filter (optional): JSON filter object for dimensions
#             ├── metric_filter (optional): JSON filter object for metrics
#             ├── scd_type (optional): "SCD_TYPE_1", "SCD_TYPE_2", or "APPEND_ONLY"
# =============================================================================

# Define your reports (mix of prebuilt and custom)
reports = [
    # Example 1: Prebuilt report (simplest approach)
    {
        "table": {
            "source_table": "traffic_by_country",
            "table_configuration": {
                "prebuilt_report": "traffic_by_country"
            },
        }
    },
    
    # Example 2: Custom report with engagement metrics
    {
        "table": {
            "source_table": "engagement_by_device",
            "table_configuration": {
                "dimensions": '["date", "deviceCategory"]',
                "metrics": '["activeUsers", "engagementRate", "averageSessionDuration"]',
                "primary_keys": ["date", "deviceCategory"],
                "start_date": "90daysAgo",
                "lookback_days": "3",
                "page_size": "5000",
            },
        }
    },
    
    # Example 3: Custom report with filters
    {
        "table": {
            "source_table": "web_traffic_sources",
            "table_configuration": {
                "dimensions": '["date", "platform", "browser"]',
                "metrics": '["sessions"]',
                "primary_keys": ["date", "platform", "browser"],
                "start_date": "7daysAgo",
                "lookback_days": "3",
                "dimension_filter": '{"filter": {"fieldName": "platform", "stringFilter": {"matchType": "EXACT", "value": "web"}}}',
            },
        }
    },
    
    # Example 4: Custom snapshot report (no date dimension)
    {
        "table": {
            "source_table": "all_time_by_country", 
            "table_configuration": {
                "dimensions": '["country"]',
                "metrics": '["totalUsers", "sessions"]',
                "primary_keys": ["country"],
                "start_date": "2020-01-01",
                "scd_type": "SCD_TYPE_1",
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
# AVAILABLE PREBUILT REPORTS
# =============================================================================
# - traffic_by_country: Daily active users, sessions, and page views by country
#
# To use a prebuilt report, just specify: "prebuilt_report": "report_name"
# You can override any defaults (start_date, lookback_days, filters, etc.)
#
# More prebuilt reports can be added to prebuilt_reports.json as needed.
# =============================================================================

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
