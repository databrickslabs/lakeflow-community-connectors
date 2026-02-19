from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

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
# 1. PREBUILT REPORTS (Simplest)
#    - Use the prebuilt report name as the source_table
#    - No table_configuration needed
#    - Dimensions, metrics, and primary_keys are automatically configured
#    - See "AVAILABLE PREBUILT REPORTS" section below for all 15 reports
#    - Can override any settings (start_date, lookback_days, filters, etc.)
#
# 2. CUSTOM REPORTS (For specific needs)
#    - Use any source_table name you want
#    - Manually specify dimensions, metrics, and primary_keys
#    - Full control over report configuration
#    - Required fields: dimensions, metrics, primary_keys
#    - Primary keys must always start with "property_id" followed by dimensions
#
# =============================================================================
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of reports to ingest (prebuilt or custom)
#     └── table
#         ├── source_table (required): For prebuilt reports, use the report name
#         │                            For custom reports, use any unique name
#         ├── destination_catalog (optional): Target catalog
#         ├── destination_schema (optional): Target schema
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         └── table_configuration (optional for prebuilt, required for custom): Report settings
#
#             FOR PREBUILT REPORTS (Optional - only to override defaults):
#             ├── start_date (optional): Override default "30daysAgo"
#             ├── lookback_days (optional): Override default 3
#             ├── dimension_filter (optional): Add filters
#             ├── metric_filter (optional): Add filters
#             ├── page_size (optional): Override default 10000
#             ├── scd_type (optional): Override default "SCD_TYPE_1"
#
#             FOR CUSTOM REPORTS (Required):
#             ├── dimensions (required): JSON array e.g., '["date", "country"]'
#             ├── metrics (required): JSON array e.g., '["activeUsers", "sessions"]'
#             ├── primary_keys (auto-inferred): Automatically set as ["property_id"] + dimensions
#             │                                 Can be overridden if needed
#             ├── start_date (optional): Initial date range start (default: "30daysAgo")
#             ├── lookback_days (optional): Days to look back (default: 3)
#             ├── page_size (optional): Records per request (default: 10000, max: 100000)
#             ├── dimension_filter (optional): JSON filter object for dimensions
#             ├── metric_filter (optional): JSON filter object for metrics
#             ├── scd_type (optional): "SCD_TYPE_1", "SCD_TYPE_2", or "APPEND_ONLY"
# =============================================================================

# Define your reports (mix of prebuilt and custom)
reports = [
    # Example 1: Prebuilt report - Use report name as source_table (No table_configuration needed)
    {
        "table": {
            "source_table": "traffic_by_country",  # Matches prebuilt report name
            # No table_configuration needed - dimensions, metrics, and primary keys are automatic
            # Optionally override defaults like start_date, lookback_days, filters:
            # "table_configuration": {
            #     "start_date": "90daysAgo",
            # }
        }
    },

    # Example 2: Custom report with engagement metrics
    {
        "table": {
            "source_table": "engagement_by_device",
            "table_configuration": {
                "dimensions": '["date", "deviceCategory"]',
                "metrics": '["activeUsers", "engagementRate", "averageSessionDuration"]',
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
                "start_date": "7daysAgo",
                "lookback_days": "3",
                # Filter to only include web traffic
                "dimension_filter": '{"filter": {"fieldName": "platform", '
                                    '"stringFilter": {"matchType": "EXACT", "value": "web"}}}',
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
# AVAILABLE PREBUILT REPORTS (15 reports)
# =============================================================================
# Traffic & Acquisition:
# - traffic_by_country: Daily active users, sessions, and page views by country
# - user_acquisition: Daily traffic sources and campaign performance
# - landing_page_performance: Performance of the first page users land on
#
# Engagement:
# - events_summary: Daily event breakdown by event name
# - page_performance: Daily page views by page path and title
# - device_breakdown: Daily users by device category and browser
# - tech_details: Detailed breakdown of browsers and operating systems
#
# Ecommerce:
# - ecommerce_purchases: Item-level performance for ecommerce sales
# - purchase_journey: User activity across shopping stages
# - checkout_journey: Daily active users at specific checkout steps
# - promotions_performance: Internal promotion clicks and views
#
# Users:
# - user_retention: Comparison of new vs returning users
# - demographic_details: User breakdown by language and location
# - audience_performance: Performance metrics for defined audiences
#
# Advertising:
# - advertising_channels: Performance by channel group, ad cost, and ROAS
#
# To use a prebuilt report, just use its name as the source_table
# No need to specify dimensions, metrics, or primary keys - it's all automatic.
#
# RESERVED NAMES: Prebuilt report names are "reserved" to enable zero-config usage.
# If you need a custom report with a prebuilt name, provide explicit "dimensions"
# in table_configuration to override (though a different name is recommended).
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
# Or use the Metadata API:
#   GET https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}/metadata
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
# For full list, see the GA4 API schema documentation.
# Or use the Metadata API:
#   GET https://analyticsdata.googleapis.com/v1beta/properties/{propertyId}/metadata
# =============================================================================

# Dynamically import and register the LakeFlow source
register(spark, source_name)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
