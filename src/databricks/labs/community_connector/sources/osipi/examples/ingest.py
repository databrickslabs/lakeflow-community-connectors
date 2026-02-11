# Databricks notebook source
"""
OSI PI Single-Pipeline Ingestion Sample

This sample demonstrates ingesting key OSI PI tables into a single pipeline.
For production deployments with many tables, use the load-balanced deployment
notebook instead (tools/load_balanced_deployment/notebooks/00_load_balanced_deployment_all_in_one.py).

Features:
- Discovery & inventory tables (data servers, points, attributes)
- Time-series data with incremental sync
- Asset Framework hierarchy and metadata
- Event Frames for batch/campaign tracking

Prerequisites:
1. Unity Catalog connection created with PI Web API credentials:
   - pi_base_url: PI Web API endpoint
   - access_token: Bearer token for authentication
   - Authentication: OIDC client credentials (recommended), Bearer token, or Basic auth
   - Must include all table_configuration options in externalOptionsAllowList:
     'tableConfigs,maxCount,startIndex,startTime,endTime,tag_webids,tag_name_filter,selectedFields'
   - Use: python3 tools/add_tableconfigs_to_connection.py <connection_name> --profile <profile>
2. Destination catalog and schema configured
3. Network connectivity to PI Web API from Databricks

For load-balanced production deployments:
See tools/load_balanced_deployment/notebooks/00_load_balanced_deployment_all_in_one.py

Table Configuration Options:
----------------------------
table_configuration allows you to pass table-specific options to control data ingestion:

Discovery Tables (pi_points, pi_dataservers, etc.):
  - maxCount: Limit number of records (e.g., "50")
  - startIndex: Starting index for pagination (e.g., "0")

Time-Series Tables (pi_timeseries, pi_interpolated, pi_plot, etc.):
  - startTime: Start time for data retrieval (PI time expressions)
      Examples: "*-1h" (last hour), "*-24h" (last 24 hours), "2024-01-01T00:00:00Z"
  - endTime: End time for data retrieval (default: "*" = current time)
  - maxCount: Max records per API call (default: 1000)
      IMPORTANT: Automatic pagination is now supported! The connector will:
      - Fetch data in batches of maxCount records per tag
      - Automatically paginate using time-based cursors
      - Continue until all data in the time range is retrieved
      - Work with pi_timeseries, pi_interpolated, and pi_plot tables
      Example: maxCount=100 will fetch 100 records per page per tag
  - tag_webids: Comma-separated list of tag WebIDs to ingest
  - tag_name_filter: Wildcard pattern to filter tags (e.g., "Temperature*")

Event Frame Tables (pi_event_frames):
  - startTime: Filter event frames by start time
  - endTime: Filter event frames by end time
  - maxCount: Limit number of event frames

Example with table_configuration:
{
    "table": {
        "source_table": "pi_timeseries",
        "destination_catalog": "osipi",
        "destination_schema": "bronze",
        "table_configuration": {
            "startTime": "*-1h",
            "endTime": "*",
            "maxCount": "100"
        }
    }
}
"""

from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Connector Configuration
SOURCE_NAME = "osipi"
CONNECTION_NAME = "osipi_connection_lakeflow"  # UC Connection with PI Web API credentials

# Destination Configuration
DESTINATION_CATALOG = "osipi"
DESTINATION_SCHEMA = "bronze"

# ==============================================================================
# PIPELINE SPECIFICATION
# ==============================================================================

pipeline_spec = {
    "connection_name": CONNECTION_NAME,
    "objects": [
        # ======================================================================
        # DISCOVERY & INVENTORY (Snapshot tables - full refresh)
        # ======================================================================

        # PI Data Servers
        {
            "table": {
                "source_table": "pi_dataservers",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
            }
        },

        # PI Points (Tags) - with pagination example
        {
            "table": {
                "source_table": "pi_points",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                # Uncomment table_configuration below to limit data for testing
                # "table_configuration": {
                #     "maxCount": "50",      # Limit to 50 points
                #     "startIndex": "0",     # Start from beginning
                # },
            }
        },

        # Point Attributes (Metadata for each point)
        {
            "table": {
                "source_table": "pi_point_attributes",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
            }
        },

        # ======================================================================
        # TIME SERIES DATA (Append tables - incremental sync)
        # ======================================================================

        # Recorded Time Series Values - with time range and pagination
        {
            "table": {
                "source_table": "pi_timeseries",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                # Uncomment table_configuration below to test with time range and pagination
                # "table_configuration": {
                #     "startTime": "*-1h",    # Last 1 hour (change to *-24h for 24 hours, *-7d for 7 days)
                #     "endTime": "*",         # Current time
                #     "maxCount": "100",      # Max 100 events per tag per API call
                #                             # Pagination example: If a tag has 500 events in the time range:
                #                             # - API call 1: Fetches events 1-100
                #                             # - API call 2: Fetches events 101-200
                #                             # - API call 3: Fetches events 201-300
                #                             # - API call 4: Fetches events 301-400
                #                             # - API call 5: Fetches events 401-500
                #                             # Result: All 500 events ingested automatically!
                # },
            }
        },

        # Current Values (Latest value for each tag)
        {
            "table": {
                "source_table": "pi_current_value",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
            }
        },

        # ======================================================================
        # ASSET FRAMEWORK (Snapshot tables - full refresh)
        # ======================================================================

        # Asset Servers
        {
            "table": {
                "source_table": "pi_assetservers",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
            }
        },

        # Asset Databases
        {
            "table": {
                "source_table": "pi_assetdatabases",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
            }
        },

        # Asset Hierarchy
        {
            "table": {
                "source_table": "pi_af_hierarchy",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
            }
        },

        # Element Attributes
        {
            "table": {
                "source_table": "pi_element_attributes",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
            }
        },

        # ======================================================================
        # EVENT FRAMES (Append tables - incremental sync)
        # ======================================================================

        # Event Frames (Batches/Campaigns)
        {
            "table": {
                "source_table": "pi_event_frames",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                # Uncomment table_configuration below to filter by time range
                # "table_configuration": {
                #     "startTime": "*-7d",   # Last 7 days (or use absolute: "2024-01-01T00:00:00Z")
                #     "endTime": "*",        # Current time
                # },
            }
        },
    ],
}

# ==============================================================================
# SETUP AND EXECUTION
# ==============================================================================

print("=" * 80)
print("OSI PI Ingestion Pipeline")
print("=" * 80)
print(f"\nTables to ingest: {len(pipeline_spec['objects'])}")
print(f"\nDestination:")
print(f"  Catalog: {DESTINATION_CATALOG}")
print(f"  Schema:  {DESTINATION_SCHEMA}")
print(f"\nTable Categories:")
print(f"  • Discovery & Inventory (4 tables)")
print(f"  • Time Series Data (2 tables)")
print(f"  • Asset Framework (4 tables)")
print(f"  • Event Frames (1 table)")
print(f"\nStarting ingestion...")
print("=" * 80)
print()

# Dynamically import and register the LakeFlow source
register(spark, SOURCE_NAME)

# Execute ingestion
ingest(spark, pipeline_spec)

print()
print("=" * 80)
print("Ingestion Complete")
print("=" * 80)
print(f"\nTables created in {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}:")
print(f"  ✓ pi_dataservers")
print(f"  ✓ pi_points")
print(f"  ✓ pi_point_attributes")
print(f"  ✓ pi_timeseries")
print(f"  ✓ pi_current_value")
print(f"  ✓ pi_assetservers")
print(f"  ✓ pi_assetdatabases")
print(f"  ✓ pi_af_hierarchy")
print(f"  ✓ pi_element_attributes")
print(f"  ✓ pi_event_frames")
print(f"\nSample queries:")
print(f"  -- View all PI data servers")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.pi_dataservers")
print(f"\n  -- View all PI points (tags)")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.pi_points LIMIT 10")
print(f"\n  -- View asset hierarchy")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.pi_af_hierarchy")
print(f"\nNext steps:")
print(f"  1. Verify data in tables above")
print(f"  2. Configure tag_webids for pi_timeseries table to ingest actual time-series data")
print(f"  3. For production deployments with all 39 tables, use:")
print(f"     tools/load_balanced_deployment/notebooks/00_load_balanced_deployment_all_in_one.py")
print("=" * 80)
