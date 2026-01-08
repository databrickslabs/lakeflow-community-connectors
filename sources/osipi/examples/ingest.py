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
   - Authentication: OIDC client credentials (recommended), Bearer token, or Basic auth
   - Must include 'tableConfigs' in externalOptionsAllowList (required for table_configuration options)
     Use: python3 tools/add_tableconfigs_to_connection.py <connection_name> --profile <profile>
2. Destination catalog and schema configured
3. Network connectivity to PI Web API from Databricks

For load-balanced production deployments:
See tools/load_balanced_deployment/notebooks/00_load_balanced_deployment_all_in_one.py
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

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

        # PI Points (Tags)
        {
            "table": {
                "source_table": "pi_points",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
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

        # Recorded Time Series Values
        {
            "table": {
                "source_table": "pi_timeseries",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "table_configuration": {
                    # Add your tag WebIDs here
                    # "tag_webids": "YOUR_TAG_WEBID_1,YOUR_TAG_WEBID_2",
                    # "startTime": "*-24h",  # Last 24 hours
                    # "endTime": "*",        # Current time
                },
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
                "table_configuration": {
                    # "startTime": "2024-01-01T00:00:00Z",
                    # "endTime": "*",
                },
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
register_lakeflow_source = get_register_function(SOURCE_NAME)
register_lakeflow_source(spark)

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
