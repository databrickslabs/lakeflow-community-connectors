"""
Microsoft Teams Fully Automated Ingestion Sample

This sample demonstrates the fully automated ingestion mode using fetch_all parameters.
It automatically discovers and ingests all teams, channels, members, messages, and replies
without requiring explicit IDs.

Features:
- Auto-discovery of all teams
- Auto-discovery of all channels per team
- Auto-discovery of all messages per channel (for message_replies)
- Incremental sync for messages/replies (messages use Delta API, replies use client-side filtering)
- Parallel fetching with ThreadPoolExecutor
- No manual configuration of team_id or channel_id needed

Prerequisites:
1. Azure AD App Registration with Application Permissions:
   - Team.ReadBasic.All
   - Channel.ReadBasic.All
   - ChannelMessage.Read.All
   - TeamMember.Read.All
2. Admin consent granted for all permissions
3. Unity Catalog connection created with credentials stored in connection properties

Usage:
1. Create Unity Catalog connection with credentials (see configs/uc_connection_example.sh)
2. Update destination catalog and schema if needed
3. Run this script as a Databricks notebook or pipeline
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# ==============================================================================
# CONFIGURATION
# ==============================================================================
source_name = "microsoft_teams"
connection_name = "microsoft_teams_connection"  # Credentials stored in connection properties

# Destination Configuration
DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "teams_data"
TABLE_PREFIX = "lakeflow_connector_"  # Tables: lakeflow_connector_teams, etc.

# Ingestion Options (credentials come from connection properties)
START_DATE = "2024-12-01T00:00:00Z"  # Start date for incremental sync (messages, message_replies)
LOOKBACK_SECONDS = "3600"            # 1-hour lookback for late-arriving data (no duplicates - deduped by ID)
TOP = "50"                           # Page size for API requests
MAX_PAGES_PER_BATCH = "200"          # Max pages per batch (controls checkpoint frequency)

# ==============================================================================
# SETUP
# ==============================================================================
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# ==============================================================================
# PIPELINE SPECIFICATION - FULLY AUTOMATED MODE
# ==============================================================================
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        # ======================================================================
        # SNAPSHOT TABLES (Full refresh each run)
        # ======================================================================

        # 1. Teams - Auto-discover all teams
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}teams",
                "table_configuration": {
                    "top": TOP
                }
            }
        },

        # 2. Channels - Auto-discover channels for ALL teams
        {
            "table": {
                "source_table": "channels",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}channels",
                "table_configuration": {
                    "fetch_all_teams": "true",  # Auto-discover all teams
                    "top": TOP
                }
            }
        },

        # 3. Members - Auto-discover members for ALL teams
        {
            "table": {
                "source_table": "members",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}members",
                "table_configuration": {
                    "fetch_all_teams": "true",  # Auto-discover all teams
                    "top": TOP
                }
            }
        },

        # ======================================================================
        # CDC TABLES (Incremental sync with change tracking)
        # ======================================================================

        # 4. Messages - Auto-discover messages for ALL teams/channels
        {
            "table": {
                "source_table": "messages",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}messages",
                "table_configuration": {
                    "fetch_all_teams": "true",     # Auto-discover all teams
                    "fetch_all_channels": "true",  # Auto-discover all channels per team
                    "start_date": START_DATE,
                    "lookback_seconds": LOOKBACK_SECONDS,
                    "top": TOP,
                    "max_pages_per_batch": MAX_PAGES_PER_BATCH
                }
            }
        },

        # 5. Message Replies - Auto-discover replies for ALL messages
        {
            "table": {
                "source_table": "message_replies",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}message_replies",
                "table_configuration": {
                    "fetch_all_teams": "true",     # Auto-discover all teams
                    "fetch_all_channels": "true",  # Auto-discover all channels per team
                    "fetch_all_messages": "true",  # Auto-discover all messages per channel
                    "start_date": START_DATE,
                    "lookback_seconds": LOOKBACK_SECONDS,
                    "top": TOP,
                    "max_pages_per_batch": MAX_PAGES_PER_BATCH
                }
            }
        }
    ]
}

# ==============================================================================
# RUN INGESTION
# ==============================================================================
print("=" * 80)
print("Microsoft Teams - Fully Automated Ingestion")
print("=" * 80)
print("\nMode: Automatic Discovery (fetch_all enabled)")
print("\nTables to ingest:")
print("  1. Teams             (snapshot) - all teams in organization")
print("  2. Channels          (snapshot) - all channels in all teams")
print("  3. Members           (snapshot) - all members in all teams")
print("  4. Messages          (CDC)      - all messages in all channels (incremental with Delta API)")
print("  5. Message Replies   (CDC)      - all replies to all messages (incremental with client-side filtering)")
print("\nDestination:")
print(f"  Catalog: {DESTINATION_CATALOG}")
print(f"  Schema:  {DESTINATION_SCHEMA}")
print(f"  Prefix:  {TABLE_PREFIX}")
print("\nStarting ingestion...")
print("=" * 80)
print()

ingest(spark, pipeline_spec)

print()
print("=" * 80)
print("Ingestion Complete")
print("=" * 80)
print("\nTables created:")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}channels")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}members")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}messages")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}message_replies")
print("\nSample queries:")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}messages")
print(f"    WHERE lastModifiedDateTime >= '2024-12-01'")
print("\nNext steps:")
print("  1. Verify data in tables")
print("  2. Set up scheduled refresh for incremental sync")
print("  3. Create analytics queries and dashboards")
print("=" * 80)
