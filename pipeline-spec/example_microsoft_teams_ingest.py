"""
Microsoft Teams Ingestion Pipeline with Configuration

This pipeline ingests Microsoft Teams data based on your configuration below.
You can configure which teams and channels to ingest by listing their IDs.

How to get team IDs and channel IDs:
1. Run the simple template first to ingest teams table
2. Query: SELECT id, displayName FROM main.teams_data.lakeflow_connector_teams
3. Copy the team IDs you want and add them to TEAM_IDS list below
4. Run this pipeline to get channels for those teams
5. Query channels table to get channel IDs if you want specific channels only

Configuration:
1. Update credentials below
2. Add team IDs to TEAM_IDS list (or set to [] to ingest all tables for ALL teams)
3. Optionally add specific channel IDs to CHANNEL_IDS
4. Run the pipeline
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# ==============================================================================
# CONFIGURATION - Update these values
# ==============================================================================
source_name = "microsoft_teams"
connection_name = "microsoft_teams_connection"

# Azure AD credentials
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Destination configuration
DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "teams_data"
TABLE_PREFIX = "lakeflow_connector_"

# Teams to ingest - ADD YOUR TEAM IDs HERE
# Example: ["abc-123-guid", "def-456-guid"]
# Leave empty [] to just ingest teams and chats tables
TEAM_IDS = []

# Optional: Specific channels to ingest messages from
# Format: [{"team_id": "abc-123", "channel_id": "xyz-789"}, ...]
# Leave empty [] to ingest messages from ALL channels in TEAM_IDS
CHANNEL_IDS = []

# Ingestion options
INGEST_TEAMS = True  # Ingest teams table
INGEST_CHATS = True  # Ingest chats table
INGEST_CHANNELS = True  # Ingest channels for teams in TEAM_IDS
INGEST_MEMBERS = True  # Ingest members for teams in TEAM_IDS
INGEST_MESSAGES = True  # Ingest messages from channels

TOP = "50"
MAX_PAGES_PER_BATCH = "10"

# ==============================================================================
# DO NOT EDIT BELOW THIS LINE
# ==============================================================================

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Build pipeline specification
pipeline_spec = {
    "connection_name": connection_name,
    "objects": []
}

# Common credentials
creds = {
    "tenant_id": TENANT_ID,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

print("=" * 80)
print("Microsoft Teams Ingestion Pipeline")
print("=" * 80)
print()

# Teams table
if INGEST_TEAMS:
    pipeline_spec["objects"].append({
        "table": {
            "source_table": "teams",
            "destination_catalog": DESTINATION_CATALOG,
            "destination_schema": DESTINATION_SCHEMA,
            "destination_table": f"{TABLE_PREFIX}teams",
            "table_configuration": creds.copy()
        }
    })
    print("✓ Will ingest: teams")

# Chats table
if INGEST_CHATS:
    pipeline_spec["objects"].append({
        "table": {
            "source_table": "chats",
            "destination_catalog": DESTINATION_CATALOG,
            "destination_schema": DESTINATION_SCHEMA,
            "destination_table": f"{TABLE_PREFIX}chats",
            "table_configuration": {
                **creds,
                "top": TOP,
                "max_pages_per_batch": MAX_PAGES_PER_BATCH
            }
        }
    })
    print("✓ Will ingest: chats")

# For each team ID
for team_id in TEAM_IDS:
    # Channels
    if INGEST_CHANNELS:
        pipeline_spec["objects"].append({
            "table": {
                "source_table": "channels",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}channels",
                "table_configuration": {
                    **creds,
                    "team_id": team_id
                }
            }
        })

    # Members
    if INGEST_MEMBERS:
        pipeline_spec["objects"].append({
            "table": {
                "source_table": "members",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}members",
                "table_configuration": {
                    **creds,
                    "team_id": team_id
                }
            }
        })

if TEAM_IDS:
    print(f"✓ Will ingest channels/members for {len(TEAM_IDS)} team(s)")

# Messages
if INGEST_MESSAGES and CHANNEL_IDS:
    for channel_config in CHANNEL_IDS:
        pipeline_spec["objects"].append({
            "table": {
                "source_table": "messages",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}messages",
                "table_configuration": {
                    **creds,
                    "team_id": channel_config["team_id"],
                    "channel_id": channel_config["channel_id"],
                    "top": TOP,
                    "max_pages_per_batch": MAX_PAGES_PER_BATCH
                }
            }
        })
    print(f"✓ Will ingest messages from {len(CHANNEL_IDS)} channel(s)")

print()
print("=" * 80)
print("Starting ingestion...")
print("=" * 80)
print()

# Run ingestion
ingest(spark, pipeline_spec)

print()
print("=" * 80)
print("✓ INGESTION COMPLETE!")
print("=" * 80)
print()
print(f"Data ingested to: {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}")
print()
print("Tables created/updated:")
tables = set()
for obj in pipeline_spec["objects"]:
    tables.add(obj["table"]["destination_table"])
for table in sorted(tables):
    print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{table}")
print()
print("Next steps:")
if not TEAM_IDS:
    print("  1. Query teams table to get team IDs:")
    print(f"     SELECT id, displayName FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
    print("  2. Add team IDs to TEAM_IDS list in this file")
    print("  3. Re-run pipeline to ingest channels/members/messages")
elif INGEST_CHANNELS and not CHANNEL_IDS and INGEST_MESSAGES:
    print("  1. Query channels table to get channel IDs:")
    print(f"     SELECT team_id, id, displayName FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}channels")
    print("  2. Add channel IDs to CHANNEL_IDS list in this file")
    print("  3. Re-run pipeline to ingest messages")
