"""
Microsoft Teams Ingestion Pipeline with Automatic Discovery (fetch_all modes)

This pipeline ingests Microsoft Teams data using automatic discovery.
No need to manually configure team IDs or channel IDs!

IMPORTANT LIMITATION - Chats Table:
The Microsoft Graph API 'chats' endpoint does NOT support Application Permissions
(tenant-wide access). It only works with Delegated Permissions (interactive user login).
Since this connector uses Application Permissions for automated/scheduled pipelines,
chats ingestion is not supported. You can still ingest: teams, channels, members, messages.

What this pipeline does:
1. Teams: Ingests all teams in your organization
2. Channels: Auto-discovers ALL teams, then ingests all channels
3. Members: Auto-discovers ALL teams, then ingests all members
4. Messages: Auto-discovers ALL channels in specified teams, then ingests all messages

Configuration:
1. Update credentials below
2. Optionally add team IDs to TEAM_IDS if you want to limit messages to specific teams
3. Run the pipeline - everything else is automatic!
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# ==============================================================================
# CONFIGURATION - Update these values
# ==============================================================================
source_name = "microsoft_teams"
connection_name = "microsoft_teams_connection"

# Azure AD credentials - REPLACE THESE
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Destination configuration
DESTINATION_CATALOG = "users"
DESTINATION_SCHEMA = "eduardo_lomonaco"
TABLE_PREFIX = "lakeflow_connector_"

# Ingestion options
INGEST_TEAMS = True           # Ingest all teams
INGEST_CHANNELS = True        # Auto-discover all teams, ingest all channels
INGEST_MEMBERS = True         # Auto-discover all teams, ingest all members
INGEST_MESSAGES = True        # Auto-discover all teams and channels, ingest all messages
INGEST_MESSAGE_REPLIES = True # Auto-discover all teams, channels, and messages, ingest all replies

# Pagination settings
TOP = "50"
MAX_PAGES_PER_BATCH = "100"

# Optional: Limit messages/replies by date (format: "YYYY-MM-DDTHH:MM:SSZ")
# Set to None to ingest all messages/replies
START_DATE = "2025-01-01T00:00:00Z"

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
print("Microsoft Teams Ingestion Pipeline with Auto-Discovery")
print("=" * 80)
print()

# Teams table (always ingests all teams)
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
    print("✓ Will ingest: teams (all teams in organization)")

# Channels - auto-discover ALL teams
if INGEST_CHANNELS:
    pipeline_spec["objects"].append({
        "table": {
            "source_table": "channels",
            "destination_catalog": DESTINATION_CATALOG,
            "destination_schema": DESTINATION_SCHEMA,
            "destination_table": f"{TABLE_PREFIX}channels",
            "table_configuration": {
                **creds,
                "fetch_all_teams": "true"  # ← Auto-discover all teams!
            }
        }
    })
    print("✓ Will ingest: channels (auto-discovering all teams)")

# Members - auto-discover ALL teams
if INGEST_MEMBERS:
    pipeline_spec["objects"].append({
        "table": {
            "source_table": "members",
            "destination_catalog": DESTINATION_CATALOG,
            "destination_schema": DESTINATION_SCHEMA,
            "destination_table": f"{TABLE_PREFIX}members",
            "table_configuration": {
                **creds,
                "fetch_all_teams": "true"  # ← Auto-discover all teams!
            }
        }
    })
    print("✓ Will ingest: members (auto-discovering all teams)")

# Messages - auto-discover ALL channels across ALL teams
if INGEST_MESSAGES:
    config = {
        **creds,
        "fetch_all_teams": "true",      # ← Auto-discover all teams!
        "fetch_all_channels": "true",   # ← Auto-discover all channels in each team!
        "top": TOP,
        "max_pages_per_batch": MAX_PAGES_PER_BATCH
    }

    # Add start_date if specified
    if START_DATE:
        config["start_date"] = START_DATE

    pipeline_spec["objects"].append({
        "table": {
            "source_table": "messages",
            "destination_catalog": DESTINATION_CATALOG,
            "destination_schema": DESTINATION_SCHEMA,
            "destination_table": f"{TABLE_PREFIX}messages",
            "table_configuration": config
        }
    })

    print("✓ Will ingest: messages (auto-discovering all teams and all channels)")
    if START_DATE:
        print(f"  └─ Starting from: {START_DATE}")

# Message Replies - auto-discover ALL teams, channels, and messages
if INGEST_MESSAGE_REPLIES:
    config = {
        **creds,
        "fetch_all_teams": "true",      # ← Auto-discover all teams!
        "fetch_all_channels": "true",   # ← Auto-discover all channels in each team!
        "fetch_all_messages": "true",   # ← Auto-discover all messages in each channel!
        "top": TOP,
        "max_pages_per_batch": MAX_PAGES_PER_BATCH
    }

    # Add start_date if specified
    if START_DATE:
        config["start_date"] = START_DATE

    pipeline_spec["objects"].append({
        "table": {
            "source_table": "message_replies",
            "destination_catalog": DESTINATION_CATALOG,
            "destination_schema": DESTINATION_SCHEMA,
            "destination_table": f"{TABLE_PREFIX}message_replies",
            "table_configuration": config
        }
    })

    print("✓ Will ingest: message_replies (auto-discovering all teams, channels, and messages)")
    if START_DATE:
        print(f"  └─ Starting from: {START_DATE}")

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
print("Key benefits of auto-discovery:")
print("  • No manual team ID configuration required")
print("  • No manual channel ID configuration required")
print("  • Automatically discovers new teams/channels on each run")
print("  • Single pipeline run ingests everything")
print()
print("Next steps:")
print("  1. Query your data:")
print(f"     SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
print(f"     SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}channels")
print(f"     SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}messages")
print()
print("  2. To add more teams for messages:")
print("     - Query teams table to get team IDs")
print("     - Add team IDs to TEAM_IDS list in this file")
print("     - Re-run pipeline")
