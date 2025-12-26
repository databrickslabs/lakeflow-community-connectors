"""
Microsoft Teams Simple Ingestion Pipeline

This is a simplified version that ingests teams and chats only (no channels/messages).
Use this when you only need basic team and chat data.

For full dynamic ingestion across all teams/channels/messages, use:
  example_microsoft_teams_ingest.py

This simple version:
- Ingests teams table (all teams)
- Ingests chats table (all chats)
- No discovery of channels or messages
- Useful for quick setup or when you only need team/chat metadata

Configuration:
1. Update the credentials below
2. Update the destination catalog and schema
3. Run the pipeline
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# ==============================================================================
# CONFIGURATION
# ==============================================================================
source_name = "microsoft_teams"
connection_name = "microsoft_teams_connection"

# Azure AD credentials
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Destination
DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "teams_data"
TABLE_PREFIX = "lakeflow_connector_"  # Prefix for all tables (e.g., "lakeflow_connector_teams")

# Options
TOP = "50"
MAX_PAGES_PER_BATCH = "10"

# ==============================================================================
# SETUP
# ==============================================================================
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# ==============================================================================
# PIPELINE SPECIFICATION
# ==============================================================================

# Ingest teams and chats (no parent IDs required)
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        # Teams table
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}teams",
                "table_configuration": {
                    "tenant_id": TENANT_ID,
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET
                }
            }
        },
        # Chats table
        {
            "table": {
                "source_table": "chats",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}chats",
                "table_configuration": {
                    "tenant_id": TENANT_ID,
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "top": TOP,
                    "max_pages_per_batch": MAX_PAGES_PER_BATCH
                }
            }
        }
    ]
}

# Run ingestion
print("=" * 80)
print("Microsoft Teams Ingestion")
print("=" * 80)
print("\nIngesting:")
print("  • Teams")
print("  • Chats")
print("\nNote: This is the simple version (teams + chats only)")
print("For full dynamic ingestion (teams/channels/members/messages):")
print("  Use example_microsoft_teams_ingest.py instead")
print()

ingest(spark, pipeline_spec)

print("=" * 80)
print("✓ Ingestion Complete")
print("=" * 80)
print(f"\nTables created:")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}chats")
print(f"\nSample queries:")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}chats")
print(f"\nFor full ingestion (channels/members/messages):")
print(f"  Use example_microsoft_teams_ingest.py")
