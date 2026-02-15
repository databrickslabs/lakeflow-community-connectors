"""
Microsoft Teams Simple Ingestion Pipeline

This is a simplified version that ingests only the teams table.

IMPORTANT LIMITATION - Chats Table:
The Microsoft Graph API 'chats' endpoint does NOT support Application Permissions.
Chats have been removed from this template. You can only ingest: teams table.

For full ingestion (teams/channels/members/messages), use:
  example_microsoft_teams_ingest.py

This simple version:
- Ingests teams table (all teams)
- No channels, messages, members, or chats
- Useful for quick setup or when you only need team metadata

Configuration:
1. Update the credentials below
2. Update the destination catalog and schema
3. Run the pipeline
"""

from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

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
# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

# Register the LakeFlow source
register(spark, source_name)

# ==============================================================================
# PIPELINE SPECIFICATION
# ==============================================================================

# Ingest teams only (chats not supported with Application Permissions)
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
        }
    ]
}

# Run ingestion
print("=" * 80)
print("Microsoft Teams Ingestion")
print("=" * 80)
print("\nIngesting:")
print("  • Teams")
print("\nNote: This is the simple version (teams only)")
print("Note: Chats table NOT supported with Application Permissions")
print("\nFor full ingestion (teams/channels/members/messages):")
print("  Use example_microsoft_teams_ingest.py instead")
print()

ingest(spark, pipeline_spec)

print("=" * 80)
print("✓ Ingestion Complete")
print("=" * 80)
print(f"\nTables created:")
print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
print(f"\nSample query:")
print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams")
print(f"\nFor full ingestion (channels/members/messages):")
print(f"  Use example_microsoft_teams_ingest.py")
