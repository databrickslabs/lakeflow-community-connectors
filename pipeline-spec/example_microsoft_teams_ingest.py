"""
Microsoft Teams Dynamic Ingestion Pipeline

This pipeline automatically ingests all Microsoft Teams data without requiring
manual configuration of team IDs or channel IDs.

Configuration:
1. Update the credentials below with your Azure AD app credentials
2. Update the destination catalog and schema if needed
3. Run the pipeline

The pipeline will:
- Ingest all teams
- Ingest all chats
- For each team discovered:
  - Ingest all channels
  - Ingest all members
  - For each channel: Ingest all messages (incremental CDC)
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# ==============================================================================
# CONFIGURATION - Update these values
# ==============================================================================
source_name = "microsoft_teams"
connection_name = "microsoft_teams_connection"

# Azure AD credentials
TENANT_ID = "YOUR_TENANT_ID"  # Azure AD Directory (tenant) ID
CLIENT_ID = "YOUR_CLIENT_ID"  # Application (client) ID
CLIENT_SECRET = "YOUR_CLIENT_SECRET"  # Client secret value

# Destination configuration
DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "teams_data"

# Ingestion options
ENABLE_MESSAGES_INGESTION = True  # Set to False to skip messages (can be large dataset)
ENABLE_CHATS_INGESTION = True  # Set to False to skip chats
TOP = "50"  # Page size for API requests
MAX_PAGES_PER_BATCH = "10"  # Max pages per batch for CDC tables

# ==============================================================================
# DO NOT EDIT BELOW THIS LINE
# ==============================================================================

# Register the Microsoft Teams connector
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

def ingest_teams():
    """Ingest all teams"""
    print("=" * 80)
    print("STEP 1: Ingesting all teams")
    print("=" * 80)

    spec = {
        "connection_name": connection_name,
        "objects": [
            {
                "table": {
                    "source_table": "teams",
                    "destination_catalog": DESTINATION_CATALOG,
                    "destination_schema": DESTINATION_SCHEMA,
                    "table_configuration": {
                        "tenant_id": TENANT_ID,
                        "client_id": CLIENT_ID,
                        "client_secret": CLIENT_SECRET
                    }
                }
            }
        ]
    }
    ingest(spark, spec)
    print("✓ Teams ingested\n")

def ingest_chats():
    """Ingest all chats"""
    if not ENABLE_CHATS_INGESTION:
        print("STEP 2: Skipping chats (ENABLE_CHATS_INGESTION=False)\n")
        return

    print("=" * 80)
    print("STEP 2: Ingesting all chats")
    print("=" * 80)

    spec = {
        "connection_name": connection_name,
        "objects": [
            {
                "table": {
                    "source_table": "chats",
                    "destination_catalog": DESTINATION_CATALOG,
                    "destination_schema": DESTINATION_SCHEMA,
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
    ingest(spark, spec)
    print("✓ Chats ingested\n")

def ingest_team_data():
    """Dynamically ingest channels, members, and messages for each team"""
    print("=" * 80)
    print("STEP 3: Discovering teams and ingesting related data")
    print("=" * 80)

    # Refresh tables to ensure they're visible
    spark.sql(f"REFRESH TABLE {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.teams")

    # Query teams table to get all team IDs
    teams_df = spark.table(f"{DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.teams")
    teams_list = teams_df.collect()

    print(f"Found {len(teams_list)} team(s)\n")

    for idx, team_row in enumerate(teams_list, 1):
        team_id = team_row["id"]
        team_name = team_row["displayName"]

        print(f"[{idx}/{len(teams_list)}] Processing team: {team_name}")

        # Ingest channels
        print(f"  → Ingesting channels...")
        channels_spec = {
            "connection_name": connection_name,
            "objects": [
                {
                    "table": {
                        "source_table": "channels",
                        "destination_catalog": DESTINATION_CATALOG,
                        "destination_schema": DESTINATION_SCHEMA,
                        "table_configuration": {
                            "tenant_id": TENANT_ID,
                            "client_id": CLIENT_ID,
                            "client_secret": CLIENT_SECRET,
                            "team_id": team_id
                        }
                    }
                }
            ]
        }
        ingest(spark, channels_spec)

        # Ingest members
        print(f"  → Ingesting members...")
        members_spec = {
            "connection_name": connection_name,
            "objects": [
                {
                    "table": {
                        "source_table": "members",
                        "destination_catalog": DESTINATION_CATALOG,
                        "destination_schema": DESTINATION_SCHEMA,
                        "table_configuration": {
                            "tenant_id": TENANT_ID,
                            "client_id": CLIENT_ID,
                            "client_secret": CLIENT_SECRET,
                            "team_id": team_id
                        }
                    }
                }
            ]
        }
        ingest(spark, members_spec)

        # Ingest messages if enabled
        if ENABLE_MESSAGES_INGESTION:
            # Refresh channels table to ensure latest data is visible
            try:
                spark.sql(f"REFRESH TABLE {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.channels")
            except:
                pass  # Table might not exist yet on first run

            # Query channels for this team
            try:
                channels_df = spark.table(f"{DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.channels").filter(f"team_id = '{team_id}'")
                channels_list = channels_df.collect()
                print(f"  → Found {len(channels_list)} channel(s)")
            except Exception as e:
                print(f"  → Warning: Could not query channels table: {str(e)[:100]}")
                print(f"  → Skipping messages for this team")
                print()
                continue

            for channel_row in channels_list:
                channel_id = channel_row["id"]
                channel_name = channel_row["displayName"]
                print(f"    • Ingesting messages from: {channel_name}")

                messages_spec = {
                    "connection_name": connection_name,
                    "objects": [
                        {
                            "table": {
                                "source_table": "messages",
                                "destination_catalog": DESTINATION_CATALOG,
                                "destination_schema": DESTINATION_SCHEMA,
                                "table_configuration": {
                                    "tenant_id": TENANT_ID,
                                    "client_id": CLIENT_ID,
                                    "client_secret": CLIENT_SECRET,
                                    "team_id": team_id,
                                    "channel_id": channel_id,
                                    "top": TOP,
                                    "max_pages_per_batch": MAX_PAGES_PER_BATCH
                                }
                            }
                        }
                    ]
                }
                ingest(spark, messages_spec)
        else:
            print(f"  → Skipping messages (ENABLE_MESSAGES_INGESTION=False)")

        print()  # Blank line between teams

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__" or True:  # Always run in Databricks
    try:
        # Step 1: Ingest all teams
        ingest_teams()

        # Step 2: Ingest all chats
        ingest_chats()

        # Step 3: Ingest channels, members, and messages for each team
        ingest_team_data()

        # Summary
        print("=" * 80)
        print("✓ INGESTION COMPLETE!")
        print("=" * 80)
        print(f"\nData ingested to: {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}")
        print("\nTables created:")
        print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.teams")
        print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.channels")
        print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.members")
        if ENABLE_MESSAGES_INGESTION:
            print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.messages")
        if ENABLE_CHATS_INGESTION:
            print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.chats")

    except Exception as e:
        print("=" * 80)
        print("✗ INGESTION FAILED")
        print("=" * 80)
        print(f"Error: {str(e)}")
        raise
