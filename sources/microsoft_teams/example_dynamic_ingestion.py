"""
Dynamic Microsoft Teams Ingestion Pipeline

This pipeline automatically discovers and ingests:
1. All teams the app has access to
2. All channels within each team
3. All messages within each channel
4. All members within each team
5. All chats

No manual team_id or channel_id configuration needed - everything is discovered dynamically.
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "microsoft_teams"

# Azure AD credentials - REPLACE WITH YOUR VALUES
TENANT_ID = "YOUR_TENANT_ID"  # Your Azure AD Directory (tenant) ID
CLIENT_ID = "YOUR_CLIENT_ID"  # Your Application (client) ID
CLIENT_SECRET = "YOUR_CLIENT_SECRET"  # Your client secret value

# Register the Microsoft Teams connector
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# ==============================================================================
# STEP 1: Ingest all teams
# ==============================================================================
print("Step 1: Ingesting all teams...")

teams_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "tenant_id": TENANT_ID,
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET
                }
            }
        }
    ]
}

ingest(spark, teams_spec)
print("✓ Teams ingested")

# ==============================================================================
# STEP 2: Ingest all chats (no parent IDs required)
# ==============================================================================
print("Step 2: Ingesting all chats...")

chats_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "chats",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "tenant_id": TENANT_ID,
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "top": "50",
                    "max_pages_per_batch": "10"
                }
            }
        }
    ]
}

ingest(spark, chats_spec)
print("✓ Chats ingested")

# ==============================================================================
# STEP 3: Dynamically ingest channels, members, and messages for each team
# ==============================================================================
print("Step 3: Discovering teams and ingesting related data...")

# Query the teams table to get all team IDs
teams_df = spark.sql("SELECT id, displayName FROM main.teams_data.teams")
teams_list = teams_df.collect()

print(f"Found {len(teams_list)} teams")

for team_row in teams_list:
    team_id = team_row["id"]
    team_name = team_row["displayName"]
    print(f"\nProcessing team: {team_name} ({team_id})")

    # Ingest channels for this team
    print(f"  - Ingesting channels for {team_name}...")
    channels_spec = {
        "connection_name": "microsoft_teams_connection",
        "objects": [
            {
                "table": {
                    "source_table": "channels",
                    "destination_catalog": "main",
                    "destination_schema": "teams_data",
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

    # Ingest members for this team
    print(f"  - Ingesting members for {team_name}...")
    members_spec = {
        "connection_name": "microsoft_teams_connection",
        "objects": [
            {
                "table": {
                    "source_table": "members",
                    "destination_catalog": "main",
                    "destination_schema": "teams_data",
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

    # Query channels for this team to get channel IDs
    channels_df = spark.sql(f"""
        SELECT id, displayName
        FROM main.teams_data.channels
        WHERE team_id = '{team_id}'
    """)
    channels_list = channels_df.collect()

    print(f"  - Found {len(channels_list)} channels in {team_name}")

    # Ingest messages for each channel
    for channel_row in channels_list:
        channel_id = channel_row["id"]
        channel_name = channel_row["displayName"]
        print(f"    • Ingesting messages from channel: {channel_name}")

        messages_spec = {
            "connection_name": "microsoft_teams_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "messages",
                        "destination_catalog": "main",
                        "destination_schema": "teams_data",
                        "table_configuration": {
                            "tenant_id": TENANT_ID,
                            "client_id": CLIENT_ID,
                            "client_secret": CLIENT_SECRET,
                            "team_id": team_id,
                            "channel_id": channel_id,
                            "top": "50",
                            "max_pages_per_batch": "10"
                        }
                    }
                }
            ]
        }
        ingest(spark, messages_spec)

print("\n" + "="*80)
print("✓ Dynamic ingestion complete!")
print("="*80)
print("\nIngested data summary:")
print("  - Teams: main.teams_data.teams")
print("  - Channels: main.teams_data.channels")
print("  - Messages: main.teams_data.messages")
print("  - Members: main.teams_data.members")
print("  - Chats: main.teams_data.chats")
