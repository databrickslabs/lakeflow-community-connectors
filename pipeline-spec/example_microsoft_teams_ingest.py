"""
Microsoft Teams Fully Automated Dynamic Ingestion Pipeline

This pipeline automatically discovers and ingests all Microsoft Teams data without
requiring manual configuration of team IDs or channel IDs.

** HOW IT WORKS **

On First Run:
1. Discovers all teams and channels via Microsoft Graph API
2. Saves configuration to a JSON file for future runs
3. Builds complete pipeline specification
4. Ingests all data into unified tables

On Subsequent Runs:
1. Reads existing configuration from JSON file
2. Builds pipeline specification from config
3. Ingests data (incremental CDC for messages/chats)

To re-discover teams/channels: Delete the config file and re-run

Benefits:
- Fully automated discovery (no manual team_id or channel_id configuration)
- Single file solution (plug and play)
- DLT-compatible (static graph built from config)
- Unified tables (all teams/channels in single tables)
- Incremental CDC for messages and chats
- Configurable table prefix

Configuration:
1. Update the credentials below
2. Update the destination catalog/schema if needed
3. Run the pipeline
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function
import json
import requests

# ==============================================================================
# CONFIGURATION - Update these values
# ==============================================================================
source_name = "microsoft_teams"
connection_name = "microsoft_teams_connection"

# Azure AD credentials
TENANT_ID = "YOUR_TENANT_ID"  # e.g., "12345678-1234-1234-1234-123456789abc"
CLIENT_ID = "YOUR_CLIENT_ID"  # e.g., "87654321-4321-4321-4321-cba987654321"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"  # e.g., "your-secret-value-here"

# Destination configuration
DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "teams_data"
TABLE_PREFIX = "lakeflow_connector_"  # Prefix for all tables

# Config file path (automatically managed)
CONFIG_PATH = "/FileStore/teams_connector/teams_config.json"

# Discovery options (only used on first run)
DISCOVER_CHANNELS = True  # Set False to skip channel discovery (teams/chats only)
DISCOVER_MESSAGES = True  # Set False to skip message ingestion

# Ingestion options
TOP = "50"  # Page size for API requests
MAX_PAGES_PER_BATCH = "10"  # Max pages per batch for CDC tables

# ==============================================================================
# DO NOT EDIT BELOW THIS LINE
# ==============================================================================

# Register the Microsoft Teams connector
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# ==============================================================================
# DISCOVERY FUNCTIONS
# ==============================================================================

def get_access_token() -> str:
    """Get OAuth access token using client credentials flow"""
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }

    response = requests.post(token_url, data=data)
    response.raise_for_status()

    return response.json()["access_token"]

def discover_teams(access_token: str) -> list:
    """Discover all teams accessible to the app"""
    print("  → Discovering teams...")

    url = "https://graph.microsoft.com/v1.0/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
    headers = {"Authorization": f"Bearer {access_token}"}

    teams = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        for team in data.get("value", []):
            teams.append({
                "id": team["id"],
                "displayName": team.get("displayName", "Unknown")
            })

        url = data.get("@odata.nextLink")

    print(f"    ✓ Found {len(teams)} team(s)")
    return teams

def discover_channels(access_token: str, team_id: str) -> list:
    """Discover all channels in a team"""
    url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels"
    headers = {"Authorization": f"Bearer {access_token}"}

    channels = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        for channel in data.get("value", []):
            channels.append({
                "id": channel["id"],
                "displayName": channel.get("displayName", "Unknown")
            })

        url = data.get("@odata.nextLink")

    return channels

def generate_config(access_token: str) -> dict:
    """Generate pipeline configuration by discovering all teams and channels"""
    print("\n" + "=" * 80)
    print("DISCOVERY MODE - First Run")
    print("=" * 80)
    print("Discovering Teams and channels via Microsoft Graph API...")
    print()

    # Discover teams
    teams = discover_teams(access_token)

    # Build configuration
    config = {
        "metadata": {
            "destination_catalog": DESTINATION_CATALOG,
            "destination_schema": DESTINATION_SCHEMA,
            "table_prefix": TABLE_PREFIX
        },
        "teams": [],
        "objects_to_ingest": []
    }

    # Always add teams and chats (no parent IDs required)
    config["objects_to_ingest"].extend([
        {
            "source_table": "teams",
            "destination_table": f"{TABLE_PREFIX}teams",
            "table_configuration": {}
        },
        {
            "source_table": "chats",
            "destination_table": f"{TABLE_PREFIX}chats",
            "table_configuration": {
                "top": TOP,
                "max_pages_per_batch": MAX_PAGES_PER_BATCH
            }
        }
    ])

    if not DISCOVER_CHANNELS:
        print("\n  → Skipping channel discovery (DISCOVER_CHANNELS=False)")
        return config

    # Discover channels and build ingestion objects
    print("  → Discovering channels for each team...")
    for idx, team in enumerate(teams, 1):
        team_id = team["id"]
        team_name = team["displayName"]

        print(f"\n  [{idx}/{len(teams)}] {team_name}")

        try:
            channels = discover_channels(access_token, team_id)
            print(f"    ✓ Found {len(channels)} channel(s)")

            team_config = {
                "team_id": team_id,
                "team_name": team_name,
                "channels": channels
            }
            config["teams"].append(team_config)

            # Add channels ingestion object
            config["objects_to_ingest"].append({
                "source_table": "channels",
                "destination_table": f"{TABLE_PREFIX}channels",
                "table_configuration": {
                    "team_id": team_id
                }
            })

            # Add members ingestion object
            config["objects_to_ingest"].append({
                "source_table": "members",
                "destination_table": f"{TABLE_PREFIX}members",
                "table_configuration": {
                    "team_id": team_id
                }
            })

            # Add messages ingestion objects if enabled
            if DISCOVER_MESSAGES:
                for channel in channels:
                    channel_id = channel["id"]
                    config["objects_to_ingest"].append({
                        "source_table": "messages",
                        "destination_table": f"{TABLE_PREFIX}messages",
                        "table_configuration": {
                            "team_id": team_id,
                            "channel_id": channel_id,
                            "top": TOP,
                            "max_pages_per_batch": MAX_PAGES_PER_BATCH
                        }
                    })

        except Exception as e:
            print(f"    ✗ Error discovering channels: {str(e)[:100]}")
            continue

    return config

def load_or_generate_config() -> dict:
    """Load existing config or generate new one if doesn't exist"""

    # Try to load existing config
    try:
        config_json = dbutils.fs.head(CONFIG_PATH)
        config = json.loads(config_json)

        print("=" * 80)
        print("LOADING EXISTING CONFIGURATION")
        print("=" * 80)
        print(f"Configuration loaded from: {CONFIG_PATH}")
        print()
        print("Configuration summary:")
        print(f"  • Teams discovered: {len(config.get('teams', []))}")
        print(f"  • Total ingestion objects: {len(config['objects_to_ingest'])}")
        print()
        print("To re-discover teams/channels, delete the config file:")
        print(f"  dbutils.fs.rm('{CONFIG_PATH}')")
        print()

        return config

    except Exception:
        # Config doesn't exist - generate it
        print("=" * 80)
        print("FIRST RUN - Auto-Discovery Enabled")
        print("=" * 80)
        print(f"Configuration file not found: {CONFIG_PATH}")
        print("Generating configuration by discovering teams and channels...")

        # Authenticate
        print("\n  → Authenticating with Microsoft Graph API...")
        access_token = get_access_token()
        print("    ✓ Authentication successful")

        # Discover and generate config
        config = generate_config(access_token)

        # Save configuration
        print()
        print("=" * 80)
        print("SAVING CONFIGURATION")
        print("=" * 80)

        config_json = json.dumps(config, indent=2)
        dbutils.fs.put(CONFIG_PATH, config_json, overwrite=True)

        print(f"✓ Configuration saved to: {CONFIG_PATH}")
        print()
        print("Configuration summary:")
        print(f"  • Teams discovered: {len(config.get('teams', []))}")
        print(f"  • Total ingestion objects: {len(config['objects_to_ingest'])}")
        print()

        return config

def build_pipeline_spec(config: dict) -> dict:
    """Build pipeline specification from configuration"""

    objects = []

    # Process each ingestion object from the config
    for obj_config in config["objects_to_ingest"]:
        # Build table configuration with credentials
        table_config = {
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }

        # Add any additional table-specific configuration
        table_config.update(obj_config.get("table_configuration", {}))

        # Build table spec
        table_spec = {
            "table": {
                "source_table": obj_config["source_table"],
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": obj_config["destination_table"],
                "table_configuration": table_config
            }
        }

        objects.append(table_spec)

    return {
        "connection_name": connection_name,
        "objects": objects
    }

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__" or True:  # Always run in Databricks
    try:
        print("=" * 80)
        print("Microsoft Teams Fully Automated Dynamic Ingestion")
        print("=" * 80)
        print()

        # Load or generate configuration
        config = load_or_generate_config()

        # Build pipeline specification
        print("=" * 80)
        print("BUILDING PIPELINE SPECIFICATION")
        print("=" * 80)
        pipeline_spec = build_pipeline_spec(config)
        print(f"✓ Pipeline spec built with {len(pipeline_spec['objects'])} table configuration(s)")
        print()

        # List unique tables to be created
        print("Tables to be created/updated:")
        tables_created = set()
        for obj in pipeline_spec['objects']:
            dest_table = obj['table']['destination_table']
            if dest_table not in tables_created:
                print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{dest_table}")
                tables_created.add(dest_table)
        print()

        # Run ingestion
        print("=" * 80)
        print("STARTING INGESTION")
        print("=" * 80)
        print()

        ingest(spark, pipeline_spec)

        # Summary
        print()
        print("=" * 80)
        print("✓ INGESTION COMPLETE!")
        print("=" * 80)
        print()
        print(f"Data ingested to: {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}")
        print()
        print("All data is now in unified tables:")
        for table in sorted(tables_created):
            print(f"  • {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{table}")
        print()
        print("Sample queries:")
        print(f"  -- View teams")
        print(f"  SELECT * FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}teams;")
        print()
        print(f"  -- View recent messages")
        print(f"  SELECT createdDateTime, from.user.displayName as sender, body.content")
        print(f"  FROM {DESTINATION_CATALOG}.{DESTINATION_SCHEMA}.{TABLE_PREFIX}messages")
        print(f"  ORDER BY createdDateTime DESC LIMIT 10;")
        print()
        print("To re-discover teams/channels:")
        print(f"  dbutils.fs.rm('{CONFIG_PATH}')")
        print("  Then re-run this pipeline")

    except Exception as e:
        print()
        print("=" * 80)
        print("✗ INGESTION FAILED")
        print("=" * 80)
        print(f"Error: {str(e)}")
        print()
        import traceback
        traceback.print_exc()
        raise
