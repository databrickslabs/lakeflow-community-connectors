"""
Microsoft Teams DLT Pipeline - Direct Implementation

This pipeline uses the connector directly without the generated source file.
It ingests Teams data with automatic discovery (fetch_all modes).
"""

import dlt
from microsoft_teams import MicrosoftTeamsConnector

# ==============================================================================
# CONFIGURATION - Update these values
# ==============================================================================

# Azure AD credentials - REPLACE THESE
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Destination configuration
DESTINATION_CATALOG = "users"
DESTINATION_SCHEMA = "eduardo_lomonaco"
TABLE_PREFIX = "lakeflow_connector_"

# Ingestion options
INGEST_TEAMS = True      # Ingest all teams
INGEST_CHANNELS = True   # Auto-discover all teams, ingest all channels
INGEST_MEMBERS = True    # Auto-discover all teams, ingest all members
INGEST_MESSAGES = False  # Set to True to ingest messages (requires team IDs below)

# Optional: Limit messages to specific teams (leave empty [] to skip messages)
TEAM_IDS = []

# Pagination settings
TOP = "50"
MAX_PAGES_PER_BATCH = "100"

# Optional: Limit messages by date (format: "YYYY-MM-DDTHH:MM:SSZ")
START_DATE = "2025-01-01T00:00:00Z"

# ==============================================================================
# DO NOT EDIT BELOW THIS LINE
# ==============================================================================

# Common credentials
creds = {
    "tenant_id": TENANT_ID,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

# Initialize connector
connector = MicrosoftTeamsConnector()

# Teams table
if INGEST_TEAMS:
    @dlt.table(
        name=f"{TABLE_PREFIX}teams",
        table_properties={"quality": "bronze"}
    )
    def teams():
        """Ingest all teams in the organization"""
        connector.open(partition={}, connection_config=creds)
        records, _ = connector.read_table("teams", {}, {})
        for record in records:
            yield record

# Channels table - auto-discover all teams
if INGEST_CHANNELS:
    @dlt.table(
        name=f"{TABLE_PREFIX}channels",
        table_properties={"quality": "bronze"}
    )
    def channels():
        """Ingest channels across all teams (auto-discovery)"""
        connector.open(partition={}, connection_config=creds)
        table_options = {"fetch_all_teams": "true"}
        records, _ = connector.read_table("channels", {}, table_options)
        for record in records:
            yield record

# Members table - auto-discover all teams
if INGEST_MEMBERS:
    @dlt.table(
        name=f"{TABLE_PREFIX}members",
        table_properties={"quality": "bronze"}
    )
    def members():
        """Ingest members across all teams (auto-discovery)"""
        connector.open(partition={}, connection_config=creds)
        table_options = {"fetch_all_teams": "true"}
        records, _ = connector.read_table("members", {}, table_options)
        for record in records:
            yield record

# Messages table - auto-discover all channels in specified teams
if INGEST_MESSAGES and TEAM_IDS:
    @dlt.table(
        name=f"{TABLE_PREFIX}messages",
        table_properties={"quality": "bronze"}
    )
    def messages():
        """Ingest messages from all channels in specified teams"""
        connector.open(partition={}, connection_config=creds)
        for team_id in TEAM_IDS:
            table_options = {
                "team_id": team_id,
                "fetch_all_channels": "true",
                "top": TOP,
                "max_pages_per_batch": MAX_PAGES_PER_BATCH,
                "start_date": START_DATE
            }
            records, _ = connector.read_table("messages", {}, table_options)
            for record in records:
                yield record

print("✓ Microsoft Teams DLT pipeline configured")
print(f"  Tables: {', '.join([f'{TABLE_PREFIX}{t}' for t in ['teams', 'channels', 'members'] if eval(f'INGEST_{t.upper()}')])}")
