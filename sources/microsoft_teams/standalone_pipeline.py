"""
Microsoft Teams Standalone DLT Pipeline

This pipeline uses the connector directly with DLT, bypassing the Lakeflow framework.
It supports all fetch_all modes for automatic discovery.

Usage in Databricks:
1. Update credentials below
2. Run this notebook as a DLT pipeline
"""

import dlt
import sys
import os

# Add sources to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from microsoft_teams.microsoft_teams import MicrosoftTeamsConnector

# ==============================================================================
# CONFIGURATION - Update these values
# ==============================================================================

# Azure AD credentials - REPLACE THESE
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# Ingestion options
INGEST_TEAMS = True
INGEST_CHANNELS = True
INGEST_MEMBERS = True
INGEST_MESSAGES = False  # Set to True to ingest messages

# Optional: Limit messages to specific teams (leave empty [] to skip messages)
TEAM_IDS = []

# Pagination settings
TOP = "50"
MAX_PAGES_PER_BATCH = "100"
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


# Teams table
if INGEST_TEAMS:
    @dlt.view(name="teams_raw")
    def teams_view():
        """Fetch teams data from Microsoft Graph API"""
        connector = MicrosoftTeamsConnector()
        connector.open(partition={}, connection_config=creds)
        records, _ = connector.read_table("teams", {}, {})
        return list(records)

    @dlt.table(name="teams", comment="Microsoft Teams in the organization")
    def teams():
        """Process teams into final table"""
        return dlt.read("teams_raw")


# Channels table - auto-discover all teams
if INGEST_CHANNELS:
    @dlt.view(name="channels_raw")
    def channels_view():
        """Fetch channels from all teams (auto-discovery)"""
        connector = MicrosoftTeamsConnector()
        connector.open(partition={}, connection_config=creds)
        table_options = {"fetch_all_teams": "true"}
        records, _ = connector.read_table("channels", {}, table_options)
        return list(records)

    @dlt.table(name="channels", comment="Microsoft Teams channels across all teams")
    def channels():
        """Process channels into final table"""
        return dlt.read("channels_raw")


# Members table - auto-discover all teams
if INGEST_MEMBERS:
    @dlt.view(name="members_raw")
    def members_view():
        """Fetch members from all teams (auto-discovery)"""
        connector = MicrosoftTeamsConnector()
        connector.open(partition={}, connection_config=creds)
        table_options = {"fetch_all_teams": "true"}
        records, _ = connector.read_table("members", {}, table_options)
        return list(records)

    @dlt.table(name="members", comment="Microsoft Teams members across all teams")
    def members():
        """Process members into final table"""
        return dlt.read("members_raw")


# Messages table - auto-discover all channels in specified teams
if INGEST_MESSAGES and TEAM_IDS:
    @dlt.view(name="messages_raw")
    def messages_view():
        """Fetch messages from all channels in specified teams"""
        all_records = []
        connector = MicrosoftTeamsConnector()
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
            all_records.extend(list(records))

        return all_records

    @dlt.table(
        name="messages",
        comment="Microsoft Teams messages (incremental)",
        partition_cols=["team_id"]
    )
    @dlt.expect_or_drop("valid_timestamp", "lastModifiedDateTime IS NOT NULL")
    def messages():
        """Process messages into final table with CDC"""
        return dlt.read("messages_raw")


print(f"✓ Microsoft Teams Standalone DLT Pipeline configured")
print(f"  Tables enabled: {', '.join([t for t in ['teams', 'channels', 'members', 'messages'] if eval(f'INGEST_{t.upper()}')])}")
