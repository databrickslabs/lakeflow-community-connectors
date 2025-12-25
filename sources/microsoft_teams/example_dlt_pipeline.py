"""
Microsoft Teams DLT Pipeline Example

This file demonstrates how to create Delta Live Tables (DLT) for Microsoft Teams data.
Use this as a template for your DLT pipeline's ingest.py file.

Prerequisites:
1. Create Unity Catalog connection named 'microsoft_teams_connection' (see QUICKSTART.md)
2. Replace placeholder credentials below with your actual Azure AD credentials
3. Upload this code to a DLT pipeline's ingest.py file

Available Tables:
- teams: All teams the app has access to
- channels: Channels within a specific team (requires team_id)
- messages: Messages in a channel (requires team_id and channel_id)
- members: Members of a team (requires team_id)
- chats: All chats the app has access to
"""

import dlt
from sources.microsoft_teams._generated_microsoft_teams_python_source import register_lakeflow_source

# Register the Microsoft Teams connector
register_lakeflow_source(spark)

# =============================================================================
# CONFIGURATION - Replace these values with your credentials
# =============================================================================
CONNECTION_NAME = "microsoft_teams_connection"
TENANT_ID = "YOUR_TENANT_ID"         # Azure AD Directory (tenant) ID
CLIENT_ID = "YOUR_CLIENT_ID"          # Application (client) ID
CLIENT_SECRET = "YOUR_CLIENT_SECRET"  # Client secret value

# =============================================================================
# DLT TABLE DEFINITIONS
# =============================================================================

@dlt.table(
    name="teams",
    comment="Microsoft Teams - all teams the authenticated app has access to",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def teams():
    """
    Ingests all Microsoft Teams that the app has access to.

    Primary Keys: id
    Ingestion Type: Snapshot (full refresh)
    """
    return spark.read.format("lakeflow_python_source") \
        .option("sourceName", "microsoft_teams") \
        .option("connectionName", CONNECTION_NAME) \
        .option("sourceTable", "teams") \
        .option("tenant_id", TENANT_ID) \
        .option("client_id", CLIENT_ID) \
        .option("client_secret", CLIENT_SECRET) \
        .load()


# Uncomment and configure the tables below as needed

# @dlt.table(
#     name="channels",
#     comment="Microsoft Teams Channels for a specific team"
# )
# def channels():
#     """
#     Ingests channels from a specific team.
#
#     Primary Keys: id
#     Ingestion Type: Snapshot (full refresh)
#     Required Options: team_id
#     """
#     return spark.read.format("lakeflow_python_source") \
#         .option("sourceName", "microsoft_teams") \
#         .option("connectionName", CONNECTION_NAME) \
#         .option("sourceTable", "channels") \
#         .option("tenant_id", TENANT_ID) \
#         .option("client_id", CLIENT_ID) \
#         .option("client_secret", CLIENT_SECRET) \
#         .option("team_id", "YOUR_TEAM_ID") \
#         .load()


# @dlt.table(
#     name="messages",
#     comment="Microsoft Teams Messages from a specific channel"
# )
# def messages():
#     """
#     Ingests messages from a specific channel.
#
#     Primary Keys: id
#     Cursor Field: lastModifiedDateTime
#     Ingestion Type: CDC (incremental)
#     Required Options: team_id, channel_id
#     Optional: top (default 50), max_pages_per_batch (default 10)
#     """
#     return spark.read.format("lakeflow_python_source") \
#         .option("sourceName", "microsoft_teams") \
#         .option("connectionName", CONNECTION_NAME) \
#         .option("sourceTable", "messages") \
#         .option("tenant_id", TENANT_ID) \
#         .option("client_id", CLIENT_ID) \
#         .option("client_secret", CLIENT_SECRET) \
#         .option("team_id", "YOUR_TEAM_ID") \
#         .option("channel_id", "YOUR_CHANNEL_ID") \
#         .option("top", "50") \
#         .option("max_pages_per_batch", "10") \
#         .load()


# @dlt.table(
#     name="members",
#     comment="Microsoft Teams Members of a specific team"
# )
# def members():
#     """
#     Ingests members from a specific team.
#
#     Primary Keys: id
#     Ingestion Type: Snapshot (full refresh)
#     Required Options: team_id
#     """
#     return spark.read.format("lakeflow_python_source") \
#         .option("sourceName", "microsoft_teams") \
#         .option("connectionName", CONNECTION_NAME) \
#         .option("sourceTable", "members") \
#         .option("tenant_id", TENANT_ID) \
#         .option("client_id", CLIENT_ID) \
#         .option("client_secret", CLIENT_SECRET) \
#         .option("team_id", "YOUR_TEAM_ID") \
#         .load()


# @dlt.table(
#     name="chats",
#     comment="Microsoft Teams Chats accessible to the authenticated user"
# )
# def chats():
#     """
#     Ingests all chats the app has access to.
#
#     Primary Keys: id
#     Cursor Field: lastUpdatedDateTime
#     Ingestion Type: CDC (incremental)
#     Optional: top (default 50), max_pages_per_batch (default 10)
#     """
#     return spark.read.format("lakeflow_python_source") \
#         .option("sourceName", "microsoft_teams") \
#         .option("connectionName", CONNECTION_NAME) \
#         .option("sourceTable", "chats") \
#         .option("tenant_id", TENANT_ID) \
#         .option("client_id", CLIENT_ID) \
#         .option("client_secret", CLIENT_SECRET) \
#         .option("top", "50") \
#         .option("max_pages_per_batch", "10") \
#         .load()
