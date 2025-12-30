"""
Example DLT pipeline using fetch_all modes for automatic discovery.

This demonstrates the simplest possible configuration - just credentials,
no manual ID configuration required!
"""

# Your credentials
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"

# Destination configuration
DESTINATION_CATALOG = "users"
DESTINATION_SCHEMA = "eduardo_lomonaco"
TABLE_PREFIX = "teams_"

# Build credentials dict
creds = {
    "tenant_id": TENANT_ID,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

# ============================================================================
# EXAMPLE 1: Minimal Configuration - Ingest Everything with Zero Manual Setup
# ============================================================================

pipeline_spec_minimal = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        # 1. Teams (no parent IDs needed)
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}teams",
                "table_configuration": creds
            }
        },
        # 2. Channels (auto-discover ALL teams)
        {
            "table": {
                "source_table": "channels",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}channels",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true"  # ← Automatic discovery!
                }
            }
        },
        # 3. Members (auto-discover ALL teams)
        {
            "table": {
                "source_table": "members",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}members",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true"  # ← Automatic discovery!
                }
            }
        }
    ]
}

# ============================================================================
# EXAMPLE 2: Selective Team with Auto-Discovery of Channels/Messages
# ============================================================================

# If you want to limit to specific teams but auto-discover everything within them
SPECIFIC_TEAM_ID = "your-team-guid"

pipeline_spec_selective = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        # Messages from ALL channels in a specific team
        {
            "table": {
                "source_table": "messages",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}messages",
                "table_configuration": {
                    **creds,
                    "team_id": SPECIFIC_TEAM_ID,
                    "fetch_all_channels": "true",  # ← Auto-discover all channels
                    "start_date": "2025-01-01T00:00:00Z",
                    "top": "50",
                    "max_pages_per_batch": "100"
                }
            }
        }
    ]
}

# ============================================================================
# EXAMPLE 3: Complete Auto-Discovery with Replies
# ============================================================================

pipeline_spec_with_replies = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        # Teams
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}teams",
                "table_configuration": creds
            }
        },
        # Channels (all teams)
        {
            "table": {
                "source_table": "channels",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}channels",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true"
                }
            }
        },
        # Members (all teams)
        {
            "table": {
                "source_table": "members",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}members",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true"
                }
            }
        },
        # Messages (specific team, all channels)
        {
            "table": {
                "source_table": "messages",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}messages",
                "table_configuration": {
                    **creds,
                    "team_id": SPECIFIC_TEAM_ID,
                    "fetch_all_channels": "true",
                    "start_date": "2025-01-01T00:00:00Z"
                }
            }
        },
        # Message Replies (specific team and channel, all messages)
        {
            "table": {
                "source_table": "message_replies",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}message_replies",
                "table_configuration": {
                    **creds,
                    "team_id": SPECIFIC_TEAM_ID,
                    "channel_id": "your-channel-guid",  # Still need to specify which channel
                    "fetch_all_messages": "true",  # ← Auto-discover all threaded messages
                    "start_date": "2025-01-01T00:00:00Z"
                }
            }
        }
    ]
}

# ============================================================================
# Usage in DLT Pipeline
# ============================================================================

# Uncomment to use in your DLT pipeline:
# from pipeline.ingestion_pipeline import ingest
# ingest(spark, pipeline_spec_minimal)  # Or pipeline_spec_selective, or pipeline_spec_with_replies

print("Example pipeline configurations created!")
print("\nTo use:")
print("1. Replace credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET)")
print("2. Choose a pipeline_spec (minimal, selective, or with_replies)")
print("3. Run: ingest(spark, pipeline_spec_minimal)")
