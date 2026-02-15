#!/usr/bin/env python3
"""
Debug script to validate pipeline spec generation.
This helps verify that CHANNEL_IDS is correctly configured.
"""

import json

# Paste your exact configuration here
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

DESTINATION_CATALOG = "users"
DESTINATION_SCHEMA = "eduardo_lomonaco"
TABLE_PREFIX = "lakeflow_connector_"

TEAM_IDS = [
    "YOUR_TEAM_ID_1",
    "YOUR_TEAM_ID_2"
]

CHANNEL_IDS = [
    {
        "team_id": "YOUR_TEAM_ID_1",
        "channel_id": "YOUR_CHANNEL_ID_1"
    },
    {
        "team_id": "YOUR_TEAM_ID_2",
        "channel_id": "YOUR_CHANNEL_ID_2"
    }
]

INGEST_TEAMS = True
INGEST_CHANNELS = True
INGEST_MEMBERS = True
INGEST_MESSAGES = True

TOP = "50"
MAX_PAGES_PER_BATCH = "10"

# Build pipeline specification (same logic as template)
connection_name = "microsoft_teams_connection"
pipeline_spec = {
    "connection_name": connection_name,
    "objects": []
}

creds = {
    "tenant_id": TENANT_ID,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET
}

# Teams table
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

# For each team ID
for team_id in TEAM_IDS:
    # Channels
    if INGEST_CHANNELS:
        pipeline_spec["objects"].append({
            "table": {
                "source_table": "channels",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}channels",
                "table_configuration": {
                    **creds,
                    "team_id": team_id
                }
            }
        })

    # Members
    if INGEST_MEMBERS:
        pipeline_spec["objects"].append({
            "table": {
                "source_table": "members",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}members",
                "table_configuration": {
                    **creds,
                    "team_id": team_id
                }
            }
        })

# Messages
if INGEST_MESSAGES and CHANNEL_IDS:
    for channel_config in CHANNEL_IDS:
        pipeline_spec["objects"].append({
            "table": {
                "source_table": "messages",
                "destination_catalog": DESTINATION_CATALOG,
                "destination_schema": DESTINATION_SCHEMA,
                "destination_table": f"{TABLE_PREFIX}messages",
                "table_configuration": {
                    **creds,
                    "team_id": channel_config["team_id"],
                    "channel_id": channel_config["channel_id"],
                    "top": TOP,
                    "max_pages_per_batch": MAX_PAGES_PER_BATCH
                }
            }
        })

print("=" * 80)
print("PIPELINE SPECIFICATION DEBUG")
print("=" * 80)
print()
print(f"Total objects: {len(pipeline_spec['objects'])}")
print()

# Count by source table
from collections import Counter
source_tables = Counter([obj["table"]["source_table"] for obj in pipeline_spec["objects"]])

print("Objects by source table:")
for table, count in source_tables.items():
    print(f"  {table}: {count} configuration(s)")
print()

# Show messages configurations in detail
messages_objects = [obj for obj in pipeline_spec["objects"] if obj["table"]["source_table"] == "messages"]
if messages_objects:
    print(f"Messages configurations ({len(messages_objects)} total):")
    for idx, obj in enumerate(messages_objects, 1):
        config = obj["table"]["table_configuration"]
        print(f"  {idx}. team_id: {config.get('team_id')}")
        print(f"     channel_id: {config.get('channel_id')}")
        print()
else:
    print("❌ NO MESSAGES CONFIGURATIONS FOUND!")
    print()
    print("Debug info:")
    print(f"  INGEST_MESSAGES = {INGEST_MESSAGES}")
    print(f"  CHANNEL_IDS = {CHANNEL_IDS}")
    print(f"  len(CHANNEL_IDS) = {len(CHANNEL_IDS)}")
    print()

print("=" * 80)
print("Full pipeline spec (redacted credentials):")
print("=" * 80)

# Create redacted version for display
redacted_spec = {"connection_name": pipeline_spec["connection_name"], "objects": []}
for obj in pipeline_spec["objects"]:
    redacted_obj = {
        "table": {
            "source_table": obj["table"]["source_table"],
            "destination_catalog": obj["table"]["destination_catalog"],
            "destination_schema": obj["table"]["destination_schema"],
            "destination_table": obj["table"]["destination_table"],
            "table_configuration": {
                k: ("***REDACTED***" if k in ["tenant_id", "client_id", "client_secret"] else v)
                for k, v in obj["table"]["table_configuration"].items()
            }
        }
    }
    redacted_spec["objects"].append(redacted_obj)

print(json.dumps(redacted_spec, indent=2))
