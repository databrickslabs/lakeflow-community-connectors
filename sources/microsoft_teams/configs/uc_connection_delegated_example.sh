#!/bin/bash
# Unity Catalog Connection Setup - Delegated Permissions
# Use this when you don't have admin consent for application permissions

# Replace these values with your actual credentials
TENANT_ID="YOUR_TENANT_ID"
CLIENT_ID="YOUR_CLIENT_ID"
REFRESH_TOKEN="YOUR_REFRESH_TOKEN"  # Get from get_refresh_token.py

# Create UC connection with delegated permissions
databricks connections create \
  --json "{
    \"name\": \"microsoft_teams_connection\",
    \"connection_type\": \"GENERIC_LAKEFLOW_CONNECT\",
    \"options\": {
      \"sourceName\": \"microsoft_teams\",
      \"tenant_id\": \"$TENANT_ID\",
      \"client_id\": \"$CLIENT_ID\",
      \"refresh_token\": \"$REFRESH_TOKEN\",
      \"externalOptionsAllowList\": \"team_id,channel_id,top,max_pages_per_batch,lookback_seconds,start_date\"
    }
  }"

echo "✅ Unity Catalog connection created with delegated permissions"
echo "You can now use this connection in your pipeline specs"
