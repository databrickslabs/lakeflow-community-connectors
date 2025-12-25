#!/bin/bash
# Create Unity Catalog Connection for Microsoft Teams using Databricks CLI
#
# IMPORTANT: Replace the placeholder values below with your actual credentials
# from Azure AD App Registration:
#   - tenant_id: Your Azure AD Directory (tenant) ID
#   - client_id: Your Application (client) ID
#   - client_secret: Your client secret value

# First, delete the old connection if it exists
echo "Deleting old connection (if exists)..."
databricks connections delete --name microsoft_teams_connection 2>/dev/null || true

echo "Creating new connection..."
databricks connections create \
  --json '{
    "name": "microsoft_teams_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "microsoft_teams",
      "tenant_id": "YOUR_TENANT_ID_HERE",
      "client_id": "YOUR_CLIENT_ID_HERE",
      "client_secret": "YOUR_CLIENT_SECRET_HERE",
      "externalOptionsAllowList": "team_id,channel_id,top,max_pages_per_batch,lookback_seconds,start_date"
    }
  }'

echo ""
echo "✅ Connection created successfully!"
echo ""
echo "Verify with:"
echo "  databricks connections get --name microsoft_teams_connection"
