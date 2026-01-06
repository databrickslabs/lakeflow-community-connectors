#!/bin/bash
# Example: Create Unity Catalog Connection for Microsoft Teams Connector
#
# This script demonstrates how to create a UC connection using Databricks CLI.
#
# Prerequisites:
# 1. Install Databricks CLI: pip install databricks-cli
# 2. Configure authentication: databricks configure --token
# 3. Create a Databricks Secret Scope with your Azure AD credentials:
#    databricks secrets create-scope microsoft_teams
#    databricks secrets put microsoft_teams tenant_id --string-value "your-tenant-id"
#    databricks secrets put microsoft_teams client_id --string-value "your-client-id"
#    databricks secrets put microsoft_teams client_secret --string-value "your-client-secret"
#
# Usage:
#   chmod +x uc_connection_example.sh
#   ./uc_connection_example.sh

# Configuration variables
CONNECTION_NAME="microsoft_teams_connection"     # Choose your connection name
TENANT_ID="{{secrets/microsoft_teams/tenant_id}}"
CLIENT_ID="{{secrets/microsoft_teams/client_id}}"
CLIENT_SECRET="{{secrets/microsoft_teams/client_secret}}"

# Optional: Whitelist of table-specific options users can pass
EXTERNAL_OPTIONS_ALLOW_LIST="team_id,channel_id,message_id,start_date,top,max_pages_per_batch,lookback_seconds,fetch_all_teams,fetch_all_channels,fetch_all_messages,use_delta_api,max_concurrent_threads,reaction_poll_window_days,reaction_poll_batch_size"

# Create the UC connection
databricks connections create \
  --json "{
    \"name\": \"${CONNECTION_NAME}\",
    \"connection_type\": \"GENERIC_LAKEFLOW_CONNECT\",
    \"options\": {
      \"sourceName\": \"microsoft_teams\",
      \"tenant_id\": \"${TENANT_ID}\",
      \"client_id\": \"${CLIENT_ID}\",
      \"client_secret\": \"${CLIENT_SECRET}\",
      \"externalOptionsAllowList\": \"${EXTERNAL_OPTIONS_ALLOW_LIST}\"
    }
  }"

# Verify connection was created
echo ""
echo "Connection created successfully!"
echo "Connection name: ${CONNECTION_NAME}"
echo ""
echo "NEXT STEPS:"
echo "1. Use this connection in your pipeline specs:"
echo "   connection_name: \"${CONNECTION_NAME}\""
echo "   table_configuration:"
echo "     team_id: \"your-team-id\"  # Or use fetch_all_teams: \"true\""
echo "     channel_id: \"your-channel-id\"  # Or use fetch_all_channels: \"true\""
echo ""
echo "Verify the connection with:"
echo "  databricks connections get --name ${CONNECTION_NAME}"
