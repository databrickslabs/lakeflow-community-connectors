#!/bin/bash
# Example: Create Unity Catalog Connection for Microsoft Teams Connector
#
# This script demonstrates how to create a UC connection using Databricks CLI.
#
# IMPORTANT: Due to Python Data Source API limitations, credentials should NOT be
# stored in the connection. They must be passed via table_configuration in pipeline specs.
#
# Prerequisites:
# 1. Install Databricks CLI: pip install databricks-cli
# 2. Configure authentication: databricks configure --token
#
# Usage:
#   chmod +x uc_connection_example.sh
#   ./uc_connection_example.sh

# Configuration variables
CONNECTION_NAME="microsoft_teams_connection"     # Choose your connection name

# Note: externalOptionsAllowList is optional but recommended for future compatibility
# Credentials should NEVER be stored in the connection - pass them via table_configuration
EXTERNAL_OPTIONS_ALLOW_LIST="team_id,channel_id,message_id,start_date,top,max_pages_per_batch,lookback_seconds,fetch_all_teams,fetch_all_channels,fetch_all_messages"

# Create the UC connection
databricks connections create \
  --json "{
    \"name\": \"${CONNECTION_NAME}\",
    \"connection_type\": \"GENERIC_LAKEFLOW_CONNECT\",
    \"options\": {
      \"sourceName\": \"microsoft_teams\",
      \"externalOptionsAllowList\": \"${EXTERNAL_OPTIONS_ALLOW_LIST}\"
    }
  }"

# Verify connection was created
echo ""
echo "Connection created successfully!"
echo "Connection name: ${CONNECTION_NAME}"
echo ""
echo "NEXT STEPS:"
echo "1. Store credentials in Databricks Secrets:"
echo "   databricks secrets create-scope microsoft_teams"
echo "   databricks secrets put microsoft_teams tenant_id --string-value \"your-tenant-id\""
echo "   databricks secrets put microsoft_teams client_id --string-value \"your-client-id\""
echo "   databricks secrets put microsoft_teams client_secret --string-value \"your-client-secret\""
echo ""
echo "2. Use this connection in your pipeline specs with credentials from secrets:"
echo "   connection_name: \"${CONNECTION_NAME}\""
echo "   table_configuration:"
echo "     tenant_id: dbutils.secrets.get(\"microsoft_teams\", \"tenant_id\")"
echo "     client_id: dbutils.secrets.get(\"microsoft_teams\", \"client_id\")"
echo "     client_secret: dbutils.secrets.get(\"microsoft_teams\", \"client_secret\")"
echo ""
echo "Verify the connection with:"
echo "  databricks connections get --name ${CONNECTION_NAME}"
