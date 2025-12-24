#!/bin/bash
# Example: Create Unity Catalog Connection for Microsoft Teams Connector
#
# This script demonstrates how to create a UC connection using Databricks CLI.
#
# Prerequisites:
# 1. Install Databricks CLI: pip install databricks-cli
# 2. Configure authentication: databricks configure --token
# 3. Replace placeholder values with your actual credentials
#
# Usage:
#   chmod +x uc_connection_example.sh
#   ./uc_connection_example.sh

# Configuration variables - REPLACE THESE VALUES
TENANT_ID="your-azure-ad-tenant-id"              # From Azure Portal → Azure AD → Overview
CLIENT_ID="your-application-client-id"           # From Azure Portal → App Registrations → Overview
CLIENT_SECRET="your-client-secret-value"         # From Azure Portal → App Registrations → Certificates & secrets
CONNECTION_NAME="microsoft_teams_connection"     # Choose your connection name

# External options that users can pass in pipeline specs
EXTERNAL_OPTIONS_ALLOW_LIST="team_id,channel_id,top,max_pages_per_batch,lookback_seconds,start_date"

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
echo "You can now use this connection in your pipeline specs:"
echo "  connection_name: \"${CONNECTION_NAME}\""
echo ""
echo "Verify the connection with:"
echo "  databricks connections get --name ${CONNECTION_NAME}"
