# Microsoft Teams Connector - Complete Setup Guide

This guide walks you through setting up and using the Microsoft Teams connector in Databricks, from Azure AD configuration to viewing your data.

**Time to complete:** 15-20 minutes

**Requirements:**
- Azure AD admin access (required for granting consent)
- Databricks workspace with Unity Catalog enabled

---

## Part 1: Azure AD Setup

**Time: 5 minutes**

### Step 1: Create Azure AD App Registration

1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to **Azure Active Directory** → **App registrations**
3. Click **New registration**
   - **Name:** `Databricks Teams Connector`
   - **Supported account types:** Accounts in this organizational directory only
   - Click **Register**

### Step 2: Copy Credentials

After registration, you'll see the app overview page:

1. **Copy these values** (you'll need them later):
   - **Application (client) ID** - this is your `client_id`
   - **Directory (tenant) ID** - this is your `tenant_id`

### Step 3: Create Client Secret

1. In the left menu, click **Certificates & secrets**
2. Click **New client secret**
   - **Description:** `databricks-connector`
   - **Expires:** 24 months (or per your security policy)
   - Click **Add**
3. **IMMEDIATELY COPY the secret Value** - this is your `client_secret`
   - ⚠️ You cannot see this value again after leaving the page!

### Step 4: Grant Application Permissions

1. In the left menu, click **API permissions**
2. Click **Add a permission** → **Microsoft Graph** → **Application permissions**
3. Search for and add these 5 permissions:
   - `Team.ReadBasic.All`
   - `Channel.ReadBasic.All`
   - `ChannelMessage.Read.All`
   - `TeamMember.Read.All`
   - `Chat.Read.All`
4. Click **Add permissions**
5. **Click "Grant admin consent for [Your Organization]"** (requires admin rights)
   - ⚠️ Without this, authentication will fail with 403 errors

### Step 5: Verify Permissions

Confirm you see all 5 permissions with green checkmarks under "Status: Granted"

**✅ Azure AD setup complete!** You now have:
- `tenant_id`
- `client_id`
- `client_secret`

---

## Part 2: Databricks Setup

**Time: 10 minutes**

### Step 1: Upload Connector to Databricks

**Option A: Using Databricks CLI**
```bash
# Install Databricks CLI if needed
pip install databricks-cli

# Configure authentication
databricks configure --token

# Upload connector source code
databricks workspace import-dir \
  sources/microsoft_teams \
  /Workspace/Shared/connectors/microsoft_teams
```

**Option B: Using Databricks UI**
1. Navigate to your Databricks workspace
2. Go to **Workspace** → **Shared**
3. Create folder: `connectors/microsoft_teams`
4. Upload `microsoft_teams.py` to this folder

### Step 2: Create Unity Catalog Connection

Create a UC connection for the Microsoft Teams connector using Databricks CLI or UI.

**Option A: Using Databricks CLI**

```bash
databricks connections create \
  --json '{
    "name": "microsoft_teams_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "microsoft_teams",
      "tenant_id": "your-tenant-id-guid",
      "client_id": "your-client-id-guid",
      "client_secret": "your-client-secret-value",
      "externalOptionsAllowList": "team_id,channel_id,top,max_pages_per_batch,lookback_seconds,start_date"
    }
  }'
```

**Option B: Using Databricks UI**

1. Navigate to **Catalog** → **Connections**
2. Click **Create connection**
3. Select **Lakeflow Community Connector**
4. Choose **Microsoft Teams** (or **Add Community Connector** if not listed)
5. Configure connection options:
   - **Name**: `microsoft_teams_connection`
   - **Source Name**: `microsoft_teams`
   - **Tenant ID**: Your Azure AD tenant ID
   - **Client ID**: Your application client ID
   - **Client Secret**: Your client secret
   - **External Options Allow List**: `team_id,channel_id,top,max_pages_per_batch,lookback_seconds,start_date`

**IMPORTANT:** The `externalOptionsAllowList` must include all table-specific options that will be used in pipeline specs.

### Step 3: Test the Connection

Test your connection by listing available tables:

```sql
SHOW TABLES IN microsoft_teams_connection;
```

You should see:
- teams
- channels
- messages
- members
- chats

---

## Part 3: Create Your First Pipeline

**Time: 5 minutes**

### Step 1: Find Your Team IDs

First, ingest the teams table to get team IDs:

```python
# Create a pipeline to ingest teams
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "teams"
            }
        }
    ]
}
```

After running, query to find your team IDs:

```sql
SELECT id, displayName, description
FROM main.teams_data.teams;
```

### Step 2: Ingest Channels from a Team

Using a team_id from step 1:

```python
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "channels",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "team_id": "YOUR_TEAM_ID_HERE"
                }
            }
        }
    ]
}
```

### Step 3: Ingest Messages (CDC Mode)

Ingest messages from a specific channel with incremental loading:

```python
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "messages",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "team_id": "YOUR_TEAM_ID",
                    "channel_id": "YOUR_CHANNEL_ID",
                    "start_date": "2025-01-01T00:00:00Z"
                }
            }
        }
    ]
}
```

---

## Part 4: Verify Your Data

Query your ingested data:

```sql
-- View teams
SELECT * FROM main.teams_data.teams;

-- View channels
SELECT * FROM main.teams_data.channels;

-- View recent messages
SELECT
  createdDateTime,
  from.user.displayName as sender,
  body.content as message_text
FROM main.teams_data.messages
ORDER BY createdDateTime DESC
LIMIT 10;
```

---

## Next Steps

- [View full README](README.md) for complete documentation
- [API Documentation](microsoft_teams_api_doc.md) for all available fields
- [Test examples](test/README.md) for more pipeline configurations

---

## Troubleshooting

### Authentication Failed (401)
- Verify your `tenant_id`, `client_id`, and `client_secret` are correct
- Check if the client secret has expired
- Ensure the app exists in the correct tenant

### Permission Denied (403)
- Verify admin consent was granted for all 5 permissions
- Check that permissions are type "Application" not "Delegated"
- Wait 5-10 minutes for permission changes to propagate

### Resource Not Found (404)
- Verify the `team_id` and `channel_id` are correct
- Ensure the app has access to the team
- Check if the team or channel has been deleted

### No Data Returned
- Verify the team/channel exists and has data
- Check `start_date` is in the past (for CDC tables)
- Ensure the app has the required permissions

---

## Support

For issues or questions:
1. Check the [README](README.md) troubleshooting section
2. Verify your Azure AD app configuration
3. File an issue in the GitHub repository with error details
