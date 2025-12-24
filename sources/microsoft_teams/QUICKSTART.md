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
4. On **Step 1: Connection basics**:
   - **Name**: `microsoft_teams_connection`
   - Click **Next**
5. On **Step 2: Connection details**:
   - **Source name**: `microsoft_teams`
   - **Additional Options** - Add these key-value pairs:

   | Key | Value |
   |-----|-------|
   | `tenant_id` | Your Azure AD tenant ID (e.g., `12345678-1234-1234-1234-123456789abc`) |
   | `client_id` | Your application client ID (e.g., `87654321-4321-4321-4321-cba987654321`) |
   | `client_secret` | Your client secret value (e.g., `abc123~DEF456_ghi789`) |
   | `externalOptionsAllowList` | `team_id,channel_id,top,max_pages_per_batch,lookback_seconds,start_date` |

6. Click **Create connection**

**IMPORTANT:** The `externalOptionsAllowList` must include all table-specific options that will be used in pipeline specs.

**✅ Connection created!**

---

## Part 3: Create Ingestion Pipeline

**Time: 5-10 minutes**

After creating the Unity Catalog connection, you need to create a pipeline to actually ingest data.

### Option A: Use Databricks Lakeflow UI (Recommended)

1. Navigate to **Catalog** → **Connections**
2. Click on your `microsoft_teams_connection`
3. Follow Databricks' UI to create an ingestion pipeline
   - The exact UI flow depends on your Databricks workspace version
   - You'll select which table(s) to ingest (start with `teams`)
   - Databricks will generate the pipeline code for you

### Option B: Create Pipeline Manually

If you prefer to create the pipeline manually, use Delta Live Tables (DLT):

**1. Create a new notebook or Python file called `microsoft_teams_ingestion.py`**

**2. Add this code:**

```python
import dlt
from pyspark.sql import DataFrame

# Ingest teams table (no configuration needed)
@dlt.table(
    name="teams",
    comment="Microsoft Teams - all teams in the organization"
)
def teams():
    return dlt.read_stream(
        connection="microsoft_teams_connection",
        format="lakeflow",
        table="teams"
    )
```

**3. Create a DLT Pipeline:**

1. Navigate to **Workflows** → **Delta Live Tables**
2. Click **Create pipeline**
3. Configure:
   - **Pipeline name**: `microsoft_teams_pipeline`
   - **Source code**: Select the file you just created
   - **Target schema**: e.g., `main.teams_data`
   - **Cluster mode**: Your preference
4. Click **Create** and then **Start**

### Verify the Data

After the pipeline runs successfully, query the ingested data:

```sql
-- View your teams
SELECT id, displayName, description
FROM main.teams_data.teams;
```

Copy a `team_id` from the results - you'll need it for the next steps.

---

## Part 4: Ingest Additional Tables (Optional)

Once you have team IDs from the initial ingestion, you can add more tables to your pipeline.

### Channels Table

Add channels for a specific team:

```python
@dlt.table(
    name="channels",
    comment="Microsoft Teams - channels for a specific team"
)
def channels():
    return dlt.read_stream(
        connection="microsoft_teams_connection",
        format="lakeflow",
        table="channels",
        options={
            "team_id": "paste-your-team-id-here"
        }
    )
```

### Messages Table

Add messages for a specific channel:

```python
@dlt.table(
    name="messages",
    comment="Microsoft Teams - messages from a channel"
)
def messages():
    return dlt.read_stream(
        connection="microsoft_teams_connection",
        format="lakeflow",
        table="messages",
        options={
            "team_id": "paste-your-team-id-here",
            "channel_id": "paste-your-channel-id-here",
            "start_date": "2025-01-01T00:00:00Z"
        }
    )
```

### Chats Table

Add all accessible chats:

```python
@dlt.table(
    name="chats",
    comment="Microsoft Teams - all accessible chats"
)
def chats():
    return dlt.read_stream(
        connection="microsoft_teams_connection",
        format="lakeflow",
        table="chats"
    )
```

---

## Part 5: Query Your Data

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
