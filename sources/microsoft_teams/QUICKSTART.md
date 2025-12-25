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

**Time: 5-10 minutes**

### Create Connection and Pipeline via Custom Connector

Databricks provides a streamlined "Custom Connector" flow that handles everything in one place.

1. Navigate to **Jobs & Pipelines** → Click **Ingestion pipeline**
2. Or go to **Data Ingestion** and click **Add data** → Scroll to **Community connectors** → Click **Custom Connector**

3. **Add custom connector** dialog:
   - **Source name**: `microsoft_teams`
   - **Git Repository URL**: `https://github.com/eduardohl/lakeflow-community-connectors-teams`
   - Click **Add Connector**

4. **Step 1: Connection** (Provide credentials):
   - Select or create a connection named: `microsoft_teams_connection`
   - **Connection details** - Add these key-value pairs:

   | Key | Value |
   |-----|-------|
   | `tenant_id` | Your Azure AD tenant ID (e.g., `12345678-1234-1234-1234-123456789abc`) |
   | `client_id` | Your application client ID (e.g., `87654321-4321-4321-4321-cba987654321`) |
   | `client_secret` | Your client secret value (e.g., `abc123~DEF456_ghi789`) |
   | `externalOptionsAllowList` | `team_id,channel_id,top,max_pages_per_batch,lookback_seconds,start_date` |

   - **IMPORTANT**:
     - Credentials (`tenant_id`, `client_id`, `client_secret`) are stored in the connection and passed automatically by Databricks
     - `externalOptionsAllowList` only includes table-specific options (NOT credentials)
     - This is a security feature - credentials are never in the allowlist
   - Click **Create connection** (if new) or **Next** (if existing)

5. **Step 2: Ingestion setup**:
   - **Pipeline name**: `microsoft_teams_ingestion_pipeline`
   - **Event log location**: Choose catalog and schema (e.g., `users` / `eduardo_lomonaco`)
   - **Root path**: Click the folder icon to create/select a path (e.g., `/Users/eduardo.lomonaco@databricks.com/connectors/microsoft_teams`)
     - You may need to create the folder first in Workspace if it doesn't exist
   - Click **Create**

**✅ Connection and pipeline created!** Databricks automatically generates an `ingest.py` file for you.

---

## Part 3: Configure Dynamic Ingestion Pipeline

The Microsoft Teams connector supports **fully automated ingestion** that discovers all teams and channels without requiring manual configuration.

### Step 1: Copy the Dynamic Ingestion Template

1. Open your pipeline `microsoft_teams_ingestion_pipeline`
2. Click on the `ingest.py` tab to edit it
3. Copy the full code from [`pipeline-spec/example_microsoft_teams_ingest.py`](../../pipeline-spec/example_microsoft_teams_ingest.py) and paste it into `ingest.py`

### Step 2: Update Configuration

Update only the top section with your credentials:

```python
# ==============================================================================
# CONFIGURATION - Update these values
# ==============================================================================
source_name = "microsoft_teams"
connection_name = "microsoft_teams_connection"

# Azure AD credentials - REPLACE THESE
TENANT_ID = "YOUR_TENANT_ID"  # e.g., "12345678-1234-1234-1234-123456789abc"
CLIENT_ID = "YOUR_CLIENT_ID"  # e.g., "87654321-4321-4321-4321-cba987654321"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"  # e.g., "your-secret-value-here"

# Destination configuration
DESTINATION_CATALOG = "main"
DESTINATION_SCHEMA = "teams_data"

# Ingestion options
ENABLE_MESSAGES_INGESTION = True  # Set False to skip messages (large dataset)
ENABLE_CHATS_INGESTION = True     # Set False to skip chats
TOP = "50"
MAX_PAGES_PER_BATCH = "10"
```

Replace `TENANT_ID`, `CLIENT_ID`, and `CLIENT_SECRET` with your actual Azure AD credentials.

### How Dynamic Ingestion Works

The pipeline automatically:

1. **Ingests all teams** your app has access to
2. **Ingests all chats** (if enabled)
3. **For each team discovered**:
   - Ingests all channels in that team
   - Ingests all members of that team
   - **For each channel**: Ingests all messages (if enabled)

**No manual team IDs or channel IDs needed!** The pipeline queries ingested data to discover IDs dynamically.

### Configuration Options

- **ENABLE_MESSAGES_INGESTION**: Set to `False` to skip messages (can be a large dataset)
- **ENABLE_CHATS_INGESTION**: Set to `False` to skip chats
- **DESTINATION_CATALOG / DESTINATION_SCHEMA**: Where to store the data
- **TOP**: Page size for API requests (default: 50)
- **MAX_PAGES_PER_BATCH**: Max pages per batch for CDC tables (default: 10)

### Important Notes

- **Fully automated**: No manual team_id or channel_id configuration required
- **Incremental CDC**: Messages and chats use Change Data Capture - only new/modified records on subsequent runs
- **Configurable**: Use flags to skip large datasets if needed
- **Credentials required**: Must pass credentials via table_configuration for each table

### Step 2: Run the Pipeline

1. Go back to the pipeline view
2. Click **Start** to run the pipeline
3. Wait for the pipeline to complete (monitor the progress in the UI)

### Step 3: Monitor Pipeline Execution

The pipeline will display progress as it runs:

```
================================================================================
STEP 1: Ingesting all teams
================================================================================
✓ Teams ingested

================================================================================
STEP 2: Ingesting all chats
================================================================================
✓ Chats ingested

================================================================================
STEP 3: Discovering teams and ingesting related data
================================================================================
Found 3 team(s)

[1/3] Processing team: Sales Team
  → Ingesting channels...
  → Ingesting members...
  → Found 5 channel(s)
    • Ingesting messages from: General
    • Ingesting messages from: Customer Updates
    ...

================================================================================
✓ INGESTION COMPLETE!
================================================================================
```

---

## Part 4: Query Your Data

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

### Missing Credentials Error: "tenant_id and client_id are required"

This error means the connector isn't receiving credentials from the connection. Common causes:

**1. Connection not created properly via UI**
   - The Databricks UI may have issues saving connection options
   - **Solution**: Use the CLI script instead:
   ```bash
   cd sources/microsoft_teams
   chmod +x create_connection.sh
   ./create_connection.sh
   ```
   - Verify the connection was created:
   ```bash
   databricks connections get microsoft_teams_connection
   ```

**2. Wrong connection name in pipeline spec**
   - Ensure `pipeline_spec` has exact connection name:
   ```python
   pipeline_spec = {
       "connection_name": "microsoft_teams_connection",  # Must match exactly
       ...
   }
   ```

**3. Credentials in wrong place**
   - ❌ WRONG: Don't put credentials in `table_configuration`
   - ✅ RIGHT: Credentials go in the connection options only
   - The connection automatically passes credentials to the connector

**4. Debug: Print what the connector receives**
   - Temporarily add debug logging to see what options are passed:
   ```python
   # In microsoft_teams.py __init__ method, add after line 32:
   print(f"DEBUG: Received options keys: {list(options.keys())}")
   print(f"DEBUG: Has tenant_id: {'tenant_id' in options}")
   ```
   - Regenerate: `python scripts/merge_python_source.py microsoft_teams`
   - Run pipeline and check logs

### Authentication Failed (401)
- Verify your `tenant_id`, `client_id`, and `client_secret` are correct
- Check if the client secret has expired (check Azure Portal)
- Ensure the app exists in the correct tenant
- Test authentication manually:
  ```bash
  curl -X POST "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token" \
    -d "client_id={client_id}" \
    -d "client_secret={client_secret}" \
    -d "scope=https://graph.microsoft.com/.default" \
    -d "grant_type=client_credentials"
  ```

### Permission Denied (403)
- Verify admin consent was granted for all 5 permissions
- Check that permissions are type "Application" not "Delegated"
- Wait 5-10 minutes for permission changes to propagate
- Verify in Azure Portal: App registrations → Your app → API permissions → Status column shows "Granted"

### Resource Not Found (404)
- Verify the `team_id` and `channel_id` are correct
- Ensure the app has access to the team
- Check if the team or channel has been deleted
- Test the API directly:
  ```bash
  # Get access token first, then:
  curl -H "Authorization: Bearer {token}" \
    "https://graph.microsoft.com/v1.0/teams/{team_id}"
  ```

### No Data Returned
- Verify the team/channel exists and has data
- Check `start_date` is in the past (for CDC tables)
- Ensure the app has the required permissions
- Run a test query to see what teams are accessible:
  ```sql
  SELECT id, displayName FROM main.your_schema.teams;
  ```

### Connection UI Shows Empty Fields
- This is a known UI issue where connection options may not display after saving
- The connection may still be correctly created - verify with CLI:
  ```bash
  databricks connections get microsoft_teams_connection
  ```
- If truly empty, recreate using the CLI script instead of the UI

---

## Support

For issues or questions:
1. Check the [README](README.md) troubleshooting section
2. Verify your Azure AD app configuration
3. File an issue in the GitHub repository with error details
