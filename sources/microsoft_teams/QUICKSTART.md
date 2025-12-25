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

## Part 3: Configure Your Pipeline

After the pipeline is created, you need to edit the `ingest.py` file to configure which tables to ingest.

### Step 1: Edit the ingest.py File

1. Open your pipeline `microsoft_teams_ingestion_pipeline`
2. Click on the `ingest.py` tab to edit it
3. Replace the generated code with this configuration:

   ```python
   # Import and register the Microsoft Teams connector
   from sources.microsoft_teams._generated_microsoft_teams_python_source import register_lakeflow_source
   register_lakeflow_source(spark)

   # Import the ingestion pipeline
   from pipeline.ingestion_pipeline import ingest

   # Pipeline specification - defines which tables to ingest
   # NOTE: Credentials must be passed via table_configuration for each table
   # because Databricks stores connection credentials in 'properties' field
   # but connectors receive the 'options' field (which doesn't include properties)
   pipeline_spec = {
       "connection_name": "microsoft_teams_connection",
       "objects": [
           {
               "table": {
                   "source_table": "teams",
                   "table_configuration": {
                       "tenant_id": "YOUR_TENANT_ID",      # Replace with your Azure AD tenant ID
                       "client_id": "YOUR_CLIENT_ID",       # Replace with your application client ID
                       "client_secret": "YOUR_CLIENT_SECRET" # Replace with your client secret
                   }
               }
           }
       ]
   }

   # Run the ingestion
   ingest(spark, pipeline_spec)
   ```

4. **IMPORTANT**: Replace the placeholder credentials with your actual Azure AD credentials:
   - `YOUR_TENANT_ID`: Your Azure AD Directory (tenant) ID
   - `YOUR_CLIENT_ID`: Your Application (client) ID
   - `YOUR_CLIENT_SECRET`: Your client secret value

5. Save the file (Cmd+S or click Save icon)

### Important Notes

- **Why credentials go in table_configuration**: Databricks stores connection credentials in the `properties` field, but PySpark DataSource connectors receive the `options` field. The `options` field does NOT include `properties`. By passing credentials via `table_configuration`, they get merged into the options that the connector receives.
- **externalOptionsAllowList enables this**: The connection has `tenant_id`, `client_id`, and `client_secret` in its `externalOptionsAllowList`, which allows them to be passed via `table_configuration`
- **Security consideration**: Since credentials are in the pipeline code, ensure your Databricks workspace has appropriate access controls

### Step 2: Run the Pipeline

1. Go back to the pipeline view
2. Click **Start** to run the pipeline
3. Wait for the pipeline to complete (monitor the progress in the UI)

### Step 3: Verify the Data

After the pipeline runs successfully, query the ingested data:

```sql
-- View your teams
SELECT id, displayName, description
FROM main.users.teams;
```

Copy a `team_id` from the results - you'll need it for ingesting additional tables.

---

## Part 4: Ingest Additional Tables (Optional)

Once you have team IDs from the initial ingestion, you can add more tables to your pipeline by updating the `pipeline_spec`.

### Add Channels Table

Edit `ingest.py` to add channels for a specific team:

```python
from pipeline.ingestion_pipeline import ingest

pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "channels",
                "table_configuration": {
                    "team_id": "paste-your-team-id-here"
                }
            }
        }
    ]
}

ingest(spark, pipeline_spec)
```

### Add Messages Table

For messages from a specific channel:

```python
from pipeline.ingestion_pipeline import ingest

pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "messages",
                "table_configuration": {
                    "team_id": "paste-your-team-id-here",
                    "channel_id": "paste-your-channel-id-here",
                    "start_date": "2025-01-01T00:00:00Z"
                }
            }
        }
    ]
}

ingest(spark, pipeline_spec)
```

### Add Multiple Tables at Once

Ingest multiple tables in a single pipeline:

```python
from pipeline.ingestion_pipeline import ingest

pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "teams"
            }
        },
        {
            "table": {
                "source_table": "channels",
                "table_configuration": {
                    "team_id": "paste-your-team-id-here"
                }
            }
        },
        {
            "table": {
                "source_table": "chats"
            }
        }
    ]
}

ingest(spark, pipeline_spec)
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
