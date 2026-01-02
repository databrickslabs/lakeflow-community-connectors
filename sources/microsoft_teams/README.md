# Lakeflow Microsoft Teams Community Connector

Production-grade connector for ingesting Microsoft Teams data into Databricks using the Microsoft Graph API v1.0.

## Overview

The Microsoft Teams connector enables you to:
- Extract teams, channels, messages, and members from Microsoft Teams
- Support both snapshot (full refresh) and CDC (incremental) ingestion modes
- Leverage Microsoft Graph API v1.0 with OAuth 2.0 authentication
- Ingest structured data with proper schema definitions into Delta tables

## Quick Start

### Step 1: Register Azure AD Application

1. **Navigate to Azure Portal**
   - Go to [Azure Portal](https://portal.azure.com)
   - Sign in with an account that has **Global Administrator** or **Application Administrator** role

2. **Register New Application**
   - In the left navigation, select **Microsoft Entra ID** (formerly Azure Active Directory)
   - Select **App registrations** → **New registration**
   - Configure the registration:
     - **Name**: `Microsoft Teams Connector` (or your preferred name)
     - **Supported account types**: Select "Accounts in this organizational directory only (Single tenant)"
     - **Redirect URI**: Leave blank (not needed for Application permissions)
   - Click **Register**

3. **Copy Essential Credentials**

   After registration, copy these values (you'll need them for the connector):

   - **Application (client) ID**: Found on the Overview page
   - **Directory (tenant) ID**: Found on the Overview page

   Example values:

   ```text
   Tenant ID:  a1b2c3d4-e5f6-7890-abcd-ef1234567890
   Client ID:  f9e8d7c6-b5a4-3210-9876-543210fedcba
   ```

4. **Create Client Secret**
   - In your app registration, navigate to **Certificates & secrets**
   - Under **Client secrets**, click **New client secret**
   - Add a description: `Databricks Teams Connector`
   - Select expiration: **24 months** (recommended) or custom duration
   - Click **Add**
   - **⚠️ IMPORTANT**: Copy the **Value** immediately (it won't be shown again)

   Example secret value:

   ```text
   Client Secret: abc123~xyz789.aBcDeFgHiJkLmNoPqRsTuVwXyZ
   ```

### Step 2: Configure API Permissions

1. **Add Microsoft Graph Permissions**
   - In your app registration, navigate to **API permissions**
   - Click **Add a permission** → **Microsoft Graph** → **Application permissions**

2. **Select Required Permissions**

   Add each of the following permissions:

   | Permission | Category | Required For |
   |------------|----------|--------------|
   | `Team.ReadBasic.All` | Microsoft Teams | Read team names and basic properties |
   | `Channel.ReadBasic.All` | Microsoft Teams | Read channel names and properties |
   | `ChannelMessage.Read.All` | Microsoft Teams | Read messages in all channels |
   | `TeamMember.Read.All` | Microsoft Teams | Read team membership information |

   **How to add each permission:**
   - Click **Add a permission** → **Microsoft Graph** → **Application permissions**
   - Search for the permission name (e.g., "Team.ReadBasic.All")
   - Check the box next to the permission
   - Click **Add permissions**
   - Repeat for each permission

3. **Grant Admin Consent** ⚠️ **CRITICAL STEP**

   After adding all permissions:
   - Click **Grant admin consent for [Your Organization Name]**
   - Confirm by clicking **Yes**
   - Verify all permissions show a **green checkmark** under "Status"

   **Without admin consent, the connector will fail with 403 Forbidden errors.**

### Step 3: Verify Setup

Your **API permissions** page should look like this:

```text
API / Permissions name              Type         Status
Microsoft Graph
  Team.ReadBasic.All               Application  ✓ Granted for [Org]
  Channel.ReadBasic.All            Application  ✓ Granted for [Org]
  ChannelMessage.Read.All          Application  ✓ Granted for [Org]
  TeamMember.Read.All              Application  ✓ Granted for [Org]
```

**⚠️ IMPORTANT - Chats Not Supported:**

- The Microsoft Graph API `/chats` endpoint does NOT support Application Permissions
- It only works with Delegated Permissions (interactive user login)
- This connector uses Application Permissions for automated/scheduled pipelines
- Therefore, **chats cannot be ingested**
- Supported tables: teams, channels, members, messages, message_replies

### Step 4: Setup in Databricks

After registering your Azure AD application and granting permissions, you need to configure the connector in your Databricks workspace.

#### Store Credentials Securely

**Before creating the connection**, store your Azure AD credentials in Databricks Secrets:

```bash
# Create a secret scope (one-time setup)
databricks secrets create-scope microsoft_teams

# Store your credentials using the Databricks CLI
# You'll need to provide the secret value via string-value flag or it will open an editor
databricks secrets put microsoft_teams tenant_id --string-value "your-tenant-id-here"
databricks secrets put microsoft_teams client_id --string-value "your-client-id-here"
databricks secrets put microsoft_teams client_secret --string-value "your-client-secret-here"
```

Alternatively, you can create secrets via the Databricks UI:

1. Navigate to **Settings** → **Secrets** (or use the URL: `https://<your-workspace>/#secrets/createScope`)
2. Create a new scope named `microsoft_teams`
3. Add secrets for `tenant_id`, `client_id`, and `client_secret`

**⚠️ IMPORTANT**:
- Credentials stored here will be referenced in your pipeline code using `dbutils.secrets.get()`
- **Do NOT store credentials in the Unity Catalog Connection parameters**
- The connection only needs `externalOptionsAllowList` configured (optional)

#### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created via the Databricks UI:

1. **Navigate to Catalog → External Data**
   - In your Databricks workspace, click **Catalog** in the left sidebar
   - Click **External Data** at the top
   - Navigate to the **Connections** tab

2. **Create Connection**
   - Click **Create connection** button
   - This will open the connection setup wizard

3. **Step 1: Connection Basics**
   - **Connection name**: Enter a name for your connection (e.g., `LakeflowCommunityConnectorMSTeams`)
   - **Connection type**: Select **Lakeflow Community Connector**
   - **Comment** (optional): Add a description of the connection

4. **Step 2: Connection Details**
   - **Source name**: Enter `microsoft_teams` (this must match the connector source name)
   - **Additional Options**: Configure the connection parameters as key-value pairs

   **Optional Parameters:**

   | Key | Value | Description |
   | --- | ----- | ----------- |
   | `externalOptionsAllowList` | `team_id,channel_id,message_id,start_date,top,max_pages_per_batch,lookback_seconds,fetch_all_teams,fetch_all_channels,fetch_all_messages` | (Optional) Comma-separated list of table-specific options users can pass. May not be enforced in current Databricks versions but recommended for future compatibility. |

   **⚠️ IMPORTANT - Credentials:**
   - **Do NOT store credentials (tenant_id, client_id, client_secret) in the connection**
   - Due to Python Data Source API limitations, connection-level credentials are not accessible to the connector
   - All credentials must be passed via `table_configuration` in your pipeline spec using `dbutils.secrets.get()`
   - See the configuration examples below for the correct pattern

5. **Create Connection**
   - Click **Create** to save the connection
   - The connection will now be available in the **Connections** list

The connection can also be created programmatically using the Unity Catalog API or Databricks CLI.

#### Add Custom Connector Source

You need to register the connector source code from GitHub so Databricks can access it during ingestion:

1. **Navigate to Catalog → Connections**
   - In your Databricks workspace, click **Catalog** in the left sidebar
   - Click **External Data** at the top
   - Navigate to the **Connections** tab

2. **Add Custom Connector**
   - Click on your connection (e.g., `LakeflowCommunityConnectorMSTeams`)
   - In the connection details page, look for the **Add custom connector** option or button
   - This will open the "Add custom connector" dialog

3. **Configure Connector Source**
   - **Source name**: Enter `microsoft_teams` (must match the directory name in the repository)
   - **Git Repository URL**: Enter the repository URL:
     ```
     https://github.com/eduardohl/lakeflow-community-connectors-teams
     ```
   - Click **Add Connector**

4. **Verify Registration**
   - The connector source will now appear in your connection details
   - Databricks will fetch the connector code from GitHub when running ingestion pipelines

**📝 Note**: The source name must exactly match the directory name containing the connector code in the repository (in this case: `microsoft_teams`).

#### Architecture Note: Credential Flow

Due to Python Data Source API limitations in Databricks:

1. **Unity Catalog Connection** serves as a reference and defines:
   - Connection name for pipeline specs
   - `externalOptionsAllowList` (optional table-specific options whitelist)

2. **Credentials flow** happens through pipeline code:
   - Retrieved from Databricks Secrets via `dbutils.secrets.get()`
   - Passed in `table_configuration` for each table
   - Received by connector in the `options` dictionary

This pattern differs from managed Databricks connectors where credentials can be stored at the connection level. Custom Python Data Source connectors require explicit credential passing through table configuration.

#### Run Your First Pipeline

Once the connection is created and the custom connector is registered, you can create an ingestion pipeline using the Databricks UI:

1. **Navigate to Jobs & Pipelines**
   - In your Databricks workspace, click **Jobs & Pipelines** in the left sidebar
   - Click **Create new** dropdown
   - Select **Ingestion pipeline** (Ingest data from apps, databases and files)

2. **Add Data Source**
   - In the "Add data" screen, scroll down to **Community connectors** section
   - Click **Custom Connector**
   - In the "Add custom connector" dialog:
     - **Source name**: Enter `microsoft_teams`
     - **Git Repository URL**: Enter `https://github.com/eduardohl/lakeflow-community-connectors-teams`
     - Click **Add Connector**

3. **Select Connection (STEP 1)**
   - The wizard will show "Ingest data from Microsoft_teams"
   - Under "Connection to the source", select your connection from the dropdown
   - Available connections will be listed (e.g., `lakeflowcommunityconnectormsteams`, `microsoft_teams_connection`)
   - Click to proceed to **STEP 2**

4. **Configure Ingestion Setup (STEP 2)**
   - **Pipeline name**: Enter a descriptive name (e.g., `LakeflowCommunityConnectorMSTeams`)
   - **Event log location**:
     - Select catalog (e.g., `users`)
     - Select schema for event logs (or leave default)
   - **Root path**: Enter the path where connector assets will be stored
     - Example: `/Users/eduardo.lomonaco@databricks.com/connectors/microsoft_teams`
   - The system will create the ingestion pipeline

5. **View and Run Pipeline**
   - Once created, you'll see your pipeline listed under **Jobs & Pipelines** → **Pipelines**
   - Click on the pipeline name to view details
   - The pipeline will show your connection and ingestion setup
   - Click **Run now** or configure a schedule to start ingesting data

**📝 Note**: For a complete example with all 5 tables, see [sample-ingest.py](sample-ingest.py).

---

## Prerequisites

### Microsoft 365 Environment

- Microsoft 365 tenant with Microsoft Teams enabled
- Teams with data to ingest (teams, channels, messages)
- **Global Administrator** or **Application Administrator** role for app registration

### Databricks Environment

- Databricks workspace with Unity Catalog enabled
- Permissions to create UC connections
- Delta Live Tables (DLT) support for declarative pipelines

---

## Supported Tables

The Microsoft Teams connector supports **5 core tables** with two ingestion modes:

**Ingestion Modes:**
- **Snapshot (Full Refresh)**: Fetches all data on every run. Recommended for small, infrequently changing datasets (teams, channels, members).
- **CDC (Change Data Capture/Incremental)**: Only fetches new or modified records after the last cursor value. Recommended for large, frequently changing datasets (messages, message_replies).

### Summary Table

| Table | Ingestion Type | Cursor Field | Parent IDs Required | Auto-Discovery Mode |
|-------|---------------|--------------|---------------------|---------------------|
| teams | Snapshot | - | None | - |
| channels | Snapshot | - | `team_id` OR `fetch_all_teams` | `fetch_all_teams=true` |
| messages | **CDC** | `lastModifiedDateTime` | `team_id`, `channel_id` OR `fetch_all_channels` | `fetch_all_channels=true` |
| members | Snapshot | - | `team_id` OR `fetch_all_teams` | `fetch_all_teams=true` |
| message_replies | **CDC** | `lastModifiedDateTime` | `team_id`, `channel_id`, `message_id` OR `fetch_all_messages` | `fetch_all_messages=true` |

**Auto-Discovery Modes**: Instead of manually specifying parent IDs, you can use `fetch_all` modes to automatically discover and ingest all resources. See the [Automatic Discovery (fetch_all Modes)](#automatic-discovery-fetch_all-modes) section below.

### 1. teams (Snapshot)

List of Microsoft Teams accessible by the application.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Unique team identifier (GUID) |
| displayName | String | Team name |
| description | String | Team description |
| visibility | String | `public`, `private`, or `hiddenMembership` |
| isArchived | Boolean | Whether team is archived |
| createdDateTime | String | ISO 8601 timestamp |
| memberSettings | String | JSON string with member permissions |
| guestSettings | String | JSON string with guest permissions |
| messagingSettings | String | JSON string with messaging settings |
| funSettings | String | JSON string with fun features config |

**Ingestion Type:** Snapshot (full refresh every run)
**Primary Key:** `id`
**Required Table Options:** None
**Graph API Endpoint:** `GET /groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')`

---

### 2. channels (Snapshot)

Channels within teams.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Unique channel identifier (GUID) |
| team_id | String | Parent team ID (connector-derived) |
| displayName | String | Channel name |
| description | String | Channel description |
| membershipType | String | `standard`, `private`, or `shared` |
| email | String | Email address for channel |
| webUrl | String | URL to channel in Teams client |
| createdDateTime | String | ISO 8601 timestamp |
| isFavoriteByDefault | Boolean | Auto-favorite for members |
| isArchived | Boolean | Whether channel is archived |

**Ingestion Type:** Snapshot (full refresh every run)
**Primary Key:** `id`
**Required Table Options:** `team_id`
**Graph API Endpoint:** `GET /teams/{team_id}/channels`

---

### 3. messages (CDC)

Messages within channels.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Unique message identifier |
| team_id | String | Parent team ID (connector-derived) |
| channel_id | String | Parent channel ID (connector-derived) |
| messageType | String | `message`, `systemEventMessage`, etc. |
| createdDateTime | String | ISO 8601 timestamp |
| lastModifiedDateTime | String | Last modification timestamp (cursor field) |
| lastEditedDateTime | String | Last edit timestamp (null if never edited) |
| deletedDateTime | String | Deletion timestamp (null if not deleted) |
| importance | String | `normal`, `high`, or `urgent` |
| from | Struct | Sender information (user, application, device) |
| body | Struct | Message content (contentType, content) |
| attachments | Array[Struct] | File attachments |
| mentions | Array[Struct] | @mentions in message |
| reactions | Array[Struct] | Emoji reactions |

**Ingestion Type:** CDC (incremental - only fetches new/modified records after cursor)
**Primary Key:** `id`
**Cursor Field:** `lastModifiedDateTime`
**Required Table Options:** `team_id`, `channel_id`
**Graph API Endpoint:** `GET /teams/{team_id}/channels/{channel_id}/messages`

---

### 4. members (Snapshot)

Members of teams.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Unique membership identifier |
| team_id | String | Parent team ID (connector-derived) |
| roles | Array[String] | Roles: `owner`, `member`, `guest` |
| displayName | String | User display name |
| userId | String | User ID (GUID) |
| email | String | Email address |
| visibleHistoryStartDateTime | String | When member can start seeing history |

**Ingestion Type:** Snapshot (full refresh every run)
**Primary Key:** `id`
**Required Table Options:** `team_id`
**Graph API Endpoint:** `GET /teams/{team_id}/members`

---

### 5. message_replies (CDC)

Threaded message replies within channels.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Unique reply identifier |
| parent_message_id | String | Parent message ID (connector-derived) |
| team_id | String | Parent team ID (connector-derived) |
| channel_id | String | Parent channel ID (connector-derived) |
| replyToId | String | ID of message being replied to |
| messageType | String | `message`, `systemEventMessage`, etc. |
| createdDateTime | String | ISO 8601 timestamp |
| lastModifiedDateTime | String | Last modification timestamp (cursor field) |
| lastEditedDateTime | String | Last edit timestamp (null if never edited) |
| deletedDateTime | String | Deletion timestamp (null if not deleted) |
| importance | String | `normal`, `high`, or `urgent` |
| from | Struct | Sender information (user, application, device) |
| body | Struct | Reply content (contentType, content) |
| attachments | Array[Struct] | File attachments |
| mentions | Array[Struct] | @mentions in reply |
| reactions | Array[Struct] | Emoji reactions |

**Ingestion Type:** CDC (incremental - only fetches new/modified replies after cursor)
**Primary Key:** `id`
**Cursor Field:** `lastModifiedDateTime`
**Required Table Options:** `team_id`, `channel_id`, `message_id`
**Graph API Endpoint:** `GET /teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies`

**Usage Notes:**

- Use this table to capture threaded conversations (replies to messages)
- Requires manually specifying which parent messages to track
- To find message IDs with replies:
  1. First ingest the messages table
  2. Query for messages that likely have threads (e.g., by subject, date range)
  3. Use the connector's test suite to verify which messages have replies
  4. Add those message IDs to your pipeline configuration
- The `parent_message_id` field enables joining replies back to parent messages
- Same schema as messages table for consistency

---

## Automatic Discovery (fetch_all Modes)

The connector supports automatic discovery modes that eliminate the need to manually configure parent resource IDs. Instead of running pipelines iteratively to discover teams, channels, and messages, you can use `fetch_all` options to automatically discover all resources in one run.

### Overview

| Table | Auto-Discovery Option | What It Does | Still Required |
|-------|----------------------|--------------|----------------|
| channels | `fetch_all_teams=true` | Discovers all teams, then fetches channels for each | Credentials only |
| members | `fetch_all_teams=true` | Discovers all teams, then fetches members for each | Credentials only |
| messages | `fetch_all_channels=true` | Discovers all channels in a team, then fetches messages for each | `team_id` |
| message_replies | `fetch_all_messages=true` | Discovers all messages in a channel, then fetches replies for each | `team_id`, `channel_id` |

### Benefits

**Before (Manual Configuration):**
```python
# Step 1: Run pipeline to get teams
# Step 2: Query teams table, copy IDs
TEAM_IDS = ["abc-123-...", "def-456-...", ...]  # Manual copy-paste

# Step 3: Run pipeline to get channels
# Step 4: Query channels table, copy IDs
CHANNEL_IDS = [
    {"team_id": "abc-123-...", "channel_id": "xyz-789-..."},
    {"team_id": "abc-123-...", "channel_id": "uvw-101-..."},
    # ... dozens or hundreds of manual entries
]
```

**After (Automatic Discovery):**
```python
# Just credentials from secrets - connector discovers everything automatically
{
    "tenant_id": dbutils.secrets.get("microsoft_teams", "tenant_id"),
    "client_id": dbutils.secrets.get("microsoft_teams", "client_id"),
    "client_secret": dbutils.secrets.get("microsoft_teams", "client_secret"),
    "fetch_all_teams": "true"  # ← That's it!
}
```

### Example: Zero-Configuration Ingestion

The simplest possible configuration - ingest all teams, channels, and members without any manual ID configuration:

```python
# Your credentials from Databricks Secrets
creds = {
    "tenant_id": dbutils.secrets.get("microsoft_teams", "tenant_id"),
    "client_id": dbutils.secrets.get("microsoft_teams", "client_id"),
    "client_secret": dbutils.secrets.get("microsoft_teams", "client_secret")
}

pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        # 1. Teams (no fetch_all needed - already fetches all)
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "teams",
                "table_configuration": creds
            }
        },
        # 2. Channels (auto-discover ALL teams)
        {
            "table": {
                "source_table": "channels",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "channels",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true"  # ← Automatic discovery!
                }
            }
        },
        # 3. Members (auto-discover ALL teams)
        {
            "table": {
                "source_table": "members",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "members",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true"  # ← Automatic discovery!
                }
            }
        }
    ]
}
```

**Result:** Single pipeline run ingests all teams, all channels across all teams, and all members across all teams - zero manual configuration.

### Example: Auto-Discover All Channels for Messages

Fetch messages from ALL channels in a specific team (useful for large teams with many channels):

```python
{
    "table": {
        "source_table": "messages",
        "destination_catalog": "main",
        "destination_schema": "teams_data",
        "destination_table": "messages",
        "table_configuration": {
            **creds,
            "team_id": "your-team-id",  # Still need to specify which team
            "fetch_all_channels": "true",  # ← Auto-discover all channels
            "start_date": "2025-01-01T00:00:00Z",
            "top": "50",
            "max_pages_per_batch": "100"
        }
    }
}
```

**Result:** Discovers all channels in the team, then fetches messages from each channel. No need to manually list channel IDs.

### Example: Auto-Discover All Messages for Replies

Fetch threaded replies from ALL messages in a specific channel:

```python
{
    "table": {
        "source_table": "message_replies",
        "destination_catalog": "main",
        "destination_schema": "teams_data",
        "destination_table": "message_replies",
        "table_configuration": {
            **creds,
            "team_id": "your-team-id",
            "channel_id": "your-channel-id",  # Still need to specify which channel
            "fetch_all_messages": "true",  # ← Auto-discover all messages with replies
            "start_date": "2025-01-01T00:00:00Z",
            "top": "50",
            "max_pages_per_batch": "100"
        }
    }
}
```

**Result:** Discovers all messages in the channel, then fetches replies for each message (skipping messages with no replies). No need to manually identify which messages have threads.

### How It Works

The connector implements discovery in two phases:

**Phase 1: Discovery**
- Fetches parent resources (teams, channels, or messages)
- Uses lightweight queries with `$select=id` for efficiency
- Handles pagination to discover all parent resources

**Phase 2: Ingestion**
- Iterates through discovered parent IDs
- Fetches child resources for each parent
- Gracefully handles inaccessible resources (404/403) by skipping them
- Merges all results into a single output stream

### Performance Considerations

**Automatic discovery is safe for production use:**
- Connector respects API rate limits (100ms between requests, automatic retry on 429)
- Uses pagination limits (`max_pages_per_batch`) to prevent runaway queries
- Skips inaccessible resources instead of failing
- Discovery phase uses minimal bandwidth (`$select=id` queries)

**When to use fetch_all modes:**
- **Use for channels/members:** When you have many teams and want all data without manual configuration
- **Use for messages:** When a team has many channels and you want complete coverage
- **Consider carefully for message_replies:** Fetching all replies can be expensive for channels with many messages. Use `start_date` to limit scope.

**When to use manual IDs:**
- When you only want data from specific teams/channels
- When you need fine-grained control over which resources to ingest
- For testing with a small subset of data

### Complete Example

For complete working examples, see the [Configuration Examples](#configuration-examples) section below which includes:

- Configuration-based ingestion (incremental workflow)
- Simple teams-only ingestion
- **Fully automated ingestion (zero configuration)** - See [sample-ingest.py](sample-ingest.py)
- Message replies ingestion (threaded conversations)

### Testing

Before running in production, verify your setup with the unit test suite:

```bash
# Run the comprehensive test suite
pytest sources/microsoft_teams/test/test_microsoft_teams.py -v
```

---

## Configuration Examples

### Example 1: Configuration-Based Ingestion (Recommended)

The recommended approach uses Python configuration lists for a DLT-compatible, incremental workflow.

See the complete template in [`pipeline-spec/example_microsoft_teams_ingest.py`](../../pipeline-spec/example_microsoft_teams_ingest.py).

```python
# Configure credentials using Databricks Secrets
TENANT_ID = dbutils.secrets.get("microsoft_teams", "tenant_id")
CLIENT_ID = dbutils.secrets.get("microsoft_teams", "client_id")
CLIENT_SECRET = dbutils.secrets.get("microsoft_teams", "client_secret")

# Add team IDs to ingest
TEAM_IDS = ["team-guid-1", "team-guid-2"]

# Add specific channels for messages (optional)
CHANNEL_IDS = [
    {"team_id": "team-guid-1", "channel_id": "channel-guid-1"},
]

# Add specific messages for replies/threads (optional)
REPLY_CONFIGS = [
    {"team_id": "team-guid-1", "channel_id": "channel-guid-1", "message_id": "message-guid-1"},
]

# Run pipeline
ingest(spark, pipeline_spec)
```

**Incremental Workflow:**
1. Run with `TEAM_IDS = []` to ingest teams table
2. Query teams table, copy IDs to `TEAM_IDS` list
3. Run again to ingest channels and members for those teams
4. Query channels table, copy IDs to `CHANNEL_IDS` list
5. Run again to ingest messages from those channels
6. Query messages table to identify threads, copy to `REPLY_CONFIGS` list
7. Run again to ingest message_replies from those threads

---

### Example 2: Simple Teams-Only Ingestion

For getting started, ingest just the teams table:

See [`pipeline-spec/example_microsoft_teams_simple_ingest.py`](../../pipeline-spec/example_microsoft_teams_simple_ingest.py).

```python
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "tenant_id": dbutils.secrets.get("microsoft_teams", "tenant_id"),
                    "client_id": dbutils.secrets.get("microsoft_teams", "client_id"),
                    "client_secret": dbutils.secrets.get("microsoft_teams", "client_secret")
                }
            }
        }
    ]
}

ingest(spark, pipeline_spec)
```

---

### Example 3: Fully Automated Ingestion (Zero Configuration)

For the fastest setup, use the fully automated mode that discovers all resources without manual configuration:

See [`sample-ingest.py`](sample-ingest.py) for a complete working example.

```python
# Credentials from Databricks Secrets - connector auto-discovers everything!
creds = {
    "tenant_id": dbutils.secrets.get("microsoft_teams", "tenant_id"),
    "client_id": dbutils.secrets.get("microsoft_teams", "client_id"),
    "client_secret": dbutils.secrets.get("microsoft_teams", "client_secret")
}

pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        # Teams (auto-discovers all teams)
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "lakeflow_connector_teams",
                "table_configuration": creds
            }
        },
        # Channels (auto-discovers all teams, then all channels)
        {
            "table": {
                "source_table": "channels",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "lakeflow_connector_channels",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true"  # Auto-discover all teams
                }
            }
        },
        # Messages (auto-discovers all teams, channels, then messages)
        {
            "table": {
                "source_table": "messages",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "lakeflow_connector_messages",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true",     # Auto-discover all teams
                    "fetch_all_channels": "true",  # Auto-discover all channels
                    "start_date": "2024-12-01T00:00:00Z"
                }
            }
        },
        # Message Replies (auto-discovers teams, channels, messages, then replies)
        {
            "table": {
                "source_table": "message_replies",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "lakeflow_connector_message_replies",
                "table_configuration": {
                    **creds,
                    "fetch_all_teams": "true",     # Auto-discover all teams
                    "fetch_all_channels": "true",  # Auto-discover all channels
                    "fetch_all_messages": "true",  # Auto-discover all messages
                    "start_date": "2024-12-01T00:00:00Z"
                }
            }
        }
    ]
}

ingest(spark, pipeline_spec)
```

**Benefits:**

- No manual ID configuration required
- Single pipeline run ingests all data
- Perfect for initial setup or complete organization sync
- Automatically handles new teams/channels as they're created

**Use this when:**

- You want to ingest all Teams data from your organization
- You don't want to manually configure team/channel IDs
- You're setting up for the first time and want a quick win

**Consider the incremental approach (Example 1) when:**

- You have many teams and want to start with a subset
- You need fine-grained control over which teams/channels to ingest
- You want to minimize API usage and ingestion time

---

### Example 4: Message Replies (Threaded Conversations)

To ingest threaded message replies, you need to specify which parent messages to track:

```python
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "message_replies",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "destination_table": "message_replies",
                "table_configuration": {
                    "tenant_id": dbutils.secrets.get("microsoft_teams", "tenant_id"),
                    "client_id": dbutils.secrets.get("microsoft_teams", "client_id"),
                    "client_secret": dbutils.secrets.get("microsoft_teams", "client_secret"),
                    "team_id": "team-guid-1",
                    "channel_id": "channel-guid-1",
                    "message_id": "message-guid-1",  # Parent message with replies
                    "start_date": "2025-01-01T00:00:00Z",  # Optional: initial cursor
                    "top": "50",
                    "max_pages_per_batch": "100"
                }
            }
        }
    ]
}

ingest(spark, pipeline_spec)
```

**How to find message IDs with replies:**

1. First, ingest messages and query for potential threaded messages:

```sql
SELECT id, team_id, channel_id, subject, createdDateTime
FROM main.teams_data.messages
WHERE subject IS NOT NULL  -- Messages with subjects often have replies
ORDER BY createdDateTime DESC
LIMIT 100;
```

2. Query the messages table to find messages that likely have replies (e.g., filter by subject, recent dates)
3. Add confirmed message IDs to your pipeline configuration

**Multiple threads:** To track multiple threaded messages, add multiple objects with different `message_id` values. The connector will merge all replies into a single `message_replies` table.

---

## Table Configuration Options

### Common Options (All Tables)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `top` | String (integer) | `50` | Page size for pagination (max: 50 for messages/message_replies, 999 for others) |
| `max_pages_per_batch` | String (integer) | `100` | Maximum pages to fetch per batch (safety limit) |

### Parent-Child Options

| Option        | Type          | Required For                                 | Description                                    |
|---------------|---------------|----------------------------------------------|------------------------------------------------|
| `team_id`     | String (GUID) | channels, members, messages, message_replies | Parent team identifier                         |
| `channel_id`  | String (GUID) | messages, message_replies                    | Parent channel identifier                      |
| `message_id`  | String (GUID) | message_replies                              | Parent message identifier (for thread replies) |

### CDC Options (messages and message_replies)

| Option             | Type              | Default | Description                                                     |
|--------------------|-------------------|---------|----------------------------------------------------------------|
| `start_date`       | String (ISO 8601) | None    | Initial cursor for first sync (e.g., `2025-01-01T00:00:00Z`)   |
| `lookback_seconds` | String (integer)  | `300`   | Lookback window in seconds (see critical note below)           |

**⚠️ CRITICAL:** `lookback_seconds` must be >= your pipeline run frequency to avoid missing messages.

#### ⚠️ IMPORTANT: Lookback Window Must Match Run Frequency

The `lookback_seconds` parameter determines how far back from the current checkpoint the connector will fetch data on each run. **This must be at least as long as your pipeline run frequency** to avoid missing messages.

| Pipeline Frequency | Minimum `lookback_seconds` | Recommended Value  | Example                        |
|--------------------|----------------------------|--------------------|--------------------------------|
| Every 5 minutes    | `300` (5 min)              | `600` (10 min)     | Continuous/real-time ingestion |
| Every 15 minutes   | `900` (15 min)             | `1800` (30 min)    | Frequent updates               |
| Hourly             | `3600` (1 hour)            | `7200` (2 hours)   | Regular business hours sync    |
| Every 6 hours      | `21600` (6 hours)          | `43200` (12 hours) | Periodic updates               |
| Daily              | `86400` (24 hours)         | `172800` (48 hours)| Once-daily batch processing    |
| Weekly             | `604800` (7 days)          | `1209600` (14 days)| Weekly reports                 |

**Why This Matters:**

- If you run **daily** with `lookback_seconds="300"` (5 min), you'll **miss 23 hours and 55 minutes** of messages
- The lookback creates an overlap window to catch late-arriving or edited messages
- No duplicate risk: CDC deduplicates automatically using message `id` as primary key
- Longer lookback = safer, but slightly more API calls due to overlap

**Example Configurations:**

```python
# Continuous sync (every 5-10 minutes)
"lookback_seconds": "600"  # 10-minute lookback

# Hourly sync
"lookback_seconds": "7200"  # 2-hour lookback

# Daily sync (common for batch processing)
"lookback_seconds": "172800"  # 48-hour lookback (2 days)
```

### Auto-Discovery Options

| Option | Type | Default | Tables | Description |
|--------|------|---------|--------|-------------|
| `fetch_all_teams` | String (boolean) | `false` | channels, members | Set to `"true"` to automatically discover and fetch from all teams |
| `fetch_all_channels` | String (boolean) | `false` | messages | Set to `"true"` to automatically discover and fetch from all channels in the specified team |
| `fetch_all_messages` | String (boolean) | `false` | message_replies | Set to `"true"` to automatically discover and fetch replies from all messages in the specified channel |

**Note:** When using `fetch_all` modes, you don't need to specify the corresponding parent ID. For example, if `fetch_all_teams="true"`, you don't need to provide `team_id`.

---

## Best Practices

### Start Small
- Begin with a **single team** and **one or two tables** (e.g., teams + channels)
- Validate data shape and schema before expanding
- Test with a small date range for messages

### Use Incremental Sync

- For **messages** and **message_replies**, always use CDC mode with `start_date`
- Set appropriate `lookback_seconds` (default: 300 = 5 minutes) to catch late updates
- Monitor cursor progression in pipeline event logs

### Tune Batch Sizes

- Use `top=50` for messages and message_replies (API maximum)
- Use `top=100` for other tables (balance efficiency and response size)
- Adjust `max_pages_per_batch` based on dataset size (default: 100 pages)

### Respect Rate Limits
- Microsoft Graph API throttles at ~1,000-2,000 requests/minute
- The connector implements:
  - 100ms delay between requests (10 req/sec)
  - Automatic retry with exponential backoff on 429 errors
  - Respect for `Retry-After` headers
- Avoid running multiple concurrent pipelines with the same credentials

### Handle Deletions

- **Soft deletes:** Messages and message_replies have `deletedDateTime` field (filter on `deletedDateTime IS NULL` for active content)
- **Hard deletes:** Not tracked by API; rely on full re-sync or change notifications

### Schedule Appropriately

- **Snapshot tables** (teams, channels, members): Daily or weekly refresh (infrequent changes)
- **CDC tables** (messages, message_replies): Hourly or continuous (for near real-time)

---

## Troubleshooting

### Authentication Failures (401)

**Error:** `RuntimeError: Token acquisition failed: 401`

**Possible Causes:**
- Incorrect `tenant_id`, `client_id`, or `client_secret`
- Client secret has expired
- App registration doesn't exist in specified tenant

**Solutions:**
- Verify credentials in Azure Portal → App registrations
- Regenerate client secret if expired
- Ensure tenant_id matches the tenant where app is registered

---

### Permission Denied (403)

**Error:** `RuntimeError: Forbidden (403). Please verify the app has required permissions`

**Possible Causes:**
- Missing API permissions
- Admin consent not granted
- App registration doesn't have application permissions configured

**Solutions:**
- Navigate to Azure Portal → App registrations → API permissions
- Verify all required permissions are listed with type "Application"
- Click "Grant admin consent for [Your Organization]"
- Wait 5-10 minutes for permission changes to propagate

---

### Resource Not Found (404)

**Error:** `RuntimeError: Resource not found (404). Please verify team_id/channel_id`

**Possible Causes:**
- Invalid `team_id` or `channel_id`
- App doesn't have access to the specified team
- Team or channel has been deleted

**Solutions:**
- Use Microsoft Graph Explorer to verify IDs exist
- Ensure app has `Team.ReadBasic.All` permission
- Check if team is archived (archived teams may have limited access)

---

### Rate Limiting (429)

**Symptom:** Pipeline slows down or fails intermittently

**Cause:** Exceeded Microsoft Graph API rate limits

**Solutions:**
- Connector automatically handles 429 with retry logic
- Reduce `top` value to make smaller requests
- Increase `max_pages_per_batch` to spread load over time
- Avoid running multiple pipelines concurrently
- Contact Microsoft support to request higher rate limits (enterprise only)

---

## Performance Considerations

### Expected Throughput

Typical ingestion rates (approximate):

- **Teams:** 100-500 teams/minute
- **Channels:** 500-1,000 channels/minute
- **Messages:** 1,000-5,000 messages/minute (depends on nested data)
- **Members:** 500-2,000 members/minute
- **Message Replies:** 1,000-5,000 replies/minute (similar to messages)

**Factors affecting performance:**

- API rate limits (primary bottleneck)
- Network latency
- Complexity of nested data (messages/replies with many attachments/reactions)
- Concurrent pipeline runs

### Optimizing for Large Datasets

For organizations with millions of messages:
1. **Partition by team/channel:** Run separate pipelines per team
2. **Use CDC aggressively:** Always specify `start_date` to avoid full backfills
3. **Schedule off-peak:** Run during low-activity hours
4. **Monitor costs:** Graph API calls may incur costs for high-volume usage

---

## References

### Connector Documentation

- **Source Code:** [microsoft_teams.py](microsoft_teams.py)
- **Test Suite:** [test/test_microsoft_teams.py](test/test_microsoft_teams.py)
- **Generated Bundle:** [_generated_microsoft_teams_python_source.py](_generated_microsoft_teams_python_source.py)

### Microsoft Documentation
- [Microsoft Graph API Overview](https://learn.microsoft.com/en-us/graph/api/resources/teams-api-overview?view=graph-rest-1.0)
- [Teams Resource Type](https://learn.microsoft.com/en-us/graph/api/resources/team?view=graph-rest-1.0)
- [Channel Resource Type](https://learn.microsoft.com/en-us/graph/api/resources/channel?view=graph-rest-1.0)
- [ChatMessage Resource Type](https://learn.microsoft.com/en-us/graph/api/resources/chatmessage?view=graph-rest-1.0)
- [OAuth 2.0 Client Credentials Flow](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow)
- [Microsoft Graph Permissions Reference](https://learn.microsoft.com/en-us/graph/permissions-reference)

### Tools
- [Microsoft Graph Explorer](https://developer.microsoft.com/en-us/graph/graph-explorer) - Test API calls
- [Azure Portal](https://portal.azure.com) - Manage app registrations

---

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the [Prerequisites](#prerequisites) and [Configuration Examples](#configuration-examples) sections
3. Review the [Microsoft Graph API documentation](https://learn.microsoft.com/en-us/graph/api/resources/teams-api-overview)
4. Verify Azure AD app configuration and permissions
5. File an issue in the GitHub repository with:
   - Connector version
   - Error message and stack trace
   - Pipeline specification (redact credentials)
   - Steps to reproduce

---

## License

This connector is part of the Lakeflow Community Connectors project and follows the repository license.
