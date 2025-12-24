# Lakeflow Microsoft Teams Community Connector

This documentation describes how to configure and use the **Microsoft Teams** Lakeflow community connector to ingest data from Microsoft Teams into Databricks using the Microsoft Graph API.

## Overview

The Microsoft Teams connector enables you to:
- Extract teams, channels, messages, members, and chats from Microsoft Teams
- Support both snapshot (full refresh) and CDC (incremental) ingestion modes
- Leverage Microsoft Graph API v1.0 for reliable data access
- Ingest structured data with proper schema definitions into Databricks

## Prerequisites

### 1. Microsoft 365 Environment
- **Microsoft 365 tenant** with Microsoft Teams enabled
- **Teams with data** to ingest (teams, channels, messages)
- **Administrative access** to Azure AD for app registration

### 2. Azure AD App Registration
You need to register an application in Azure Active Directory to obtain credentials:
- **Tenant ID**: Your Azure AD tenant identifier
- **Client ID**: Application (client) ID from app registration
- **Client Secret**: A valid client secret for authentication

### 3. Required API Permissions

The following **Application permissions** must be granted with admin consent:

| Permission | Required For | Admin Consent |
|------------|--------------|---------------|
| `Team.ReadBasic.All` | Reading teams | Required |
| `Channel.ReadBasic.All` | Reading channels | Required |
| `ChannelMessage.Read.All` | Reading messages | Required |
| `TeamMember.Read.All` | Reading members | Required |
| `Chat.Read.All` | Reading chats | Required |

**Note:** All permissions require **tenant administrator consent**.

### 4. Databricks Environment
- Databricks workspace with Unity Catalog enabled
- Permissions to create UC connections
- SDP (Spark Declarative Pipeline) support

---

## Setup Guide

### Step 1: Register Application in Azure AD

1. **Navigate to Azure Portal**
   - Go to https://portal.azure.com
   - Sign in with administrator credentials

2. **Create App Registration**
   - Navigate to **Azure Active Directory** → **App registrations**
   - Click **New registration**
   - Enter a name: `Databricks Teams Connector`
   - Select **Accounts in this organizational directory only**
   - Click **Register**

3. **Note the Credentials**
   - On the Overview page, copy:
     - **Application (client) ID** → This is your `client_id`
     - **Directory (tenant) ID** → This is your `tenant_id`

4. **Create Client Secret**
   - Navigate to **Certificates & secrets** → **Client secrets**
   - Click **New client secret**
   - Enter description: `Databricks Connector Secret`
   - Set expiration (recommended: 24 months)
   - Click **Add**
   - **IMPORTANT:** Copy the **Value** immediately (you won't see it again) → This is your `client_secret`

5. **Grant API Permissions**
   - Navigate to **API permissions**
   - Click **Add a permission** → **Microsoft Graph** → **Application permissions**
   - Add each required permission:
     - `Team.ReadBasic.All`
     - `Channel.ReadBasic.All`
     - `ChannelMessage.Read.All`
     - `TeamMember.Read.All`
     - `Chat.Read.All`
   - Click **Grant admin consent for [Your Organization]**
   - Verify all permissions show "Granted for [Your Organization]"

### Step 2: Create Unity Catalog Connection

Create a UC connection for the Microsoft Teams connector using Databricks CLI or UI.

#### Option A: Using Databricks CLI

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

#### Option B: Using Databricks UI

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

### Step 3: Create Ingestion Pipeline

1. **Navigate to Databricks**
   - Click **New** → **Add or upload data**
   - Select **Microsoft Teams** under Community connectors

2. **Configure Pipeline**
   - **Pipeline name**: `microsoft_teams_ingestion`
   - **Event log location**: Catalog and schema for event logs (e.g., `main.teams_logs`)
   - **Root path**: Workspace directory for connector source code (e.g., `/Workspace/Shared/connectors/microsoft_teams`)
   - Click **Create pipeline**

3. **Edit Pipeline Spec** (in `ingest.py`)
   - Update the `objects` field to specify tables to ingest
   - Configure table options (see Configuration Examples below)

4. **Run Pipeline**
   - Click **Start** to begin ingestion
   - Monitor progress in pipeline UI

---

## Supported Objects (Tables)

The Microsoft Teams connector supports **5 core tables**:

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

**Ingestion Type:** Snapshot (full refresh)
**Primary Key:** `id`
**Required Table Options:** None

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

**Ingestion Type:** Snapshot (full refresh)
**Primary Key:** `id`
**Required Table Options:** `team_id`

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

**Ingestion Type:** CDC (incremental with Change Data Capture)
**Primary Key:** `id`
**Cursor Field:** `lastModifiedDateTime`
**Required Table Options:** `team_id`, `channel_id`

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

**Ingestion Type:** Snapshot (full refresh)
**Primary Key:** `id`
**Required Table Options:** `team_id`

---

### 5. chats (CDC)

1:1 and group chats.

| Field | Type | Description |
|-------|------|-------------|
| id | String | Unique chat identifier |
| topic | String | Chat subject (for group chats) |
| chatType | String | `oneOnOne`, `group`, or `meeting` |
| createdDateTime | String | ISO 8601 timestamp |
| lastUpdatedDateTime | String | Last activity timestamp (cursor field) |
| webUrl | String | URL to chat in Teams client |
| onlineMeetingInfo | String | JSON string with meeting details |

**Ingestion Type:** CDC (incremental)
**Primary Key:** `id`
**Cursor Field:** `lastUpdatedDateTime`
**Required Table Options:** None

---

## Configuration Examples

### Example 1: Basic Teams and Channels (Snapshot)

Ingest all teams and channels from a specific team:

```python
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
                    "team_id": "abc-123-team-id-guid"
                }
            }
        }
    ]
}
```

### Example 2: Incremental Message Ingestion (CDC)

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
                "destination_table": "channel_messages",
                "table_configuration": {
                    "team_id": "abc-123-team-id-guid",
                    "channel_id": "xyz-789-channel-id",
                    "start_date": "2025-01-01T00:00:00Z",
                    "lookback_seconds": "300"
                }
            }
        }
    ]
}
```

### Example 3: Multiple Tables with Custom Settings

Ingest teams, channels, members, and messages:

```python
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "teams",
                "destination_catalog": "main",
                "destination_schema": "teams_data"
            }
        },
        {
            "table": {
                "source_table": "channels",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "team_id": "abc-123-team-id-guid",
                    "top": "50"
                }
            }
        },
        {
            "table": {
                "source_table": "members",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "team_id": "abc-123-team-id-guid"
                }
            }
        },
        {
            "table": {
                "source_table": "messages",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "team_id": "abc-123-team-id-guid",
                    "channel_id": "xyz-789-channel-id",
                    "start_date": "2024-12-01T00:00:00Z",
                    "max_pages_per_batch": "100"
                }
            }
        }
    ]
}
```

### Example 4: Chats Ingestion

Ingest all chats accessible by the application:

```python
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "chats",
                "destination_catalog": "main",
                "destination_schema": "teams_data",
                "table_configuration": {
                    "start_date": "2025-01-01T00:00:00Z",
                    "top": "50"
                }
            }
        }
    ]
}
```

---

## Table Configuration Options

### Common Options (All Tables)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `top` | String (integer) | `50` | Page size for pagination (max: 50 for messages, 999 for others) |
| `max_pages_per_batch` | String (integer) | `100` | Maximum pages to fetch per batch (safety limit) |

### Parent-Child Options

| Option | Type | Required For | Description |
|--------|------|--------------|-------------|
| `team_id` | String (GUID) | channels, members, messages | Parent team identifier |
| `channel_id` | String (GUID) | messages | Parent channel identifier |

### CDC Options (messages, chats)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `start_date` | String (ISO 8601) | None | Initial cursor for first sync (e.g., `2025-01-01T00:00:00Z`) |
| `lookback_seconds` | String (integer) | `300` | Lookback window in seconds to catch late updates (5 minutes) |

---

## How to Find team_id and channel_id

### Option 1: Using Microsoft Graph Explorer

1. Go to https://developer.microsoft.com/en-us/graph/graph-explorer
2. Sign in with your account
3. **Find team_id:**
   - Run: `GET https://graph.microsoft.com/v1.0/me/joinedTeams`
   - Copy the `id` field from the desired team
4. **Find channel_id:**
   - Run: `GET https://graph.microsoft.com/v1.0/teams/{team-id}/channels`
   - Copy the `id` field from the desired channel

### Option 2: Using Teams Web Client

1. Open Microsoft Teams in your browser
2. Navigate to the desired team and channel
3. Look at the URL in the address bar:
   ```
   https://teams.microsoft.com/...?teamId=<TEAM_ID>...&channelId=<CHANNEL_ID>
   ```
4. Extract the `teamId` and `channelId` values

**Note:** Channel IDs in URLs may be URL-encoded. Use the Graph Explorer method for accurate IDs.

---

## Data Type Mapping

Microsoft Graph JSON types are mapped to PySpark/Databricks types:

| Graph API Type | Example Fields | PySpark Type | Notes |
|----------------|----------------|--------------|-------|
| String (GUID) | `id`, `userId`, `teamId` | `StringType` | UUIDs stored as strings |
| String | `displayName`, `description` | `StringType` | UTF-8 text |
| DateTimeOffset | `createdDateTime`, `lastModifiedDateTime` | `StringType` | ISO 8601 format (e.g., `2025-01-15T10:30:00.000Z`) |
| Boolean | `isArchived`, `isFavoriteByDefault` | `BooleanType` | `true` / `false` |
| Enum | `visibility`, `chatType`, `messageType` | `StringType` | Enum values as strings |
| Object | `from`, `body`, `channelIdentity` | `StructType([...])` | Nested struct with defined fields |
| Array[Object] | `attachments`, `mentions`, `reactions` | `ArrayType(StructType([...]))` | Array of structs |
| Array[String] | `roles`, `topics` | `ArrayType(StringType())` | Simple string arrays |
| Complex/Unknown | `policyViolation`, `eventDetail` | `StringType` | Stored as JSON string for flexibility |

**Timestamp Handling:**
- All timestamps are stored as **strings** in ISO 8601 format with UTC timezone
- Downstream processing can cast to `TimestampType` as needed
- Example: `2025-01-15T10:30:00.000Z`

---

## Best Practices

### Start Small
- Begin with a **single team** and **one or two tables** (e.g., teams + channels)
- Validate data shape and schema before expanding
- Test with a small date range for messages

### Use Incremental Sync
- For **messages** and **chats**, always use CDC mode with `start_date`
- Set appropriate `lookback_seconds` (default: 300 = 5 minutes) to catch late updates
- Monitor cursor progression in pipeline event logs

### Tune Batch Sizes
- Use `top=50` for messages (API maximum)
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
- **Soft deletes:** Messages have `deletedDateTime` field (filter on `deletedDateTime IS NULL` for active messages)
- **Hard deletes:** Not tracked by API; rely on full re-sync or change notifications

### Schedule Appropriately
- **Snapshot tables** (teams, channels, members): Daily or weekly refresh (infrequent changes)
- **CDC tables** (messages, chats): Hourly or continuous (for near real-time)

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

### Schema Mismatches

**Symptom:** Pipeline fails with schema errors

**Cause:** Nested objects or arrays don't match expected schema

**Solutions:**
- Verify you're using the latest connector version
- Check if Microsoft has changed Graph API schema
- File an issue in the connector GitHub repository
- As workaround, store problematic fields as JSON strings

---

### Empty Results

**Symptom:** Pipeline runs successfully but no data ingested

**Possible Causes:**
- No data exists in specified team/channel
- `start_date` is in the future
- Filters exclude all data
- App doesn't have access to data

**Solutions:**
- Verify team/channel has messages using Teams client
- Check `start_date` is in the past
- Remove or adjust filters
- Verify app has appropriate permissions

---

## Performance Considerations

### Expected Throughput

Typical ingestion rates (approximate):
- **Teams:** 100-500 teams/minute
- **Channels:** 500-1,000 channels/minute
- **Messages:** 1,000-5,000 messages/minute (depends on nested data)
- **Members:** 500-2,000 members/minute

**Factors affecting performance:**
- API rate limits (primary bottleneck)
- Network latency
- Complexity of nested data (messages with many attachments/reactions)
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
- **Source Code:** `sources/microsoft_teams/microsoft_teams.py`
- **API Documentation:** `sources/microsoft_teams/microsoft_teams_api_doc.md`
- **Test Suite:** `sources/microsoft_teams/test/test_microsoft_teams.py`

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
2. Review the API documentation (`microsoft_teams_api_doc.md`)
3. Verify Azure AD app configuration
4. File an issue in the GitHub repository with:
   - Connector version
   - Error message and stack trace
   - Pipeline specification (redact credentials)
   - Steps to reproduce

---

## License

This connector is part of the Lakeflow Community Connectors project and follows the repository license.
