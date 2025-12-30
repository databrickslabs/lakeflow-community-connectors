# Lakeflow Microsoft Teams Community Connector

Production-grade connector for ingesting Microsoft Teams data into Databricks using the Microsoft Graph API v1.0.

## Overview

The Microsoft Teams connector enables you to:
- Extract teams, channels, messages, and members from Microsoft Teams
- Support both snapshot (full refresh) and CDC (incremental) ingestion modes
- Leverage Microsoft Graph API v1.0 with OAuth 2.0 authentication
- Ingest structured data with proper schema definitions into Delta tables

## Quick Start

For a complete step-by-step setup guide, see **[QUICKSTART.md](QUICKSTART.md)**.

## Prerequisites

### 1. Microsoft 365 Environment
- Microsoft 365 tenant with Microsoft Teams enabled
- Teams with data to ingest (teams, channels, messages)
- Administrative access to Azure AD for app registration

### 2. Azure AD App Registration
Register an application in Azure Active Directory to obtain credentials:
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

**Note:** All permissions require **tenant administrator consent**.

**⚠️ IMPORTANT - Chats Not Supported:**
- The Microsoft Graph API `/chats` endpoint does NOT support Application Permissions (app-only authentication)
- It only works with Delegated Permissions (interactive user login)
- This connector uses Application Permissions for automated/scheduled pipelines
- Therefore, **chats cannot be ingested**
- You can ingest: teams, channels, members, and messages

### 4. Databricks Environment
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
  3. Use the test script [test_replies_fetch.py](test_replies_fetch.py) to verify which messages have replies
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
# Just credentials - connector discovers everything automatically
{
    "tenant_id": "your-tenant-id",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "fetch_all_teams": "true"  # ← That's it!
}
```

### Example: Zero-Configuration Ingestion

The simplest possible configuration - ingest all teams, channels, and members without any manual ID configuration:

```python
# Your credentials
creds = {
    "tenant_id": "your-tenant-id",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret"
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

See [example_fetch_all_pipeline.py](example_fetch_all_pipeline.py) for complete working examples including:
- Minimal configuration (teams + channels + members with zero manual setup)
- Selective team with auto-discovery of channels/messages
- Complete auto-discovery with replies

### Testing

Before running in production, you can test the fetch_all modes with the provided test scripts:

```bash
# Test API patterns directly (no PySpark required)
python sources/microsoft_teams/test_fetch_all_simple.py

# Test connector implementation (requires connector dependencies)
python sources/microsoft_teams/test_connector_direct.py
```

---

## Configuration Examples

### Example 1: Configuration-Based Ingestion (Recommended)

The recommended approach uses Python configuration lists for a DLT-compatible, incremental workflow.

See the complete template in [`pipeline-spec/example_microsoft_teams_ingest.py`](../../pipeline-spec/example_microsoft_teams_ingest.py).

```python
# Configure credentials and options
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-client-id"
CLIENT_SECRET = "your-client-secret"

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
                    "tenant_id": "your-tenant-id",
                    "client_id": "your-client-id",
                    "client_secret": "your-client-secret"
                }
            }
        }
    ]
}

ingest(spark, pipeline_spec)
```

---

### Example 3: Message Replies (Threaded Conversations)

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
                    "tenant_id": "your-tenant-id",
                    "client_id": "your-client-id",
                    "client_secret": "your-client-secret",
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

2. Use the test script to verify which messages have replies:

```bash
# Edit test_replies_fetch.py with your credentials and message IDs
python sources/microsoft_teams/test_replies_fetch.py
```

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

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `start_date` | String (ISO 8601) | None | Initial cursor for first sync (e.g., `2025-01-01T00:00:00Z`) |
| `lookback_seconds` | String (integer) | `300` | Lookback window in seconds to catch late updates (5 minutes) |

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
- **Quick Start Guide:** [QUICKSTART.md](QUICKSTART.md)
- **Source Code:** [microsoft_teams.py](microsoft_teams.py)
- **API Documentation:** [microsoft_teams_api_doc.md](microsoft_teams_api_doc.md)
- **Test Suite:** [test/test_microsoft_teams.py](test/test_microsoft_teams.py)

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
2. Review the [QUICKSTART.md](QUICKSTART.md) guide
3. Review the API documentation ([microsoft_teams_api_doc.md](microsoft_teams_api_doc.md))
4. Verify Azure AD app configuration
5. File an issue in the GitHub repository with:
   - Connector version
   - Error message and stack trace
   - Pipeline specification (redact credentials)
   - Steps to reproduce

---

## License

This connector is part of the Lakeflow Community Connectors project and follows the repository license.
