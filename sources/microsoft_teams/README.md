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

The Microsoft Teams connector supports **4 core tables** with two ingestion modes:

**Ingestion Modes:**
- **Snapshot (Full Refresh)**: Fetches all data on every run. Recommended for small, infrequently changing datasets (teams, channels, members).
- **CDC (Change Data Capture/Incremental)**: Only fetches new or modified records after the last cursor value. Recommended for large, frequently changing datasets (messages).

### Summary Table

| Table | Ingestion Type | Cursor Field | Parent IDs Required |
|-------|---------------|--------------|---------------------|
| teams | Snapshot | - | None |
| channels | Snapshot | - | `team_id` |
| messages | **CDC** | `lastModifiedDateTime` | `team_id`, `channel_id` |
| members | Snapshot | - | `team_id` |

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

# Run pipeline
ingest(spark, pipeline_spec)
```

**Incremental Workflow:**
1. Run with `TEAM_IDS = []` to ingest teams table
2. Query teams table, copy IDs to `TEAM_IDS` list
3. Run again to ingest channels and members for those teams
4. Query channels table, copy IDs to `CHANNEL_IDS` list
5. Run again to ingest messages from those channels

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

### CDC Options (messages only)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `start_date` | String (ISO 8601) | None | Initial cursor for first sync (e.g., `2025-01-01T00:00:00Z`) |
| `lookback_seconds` | String (integer) | `300` | Lookback window in seconds to catch late updates (5 minutes) |

---

## Best Practices

### Start Small
- Begin with a **single team** and **one or two tables** (e.g., teams + channels)
- Validate data shape and schema before expanding
- Test with a small date range for messages

### Use Incremental Sync
- For **messages**, always use CDC mode with `start_date`
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
- **CDC tables** (messages): Hourly or continuous (for near real-time)

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
