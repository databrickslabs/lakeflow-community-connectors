# Lakeflow Gmail Community Connector

This documentation describes how to configure and use the **Gmail** Lakeflow community connector to ingest email data from Gmail/Google Workspace into Databricks.

## Overview

The Gmail connector enables ingestion of email messages, threads, labels, drafts, and user profile data into your lakehouse. It supports both full synchronization and efficient incremental updates using Gmail's History API, making it ideal for:

- Customer support analytics and response time tracking
- Sales communication analysis and pipeline insights
- Compliance and eDiscovery requirements
- Email-based workflow analysis
- Security and anomaly detection

## Prerequisites

- **Gmail or Google Workspace account**: You need a Google account with access to the mailbox you want to ingest.
- **OAuth 2.0 credentials**:
  - Created in Google Cloud Console for your Gmail API project
  - Requires `gmail.readonly` scope for read-only access
  - Can be user OAuth (personal Gmail) or service account (Google Workspace with domain-wide delegation)
- **Network access**: The environment running the connector must be able to reach `https://www.googleapis.com/`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name            | Type   | Required | Description                                                                                 | Example                            |
|-----------------|--------|----------|---------------------------------------------------------------------------------------------|------------------------------------|
| `client_id`     | string | yes      | OAuth 2.0 Client ID from Google Cloud Console                                             | `123456789.apps.googleusercontent.com` |
| `client_secret` | string | yes      | OAuth 2.0 Client Secret from Google Cloud Console                                         | `GOCSPX-xxx...`                   |
| `refresh_token` | string | yes      | OAuth 2.0 refresh token obtained through authorization flow                                | `1//xxx...`                       |
| `user_id`       | string | no       | Gmail user ID to access. Defaults to `"me"` (the authenticated user)                       | `me` or specific email address    |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to be passed through          | `q,labelIds,includeSpamTrash,format` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`q,labelIds,includeSpamTrash,format`

> **Note**: Table-specific options such as `labelIds` or `q` (search query) are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

#### Step 1: Create OAuth 2.0 Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Gmail API:
   - Navigate to **APIs & Services → Library**
   - Search for "Gmail API" and click **Enable**
4. Configure OAuth consent screen:
   - Go to **APIs & Services → OAuth consent screen**
   - Choose "External" user type (or "Internal" for Google Workspace)
   - Fill in required fields (app name, support email, developer email)
   - Add scope: `https://www.googleapis.com/auth/gmail.readonly`
   - Add test users (the Gmail accounts you want to access)
5. Create OAuth 2.0 Client:
   - Go to **APIs & Services → Credentials**
   - Click **+ CREATE CREDENTIALS → OAuth client ID**
   - Application type: **Desktop app**
   - Name: "Gmail Connector" (or any descriptive name)
   - Click **CREATE**
   - Copy the **Client ID** and **Client Secret**

#### Step 2: Obtain Refresh Token

Use the provided helper script to obtain a refresh token:

```bash
# Navigate to the connector directory
cd /path/to/lakeflow-community-connectors

# Run the authentication helper
python sources/gmail/get_refresh_token.py
```

The script will:
1. Prompt for your Client ID and Client Secret
2. Open a browser for you to authorize access
3. Generate a refresh token
4. Save configuration to `sources/gmail/configs/dev_config.json`

**Alternative: Manual OAuth Flow**

If you prefer to use OAuth Playground or another method:
1. Visit [OAuth 2.0 Playground](https://developers.google.com/oauthplayground/)
2. Configure with your OAuth credentials
3. Authorize the `gmail.readonly` scope
4. Exchange authorization code for refresh token

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page
2. Select "Gmail" under community connectors or "+ Add Community Connector"
3. Click "+ Create connection" to set up a new UC connection
4. Provide the OAuth credentials (`client_id`, `client_secret`, `refresh_token`)
5. Set `externalOptionsAllowList` to `q,labelIds,includeSpamTrash,format`
6. Set `user_id` to `me` (or specific email address for service accounts)

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Gmail connector exposes **5 tables**:

- `messages` - Email messages with full content and metadata
- `threads` - Email conversation threads
- `labels` - Gmail labels (INBOX, SENT, custom labels, etc.)
- `drafts` - Draft messages
- `profile` - User profile information

### Object summary, primary keys, and ingestion mode

| Table      | Description                                           | Ingestion Type | Primary Key       | Incremental Cursor (if any) |
|------------|-------------------------------------------------------|----------------|-------------------|-----------------------------|
| `messages` | Email messages with headers, body, and attachments    | `cdc`          | `id` (string)     | `historyId`                 |
| `threads`  | Email threads with all messages in conversation       | `cdc`          | `id` (string)     | `historyId`                 |
| `labels`   | Gmail labels with metadata and message counts         | `snapshot`     | `id` (string)     | n/a                         |
| `drafts`   | Draft messages not yet sent                           | `snapshot`     | `id` (string)     | n/a                         |
| `profile`  | User profile with email address and statistics        | `snapshot`     | `emailAddress`    | n/a                         |

### Incremental Sync (CDC) Details

The `messages` and `threads` tables support efficient incremental synchronization using Gmail's History API:

- **Initial Sync**: Fetches all messages/threads and records the maximum `historyId`
- **Incremental Sync**: Uses the History API with the previous `historyId` to fetch only changed messages
- **Automatic Fallback**: If the `historyId` expires (older than ~30 days), automatically falls back to full sync
- **Efficiency**: Minimizes API calls and data transfer by only syncing changes

### Required and optional table options

Table-specific options are passed via the pipeline spec under `table` in `objects`:

#### Messages Table Options

| Option            | Type    | Required | Description                                                                          | Example             |
|-------------------|---------|----------|--------------------------------------------------------------------------------------|---------------------|
| `q`               | string  | no       | Gmail search query to filter messages                                                | `from:example.com`  |
| `labelIds`        | string  | no       | Comma-separated list of label IDs to filter messages                                 | `INBOX,IMPORTANT`   |
| `includeSpamTrash`| string  | no       | Include messages in SPAM and TRASH. Set to "true" to include. Default: "false"      | `true`              |

**Common search query examples**:
- `from:user@example.com` - Messages from specific sender
- `to:me` - Messages sent to you
- `subject:invoice` - Messages with "invoice" in subject
- `has:attachment` - Messages with attachments
- `after:2024/01/01 before:2024/01/31` - Messages in date range
- `is:unread` - Unread messages

#### Threads Table Options

| Option            | Type    | Required | Description                                                                          | Example             |
|-------------------|---------|----------|--------------------------------------------------------------------------------------|---------------------|
| `q`               | string  | no       | Gmail search query to filter threads                                                 | `from:example.com`  |
| `labelIds`        | string  | no       | Comma-separated list of label IDs to filter threads                                  | `INBOX`             |
| `includeSpamTrash`| string  | no       | Include threads in SPAM and TRASH. Set to "true" to include. Default: "false"       | `true`              |

#### Labels Table Options

No table-specific options required. Fetches all labels for the user.

#### Drafts Table Options

No table-specific options required. Fetches all drafts for the user.

#### Profile Table Options

No table-specific options required. Fetches profile information for the user.

## Schema Details

### Messages Schema

The messages table contains rich nested structures:

- **id** (string): Unique message ID
- **threadId** (string): Thread this message belongs to
- **labelIds** (array<string>): Labels applied to this message
- **snippet** (string): Short preview of message content
- **historyId** (string): History ID for incremental sync
- **internalDate** (long): Message timestamp in milliseconds
- **sizeEstimate** (long): Estimated size in bytes
- **payload** (struct): Message content structure
  - **partId** (string): Part identifier
  - **mimeType** (string): MIME type (e.g., "text/html", "text/plain")
  - **filename** (string): Filename (for attachments)
  - **headers** (array<struct>): Email headers
    - **name** (string): Header name (e.g., "From", "To", "Subject")
    - **value** (string): Header value
  - **body** (struct): Message body
    - **attachmentId** (string): Attachment ID if applicable
    - **size** (long): Body size in bytes
    - **data** (string): Base64-encoded body content
  - **parts** (string): Nested parts (for multipart messages) as JSON string

### Threads Schema

- **id** (string): Unique thread ID
- **snippet** (string): Thread snippet/preview
- **historyId** (string): History ID for incremental sync
- **messages** (array<message_struct>): All messages in the thread

### Labels Schema

- **id** (string): Label ID (e.g., "INBOX", "SENT", or custom ID)
- **name** (string): Label display name
- **type** (string): Label type ("system" or "user")
- **messageListVisibility** (string): Visibility in message list
- **labelListVisibility** (string): Visibility in label list
- **messagesTotal** (long): Total messages with this label
- **messagesUnread** (long): Unread messages with this label
- **threadsTotal** (long): Total threads with this label
- **threadsUnread** (long): Unread threads with this label
- **color** (struct): Label color configuration

### Drafts Schema

- **id** (string): Draft ID
- **message** (message_struct): The draft message content (same structure as messages table)

### Profile Schema

- **emailAddress** (string): User's email address
- **messagesTotal** (long): Total message count in mailbox
- **threadsTotal** (long): Total thread count in mailbox
- **historyId** (string): Current history ID

## Configuration Example

Example pipeline specification in `ingest.py`:

```python
from databricks.labs.lakeflow.community_connector.config import CommunityConnectorPipelineSpec

pipeline_spec = CommunityConnectorPipelineSpec(
    connection_name="gmail_connection",
    objects=[
        {
            "source_table": "messages",
            "destination_catalog": "email_analytics",
            "destination_schema": "gmail",
            "destination_table": "messages",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1",
                # Filter for customer support emails only
                "labelIds": "Customer Support",
                # Include spam/trash for complete audit trail
                "includeSpamTrash": "true"
            }
        },
        {
            "source_table": "threads",
            "destination_catalog": "email_analytics",
            "destination_schema": "gmail",
            "destination_table": "threads",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1",
                # Filter for inbox conversations
                "labelIds": "INBOX"
            }
        },
        {
            "source_table": "labels",
            "destination_catalog": "email_analytics",
            "destination_schema": "gmail",
            "destination_table": "labels",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1"
            }
        },
        {
            "source_table": "profile",
            "destination_catalog": "email_analytics",
            "destination_schema": "gmail",
            "destination_table": "profile",
            "table_configuration": {
                "scd_type": "SCD_TYPE_1"
            }
        }
    ]
)
```

## Use Cases

### Customer Support Analytics

```sql
-- Average response time for support tickets
SELECT
  DATE(FROM_UNIXTIME(CAST(internalDate AS BIGINT)/1000)) as date,
  AVG(response_time_hours) as avg_response_hours
FROM (
  SELECT
    threadId,
    MIN(CAST(internalDate AS BIGINT)) as first_message,
    MAX(CAST(internalDate AS BIGINT)) as last_message,
    (MAX(CAST(internalDate AS BIGINT)) - MIN(CAST(internalDate AS BIGINT))) / 3600000 as response_time_hours
  FROM gmail_messages
  WHERE array_contains(labelIds, 'Customer Support')
  GROUP BY threadId
)
GROUP BY date
ORDER BY date DESC;
```

### Sales Pipeline Analysis

```sql
-- Email engagement by prospect
SELECT
  header.value as from_email,
  COUNT(*) as email_count,
  MAX(FROM_UNIXTIME(CAST(internalDate AS BIGINT)/1000)) as last_contact
FROM gmail_messages
LATERAL VIEW explode(payload.headers) as header
WHERE header.name = 'From'
  AND array_contains(labelIds, 'Sales')
GROUP BY header.value
ORDER BY email_count DESC;
```

### Compliance and Audit

```sql
-- All emails for a specific date range (eDiscovery)
SELECT
  m.id,
  FROM_UNIXTIME(CAST(m.internalDate AS BIGINT)/1000) as received_date,
  (SELECT value FROM LATERAL VIEW explode(m.payload.headers) WHERE name = 'From') as from_email,
  (SELECT value FROM LATERAL VIEW explode(m.payload.headers) WHERE name = 'Subject') as subject,
  m.snippet
FROM gmail_messages m
WHERE FROM_UNIXTIME(CAST(m.internalDate AS BIGINT)/1000) BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY m.internalDate;
```

## Rate Limits and Performance

- **Gmail API Quotas**: 1.2M quota units per minute per project (15K per minute per user)
- **Typical costs**: Listing messages (5 units), Getting full message (5 units), History API (2 units)
- **Batch size**: Connector fetches up to 500 messages per API call
- **Recommendation**: For large mailboxes (>100K messages), initial sync may take 30-60 minutes

## Troubleshooting

### Authentication Errors

**Error**: "Invalid credentials" or "Token expired"
- **Solution**: Regenerate refresh token using `get_refresh_token.py` script

**Error**: "Access blocked: This app's request is invalid"
- **Solution**: Ensure you added the Gmail account as a test user in OAuth consent screen

### Rate Limiting

**Error**: "Quota exceeded" or 429 status codes
- **Solution**: Connector includes automatic retry logic. If persistent, contact Google Cloud support to request quota increase

### History ID Expired

**Error**: "historyId not found" or 404 errors
- **Solution**: Connector automatically falls back to full sync. History IDs expire after ~30 days of inactivity

### Missing Emails

**Issue**: Some emails not appearing in results
- **Solution**: Check `includeSpamTrash` setting and label filters. By default, SPAM and TRASH are excluded

## Security Considerations

- **OAuth Tokens**: Refresh tokens provide long-term access. Store securely in Unity Catalog connections (encrypted)
- **Read-Only Access**: Connector only requires `gmail.readonly` scope - cannot send, delete, or modify emails
- **Service Accounts**: For Google Workspace, consider service accounts with domain-wide delegation for centralized control
- **Audit Logging**: All API access is logged in Google Workspace admin console (for Workspace accounts)

## Additional Resources

- [Gmail API Documentation](https://developers.google.com/gmail/api)
- [OAuth 2.0 for Desktop Apps](https://developers.google.com/identity/protocols/oauth2/native-app)
- [Gmail API Quotas and Limits](https://developers.google.com/gmail/api/reference/quota)
- [Setup Guide](./SETUP_GUIDE.md) - Step-by-step OAuth configuration
- [API Research Documentation](./gmail_api_doc.md) - Detailed API analysis
- [Demo Access Guide](./DEMO_ACCESS.md) - Quick start for testing

## Support

For issues or questions:
- Review the [SETUP_GUIDE.md](./SETUP_GUIDE.md) for detailed OAuth setup
- Check the test suite: `pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v`
- Refer to Gmail API documentation for API-specific questions
- Contact the Lakeflow Community Connector team for connector-specific issues
