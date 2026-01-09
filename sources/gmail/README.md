# Lakeflow Gmail Community Connector

This documentation describes how to configure and use the **Gmail** Lakeflow community connector to ingest email data from the Gmail API into Databricks.

## Prerequisites

- **Google Account**: A Google account with Gmail access (personal Gmail or Google Workspace).
- **Google Cloud Project**: A project in Google Cloud Console with the Gmail API enabled.
- **OAuth 2.0 Credentials**:
  - `client_id`: OAuth 2.0 client ID
  - `client_secret`: OAuth 2.0 client secret
  - `refresh_token`: Long-lived refresh token obtained via OAuth consent flow
- **Required OAuth Scope**: `https://www.googleapis.com/auth/gmail.readonly`
- **Network access**: The environment must be able to reach `https://gmail.googleapis.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `client_id` | string | yes | OAuth 2.0 client ID from Google Cloud Console | `123456789-abc.apps.googleusercontent.com` |
| `client_secret` | string | yes | OAuth 2.0 client secret | `GOCSPX-xxxx...` |
| `refresh_token` | string | yes | Long-lived refresh token from OAuth flow | `1//0xxxx...` |
| `user_id` | string | no | User email or `me` (default: `me`) | `user@gmail.com` |
| `externalOptionsAllowList` | string | no | Comma-separated list of table-specific options (optional for this connector) | `q,labelIds,maxResults,includeSpamTrash,format` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`q,labelIds,maxResults,includeSpamTrash,format`

> **Note**: Table-specific options are optional for Gmail. If you want to use query filters or customize message formats, include these option names in `externalOptionsAllowList`.

### Obtaining OAuth Credentials

#### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project or select an existing one.
3. Navigate to **APIs & Services → Library**.
4. Search for "Gmail API" and enable it.

#### Step 2: Configure OAuth Consent Screen

1. Go to **APIs & Services → OAuth consent screen**.
2. Choose **External** (for personal Gmail) or **Internal** (for Google Workspace).
3. Fill in the required app information.
4. Add the scope: `https://www.googleapis.com/auth/gmail.readonly`.
5. Add your email as a test user (for External apps in testing mode).

#### Step 3: Create OAuth 2.0 Credentials

1. Go to **APIs & Services → Credentials**.
2. Click **Create Credentials → OAuth client ID**.
3. Select **Desktop app** as the application type.
4. Download the JSON file containing `client_id` and `client_secret`.

#### Step 4: Obtain a Refresh Token

Use the [Google OAuth 2.0 Playground](https://developers.google.com/oauthplayground):

1. Click the gear icon ⚙️ and check "Use your own OAuth credentials".
2. Enter your `client_id` and `client_secret`.
3. In Step 1, select `Gmail API v1` → `https://www.googleapis.com/auth/gmail.readonly`.
4. Click "Authorize APIs" and complete the consent flow.
5. In Step 2, click "Exchange authorization code for tokens".
6. Copy the `refresh_token` from the response.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `q,labelIds,maxResults,includeSpamTrash,format` if you want to use table-specific filtering options.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Gmail connector provides **100% API coverage** with the following tables:

### Core Email Data
- `messages` - Email messages with full content
- `threads` - Email conversation threads
- `labels` - Gmail labels (system and user)
- `drafts` - Draft messages

### Account Information
- `profile` - User profile and mailbox statistics

### Settings & Configuration
- `settings` - Combined account settings (IMAP, POP, vacation, language, auto-forwarding)
- `filters` - Email filter rules
- `forwarding_addresses` - Configured forwarding addresses
- `send_as` - Send-as email aliases
- `delegates` - Delegated access users

### Object Summary, Primary Keys, and Ingestion Mode

| Table | Description | Ingestion Type | Primary Key | Cursor Field |
|-------|-------------|----------------|-------------|--------------|
| `messages` | Individual email messages with headers and body | `cdc` | `id` | `historyId` |
| `threads` | Email conversation threads containing messages | `cdc` | `id` | `historyId` |
| `labels` | Gmail labels including system and user labels | `snapshot` | `id` | n/a |
| `drafts` | Draft messages in the mailbox | `snapshot` | `id` | n/a |
| `profile` | User email, message/thread counts, historyId | `snapshot` | `emailAddress` | n/a |
| `settings` | IMAP, POP, vacation, language, forwarding settings | `snapshot` | `emailAddress` | n/a |
| `filters` | Email filter rules with criteria and actions | `snapshot` | `id` | n/a |
| `forwarding_addresses` | Email forwarding destinations | `snapshot` | `forwardingEmail` | n/a |
| `send_as` | Send-as aliases and signatures | `snapshot` | `sendAsEmail` | n/a |
| `delegates` | Users with delegated mailbox access | `snapshot` | `delegateEmail` | n/a |

### Incremental Sync Strategy

For `messages` and `threads` tables, the connector uses Gmail's **History API** for efficient incremental synchronization:

- **Initial sync**: Lists all items and captures the latest `historyId`
- **Subsequent syncs**: Uses `historyId` to fetch only added/modified items since the last sync
- **Automatic fallback**: If `historyId` expires (~30 days), automatically falls back to a full sync

### Delete Synchronization

The connector supports delete tracking for `messages` and `threads` via the `read_table_deletes` method:

- Uses Gmail History API with `historyTypes=messageDeleted`
- Returns minimal records (primary key + cursor) for deleted items
- Enables downstream systems to soft-delete or remove records

### Required and Optional Table Options

Table-specific options are passed via the pipeline spec under `table` in `objects`:

| Option | Type | Tables | Description |
|--------|------|--------|-------------|
| `q` | string | messages, threads | Gmail search query (same syntax as Gmail web UI) |
| `labelIds` | string | messages, threads | Filter by label ID (e.g., `INBOX`, `SENT`) |
| `maxResults` | integer | all | Maximum items per page (default: 100, max: 500) |
| `includeSpamTrash` | boolean | messages, threads | Include spam and trash (default: false) |
| `format` | string | messages, threads, drafts | Message format: `full` (default), `metadata`, `minimal` |

### Schema Highlights

**messages**:
- `id`, `threadId` - Unique identifiers
- `labelIds` - Array of label IDs applied to the message
- `snippet` - Short preview of message content
- `historyId` - Used for incremental sync cursor
- `internalDate` - Epoch milliseconds of message creation
- `payload` - Full parsed message structure including headers and body parts

**threads**:
- `id` - Thread identifier
- `snippet` - Preview of latest message
- `messages` - Array of messages in the thread

**labels**:
- `id`, `name` - Label identification
- `type` - `system` or `user`
- `messagesTotal`, `messagesUnread` - Message counts
- `color` - Label color settings

**drafts**:
- `id` - Draft identifier
- `message` - Full message content of the draft

**profile**:
- `emailAddress` - User's email address
- `messagesTotal`, `threadsTotal` - Mailbox counts
- `historyId` - Current history ID for sync

**settings**:
- `emailAddress` - User identifier
- `autoForwarding` - Auto-forwarding configuration
- `imap` - IMAP access settings
- `pop` - POP access settings
- `language` - Display language
- `vacation` - Vacation auto-reply settings

**filters**:
- `id` - Filter identifier
- `criteria` - Match criteria (from, to, subject, query, etc.)
- `action` - Actions (addLabelIds, removeLabelIds, forward)

**forwarding_addresses**:
- `forwardingEmail` - Forwarding destination email
- `verificationStatus` - Verification state

**send_as**:
- `sendAsEmail` - Send-as email address
- `displayName`, `signature` - Display settings
- `isPrimary`, `isDefault` - Status flags
- `verificationStatus` - Verification state

**delegates**:
- `delegateEmail` - Delegate's email address
- `verificationStatus` - Delegation status

## Data Type Mapping

| Gmail API Type | Spark SQL Type | Notes |
|----------------|----------------|-------|
| string | `StringType` | Standard text fields |
| string (int64) | `StringType` | `historyId`, `internalDate` stored as strings |
| integer | `IntegerType` | `sizeEstimate`, message counts |
| boolean | `BooleanType` | Flag fields |
| array[string] | `ArrayType(StringType)` | `labelIds` |
| array[object] | `ArrayType(StructType)` | `headers`, `parts`, `messages` |
| object | `StructType` | `payload`, `body`, `color` |

The connector:
- Preserves nested structures (headers, parts) instead of flattening
- Uses `StructType` for nested objects to enforce schema
- Treats missing nested objects as `null` (not `{}`)

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Gmail connector source in your workspace. This will place the connector code (`gmail.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code, configure a `pipeline_spec` that references:

- A **Unity Catalog connection** with your Gmail OAuth credentials
- One or more **tables** to ingest, with optional filtering options

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "gmail_connection",
    "objects": [
      {
        "table": {
          "source_table": "messages",
          "maxResults": "100",
          "q": "newer_than:7d"
        }
      },
      {
        "table": {
          "source_table": "labels"
        }
      },
      {
        "table": {
          "source_table": "threads",
          "labelIds": "INBOX",
          "maxResults": "50"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Gmail OAuth credentials.
- For each `table`:
  - `source_table` must be one of: `messages`, `threads`, `labels`, `drafts`, `profile`, `settings`, `filters`, `forwarding_addresses`, `send_as`, `delegates`
  - Optional filters like `q`, `labelIds`, `maxResults` customize what data is fetched

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration:

- **First run**: Performs a full sync of all matching items
- **Subsequent runs**: Uses `historyId` cursor for incremental updates (messages/threads only)

#### Best Practices

- **Start small**: Begin with `labels` table to validate credentials, then add `messages` with a query filter like `q=newer_than:7d`
- **Use incremental sync**: For `messages` and `threads`, rely on the CDC pattern with `historyId` cursor to minimize API calls
- **Filter by label**: Use `labelIds=INBOX` to focus on inbox messages rather than all mail
- **Choose appropriate format**: Use `format=metadata` if you only need headers, `format=full` for complete message bodies
- **Respect rate limits**: Gmail allows 25,000 queries per 100 seconds per user; the connector implements exponential backoff for 429 errors

#### Troubleshooting

**Common Issues:**

- **Authentication failures (`401`)**:
  - Verify `client_id`, `client_secret`, and `refresh_token` are correct
  - Ensure the refresh token hasn't been revoked
  - Check that the OAuth consent screen includes the `gmail.readonly` scope

- **`403 Forbidden`**:
  - The Gmail API may not be enabled in your Google Cloud project
  - For Google Workspace accounts, the admin may need to authorize the app

- **`404 Not Found` on history API**:
  - The `historyId` has expired (~30 days old)
  - The connector will automatically fall back to a full sync

- **Rate limiting (`429`)**:
  - The connector automatically retries with exponential backoff
  - If persistent, reduce `maxResults` or widen schedule intervals

- **Empty results**:
  - Check your `q` query syntax matches Gmail search syntax
  - Verify the authenticated user has emails matching your filters

## Efficiency Features

This connector includes several optimizations for high-performance data ingestion:

- **Batch API**: Uses Gmail's batch endpoint to fetch up to 50 message/thread details in a single HTTP request, reducing network overhead by up to 50x
- **Connection pooling**: Reuses HTTP sessions for multiple requests
- **Token caching**: Caches OAuth access tokens until expiration to avoid redundant token exchanges
- **Parallel fallback**: Falls back to parallel sequential requests if batch API fails

## References

- Connector implementation: `sources/gmail/gmail.py`
- Connector API documentation: `sources/gmail/gmail_api_doc.md`
- Official Gmail API documentation:
  - https://developers.google.com/workspace/gmail/api/reference/rest
  - https://developers.google.com/workspace/gmail/api/guides
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.threads
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.labels
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.drafts
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history

