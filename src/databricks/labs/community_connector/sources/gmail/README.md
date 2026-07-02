# Lakeflow Gmail Community Connector

This documentation describes how to configure and use the **Gmail** Lakeflow community connector to ingest email data from the Gmail API into Databricks.

## Prerequisites

- **Google Account**: A Google account with Gmail access (personal Gmail or Google Workspace).
- **Google Cloud Project**: A project in Google Cloud Console with the Gmail API enabled.
- **Google OAuth client** (Web application): a `client_id` + `client_secret` pair you'll register with Databricks at connection-creation time. Databricks runs the user-facing OAuth flow against this client; you never paste a refresh token into the connector yourself.
- **OAuth scope** (must be granted at consent time): `https://www.googleapis.com/auth/gmail.readonly`
- **Network access**: The cluster must be able to reach `https://gmail.googleapis.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Authentication model

This connector authenticates with Google via OAuth 2.0 on a Unity Catalog **COMMUNITY** connection. You register a Google OAuth app and supply its `client_id` + `client_secret`; Databricks runs the sign-in and consent, then obtains and refreshes the token for you. You never paste a token into the connector.

You choose between two modes when creating the connection, depending on **whose mailbox** the connection reads:

| Mode | Whose mail it reads | Good for |
|---|---|---|
| Shared mailbox (default) | One person authorizes once; every pipeline on the connection reads that mailbox. | Personal or team-shared mailboxes. |
| Per-user | Each Databricks user who queries authorizes their own Google account; each sees only their own mail. | Apps serving many users. |

The default (shared mailbox) needs no extra flag. To use per-user, pass `--auth-type u2m_per_user` when creating the connection (see Step 4).

If a token expires or is revoked mid-query, Gmail returns 401; re-run the connection setup to re-authorize.

## Setup

### Step 1 — Create a Google Cloud project + enable the Gmail API

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → **Select a project** → **New Project** (e.g. `Gmail Lakeflow Connector`).
2. **APIs & Services → Library**. Enable the **Gmail API**.

![Enable Gmail API](screenshots/gmail-api-enabled.png)

### Step 2 — Configure the OAuth consent screen

1. **APIs & Services → OAuth consent screen** → **Get started**.
2. Fill in **App information** (app name, support email) and **Developer contact information**.
3. Pick **External** (personal Gmail) or **Internal** (Workspace org).
4. **Audience**: if External, add test users (your own Gmail address while the app is in test mode).
5. **Data Access → Add or Remove Scopes**. Check `https://www.googleapis.com/auth/gmail.readonly`.
6. Save and continue.

### Step 3 — Create an OAuth 2.0 Web Application client

1. **APIs & Services → Credentials → Create Credentials → OAuth client ID**.
2. **Application type**: **Web application**.
3. Name it (e.g. `Gmail Lakeflow Web Client`).
4. **Authorized redirect URIs**: add the loopback URI the Databricks community-connector CLI listens on:
   ```
   http://localhost
   ```
   The CLI picks a free port at run time and registers `http://127.0.0.1:<port>/callback`. Google requires the host to be listed; the port is not part of the matching rule.
5. **Create**, then copy and save:
   - **Client ID** (e.g. `123456789-abc.apps.googleusercontent.com`)
   - **Client Secret** (e.g. `GOCSPX-xxxx...`)

![Create Credentials](screenshots/create-credentials.png)
![OAuth Client Config](screenshots/oauth-client-config.png)
![OAuth Credentials](screenshots/oauth-credentials-dialog.png)

### Step 4 — Create the Unity Catalog COMMUNITY connection

The connection can be created either through the Databricks UI or the
`community-connector` CLI. Either way your browser opens, you sign in to
Google and grant consent, and Databricks stores the result. You only
supply the OAuth app identity (`client_id` + `client_secret`) — the scope
and Google's endpoints come from the connector spec.

#### Option A — Databricks UI

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Supply the OAuth app identity (`client_id` + `client_secret`) and complete the in-browser Google consent when prompted; the `gmail.readonly` scope and Google's authorization / token URLs come from the connector spec.

The connection can also be created using the standard Unity Catalog API.

#### Option B — `community-connector` CLI

Run the `community-connector` CLI. Your browser opens, you sign in to Google and grant consent, and the CLI registers the result with the Databricks connection. You only supply the OAuth app identity (`client_id` + `client_secret`):

```bash
community-connector create_connection gmail gmail_connector \
  -o '{"client_id": "<YOUR_CLIENT_ID>", "client_secret": "<YOUR_CLIENT_SECRET>"}'
```

This creates a **shared-mailbox** connection (the default). For a **per-user** connection where each Databricks user reads their own mail, add `--auth-type u2m_per_user`:

```bash
community-connector create_connection gmail gmail_connector_per_user \
  --auth-type u2m_per_user \
  -o '{"client_id": "<YOUR_CLIENT_ID>", "client_secret": "<YOUR_CLIENT_SECRET>"}'
```

You only ever provide `client_id` and `client_secret`. The scope and Google's endpoints come from the connector spec, and the access token that authenticates each API call is minted and refreshed by Databricks — you never set it manually. The only other value you may set is the optional `user_id` (via the pipeline spec; defaults to `me`).

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

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

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
| integer | `LongType` | `sizeEstimate`, message counts |
| boolean | `BooleanType` | Flag fields |
| array[string] | `ArrayType(StringType)` | `labelIds` |
| array[object] | `ArrayType(StructType)` | `headers`, `parts`, `messages` |
| object | `StructType` | `payload`, `body`, `color` |

The connector:
- Preserves nested structures (headers, parts) instead of flattening
- Uses `StructType` for nested objects to enforce schema
- Treats missing nested objects as `null` (not `{}`)

## How to Run

### Step 1: Create an ingestion pipeline

1. On  the left sidebar, go to Jobs & Pipelines, the click on create Ingestion pipeline. 
2. Select 'Custom Connector' on the Community connectors section. 
3. Fill the source name of the connector `gmail` and the Git Repository URL where the connector is merged. 
![Add customer connector](screenshots/add-gmail-connector.png)
4. On connection, select the gmail connection configured on the previous steps. 
![Select gmail connection](screenshots/select-gmail-connection.png)
5. Select an event log location : catalog + schema, and a Root path where the assets related to the connector would be stored. 
![Ingestion Setup](screenshots/ingestion-setup.png)

### Step 2: Configure Your `ingest.py` File

Copy the `pipeline-spec/example_ingest.py` file and modify it for Gmail. Here's a complete example:

#### Basic Configuration (Start Here)

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# Set the connector source name
source_name = "gmail"

# Configure your pipeline
pipeline_spec = {
    "connection_name": "gmail_connection",  # Your Unity Catalog connection name
    "objects": [
        # Start with labels to validate credentials
        {
            "table": {
                "source_table": "labels",
            }
        },
    ],
}

# Register the Gmail source and run ingestion
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

#### Full Example with Multiple Tables

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "gmail"

pipeline_spec = {
    "connection_name": "gmail_connection",
    "objects": [
        # Messages - with query filter for recent emails
        {
            "table": {
                "source_table": "messages",
                "table_configuration": {
                    "q": "newer_than:7d",           # Only last 7 days
                    "maxResults": "100",            # Batch size
                    "labelIds": "INBOX",            # Only inbox
                }
            }
        },
        # Threads
        {
            "table": {
                "source_table": "threads",
                "table_configuration": {
                    "maxResults": "50",
                }
            }
        },
        # Labels (no options needed)
        {
            "table": {
                "source_table": "labels",
            }
        },
        # Profile
        {
            "table": {
                "source_table": "profile",
            }
        },
        # Settings
        {
            "table": {
                "source_table": "settings",
            }
        },
        # Drafts
        {
            "table": {
                "source_table": "drafts",
            }
        },
        # Filters
        {
            "table": {
                "source_table": "filters",
            }
        },
        # Forwarding addresses
        {
            "table": {
                "source_table": "forwarding_addresses",
            }
        },
        # Send-as aliases
        {
            "table": {
                "source_table": "send_as",
            }
        },
        # Delegates
        {
            "table": {
                "source_table": "delegates",
            }
        },
    ],
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

#### Table Configuration Options Reference

| Option | Type | Tables | Description |
|--------|------|--------|-------------|
| `q` | string | messages, threads | Gmail search query (e.g., `newer_than:7d`, `from:user@example.com`) |
| `labelIds` | string | messages, threads | Filter by label ID (`INBOX`, `SENT`, `DRAFT`, `SPAM`, `TRASH`) |
| `maxResults` | string | all | Max items per page (`"100"`, max `"500"`) |
| `includeSpamTrash` | string | messages, threads | Include spam/trash (`"true"` or `"false"`) |
| `format` | string | messages, threads, drafts | Message format (`"full"`, `"metadata"`, `"minimal"`) |

#### Available Tables Quick Reference

| Table | Description | Recommended First Test |
|-------|-------------|------------------------|
| `labels` | Gmail labels | ✅ Start here (simple, validates auth) |
| `profile` | User profile info | ✅ Single record |
| `messages` | Email messages | Use with `q` filter |
| `threads` | Email threads | Use with `maxResults` |
| `drafts` | Draft messages | |
| `settings` | Account settings | |
| `filters` | Email filter rules | |
| `forwarding_addresses` | Forwarding config | |
| `send_as` | Send-as aliases | |
| `delegates` | Delegated access | |

> **Tip**: Start with the `labels` table to validate your credentials before ingesting larger tables like `messages` or `threads`.

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

**Pipeline Configuration Issues:**

- **"Connection not found"**:
  - Verify `connection_name` in your `pipeline_spec` matches your Unity Catalog connection exactly
  - Check that the connection was created successfully in Unity Catalog

- **"Table not supported"**:
  - Check spelling of `source_table` - must be one of the 10 supported tables
  - Supported tables: `messages`, `threads`, `labels`, `drafts`, `profile`, `settings`, `filters`, `forwarding_addresses`, `send_as`, `delegates`

- **"Source not found" or module import errors**:
  - Ensure `source_name = "gmail"` is set correctly
  - Verify the Gmail connector files are in `src/databricks/labs/community_connector/sources/gmail/` directory

**API and Authentication Issues:**

- **Authentication failures (`401 Unauthorized`)**:
  - The injected `access_token` has expired or been revoked — re-run the connection setup to sign in and re-authorize
  - Verify the connection's `client_id` and `client_secret` match the OAuth client registered in Google Cloud Console
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

- **Parallel fetching**: Uses ThreadPoolExecutor with 5 workers to fetch message/thread details concurrently
- **Connection pooling**: Reuses HTTP sessions for multiple requests
- **Token caching**: Caches OAuth access tokens until expiration to avoid redundant token exchanges
- **Graceful error handling**: Handles 403 errors gracefully for restricted endpoints

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/gmail/gmail.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/gmail/gmail_api_doc.md`
- Official Gmail API documentation:
  - https://developers.google.com/workspace/gmail/api/reference/rest
  - https://developers.google.com/workspace/gmail/api/guides
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.threads
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.labels
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.drafts
  - https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history

