# Lakeflow Outlook Mail Community Connector

This documentation provides setup instructions and reference information for the **Outlook Mail** Lakeflow community connector. This connector enables you to ingest email messages and folder metadata from Microsoft Outlook (Office 365 and Outlook.com) into Databricks using the Microsoft Graph Mail API.

## Prerequisites

- **Microsoft Account**: A Microsoft 365 (Office 365) or Outlook.com account with email access
- **Azure AD Application Registration**: An app registered in Azure Active Directory with appropriate permissions
- **OAuth 2.0 Credentials**:
  - **Client ID**: Application (client) ID from your Azure AD app registration
  - **Client Secret**: Client secret generated for your Azure AD application
  - **Refresh Token**: OAuth 2.0 refresh token obtained through initial user authorization
- **API Permissions**: Your Azure AD application must be granted the following Microsoft Graph permissions:
  - `Mail.Read` (read user mail) or `Mail.ReadBasic` (read basic mail properties)
  - `offline_access` (maintain access to data via refresh token)
- **Network Access**: The environment running the connector must be able to reach:
  - `https://login.microsoftonline.com` (authentication)
  - `https://graph.microsoft.com` (Microsoft Graph API)
- **Lakeflow / Databricks Environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `client_id` | string | yes | Application (client) ID from Azure AD app registration | `a1b2c3d4-e5f6-7890-abcd-ef1234567890` |
| `client_secret` | string | yes | Client secret from Azure AD app registration | `abc123def456...` |
| `refresh_token` | string | yes | OAuth 2.0 refresh token obtained during initial user authorization | `0.AXEA...` |
| `tenant` | string | no | Tenant ID or `common` for multi-tenant authentication. Defaults to `common`. | `common` or `contoso.onmicrosoft.com` |
| `base_url` | string | no | Base URL for Microsoft Graph API. Override only for specialized deployments. Defaults to `https://graph.microsoft.com/v1.0`. | `https://graph.microsoft.com/v1.0` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed. This connector requires table-specific options, so this parameter must be set. | `folder_id,folder_name,top,start_date,lookback_seconds,max_pages_per_batch,include_drafts` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`folder_id,folder_name,top,start_date,lookback_seconds,max_pages_per_batch,include_drafts`

> **Note**: Table-specific options such as `folder_id` or `folder_name` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

#### Step 1: Register an Application in Azure AD

1. Sign in to the [Azure Portal](https://portal.azure.com)
2. Navigate to **Azure Active Directory** → **App registrations**
3. Click **New registration**
4. Provide:
   - **Name**: A descriptive name (e.g., "Lakeflow Outlook Connector")
   - **Supported account types**: Choose based on your needs:
     - "Accounts in this organizational directory only" (single tenant)
     - "Accounts in any organizational directory" (multi-tenant)
     - "Accounts in any organizational directory and personal Microsoft accounts" (for both Office 365 and Outlook.com)
   - **Redirect URI**: Set to `https://login.microsoftonline.com/common/oauth2/nativeclient` or your application's redirect URI
5. Click **Register**
6. Copy the **Application (client) ID** - this is your `client_id`

#### Step 2: Create a Client Secret

1. In your registered application, navigate to **Certificates & secrets**
2. Click **New client secret**
3. Provide a description and select an expiration period
4. Click **Add**
5. **Important**: Copy the secret **Value** immediately - this is your `client_secret`. You won't be able to see it again.

#### Step 3: Configure API Permissions

1. In your registered application, navigate to **API permissions**
2. Click **Add a permission** → **Microsoft Graph** → **Delegated permissions**
3. Add the following permissions:
   - `Mail.Read` (or `Mail.ReadBasic` for basic properties only)
   - `offline_access` (required for refresh token)
4. Click **Add permissions**
5. (Optional) Click **Grant admin consent** if you have admin privileges and want to pre-approve for all users

#### Step 4: Obtain the Refresh Token

The refresh token must be obtained through an OAuth 2.0 authorization flow. You can use one of these methods:

**Option A: Using Microsoft's OAuth 2.0 Playground or Postman**
1. Use the authorization endpoint: `https://login.microsoftonline.com/common/oauth2/v2.0/authorize`
2. Required parameters:
   - `client_id`: Your application's client ID
   - `response_type`: `code`
   - `redirect_uri`: Your registered redirect URI
   - `scope`: `Mail.Read offline_access`
3. After user consent, exchange the authorization code for tokens at: `https://login.microsoftonline.com/common/oauth2/v2.0/token`
4. Copy the `refresh_token` from the response

**Option B: Programmatic Flow**
Use Microsoft's authentication libraries (MSAL) to implement the OAuth flow and obtain the refresh token programmatically.

> **Security Note**: Store the `client_secret` and `refresh_token` securely. Never commit them to version control. Consider using Databricks Secrets or Azure Key Vault.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one
3. Set `externalOptionsAllowList` to `folder_id,folder_name,top,start_date,lookback_seconds,max_pages_per_batch,include_drafts` (required for this connector to pass table-specific options)

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Outlook Mail connector exposes a **static list** of tables:

- `messages`
- `mailfolders`

### Object Summary, Primary Keys, and Ingestion Mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|-------|-------------|----------------|-------------|-------------------|
| `messages` | Email messages in the user's mailbox | `cdc` | `id` (string) | `lastModifiedDateTime` |
| `mailfolders` | Mail folder hierarchy (Inbox, Sent Items, Deleted Items, etc.) | `snapshot` | `id` (string) | n/a |

### Required and Optional Table Options

Table-specific options are passed via the pipeline spec under `table` in `objects`.

#### `messages` Table Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `folder_id` | string | no | none | Target folder by ID. If omitted along with `folder_name`, reads from all folders. |
| `folder_name` | string | no | none | Target folder by well-known name (e.g., `inbox`, `sentitems`, `deleteditems`, `drafts`). Cannot be used with `folder_id`. |
| `top` | integer | no | 999 | Number of messages to return per API page (max 999). |
| `start_date` | string (ISO 8601) | no | none | Initial cursor for first run (e.g., `2024-01-01T00:00:00Z`). If omitted, performs full historical backfill. |
| `lookback_seconds` | integer | no | 120 | Lookback window in seconds when computing next cursor to handle late updates. |
| `max_pages_per_batch` | integer | no | 50 | Safety limit on API pages per `read_table` call. |
| `include_drafts` | boolean | no | true | Whether to include draft messages in results. |

> **Note**: Do not specify both `folder_id` and `folder_name` - use only one or neither (for all folders).

#### `mailfolders` Table Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `top` | integer | no | 999 | Number of folders to return per API page (max 999). |
| `max_pages_per_batch` | integer | no | 50 | Safety limit on API pages per `read_table` call. |

### Schema Highlights

#### `messages` Table (29 fields)

The messages table includes comprehensive email metadata and content:

- **Identifiers**: `id`, `internetMessageId`, `conversationId`
- **Timestamps**: `createdDateTime`, `lastModifiedDateTime`, `receivedDateTime`, `sentDateTime`
- **Content**: `subject`, `bodyPreview`, `body` (nested: `contentType`, `content`), `uniqueBody`
- **Flags**: `hasAttachments`, `isRead`, `isDraft`, `isDeliveryReceiptRequested`, `isReadReceiptRequested`
- **Recipients**: Nested structures for:
  - `from` (sender mailbox and sender of the message)
  - `sender` (account actually used to send)
  - `toRecipients` (array of To recipients)
  - `ccRecipients` (array of Cc recipients)
  - `bccRecipients` (array of Bcc recipients, typically empty when reading)
  - `replyTo` (array of reply-to addresses)
- **Organization**: `parentFolderId`, `conversationIndex`, `importance`, `inferenceClassification`, `webLink`
- **User Actions**: `flag` (nested: `flagStatus`, `startDateTime`, `dueDateTime`, `completedDateTime`)
- `categories` (array of user-defined labels)

**Recipient Structure** (nested in `from`, `sender`, `toRecipients`, etc.):
```
{
  "emailAddress": {
    "name": "Display Name",
    "address": "email@example.com"
  }
}
```

**Body Structure**:
```
{
  "contentType": "text" or "html",
  "content": "<message body content>"
}
```

#### `mailfolders` Table (7 fields)

- **Identifiers**: `id`, `parentFolderId`, `wellKnownName`
- **Display**: `displayName`
- **Counts**: `childFolderCount`, `unreadItemCount`, `totalItemCount`

Well-known folder names include: `inbox`, `sentitems`, `deleteditems`, `drafts`, `junkemail`, `outbox`

## Data Type Mapping

Microsoft Graph JSON fields are mapped to Spark types as follows:

| Microsoft Graph Type | Example Fields | Connector Logical Type | Notes |
|---------------------|----------------|------------------------|-------|
| string (identifier) | `id`, `parentFolderId`, `conversationId` | string (`StringType`) | Opaque unique identifiers; do not parse or truncate. |
| string | `subject`, `bodyPreview`, `internetMessageId` | string (`StringType`) | UTF-8 text. Body content can be large (several MB). |
| boolean | `hasAttachments`, `isRead`, `isDraft` | boolean (`BooleanType`) | Standard true/false values. |
| string (ISO 8601 datetime) | `receivedDateTime`, `sentDateTime`, `createdDateTime`, `lastModifiedDateTime` | string in schema | Stored as UTC timestamp strings (e.g., `"2015-01-29T20:44:53Z"`). Parse downstream if needed. |
| string (enum) | `importance` (`low`, `normal`, `high`), `inferenceClassification` | string (`StringType`) | Enums represented as strings. |
| integer | `childFolderCount`, `unreadItemCount` | long (`LongType`) | 64-bit integers used to avoid overflow. |
| object (complex type) | `from`, `sender`, `body`, `flag` | struct (`StructType`) | Nested objects preserved (not flattened). |
| array | `toRecipients`, `ccRecipients`, `categories` | array of struct or string | Arrays preserved as nested collections. |
| nullable fields | `sentDateTime`, `webLink`, `flag`, `uniqueBody` | same as base type + `null` | Missing nested objects surfaced as `null`, not `{}`. |

The connector is designed to:
- Use `LongType` for all numeric fields
- Preserve nested JSON structures instead of flattening
- Treat absent nested fields as `None`/`null` to conform to Spark expectations

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Outlook connector source in your workspace. This will typically place the connector code (`outlook.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py`), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** using this Outlook connector
- One or more **tables** to ingest with optional `table_options`

Example `pipeline_spec` for reading inbox messages:

```json
{
  "pipeline_spec": {
    "connection_name": "outlook_mail_connection",
    "object": [
      {
        "table": {
          "source_table": "messages",
          "folder_name": "inbox",
          "start_date": "2024-01-01T00:00:00Z",
          "top": 100,
          "lookback_seconds": 120
        }
      },
      {
        "table": {
          "source_table": "mailfolders"
        }
      }
    ]
  }
}
```

Example for reading messages from all folders:

```json
{
  "pipeline_spec": {
    "connection_name": "outlook_mail_connection",
    "object": [
      {
        "table": {
          "source_table": "messages",
          "start_date": "2024-01-01T00:00:00Z"
        }
      }
    ]
  }
}
```

Example for reading from a specific folder by ID:

```json
{
  "pipeline_spec": {
    "connection_name": "outlook_mail_connection",
    "object": [
      {
        "table": {
          "source_table": "messages",
          "folder_id": "AAMkAGVm...",
          "start_date": "2024-01-01T00:00:00Z"
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., scheduled job or workflow).

For incremental tables (`messages`):

- **First run**:
  - Omit `start_date` to backfill all historical messages (may be large for long-lived mailboxes)
  - Or set `start_date` to a recent cutoff to limit history (e.g., last 30 days)
- **Subsequent runs**:
  - The connector uses the stored cursor (based on `lastModifiedDateTime`) plus `lookback_seconds` to pick up new and updated messages
  - Messages that are read, moved, categorized, or otherwise modified are picked up as updates

#### Best Practices

- **Start Small**:
  - Begin with a single folder (e.g., `inbox`) and recent date range (e.g., last 7 days) to validate configuration
  - Test with a small `top` value (e.g., 100) and `max_pages_per_batch` (e.g., 5) initially
  
- **Use Incremental Sync**:
  - For the `messages` table, rely on the `cdc` pattern with `lastModifiedDateTime` to minimize API calls
  - Set an appropriate `start_date` for the initial run to avoid backfilling years of email if not needed
  
- **Tune Performance Parameters**:
  - Use `top=999` (maximum) for production runs to reduce API calls
  - Adjust `max_pages_per_batch` based on mailbox size and runtime requirements
  - Set `lookback_seconds` to handle delayed email delivery or sync latency (default 120 seconds is usually sufficient)
  
- **Respect Rate Limits**:
  - Microsoft Graph enforces throttling: typically 10,000 requests per 10 minutes per user
  - The connector automatically refreshes OAuth tokens as needed
  - Consider staggering syncs or reducing frequency if you encounter HTTP 429 (Too Many Requests) errors
  
- **Handle Large Mailboxes**:
  - Mailboxes with millions of messages may require several hours for initial backfill
  - Use `start_date` to limit initial sync scope
  - Consider syncing specific folders instead of all folders to reduce load
  
- **Body Content Considerations**:
  - Email bodies can be very large (several MB for HTML with inline images)
  - Consider using `$select` parameter (future enhancement) to exclude body content if only metadata is needed
  - Be aware of downstream storage limits when ingesting full body content

#### Troubleshooting

**Common Issues:**

- **Authentication Failures (`401 Unauthorized` or token refresh errors)**:
  - Verify `client_id`, `client_secret`, and `refresh_token` are correct
  - Ensure the Azure AD application has the required API permissions (`Mail.Read`, `offline_access`)
  - Check if admin consent is required for your organization and has been granted
  - Verify the refresh token hasn't expired (typically 90 days of inactivity)
  - If using a specific tenant, ensure the `tenant` parameter matches your Azure AD tenant

- **Permission Errors (`403 Forbidden`)**:
  - Verify your Azure AD app has been granted the necessary Microsoft Graph permissions
  - Ensure admin consent has been provided if required by your organization
  - Check that the authenticated user has access to the mailbox being read

- **Missing Messages or Incomplete Data**:
  - Verify `start_date` is set correctly for initial runs
  - Check `lookback_seconds` is sufficient for your email delivery latency
  - Ensure `folder_id` or `folder_name` is correct if targeting a specific folder
  - Confirm messages exist in the specified folder using Outlook web or desktop client

- **Throttling / Rate Limiting (`429 Too Many Requests`)**:
  - Reduce sync frequency (run less often)
  - Decrease `top` value to fetch fewer messages per page
  - Reduce `max_pages_per_batch` to limit API calls per sync
  - Implement exponential backoff in your pipeline orchestration
  - Check Microsoft Graph throttling limits for your account type

- **Slow Performance**:
  - Increase `top` to maximum (999) to reduce number of API calls
  - Use folder-specific reads instead of all-folder reads when possible
  - Consider excluding draft messages with `include_drafts: false`
  - For large mailboxes, run initial backfill as a one-time job and then schedule incremental syncs

- **Schema Mismatches**:
  - The connector uses nested structs extensively for recipients, body, and flag fields
  - Ensure downstream tables are defined to accept nested types
  - Use Spark SQL or DataFrame transformations to flatten if needed
  - Missing optional fields will be `null`, not empty objects

- **Refresh Token Expiration**:
  - Refresh tokens typically expire after 90 days of inactivity
  - Re-run the OAuth flow to obtain a new refresh token if yours expires
  - Consider setting up monitoring to alert on authentication failures

## References

- **Connector Implementation**: `sources/outlook/outlook.py`
- **Connector API Documentation**: `sources/outlook/outlook_api_doc.md`
- **Official Microsoft Graph Mail API Documentation**:
  - [Get Started with Outlook REST APIs](https://learn.microsoft.com/en-us/outlook/rest/get-started)
  - [Microsoft Graph Mail API Overview](https://learn.microsoft.com/en-us/graph/api/resources/mail-api-overview)
  - [List messages](https://learn.microsoft.com/en-us/graph/api/user-list-messages)
  - [Message resource type](https://learn.microsoft.com/en-us/graph/api/resources/message)
  - [List mailFolders](https://learn.microsoft.com/en-us/graph/api/user-list-mailfolders)
  - [mailFolder resource type](https://learn.microsoft.com/en-us/graph/api/resources/mailfolder)
- **Microsoft Graph Authentication**:
  - [OAuth 2.0 with Microsoft Identity Platform](https://learn.microsoft.com/en-us/graph/auth/)
  - [Get access on behalf of a user](https://learn.microsoft.com/en-us/graph/auth-v2-user)
  - [Use the Microsoft Graph API](https://learn.microsoft.com/en-us/graph/use-the-api)
- **Microsoft Graph Best Practices**:
  - [Throttling and Paging](https://learn.microsoft.com/en-us/graph/throttling)
  - [OData Query Parameters](https://learn.microsoft.com/en-us/graph/query-parameters)
  - [Paging Microsoft Graph data](https://learn.microsoft.com/en-us/graph/paging)

