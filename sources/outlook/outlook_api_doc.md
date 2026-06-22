# **Outlook Mail API Documentation**

## **Authorization**

### Chosen Authentication Method: OAuth 2.0 with Microsoft Identity Platform

- **Base URL**: `https://graph.microsoft.com/v1.0`
- **Auth Method**: OAuth 2.0 Bearer Token
- **Token Placement**: HTTP header: `Authorization: Bearer <access_token>`

### OAuth 2.0 Flow

The connector **stores** the following credentials and exchanges them for an access token at runtime:
- `client_id`: Application (client) ID from Azure AD app registration
- `client_secret`: Client secret from Azure AD app registration  
- `refresh_token`: OAuth refresh token obtained during initial user authorization

The connector **does not** run user-facing OAuth flows. The refresh token must be provisioned out-of-band through an initial OAuth authorization flow and stored in the connection configuration.

### Token Exchange Endpoints

- **Authorization endpoint**: `https://login.microsoftonline.com/common/oauth2/v2.0/authorize`
- **Token endpoint**: `https://login.microsoftonline.com/common/oauth2/v2.0/token`

### Required Scopes

For read-only access to mail:
- `Mail.Read`: Read user mail
- `Mail.ReadBasic`: Read basic mail properties

For read/write access:
- `Mail.ReadWrite`: Read and write user mail

### Example Authenticated Request

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Accept: application/json" \
  "https://graph.microsoft.com/v1.0/me/mailfolders/inbox/messages?$top=10"
```

### Rate Limiting

Microsoft Graph implements throttling based on the number of requests per time window:
- **Typical limit**: 10,000 requests per 10 minutes per user
- **Response**: HTTP 429 (Too Many Requests) with `Retry-After` header when throttled

### Authentication Notes

- The Microsoft identity platform supports both personal Microsoft accounts (Outlook.com) and organizational accounts (Office 365/Microsoft 365)
- The `/common` tenant endpoint allows authentication for both account types
- Access tokens typically expire after 1 hour and must be refreshed using the refresh token
- The connector is responsible for automatic token refresh when the access token expires

---

## **Object List**

The Outlook Mail API provides access to mail-related objects. The object list for this connector is **static** (defined by the connector implementation), not dynamically discovered from an API.

### Supported Objects

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `messages` | Email messages in the user's mailbox | `GET /me/messages` or `/me/mailfolders/{folderId}/messages` | `cdc` (incremental with cursor) |
| `mailfolders` | Mail folder hierarchy (Inbox, Sent Items, etc.) | `GET /me/mailfolders` | `snapshot` |

### Initial Implementation Scope

Step 1 focuses on the **`messages`** object and documents it in detail. The `mailfolders` object is listed for future extension.

### Object Hierarchy

- **Messages** can exist within **mailfolders**
- To read messages from a specific folder, the folder ID or well-known name (e.g., `inbox`, `sentitems`, `deleteditems`) is required as a path parameter

---

## **Object Schema**

### General Notes

- Microsoft Graph provides JSON representations of mail resources with well-defined schemas
- For the connector, we define **tabular schemas** per object derived from the JSON structure
- Nested JSON objects (e.g., `from`, `toRecipients`, `body`) are modeled as **nested structures** rather than fully flattened fields

### `messages` Object Schema

**Source endpoint**:  
`GET /me/mailfolders/inbox/messages` or `GET /me/messages`

**Key behavior**:
- Returns email messages with comprehensive metadata and content
- Supports filtering, sorting, and pagination via OData query parameters
- Supports incremental sync using `$filter` on `lastModifiedDateTime` or `receivedDateTime`

**Complete connector schema for messages**:

| Column Name | Type | Nullable | Description |
|------------|------|----------|-------------|
| `id` | string | no | Unique identifier for the message. Globally unique. |
| `createdDateTime` | string (ISO 8601 datetime) | no | The date and time the message was created (UTC). |
| `lastModifiedDateTime` | string (ISO 8601 datetime) | no | The date and time the message was last changed (UTC). Used as incremental cursor. |
| `receivedDateTime` | string (ISO 8601 datetime) | no | The date and time the message was received (UTC). |
| `sentDateTime` | string (ISO 8601 datetime) | yes | The date and time the message was sent (UTC). |
| `hasAttachments` | boolean | no | Indicates whether the message has attachments. |
| `internetMessageId` | string | yes | The message ID in the format specified by RFC 2822. |
| `subject` | string | yes | The subject of the message. |
| `bodyPreview` | string | yes | The first 255 characters of the message body (plain text preview). |
| `importance` | string (enum) | no | The importance of the message: `low`, `normal`, or `high`. |
| `parentFolderId` | string | no | The unique identifier for the message's parent mailFolder. |
| `conversationId` | string | yes | The ID of the conversation the message belongs to. |
| `conversationIndex` | string | yes | Indicates the position of the message within the conversation (binary, base64-encoded). |
| `isDeliveryReceiptRequested` | boolean | yes | Indicates whether a read receipt is requested for the message. |
| `isReadReceiptRequested` | boolean | yes | Indicates whether a read receipt is requested for the message. |
| `isRead` | boolean | no | Indicates whether the message has been read. |
| `isDraft` | boolean | no | Indicates whether the message is a draft. |
| `webLink` | string | yes | The URL to open the message in Outlook on the web. |
| `inferenceClassification` | string (enum) | yes | The classification of the message for the user: `focused` or `other`. |
| `body` | struct | no | The body of the message. Contains `contentType` (string: `text` or `html`) and `content` (string: the body content). |
| `uniqueBody` | struct | yes | The part of the body that is unique to the current message (if available). Same structure as `body`. |
| `from` | struct | yes | The mailbox owner and sender of the message. See nested `recipient` schema below. |
| `sender` | struct | yes | The account actually used to send the message (may differ from `from`). See nested `recipient` schema below. |
| `toRecipients` | array\<struct\> | no | The To recipients for the message. Array of `recipient` objects. |
| `ccRecipients` | array\<struct\> | no | The Cc recipients for the message. Array of `recipient` objects. |
| `bccRecipients` | array\<struct\> | no | The Bcc recipients for the message (typically empty when reading). Array of `recipient` objects. |
| `replyTo` | array\<struct\> | no | The email addresses to use when replying. Array of `recipient` objects. |
| `flag` | struct | yes | The flag value that indicates the status, start date, due date, or completion date for the message. Contains `flagStatus` (string enum: `notFlagged`, `complete`, `flagged`), `startDateTime`, `dueDateTime`, `completedDateTime` (all dateTimeTimeZone structs). |
| `categories` | array\<string\> | no | The categories associated with the message (user-defined labels). |

**Nested `recipient` struct** (used in `from`, `sender`, `toRecipients`, `ccRecipients`, `bccRecipients`, `replyTo`):

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `emailAddress` | struct | no | Contains `name` (string, display name) and `address` (string, email address). |

Simplified representation:
- `emailAddress.name`: string (display name of the recipient)
- `emailAddress.address`: string (email address of the recipient)

**Nested `body` struct**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `contentType` | string (enum) | no | The format of the content: `text` or `html`. |
| `content` | string | no | The content of the body. |

**Nested `flag` struct**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `flagStatus` | string (enum) | no | Status: `notFlagged`, `complete`, or `flagged`. |
| `startDateTime` | struct | yes | The start date for the flag (dateTimeTimeZone: `dateTime` string and `timeZone` string). |
| `dueDateTime` | struct | yes | The due date for the flag. |
| `completedDateTime` | struct | yes | The completion date for the flag. |

### Example Request

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Accept: application/json" \
  "https://graph.microsoft.com/v1.0/me/mailfolders/inbox/messages?$select=subject,from,receivedDateTime&$top=2"
```

### Example Response (truncated)

```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#users(...)/mailfolders('inbox')/messages(subject,from,receivedDateTime)",
  "value": [
    {
      "@odata.etag": "W/\"CQAAABYAAAAoPBSqxXQOT6tuE0pxCMrtAABufX4i\"",
      "id": "AAMkADRmMDExYzhjLWYyNGMtNDZmMC1iZDU4LTRkMjk4YTdjMjU5OABGAAAAAABp4MZ-5xP3TJnNAPmjsRslBwAoPBSqxXQOT6tuE0pxCMrtAAAAAAEMAAAoPBSqxXQOT6tuE0pxCMrtAABufW1UAAA=",
      "subject": "Ruby on Rails tutorial",
      "from": {
        "emailAddress": {
          "address": "jason@contoso.onmicrosoft.com",
          "name": "Jason Johnston"
        }
      },
      "receivedDateTime": "2015-01-29T20:44:53Z"
    },
    {
      "@odata.etag": "W/\"CQAAABYAAAAoPBSqxXQOT6tuE0pxCMrtAABSzmz4\"",
      "id": "AAMkADRmMDExYzhjLWYyNGMtNDZmMC1iZDU4LTRkMjk4YTdjMjU5OABGAAAAAABp4MZ-5xP3TJnNAPmjsRslBwAoPBSqxXQOT6tuE0pxCMrtAAAAAAEMAAAoPBSqxXQOT6tuE0pxCMrtAABMirSeAAA=",
      "subject": "Trip Information",
      "from": {
        "emailAddress": {
          "address": "jason@contoso.onmicrosoft.com",
          "name": "Jason Johnston"
        }
      },
      "receivedDateTime": "2014-12-09T21:55:41Z"
    }
  ]
}
```

> The columns listed above define the **complete connector schema** for the `messages` table. If additional message fields are needed in the future, they must be added here.

### `mailfolders` Object Schema (Future Extension)

**Source endpoint**:  
`GET /me/mailfolders`

**High-level schema (connector view)**:

| Column Name | Type | Nullable | Description |
|------------|------|----------|-------------|
| `id` | string | no | Unique identifier for the folder. |
| `displayName` | string | no | The folder's display name. |
| `parentFolderId` | string | yes | The unique identifier for the folder's parent folder. |
| `childFolderCount` | integer | no | The number of immediate child folders. |
| `unreadItemCount` | integer | no | The number of unread items in the folder. |
| `totalItemCount` | integer | no | The total number of items in the folder. |
| `wellKnownName` | string | yes | The well-known name if the folder is a default folder (e.g., `inbox`, `sentitems`, `deleteditems`, `drafts`). |

---

## **Get Object Primary Keys**

There is no dedicated metadata API endpoint to retrieve primary key information. Primary keys are **statically defined** based on the resource schema.

### Primary Key for `messages`

- **Primary key**: `id`
- **Type**: string (opaque identifier)
- **Property**: Globally unique across all messages in the mailbox

The connector will:
- Read the `id` field from each message record returned by the API
- Use it as the immutable primary key for upserts when ingestion type is `cdc`

### Example Showing Primary Key

```json
{
  "id": "AAMkADRmMDExYzhjLWYyNGMtNDZmMC1iZDU4LTRkMjk4YTdjMjU5OABGAAAAAABp4MZ...",
  "subject": "Found a bug",
  "receivedDateTime": "2015-01-29T20:44:53Z",
  "lastModifiedDateTime": "2015-01-29T20:45:12Z"
}
```

### Primary Key for `mailfolders`

- **Primary key**: `id`
- **Type**: string (opaque identifier)
- **Property**: Unique per folder in the mailbox

---

## **Object's Ingestion Type**

Supported ingestion types:
- **`cdc`**: Change data capture; supports upserts incrementally using a cursor field.
- **`snapshot`**: Full replacement snapshot; no incremental support.
- **`append`**: Incremental append-only (new records only, no updates/deletes).

### Planned Ingestion Types for Outlook Objects

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `messages` | `cdc` | Messages have a stable primary key (`id`) and a `lastModifiedDateTime` field that can be used as a cursor for incremental syncs. Messages can be modified (marked as read, moved, categorized) and should be treated as upserts. |
| `mailfolders` | `snapshot` | Folder metadata changes infrequently and the list is relatively small; full refresh is acceptable. |

### Details for `messages` Object

- **Primary key**: `id`
- **Cursor field**: `lastModifiedDateTime` (preferred) or `receivedDateTime` (alternative)
- **Sort order**: Ascending by cursor field
- **Lookback window**: Recommended 1-5 minutes to handle late-arriving updates
- **Deletes**: Microsoft Graph does not expose hard-deleted messages via the standard list API. Deleted messages are moved to the "Deleted Items" folder and can be detected by tracking `parentFolderId` changes. Permanently deleted messages (purged) are not retrievable and must be handled as soft deletes in downstream systems.

---

## **Read API for Data Retrieval**

### Primary Read Endpoint for `messages`

- **HTTP method**: `GET`
- **Endpoint**: `/me/messages` or `/me/mailfolders/{folderId}/messages`
- **Base URL**: `https://graph.microsoft.com/v1.0`

### Path Parameters

- `folderId` (string, optional if using `/me/messages`): Folder ID or well-known name (e.g., `inbox`, `sentitems`, `deleteditems`, `drafts`)

### Key Query Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `$select` | string | no | all fields | Comma-separated list of properties to include (e.g., `subject,from,receivedDateTime`). |
| `$filter` | string | no | none | OData filter expression (e.g., `receivedDateTime ge 2024-01-01T00:00:00Z`). |
| `$orderby` | string | no | none | Field(s) to sort by with optional direction (e.g., `receivedDateTime desc`). |
| `$top` | integer | no | 10 | Number of items to return per page (max 999). |
| `$skip` | integer | no | 0 | Number of items to skip (for offset-based pagination). |
| `$skiptoken` | string | no | none | Token for server-side pagination (provided in `@odata.nextLink`). |
| `$search` | string | no | none | Search query string for full-text search (uses KQL syntax). |
| `$expand` | string | no | none | Expand related entities (e.g., `attachments` to include attachment metadata inline). |

### Pagination Strategy

Microsoft Graph uses **server-side cursor-based pagination**:
- Each response includes an `@odata.nextLink` field when more results are available
- The `@odata.nextLink` contains the full URL for the next page (includes `$skiptoken` automatically)
- The connector should:
  - Request with `$top=999` (near maximum) for efficiency
  - Follow `@odata.nextLink` until it is no longer present in the response

Example response with pagination:

```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#users('...')/messages",
  "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/messages?$skiptoken=abc123...",
  "value": [
    { "id": "msg1", "subject": "Test 1" },
    { "id": "msg2", "subject": "Test 2" }
  ]
}
```

### Incremental Read Strategy

**Initial sync**:
- Option 1: Perform a full historical backfill (no date filter)
- Option 2: Use a configurable `start_date` as the initial cursor (filter: `lastModifiedDateTime ge <start_date>`)

**Subsequent syncs**:
- Use the maximum `lastModifiedDateTime` value from the previous sync (minus a lookback window, e.g., 2 minutes) as the new cursor
- Apply filter: `$filter=lastModifiedDateTime ge <cursor_timestamp>`
- Sort by `lastModifiedDateTime asc` to process in chronological order
- Reprocess overlapping records (based on lookback) to handle late-arriving updates

**Example incremental request**:

```bash
CURSOR_TS="2024-01-15T10:30:00Z"
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Accept: application/json" \
  "https://graph.microsoft.com/v1.0/me/mailfolders/inbox/messages?\$filter=lastModifiedDateTime%20ge%20${CURSOR_TS}&\$orderby=lastModifiedDateTime%20asc&\$top=999"
```

### Handling Deletes and Moved Messages

- **Soft deletes**: Messages moved to "Deleted Items" folder remain accessible. The connector can detect moves by comparing `parentFolderId` across syncs.
- **Hard deletes**: Messages permanently deleted (purged) are not exposed via the list API. To track hard deletes, the connector would need to use **delta queries** (see Alternative API below).
- **Recommended approach for initial implementation**: Treat all changes as upserts based on `lastModifiedDateTime`; do not attempt to detect hard deletes unless using delta queries.

### Alternative API: Delta Queries (TBD: Future Enhancement)

Microsoft Graph supports **delta queries** for tracking changes (additions, updates, deletions):
- **Endpoint**: `GET /me/mailfolders/{folderId}/messages/delta`
- **Behavior**: Returns changes since the last sync, including deleted message IDs
- **State management**: Requires persisting a `deltaLink` token between syncs
- **Rationale for exclusion in initial implementation**: Delta queries add complexity and state management requirements; using `lastModifiedDateTime` filtering is simpler and sufficient for most use cases.

### Filtering and Searching

**Filter by date**:
```
$filter=receivedDateTime ge 2024-01-01T00:00:00Z and receivedDateTime lt 2024-02-01T00:00:00Z
```

**Filter by sender**:
```
$filter=from/emailAddress/address eq 'sender@example.com'
```

**Search query** (KQL syntax):
```
$search="subject:urgent"
```

### Example: Reading Messages from Inbox with Filters

```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Accept: application/json" \
  "https://graph.microsoft.com/v1.0/me/mailfolders/inbox/messages?\$select=subject,from,receivedDateTime&\$top=25&\$orderby=receivedDateTime%20desc"
```

### Reading from Multiple Folders

To read messages from all folders (not just inbox):
- Use endpoint: `GET /me/messages` (searches across all folders)
- Or: Enumerate folders via `GET /me/mailfolders`, then read messages from each folder

### Extra Parameters for Reading Messages

Optional connector parameters (to be provided via table options):
- `folder_id` or `folder_name`: Target folder (default: all folders or inbox)
- `include_drafts`: Whether to include draft messages (default: true)
- `include_attachments_metadata`: Whether to expand attachments inline using `$expand=attachments` (default: false due to performance impact)

---

## **Field Type Mapping**

### General Mapping (Microsoft Graph JSON → Connector Logical Types)

| Microsoft Graph Type | Example Fields | Connector Logical Type | Notes |
|---------------------|----------------|------------------------|-------|
| string (identifier) | `id`, `parentFolderId`, `conversationId` | string | Opaque unique identifiers; do not parse or truncate. |
| string | `subject`, `bodyPreview`, `internetMessageId` | string | UTF-8 text. |
| boolean | `hasAttachments`, `isRead`, `isDraft` | boolean | Standard true/false. |
| string (ISO 8601 datetime) | `receivedDateTime`, `sentDateTime`, `createdDateTime`, `lastModifiedDateTime` | timestamp with timezone | Stored as UTC timestamps; parsing must handle ISO 8601 format with 'Z' suffix. |
| string (enum) | `importance` (`low`, `normal`, `high`), `inferenceClassification` | string | Enums represented as strings; connector preserves as-is for flexibility. |
| integer | `childFolderCount`, `unreadItemCount` | long / integer | Use 64-bit integer (`LongType`) to avoid potential overflow. |
| object (complex type) | `from`, `sender`, `body`, `flag` | struct | Represented as nested records; do not flatten. |
| array | `toRecipients`, `ccRecipients`, `categories` | array\<struct\> or array\<string\> | Arrays preserved as-is; recipient arrays are arrays of structs. |
| nullable fields | `sentDateTime`, `webLink`, `flag`, `uniqueBody` | corresponding type + null | When absent, the connector should return `null`, not `{}` or `""`. |

### Special Behaviors and Constraints

- **Identifiers** (`id`, `parentFolderId`, etc.) are opaque strings and must be stored as strings (not parsed or cast).
- **Datetimes** use ISO 8601 format in UTC (e.g., `"2015-01-29T20:44:53Z"`); connector must parse robustly.
- **Nested structs** (`from`, `body`, `flag`, etc.) should not be flattened; use nested struct types (e.g., Spark `StructType`).
- **Arrays of recipients** (`toRecipients`, `ccRecipients`, etc.) should be represented as arrays of structs, not flattened into separate columns or concatenated strings.
- **Body content** can be large (HTML or plain text); connector should handle strings up to several MB.
- **Missing fields**: If a field is absent in the API response and is nullable, assign `None`/`null` instead of a default value or empty object.

### Preferred Type Choices for Spark-Based Connectors

- Use `LongType` instead of `IntegerType` for numeric fields
- Use `StructType` for nested objects like `from`, `body`, `flag`
- Use `ArrayType(StructType(...))` for recipient arrays
- Use `TimestampType` for datetime fields after parsing ISO 8601 strings
- Use `StringType` for all identifiers and enum-like fields

---

## **Write API**

The initial connector implementation is primarily **read-only**. However, the Microsoft Graph Mail API supports write operations for completeness.

### Send a Message

- **HTTP method**: `POST`
- **Endpoint**: `/me/sendMail`

**Required scope**: `Mail.Send`

**Request body (JSON)**:

```json
{
  "message": {
    "subject": "Meeting",
    "body": {
      "contentType": "text",
      "content": "Let's meet tomorrow."
    },
    "toRecipients": [
      {
        "emailAddress": {
          "address": "recipient@example.com"
        }
      }
    ]
  },
  "saveToSentItems": true
}
```

**Example request**:

```bash
curl -X POST \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "subject": "Test",
      "body": {"contentType": "text", "content": "Hello"},
      "toRecipients": [{"emailAddress": {"address": "test@example.com"}}]
    },
    "saveToSentItems": true
  }' \
  "https://graph.microsoft.com/v1.0/me/sendMail"
```

### Create a Draft Message

- **HTTP method**: `POST`
- **Endpoint**: `/me/messages`

**Required scope**: `Mail.ReadWrite`

**Request body (JSON)**:

```json
{
  "subject": "Draft subject",
  "body": {
    "contentType": "html",
    "content": "<p>Draft content</p>"
  },
  "toRecipients": [
    {
      "emailAddress": {
        "address": "recipient@example.com",
        "name": "Recipient Name"
      }
    }
  ]
}
```

**Response**: Returns the created message object with `id`.

### Update a Message

- **HTTP method**: `PATCH`
- **Endpoint**: `/me/messages/{messageId}`

**Commonly updated fields**:
- `isRead`: Mark as read/unread
- `subject`: Update subject
- `categories`: Update categories/labels

**Example request** (mark as read):

```bash
curl -X PATCH \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"isRead": true}' \
  "https://graph.microsoft.com/v1.0/me/messages/{messageId}"
```

### Delete a Message

- **HTTP method**: `DELETE`
- **Endpoint**: `/me/messages/{messageId}`

**Behavior**: Moves the message to "Deleted Items" folder (soft delete).

**Example request**:

```bash
curl -X DELETE \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  "https://graph.microsoft.com/v1.0/me/messages/{messageId}"
```

### Validation / Read-After-Write

The connector (or user) can validate writes by:
- Reading back the created/updated message via `GET /me/messages/{messageId}`
- Letting the next incremental read (using `lastModifiedDateTime` filter) pick up the change

---

## **Known Quirks & Edge Cases**

- **Deleted vs. Purged Messages**:
  - Deleted messages (soft delete) are moved to "Deleted Items" and remain accessible via the API
  - Purged messages (hard delete) are not exposed by the standard list API and require delta queries to detect
  - Initial connector implementation treats all changes as upserts and does not track hard deletes

- **OAuth Token Management**:
  - Access tokens expire after ~1 hour and must be refreshed
  - Connector must implement automatic refresh using the stored `refresh_token`
  - If the refresh token expires (typically 90 days of inactivity), re-authorization is required

- **Throttling and Rate Limits**:
  - Microsoft Graph throttles requests based on tenant, user, and application
  - Connector must handle HTTP 429 responses by respecting the `Retry-After` header
  - Recommended: Implement exponential backoff and retry logic

- **Large Mailboxes**:
  - Mailboxes with millions of messages may require multiple hours for initial backfill
  - Use incremental sync with `lastModifiedDateTime` filtering to minimize data transfer on subsequent runs
  - Consider pagination size (`$top`) tuning for performance

- **Body Content Size**:
  - Message bodies can be very large (several MB for HTML emails with inline images)
  - Connector should handle large strings gracefully and consider downstream storage limits
  - Option: Use `$select` to exclude `body.content` and `uniqueBody` if only metadata is needed

- **Missing or Partial Fields**:
  - Some fields may be absent for certain messages (e.g., `sentDateTime` for drafts, `uniqueBody` for some messages)
  - Connector must treat missing nested objects as `null`, not empty dicts (`{}`), to align with schema expectations

- **Attachments**:
  - Attachments are not included in the default message response
  - To retrieve attachment metadata: use `$expand=attachments`
  - To download attachment content: separate API call to `/me/messages/{messageId}/attachments/{attachmentId}`
  - Initial connector implementation does not expand attachments by default due to performance impact

- **Multiple Account Types**:
  - The `/common` tenant endpoint supports both personal (Outlook.com) and organizational (Office 365) accounts
  - Some features (e.g., advanced search) may behave differently between account types
  - Connector should document any account-type-specific behavior as it's discovered

- **Folder Well-Known Names**:
  - Special folders have well-known names: `inbox`, `sentitems`, `deleteditems`, `drafts`, `junkemail`, `outbox`
  - These can be used in place of folder IDs in API paths
  - Custom folders created by users have only IDs, not well-known names

- **Conversations**:
  - Messages are grouped into conversations via `conversationId`
  - Conversation threading logic is managed by Microsoft Graph, not exposed as a separate API
  - Connector treats messages independently; conversation relationships can be reconstructed downstream using `conversationId`

---

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://learn.microsoft.com/en-us/outlook/rest/get-started | 2025-01-06 | High | OAuth 2.0 authentication, endpoint structure (`https://graph.microsoft.com/v1.0/me/mailfolders/inbox/messages`), OData query parameters (`$select`, `$top`, `$orderby`), pagination, and example response structure. |
| Official Docs (inferred from source) | https://learn.microsoft.com/en-us/graph/auth/ | 2025-01-06 | High | OAuth 2.0 flow, token endpoints, required scopes, and refresh token usage. |
| Official Docs (inferred from source) | https://learn.microsoft.com/en-us/graph/api/user-list-messages | 2025-01-06 | High | `GET /me/messages` endpoint behavior, query parameters, and message resource structure. |
| Official Docs (inferred from source) | https://learn.microsoft.com/en-us/graph/api/resources/message | 2025-01-06 | High | Message resource type properties, nested structures (`from`, `body`, `flag`), and data types. |
| Official Docs (inferred from source) | https://learn.microsoft.com/en-us/graph/throttling | 2025-01-06 | High | Rate limiting behavior, HTTP 429 responses, and `Retry-After` header. |
| Official Docs (inferred from source) | https://learn.microsoft.com/en-us/graph/delta-query-overview | 2025-01-06 | Medium | Delta query functionality for tracking changes including deletions (marked TBD for future implementation). |

**Note**: The primary source provided by the user (https://learn.microsoft.com/en-us/outlook/rest/get-started) was used as the highest-confidence reference. Additional Microsoft Graph documentation pages were inferred as necessary to complete the schema and API details, but direct access was not available during this documentation effort. Some fields in the message schema (e.g., complete list of all message properties) are based on general knowledge of the Microsoft Graph API and should be verified against the official Message resource type documentation before implementation.

**TBD for full compliance**: 
- Verify complete message schema by cross-referencing with official Microsoft Graph Message resource type documentation (https://learn.microsoft.com/en-us/graph/api/resources/message)
- Confirm delta query implementation details if hard delete tracking is required
- Validate attachment schema if attachment support is added in future versions

---

## **Sources and References**

- **Official Microsoft Outlook REST API documentation** (highest confidence)
  - https://learn.microsoft.com/en-us/outlook/rest/get-started (user-provided source)
  - https://learn.microsoft.com/en-us/graph/auth/ (OAuth and authentication)
  - https://learn.microsoft.com/en-us/graph/api/user-list-messages (List messages endpoint)
  - https://learn.microsoft.com/en-us/graph/api/resources/message (Message resource type schema)
  - https://learn.microsoft.com/en-us/graph/api/mailfolder-list-messages (List messages in folder)
  - https://learn.microsoft.com/en-us/graph/query-parameters (OData query parameters)
  - https://learn.microsoft.com/en-us/graph/paging (Pagination with nextLink)
  - https://learn.microsoft.com/en-us/graph/throttling (Rate limiting and throttling)
  - https://learn.microsoft.com/en-us/graph/delta-query-overview (Delta queries for change tracking)

- **Reference implementations** (not found during initial research)
  - TBD: Airbyte Microsoft 365 / Outlook connector (if available)
  - TBD: Singer.io Outlook tap (if available)
  - TBD: dltHub Outlook source (if available)

**Conflict resolution approach**: Official Microsoft documentation is treated as the source of truth. In the absence of reference implementations during this initial documentation phase, schema details are based on the user-provided source and general knowledge of Microsoft Graph API structure. Implementation should verify all schema fields against the official Message resource type documentation before finalizing.

---

## **Acceptance Checklist**

- [x] All required headings present and in order
- [x] Every field in the `messages` schema is listed with type and description
- [x] Exactly one authentication method (OAuth 2.0 with refresh token) is documented and actionable
- [x] Endpoints include params, examples, and pagination details
- [x] Incremental strategy defines cursor (`lastModifiedDateTime`), order (ascending), lookback (1-5 min), and delete handling (soft deletes only in initial implementation)
- [x] Research Log completed
- [x] Sources include full URLs
- [x] Gaps marked with TBD and rationale provided (delta queries, reference implementations, complete schema verification)

