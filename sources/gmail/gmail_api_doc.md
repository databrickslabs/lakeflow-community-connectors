# **Gmail API Documentation**

## **Authorization**

- **Chosen method**: OAuth 2.0 with refresh token for the Gmail API v1.
- **Base URL**: `https://gmail.googleapis.com`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <access_token>`
  - The connector **stores** `client_id`, `client_secret`, and `refresh_token` and exchanges them for an `access_token` at runtime
  - The connector **does not** run user-facing OAuth flows; tokens must be provisioned out-of-band
- **Recommended scope for read-only access**: `https://www.googleapis.com/auth/gmail.readonly`
  - This scope provides read-only access to all resources and their metadata without any write operations
  - Alternative scopes available but not recommended for read-only connector:
    - `https://www.googleapis.com/auth/gmail.metadata` - Only labels, history records, and headers (not bodies)
    - `https://mail.google.com/` - Full mailbox access including deletion (too broad)

**OAuth 2.0 Token Exchange Flow**:
The connector exchanges the refresh token for an access token using:
```bash
POST https://oauth2.googleapis.com/token
Content-Type: application/x-www-form-urlencoded

client_id=<CLIENT_ID>&
client_secret=<CLIENT_SECRET>&
refresh_token=<REFRESH_TOKEN>&
grant_type=refresh_token
```

Response:
```json
{
  "access_token": "ya29.xxx",
  "expires_in": 3599,
  "scope": "https://www.googleapis.com/auth/gmail.readonly",
  "token_type": "Bearer"
}
```

Example authenticated API request:
```bash
curl -X GET \
  -H "Authorization: Bearer <ACCESS_TOKEN>" \
  -H "Accept: application/json" \
  "https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=10"
```

**Rate Limits**:
- **Per Project**: 1,200,000 quota units per minute (error: `rateLimitExceeded`)
- **Per User**: 15,000 quota units per user per minute (error: `userRateLimitExceeded`)
- Quota costs vary by method (1-100 units per call):
  - List operations: 5 units
  - Get operations: 5 units
  - Batch operations: 50 units
  - Send operations: 100 units


## **Object List**

For the Gmail connector, we treat Gmail API resources as **objects/tables**.
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `messages` | Email messages in the user's mailbox | `GET /gmail/v1/users/{userId}/messages` | `cdc` (incremental via history API) |
| `threads` | Email conversation threads | `GET /gmail/v1/users/{userId}/threads` | `cdc` (incremental via history API) |
| `labels` | User labels (both system and custom) | `GET /gmail/v1/users/{userId}/labels` | `snapshot` |
| `drafts` | Draft messages | `GET /gmail/v1/users/{userId}/drafts` | `cdc` (incremental via history API) |
| `profile` | User profile information | `GET /gmail/v1/users/{userId}/profile` | `snapshot` |

**Connector scope for initial implementation**:
- Step 1 focuses on the `messages` and `labels` objects with detailed documentation
- `threads`, `drafts`, and `profile` are documented for future extension

**High-level notes on objects**:
- **Messages**: Core object containing email data. Supports incremental sync via the history API which tracks changes (additions, deletions, label modifications) since a given `historyId`.
- **Threads**: Conversation containers grouping related messages. Each thread contains an array of messages. Supports incremental sync via history API.
- **Labels**: Both system labels (INBOX, SENT, TRASH, SPAM, etc.) and user-created labels. Used for filtering and organizing messages. Small dataset suitable for snapshot refresh.
- **Drafts**: Unsent email messages stored with the DRAFT label. Supports incremental tracking via history API.
- **Profile**: Contains user's email address, total message/thread counts, and history ID. Static metadata refreshed periodically.


## **Object Schema**

### General notes

- Gmail provides JSON schemas via its REST API v1 reference documentation
- The connector defines **tabular schemas** per object derived from the JSON representation
- Nested JSON objects (e.g., `payload.headers`, `payload.parts`) are modeled as **nested structures/arrays**
- Message bodies can be retrieved in different formats via the `format` query parameter:
  - `minimal`: Returns only email message ID and labels
  - `full`: Returns full email message data including body (default)
  - `raw`: Returns raw MIME message as base64url encoded string
  - `metadata`: Returns only email message metadata (headers) without body

### `messages` object (primary table)

**Source endpoint**:
`GET /gmail/v1/users/{userId}/messages`

**Key behavior**:
- Returns message IDs and thread IDs by default (minimal format)
- Requires subsequent `GET /gmail/v1/users/{userId}/messages/{id}` call to retrieve full message details
- Supports filtering via `q` parameter (Gmail search syntax) and `labelIds` parameter
- Supports incremental sync via history API using `historyId` as cursor
- Message body content is in `payload.parts` as base64url encoded data

**High-level schema (connector view)**:

Top-level fields from the Gmail API Message resource:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Immutable message identifier |
| `threadId` | string | Thread identifier this message belongs to |
| `labelIds` | array[string] | List of label IDs applied to this message |
| `snippet` | string | Short snippet of the message text (first ~200 chars) |
| `historyId` | string | History record ID when this message was last modified. Used for incremental sync. |
| `internalDate` | string (int64) | Internal message creation timestamp in epoch milliseconds. Determines inbox ordering. |
| `sizeEstimate` | integer | Estimated size in bytes of the message |
| `raw` | string (base64url) | Raw RFC 2822 formatted message (only when format=RAW parameter used) |
| `payload` | MessagePart object | Parsed MIME structure of the message (see nested schema below) |

**Nested schema: `payload` (MessagePart object)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `partId` | string | Immutable identifier of the message part |
| `mimeType` | string | MIME type of this message part (e.g., `text/plain`, `text/html`, `multipart/alternative`) |
| `filename` | string | Filename if this part represents an attachment |
| `headers` | array[Header] | Array of email headers. For top-level part, contains RFC 2822 headers like `To`, `From`, `Subject`, `Date`. |
| `body` | MessagePartBody object | The body content of this part (see nested schema below) |
| `parts` | array[MessagePart] | Child MIME parts (for container types like `multipart/*`) |

**Nested schema: `headers` (Header object)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `name` | string | Header name (e.g., `To`, `From`, `Subject`, `Date`, `Cc`, `Bcc`) |
| `value` | string | Header value (e.g., `user@example.com`) |

**Nested schema: `body` (MessagePartBody object)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `attachmentId` | string | ID for external attachment (retrieve via separate attachments.get endpoint) |
| `size` | integer | Number of bytes in the body data |
| `data` | string (base64url) | Base64url encoded body data. May be empty for container MIME types or when body is sent as separate attachment. |

**Important headers to extract** (found in `payload.headers` array):
- `From`: Sender email address
- `To`: Recipient email addresses
- `Cc`: Carbon copy recipients
- `Bcc`: Blind carbon copy recipients
- `Subject`: Email subject line
- `Date`: Message send date
- `Message-ID`: Unique message identifier
- `In-Reply-To`: Message ID this message replies to
- `References`: Message IDs in the conversation thread

### `labels` object

**Source endpoint**:
`GET /gmail/v1/users/{userId}/labels`

**Key behavior**:
- Returns all labels (both system and user-created) in a single response
- No pagination required (max 10,000 labels per user)
- System labels have predefined IDs: `INBOX`, `SENT`, `TRASH`, `SPAM`, `DRAFT`, `UNREAD`, `STARRED`, `IMPORTANT`, `CATEGORY_PERSONAL`, `CATEGORY_SOCIAL`, `CATEGORY_PROMOTIONS`, `CATEGORY_UPDATES`, `CATEGORY_FORUMS`

**Schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Immutable label identifier |
| `name` | string | Display name of the label |
| `type` | string (enum) | Label type: `system` or `user` |
| `messageListVisibility` | string (enum) | Visibility in message list: `show` or `hide` |
| `labelListVisibility` | string (enum) | Visibility in label list: `labelShow`, `labelShowIfUnread`, or `labelHide` |
| `messagesTotal` | integer | Total number of messages with this label |
| `messagesUnread` | integer | Number of unread messages with this label |
| `threadsTotal` | integer | Total number of threads with this label |
| `threadsUnread` | integer | Number of unread threads with this label |
| `color` | object | Color settings for user labels (null for system labels) |

**Nested schema: `color` object** (user labels only):

| Column Name | Type | Description |
|------------|------|-------------|
| `textColor` | string | Hex color for label text (e.g., `#000000`) |
| `backgroundColor` | string | Hex color for label background (e.g., `#16a766`) |

### `threads` object

**Source endpoint**:
`GET /gmail/v1/users/{userId}/threads`

**Key behavior**:
- Returns thread IDs and snippets by default
- Requires subsequent `GET /gmail/v1/users/{userId}/threads/{id}` call to retrieve full thread with all messages
- Each thread contains an array of complete message objects
- Supports same filtering as messages (`q` and `labelIds` parameters)

**Schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Immutable thread identifier |
| `snippet` | string | Short snippet from the most recent message in the thread |
| `historyId` | string | History record ID when this thread was last modified |
| `messages` | array[Message] | Array of complete message objects in this thread (from get endpoint) |

### `drafts` object

**Source endpoint**:
`GET /gmail/v1/users/{userId}/drafts`

**Key behavior**:
- Returns draft IDs by default
- Requires subsequent `GET /gmail/v1/users/{userId}/drafts/{id}` call to retrieve full draft
- Each draft contains a complete message object with the DRAFT label

**Schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Immutable draft identifier |
| `message` | Message object | Complete message object containing the draft content |

### `profile` object

**Source endpoint**:
`GET /gmail/v1/users/{userId}/profile`

**Key behavior**:
- Returns user's Gmail profile information
- Single record per user (no pagination)
- Includes current `historyId` which can be used to start incremental sync

**Schema**:

| Column Name | Type | Description |
|------------|------|-------------|
| `emailAddress` | string | User's email address |
| `messagesTotal` | integer | Total number of messages in the mailbox |
| `threadsTotal` | integer | Total number of threads in the mailbox |
| `historyId` | string | Current mailbox history ID (can be used as starting point for incremental sync) |


## **Get Object Primary Keys**

Primary keys for each object are defined statically in the connector based on Gmail API documentation.

| Object Name | Primary Key Column(s) | Notes |
|------------|----------------------|-------|
| `messages` | `id` | Immutable message identifier, globally unique |
| `threads` | `id` | Immutable thread identifier, globally unique |
| `labels` | `id` | Immutable label identifier, unique per user |
| `drafts` | `id` | Immutable draft identifier, unique per user |
| `profile` | `emailAddress` | User's email address (single record) |

**Notes**:
- All primary keys are provided directly by the Gmail API in the response
- No additional API calls are needed to determine primary keys
- Primary keys are immutable and guaranteed unique by Gmail


## **Object's ingestion type**

Ingestion types are defined statically based on Gmail API capabilities and resource characteristics:

| Object Name | Ingestion Type | Cursor Field | Rationale |
|------------|----------------|--------------|-----------|
| `messages` | `cdc` | `historyId` | Supports incremental sync via history API. History records track message additions, deletions, and label changes. |
| `threads` | `cdc` | `historyId` | Supports incremental sync via history API. Thread changes are tracked when messages within threads are modified. |
| `labels` | `snapshot` | N/A | Small dataset (max 10,000 labels). No incremental API. Full refresh is efficient. |
| `drafts` | `cdc` | `historyId` | Can be incrementally synced via history API by filtering for DRAFT label changes. |
| `profile` | `snapshot` | N/A | Single record per user. Metadata only. Full refresh is efficient. |

**Note on `cdc_with_deletes`**:
- Gmail's history API provides `messagesDeleted` events, making it theoretically possible to support `cdc_with_deletes`
- However, implementing this requires careful handling of trash vs. permanent deletion
- For initial implementation, we use `cdc` ingestion type and rely on label changes to detect messages moved to TRASH
- Future enhancement: Support `cdc_with_deletes` by tracking `messagesDeleted` events in history records


## **Read API for Data Retrieval**

### **messages** - Reading Messages

#### Initial Full Sync

**Step 1: List all message IDs**

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=500
Authorization: Bearer <access_token>
```

Query parameters:
- `maxResults` (optional, integer): Maximum messages to return per page. Default: 100, Max: 500.
- `pageToken` (optional, string): Token for retrieving next page of results.
- `q` (optional, string): Gmail search query to filter messages (e.g., `from:user@example.com is:unread`). Not available with `gmail.metadata` scope.
- `labelIds` (optional, array[string]): Filter by label IDs. Can specify multiple (e.g., `labelIds=INBOX&labelIds=UNREAD`).
- `includeSpamTrash` (optional, boolean): Include messages from SPAM and TRASH folders. Default: false.

Response:
```json
{
  "messages": [
    {"id": "18cf18a1b2e3f4d5", "threadId": "18cf18a1b2e3f4d5"},
    {"id": "18cf192837465abc", "threadId": "18cf18a1b2e3f4d5"}
  ],
  "nextPageToken": "09876543210",
  "resultSizeEstimate": 2450
}
```

**Step 2: Retrieve full message details**

For each message ID from step 1:

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/messages/18cf18a1b2e3f4d5?format=full
Authorization: Bearer <access_token>
```

Query parameters:
- `format` (optional, string): Format of the message. Options: `minimal`, `full` (default), `raw`, `metadata`.
- `metadataHeaders` (optional, array[string]): When format=metadata, specify which headers to include.

Response (abbreviated):
```json
{
  "id": "18cf18a1b2e3f4d5",
  "threadId": "18cf18a1b2e3f4d5",
  "labelIds": ["INBOX", "UNREAD"],
  "snippet": "Hi there, this is a test email...",
  "historyId": "543210",
  "internalDate": "1673980800000",
  "sizeEstimate": 4532,
  "payload": {
    "partId": "",
    "mimeType": "multipart/alternative",
    "filename": "",
    "headers": [
      {"name": "From", "value": "sender@example.com"},
      {"name": "To", "value": "recipient@example.com"},
      {"name": "Subject", "value": "Test Email"},
      {"name": "Date", "value": "Tue, 17 Jan 2023 10:00:00 -0800"}
    ],
    "body": {"size": 0},
    "parts": [
      {
        "partId": "0",
        "mimeType": "text/plain",
        "filename": "",
        "headers": [{"name": "Content-Type", "value": "text/plain; charset=UTF-8"}],
        "body": {
          "size": 89,
          "data": "SGkgdGhlcmUsIHRoaXMgaXMgYSB0ZXN0IGVtYWlsLg=="
        }
      },
      {
        "partId": "1",
        "mimeType": "text/html",
        "filename": "",
        "headers": [{"name": "Content-Type", "value": "text/html; charset=UTF-8"}],
        "body": {
          "size": 142,
          "data": "PGh0bWw+PGJvZHk+SGkgdGhlcmUsIHRoaXMgaXMgYSB0ZXN0IGVtYWlsLjwvYm9keT48L2h0bWw+"
        }
      }
    ]
  }
}
```

**Step 3: Retrieve attachments (if present)**

If a message part has `attachmentId` instead of inline `data`:

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/messages/{messageId}/attachments/{attachmentId}
Authorization: Bearer <access_token>
```

Response:
```json
{
  "size": 82654,
  "data": "R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw=="
}
```

**Step 4: Store the maximum historyId**

After syncing all messages, store the highest `historyId` value encountered. This will be used as the starting point for incremental syncs.

#### Incremental Sync via History API

**Step 1: List history changes since last sync**

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/history?startHistoryId=543210&maxResults=500
Authorization: Bearer <access_token>
```

Query parameters:
- `startHistoryId` (required, string): Returns history records after this ID.
- `maxResults` (optional, integer): Max history records to return. Default: 100, Max: 500.
- `pageToken` (optional, string): Token for next page.
- `labelId` (optional, string): Filter to changes for messages with this label.
- `historyTypes` (optional, array[string]): Types of changes to return. Options: `messageAdded`, `messageDeleted`, `labelAdded`, `labelRemoved`. If not specified, all types are returned.

Response:
```json
{
  "history": [
    {
      "id": "543211",
      "messages": [{"id": "18cf192837465abc", "threadId": "18cf18a1b2e3f4d5"}],
      "messagesAdded": [
        {
          "message": {
            "id": "18cf192837465abc",
            "threadId": "18cf18a1b2e3f4d5",
            "labelIds": ["INBOX", "UNREAD"]
          }
        }
      ]
    },
    {
      "id": "543212",
      "messages": [{"id": "18cf18a1b2e3f4d5", "threadId": "18cf18a1b2e3f4d5"}],
      "labelsAdded": [
        {
          "message": {
            "id": "18cf18a1b2e3f4d5",
            "threadId": "18cf18a1b2e3f4d5",
            "labelIds": ["STARRED"]
          },
          "labelIds": ["STARRED"]
        }
      ]
    },
    {
      "id": "543213",
      "messages": [{"id": "18cf100000000001", "threadId": "18cf100000000001"}],
      "messagesDeleted": [
        {
          "message": {
            "id": "18cf100000000001",
            "threadId": "18cf100000000001"
          }
        }
      ]
    }
  ],
  "nextPageToken": "09876543211",
  "historyId": "543213"
}
```

**Step 2: Process history changes**

For each history record:
- **messagesAdded**: Fetch full message details using messages.get endpoint
- **messagesDeleted**: Mark message as deleted in destination (or use `read_table_deletes` if implementing `cdc_with_deletes`)
- **labelsAdded/labelsRemoved**: Fetch updated message to get current label state, or update labels directly if tracking label changes

**Step 3: Update stored historyId**

Store the `historyId` from the response for the next incremental sync.

**Error handling**:
- **HTTP 404 with `historyId` not found**: The stored `historyId` is too old (history data expired, typically after 1 week). Perform a full sync and obtain a new `historyId`.

### **labels** - Reading Labels

**Endpoint**: `GET /gmail/v1/users/me/labels`

No pagination required (returns all labels in single response):

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/labels
Authorization: Bearer <access_token>
```

Response:
```json
{
  "labels": [
    {
      "id": "INBOX",
      "name": "INBOX",
      "type": "system",
      "messageListVisibility": "show",
      "labelListVisibility": "labelShow",
      "messagesTotal": 1245,
      "messagesUnread": 34,
      "threadsTotal": 892,
      "threadsUnread": 28
    },
    {
      "id": "Label_1",
      "name": "Important Projects",
      "type": "user",
      "messageListVisibility": "show",
      "labelListVisibility": "labelShow",
      "messagesTotal": 67,
      "messagesUnread": 5,
      "threadsTotal": 45,
      "threadsUnread": 4,
      "color": {
        "textColor": "#ffffff",
        "backgroundColor": "#16a766"
      }
    }
  ]
}
```

### **threads** - Reading Threads

Similar to messages, with two-step process:

**Step 1: List thread IDs**

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/threads?maxResults=500
Authorization: Bearer <access_token>
```

Same query parameters as messages list endpoint.

**Step 2: Retrieve full thread details**

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/threads/{threadId}?format=full
Authorization: Bearer <access_token>
```

Response includes complete thread with all messages in the `messages` array.

**Incremental sync**: Use same history API approach as messages.

### **drafts** - Reading Drafts

**Step 1: List draft IDs**

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/drafts?maxResults=500
Authorization: Bearer <access_token>
```

Query parameters:
- `maxResults` (optional, integer): Default: 100, Max: 500.
- `pageToken` (optional, string): Pagination token.

**Step 2: Retrieve full draft details**

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/drafts/{draftId}?format=full
Authorization: Bearer <access_token>
```

**Incremental sync**: Use history API filtered by DRAFT label changes.

### **profile** - Reading Profile

Single API call, no pagination:

```bash
GET https://gmail.googleapis.com/gmail/v1/users/me/profile
Authorization: Bearer <access_token>
```

Response:
```json
{
  "emailAddress": "user@example.com",
  "messagesTotal": 1245,
  "threadsTotal": 892,
  "historyId": "543210"
}
```

### **Pagination Strategy**

All list endpoints use cursor-based pagination:
1. First request: Include `maxResults` parameter (recommend 500 for efficiency)
2. Response includes `nextPageToken` if more results exist
3. Subsequent requests: Include `pageToken` parameter with the token from previous response
4. Continue until response has no `nextPageToken`

**Best practice**: Set `maxResults=500` to minimize API calls and quota usage (5 units per call).

### **Rate Limiting Best Practices**

1. **Monitor quota usage**: Track API calls to stay within limits (1.2M per minute project-wide, 15K per minute per user)
2. **Implement exponential backoff**: When receiving `rateLimitExceeded` or `userRateLimitExceeded` errors
3. **Batch where possible**: Use batch requests for fetching multiple messages (though Gmail API batch support is limited)
4. **Use format=metadata for updates**: When only checking for changes, use `format=metadata` or `format=minimal` to reduce response size and processing
5. **Optimize pagination**: Use maximum `maxResults=500` to minimize number of list calls


## **Field Type Mapping**

Gmail API field types mapped to standard data types:

| Gmail API Type | Standard Type | Python Type | Spark SQL Type | Notes |
|----------------|---------------|-------------|----------------|-------|
| `string` | string | str | StringType() | Text values, IDs |
| `integer` | integer | int | IntegerType() | Counts, small numbers |
| `string (int64)` | long | int | LongType() | Large numbers, timestamps (internalDate, historyId) |
| `string (bytes, base64url)` | binary | bytes | BinaryType() | Encoded data, raw messages, attachment data |
| `boolean` | boolean | bool | BooleanType() | True/false values |
| `array[string]` | array | list[str] | ArrayType(StringType()) | Lists of IDs, labels |
| `array[object]` | array | list[dict] | ArrayType(StructType(...)) | Lists of nested objects (headers, parts) |
| `object` | struct | dict | StructType(...) | Nested objects (payload, body, color) |
| `string (enum)` | string | str | StringType() | Enumerated values (state, type, visibility) |
| `string (ISO 8601 datetime)` | timestamp | datetime | TimestampType() | Date header values |
| `string (epoch ms)` | timestamp | datetime | TimestampType() | internalDate field (requires conversion from milliseconds) |
| `null` | null | None | NullType | Nullable fields (closedAt, body, color) |

**Special field behaviors**:

1. **historyId**: Stored as string in API but represents a 64-bit unsigned integer. Should be treated as opaque string for storage and comparison.

2. **internalDate**: Returned as string containing epoch milliseconds. Convert to timestamp: `datetime.fromtimestamp(int(internalDate) / 1000)`

3. **Base64url encoding**: Fields like `raw`, `body.data`, and attachment data are base64url encoded (not standard base64). Use URL-safe base64 decoding.

4. **labelIds**: Array of strings, can be empty array. System label IDs are constants (INBOX, SENT, etc.). User label IDs have format `Label_<number>`.

5. **Headers**: Array of name-value pairs. Common headers (From, To, Subject) should be extracted and stored as top-level fields for easier querying. Date header is ISO 8601 string.

6. **Nested MIME structure**: `payload.parts` can be arbitrarily nested for multipart messages. Recursive processing required.

7. **Attachment data**: When `attachmentId` is present, `body.data` is empty. Separate API call required to fetch attachment data.

**Data quality considerations**:
- `snippet` is truncated and may not represent full message content
- `sizeEstimate` is approximate, not exact byte count
- `resultSizeEstimate` from list endpoint is approximate
- Deleted messages may still appear in history records after permanent deletion


## **Sources and References**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official API Reference | https://developers.google.com/workspace/gmail/api/reference/rest | 2026-01-07 | **Highest** | API endpoint structure, resource schemas, methods, authentication requirements |
| Official Gmail API Guide | https://developers.google.com/workspace/gmail/api/guides/sync | 2026-01-07 | **Highest** | Incremental sync strategy using history API, error handling, best practices |
| Official OAuth Scopes Documentation | https://developers.google.com/workspace/gmail/api/auth/scopes | 2026-01-07 | **Highest** | Available OAuth scopes, permissions, security recommendations |
| Official Rate Limits Documentation | https://developers.google.com/workspace/gmail/api/reference/quota | 2026-01-07 | **Highest** | Quota limits, per-method costs, rate limit errors |
| Messages API Reference | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages | 2026-01-07 | **Highest** | Complete Message resource schema including all nested objects |
| Messages List Reference | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/list | 2026-01-07 | **Highest** | List endpoint parameters, pagination, filtering options |
| Threads API Reference | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.threads | 2026-01-07 | **Highest** | Thread resource schema, relationship to messages |
| Labels API Reference | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.labels | 2026-01-07 | **Highest** | Label resource schema, system vs user labels |
| History API Reference | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list | 2026-01-07 | **Highest** | History resource structure, incremental sync parameters, change types |
| Drafts API Reference | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.drafts | 2026-01-07 | **Highest** | Draft resource schema, relationship to messages |
| Attachments API Reference | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages.attachments | 2026-01-07 | **Highest** | Attachment retrieval method, MessagePartBody schema |
| Airbyte Gmail Connector Documentation | https://docs.airbyte.com/integrations/sources/gmail | 2026-01-07 | **High** | Supported streams, sync modes, authentication approach, implementation limitations |
| Airbyte Gmail Connector GitHub | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-gmail | 2026-01-07 | **High** | Manifest-only connector structure, available streams confirmation |

**Conflict Resolution**:
No conflicts were found between sources. Official Google documentation was used as the primary source for all technical details. Airbyte connector was used as a secondary reference to validate practical implementation approach and confirm which resources are most commonly needed.

**Notes**:
- Gmail API v1 is the current stable version (no v2 announced)
- History API availability is typically 1 week but not guaranteed - implementation must handle 404 errors
- All write/modify operations were excluded from this documentation as per Step 1 guidelines
- Focus on READ operations only: list, get endpoints for all resources
