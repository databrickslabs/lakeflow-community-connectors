# Gmail API Documentation

## Authorization

### OAuth 2.0 (Preferred Method)

Gmail API uses OAuth 2.0 for authentication. The connector stores `client_id`, `client_secret`, and `refresh_token`, then exchanges for an access token at runtime.

**Required Credentials:**
- `client_id`: OAuth 2.0 client ID from Google Cloud Console
- `client_secret`: OAuth 2.0 client secret
- `refresh_token`: Long-lived refresh token obtained via OAuth flow

**Required Scopes:**
- `https://www.googleapis.com/auth/gmail.readonly` - Read-only access to Gmail

**Token Exchange Request:**
```http
POST https://oauth2.googleapis.com/token
Content-Type: application/x-www-form-urlencoded

client_id={client_id}&
client_secret={client_secret}&
refresh_token={refresh_token}&
grant_type=refresh_token
```

**Token Exchange Response:**
```json
{
  "access_token": "ya29.a0AfH6SM...",
  "expires_in": 3599,
  "scope": "https://www.googleapis.com/auth/gmail.readonly",
  "token_type": "Bearer"
}
```

**Using Access Token in API Requests:**
```http
GET https://gmail.googleapis.com/gmail/v1/users/me/messages
Authorization: Bearer {access_token}
```

### Alternative: Service Account (for Google Workspace)
For Google Workspace domains, service accounts with domain-wide delegation can be used. This requires admin configuration and is typically used for enterprise deployments.

## Object List

The Gmail API provides the following objects/resources. The object list is **static** and defined by the API.

| Object | Description | Recommended for Connector |
|--------|-------------|---------------------------|
| `messages` | Email messages in mailbox | ✅ Yes |
| `threads` | Email conversation threads | ✅ Yes |
| `labels` | Labels/folders for organizing emails | ✅ Yes |
| `drafts` | Draft messages | Optional |
| `profile` | Mailbox profile (email address, totals) | ✅ Yes |
| `settings` | IMAP/POP/auto-forwarding/language/vacation settings | ✅ Yes |
| `filters` | Server-side mail filter rules | ✅ Yes |
| `forwarding_addresses` | Verified forwarding destinations | ✅ Yes |
| `send_as` | Send-as identities (alias from-addresses) | ✅ Yes |
| `delegates` | Mailbox delegation grants | ✅ Yes |
| `history` | Mailbox change history (for incremental sync) | Used internally |

### Object Hierarchy
- **messages** and **threads** are top-level objects
- **labels** are independent organizational units
- **attachments** are nested under messages (`users.messages.attachments`)
- **history** tracks changes across messages and labels

## Object Schema

### Messages Schema

Retrieved via `GET /gmail/v1/users/{userId}/messages/{id}`

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Immutable message ID |
| `threadId` | string | ID of the thread this message belongs to |
| `labelIds` | array[string] | List of label IDs applied to message |
| `snippet` | string | Short preview of message content |
| `historyId` | string | ID of the last history record that modified this message |
| `internalDate` | string (int64) | Internal message creation timestamp (epoch ms) |
| `payload` | object | Parsed email structure (MessagePart) |
| `payload.partId` | string | Part ID (for multipart messages) |
| `payload.mimeType` | string | MIME type of the message part |
| `payload.filename` | string | Filename for attachment parts |
| `payload.headers` | array[object] | List of headers (name/value pairs) |
| `payload.body` | object | Message body data |
| `payload.body.attachmentId` | string | Attachment ID (if body is attachment) |
| `payload.body.size` | integer | Body size in bytes |
| `payload.body.data` | string | Base64url encoded body data |
| `payload.parts` | array[object] | Child parts (for multipart messages) |
| `sizeEstimate` | integer | Estimated total message size in bytes |
| `raw` | string | Entire email in RFC 2822 format (base64url, only with format=raw) |

**Common Headers (extracted from payload.headers):**
- `From`, `To`, `Cc`, `Bcc`, `Subject`, `Date`, `Message-ID`, `In-Reply-To`, `References`

### Threads Schema

Retrieved via `GET /gmail/v1/users/{userId}/threads/{id}`

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Immutable thread ID |
| `snippet` | string | Short preview of the latest message |
| `historyId` | string | ID of last history record modifying this thread |
| `messages` | array[Message] | List of messages in thread (when fetching full thread) |

### Labels Schema

Retrieved via `GET /gmail/v1/users/{userId}/labels/{id}`

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Immutable label ID |
| `name` | string | Display name of label |
| `messageListVisibility` | string | `show` or `hide` in message list |
| `labelListVisibility` | string | `labelShow`, `labelShowIfUnread`, `labelHide` |
| `type` | string | `system` or `user` |
| `messagesTotal` | integer | Total messages with this label |
| `messagesUnread` | integer | Unread messages with this label |
| `threadsTotal` | integer | Total threads with this label |
| `threadsUnread` | integer | Unread threads with this label |
| `color` | object | Label color (textColor, backgroundColor) |

### Drafts Schema

Retrieved via `GET /gmail/v1/users/{userId}/drafts/{id}`

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Immutable draft ID |
| `message` | object | The message content of the draft |

### Profile Schema

Retrieved via `GET /gmail/v1/users/{userId}/profile`

| Field | Type | Description |
|-------|------|-------------|
| `emailAddress` | string | The user's email address (primary key) |
| `messagesTotal` | integer | Total number of messages in the mailbox |
| `threadsTotal` | integer | Total number of threads in the mailbox |
| `historyId` | string | The mailbox's current history ID |

### Settings Schema

Retrieved via the `users.settings.*` API surface (IMAP, POP, auto-forwarding,
language, vacation responder). The connector packs these into a single
`settings` row keyed by `userId`.

| Field | Type | Description |
|-------|------|-------------|
| `userId` | string | Mailbox owner identifier (primary key) |
| `imap` | struct | IMAP settings (`enabled`, `autoExpunge`, `expungeBehavior`, `maxFolderSize`) |
| `pop` | struct | POP settings (`accessWindow`, `disposition`) |
| `autoForwarding` | struct | Auto-forwarding settings (`enabled`, `emailAddress`, `disposition`) |
| `language` | struct | Display language (`displayLanguage`) |
| `vacation` | struct | Vacation responder (`enableAutoReply`, `responseSubject`, `responseBodyPlainText`, `restrictToContacts`, `restrictToDomain`, `startTime`, `endTime`) |

Source endpoints:
- `GET /gmail/v1/users/{userId}/settings/imap`
- `GET /gmail/v1/users/{userId}/settings/pop`
- `GET /gmail/v1/users/{userId}/settings/autoForwarding`
- `GET /gmail/v1/users/{userId}/settings/language`
- `GET /gmail/v1/users/{userId}/settings/vacation`

### Filters Schema

Retrieved via `GET /gmail/v1/users/{userId}/settings/filters`

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Filter identifier (primary key) |
| `criteria` | struct | Match criteria (`from`, `to`, `subject`, `query`, `negatedQuery`, `hasAttachment`, `excludeChats`, `size`, `sizeComparison`) |
| `action` | struct | Filter action (`addLabelIds`, `removeLabelIds`, `forward`) |

### Forwarding Addresses Schema

Retrieved via `GET /gmail/v1/users/{userId}/settings/forwardingAddresses`

| Field | Type | Description |
|-------|------|-------------|
| `forwardingEmail` | string | Verified forwarding destination (primary key) |
| `verificationStatus` | string | `pending`, `accepted`, `verificationStatusUnspecified` |

### Send-As Schema

Retrieved via `GET /gmail/v1/users/{userId}/settings/sendAs`

| Field | Type | Description |
|-------|------|-------------|
| `sendAsEmail` | string | Send-as identity address (primary key) |
| `displayName` | string | Display name shown to recipients |
| `replyToAddress` | string | Reply-to address |
| `signature` | string | HTML signature for this identity |
| `isPrimary` | boolean | Whether this is the primary identity |
| `isDefault` | boolean | Whether this is the default identity |
| `treatAsAlias` | boolean | Whether the address is treated as an alias |
| `verificationStatus` | string | Verification status |
| `smtpMsa` | struct | SMTP MSA settings (`host`, `port`, `username`, `securityMode`) |

### Delegates Schema

Retrieved via `GET /gmail/v1/users/{userId}/settings/delegates`

| Field | Type | Description |
|-------|------|-------------|
| `delegateEmail` | string | Delegate's email address (primary key) |
| `verificationStatus` | string | `pending`, `accepted`, `rejected`, `expired` |

## Get Object Primary Keys

Primary keys are **static** and defined by the API structure:

| Object | Primary Key | Notes |
|--------|-------------|-------|
| `messages` | `id` | Immutable message identifier |
| `threads` | `id` | Immutable thread identifier |
| `labels` | `id` | Immutable label identifier |
| `drafts` | `id` | Immutable draft identifier |
| `profile` | `emailAddress` | One row per mailbox, keyed by user email |
| `settings` | `userId` | One row per mailbox, packs all settings sub-resources |
| `filters` | `id` | Server-assigned filter identifier |
| `forwarding_addresses` | `forwardingEmail` | The verified forwarding destination |
| `send_as` | `sendAsEmail` | The send-as identity address |
| `delegates` | `delegateEmail` | The delegate's email address |

## Object's Ingestion Type

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `messages` | `cdc_with_deletes` | Can use `historyId` for incremental sync; deleted messages returned via history API |
| `threads` | `cdc_with_deletes` | Threads modified when messages change; use history for incremental |
| `labels` | `snapshot` | Labels change infrequently; full refresh recommended |
| `drafts` | `snapshot` | Drafts change frequently via user edits; snapshot safer |
| `profile` | `snapshot` | Single-row mailbox summary; refreshes on every microbatch |
| `settings` | `snapshot` | Settings change rarely; snapshot per microbatch |
| `filters` | `snapshot` | List endpoint returns all filters; no incremental cursor available |
| `forwarding_addresses` | `snapshot` | List endpoint returns all addresses; small set |
| `send_as` | `snapshot` | List endpoint returns all identities; small set |
| `delegates` | `snapshot` | List endpoint returns all delegates; small set |

**History API for Incremental Sync:**
The `users.history.list` endpoint returns changes since a given `historyId`:
- `messagesAdded` - New messages
- `messagesDeleted` - Deleted messages  
- `labelsAdded` - Labels added to messages
- `labelsRemoved` - Labels removed from messages

## Read API for Data Retrieval

### List Messages

**Endpoint:** `GET /gmail/v1/users/{userId}/messages`

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `userId` | string | Yes | User's email or `me` for authenticated user |
| `maxResults` | integer | No | Max messages to return (default 100, max 500) |
| `pageToken` | string | No | Token for next page of results |
| `q` | string | No | Gmail search query (same syntax as web UI) |
| `labelIds` | array[string] | No | Only return messages with these labels |
| `includeSpamTrash` | boolean | No | Include SPAM and TRASH (default false) |

**Example Request:**
```http
GET https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=100
Authorization: Bearer {access_token}
```

**Example Response:**
```json
{
  "messages": [
    {"id": "18d1234abcd", "threadId": "18d1234abcd"},
    {"id": "18d1235efgh", "threadId": "18d1234abcd"}
  ],
  "nextPageToken": "12345678901234567890",
  "resultSizeEstimate": 1500
}
```

### Get Message Details

**Endpoint:** `GET /gmail/v1/users/{userId}/messages/{id}`

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `userId` | string | Yes | User's email or `me` |
| `id` | string | Yes | Message ID |
| `format` | string | No | `minimal`, `full` (default), `raw`, `metadata` |
| `metadataHeaders` | array[string] | No | Headers to include (only with format=metadata) |

**Example Request:**
```http
GET https://gmail.googleapis.com/gmail/v1/users/me/messages/18d1234abcd?format=full
Authorization: Bearer {access_token}
```

### List Threads

**Endpoint:** `GET /gmail/v1/users/{userId}/threads`

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `userId` | string | Yes | User's email or `me` |
| `maxResults` | integer | No | Max threads to return (default 100, max 500) |
| `pageToken` | string | No | Token for next page |
| `q` | string | No | Gmail search query |
| `labelIds` | array[string] | No | Filter by labels |
| `includeSpamTrash` | boolean | No | Include SPAM and TRASH |

### Get Thread Details

**Endpoint:** `GET /gmail/v1/users/{userId}/threads/{id}`

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `userId` | string | Yes | User's email or `me` |
| `id` | string | Yes | Thread ID |
| `format` | string | No | `minimal`, `full` (default), `metadata` |
| `metadataHeaders` | array[string] | No | Headers to include |

### List Labels

**Endpoint:** `GET /gmail/v1/users/{userId}/labels`

**Example Request:**
```http
GET https://gmail.googleapis.com/gmail/v1/users/me/labels
Authorization: Bearer {access_token}
```

**Example Response:**
```json
{
  "labels": [
    {"id": "INBOX", "name": "INBOX", "type": "system"},
    {"id": "SENT", "name": "SENT", "type": "system"},
    {"id": "Label_1", "name": "Work", "type": "user"}
  ]
}
```

### History API (for Incremental Sync)

**Endpoint:** `GET /gmail/v1/users/{userId}/history`

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `userId` | string | Yes | User's email or `me` |
| `startHistoryId` | string | Yes | History ID to start from |
| `maxResults` | integer | No | Max history records (default 100, max 500) |
| `pageToken` | string | No | Token for next page |
| `labelId` | string | No | Filter by label |
| `historyTypes` | array[string] | No | `messageAdded`, `messageDeleted`, `labelAdded`, `labelRemoved` |

**Example Request:**
```http
GET https://gmail.googleapis.com/gmail/v1/users/me/history?startHistoryId=12345
Authorization: Bearer {access_token}
```

**Example Response:**
```json
{
  "history": [
    {
      "id": "12346",
      "messages": [{"id": "18d1234abcd", "threadId": "18d1234abcd"}],
      "messagesAdded": [{"message": {"id": "18d1234abcd", "threadId": "18d1234abcd", "labelIds": ["INBOX"]}}]
    }
  ],
  "historyId": "12350",
  "nextPageToken": "token123"
}
```

### Pagination Strategy

All list endpoints use **token-based pagination**:
1. Initial request returns `nextPageToken` if more results exist
2. Subsequent requests include `pageToken` parameter
3. Continue until no `nextPageToken` returned

### Incremental Sync Strategy

1. **Initial Full Sync:** List all messages, store the latest `historyId`
2. **Incremental Updates:** Call history API with stored `startHistoryId`
3. **Process Changes:**
   - `messagesAdded`: Fetch full message details
   - `messagesDeleted`: Mark as deleted in destination
   - `labelsAdded`/`labelsRemoved`: Update label associations
4. **Update Cursor:** Store new `historyId` from response

**Important:** If `historyId` is too old (>~30 days), API returns 404. Fall back to full sync.

### Rate Limits

| Quota Type | Limit |
|------------|-------|
| Queries per day | 1,000,000,000 |
| Queries per 100 seconds per user | 25,000 |
| Queries per 100 seconds | 250,000 |

**Best Practices:**
- Use batch requests when possible
- Implement exponential backoff for 429 errors
- Use `format=metadata` when full body not needed
- Cache label list (changes infrequently)

## Field Type Mapping

| Gmail API Type | Spark SQL Type | Notes |
|----------------|----------------|-------|
| string | StringType | Default for most fields |
| string (int64) | LongType | `internalDate`, `historyId` are numeric strings |
| integer | IntegerType | `sizeEstimate`, message counts |
| boolean | BooleanType | `includeSpamTrash` flags |
| array[string] | ArrayType(StringType) | `labelIds` |
| array[object] | ArrayType(StructType) | `headers`, `parts` |
| object | StructType | Nested objects like `payload`, `body` |
| base64url string | StringType | `raw` message data, `body.data` |

### Special Field Behaviors

- **`internalDate`**: Epoch milliseconds as string; convert to timestamp
- **`historyId`**: Monotonically increasing; use for cursor comparison
- **`payload.body.data`**: Base64url encoded; requires decoding
- **`raw`**: Only returned with `format=raw`; entire RFC 2822 message

## Known Quirks

1. **History ID Expiration:** History records older than ~30 days may be unavailable; requires full resync fallback
2. **Message Format Trade-offs:**
   - `full`: Complete parsed structure but larger response
   - `metadata`: Only headers, smaller but need separate call for body
   - `raw`: Entire message but requires parsing
3. **Label IDs vs Names:** System labels use predefined IDs (INBOX, SENT, etc.); user labels have generated IDs

## Sources and References

| Source Type | URL | Confidence | What it confirmed |
|-------------|-----|------------|-------------------|
| Official API Docs | https://developers.google.com/workspace/gmail/api/reference/rest | Highest | All endpoints, schemas, parameters |
| Gmail API Guides | https://developers.google.com/workspace/gmail/api/guides | High | Auth flow, incremental sync strategy |
| Google OAuth 2.0 | https://developers.google.com/identity/protocols/oauth2 | Highest | Token exchange, refresh flow |
| API Usage Limits | https://developers.google.com/workspace/gmail/api/reference/quota | High | Rate limits |

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.google.com/workspace/gmail/api/reference/rest | 2026-01-09 | Highest | All REST endpoints, parameters, schemas |
| Official Docs | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages | 2026-01-09 | Highest | Messages schema, list/get endpoints |
| Official Docs | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.threads | 2026-01-09 | Highest | Threads schema, list/get endpoints |
| Official Docs | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.labels | 2026-01-09 | Highest | Labels schema, list endpoint |
| Official Docs | https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history | 2026-01-09 | Highest | History API for incremental sync |

