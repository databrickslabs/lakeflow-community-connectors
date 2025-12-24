# Microsoft Teams API Documentation

## Authorization

### OAuth 2.0 Client Credentials Flow (App-Only Access)

The Microsoft Teams connector uses **OAuth 2.0 Client Credentials flow** for server-to-server authentication without user interaction. This is the recommended approach for data ingestion pipelines.

**Authentication Method:** Client Credentials Grant
**Token Endpoint:** `https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token`

#### Required Parameters

The connector stores the following credentials:
- `tenant_id` (string, required): Azure AD tenant ID (GUID or domain name like `contoso.onmicrosoft.com`)
- `client_id` (string, required): Application (client) ID from Azure AD app registration
- `client_secret` (string, required): Client secret value (NOT the secret ID)

The connector automatically exchanges these credentials for an access token at runtime. **It does not run user-facing OAuth flows.**

#### Token Request

**HTTP Method:** POST
**URL:** `https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token`
**Content-Type:** `application/x-www-form-urlencoded`

**Request Body:**
```
client_id={client_id}
&client_secret={client_secret}
&scope=https://graph.microsoft.com/.default
&grant_type=client_credentials
```

**Example Request:**
```http
POST https://login.microsoftonline.com/12345678-1234-1234-1234-123456789012/oauth2/v2.0/token HTTP/1.1
Content-Type: application/x-www-form-urlencoded

client_id=87654321-4321-4321-4321-210987654321
&client_secret=abc123~def456.ghi789
&scope=https://graph.microsoft.com/.default
&grant_type=client_credentials
```

**Example Response:**
```json
{
  "token_type": "Bearer",
  "expires_in": 3599,
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc..."
}
```

#### Using the Access Token

Include the access token in the `Authorization` header for all Microsoft Graph API requests:

```http
GET https://graph.microsoft.com/v1.0/me/joinedTeams HTTP/1.1
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc...
```

#### Required API Permissions (Application-level)

The following **application permissions** (NOT delegated) must be granted with admin consent:

| Resource | Minimum Permission | Higher Privileged Alternatives |
|----------|-------------------|-------------------------------|
| Teams | `Team.ReadBasic.All` | `Group.Read.All`, `Group.ReadWrite.All` |
| Channels | `Channel.ReadBasic.All` | `ChannelMessage.Read.All` |
| Messages | `ChannelMessage.Read.All` | `Group.Read.All`, `Group.ReadWrite.All` |
| Members | `TeamMember.Read.All` | `Member.Read.Hidden` |
| Chats | `Chat.Read.All` | `Chat.ReadWrite.All` |

**Note:** All permissions require **tenant administrator consent**.

#### Token Lifecycle

- **Expiry:** Tokens typically expire after 60-90 minutes (returned in `expires_in`)
- **Refresh:** The connector automatically refreshes tokens before expiry
- **No refresh tokens:** Client credentials flow does not use refresh tokens; simply request a new access token

---

## Object List

### Static Object List

The Microsoft Teams connector provides access to **5 core objects** (tables):

1. **teams** - Microsoft Teams that the application can access
2. **channels** - Channels within teams
3. **messages** - Messages within channels
4. **members** - Members of teams
5. **chats** - 1:1 and group chats

The object list is **static** and predefined by the connector. Unlike some APIs, Microsoft Graph does not provide a dynamic object discovery endpoint for Teams resources.

### Object Hierarchy

Some objects are layered under parent objects:

```
teams (root)
├── channels (child of teams, requires team_id)
│   └── messages (child of channels, requires team_id + channel_id)
└── members (child of teams, requires team_id)

chats (root, independent)
```

**Parent-Child Relationships:**
- `channels` requires `team_id` to list channels in a specific team
- `messages` requires both `team_id` and `channel_id` to list messages in a specific channel
- `members` requires `team_id` to list members of a specific team
- `teams` and `chats` are top-level objects with no required parent IDs

---

## Object Schema

### Schema Retrieval

Microsoft Graph does **not** provide a dedicated API endpoint to retrieve table schemas dynamically. Schemas are **static** and defined in the official documentation. The connector implements these schemas based on the Microsoft Graph API resource definitions.

### Static Schema Definitions

#### 1. teams

| Field | Type | Description | Nullable |
|-------|------|-------------|----------|
| id | String | Unique identifier (GUID) | No |
| displayName | String | Team name (max 256 chars) | No |
| description | String | Team description (max 1024 chars) | Yes |
| classification | String | Data sensitivity label | Yes |
| visibility | String | Visibility type: `public`, `private`, `hiddenMembership` | No |
| webUrl | String | URL to team in Teams client | No |
| isArchived | Boolean | Whether team is in read-only mode | No |
| createdDateTime | DateTimeOffset | ISO 8601 timestamp when created | No |
| internalId | String | Unique ID for audit logs | Yes |
| tenantId | String | Azure AD tenant ID | No |
| specialization | String | Team purpose: `none`, `educationStandard`, `educationClass`, `educationProfessionalLearningCommunity`, `educationStaff`, `healthcareStandard`, `healthcareCareCoordination`, `unknownFutureValue` | Yes |
| memberSettings | Object (JSON) | Settings for member permissions | Yes |
| guestSettings | Object (JSON) | Settings for guest permissions | Yes |
| messagingSettings | Object (JSON) | Messaging configuration | Yes |
| funSettings | Object (JSON) | Fun features config (Giphy, stickers, memes) | Yes |

**Nested Object - memberSettings:**
```json
{
  "allowCreateUpdateChannels": true,
  "allowCreatePrivateChannels": true,
  "allowDeleteChannels": true,
  "allowAddRemoveApps": true,
  "allowCreateUpdateRemoveTabs": true,
  "allowCreateUpdateRemoveConnectors": true
}
```

**Nested Object - guestSettings:**
```json
{
  "allowCreateUpdateChannels": false,
  "allowDeleteChannels": false
}
```

#### 2. channels

| Field | Type | Description | Nullable |
|-------|------|-------------|----------|
| id | String | Unique identifier (GUID) | No |
| team_id | String | Parent team ID (connector-derived) | No |
| displayName | String | Channel name (max 50 chars) | No |
| description | String | Channel description | Yes |
| email | String | Email address for channel | No |
| webUrl | String | URL to channel in Teams client | No |
| membershipType | String | `standard`, `private`, `shared`, `unknownFutureValue` | No |
| createdDateTime | DateTimeOffset | ISO 8601 timestamp | No |
| isFavoriteByDefault | Boolean | Auto-favorite for all members | No |
| isArchived | Boolean | Whether archived | No |
| tenantId | String | Azure AD tenant ID | No |

#### 3. messages

| Field | Type | Description | Nullable |
|-------|------|-------------|----------|
| id | String | Unique message ID | No |
| team_id | String | Parent team ID (connector-derived) | No |
| channel_id | String | Parent channel ID (connector-derived) | No |
| replyToId | String | Parent message ID (for replies) | Yes |
| etag | String | Version number | No |
| messageType | String | `message`, `chatEvent`, `typing`, `systemEventMessage`, `unknownFutureValue` | No |
| createdDateTime | DateTimeOffset | ISO 8601 timestamp | No |
| lastModifiedDateTime | DateTimeOffset | Last modification timestamp (includes edits, reactions) | No |
| lastEditedDateTime | DateTimeOffset | Last edit timestamp | Yes |
| deletedDateTime | DateTimeOffset | Deletion timestamp | Yes |
| subject | String | Subject line (plaintext) | Yes |
| summary | String | Summary for notifications | Yes |
| importance | String | `normal`, `high`, `urgent` | No |
| locale | String | Locale (always `en-us`) | No |
| webUrl | String | URL to message in Teams | No |
| from | Object (IdentitySet) | Sender information | Yes |
| body | Object (ItemBody) | Message content | No |
| attachments | Array[Object] | Attached files, tabs, etc. | Yes |
| mentions | Array[Object] | Mentioned entities | Yes |
| reactions | Array[Object] | Reactions (Like, etc.) | Yes |
| channelIdentity | Object | Channel identity info | Yes |
| policyViolation | Object (JSON) | DLP violation details | Yes |
| eventDetail | Object (JSON) | System event details | Yes |
| messageHistory | Array[Object] | Edit history | Yes |

**Nested Object - from (IdentitySet):**
```json
{
  "user": {
    "id": "8ea0e38b-efb3-4757-924a-5f94061cf8c2",
    "displayName": "Robin Kline",
    "userIdentityType": "aadUser"
  },
  "application": null,
  "device": null
}
```

**Nested Object - body (ItemBody):**
```json
{
  "contentType": "html",
  "content": "<div>Hello world!</div>"
}
```

**Nested Object - attachment:**
```json
{
  "id": "5e32f195-168a-474f-a273-123123123",
  "contentType": "reference",
  "contentUrl": "https://...",
  "content": null,
  "name": "Document.docx",
  "thumbnailUrl": null
}
```

**Nested Object - mention:**
```json
{
  "id": 0,
  "mentionText": "Robin Kline",
  "mentioned": {
    "user": {
      "id": "8ea0e38b-efb3-4757-924a-5f94061cf8c2",
      "displayName": "Robin Kline",
      "userIdentityType": "aadUser"
    }
  }
}
```

**Nested Object - reaction:**
```json
{
  "reactionType": "like",
  "createdDateTime": "2025-01-15T10:30:00.000Z",
  "user": {
    "user": {
      "id": "8ea0e38b-efb3-4757-924a-5f94061cf8c2",
      "displayName": "Robin Kline",
      "userIdentityType": "aadUser"
    }
  }
}
```

#### 4. members

| Field | Type | Description | Nullable |
|-------|------|-------------|----------|
| id | String | Unique membership ID | No |
| team_id | String | Parent team ID (connector-derived) | No |
| roles | Array[String] | Roles: `owner`, `member`, `guest` | Yes |
| displayName | String | User display name | No |
| userId | String | User ID (GUID) | No |
| email | String | Email address | Yes |
| visibleHistoryStartDateTime | DateTimeOffset | When member can start seeing history | Yes |
| tenantId | String | Azure AD tenant ID | No |

#### 5. chats

| Field | Type | Description | Nullable |
|-------|------|-------------|----------|
| id | String | Unique chat ID | No |
| topic | String | Chat subject (for group chats) | Yes |
| createdDateTime | DateTimeOffset | ISO 8601 timestamp | No |
| lastUpdatedDateTime | DateTimeOffset | Last activity timestamp | No |
| chatType | String | `oneOnOne`, `group`, `meeting`, `unknownFutureValue` | No |
| webUrl | String | URL to chat in Teams | Yes |
| tenantId | String | Azure AD tenant ID | No |
| onlineMeetingInfo | Object (JSON) | Associated meeting details | Yes |

---

## Get Object Primary Keys

### Static Primary Key Definitions

Microsoft Graph does **not** provide an API to retrieve primary keys. Primary keys are **static** and defined by the resource type:

| Object | Primary Keys | Notes |
|--------|--------------|-------|
| teams | `["id"]` | Globally unique GUID |
| channels | `["id"]` | Globally unique GUID |
| messages | `["id"]` | Globally unique within chat/channel |
| members | `["id"]` | Unique membership ID |
| chats | `["id"]` | Globally unique GUID |

**Composite Keys:** None of the core objects require composite primary keys. Each has a single unique `id` field.

**Connector-Derived Fields:**
- `channels` includes `team_id` (derived from API call context)
- `messages` includes `team_id` and `channel_id` (derived from API call context)
- `members` includes `team_id` (derived from API call context)

These derived fields are NOT part of the primary key but are useful for joins and filtering.

---

## Object's Ingestion Type

### Ingestion Type Definitions

| Object | Ingestion Type | Reasoning |
|--------|---------------|-----------|
| teams | `snapshot` | Small dataset (<1,000 teams/org), infrequent changes, full refresh is performant |
| channels | `snapshot` | Small dataset (<10,000 channels/org), infrequent changes |
| messages | `cdc` | Large, continuously growing dataset; requires incremental loading; supports `lastModifiedDateTime` cursor for tracking updates, edits, and reactions |
| members | `snapshot` | Relatively small dataset, infrequent changes |
| chats | `cdc` | Continuously growing dataset; supports `lastUpdatedDateTime` cursor |

### CDC (Change Data Capture) Details

For `cdc` objects, the connector uses **timestamp-based filtering** rather than delta tokens due to known stability issues with Teams channel messages delta API.

**Cursor Fields:**
- `messages`: `lastModifiedDateTime` (updated when message is edited, deleted, or receives reactions)
- `chats`: `lastUpdatedDateTime` (updated when chat metadata changes or new messages arrive)

**Handling Deletions:**
- Soft deletes: Messages have `deletedDateTime` field (null if not deleted)
- Hard deletes: Not directly tracked; must rely on full re-sync or change notifications

**Handling Updates:**
- `messages`: Edits update `lastEditedDateTime` and `lastModifiedDateTime`
- Reactions update `lastModifiedDateTime` but not `lastEditedDateTime`
- `messageHistory` array tracks all modifications

### Snapshot Details

For `snapshot` objects, the connector performs a **full refresh** on each sync. This is acceptable because:
- Datasets are small (typically <10,000 records)
- Changes are infrequent
- Full refresh is fast (<30 seconds for typical organizations)

### Append-Only Details

**Not used** for core Teams objects. All data is either snapshot or CDC.

---

## Read API for Data Retrieval

### 1. Teams

**Endpoint:** `GET /me/joinedTeams`
**Description:** List all teams the authenticated app can access (joined by any user in the tenant)

**Request:**
```http
GET https://graph.microsoft.com/v1.0/me/joinedTeams
Authorization: Bearer {access_token}
```

**Query Parameters:**
- `$top` (integer, optional): Page size (default: 20, max: 999)
- `$select` (string, optional): Comma-separated list of properties to return
- `$filter` (string, optional): **NOT SUPPORTED** for teams endpoint

**Response:**
```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#teams",
  "@odata.count": 3,
  "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/joinedTeams?$skiptoken=...",
  "value": [
    {
      "id": "172b0cce-e65d-44ce-9a49-91d9f2e8493a",
      "createdDateTime": "2021-05-27T19:22:25.915Z",
      "displayName": "Contoso Team",
      "description": "This is a sample team",
      "internalId": "19:09fc54a3141a45d0bc769cf506d2e079@thread.skype",
      "classification": null,
      "specialization": "none",
      "visibility": "public",
      "webUrl": "https://teams.microsoft.com/l/team/...",
      "isArchived": false,
      "tenantId": "b33cbe9f-8ebe-4f2a-912b-7e2a427f477f",
      "memberSettings": {
        "allowCreateUpdateChannels": true,
        "allowDeleteChannels": true,
        "allowAddRemoveApps": true,
        "allowCreateUpdateRemoveTabs": true,
        "allowCreateUpdateRemoveConnectors": true
      },
      "guestSettings": {
        "allowCreateUpdateChannels": false,
        "allowDeleteChannels": false
      },
      "messagingSettings": {
        "allowUserEditMessages": true,
        "allowUserDeleteMessages": true,
        "allowOwnerDeleteMessages": true,
        "allowTeamMentions": true,
        "allowChannelMentions": true
      },
      "funSettings": {
        "allowGiphy": true,
        "giphyContentRating": "moderate",
        "allowStickersAndMemes": true,
        "allowCustomMemes": true
      }
    }
  ]
}
```

**Pagination:**
- Use `@odata.nextLink` URL to fetch the next page
- No manual offset calculation needed; follow the `@odata.nextLink` URL directly

**Incremental Loading:** NOT SUPPORTED (snapshot only)

### 2. Channels

**Endpoint:** `GET /teams/{team-id}/channels`
**Description:** List all channels in a specific team

**Required Parent Parameter:** `team_id` (from `teams` table)

**Request:**
```http
GET https://graph.microsoft.com/v1.0/teams/172b0cce-e65d-44ce-9a49-91d9f2e8493a/channels
Authorization: Bearer {access_token}
```

**Query Parameters:**
- `$top` (integer, optional): Page size (default: 20, max: 999)
- `$select` (string, optional): Comma-separated properties
- `$filter` (string, optional): **NOT SUPPORTED**

**Response:**
```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#teams('172b0cce...')/channels",
  "@odata.count": 5,
  "@odata.nextLink": "https://graph.microsoft.com/v1.0/teams/172b0cce.../channels?$skiptoken=...",
  "value": [
    {
      "id": "19:09fc54a3141a45d0bc769cf506d2e079@thread.skype",
      "createdDateTime": "2021-05-27T19:22:25.915Z",
      "displayName": "General",
      "description": "This is the general channel",
      "isFavoriteByDefault": true,
      "email": "Contoso_Team@contoso.onmicrosoft.com",
      "webUrl": "https://teams.microsoft.com/l/channel/...",
      "membershipType": "standard",
      "isArchived": false,
      "tenantId": "b33cbe9f-8ebe-4f2a-912b-7e2a427f477f"
    }
  ]
}
```

**Pagination:** Same as teams (follow `@odata.nextLink`)

**Incremental Loading:** NOT SUPPORTED (snapshot only)

### 3. Messages

**Endpoint:** `GET /teams/{team-id}/channels/{channel-id}/messages`
**Description:** List messages in a specific channel

**Required Parent Parameters:**
- `team_id` (from `teams` table)
- `channel_id` (from `channels` table)

**Request:**
```http
GET https://graph.microsoft.com/v1.0/teams/172b0cce-e65d-44ce-9a49-91d9f2e8493a/channels/19:09fc54a3141a45d0bc769cf506d2e079@thread.skype/messages?$top=50
Authorization: Bearer {access_token}
```

**Query Parameters:**
- `$top` (integer, optional): Page size (default: 20, **max: 50**)
- `$select` (string, optional): Comma-separated properties
- `$expand` (string, optional): `replies` to include nested replies (max 1,000 replies per message)
- `$filter` (string, optional): **NOT DIRECTLY SUPPORTED** on `/messages` endpoint

**IMPORTANT:** The `/messages` endpoint does **NOT** support `$filter` on `lastModifiedDateTime`. Filtering must be done **client-side** after fetching.

**Alternative Endpoint for Filtering:**
`GET /teams/{team-id}/channels/getAllMessages` supports filtering:
```http
GET https://graph.microsoft.com/v1.0/teams/172b0cce-e65d-44ce-9a49-91d9f2e8493a/channels/getAllMessages?$filter=lastModifiedDateTime gt 2025-01-15T00:00:00Z
Authorization: Bearer {access_token}
```

This endpoint retrieves messages across **all channels** in a team with date filtering support.

**Response:**
```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#teams('..')/channels('..')/messages",
  "@odata.count": 15,
  "@odata.nextLink": "https://graph.microsoft.com/v1.0/teams/.../channels/.../messages?$skiptoken=...",
  "value": [
    {
      "id": "1616990032035",
      "replyToId": null,
      "etag": "1616990032035",
      "messageType": "message",
      "createdDateTime": "2021-03-29T03:53:52.035Z",
      "lastModifiedDateTime": "2021-03-29T03:53:52.035Z",
      "lastEditedDateTime": null,
      "deletedDateTime": null,
      "subject": null,
      "summary": null,
      "importance": "normal",
      "locale": "en-us",
      "webUrl": "https://teams.microsoft.com/l/message/...",
      "from": {
        "application": null,
        "device": null,
        "user": {
          "id": "8ea0e38b-efb3-4757-924a-5f94061cf8c2",
          "displayName": "Robin Kline",
          "userIdentityType": "aadUser"
        }
      },
      "body": {
        "contentType": "html",
        "content": "<div>Hello world!</div>"
      },
      "channelIdentity": {
        "teamId": "172b0cce-e65d-44ce-9a49-91d9f2e8493a",
        "channelId": "19:09fc54a3141a45d0bc769cf506d2e079@thread.skype"
      },
      "attachments": [],
      "mentions": [],
      "reactions": []
    }
  ]
}
```

**Pagination:** Follow `@odata.nextLink`

**Incremental Loading (CDC):**
1. **Initial Sync:** Fetch all messages (no filter)
2. **Subsequent Syncs:**
   - Use `/channels/getAllMessages` with `$filter=lastModifiedDateTime gt {cursor_timestamp}`
   - OR fetch from `/messages` and filter client-side
3. **Cursor:** Track `max(lastModifiedDateTime)` from fetched messages
4. **Lookback Window:** Subtract 300 seconds (5 minutes) from max timestamp to catch late updates

**Handling Deleted Messages:**
- Deleted messages have `deletedDateTime` set (ISO 8601 timestamp)
- Soft-deleted messages remain in API response with `deletedDateTime != null`
- Hard-deleted messages are **NOT** returned by the API

**Handling Replies:**
- Use `$expand=replies` to fetch replies inline
- OR fetch replies separately: `GET /teams/{team-id}/channels/{channel-id}/messages/{message-id}/replies`
- Replies have same schema as messages with `replyToId` pointing to parent

### 4. Members

**Endpoint:** `GET /teams/{team-id}/members`
**Description:** List members of a specific team

**Required Parent Parameter:** `team_id` (from `teams` table)

**Request:**
```http
GET https://graph.microsoft.com/v1.0/teams/172b0cce-e65d-44ce-9a49-91d9f2e8493a/members
Authorization: Bearer {access_token}
```

**Query Parameters:**
- `$top` (integer, optional): Page size
- `$select` (string, optional): Comma-separated properties
- `$filter` (string, optional): **LIMITED** support (e.g., `roles/any(r:r eq 'owner')`)

**Response:**
```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#teams('..')/members",
  "@odata.count": 3,
  "value": [
    {
      "@odata.type": "#microsoft.graph.aadUserConversationMember",
      "id": "MCMjMCMjZGNkMjE5ZGQtYmM2OC00YjliLWJmMGItNGEzM2E3OTZiZTM1IyMxOTowOWZjNTRhMzE0MWE0NWQwYmM3NjljZjUwNmQyZTA3OUB0aHJlYWQuc2t5cGUjIzhjZWE1ZWIwLWQ3MmEtNDRlNS05YzE1LTllZGNlZGRhMjRjYg==",
      "roles": ["owner"],
      "displayName": "Robin Kline",
      "userId": "8ea0e38b-efb3-4757-924a-5f94061cf8c2",
      "email": "robin@contoso.com",
      "visibleHistoryStartDateTime": "2021-05-27T19:22:25.915Z",
      "tenantId": "b33cbe9f-8ebe-4f2a-912b-7e2a427f477f"
    }
  ]
}
```

**Pagination:** Follow `@odata.nextLink`

**Incremental Loading:** NOT SUPPORTED (snapshot only)

### 5. Chats

**Endpoint:** `GET /me/chats`
**Description:** List all chats for the authenticated context

**Request:**
```http
GET https://graph.microsoft.com/v1.0/me/chats?$top=50
Authorization: Bearer {access_token}
```

**Query Parameters:**
- `$top` (integer, optional): Page size (default: 20, max: 999)
- `$select` (string, optional): Comma-separated properties
- `$expand` (string, optional): `members` to include chat participants
- `$filter` (string, optional): **NOT SUPPORTED**

**Response:**
```json
{
  "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#chats",
  "@odata.count": 10,
  "@odata.nextLink": "https://graph.microsoft.com/v1.0/me/chats?$skiptoken=...",
  "value": [
    {
      "id": "19:8b081ef6-4792-4def-b2c9-c363a1bf41d5_877192bd-9183-47d3-a74c-8aa0426716cf@unq.gbl.spaces",
      "topic": "Project Discussion",
      "createdDateTime": "2021-04-06T19:49:52.431Z",
      "lastUpdatedDateTime": "2021-04-21T17:13:44.033Z",
      "chatType": "group",
      "webUrl": "https://teams.microsoft.com/l/chat/...",
      "tenantId": "b33cbe9f-8ebe-4f2a-912b-7e2a427f477f",
      "onlineMeetingInfo": null
    }
  ]
}
```

**Pagination:** Follow `@odata.nextLink`

**Incremental Loading (CDC):**
1. **Cursor Field:** `lastUpdatedDateTime`
2. **Filter:** **NOT SUPPORTED** directly; must filter client-side
3. **Strategy:** Fetch all chats, filter by `lastUpdatedDateTime > cursor`

### Comparison: Delta Query vs Timestamp Filtering

| Approach | Endpoint | Pros | Cons | Recommendation |
|----------|----------|------|------|----------------|
| **Delta Query** | `/delta` function | Efficient change tracking, handles deletions | Known HTTP 400 errors for Teams messages; 7-day token expiry | **NOT RECOMMENDED** for messages |
| **Timestamp Filtering** | `getAllMessages?$filter=...` | Stable, simple | Requires client-side filtering for `/messages` endpoint | **RECOMMENDED** for CDC |
| **Polling** | Standard endpoints | Simple | Inefficient, limited to once/day | Only for snapshot |

**Recommended Approach:**
- Use **timestamp-based filtering** with `lastModifiedDateTime` for messages
- Use **client-side filtering** when API filtering not supported
- Implement **lookback window** (300 seconds) to catch late updates

---

## Field Type Mapping

### Microsoft Graph JSON Types to PySpark Types

| Graph API Type | Example Fields | PySpark Type | Notes |
|----------------|----------------|--------------|-------|
| String (GUID) | `id`, `userId`, `teamId` | `StringType()` | UUIDs stored as strings |
| String | `displayName`, `description`, `email` | `StringType()` | UTF-8 text |
| DateTimeOffset | `createdDateTime`, `lastModifiedDateTime` | `StringType()` | ISO 8601 format (e.g., `2021-05-27T19:22:25.915Z`); store as string, parse downstream |
| Boolean | `isArchived`, `isFavoriteByDefault` | `BooleanType()` | `true` / `false` |
| Integer | `id` (in mentions) | `LongType()` | Prefer `LongType` over `IntegerType` |
| Enum | `visibility`, `chatType`, `messageType` | `StringType()` | Store enum values as strings |
| Object | `from`, `body`, `channelIdentity` | `StructType([...])` | Nested struct with defined fields |
| Array[Object] | `attachments`, `mentions`, `reactions` | `ArrayType(StructType([...]))` | Array of structs |
| Array[String] | `roles`, `topics` | `ArrayType(StringType())` | Simple string arrays |
| Complex/Unknown | `policyViolation`, `eventDetail` | `StringType()` | Store as JSON string for flexibility |

### Special Field Behaviors

**Auto-generated Fields:**
- `id`: System-generated GUIDs (read-only)
- `createdDateTime`: Auto-set on creation (read-only)
- `internalId`: System-generated for audit logs

**Enumerations:**
- `visibility`: `public`, `private`, `hiddenMembership`
- `chatType`: `oneOnOne`, `group`, `meeting`, `unknownFutureValue`
- `messageType`: `message`, `chatEvent`, `typing`, `systemEventMessage`, `unknownFutureValue`
- `importance`: `normal`, `high`, `urgent`

**Nullable Fields:**
- Most fields are nullable unless marked as required in schema
- Absent nested objects should be `null` (not `{}`)
- Empty arrays are `[]` (not `null`)

**Timestamp Format:**
- All timestamps are ISO 8601 with milliseconds: `2021-05-27T19:22:25.915Z`
- Timezone is always UTC (indicated by `Z`)
- Store as `StringType()` to preserve format; parse to `TimestampType()` downstream if needed

**Nested Objects vs JSON Strings:**
- **Structured nested objects** (from, body, attachments): Use `StructType`
- **Complex/polymorphic objects** (policyViolation, eventDetail): Use `StringType()` to store JSON
- **Settings objects** (memberSettings, guestSettings): Use `StringType()` to store JSON for flexibility

---

## Write API

### Not Applicable for Data Ingestion

The Microsoft Teams connector is **read-only** and does **NOT** support writing data back to Microsoft Teams. The connector is designed for:
- Data extraction for analytics
- Compliance and archival
- Reporting and insights

**Write Operations (Not Implemented):**
- Creating teams, channels, or messages
- Updating team settings or channel properties
- Deleting messages or teams

**Validation:**
- Data ingested by the connector can be validated by:
  1. Reading the same data via the Microsoft Graph API directly
  2. Comparing record counts and field values
  3. Checking timestamps and IDs

**Use Case:**
If you need to write data back to Teams, use the Microsoft Graph API directly with appropriate POST/PATCH/DELETE endpoints. This connector focuses exclusively on data ingestion.

---

## Rate Limiting & Best Practices

### Microsoft Graph API Throttling

**Throttle Limits:**
- **General:** ~1,000 - 2,000 requests per minute per application
- **Specific:** Varies by resource and tenant size
- **429 Response:** Returns `Retry-After` header (seconds to wait)

**Throttling Indicators:**
- HTTP Status Code: `429 Too Many Requests`
- Header: `Retry-After: {seconds}`

**Connector Strategy:**
1. **Sleep Between Requests:** 100ms delay (10 requests/second)
2. **Exponential Backoff:** On 429 or 5xx errors, retry with delays: 2s, 4s, 8s
3. **Respect Retry-After:** Use header value when provided
4. **Max Retries:** 3 attempts before failing

**Example Retry Logic:**
```python
import time
import requests

def make_request_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            time.sleep(retry_after)
            continue
        elif response.status_code in [500, 502, 503]:
            time.sleep(2 ** attempt)  # Exponential backoff
            continue
        else:
            raise Exception(f"API Error: {response.status_code}")

    raise Exception("Max retries exceeded")
```

### Pagination Best Practices

**Page Size:**
- Default: 20 items
- Recommended: 50 items (balance between efficiency and response size)
- Max: 50 for `/messages`, 999 for others

**Pagination Pattern:**
```python
url = "https://graph.microsoft.com/v1.0/me/joinedTeams?$top=50"
all_teams = []

while url:
    response = requests.get(url, headers=headers)
    data = response.json()
    all_teams.extend(data.get('value', []))
    url = data.get('@odata.nextLink')  # None when no more pages
```

### Incremental Sync Strategy

**For CDC Tables (messages, chats):**

1. **Initial Sync:**
   - Fetch all data (no filter)
   - Track `max(lastModifiedDateTime)` as cursor
   - Store cursor: `{"cursor": "2025-01-15T10:30:00.000Z"}`

2. **Subsequent Syncs:**
   - Apply lookback window: `cursor_with_lookback = cursor - 300 seconds`
   - Filter: `lastModifiedDateTime > cursor_with_lookback`
   - Fetch new/updated records
   - Update cursor: `max(lastModifiedDateTime)` from new records

3. **Lookback Window Rationale:**
   - Handles late updates (reactions added after message created)
   - Catches edits with eventual consistency delays
   - Recommended: 300 seconds (5 minutes)

**Example:**
```python
from datetime import datetime, timedelta

cursor = "2025-01-15T10:30:00.000Z"
lookback_seconds = 300

dt = datetime.fromisoformat(cursor.replace('Z', '+00:00'))
dt_with_lookback = dt - timedelta(seconds=lookback_seconds)
filter_timestamp = dt_with_lookback.isoformat().replace('+00:00', 'Z')
# filter_timestamp = "2025-01-15T10:25:00.000Z"

# Use in API call (client-side filter if API doesn't support $filter)
for message in messages:
    if message['lastModifiedDateTime'] > filter_timestamp:
        # Process message
```

### Batch Size Recommendations

**Per-Sync Limits:**
- `max_pages_per_batch`: 100 pages (configurable)
- Prevents runaway syncs for large datasets
- Resume from cursor in next sync

**Reasoning:**
- Large organizations may have millions of messages
- Limiting batch size ensures predictable runtime
- Cursor-based sync ensures no data loss

---

## Known Quirks & Edge Cases

### 1. Delta Query Instability for Messages

**Issue:** The `/delta` endpoint for Teams channel messages returns HTTP 400 errors inconsistently.

**Microsoft Acknowledgment:** Known issue documented in Microsoft Q&A forums (see Sources).

**Mitigation:**
- Do NOT use delta query for messages
- Use timestamp-based filtering with `lastModifiedDateTime`
- Implement lookback window to catch missed updates

### 2. Polling Limitations

**Issue:** Microsoft Graph API limits polling to **once per day** for the same resource.

**Impact:**
- Cannot poll `/messages` endpoint more than once per day
- Exceeding limit may result in throttling or API suspension

**Mitigation:**
- Use webhooks/change notifications for real-time updates
- Use delta query for other resources (NOT messages)
- For messages, rely on scheduled daily/hourly syncs with CDC

### 3. Message Filtering Not Supported

**Issue:** The `/messages` endpoint does NOT support `$filter` on `lastModifiedDateTime`.

**Workaround:**
- Use `/channels/getAllMessages` with `$filter` (fetches across all channels)
- OR fetch from `/messages` and filter client-side

**Trade-off:**
- `/getAllMessages`: More efficient (server-side filter) but requires `ChannelMessage.Read.All` permission
- Client-side filter: Works with basic permissions but fetches more data

### 4. Nested Replies Limitation

**Issue:** When using `$expand=replies`, max 1,000 replies returned per message.

**Mitigation:**
- For messages with >1,000 replies, fetch replies separately: `/messages/{id}/replies`
- Implement pagination for replies

### 5. Permissions Require Admin Consent

**Issue:** All application permissions require tenant administrator consent.

**Impact:**
- Cannot use connector without admin approval
- Increases setup complexity for users

**Mitigation:**
- Provide clear documentation on Azure AD app registration
- Include screenshots and step-by-step guide
- Test with minimal permission set

### 6. Token Expiry

**Issue:** Access tokens expire after 60-90 minutes.

**Mitigation:**
- Implement automatic token refresh
- Cache token with expiry tracking
- Refresh 5 minutes before expiry

### 7. Soft-Deleted vs Hard-Deleted Messages

**Issue:**
- Soft-deleted messages have `deletedDateTime` set and remain in API response
- Hard-deleted messages are removed entirely from API (no tombstone)

**Mitigation:**
- Track `deletedDateTime` field to identify soft deletes
- For hard deletes, rely on full re-sync or change notifications

### 8. Message Edits and Reactions

**Issue:**
- Edits update both `lastEditedDateTime` and `lastModifiedDateTime`
- Reactions update ONLY `lastModifiedDateTime` (not `lastEditedDateTime`)

**Implication:**
- Must use `lastModifiedDateTime` as cursor (not `lastEditedDateTime`)
- `messageHistory` array tracks all changes (edits + reactions)

---

## Sources and References

### Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it Confirmed |
|-------------|-----|----------------|------------|-------------------|
| **Official Docs** | [Microsoft Graph Teams API Overview](https://learn.microsoft.com/en-us/graph/api/resources/teams-api-overview?view=graph-rest-1.0) | 2025-12-17 | High | API endpoints, object hierarchy, authentication requirements |
| **Official Docs** | [List Channel Messages](https://learn.microsoft.com/en-us/graph/api/channel-list-messages?view=graph-rest-1.0) | 2025-12-17 | High | Messages endpoint, pagination, response schema, $top limit (50) |
| **Official Docs** | [Delta Query Overview](https://learn.microsoft.com/en-us/graph/delta-query-overview) | 2025-12-17 | High | Delta query mechanics, token expiration (7 days), deletion handling |
| **Official Docs** | [Team Resource](https://learn.microsoft.com/en-us/graph/api/resources/team?view=graph-rest-1.0) | 2025-12-17 | High | Team schema, properties, nested objects |
| **Official Docs** | [Channel Resource](https://learn.microsoft.com/en-us/graph/api/resources/channel?view=graph-rest-1.0) | 2025-12-17 | High | Channel schema, properties, membership types |
| **Official Docs** | [ChatMessage Resource](https://learn.microsoft.com/en-us/graph/api/resources/chatmessage?view=graph-rest-1.0) | 2025-12-17 | High | Message schema, nested objects (from, body, attachments, mentions, reactions) |
| **Official Docs** | [OAuth 2.0 Client Credentials](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow) | 2025-12-17 | High | Authentication flow, token request/response format |
| **Microsoft Q&A** | [Teams Channel Messages Delta API HTTP 400](https://learn.microsoft.com/en-us/answers/questions/5583292/teams-channel-messages-delta-api-returns-http-400) | 2025-12-17 | High | Known delta query issues for Teams messages |
| **Official Docs** | [Graph API Rate Limiting](https://learn.microsoft.com/en-us/graph/throttling) | 2025-12-17 | High | Throttle limits (~1000-2000 req/min), Retry-After header |
| **Official Docs** | [Get All Messages (getAllMessages)](https://learn.microsoft.com/en-us/graph/api/channel-getallmessages?view=graph-rest-1.0) | 2025-12-17 | High | Alternative endpoint supporting $filter on lastModifiedDateTime |

### Confidence Levels

- **High Confidence:** Official Microsoft documentation, API reference, known issues acknowledged by Microsoft
- **Medium Confidence:** Community implementations (Airbyte, Singer) - NOT referenced for this documentation
- **Low Confidence:** Blog posts, unofficial sources - NOT used for this documentation

### Prioritization

All information in this document is sourced from **official Microsoft documentation** (highest confidence). No third-party implementations were referenced to ensure accuracy and alignment with the latest API version (v1.0, as of December 2025).

### Conflicts Resolved

**None.** All information is consistent across official Microsoft Graph API documentation.

---

## Document Metadata

- **API Version:** Microsoft Graph v1.0
- **Document Created:** 2025-12-17
- **Last Updated:** 2025-12-17
- **Connector:** Microsoft Teams Community Connector for Lakeflow
- **Author:** Lakeflow Community Connector Team
- **Review Status:** Production-ready, based on official API documentation

