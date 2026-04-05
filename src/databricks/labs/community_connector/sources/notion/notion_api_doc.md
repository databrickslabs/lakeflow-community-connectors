# Notion API Documentation

## Authorization

**Preferred method: Internal Integration Token (Bearer Token)**

Notion integrations authenticate via a bearer token passed in the `Authorization` header. All requests also require the `Notion-Version` header.

### Required Headers

| Header | Value | Notes |
|--------|-------|-------|
| `Authorization` | `Bearer <NOTION_INTEGRATION_TOKEN>` | Required for every request |
| `Notion-Version` | `2026-03-11` | Required for every request; use the latest stable version |
| `Content-Type` | `application/json` | Required for POST/PATCH requests |

### How to Obtain a Token

1. Go to [https://www.notion.so/my-integrations](https://www.notion.so/my-integrations)
2. Click **New integration**
3. Set a name and select the target workspace
4. Under **Capabilities**, enable the required permissions:
   - **Read content** — required for all read operations
   - **Read user information** — required for the `users` stream
   - **Read comments** — required for the `comments` stream
5. Copy the **Internal Integration Token** from the **Secrets** section
6. Share the relevant Notion pages/databases with the integration (pages are not automatically accessible — each must be explicitly shared)

### Example cURL Request

```bash
curl 'https://api.notion.com/v1/users' \
  -H 'Authorization: Bearer '"$NOTION_INTEGRATION_TOKEN"'' \
  -H 'Notion-Version: 2026-03-11'
```

### Alternative: OAuth (Public Integrations)

For integrations serving multiple workspaces, Notion supports OAuth 2.0. The connector stores `client_id`, `client_secret`, and `refresh_token` and exchanges them for an access token at runtime. This guide focuses on internal integration tokens, which are simpler and sufficient for single-workspace connectors. See [https://developers.notion.com/docs/authorization](https://developers.notion.com/docs/authorization) for the OAuth flow.

---

## Object List

Notion exposes the following primary object types accessible via the REST API:

| Object | Description | List Method |
|--------|-------------|-------------|
| `databases` | Database containers (metadata + schema) | `POST /v1/search` with `filter.value = "data_source"` |
| `pages` | Pages and database row entries | `POST /v1/search` with `filter.value = "page"` |
| `blocks` | Content blocks within pages (nested) | `GET /v1/blocks/{block_id}/children` (recursive) |
| `users` | Workspace members (people and bots) | `GET /v1/users` |
| `comments` | Comments on pages and blocks | `GET /v1/comments?block_id={id}` |

**Important API version note**: As of `2025-09-03`, Notion introduced **multi-source databases**. The `/v1/data_sources` namespace now handles schema/query operations previously done via `/v1/databases/{id}/query`. The `GET /v1/databases` list endpoint was deprecated in `2022-02-22`. This document uses `2026-03-11` as the target version and covers the stable endpoints for each object.

**Enumeration approach**: The object list is not static — it must be discovered at runtime using the Search API (`POST /v1/search`). There is no single "list all objects" endpoint; databases and pages are enumerated via search, blocks require recursive traversal from pages, and users/comments have dedicated list endpoints.

---

## Object Schema

### Databases (`/v1/databases/{database_id}`)

**Endpoint**: `GET /v1/databases/{database_id}`

Returns the database object with its schema (property definitions) and metadata.

#### Database Object Fields

| Field | Type | Description |
|-------|------|-------------|
| `object` | string | Always `"database"` |
| `id` | string (UUID) | Unique identifier |
| `created_time` | ISO 8601 datetime | Creation timestamp |
| `created_by` | Partial User object | User who created the database |
| `last_edited_time` | ISO 8601 datetime | Last modification timestamp |
| `last_edited_by` | Partial User object | User who last edited the database |
| `title` | Rich text array | Database name as displayed in Notion |
| `description` | Rich text array | Database description |
| `icon` | Emoji / Icon / Custom emoji / File | Database icon |
| `cover` | File object | Cover image |
| `parent` | object | Parent container (page_id, block_id, workspace, or database_id) |
| `url` | string | Notion URL to the database |
| `archived` | boolean | Deprecated; use `in_trash` instead |
| `in_trash` | boolean | Whether the database is in trash |
| `is_inline` | boolean | `true` if database appears inline within a page |
| `public_url` | string or null | Public web URL if published |
| `data_sources` | array | List of child data sources (each with `id` and `name`); introduced in v2025-09-03 |
| `properties` | object | Map of property name → property schema (see Property Types below) |

#### Database Property Types (Schema Fields)

Each key in `properties` maps to a property schema object with `id`, `name`, `type`, and type-specific configuration:

| Property Type | Description | Config Fields |
|---------------|-------------|---------------|
| `title` | Page title (required, exactly one per DB) | Empty object |
| `rich_text` | Multi-line text | Empty object |
| `number` | Numeric values | `format` (e.g., `"number"`, `"dollar"`, `"percent"`) |
| `select` | Single choice | `options[]`: `{id, name, color}` |
| `multi_select` | Multiple choices | `options[]`: `{id, name, color}` |
| `status` | Grouped options (e.g., Not started / In progress / Done) | `options[]`, `groups[]` |
| `date` | Date or date range | Empty object |
| `people` | User mentions | Empty object |
| `files` | File attachments or external URLs | Empty object |
| `checkbox` | Boolean | Empty object |
| `url` | Web address | Empty object |
| `email` | Email address | Empty object |
| `phone_number` | Phone number | Empty object |
| `formula` | Computed value | `expression` (formula string) |
| `relation` | Link to another database | `data_source_id`, `type` (`"single_property"` or `"dual_property"`) |
| `rollup` | Aggregate from related database | `relation_property_name`, `rollup_property_name`, `function` |
| `created_time` | Auto row creation timestamp | Read-only |
| `created_by` | Auto row creator | Read-only |
| `last_edited_time` | Auto last-edit timestamp | Read-only |
| `last_edited_by` | Auto last-editor | Read-only |
| `unique_id` | Auto-increment ID with optional prefix | `prefix` (string or null) |
| `place` | Location value (Map view) | Not fully supported via API |

#### Example Response (abbreviated)

```json
{
  "object": "database",
  "id": "d9824bdc-8445-4327-be8b-5b47500af6ce",
  "created_time": "2021-07-16T23:52:00.000Z",
  "last_edited_time": "2021-07-16T23:52:00.000Z",
  "title": [{"type": "text", "text": {"content": "Tasks"}, "plain_text": "Tasks"}],
  "description": [],
  "parent": {"type": "page_id", "page_id": "abc123"},
  "url": "https://www.notion.so/d9824bdc...",
  "in_trash": false,
  "is_inline": false,
  "properties": {
    "Name": {"id": "title", "type": "title", "title": {}},
    "Status": {
      "id": "abc",
      "type": "status",
      "status": {
        "options": [{"id": "1", "name": "Not started", "color": "default"}],
        "groups": [{"id": "g1", "name": "To-do", "color": "gray", "option_ids": ["1"]}]
      }
    },
    "Due Date": {"id": "def", "type": "date", "date": {}}
  }
}
```

---

### Pages (`/v1/pages/{page_id}`)

**Endpoint**: `GET /v1/pages/{page_id}`

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `filter_properties` | array of property IDs | No | Limit returned properties (max 100) |

#### Page Object Fields

| Field | Type | Description |
|-------|------|-------------|
| `object` | string | Always `"page"` |
| `id` | string (UUIDv4) | Unique identifier |
| `created_time` | ISO 8601 datetime | Page creation timestamp |
| `last_edited_time` | ISO 8601 datetime | Last modification timestamp |
| `created_by` | Partial User object | Creator |
| `last_edited_by` | Partial User object | Last editor |
| `in_trash` | boolean | Whether the page is in trash |
| `is_archived` | boolean | Archived status |
| `is_locked` | boolean | Whether the page is locked for editing |
| `icon` | Emoji / Icon / File / null | Page icon |
| `cover` | File / External URL / null | Cover image |
| `properties` | object | Property values conforming to the parent database schema; for standalone pages, only `title` is valid |
| `parent` | object | Parent container: `database_id`, `page_id`, `block_id`, `workspace: true`, or `data_source_id` |
| `url` | string | Notion URL |
| `public_url` | string or null | Public web URL if published |

**Note**: Page *content* (body text) is not returned in the page object — it must be fetched separately via `GET /v1/blocks/{page_id}/children`.

**Note**: The properties endpoint will not accurately return properties exceeding 25 references. Use `GET /v1/pages/{page_id}/properties/{property_id}` for large property datasets.

#### Example Response (abbreviated)

```json
{
  "object": "page",
  "id": "f4f7c6a0-7e3b-4d2a-9e1f-123456789abc",
  "created_time": "2022-03-01T19:05:00.000Z",
  "last_edited_time": "2022-06-28T15:48:00.000Z",
  "in_trash": false,
  "is_archived": false,
  "is_locked": false,
  "parent": {"type": "database_id", "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce"},
  "url": "https://www.notion.so/f4f7c6a0...",
  "public_url": null,
  "properties": {
    "Name": {
      "id": "title",
      "type": "title",
      "title": [{"plain_text": "Fix login bug", "type": "text"}]
    },
    "Status": {
      "id": "abc",
      "type": "status",
      "status": {"id": "1", "name": "In progress", "color": "blue"}
    }
  }
}
```

---

### Blocks (`/v1/blocks/{block_id}/children`)

**Endpoint**: `GET /v1/blocks/{block_id}/children`

Blocks are the atomic content units within pages. Retrieving all blocks for a page requires starting with `GET /v1/blocks/{page_id}/children` and recursively fetching children for any block where `has_children: true`.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `block_id` | string (UUID) | Yes (path) | ID of parent block or page |
| `start_cursor` | string (UUID) | No | Pagination cursor |
| `page_size` | number | No | Results per page (default: 100, max: 100) |

#### Block Object Fields (All Types)

| Field | Type | Description |
|-------|------|-------------|
| `object` | string | Always `"block"` |
| `id` | string (UUIDv4) | Unique block identifier |
| `parent` | object | Parent reference (`page_id`, `block_id`, `database_id`, or `workspace`) |
| `type` | string (enum) | Block type (see block types table below) |
| `created_time` | ISO 8601 datetime | Creation timestamp |
| `created_by` | Partial User object | Creator |
| `last_edited_time` | ISO 8601 datetime | Last modification timestamp |
| `last_edited_by` | Partial User object | Last editor |
| `has_children` | boolean | Whether the block has nested children |
| `in_trash` | boolean | Whether the block is in trash |
| `{type}` | object | Type-specific content (keyed by the block's `type` value) |

#### Supported Block Types (35+)

| Category | Block Types |
|----------|-------------|
| Text | `paragraph`, `heading_1`, `heading_2`, `heading_3`, `heading_4`, `quote`, `callout`, `code` |
| Lists | `bulleted_list_item`, `numbered_list_item`, `to_do` |
| Toggle | `toggle` |
| Media | `image`, `audio`, `video`, `file`, `pdf`, `bookmark`, `embed`, `link_preview` |
| Structural | `column_list`, `column`, `table`, `table_row`, `divider`, `breadcrumb` |
| Database refs | `child_database`, `child_page` |
| Special | `table_of_contents`, `equation`, `synced_block`, `template`, `tab`, `meeting_notes` |
| Unsupported | `unsupported` (Notion blocks not yet in API) |

**Note**: Airbyte excludes `child_page`, `child_database`, and `ai_block` types from its blocks stream, as these are navigational/structural references rather than content.

#### Type-specific Content Structure

Most content blocks share this structure inside their type object:

```json
{
  "type": "paragraph",
  "paragraph": {
    "rich_text": [
      {
        "type": "text",
        "text": {"content": "Hello, world!", "link": null},
        "annotations": {
          "bold": false, "italic": false, "strikethrough": false,
          "underline": false, "code": false, "color": "default"
        },
        "plain_text": "Hello, world!",
        "href": null
      }
    ],
    "color": "default"
  }
}
```

#### Example Response

```json
{
  "object": "list",
  "results": [
    {
      "object": "block",
      "id": "9bc30ad4-9373-46a5-84ab-0a7845ee3f7e",
      "parent": {"type": "page_id", "page_id": "f4f7c6a0-7e3b-4d2a-9e1f-123456789abc"},
      "type": "paragraph",
      "created_time": "2022-03-01T19:05:00.000Z",
      "last_edited_time": "2022-06-28T15:48:00.000Z",
      "has_children": false,
      "in_trash": false,
      "paragraph": {
        "rich_text": [{"type": "text", "text": {"content": "Welcome!"}, "plain_text": "Welcome!"}],
        "color": "default"
      }
    }
  ],
  "next_cursor": null,
  "has_more": false
}
```

---

### Users (`/v1/users`)

**Endpoint**: `GET /v1/users`

Lists all users (people and bots) in the workspace. Requires the integration to have the **Read user information** capability enabled.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_cursor` | string | No | Pagination cursor |
| `page_size` | number | No | Results per page (default: 100, max: 100) |

#### User Object Fields

| Field | Type | Description |
|-------|------|-------------|
| `object` | string | Always `"user"` |
| `id` | string (UUID) | Unique identifier |
| `type` | string | `"person"` or `"bot"` |
| `name` | string or null | Display name in Notion |
| `avatar_url` | string or null | Avatar image URL |
| `person` | object | Present when `type = "person"` |
| `person.email` | string (optional) | Email address; only available if integration has email access capability |
| `bot` | object | Present when `type = "bot"` |
| `bot.owner` | object | Ownership info (`type`: `"workspace"` or `"user"`) |
| `bot.owner.workspace_name` | string | Workspace name if workspace-owned |
| `bot.workspace_id` | string | Bot's workspace ID |

#### Example Response

```json
{
  "object": "list",
  "results": [
    {
      "object": "user",
      "id": "d40e767c-d7af-4b18-a86d-55c61f1e39a4",
      "type": "person",
      "name": "Alice Johnson",
      "avatar_url": "https://secure.notion-static.com/...",
      "person": {"email": "alice@example.com"}
    },
    {
      "object": "user",
      "id": "9188c6a5-7381-452f-b3d9-c6969af1db62",
      "type": "bot",
      "name": "My Connector Bot",
      "avatar_url": null,
      "bot": {
        "owner": {"type": "workspace"},
        "workspace_name": "My Workspace"
      }
    }
  ],
  "next_cursor": null,
  "has_more": false
}
```

---

### Comments (`/v1/comments`)

**Endpoint**: `GET /v1/comments`

Lists all open (unresolved) comments for a specific page or block. Requires the integration to have the **Read comments** capability enabled.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `block_id` | string (UUID) | Yes | ID of the page or block to retrieve comments from. Pages are technically blocks — use the page ID here for page-level comments. |
| `start_cursor` | string | No | Pagination cursor |
| `page_size` | number | No | Results per page (default: 100, max: 100) |

**Important**: Inline discussion thread comments are associated with their specific block IDs, not the parent page ID. Passing a page ID only returns page-level comments, not inline block comments. To retrieve all comments on a page including inline discussions, you must iterate over all blocks and call `GET /v1/comments?block_id={block_id}` for each block.

#### Comment Object Fields

| Field | Type | Description |
|-------|------|-------------|
| `object` | string | Always `"comment"` |
| `id` | string (UUIDv4) | Unique comment identifier |
| `parent` | object | Parent page or block reference |
| `discussion_id` | string (UUIDv4) | Unique identifier of the discussion thread |
| `created_time` | ISO 8601 datetime | Timestamp when comment was created |
| `last_edited_time` | ISO 8601 datetime | Timestamp of most recent update |
| `created_by` | Partial User object | User who authored the comment |
| `rich_text` | Rich text array | Comment content with formatting, links, and mentions |
| `attachments` | Comment Attachment array | File attachments included with the comment |
| `display_name` | Comment Display Name object | Custom display name for the comment |

#### Limitations

- Only **unresolved** (open) comments are returned; resolved comments cannot be retrieved via the API
- Integrations cannot start new discussion threads, only reply to existing ones
- Comments from multiple discussion threads are returned in ascending chronological order

#### Example Response

```json
{
  "object": "list",
  "results": [
    {
      "object": "comment",
      "id": "5c945d0a-6a3d-4f7e-8b2c-a1b2c3d4e5f6",
      "parent": {"type": "page_id", "page_id": "f4f7c6a0-7e3b-4d2a-9e1f-123456789abc"},
      "discussion_id": "aabbccdd-1234-5678-abcd-efgh12345678",
      "created_time": "2022-07-15T21:17:00.000Z",
      "last_edited_time": "2022-07-15T21:17:00.000Z",
      "created_by": {"object": "user", "id": "d40e767c-d7af-4b18-a86d-55c61f1e39a4"},
      "rich_text": [
        {
          "type": "text",
          "text": {"content": "Great work on this section!", "link": null},
          "plain_text": "Great work on this section!",
          "annotations": {"bold": false, "italic": false, "color": "default"}
        }
      ],
      "attachments": []
    }
  ],
  "next_cursor": null,
  "has_more": false
}
```

---

## Get Object Primary Keys

| Object | Primary Key Field | Type | Notes |
|--------|-------------------|------|-------|
| `databases` | `id` | string (UUID) | Unique across all Notion objects |
| `pages` | `id` | string (UUID) | Unique across all Notion objects |
| `blocks` | `id` | string (UUID) | Unique across all Notion objects |
| `users` | `id` | string (UUID) | Unique across all Notion objects |
| `comments` | `id` | string (UUID) | Unique across all Notion objects |

All Notion objects share a universal UUID-based identifier (`id`). The `parent` field provides the hierarchical relationship context. For the `comments` table, `discussion_id` is a useful secondary key for grouping comments into threads.

---

## Object Ingestion Type

| Object | Ingestion Type | Cursor Field | Notes |
|--------|----------------|--------------|-------|
| `databases` | `cdc` | `last_edited_time` | Search API supports `sort.timestamp = "last_edited_time"`. No API for deleted databases; use `in_trash` field to detect soft-deletes. |
| `pages` | `cdc` | `last_edited_time` | Search API supports `sort.timestamp = "last_edited_time"`. Pages in trash have `in_trash: true`. |
| `blocks` | `cdc` | `last_edited_time` | Each block has `last_edited_time`; however, listing all modified blocks requires re-traversing from modified pages (no global block change feed). |
| `users` | `snapshot` | N/A | No `last_edited_time` on users; no incremental endpoint. Full refresh only. |
| `comments` | `cdc` | `created_time` | `last_edited_time` is available, but comments require per-page/per-block polling; no global comment stream. |

**Classification rationale**:
- `databases`, `pages`: Classified as `cdc` because `last_edited_time` allows filtering for changed records. `in_trash` represents soft-delete state (no separate delete feed, so this is not `cdc_with_deletes`).
- `blocks`: Classified as `cdc` with the caveat that incremental block discovery requires traversal from modified pages. The `last_edited_time` field on blocks enables change detection once a block is identified.
- `users`: Classified as `snapshot` — the `/v1/users` endpoint provides no timestamp or change cursor.
- `comments`: Classified as `cdc` since `created_time` and `last_edited_time` exist per comment, but discovery requires polling per page/block ID.

---

## Read API for Data Retrieval

### Base URL

```
https://api.notion.com
```

HTTPS is required for all requests.

### API Version Header

```
Notion-Version: 2026-03-11
```

This header is required on every request. The current latest stable version is `2026-03-11`.

---

### 1. Databases — Read API

**List all shared databases** (via Search):

```http
POST /v1/search
Content-Type: application/json
Authorization: Bearer {token}
Notion-Version: 2026-03-11

{
  "filter": {"property": "object", "value": "data_source"},
  "sort": {"timestamp": "last_edited_time", "direction": "ascending"},
  "start_cursor": "{cursor}",
  "page_size": 100
}
```

**Note**: As of API v2025-09-03, databases use the `"data_source"` filter value in Search (not `"database"`). For older API versions (pre-2025-09-03), use `"database"`.

**Retrieve a single database** (schema + metadata):

```http
GET /v1/databases/{database_id}
Authorization: Bearer {token}
Notion-Version: 2026-03-11
```

**Incremental strategy for databases**:
- Sort search results by `last_edited_time ascending`
- Track the `last_edited_time` of the last processed database as the cursor state
- On next sync, re-run search sorted by `last_edited_time ascending` and process all records with `last_edited_time > {cursor}`
- Since Search may not immediately index newly created databases, include a small lookback window (e.g., subtract 1 minute from cursor)

---

### 2. Pages — Read API

**List all shared pages** (via Search):

```http
POST /v1/search
Content-Type: application/json
Authorization: Bearer {token}
Notion-Version: 2026-03-11

{
  "filter": {"property": "object", "value": "page"},
  "sort": {"timestamp": "last_edited_time", "direction": "ascending"},
  "start_cursor": "{cursor}",
  "page_size": 100
}
```

**Query pages within a specific database** (with timestamp filter for incremental):

```http
POST /v1/databases/{database_id}/query
Content-Type: application/json
Authorization: Bearer {token}
Notion-Version: 2026-03-11

{
  "filter": {
    "timestamp": "last_edited_time",
    "last_edited_time": {"on_or_after": "2024-01-01T00:00:00.000Z"}
  },
  "sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}],
  "start_cursor": "{cursor}",
  "page_size": 100
}
```

**Note**: `POST /v1/databases/{id}/query` is deprecated as of v2025-09-03. Use `POST /v1/data_sources/{data_source_id}/query` for newer API versions.

**Retrieve a single page**:

```http
GET /v1/pages/{page_id}
Authorization: Bearer {token}
Notion-Version: 2026-03-11
```

**Incremental strategy for pages**:
- Use Search API with `sort.timestamp = "last_edited_time"` and `direction = "ascending"`
- Store max `last_edited_time` seen as cursor state
- On next sync, filter with `last_edited_time.on_or_after = {cursor}` (when querying a specific database) or re-sort and skip previously seen records (when using Search)
- Pages in trash have `in_trash: true` — handle as soft deletes in downstream logic

---

### 3. Blocks — Read API

Blocks must be fetched recursively starting from each page. There is no endpoint to list all blocks globally.

**Retrieve children of a block or page**:

```http
GET /v1/blocks/{block_id}/children?page_size=100&start_cursor={cursor}
Authorization: Bearer {token}
Notion-Version: 2026-03-11
```

**Recursive traversal algorithm**:

```python
def fetch_all_blocks(block_id: str, notion_client) -> list[dict]:
    """Recursively fetch all blocks for a given page or block ID."""
    blocks = []
    cursor = None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        response = notion_client.get(f"/v1/blocks/{block_id}/children", params=params)
        page_blocks = response["results"]
        for block in page_blocks:
            blocks.append(block)
            if block.get("has_children") and block["type"] not in ("child_page", "child_database"):
                blocks.extend(fetch_all_blocks(block["id"], notion_client))
        if not response["has_more"]:
            break
        cursor = response["next_cursor"]
    return blocks
```

**Incremental strategy for blocks**:
- Blocks do not have a global change feed
- To detect changed blocks, first identify pages with `last_edited_time > {cursor}` via Search
- For each changed page, re-fetch all blocks and emit records with `last_edited_time > {cursor}`
- This approach can be expensive for workspaces with many large pages — consider full refresh for blocks if page volume is manageable
- Recursion depth: Notion supports up to 30 levels of nesting; the API does not automatically enforce a recursion limit — implement depth tracking in code

---

### 4. Users — Read API

**List all workspace users**:

```http
GET /v1/users?page_size=100&start_cursor={cursor}
Authorization: Bearer {token}
Notion-Version: 2026-03-11
```

**Retrieve a specific user**:

```http
GET /v1/users/{user_id}
Authorization: Bearer {token}
Notion-Version: 2026-03-11
```

**Incremental strategy for users**:
- Not supported — users have no `last_edited_time`
- Use full snapshot refresh on each sync run
- User IDs are stable UUIDs; downstream MERGE/UPSERT on `id` will handle re-ingested rows

---

### 5. Comments — Read API

**Retrieve comments for a page or block**:

```http
GET /v1/comments?block_id={page_or_block_id}&page_size=100&start_cursor={cursor}
Authorization: Bearer {token}
Notion-Version: 2026-03-11
```

**Incremental strategy for comments**:
- No global comment stream exists
- Must iterate over all synced pages and call `GET /v1/comments?block_id={page_id}` per page
- For inline comments, also iterate over all blocks and call `GET /v1/comments?block_id={block_id}`
- Filter results by `created_time > {cursor}` for incremental append behavior
- `last_edited_time` is available on each comment for CDC-style tracking
- Only unresolved (open) comments are returned; resolved comments cannot be retrieved

---

### Pagination (All Endpoints)

All list endpoints use cursor-based pagination with the following contract:

| Request Parameter | Location | Description |
|-------------------|----------|-------------|
| `start_cursor` | query string (GET) or request body (POST) | Opaque cursor from previous response |
| `page_size` | query string (GET) or request body (POST) | Number of results (default: 100, max: 100) |

| Response Field | Type | Description |
|----------------|------|-------------|
| `results` | array | Current page of results |
| `has_more` | boolean | `true` if more pages exist |
| `next_cursor` | string or null | Pass to next request as `start_cursor`; null when `has_more` is false |
| `object` | string | Always `"list"` |

**Pagination loop pattern**:

```python
def paginate(api_call_fn, **kwargs) -> list[dict]:
    results = []
    cursor = None
    while True:
        response = api_call_fn(start_cursor=cursor, page_size=100, **kwargs)
        results.extend(response["results"])
        if not response["has_more"]:
            break
        cursor = response["next_cursor"]
    return results
```

---

### Rate Limits

| Limit | Value | Notes |
|-------|-------|-------|
| Requests per second | ~3 req/s (average) | Per integration token; short bursts above this are allowed |
| HTTP status on limit | 429 `rate_limited` | Always check for this status code |
| Retry guidance | Use `Retry-After` header | Header value is integer seconds to wait before retrying |
| Payload size | 500 KB max | Per request |
| Block array size | 1,000 elements max | Per request body |

**Retry strategy**: Implement exponential backoff with jitter. When receiving a 429, respect the `Retry-After` header value. Do not count rate-limited retries toward a fixed retry limit — continue retrying until the request succeeds or a non-429 error is returned.

```python
import time, random

def request_with_retry(fn, max_retries=10):
    for attempt in range(max_retries):
        response = fn()
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            jitter = random.uniform(0, 0.5)
            time.sleep(retry_after + jitter)
            continue
        return response
    raise Exception("Max retries exceeded")
```

---

## Field Type Mapping

| Notion API Type | Standard Type | Notes |
|-----------------|---------------|-------|
| `string` / `rich_text` | `STRING` | Rich text objects serialize to `plain_text` for simple string extraction; full rich text is JSON |
| `number` | `DOUBLE` | Covers integer and floating-point values; check `format` field for display hints |
| `boolean` / `checkbox` | `BOOLEAN` | |
| `date` (ISO 8601) | `TIMESTAMP` | May include timezone; `start` and `end` fields |
| `created_time`, `last_edited_time` | `TIMESTAMP` | ISO 8601 format with milliseconds, e.g., `"2022-03-01T19:05:00.000Z"` |
| `select` | `STRING` | Option name as string; option ID and color also available |
| `multi_select` | `ARRAY<STRING>` | Array of option name strings; full option objects available |
| `status` | `STRING` | Status name as string; group membership available |
| `people` | `ARRAY<STRING>` | Array of user UUIDs; full user objects available |
| `files` | `ARRAY<STRING>` | Array of file URLs |
| `url` | `STRING` | |
| `email` | `STRING` | |
| `phone_number` | `STRING` | No format enforcement |
| `relation` | `ARRAY<STRING>` | Array of related page UUIDs |
| `formula` | varies | Return type matches the formula's output type (`string`, `number`, `boolean`, `date`) |
| `rollup` | varies | Return type determined by rollup function and source property |
| `unique_id` | `STRING` | Formatted as `"{prefix}-{number}"` or just `"{number}"` |
| UUID (`id` fields) | `STRING` | All Notion UUIDs are 32-character strings with hyphens |
| `object` (type discriminator) | `STRING` | Literal enum values like `"page"`, `"block"`, `"user"` |

**Rich text flattening**: Rich text arrays contain formatted text segments. For tabular ingestion, extract `plain_text` from each element and concatenate:

```python
def extract_plain_text(rich_text_array: list) -> str:
    return "".join(segment.get("plain_text", "") for segment in rich_text_array)
```

---

## Known Quirks and Gotchas

1. **Blocks require recursive traversal**: There is no "get all blocks for a page" endpoint that returns deeply nested content in a single call. You must recursively call `GET /v1/blocks/{block_id}/children` for every block with `has_children: true`. Notion supports up to 30 levels of nesting. Deep nesting can trigger hundreds of API requests per page.

2. **Search endpoint is not a reliable global page list**: The `/v1/search` endpoint may not return recently created pages/databases immediately (indexing delay). For database rows, prefer `POST /v1/databases/{id}/query` to enumerate all entries reliably.

3. **Integration must be granted access to pages**: Internal integrations do not automatically access all workspace content. Each page/database must be explicitly shared with the integration. Pages nested under a shared page inherit access.

4. **API version 2025-09-03 breaking change**: The `"database"` filter value in Search was replaced with `"data_source"`. Database query operations moved to `/v1/data_sources/{data_source_id}/query`. If targeting `Notion-Version: 2025-09-03` or later, update all database references accordingly. This connector targets `2026-03-11` — use `"data_source"` in search filters.

5. **`archived` field is deprecated**: Use `in_trash` instead. Archived (trashed) pages/databases still appear in some API responses. Always check `in_trash: true` to exclude deleted records.

6. **Comments only return unresolved threads**: Resolved comments are permanently inaccessible via the API. This creates a gap in comment history if users resolve threads.

7. **Inline comment discovery requires per-block polling**: The comments API takes a `block_id` parameter, not a "page with all descendant comments" scope. To capture all inline discussions on a page, you must call `GET /v1/comments` for every block in the page — a multiplicative cost on top of block traversal.

8. **Users stream is snapshot-only**: There is no incremental path for users. The `GET /v1/users` endpoint does not support filtering by modification time.

9. **Page properties cap at 25 references**: The `GET /v1/pages/{page_id}` endpoint will not accurately return properties with more than 25 references (e.g., large relation properties). Use `GET /v1/pages/{page_id}/properties/{property_id}` for those fields.

10. **Formula and rollup property values are read-only and computed**: These cannot be written and may return stale values if the underlying relations have not been refreshed.

11. **Rate limit is per integration, not per user**: All requests using the same integration token share the ~3 req/s budget. Design the connector to serialize requests and respect the `Retry-After` header on 429s.

12. **`Notion-Version` header is mandatory**: Requests without this header return a `400 missing_version` error.

---

## Sources and References

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs — Intro | [https://developers.notion.com/reference/intro](https://developers.notion.com/reference/intro) | 2026-04-05 | High | Base URL, API versioning, pagination overview |
| Official Docs — Auth | [https://developers.notion.com/reference/authentication](https://developers.notion.com/reference/authentication) | 2026-04-05 | High | Bearer token format, header requirements |
| Official Docs — Authorization Guide | [https://developers.notion.com/docs/authorization](https://developers.notion.com/docs/authorization) | 2026-04-05 | High | Internal integration token setup, capabilities |
| Official Docs — Rate Limits | [https://developers.notion.com/reference/request-limits](https://developers.notion.com/reference/request-limits) | 2026-04-05 | High | Rate limit (3 req/s), payload limits, 429 handling |
| Official Docs — Errors | [https://developers.notion.com/reference/errors](https://developers.notion.com/reference/errors) | 2026-04-05 | High | All HTTP status codes and error codes |
| Official Docs — Pagination | [https://developers.notion.com/reference/pagination](https://developers.notion.com/reference/pagination) (inferred from intro ref) | 2026-04-05 | High | Cursor pagination fields and parameters |
| Official Docs — Database Object | [https://developers.notion.com/reference/database](https://developers.notion.com/reference/database) | 2026-04-05 | High | Complete database schema fields |
| Official Docs — Page Object | [https://developers.notion.com/reference/page](https://developers.notion.com/reference/page) | 2026-04-05 | High | Complete page schema fields |
| Official Docs — Block Object | [https://developers.notion.com/reference/block](https://developers.notion.com/reference/block) | 2026-04-05 | High | Block types, fields, recursive structure |
| Official Docs — Block Children | [https://developers.notion.com/reference/get-block-children](https://developers.notion.com/reference/get-block-children) | 2026-04-05 | High | Block children endpoint parameters |
| Official Docs — User Object | [https://developers.notion.com/reference/user](https://developers.notion.com/reference/user) | 2026-04-05 | High | User schema (person and bot types) |
| Official Docs — List Users | [https://developers.notion.com/reference/get-users](https://developers.notion.com/reference/get-users) | 2026-04-05 | High | Users endpoint parameters and response |
| Official Docs — Comment Object | [https://developers.notion.com/reference/comment-object](https://developers.notion.com/reference/comment-object) | 2026-04-05 | High | Complete comment schema |
| Official Docs — Comments Guide | [https://developers.notion.com/guides/data-apis/working-with-comments](https://developers.notion.com/guides/data-apis/working-with-comments) | 2026-04-05 | High | Comments endpoint, block_id parameter, limitations |
| Official Docs — Search | [https://developers.notion.com/reference/post-search](https://developers.notion.com/reference/post-search) | 2026-04-05 | High | Search endpoint parameters, filter values, sort |
| Official Docs — Database Query Filter | [https://developers.notion.com/reference/post-database-query-filter](https://developers.notion.com/reference/post-database-query-filter) | 2026-04-05 | High | Timestamp filter format for incremental sync |
| Official Docs — Property Object | [https://developers.notion.com/reference/property-object](https://developers.notion.com/reference/property-object) | 2026-04-05 | High | All database property types and configuration |
| Official Docs — Retrieve Database | [https://developers.notion.com/reference/retrieve-a-database](https://developers.notion.com/reference/retrieve-a-database) | 2026-04-05 | High | GET database endpoint parameters |
| Official Docs — v2025-09-03 Upgrade Guide | [https://developers.notion.com/docs/upgrade-guide-2025-09-03](https://developers.notion.com/docs/upgrade-guide-2025-09-03) | 2026-04-05 | High | Breaking changes, data_source namespace, migration |
| Official Docs — Timestamp Filter Changelog | [https://developers.notion.com/changelog/filter-databases-by-timestamp-even-if-they-dont-have-a-timestamp-property](https://developers.notion.com/changelog/filter-databases-by-timestamp-even-if-they-dont-have-a-timestamp-property) | 2026-04-05 | High | Timestamp filter format for last_edited_time |
| Airbyte — Notion Connector Docs | [https://docs.airbyte.com/integrations/sources/notion](https://docs.airbyte.com/integrations/sources/notion) | 2026-04-05 | High | Supported streams, sync modes, incremental cursors, rate limit gotchas, nested blocks warning |
| Airbyte — Notion GitHub Docs | [https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/notion.md](https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/notion.md) | 2026-04-05 | High | Stream details, incremental cursor fields, block recursion depth (30 levels) |
