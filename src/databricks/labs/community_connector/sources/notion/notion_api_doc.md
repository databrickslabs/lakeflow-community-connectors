# Notion API Documentation

## Authorization

### Supported Methods

Notion supports two authentication methods:

1. **API Token (Internal Integration)** - Preferred for connector usage
2. **OAuth 2.0** - For user-facing applications

### API Token Authentication

For internal integrations, use a Bot API token:

- **Header**: `Authorization: Bearer {token}`
- **Header**: `Notion-Version: 2025-09-03` (API version)
- **Header**: `Content-Type: application/json`

### OAuth 2.0 Authentication

For OAuth flows, the connector stores:
- `client_id`
- `client_secret`
- `access_token` (exchanged at runtime)

The connector does NOT run user-facing OAuth flows; it uses pre-obtained tokens.

### Example API Request

```bash
# Using API Token
curl -X GET https://api.notion.com/v1/users \
  -H "Authorization: Bearer secret_xxx" \
  -H "Notion-Version: 2025-09-03" \
  -H "Content-Type: application/json"
```

## Object List

The Notion API provides access to the following objects:

| Object | Description | Incremental |
|--------|-------------|-------------|
| `pages` | Page content and metadata | Yes |
| `data_sources` | Tables of data that live under a Notion database | Yes |
| `blocks` | Block content within pages | Yes |
| `users` | Workspace users | No (snapshot) |
| `comments` | Page/block comments | Yes |

**Object List Retrieval**: Objects are discovered via the `/search` endpoint which returns pages and data_sources.

## Object Schema

### Pages Schema

Pages contain properties (custom fields) and content blocks. The properties structure is dynamic based on the page template.

**Key Fields**:
- `id`: Unique page identifier
- `url`: Full URL to the page
- `properties`: Object containing page properties (title, date, people, etc.)
- `parent`: Parent reference (workspace, page, or database)
- `created_time`: ISO 8601 timestamp
- `last_edited_time`: ISO 8601 timestamp
- `archived`: Boolean indicating if page is archived

### Datasources Schema

Datasources define structured data with property templates.

**Key Fields**:
- `id`: Unique data_source identifier
- `title`: Array of rich text objects
- `properties`: Property definitions with types
- `parent`: Parent reference
- `created_time`: ISO 8601 timestamp
- `last_edited_time`: ISO 8601 timestamp

### Blocks Schema

Blocks represent content units within pages.

**Block Types**:
- `paragraph`, `heading_1`, `heading_2`, `heading_3`
- `bulleted_list_item`, `numbered_list_item`
- `to_do`, `toggle`, `child_page`, `child_database`
- `image`, `video`, `file`, `pdf`, `bookmark`
- `code`, `quote`, `divider`, `callout`
- `embed`, `link_preview`, `table`, `table_row`

**Key Fields**:
- `id`: Unique block identifier
- `type`: Block type string
- `has_children`: Boolean for nested blocks
- `created_time`, `last_edited_time`: Timestamps

### Users Schema

**Key Fields**:
- `id`: Unique user identifier
- `name`: Display name
- `avatar_url`: URL to avatar image
- `type`: `person` or `bot`
- `person.email`: Email address (for person type)
- `bot.owner`: Owner information (for bot type)

### Comments Schema

**Key Fields**:
- `id`: Unique comment identifier
- `parent`: Reference to page or block
- `discussion_id`: Discussion thread identifier
- `created_by`: User object who created comment
- `created_time`: ISO 8601 timestamp
- `rich_text`: Array of comment content

## Get Object Primary Keys

All Notion objects use a consistent primary key pattern:

| Object | Primary Key |
|--------|-------------|
| `pages` | `id` |
| `data_sources` | `id` |
| `blocks` | `id` |
| `users` | `id` |
| `comments` | `id` |
| `data_sources` | `id` |

The `id` field is a UUID string returned in every API response.

## Object's Ingestion Type

| Object | Ingestion Type | Cursor Field |
|--------|----------------|--------------|
| `pages` | `cdc` | `last_edited_time` |
| `data_sources` | `cdc` | `last_edited_time` |
| `blocks` | `cdc` | `last_edited_time` |
| `users` | `snapshot` | N/A |
| `comments` | `cdc` | `created_time` |
| `data_sources` | `cdc` | `last_edited_time` |

- **`cdc`**: Incremental sync with upserts using `last_edited_time` or `created_time`
- **`snapshot`**: Full refresh only (users don't have incremental updates)

## Read API for Data Retrieval

### Search Endpoint (Pages & Data_sources)

**Endpoint**: `POST /search`

**Purpose**: Discover pages and data_sources accessible to the integration.

**Request Body**:
```json
{
  "filter": {
    "property": "object",
    "value": "page"
  },
  "sort": {
    "direction": "descending",
    "timestamp": "last_edited_time"
  },
  "page_size": 100,
  "start_cursor": "..."
}
```

**Response**:
```json
{
  "object": "list",
  "results": [...],
  "has_more": true,
  "next_cursor": "..."
}
```

**Pagination**: Cursor-based using `start_cursor` and `next_cursor`.

### Users Endpoint

**Endpoint**: `GET /users`

**Purpose**: List all users in the workspace.

**Query Parameters**:
- `page_size`: 1-100 (default: 100)
- `start_cursor`: Pagination cursor

**Response**:
```json
{
  "object": "list",
  "results": [...],
  "has_more": true,
  "next_cursor": "..."
}
```

### Comments Endpoint

**Endpoint**: `GET /comments`

**Purpose**: Retrieve comments on a page or block.

**Query Parameters**:
- `block_id`: Required - ID of the block to get comments for
- `page_id`: Alternative - ID of the page
- `page_size`: 1-100
- `start_cursor`: Pagination cursor

**Response**:
```json
{
  "object": "list",
  "results": [...],
  "has_more": true,
  "next_cursor": "..."
}
```

### Blocks Endpoint

**Endpoint**: `GET /blocks/{block_id}/children`

**Purpose**: Retrieve child blocks of a page or block.

**Path Parameters**:
- `block_id`: Parent block ID

**Query Parameters**:
- `page_size`: 1-100
- `start_cursor`: Pagination cursor

**Response**:
```json
{
  "object": "list",
  "results": [...],
  "has_more": true,
  "next_cursor": "..."
}
```

### Datasources Query Endpoint

**Endpoint**: `POST /search`

**Purpose**: Query data_sources contents with filters and sorts.

**Request Body**:
```json
{
  "filter": {...},
  "sorts": [...],
  "page_size": 100,
  "start_cursor": "..."
}
```

**Response**: Same pagination structure as search.

### Incremental Sync Strategy

**Cursor Field**: `last_edited_time` for pages/data_sources/blocks, `created_time` for comments.

**Incremental Query Pattern**:
1. Store the max `last_edited_time` from previous sync
2. Query with `sort: {direction: "descending", timestamp: "last_edited_time"}`
3. Filter results client-side: `record.last_edited_time >= last_sync_time`
4. Update cursor to max `last_edited_time` from current batch

**Lookback Window**: Apply a 5-second lookback to catch concurrently updated records:
```
start_time = last_cursor - 5 seconds
```

**Rate Limits**: 
- Approximately 3 requests per second per integration
- 429 status code on rate limit exceeded
- Retry after `retry-after` header value

## Field Type Mapping

| Notion Type | Spark Type | Notes |
|-------------|------------|-------|
| `string` | `StringType` | Text values |
| `number` | `DoubleType` | Numeric values |
| `boolean` | `BooleanType` | True/false |
| `date` | `StringType` | ISO 8601 format |
| `datetime` | `StringType` | ISO 8601 format |
| `email` | `StringType` | Email addresses |
| `phone_number` | `StringType` | Phone numbers |
| `url` | `StringType` | URLs |
| `rich_text` | `ArrayType(MapType)` | Array of text objects |
| `people` | `ArrayType(MapType)` | Array of user references |
| `files` | `ArrayType(MapType)` | Array of file objects |
| `relation` | `ArrayType(MapType)` | Array of related page IDs |
| `rollup` | `MapType` | Aggregated values |
| `select` | `MapType` | Single select option |
| `multi_select` | `ArrayType(MapType)` | Multiple select options |
| `status` | `MapType` | Status field |
| `created_time` | `StringType` | ISO 8601 timestamp |
| `last_edited_time` | `StringType` | ISO 8601 timestamp |
| `created_by` | `MapType` | User object |
| `last_edited_by` | `MapType` | User object |
| `archived` | `BooleanType` | Archive status |

### Special Field Behaviors

- **`properties`**: Dynamic structure based on page/database template. Each property has a type and value.
- **`rich_text`**: Array of objects with `text`, `plain_text`, `href` fields.
- **`parent`**: Object with `type` (workspace/page/database) and `id` or `workspace` boolean.
- **`url`**: Full Notion URL to the resource.

## Rate Limits

- **Limit**: ~3 requests per second per integration
- **Exceeded**: HTTP 429 Too Many Requests
- **Retry**: Use `retry-after` header value (in seconds)
- **Recommendation**: Implement exponential backoff with max 3 retries

## Sources and References

| Source Type | URL | Confidence | What it confirmed |
|-------------|-----|------------|-------------------|
| Official Notion API Docs | https://developers.notion.com/ | Highest | All endpoints, auth, pagination |
| Airbyte Notion Connector | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-notion | High | Stream definitions, schemas, incremental logic |
| Airbyte Manifest | manifest.yaml | High | Pagination config, error handling, field mappings |

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.notion.com/reference | 2026-07-21 | High | API structure, auth, rate limits |
| Airbyte Connector | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-notion | 2026-07-21 | High | Stream configs, schemas |
| Airbyte Manifest | https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-integrations/connectors/source-notion/manifest.yaml | 2026-07-21 | High | Pagination, error handling |

## Known Quirks

1. **Dynamic Properties**: Page/database properties are schema-less and vary by template. The connector must handle arbitrary property structures.

2. **Block Hierarchy**: Blocks can have nested children (up to 30 levels deep). Recursive fetching is required for complete content.

3. **Permission Errors**: 404 with "Make sure the relevant pages and data_sources are shared with your integration" should be ignored (not a connector error).

4. **Invalid Cursor**: 400 with "The start_cursor provided is invalid" should be handled gracefully (cursor expiration).

5. **Users Stream**: Requires explicit "Read user information" permission in Notion integration settings.

6. **Timestamp Format**: All timestamps use ISO 8601 format: `YYYY-MM-DDTHH:MM:SS.000Z`