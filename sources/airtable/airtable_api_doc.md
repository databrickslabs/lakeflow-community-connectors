# **Airtable API Documentation**

## **Authorization**

- **Chosen method**: Personal Access Token (PAT) for the Airtable REST API.
- **Base URL**: `https://api.airtable.com/v0`
- **Metadata Base URL**: `https://api.airtable.com/v0/meta`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <personal_access_token>`
  - Required scopes:
    - `data.records:read` — Read records from bases.
    - `schema.bases:read` — Read base schema (tables and fields).
  - Optional scopes:
    - `data.recordComments:read` — Read record comments (if needed).
- **Other supported methods (not used by this connector)**:
  - OAuth 2.0 is supported by Airtable, but the connector will **not** perform interactive OAuth flows. Tokens must be provisioned out-of-band and stored in configuration.

**Creating a Personal Access Token**:
1. Go to https://airtable.com/create/tokens
2. Click "Create new token"
3. Add required scopes: `data.records:read`, `schema.bases:read`
4. Select the bases you want to access (or all bases)
5. Click "Create Token" and save the token securely

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/meta/bases"
```

**Rate Limits**:
- **5 requests per second** per base for all API requests.
- **50 requests per second** aggregate across all bases for all traffic from a given PAT user/service account.
- Exceeding rate limits returns HTTP 429 with a `Retry-After` header indicating when to retry.
- The connector should implement exponential backoff with jitter for rate limit handling.


## **Object List**

For connector purposes, we treat Airtable **tables within bases** as objects/tables.
The object list is **dynamic** — discovered via the Metadata API.

### Object Discovery

Airtable uses a two-level hierarchy:
1. **Bases** — Workspaces that contain tables (similar to databases)
2. **Tables** — Tabular data within a base (each table becomes a connector object)

| Object Type | Description | Discovery Endpoint | Notes |
|------------|-------------|-------------------|-------|
| `bases` | List of all accessible bases | `GET /meta/bases` | Returns base IDs and names |
| `tables` (per base) | Tables within a specific base | `GET /meta/bases/{baseId}/tables` | Returns table schemas |

### List Bases Endpoint

**Endpoint**: `GET /meta/bases`

Returns a paginated list of bases the token has access to.

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `offset` | string | no | Pagination cursor from previous response |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/meta/bases"
```

**Example response**:

```json
{
  "bases": [
    {
      "id": "appxxxxxxxxxxxxxxx",
      "name": "My Project Tracker",
      "permissionLevel": "create"
    },
    {
      "id": "appyyyyyyyyyyyyyyy",
      "name": "Customer Database",
      "permissionLevel": "edit"
    }
  ],
  "offset": "itrxxxxxxxxxxxxxxxxx/recxxxxxxxxxxxxxxxxx"
}
```

### Connector Configuration

The connector will require:
- **base_id** (required): The Airtable base ID to connect to (e.g., `appxxxxxxxxxxxxxxx`).
- **table_ids** (optional): Specific table IDs or names to sync; if not specified, sync all tables in the base.


## **Object Schema**

### Schema Discovery

Airtable provides schema information via the Metadata API. Each table's schema is retrieved along with all other tables in the base.

**Endpoint**: `GET /meta/bases/{baseId}/tables`

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `baseId` | string | yes | The ID of the base (e.g., `appxxxxxxxxxxxxxxx`) |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/meta/bases/appxxxxxxxxxxxxxxx/tables"
```

**Example response**:

```json
{
  "tables": [
    {
      "id": "tblxxxxxxxxxxxxxxx",
      "name": "Tasks",
      "primaryFieldId": "fldxxxxxxxxxxxxxxx",
      "fields": [
        {
          "id": "fldxxxxxxxxxxxxxxx",
          "name": "Name",
          "type": "singleLineText"
        },
        {
          "id": "fldyyyyyyyyyyyyyyy",
          "name": "Status",
          "type": "singleSelect",
          "options": {
            "choices": [
              {"id": "selxxxxxxxxx", "name": "Todo", "color": "redLight2"},
              {"id": "selyyyyyyyyy", "name": "In Progress", "color": "yellowLight2"},
              {"id": "selzzzzzzzzz", "name": "Done", "color": "greenLight2"}
            ]
          }
        },
        {
          "id": "fldzzzzzzzzzzzzzzz",
          "name": "Due Date",
          "type": "date",
          "options": {
            "dateFormat": {"name": "local"}
          }
        },
        {
          "id": "fldaaaaaaaaaaaaaaa",
          "name": "Assignee",
          "type": "singleCollaborator"
        },
        {
          "id": "fldbbbbbbbbbbbbbb",
          "name": "Priority",
          "type": "number",
          "options": {
            "precision": 0
          }
        }
      ],
      "views": [
        {
          "id": "viwxxxxxxxxxxxxxxx",
          "name": "Grid view",
          "type": "grid"
        }
      ]
    }
  ]
}
```

### System Fields (Always Present on Records)

Every record in Airtable has these system-level fields that are always present in the API response:

| Field Name | Type | Description |
|------------|------|-------------|
| `id` | string | Unique record ID (e.g., `recxxxxxxxxxxxxxxx`). Primary key for the record. |
| `createdTime` | string (ISO 8601 datetime) | Timestamp when the record was created. |
| `fields` | object | Key-value pairs of field names to cell values. |

**Note**: `lastModifiedTime` is **not** a system field in Airtable. To track modification times, users must create a "Last Modified" formula or "Last modified time" field type in their table schema.


## **Get Object Primary Keys**

### Primary Key Definition

There is no dedicated metadata endpoint to get the primary key for Airtable tables.
The primary key is defined **statically** based on Airtable's record structure.

- **Primary key for all tables**: `id`
  - Type: string (17 characters, prefixed with `rec`)
  - Property: Unique across all records in a base.

The connector will:
- Read the `id` field from each record returned by `GET /v0/{baseId}/{tableIdOrName}`.
- Use it as the immutable primary key for upserts.

### Primary Field vs Primary Key

Airtable distinguishes between:
- **Primary Key** (`id`): The unique record identifier used by the API.
- **Primary Field** (`primaryFieldId` in schema): The first column displayed in the table UI, often used as a human-readable identifier.

For connector purposes, **always use `id` as the primary key**, not the primary field value.

Example showing primary key in response:

```json
{
  "records": [
    {
      "id": "recxxxxxxxxxxxxxxx",
      "createdTime": "2024-01-15T10:30:00.000Z",
      "fields": {
        "Name": "Task 1",
        "Status": "In Progress"
      }
    }
  ]
}
```


## **Object's Ingestion Type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

### Airtable Ingestion Considerations

**Default ingestion type for all tables**: `snapshot`

**Rationale**:
- Airtable's List Records API does **not** support a `since` or `updated_at` filter parameter.
- There is no built-in `lastModifiedTime` system field on records.
- The `createdTime` field is immutable and only useful for append-only scenarios of new records.

**Alternative: CDC with Custom Last Modified Field**

If the Airtable table includes a "Last modified time" field type (manually configured by the user), the connector could potentially support `cdc` ingestion:

1. User creates a "Last modified time" field in their Airtable table.
2. Connector uses `filterByFormula` with that field as a cursor.
3. Example: `filterByFormula=IS_AFTER({Last Modified}, '2024-01-01T00:00:00.000Z')`

**Limitations of this approach**:
- Requires user to configure the field in every table.
- Formula filter syntax can be complex.
- Not all existing tables will have this field.

**Recommendation**: Start with `snapshot` ingestion for simplicity and reliability. Consider adding optional `cdc` support later if the user explicitly configures a last-modified field name.

### Handling Deletes

- Airtable does not provide a "deleted records" stream or tombstone mechanism.
- For `snapshot` ingestion, deleted records will naturally disappear when the table is fully refreshed.
- For potential future `cdc` support, detecting deletes would require comparing current records against previously synced records.


## **Read API for Data Retrieval**

### Primary Read Endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/v0/{baseId}/{tableIdOrName}`
- **Base URL**: `https://api.airtable.com`

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `baseId` | string | yes | The ID of the base (e.g., `appxxxxxxxxxxxxxxx`) |
| `tableIdOrName` | string | yes | The table ID (e.g., `tblxxxxxxxxxxxxxxx`) or URL-encoded table name |

**Query Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `pageSize` | integer | no | 100 | Number of records per page (max 100). |
| `offset` | string | no | none | Pagination cursor from previous response. |
| `fields[]` | string (repeated) | no | all | Specific field names to return. Repeat for multiple fields. |
| `filterByFormula` | string | no | none | Airtable formula to filter records. |
| `sort[0][field]` | string | no | none | Field name to sort by. |
| `sort[0][direction]` | string | no | `asc` | Sort direction: `asc` or `desc`. |
| `view` | string | no | none | Name or ID of a view to use (applies view's filters/sorts). |
| `cellFormat` | string | no | `json` | Format for cell values: `json` or `string`. |
| `timeZone` | string | no | none | Timezone for date/time formatting (e.g., `America/New_York`). |
| `userLocale` | string | no | none | User locale for date formatting (e.g., `en-us`). |
| `returnFieldsByFieldId` | boolean | no | false | If true, returns fields by ID instead of name. |

### Pagination Strategy

Airtable uses **cursor-based pagination** with the `offset` parameter:

1. Make initial request without `offset`.
2. If response contains an `offset` field, use that value in the next request.
3. Continue until response does not contain `offset`.

**Example pagination flow**:

```bash
# First request
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/appxxxxxxxxxxxxxxx/Tasks?pageSize=100"

# Response includes offset
# {
#   "records": [...],
#   "offset": "itrxxxxxxxxxxxxxxxxx/recxxxxxxxxxxxxxxxxx"
# }

# Second request with offset
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/appxxxxxxxxxxxxxxx/Tasks?pageSize=100&offset=itrxxxxxxxxxxxxxxxxx/recxxxxxxxxxxxxxxxxx"
```

### Example Request and Response

**Request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/appxxxxxxxxxxxxxxx/Tasks?pageSize=2"
```

**Response**:

```json
{
  "records": [
    {
      "id": "recxxxxxxxxxxxxxxx",
      "createdTime": "2024-01-15T10:30:00.000Z",
      "fields": {
        "Name": "Implement login page",
        "Status": "In Progress",
        "Due Date": "2024-02-01",
        "Assignee": {
          "id": "usrxxxxxxxxxxxxxxx",
          "email": "alice@example.com",
          "name": "Alice Smith"
        },
        "Priority": 1
      }
    },
    {
      "id": "recyyyyyyyyyyyyyyy",
      "createdTime": "2024-01-16T14:00:00.000Z",
      "fields": {
        "Name": "Design system documentation",
        "Status": "Todo",
        "Due Date": "2024-02-15",
        "Priority": 2
      }
    }
  ],
  "offset": "itrxxxxxxxxxxxxxxxxx/reczzzzzzzzzzzzzzzzz"
}
```

### Filtering with Formulas

The `filterByFormula` parameter accepts Airtable formula syntax for server-side filtering.

**Common filter examples**:

```bash
# Filter by exact match
filterByFormula={Status}='Done'

# Filter by date comparison (useful for pseudo-incremental reads)
filterByFormula=IS_AFTER({Last Modified}, '2024-01-01T00:00:00.000Z')

# Filter by non-empty field
filterByFormula=NOT({Assignee}='')

# Combine conditions
filterByFormula=AND({Status}='In Progress', {Priority}>=1)
```

**URL encoding required**: Formula must be URL-encoded when passed as query parameter.

### Using Views

If a specific view is configured, passing the `view` parameter applies that view's:
- Filters
- Sorts
- Hidden fields (only visible fields returned)

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/appxxxxxxxxxxxxxxx/Tasks?view=Active%20Tasks"
```


## **Field Type Mapping**

### Airtable Field Types to Connector Logical Types

| Airtable Field Type | Cell Value Format | Connector Logical Type | Notes |
|---------------------|-------------------|------------------------|-------|
| `singleLineText` | string | string | Simple text field. |
| `multilineText` | string | string | Multi-line text with line breaks. |
| `richText` | string | string | Rich text with markdown-like formatting. |
| `email` | string | string | Email address. |
| `url` | string | string | URL. |
| `phoneNumber` | string | string | Phone number. |
| `number` | number | double/float | Numeric value with configurable precision. |
| `currency` | number | double/float | Currency value (number with currency symbol in UI). |
| `percent` | number | double/float | Percentage (0.5 = 50% in UI). |
| `duration` | number | long/integer | Duration in seconds. |
| `rating` | number | integer | Rating value (1-10 typically). |
| `count` | number | integer | Count of linked records. |
| `autoNumber` | number | integer | Auto-incrementing number. |
| `checkbox` | boolean | boolean | True/false (null if unchecked in some configs). |
| `date` | string (ISO 8601 date) | date | Date without time (e.g., `"2024-01-15"`). |
| `dateTime` | string (ISO 8601 datetime) | timestamp | Date with time (e.g., `"2024-01-15T10:30:00.000Z"`). |
| `createdTime` | string (ISO 8601 datetime) | timestamp | System field: record creation time. |
| `lastModifiedTime` | string (ISO 8601 datetime) | timestamp | User-configured field: last modification time. |
| `singleSelect` | string | string | Selected option name. |
| `multipleSelects` | array of strings | array\<string\> | Array of selected option names. |
| `singleCollaborator` | object | struct | Collaborator with `id`, `email`, `name`. |
| `multipleCollaborators` | array of objects | array\<struct\> | Array of collaborators. |
| `multipleRecordLinks` | array of strings | array\<string\> | Array of linked record IDs. |
| `multipleAttachments` | array of objects | array\<struct\> | Array of attachment objects with `url`, `filename`, etc. |
| `barcode` | object | struct | Object with `text` and optional `type`. |
| `button` | object | struct | Button field (typically not synced). |
| `formula` | varies | string/number/array | Result type depends on formula. |
| `rollup` | varies | string/number/array | Aggregation result, type depends on config. |
| `lookup` | array of any | array\<any\> | Values looked up from linked records. |
| `multipleLookupValues` | array of any | array\<any\> | Multiple lookup values. |
| `createdBy` | object | struct | Collaborator who created the record. |
| `lastModifiedBy` | object | struct | Collaborator who last modified the record. |
| `externalSyncSource` | string | string | External sync source identifier. |

### Nested Struct Definitions

**Collaborator struct** (`singleCollaborator`, `createdBy`, `lastModifiedBy`):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Collaborator user ID. |
| `email` | string | Email address. |
| `name` | string | Display name. |

**Attachment struct** (elements of `multipleAttachments`):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Attachment ID. |
| `url` | string | URL to download the attachment. |
| `filename` | string | Original filename. |
| `size` | number | File size in bytes. |
| `type` | string | MIME type. |
| `width` | number (optional) | Image width in pixels. |
| `height` | number (optional) | Image height in pixels. |
| `thumbnails` | object (optional) | Thumbnail versions for images. |

**Barcode struct**:

| Field | Type | Description |
|-------|------|-------------|
| `text` | string | Barcode text/value. |
| `type` | string (optional) | Barcode type (e.g., `upca`, `code128`). |

### Special Behaviors and Constraints

- **Null handling**: Fields not present in the `fields` object should be treated as `null`.
- **Empty strings vs null**: An empty string `""` is different from a missing/null field.
- **Linked records**: `multipleRecordLinks` returns record IDs, not the linked record data. To get linked data, use lookups or separate API calls.
- **Attachment URLs**: Attachment URLs are temporary and expire after a few hours. For long-term storage, download the files.
- **Formula/Rollup types**: The result type of formula and rollup fields depends on the formula configuration; inspect the actual cell value type at runtime.


## **Write API**

The initial connector implementation is primarily **read-only**. However, for completeness, the Airtable REST API supports write operations.

### Create Records

- **HTTP method**: `POST`
- **Endpoint**: `/v0/{baseId}/{tableIdOrName}`

**Request body (JSON)**:

```json
{
  "records": [
    {
      "fields": {
        "Name": "New Task",
        "Status": "Todo",
        "Priority": 1
      }
    },
    {
      "fields": {
        "Name": "Another Task",
        "Status": "In Progress"
      }
    }
  ],
  "typecast": true
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `records` | array | yes | Array of record objects with `fields`. |
| `fields` | object | yes | Key-value pairs of field name to value. |
| `typecast` | boolean | no | If true, Airtable will attempt to convert string values to appropriate types. |

**Limits**: Maximum 10 records per request.

**Example request**:

```bash
curl -X POST \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "fields": {
          "Name": "New Task",
          "Status": "Todo"
        }
      }
    ]
  }' \
  "https://api.airtable.com/v0/appxxxxxxxxxxxxxxx/Tasks"
```

**Response**: Returns the created records with their assigned `id` and `createdTime`.

### Update Records (Partial Update)

- **HTTP method**: `PATCH`
- **Endpoint**: `/v0/{baseId}/{tableIdOrName}`

Updates only the specified fields; other fields remain unchanged.

**Request body**:

```json
{
  "records": [
    {
      "id": "recxxxxxxxxxxxxxxx",
      "fields": {
        "Status": "Done"
      }
    }
  ]
}
```

### Update Records (Destructive Update)

- **HTTP method**: `PUT`
- **Endpoint**: `/v0/{baseId}/{tableIdOrName}`

Replaces all fields; unspecified fields are cleared.

### Delete Records

- **HTTP method**: `DELETE`
- **Endpoint**: `/v0/{baseId}/{tableIdOrName}`
- **Query Parameters**: `records[]` - Record IDs to delete (max 10).

**Example**:

```bash
curl -X DELETE \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  "https://api.airtable.com/v0/appxxxxxxxxxxxxxxx/Tasks?records[]=recxxxxxxxxxxxxxxx&records[]=recyyyyyyyyyyyyyyy"
```

### Validation / Read-After-Write

To validate writes, the connector (or user) can:
- Parse the response from create/update operations (returns the modified records).
- Perform a subsequent read of the affected records by ID.


## **Known Quirks & Edge Cases**

- **No incremental read support**: Unlike many APIs, Airtable does not have a `since` or `updated_at` query parameter. Incremental reads require user-configured "Last modified time" fields.

- **Rate limits are per-base**: The 5 req/sec limit applies per base, so syncing multiple tables from the same base shares this limit.

- **Attachment URL expiration**: URLs in `multipleAttachments` fields expire after a few hours. If storing attachments, download them promptly.

- **Field names can change**: Unlike field IDs, field names can be renamed by users. Consider using `returnFieldsByFieldId=true` for stability if field renames are a concern.

- **Table names can change**: Similar to fields, table names can change. Using table IDs (e.g., `tblxxxxxxxxx`) is more stable than names.

- **Empty cells**: Fields with no value are omitted from the `fields` object (not returned as null). The connector should handle missing keys gracefully.

- **Checkbox behavior**: Unchecked checkboxes may return `null`, `false`, or be omitted entirely depending on table configuration.

- **Formula fields are read-only**: Formula, rollup, count, and lookup fields cannot be written to; they are computed values.

- **Large tables**: Airtable has limits on records per base (50,000 for free, higher for paid plans). Very large tables may require pagination handling across many pages.

- **Timezone handling**: Date fields without times are returned as ISO date strings (e.g., `"2024-01-15"`). DateTime fields include timezone info. Pass `timeZone` parameter if specific timezone formatting is needed.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://airtable.com/developers/web/api/introduction | 2024-12-17 | Highest | Base URL, API structure, and general overview. |
| Official Docs | https://airtable.com/developers/web/api/authentication | 2024-12-17 | Highest | PAT authentication method, header format, scopes. |
| Official Docs | https://airtable.com/developers/web/api/list-records | 2024-12-17 | Highest | Record listing endpoint, query parameters, pagination via offset. |
| Official Docs | https://airtable.com/developers/web/api/list-bases | 2024-12-17 | Highest | Base discovery endpoint and response structure. |
| Official Docs | https://airtable.com/developers/web/api/get-base-schema | 2024-12-17 | Highest | Schema discovery endpoint, table and field metadata. |
| Official Docs | https://airtable.com/developers/web/api/field-model | 2024-12-17 | Highest | Complete field type definitions and cell value formats. |
| Official Docs | https://airtable.com/developers/web/api/rate-limits | 2024-12-17 | Highest | Rate limit details (5/sec per base, 50/sec per user). |
| Official Docs | https://airtable.com/developers/web/api/create-records | 2024-12-17 | Highest | Record creation endpoint and request format. |
| Official Docs | https://airtable.com/developers/web/api/update-record | 2024-12-17 | Highest | PATCH vs PUT update semantics. |
| Official Docs | https://airtable.com/developers/web/api/delete-record | 2024-12-17 | Highest | Record deletion endpoint. |
| OSS Connector Docs | https://docs.airbyte.com/integrations/sources/airtable | 2024-12-17 | High | Airbyte implementation confirms PAT auth, scopes, Full Refresh mode (no incremental), and data type mappings. |


## **Sources and References**

- **Official Airtable Web API documentation** (highest confidence)
  - https://airtable.com/developers/web/api/introduction
  - https://airtable.com/developers/web/api/authentication
  - https://airtable.com/developers/web/api/list-records
  - https://airtable.com/developers/web/api/list-bases
  - https://airtable.com/developers/web/api/get-base-schema
  - https://airtable.com/developers/web/api/field-model
  - https://airtable.com/developers/web/api/rate-limits
  - https://airtable.com/developers/web/api/create-records
  - https://airtable.com/developers/web/api/update-record
  - https://airtable.com/developers/web/api/delete-record

- **Airtable Personal Access Tokens guide** (highest confidence)
  - https://airtable.com/developers/web/guides/personal-access-tokens

- **Airbyte Airtable source connector documentation** (high confidence)
  - https://docs.airbyte.com/integrations/sources/airtable
  - Confirms PAT authentication, required scopes, Full Refresh sync mode (no incremental), and field type mappings.

When conflicts arise, **official Airtable documentation** is treated as the source of truth, with the Airbyte connector used to validate practical implementation details.

