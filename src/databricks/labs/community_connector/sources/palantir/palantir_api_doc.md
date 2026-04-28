# **Palantir Foundry Ontology API Documentation**

## **Authorization**

- **Chosen method**: Bearer Token for the Palantir Foundry REST API v2.
- **Base URL**: `https://{hostname}/api/v2`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <token>`
  - Required scope for read-only ontology access: `api:ontologies-read`
- **Token generation**: Generate tokens via **Account > Settings > Tokens** in the Foundry UI. These tokens carry full user permissions and should be stored securely.
- **Other supported methods (not used by this connector)**:
  - OAuth 2.0 Authorization Code grant (for apps acting on behalf of users) and Client Credentials grant (for service users) are supported by Foundry, but the connector uses pre-provisioned Bearer tokens stored in Unity Catalog connection configuration.

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies"
```

Notes:
- All requests must use HTTPS.
- Rate limiting for individual users: **5,000 requests/minute** with **30 simultaneous** connections. Service users have no request limit but are capped at **800 simultaneous** connections.
- Exceeding rate limits returns `429` or `503`. The connector retries with exponential backoff (up to 5 attempts) and includes a 0.1s delay between page fetches.


## **Object List**

For this connector, Palantir ontology object types are treated as **tables**. The object list is **dynamic** — discovered at runtime via the List Object Types API, not statically defined in the connector code.

| API Endpoint | Purpose | Connector Method |
|---|---|---|
| `GET /api/v2/ontologies/{ontology}/objectTypes` | List all object types in an ontology | `list_tables()` |
| `GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}` | Get object type definition (schema, primary key) | `get_table_schema()`, `read_table_metadata()` |
| `POST /api/v2/ontologies/{ontology}/objectSets/loadObjects` | Load object instances with snapshot-consistent pagination | `read_table()` → `_fetch_page()` |
| `POST /api/v2/ontologies/{ontology}/objectSets/aggregate` | Get max cursor value (lightweight, no records) | `_get_max_cursor_value()` (primary) |
| `POST /api/v2/ontologies/{ontology}/objects/{objectType}/search` | Get max cursor value (orderBy desc, limit 1) | `_get_max_cursor_value()` (fallback) |

**Connector scope**:
- All object types in the configured ontology are available as tables.
- Each object type maps to one table, named by its `apiName`.
- The connector supports snapshot (full refresh) and CDC (incremental with server-side filtering) ingestion modes.
- Data is fetched via `loadObjectSet` with `snapshot=true` for consistent pagination across large datasets.
- CDC incremental runs use server-side `where: gt` filtering via objectSet composition.
- Max cursor lookup uses the aggregate endpoint with orderBy desc fallback.
- Transient errors (429, 503) are retried with exponential backoff (up to 5 attempts).
- Link types, action types, and query types are **not** supported.


## **Endpoint Details**

### 1. List Ontologies

**Endpoint**: `GET /api/v2/ontologies`

**Purpose**: Discover available ontologies (used during setup to find the `ontology_api_name`).

**Not called by the connector at runtime** — the ontology API name is provided as a connection parameter.

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies"
```

**Example response**:

```json
{
  "data": [
    {
      "apiName": "ontology-282f1207-a9f0-408c-8820-741db4f051b1",
      "displayName": "My Ontology",
      "description": "Production ontology for flight data",
      "rid": "ri.ontology.main.ontology.abc123"
    }
  ]
}
```


### 2. List Object Types

**Endpoint**: `GET /api/v2/ontologies/{ontologyApiName}/objectTypes`

**Used by**: `list_tables()`

**Purpose**: Retrieve all object types defined in the ontology. Each object type becomes a table the connector can ingest.

**Path parameters**:

| Parameter | Type | Description |
|---|---|---|
| `ontologyApiName` | string | API name of the target ontology |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies/ontology-282f1207-a9f0-408c-8820-741db4f051b1/objectTypes"
```

**Example response**:

```json
{
  "data": [
    {
      "apiName": "ExampleFlight",
      "displayName": "Example Flight",
      "description": "Represents commercial passenger flights in the US",
      "pluralDisplayName": "Example Flights",
      "primaryKey": "flightId",
      "properties": {
        "flightId": {
          "dataType": { "type": "string" },
          "description": "Unique flight identifier"
        },
        "airlineId": {
          "dataType": { "type": "string" },
          "description": "Airline carrier code"
        },
        "departureTimestamp": {
          "dataType": { "type": "timestamp" },
          "description": "Scheduled departure time"
        },
        "airTime": {
          "dataType": { "type": "integer" },
          "description": "Air time in minutes"
        }
      },
      "rid": "ri.ontology.main.object-type.abc123",
      "status": "ACTIVE"
    },
    {
      "apiName": "ExampleRouteAlert",
      "displayName": "Example Route Alert",
      "primaryKey": "alertId",
      "properties": { "..." : "..." }
    }
  ]
}
```

**Response schema**:

| Field | Type | Description |
|---|---|---|
| `data` | array | List of object type definitions |
| `data[].apiName` | string | API name used to reference this object type (used as table name) |
| `data[].displayName` | string | Human-readable name |
| `data[].description` | string | Object type description |
| `data[].primaryKey` | string or array | Primary key field name(s) |
| `data[].properties` | object | Map of property name to property definition |
| `data[].rid` | string | Resource identifier |
| `data[].status` | string | Object type status (e.g., `ACTIVE`) |

**Connector behavior**:
- Extracts `apiName` from each object type in `data` to build the table list.
- Result is cached in `_object_types_cache` for the lifetime of the connector instance.


### 3. Get Object Type Definition

**Endpoint**: `GET /api/v2/ontologies/{ontologyApiName}/objectTypes/{objectTypeApiName}`

**Used by**: `get_table_schema()`, `read_table_metadata()`

**Purpose**: Retrieve the full definition of a single object type, including properties and primary key. Used to dynamically build the Spark schema.

**Path parameters**:

| Parameter | Type | Description |
|---|---|---|
| `ontologyApiName` | string | API name of the target ontology |
| `objectTypeApiName` | string | API name of the object type |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies/ontology-282f1207-a9f0-408c-8820-741db4f051b1/objectTypes/ExampleFlight"
```

**Example response**:

```json
{
  "apiName": "ExampleFlight",
  "displayName": "Example Flight",
  "description": "Represents commercial passenger flights in the US",
  "pluralDisplayName": "Example Flights",
  "primaryKey": "flightId",
  "properties": {
    "flightId": {
      "dataType": { "type": "string" },
      "description": "Unique flight identifier"
    },
    "flightNumber": {
      "dataType": { "type": "string" },
      "description": "Flight number"
    },
    "airlineId": {
      "dataType": { "type": "string" },
      "description": "Airline carrier code"
    },
    "departureTimestamp": {
      "dataType": { "type": "timestamp" },
      "description": "Scheduled departure time"
    },
    "arrivalTimestamp": {
      "dataType": { "type": "timestamp" },
      "description": "Scheduled arrival time"
    },
    "airTime": {
      "dataType": { "type": "integer" },
      "description": "Air time in minutes"
    },
    "depDelay": {
      "dataType": { "type": "double" },
      "description": "Departure delay in minutes"
    },
    "originAirport": {
      "dataType": { "type": "string" },
      "description": "Origin airport code"
    },
    "destinationAirport": {
      "dataType": { "type": "string" },
      "description": "Destination airport code"
    },
    "originLocation": {
      "dataType": { "type": "geopoint" },
      "description": "Origin airport coordinates"
    },
    "tags": {
      "dataType": {
        "type": "array",
        "itemType": { "type": "string" }
      },
      "description": "Flight tags"
    }
  },
  "rid": "ri.ontology.main.object-type.abc123",
  "status": "ACTIVE"
}
```

**Property definition schema**:

Each property in the `properties` map has:

| Field | Type | Description |
|---|---|---|
| `dataType` | object | Type definition (see Data Types section below) |
| `description` | string | Property description |

**Connector behavior**:
- Reads `primaryKey` to determine primary key field(s) for metadata.
- Iterates `properties` to build a Spark `StructType` schema, mapping each property's `dataType` to a Spark type.
- Schema and metadata results are cached in `_schema_cache` and `_metadata_cache`.


### 4. Load Object Set (Fetch Data)

**Endpoint**: `POST /api/v2/ontologies/{ontologyApiName}/objectSets/loadObjects?snapshot=true`

**Used by**: `read_table()` → `_fetch_page()`

**Purpose**: Load object instances (records) with snapshot-consistent pagination. The `snapshot=true` parameter freezes data at a point-in-time when pagination starts, ensuring no duplicates or missing records across pages — critical for large datasets where pagination takes minutes.

**Path parameters**:

| Parameter | Type | Description |
|---|---|---|
| `ontologyApiName` | string | API name of the target ontology |

**Query parameters**:

| Parameter | Type | Description |
|---|---|---|
| `snapshot` | boolean | When `true`, enables consistent point-in-time pagination |

**Request body (JSON)**:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `objectSet` | object | — | Object set definition (base or filtered) |
| `pageSize` | integer | 1000 | Number of objects per page (max 10,000) |
| `pageToken` | string | — | Pagination token from previous response |

**ObjectSet definitions**:

Base (all objects):
```json
{"type": "base", "objectType": "ExampleFlight"}
```

Filtered (CDC incremental — only new records):
```json
{
  "type": "filter",
  "objectSet": {"type": "base", "objectType": "ExampleFlight"},
  "where": {"type": "gt", "field": "arrivalTimestamp", "value": "2024-01-01T15:35:00Z"}
}
```

**Example request** (snapshot, all records):

```bash
curl -X POST \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"objectSet": {"type": "base", "objectType": "ExampleFlight"}, "pageSize": 10000}' \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies/ontology-282f1207/objectSets/loadObjects?snapshot=true"
```

**Example request** (CDC incremental with where filter):

```bash
curl -X POST \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "objectSet": {
      "type": "filter",
      "objectSet": {"type": "base", "objectType": "ExampleFlight"},
      "where": {"type": "gt", "field": "arrivalTimestamp", "value": "2024-01-01T15:35:00Z"}
    },
    "pageSize": 10000
  }' \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies/ontology-282f1207/objectSets/loadObjects?snapshot=true"
```

**Example response**:

```json
{
  "nextPageToken": "v1.eyJsaW1pdCI6MTAsIm9mZnNldCI6MTB9",
  "totalCount": "1660475",
  "data": [
    {
      "flightId": "FL-2024-001",
      "flightNumber": "AA123",
      "arrivalTimestamp": "2024-01-15T11:45:00Z",
      "originAirport": "SFO",
      "destinationAirport": "JFK"
    }
  ]
}
```

**Response schema**:

| Field | Type | Description |
|---|---|---|
| `data` | array | List of object instances (records) |
| `nextPageToken` | string or null | Token for next page; `null` when no more pages |
| `totalCount` | string | Total number of matching objects |

**Pagination behavior**:
- `snapshot=true` freezes data at the start of pagination — all pages read from the same consistent state.
- Pass `nextPageToken` from the response as `pageToken` in the next request body.
- When `nextPageToken` is `null` or absent, all pages have been consumed.
- No cross-page limit (unlike the GET list endpoint which caps at 10K).

**Connector behavior**:
- Uses a generator to yield records page by page, keeping only one page (~10K records) in memory at a time. Avoids OOM on large datasets.
- In snapshot mode: base objectSet, pages through all objects.
- In CDC mode: filtered objectSet with `where: gt` on cursor field. On incremental runs, the API only returns records newer than the checkpoint — no full scan needed.
- Retries transient errors (429, 503, network timeouts) with exponential backoff up to 5 attempts.
- 0.1-second delay between page fetches for rate-limit safety.
- `pageSize` is capped at 10,000.


### 5. Aggregate Object Set (Max Cursor Lookup)

**Endpoint**: `POST /api/v2/ontologies/{ontologyApiName}/objectSets/aggregate`

**Used by**: `_get_max_cursor_value()` (primary method)

**Purpose**: Compute aggregate metrics (max, count, min, etc.) over an object set without fetching any records. Used to efficiently determine the current max cursor value for CDC checkpointing.

**Example request**:

```bash
curl -X POST \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "objectSet": {"type": "base", "objectType": "ExampleFlight"},
    "aggregation": [
      {"type": "max", "field": "arrivalTimestamp", "name": "max_cursor"},
      {"type": "count", "name": "total"}
    ],
    "groupBy": []
  }' \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies/ontology-282f1207/objectSets/aggregate"
```

**Example response**:

```json
{
  "accuracy": "ACCURATE",
  "data": [
    {
      "group": {},
      "metrics": [
        {"name": "total", "value": 1660475.0},
        {"name": "max_cursor", "value": "2024-01-01T15:35:00Z"}
      ]
    }
  ]
}
```

**Connector behavior**:
- Calls aggregate first (lightweight — no records fetched).
- If aggregate fails (some object types return `INTERNAL` error), falls back to the search endpoint with `orderBy desc, pageSize=1`.
- Result is compared against the checkpoint: if unchanged, returns immediately with no data fetch.


### 6. Search Objects (Max Cursor Fallback)

**Endpoint**: `POST /api/v2/ontologies/{ontologyApiName}/objects/{objectTypeApiName}/search`

**Used by**: `_get_max_cursor_value()` (fallback when aggregate fails)

**Purpose**: Fallback method to get the max cursor value by fetching a single record sorted descending.

**Example request**:

```bash
curl -X POST \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"orderBy": {"fields": [{"field": "arrivalTimestamp", "direction": "desc"}]}, "pageSize": 1}' \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies/ontology-282f1207/objects/ExampleFlight/search"
```

**Supported where clause filter types** (for reference):

| Type | Description | Example |
|---|---|---|
| `gt` | Greater than | `{"type": "gt", "field": "ts", "value": "2024-01-01"}` |
| `gte` | Greater than or equal | `{"type": "gte", "field": "ts", "value": "2024-01-01"}` |
| `lt` | Less than | `{"type": "lt", "field": "ts", "value": "2024-12-31"}` |
| `lte` | Less than or equal | `{"type": "lte", "field": "ts", "value": "2024-12-31"}` |
| `eq` | Equals | `{"type": "eq", "field": "status", "value": "active"}` |
| `isNull` | Is null check | `{"type": "isNull", "field": "name", "value": true}` |
| `and` | Logical AND | `{"type": "and", "value": [filter1, filter2]}` |
| `or` | Logical OR | `{"type": "or", "value": [filter1, filter2]}` |
| `not` | Logical NOT | `{"type": "not", "value": filter}` |


## **Data Types**

The Palantir Ontology API defines property data types in the `dataType` field of each property definition. The connector maps these to Spark types as follows:

### Primitive Types

| Palantir `dataType.type` | Spark Type | Notes |
|---|---|---|
| `string` | `StringType` | |
| `integer` | `LongType` | Mapped to 64-bit for safety |
| `long` | `LongType` | |
| `double` | `DoubleType` | |
| `float` | `DoubleType` | Promoted to double |
| `boolean` | `BooleanType` | |
| `timestamp` | `StringType` | Preserved as ISO 8601 string |
| `date` | `StringType` | Preserved as ISO date string |
| `datetime` | `StringType` | Preserved as ISO 8601 string |
| `decimal` | `DoubleType` | Simplified for MVP |
| `attachment` | `StringType` | JSON string representation |

### Complex Types

**Geopoint**:

```json
{ "type": "geopoint" }
```

Mapped to Spark `StructType`:
```
StructType([
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType)
])
```

**Array**:

```json
{
  "type": "array",
  "itemType": { "type": "string" }
}
```

Mapped to Spark `ArrayType` with recursive element type mapping.

**Struct**:

```json
{
  "type": "struct",
  "properties": {
    "fieldName": {
      "dataType": { "type": "string" }
    }
  }
}
```

Mapped to Spark `StructType` with recursive field mapping.


## **Ingestion Modes**

The connector supports two ingestion strategies based on whether a `cursor_field` is configured:

### Snapshot Mode (Default)

- **Triggered when**: No `cursor_field` specified in table options.
- **Behavior**: Full refresh — reads all objects from the API on each sync via a memory-efficient generator.
- **Offset tracking**: Returns a sentinel offset (`{"done": "true"}`) after consuming all pages.
- **Metadata**: `ingestion_type: "snapshot"`
- **Memory**: Only one page (~10K records) is held in memory at a time.

### Incremental / CDC Mode

- **Triggered when**: `cursor_field` is specified in table options (e.g., `"updatedAt"`, `"arrivalTimestamp"`).
- **Behavior**:
  1. Pre-computes the current max cursor value via aggregate endpoint (falls back to orderBy desc if aggregate fails).
  2. If max hasn't changed since last checkpoint → returns empty iterator immediately (no API calls for data).
  3. On first run (no checkpoint): fetches all records via `loadObjectSet` with base objectSet (full load).
  4. On incremental runs: fetches only new records via `loadObjectSet` with filtered objectSet (`where: gt` on cursor field). The API performs server-side filtering — only matching records are returned.
- **Offset tracking**: Checkpoints `{"max_cursor_value": <max_value>}`. On next run, this becomes the filter baseline.
- **Metadata**: `ingestion_type: "cdc"`
- **Note**: Records with null cursor field values are excluded from ingestion.


## **Error Handling**

All Palantir API errors follow a consistent JSON structure:

```json
{
  "errorCode": "NOT_FOUND",
  "errorName": "ObjectTypeNotFound",
  "errorInstanceId": "00813215-0844-4716-be7b-a3fe0fce9e42",
  "parameters": {
    "objectType": "NonExistentType"
  }
}
```

### HTTP Status Codes

| Status | Meaning | Connector Behavior |
|---|---|---|
| 200 | Success | Process response |
| 400 | Bad request (invalid parameters) | Raise exception |
| 401 | Authentication failure (invalid/expired token) | Raise exception |
| 403 | Insufficient permissions | Raise exception |
| 404 | Object type or ontology not found | Raise exception |
| 429 | Rate limit exceeded | Retry with exponential backoff (up to 5 attempts) |
| 500 | Server error | Raise exception |
| 503 | Service unavailable | Retry with exponential backoff (up to 5 attempts) |

### Relevant Error Codes

| Error Code | When It Occurs |
|---|---|
| `ObjectTypeNotFound` | Object type API name doesn't exist in ontology |
| `OntologyNotFound` | Ontology API name is invalid |
| `PERMISSION_DENIED` | Token lacks `api:ontologies-read` scope |
| `UNAUTHORIZED` | Token is invalid or expired |
| `InvalidPageSize` | `pageSize` exceeds maximum (10,000 for search) |
| `OntologySyncingObjectTypes` | Object type is still being indexed (409 Conflict) |


## **Pagination Summary**

| Aspect | Detail |
|---|---|
| Endpoint | `POST /objectSets/loadObjects?snapshot=true` |
| Mechanism | Cursor-based via `pageToken` / `nextPageToken` in POST body |
| Consistency | `snapshot=true` — frozen point-in-time across all pages |
| Page size parameter | `pageSize` (default 1,000, max 10,000) |
| End-of-data signal | `nextPageToken` is `null` or absent |
| Cross-page limit | None (unlike GET list endpoint which caps at 10K) |
| Memory efficiency | Generator-based — only one page in memory at a time |
| Error handling | Exponential backoff retry on 429/503 (up to 5 attempts) |


## **Rate Limits Summary**

| User Type | Request Limit | Concurrent Limit |
|---|---|---|
| Individual users | 5,000 requests/min | 30 simultaneous |
| Service users | No request limit | 800 simultaneous |

The connector retries 429/503 errors with exponential backoff (up to 5 attempts: 1s, 2s, 4s, 8s, 16s) and adds a 0.1-second delay between page fetches. For production use with large datasets, consider using a service user token for higher concurrency limits.


## **References**

- [Palantir Foundry API v2 — Introduction](https://www.palantir.com/docs/foundry/api/v2/general/overview/introduction/)
- [Palantir Foundry API v2 — Authentication](https://www.palantir.com/docs/foundry/api/v2/general/overview/authentication/)
- [Palantir Foundry API v2 — Pagination](https://www.palantir.com/docs/foundry/api/v2/general/overview/pagination/)
- [Palantir Foundry API v2 — Rate Limits](https://www.palantir.com/docs/foundry/api/v2/general/overview/rate-limits/)
- [Palantir Foundry API v2 — Ontology Objects](https://www.palantir.com/docs/foundry/api/v2/ontologies/ontology-objects/)
- [Palantir Platform Python SDK](https://github.com/palantir/foundry-platform-python)
