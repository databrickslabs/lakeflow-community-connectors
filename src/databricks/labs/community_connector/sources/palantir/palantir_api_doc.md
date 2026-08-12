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
- Exceeding rate limits returns `429` or `503`. The connector retries those statuses (and `ConnectionError` / `Timeout`) with **jittered exponential backoff** (base `2^attempt` seconds × uniform `[0.5, 1.5)`) up to 5 attempts. When the server sets a `Retry-After` header on 429/503, it is honoured as the base wait (with jitter still applied) — only the integer-seconds form (RFC 7231) is parsed; the HTTP-date form falls back to exponential backoff. Non-transient 4xx/5xx responses (e.g. `401` expired token, `404` wrong ontology) propagate on the first attempt so misconfiguration fails fast.


## **Object List**

For this connector, Palantir ontology object types are treated as **tables**. The object list is **dynamic** — discovered at runtime via the List Object Types API, not statically defined in the connector code.

| API Endpoint | Purpose | Connector Method |
|---|---|---|
| `GET /api/v2/ontologies/{ontology}/objectTypes` | List all object types — the response includes each type's full property definition, which the connector indexes by `apiName` and reuses for both `list_tables()` and per-type schema discovery (no per-type GET call is made) | `list_tables()`, `get_table_schema()`, `read_table_metadata()` |
| `POST /api/v2/ontologies/{ontology}/objectSets/loadObjects` | Load object instances with snapshot-consistent pagination | `read_table()` → `_fetch_page()` |
| `POST /api/v2/ontologies/{ontology}/objects/{objectType}/search` | Peek dataset max cursor for the incremental early-exit short-circuit (orderBy desc, pageSize=1) | `_get_max_cursor_value()` |

**Connector scope**:
- All object types in the configured ontology are available as tables.
- Each object type maps to one table, named by its `apiName`.
- The connector supports snapshot (full refresh) and CDC (incremental with server-side filtering) ingestion modes.
- Data is fetched via `loadObjectSet` with `snapshot=true` for consistent pagination across large datasets.
- CDC incremental runs use a server-side composite `(cursor_field, tiebreaker)` filter via objectSet composition.
- Max cursor lookup uses the search endpoint with `orderBy desc, pageSize=1`.
- Transient errors (429, 503, `ConnectionError`, `Timeout`) are retried with jittered exponential backoff up to 5 attempts; ``Retry-After`` is honoured on 429/503 when present.
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
      "apiName": "FlightsFinal",
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


### 3. Load Object Set (Fetch Data)

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
{"type": "base", "objectType": "FlightsFinal"}
```

Filtered (CDC incremental — only new records):
```json
{
  "type": "filter",
  "objectSet": {"type": "base", "objectType": "FlightsFinal"},
  "where": {"type": "gt", "field": "arrivalTimestamp", "value": "2024-01-01T15:35:00Z"}
}
```

**Example request** (snapshot, all records):

```bash
curl -X POST \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"objectSet": {"type": "base", "objectType": "FlightsFinal"}, "pageSize": 10000}' \
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
      "objectSet": {"type": "base", "objectType": "FlightsFinal"},
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

**System fields** — every record also carries four `__`-prefixed system fields that are **not** part of the object type's declared `properties` and are therefore **excluded from the connector's discovered schema** (the example above shows declared properties only):

| Field | Example | Notes |
|---|---|---|
| `__rid` | `ri.phonograph2-objects.main.object.v4.…` | Stable, globally-unique object Resource ID — the canonical identifier for joins/lineage |
| `__primaryKey` | `98d908cb…` | Foundry's internal primary-key value (used by the connector as the merge key **only** when the ontology declares no `primaryKey`) |
| `__apiName` | `FlightsFinal` | The object type's API name |
| `__title` | `98d908cb…` | The object's display title |

These cannot be used as a `cursor_field` or `tiebreaker_field`: the objectSet API rejects `__`-prefixed fields in `orderBy`/`where` with `PropertiesNotFound`. To ingest one (e.g. `__rid`), expose it as a declared property in the ontology or capture it downstream.

**Pagination behavior**:
- `snapshot=true` freezes data at the start of pagination — all pages read from the same consistent state.
- Pass `nextPageToken` from the response as `pageToken` in the next request body.
- When `nextPageToken` is `null` or absent, all pages have been consumed.
- No cross-page limit (unlike the GET list endpoint which caps at 10K).

**Connector behavior**:
- Uses a generator to yield records page by page, keeping only one page (~10K records) in memory at a time. Avoids OOM on large datasets.
- In snapshot mode: base objectSet, pages through all objects.
- In CDC mode: filtered objectSet with a composite `(cursor, tiebreaker)` filter — `cursor > prev` OR (`cursor == prev` AND `tiebreaker > prev_tiebreak`). On incremental runs the API only returns records after the checkpoint — no full scan needed.
- Retries transient errors (429, 503, `ConnectionError`, `Timeout`) with jittered exponential backoff up to 5 attempts. On 429/503, honours the `Retry-After` header as the base wait when present (integer-seconds form). Non-transient 4xx/5xx propagate immediately so misconfiguration (e.g. 401 expired token, 404 wrong ontology) surfaces without burning retry budget.
- `pageSize` is capped at 10,000.


### 4. Search Objects (Max Cursor Lookup)

**Endpoint**: `POST /api/v2/ontologies/{ontologyApiName}/objects/{objectTypeApiName}/search`

**Used by**: `_get_max_cursor_value()`

**Purpose**: Peek the dataset's current max cursor value (single record sorted descending on the cursor field) to drive the incremental early-exit short-circuit. The read is skipped (empty iterator, unchanged offset) **only when the peeked max is strictly *below* the checkpoint** — definitively nothing new. An *equal* max may still hide un-read rows that share that cursor value (resolved by the composite tiebreaker on the read), so the read proceeds. The checkpoint itself is set from the last emitted record — this endpoint is an optimization, not the checkpointing mechanism.

> **Note**: An earlier version of this connector tried the `/objectSets/aggregate` endpoint first as a "lightweight max-cursor lookup" and fell back to `search`. Palantir returns HTTP 500 for `aggregate` against the object types we use, so the fallback path was the only one that ever produced a result. The aggregate code path was removed to skip the doomed HTTP round-trip on every CDC tick.

**Example request**:

```bash
curl -X POST \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"orderBy": {"fields": [{"field": "arrivalTimestamp", "direction": "desc"}]}, "pageSize": 1}' \
  "https://yourcompany.palantirfoundry.com/api/v2/ontologies/ontology-282f1207/objects/FlightsFinal/search"
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
| `decimal` | `DecimalType(precision, scale)` | Precision/scale forwarded from Palantir; clamped to Spark's max precision of 38; defaults to `(38, 18)` when unspecified |
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
- **Offset tracking**: Returns an empty offset (`{}`) — snapshot is a single framework-driven pass with no resume point (see the README note on mid-snapshot restart cost).
- **Metadata**: `ingestion_type: "snapshot"`
- **Memory**: Only one page (~10K records) is held in memory at a time.

### Incremental / CDC Mode

- **Triggered when**: `cursor_field` is specified in table options (e.g., `"updatedAt"`, `"arrivalTimestamp"`).
- **Behavior**:
  1. On first run (no checkpoint): fetches all records via `loadObjectSet` with the base objectSet (full load), sorted ASC by `(cursor_field, tiebreaker)`.
  2. On subsequent runs: peeks the dataset's current max cursor via the search endpoint (`orderBy desc, pageSize=1`). The early-exit short-circuits only when the peeked max is strictly *below* the checkpoint — an *equal* max may hide un-read rows sharing that cursor value, so the read proceeds.
  3. Otherwise: fetches only new records via `loadObjectSet` with a **composite filter** — `cursor > prev` OR (`cursor == prev` AND `tiebreaker > prev_tiebreak`) — sorted ASC by `(cursor_field, tiebreaker)`. This total ordering means rows sharing a `cursor_field` value are never skipped or duplicated across a batch boundary.
- **Tiebreaker**: a unique, server-sortable property used as the secondary sort/filter key. Resolves to the `tiebreaker_field` table option, else the object type's single-column primary key. If neither resolves (a multi-column primary key, or no primary key), incremental reads are **refused with an actionable error** (set `tiebreaker_field`, or run the table in snapshot mode).
- **Offset tracking**: Checkpoints `{"max_cursor_value": <cursor>, "max_tiebreak_value": <tiebreaker>}` from the last *emitted* record (not the dataset max), so admission-capped batches resume exactly past the last `(cursor, tiebreaker)` tuple without skipping records. `max_tiebreak_value` is present on every incremental read that emits at least one record (a tiebreaker always resolves, or the read is refused); a no-progress batch that emits zero records may carry only `max_cursor_value`.
- **Metadata**: `ingestion_type: "cdc"`
- **Note**: Records with a null `cursor_field` value are excluded from ingestion. A null *tiebreaker* value is rejected (a tiebreaker must be unique and non-null). If the search peek fails (4xx/5xx/network), the early-exit is skipped and the connector falls through to the composite read so it still completes.


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
| 429 | Rate limit exceeded | Retry with jittered exponential backoff, honouring `Retry-After` header when present (up to 5 attempts) |
| 500 | Server error | Raise exception |
| 503 | Service unavailable | Retry with jittered exponential backoff, honouring `Retry-After` header when present (up to 5 attempts) |

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
| Error handling | Jittered exponential backoff retry on 429/503/network errors (up to 5 attempts); honours `Retry-After` on 429/503 |


## **Rate Limits Summary**

| User Type | Request Limit | Concurrent Limit |
|---|---|---|
| Individual users | 5,000 requests/min | 30 simultaneous |
| Service users | No request limit | 800 simultaneous |

The connector retries 429/503 errors and `ConnectionError`/`Timeout` with jittered exponential backoff up to 5 attempts. Base waits are `2^attempt` seconds (1, 2, 4, 8, 16) with a uniform `[0.5, 1.5)` jitter multiplier applied to each — so the actual sleeps fall in `[0.5, 1.5)`, `[1, 3)`, `[2, 6)`, `[4, 12)`, `[8, 24)` seconds. Jitter decorrelates concurrent Spark tasks so a rate-limit storm doesn't have every task retry in lockstep. On 429/503 the `Retry-After` header (integer-seconds form per RFC 7231) is honoured as the base wait when present, with jitter still applied; the HTTP-date form is not parsed and falls back to exponential backoff. Non-transient 4xx/5xx (e.g. 401, 404) propagate on the first attempt. For production use with large datasets, consider using a service user token for higher concurrency limits.


## **References**

- [Palantir Foundry API v2 — Introduction](https://www.palantir.com/docs/foundry/api/v2/general/overview/introduction/)
- [Palantir Foundry API v2 — Authentication](https://www.palantir.com/docs/foundry/api/v2/general/overview/authentication/)
- [Palantir Foundry API v2 — Pagination](https://www.palantir.com/docs/foundry/api/v2/general/overview/pagination/)
- [Palantir Foundry API v2 — Rate Limits](https://www.palantir.com/docs/foundry/api/v2/general/overview/rate-limits/)
- [Palantir Foundry API v2 — Ontology Objects](https://www.palantir.com/docs/foundry/api/v2/ontologies/ontology-objects/)
- [Palantir Platform Python SDK](https://github.com/palantir/foundry-platform-python)
