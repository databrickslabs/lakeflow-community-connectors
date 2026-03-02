# Example Source API Documentation

## Authorization

- **Chosen method**: Username / Password passed at client construction.
- **Auth placement**: Constructor arguments `username` and `password`.
- **Obtaining credentials**: Any non-empty strings work. For testing, use `"test_user"` / `"test_pass"`.

### Connection Parameters 

| Parameter  | Type   | Required | Default | Description                                    |
|------------|--------|----------|---------|------------------------------------------------|
| `username` | string | ✅ Yes   | —       | Non-empty string. Any value accepted.          |
| `password` | string | ✅ Yes   | —       | Non-empty string. Any value accepted.          |

**Example client construction**:
```python
from databricks.labs.community_connector.libs.simulated_source.api import get_api

api = get_api("test_user", "test_pass")
resp = api.get("/tables")
tables = resp.json()["tables"]   # ["products", "events", "users", "orders"]
```

**Notes**:
- `get_api()` returns a module-level singleton; `reset_api()` replaces it (use between test runs).
- The API might have receive retriable errors with status codes: `429`, `500`, `503`. 
- All responses expose `.status_code` (int) and `.json()` (dict), mirroring `requests.Response`.

---

## Object List

Discoverable tables are retrieved via `GET /tables`, which returns a JSON object with a `"tables"` key containing a list of table name strings. 

**Parameters**: None

**Response** (`200 OK`):
```json
{
  "tables": ["products", "events", "users", "orders"]
}
```

In addition to the discoverable tables, there is one hidden table:

| Table Name | Description | Ingestion Type | Cursor Field | Primary Key |
|------------|-------------|----------------|--------------|-------------|
| `metrics` | *(hidden)* Time-series metrics — not discoverable | `cdc` | `updated_at` (timestamp) | `metric_id` |

The `metrics` table does not appear in `GET /tables`, and its `/schema` and `/metadata` endpoints return 404. 

---

## Object Schema

### Retrieving Schemas via the API

For any discoverable table, retrieve its column definitions with `GET /tables/{table}/schema`. This returns a list of field descriptors, each containing `name`, `type`, `nullable`, and optionally `fields` (for `struct` types).

Pair this with `GET /tables/{table}/metadata` to get `primary_keys` and `cursor_field` for the same table.

**Field descriptor shape**:

| Key        | Type    | Description |
|------------|---------|-------------|
| `name`     | string  | Column name |
| `type`     | string  | Data type: `string`, `integer`, `double`, `timestamp`, `date`, `struct` |
| `nullable` | boolean | Whether the field can be null |
| `fields`   | list    | Present only for `struct` type; contains nested field descriptors with the same shape |

#### Example: orders schema

**Request**:
```
GET /tables/orders/schema
```

**Response** (`200 OK`):
```json
{
  "schema": [
    {"name": "order_id",   "type": "string",    "nullable": false},
    {"name": "user_id",    "type": "string",    "nullable": true},
    {"name": "amount",     "type": "double",    "nullable": true},
    {"name": "status",     "type": "string",    "nullable": true},
    {"name": "updated_at", "type": "timestamp", "nullable": true}
  ]
}
```

#### Example: orders metadata

**Request**:
```
GET /tables/orders/metadata
```

**Response** (`200 OK`):
```json
{
  "metadata": {
    "primary_keys": ["order_id"],
    "cursor_field": "updated_at"
  }
}
```

#### Python example: discovering all schemas

```python
api = get_api("test_user", "test_pass")

tables = api.get("/tables").json()["tables"]
for table in tables:
    schema = api.get(f"/tables/{table}/schema").json()["schema"]
    metadata = api.get(f"/tables/{table}/metadata").json()["metadata"]
    print(f"{table}: {len(schema)} fields, metadata={metadata}")
```

### Hidden table: metrics (static schema)

The `metrics` table is not discoverable — `GET /tables/metrics/schema` and `GET /tables/metrics/metadata` both return `404`. The connector must hard-code its schema. The field definitions are:

| Field | Type | Nullable | Notes |
|-------|------|----------|-------|
| `metric_id` | string | No | Primary key. Format: `metric_NNNN`. |
| `name` | string | Yes | Dot-separated metric name, e.g. `metric.cpu`. |
| `value` | struct | Yes | Nested struct with sub-fields below. |
| `value.count` | integer | Yes | Integer sub-field of `value`. |
| `value.label` | string | Yes | One of: `low`, `medium`, `high`, `critical`. |
| `value.measure` | double | Yes | Float sub-field of `value`. |
| `host` | string | Yes | Host identifier, e.g. `host-0`. |
| `updated_at` | timestamp | No | Cursor field. ISO 8601 with UTC offset. |

**Metadata** (not available via API, must be hard-coded):
- `primary_keys`: `["metric_id"]`
- `cursor_field`: `"updated_at"`

---

## Object Ingestion Type

The API does not expose ingestion type directly. The connector must determine it using the following rules:

| Table | Ingestion Type | Rule |
|-------|---------------|------|
| `metrics` | `cdc` | Hard-coded — hidden table, always CDC. |
| `events` | `append_only` | Hard-coded — append-only event log, no updates or deletes. |
| `orders` | `cdc_with_deletes` | Hard-coded — supports `deleted_records` endpoint. |
| *(any other)* | `cdc` | If `cursor_field` is present in metadata. |
| *(any other)* | `snapshot` | If `cursor_field` is absent from metadata. |

**Resolution logic** (applied in order):

1. If the table is `metrics`, use `cdc`.
2. If the table is `events`, use `append_only`.
3. If the table is `orders`, use `cdc_with_deletes`.
4. Otherwise, call `GET /tables/{table}/metadata` and check for `cursor_field`:
   - Present → `cdc`
   - Absent → `snapshot`

---

## Read API for Data Retrieval

### GET /tables/{table}/records

List records for a table. Supported query params vary by table.

#### Pagination

All tables support a `page` query parameter (integer, 1-based). When omitted, the request starts from page 1. Each table has a `max_page_size` that caps records per page. The response always includes a `next_page` field: an integer for the next page number when more records exist, or `null` when the current page is the last.

The `page` value is replayable — the same page number returns the same data slice (assuming no mutations between calls).

#### Per-table query parameters

| Table | Supported Params | Notes |
|-------|-----------------|-------|
| `products` | `category`, `page` | Exact-match filter. No cursor, always full refresh. |
| `events` | `since`, `limit`, `page` | Cursor-based. `since` is exclusive lower bound on `created_at`. Default `limit`=`max_page_size`. |
| `users` | `since`, `page` | Cursor-based. `since` is exclusive lower bound on `updated_at`. |
| `orders` | `since`, `user_id`, `status`, `page` | Cursor-based + exact-match filters. |
| `metrics` *(hidden)* | `since`, `until`, `page` | Time-range query. `since` exclusive, `until` inclusive on `updated_at`. |

**`since` semantics**: records with `cursor_field > since` (strictly greater than).
**`until` semantics**: records with `cursor_field <= until` (inclusive).
**`limit`**: integer string; default equals `max_page_size`. Only supported on `events`.
**`page`**: integer string; 1-based page number. Default `1`.
Unsupported params return `400`.

**Response** (`200 OK`):
```json
{
  "records": [ { ... }, { ... } ],
  "next_page": 2
}
```

When on the last page:
```json
{
  "records": [ { ... } ],
  "next_page": null
}
```

Records are sorted by `cursor_field` (if present), ascending.

**Errors**:
- `400` — unsupported query parameter for this table, or `page < 1`.
- `404` — unknown table.

#### Python read example (paginated incremental)

```python
# Paginated incremental read for orders
since = last_checkpoint  # ISO timestamp string or None

params = {"since": since} if since else {}
page = 1

while True:
    params["page"] = str(page)
    resp = api.get("/tables/orders/records", params=params)

    if resp.status_code in (429, 500, 503):
        time.sleep(backoff)
        continue

    body = resp.json()
    records = body["records"]
    if not records:
        break

    process(records)
    since = records[-1]["updated_at"]  # advance cursor

    if body["next_page"] is None:
        break
    page = body["next_page"]
```

---

### GET /tables/{table}/deleted_records

Return deleted record tombstones. Only supported by `orders`.

**Path params**: `table` — must be `orders`; other tables return `400`.

**Supported query params** (`orders` only):

| Param | Description |
|-------|-------------|
| `since` | Exclusive lower bound on `updated_at` (tombstone timestamp). |
| `page` | Integer, 1-based page number. Default `1`. |

Pagination uses the same `max_page_size` as the records endpoint (40 for orders). Response includes `next_page` (int or null).

**Response** (`200 OK`):
```json
{
  "records": [
    {
      "order_id": "order_0001",
      "user_id": null,
      "amount": null,
      "status": null,
      "updated_at": "2024-01-01T12:00:00+00:00"
    }
  ],
  "next_page": null
}
```

Tombstone shape: all non-PK fields are set to `null`; only the primary key and `updated_at` are populated.

**Errors**:
- `400` — table does not support deleted records, or unsupported query param.
- `404` — unknown table.

---

## Error Handling

| Status | Meaning | Action |
|--------|---------|--------|
| `200` | Success (reads, upserts, deletes) | Process response. |
| `201` | Created (insert into tables without cursor) | Process response. |
| `400` | Bad request (unsupported param, operation not allowed) | Do not retry; fix the request. |
| `404` | Not found (unknown table, missing record, hidden table) | Do not retry. |
| `429` | Rate limit exceeded | Retry with exponential backoff. |
| `500` | Internal server error | Retry with exponential backoff. |
| `503` | Service unavailable | Retry with exponential backoff. |

Error response body always contains `{"error": "<message>"}`.

---

## Field Type Mapping

| API Field Type | Spark Data Type |
|---------------|-----------------|
| `string` | `StringType` |
| `integer` | `LongType` |
| `double` | `DoubleType` |
| `timestamp` | `TimestampType` |
| `date` | `DateType` |
| `struct` | `StructType` (recurse into `fields` to build nested `StructField` list) |
