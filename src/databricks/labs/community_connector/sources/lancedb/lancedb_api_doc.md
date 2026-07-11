# LanceDB Cloud API Documentation

## Authorization

- **Chosen method**: API key via HTTP header. LanceDB Cloud's primary/documented mechanism is the `x-api-key` header. (LanceDB Enterprise additionally supports OAuth 2.0 client-credentials and Bearer-token auth, and the generic Lance REST Namespace spec accepts credentials either as the `x-api-key`/`Authorization: Bearer` header **or** inline in the JSON request body under `identity.api_key` / `identity.auth_token` — the connector should use the header form only, per the "single auth method" principle.)
- **Auth placement**: HTTP header `x-api-key: <API_KEY>` on every request.
- **Host/region placement**: Not a header — encoded directly into the base URL hostname (see below). LanceDB Cloud does not use a single shared host; each database ("project") + region pair maps to its own subdomain.

### Connection Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_key` | string | Yes | LanceDB Cloud API key, generated in the Cloud console under Settings → API Keys. | `sk_abc123...` |
| `project_name` (a.k.a. "database name") | string | Yes | The database/project identifier shown in the LanceDB Cloud dashboard. Used as the first host-name label. | `my-lancedb-project` |
| `region` | string | Yes | Cloud region where the project is hosted, shown next to the project name in the dashboard. Used as the second host-name label. | `us-east-1` |

### Base URL construction

```
https://{project_name}.{region}.api.lancedb.com/v1
```

Example: `https://my-lancedb-project.us-east-1.api.lancedb.com/v1`

This matches the `lancedb.connect(uri="db://...", api_key=..., region=...)` pattern used by the official SDKs — the SDK resolves the same per-project/region host internally. `project_name` and `region` must be sanitized (alphanumeric, hyphen, underscore only) before being interpolated into the URL to avoid host-header/URL injection, since they come directly from connector configuration.

### Example request

```
GET https://my-lancedb-project.us-east-1.api.lancedb.com/v1/table/ HTTP/1.1
x-api-key: sk_abc123...
Content-Type: application/json
Accept: application/json
```

```python
import requests

session = requests.Session()
session.headers.update({
    "x-api-key": "sk_abc123...",
    "Content-Type": "application/json",
    "Accept": "application/json",
})
resp = session.get("https://my-lancedb-project.us-east-1.api.lancedb.com/v1/table/", params={"limit": 100})
tables = resp.json()["tables"]
```

**Notes**:
- No OAuth flow or token exchange is needed for LanceDB Cloud API-key auth; the key is a long-lived shared secret used directly on every call.
- An optional `x-lancedb-database` header exists in the generic Lance REST Namespace spec for deployments where the database is not already encoded in the hostname; it is **not needed** for LanceDB Cloud because the database (project) is already part of the per-project subdomain.
- Handle `401` (invalid/revoked key) and `403` (insufficient permission) as non-retryable auth errors.
- Handle `429` with exponential backoff, honoring `Retry-After` if present.

---

## Object List

LanceDB tables are **user-defined and fully dynamic** — there is no static/fixed table list. Every "object" the connector can expose is discovered at runtime via the List Tables endpoint. Treat table discovery as the source of truth on every sync (or at least once per pipeline run), since users can create/drop LanceDB tables outside of the connector.

### Endpoint: List Tables

- **Method**: `GET`
- **Path**: `/v1/table/` (trailing slash observed in the production implementation; the generic spec documents it without the trailing slash as `/v1/table`, both resolve to the same route)
- **Auth**: `x-api-key` header
- **Query parameters**:

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `limit` | integer | No | Maximum number of table names to return in one page. No documented default/maximum was found in current public docs; the reference implementation uses `100`. `TBD: exact default/max — not published; treat 100 as a safe, previously-validated value.` |
| `page_token` | string | No | Opaque cursor from the previous response's `page_token`. Omit on the first call. |

**Example request**:
```
GET /v1/table/?limit=100 HTTP/1.1
Host: my-lancedb-project.us-east-1.api.lancedb.com
x-api-key: sk_abc123...
```

**Example response** (`200 OK`):
```json
{
  "tables": ["documents", "embeddings", "user_vectors"],
  "page_token": null
}
```

- `tables` is a list of plain table-name strings for a LanceDB Cloud project without namespaces. (The generic, namespace-aware Lance REST Namespace spec can return namespace-qualified names like `"namespace1$table1"` when namespaces are used; LanceDB Cloud's default/root namespace returns bare table names as shown above and as observed in the production reference implementation.)
- **Pagination**: when `page_token` in the response is non-null, issue another `GET /v1/table/` with `page_token` set to that value (and the same `limit`) to fetch the next page. Stop when `page_token` is `null`/absent. Accumulate `tables` across all pages to get the full list.
- No documented ordering guarantee (alphabetical vs. creation order) was found; do not rely on sort order for incremental table-discovery diffing.

### Read model

Each discovered table name is exposed as one Lakeflow "object"/table. There is no nesting — LanceDB tables are flat, top-level collections (rows with a fixed Arrow schema, one of whose columns is typically a fixed-size vector/embedding column). There are no sub-objects or child tables to enumerate separately.

---

## Object Schema

### Endpoint: Describe Table (schema)

- **Method**: `POST`
- **Path**: `/v1/table/{table_name}/describe/` (URL-encode `table_name`)
- **Auth**: `x-api-key` header
- **Request body**: `{}` is sufficient to get the current-version schema. The generic spec also accepts optional fields (`version`, `tag`, `branch`, `load_detailed_metadata`, `with_table_uri`) but none are required for basic schema retrieval.

**Example request**:
```
POST /v1/table/documents/describe/ HTTP/1.1
Host: my-lancedb-project.us-east-1.api.lancedb.com
x-api-key: sk_abc123...
Content-Type: application/json

{}
```

**Example response** (`200 OK`):
```json
{
  "table": "documents",
  "version": 1,
  "schema": {
    "fields": [
      {"name": "id", "type": {"type": "int64"}, "nullable": false},
      {"name": "text", "type": {"type": "string"}, "nullable": true},
      {"name": "embedding", "type": {"type": "fixed_size_list", "length": 384}, "nullable": true},
      {"name": "updated_at", "type": {"type": "timestamp"}, "nullable": true}
    ],
    "metadata": {}
  },
  "stats": {
    "num_deleted_rows": 0,
    "num_fragments": 5
  }
}
```

- `schema.fields[]` is the full column list — each entry has `name`, `type` (an object with a `type` discriminator, e.g. `int64`, `string`, `fixed_size_list` with a `length`, etc. — some SDK/tooling paths render `type` as a bare string like `"fixed_size_list<float32>[384]"` instead of the structured object; the connector should handle **both** shapes defensively), and `nullable`.
- **Vector dimension detection**: for a `fixed_size_list` field, the `length` (structured form) or the trailing `[N]` (string form) is the embedding dimension. This is how the connector determines the dummy-vector size needed for full-table-scan queries (see Read API below) without the caller having to know the embedding model's output dimension.
- This same endpoint doubles as the **metadata** source (see next section) — there is no separate "get metadata" endpoint.

### Python example: discovering schema for all tables

```python
tables = session.get(f"{base_url}/v1/table/", params={"limit": 100}).json()["tables"]
for name in tables:
    resp = session.post(f"{base_url}/v1/table/{name}/describe/", json={})
    fields = resp.json()["schema"]["fields"]
    print(name, [(f["name"], f["type"]) for f in fields])
```

---

## Get Object Primary Keys

LanceDB does support an **unenforced, single-column primary key** concept (used internally by `merge_insert`), but:

- It is a write-path/SDK concept (`set_unenforced_primary_key()` in the Python/JS SDKs), and
- The REST `describe` endpoint response **does not surface** which column (if any) was designated as the primary key — no `primary_key`/`pk` field was found in the schema or table-description response in current docs or in the production reference implementation.

**Conclusion**: the connector **cannot discover primary keys via the API**, so it uses LanceDB's guaranteed-unique **`_rowid`** system column (requested via `with_row_id: true` on every query) as the default merge/upsert key for `snapshot` and `cdc`. Users may override this with a `primary_keys` table option (validated as a list of column names) when their table has a natural key; it is not auto-detected.

`TBD: if LanceDB Cloud later exposes the unenforced primary key in the describe/table-details response, prefer that over user-supplied config.`

---

## Object's ingestion type

LanceDB Cloud tables are, from the API's perspective, **snapshot** sources by default:

- The `describe` endpoint returns no field indicating an incremental/cursor column — there is no built-in "updated since" concept exposed by the API itself.
- LanceDB does support table **versioning** (MVCC) and **time travel** (`version` param on `describe`/`query`), and soft-deletes (rows are tombstoned, not physically removed, until compaction) — but the REST API does not expose a change-feed / diff-between-versions endpoint. Consuming version deltas would require the connector to fetch two full versions and diff client-side, which is out of scope for a read connector and not implemented in the reference.
- Therefore:
  - **Default ingestion type: `snapshot`** — full-table read each run, unless the user configures a `cursor_field`.
  - **If the user supplies a `cursor_field`** (any column with mono­tonically increasing values, e.g. an `updated_at` timestamp or a version/sequence integer that the *application* writing to LanceDB maintains): the connector can approximate `cdc`-like incremental reads by appending a `filter` predicate `"{cursor_field} > '{last_cursor_value}'"` to the query request. This is an **append/upsert-only approximation** — it will pick up new rows and rows whose `cursor_field` was bumped on update, but it cannot detect deletes (no `cdc_with_deletes` support) and only works if the user's own write path reliably bumps that column.
  - **`append`** is a connector-level insert-only mode: it watermarks LanceDB's monotonic `_rowid` (`with_row_id`) and, each run, emits only rows whose `_rowid` exceeds the last checkpoint (no upsert key; inserts only, not updates/deletes). Suitable for immutable/append-only tables.
  - **No delete detection is possible** via this API — there is no tombstone/deleted-records endpoint exposed over REST. Ingestion type is never `cdc_with_deletes` for this source.

The connector selects the mode via an `ingestion_type` table option (default `snapshot`):

| `ingestion_type` | Behavior |
|-----------|---------------|
| `snapshot` (default) | Full-table read each run; upsert on `_rowid` (or user `primary_keys`) |
| `cdc` (requires `cursor_field`) | Incremental read where `cursor_field > last-synced`; upsert; no delete feed |
| `append` | Insert-only; watermarked on `_rowid`; captures inserts only |

---

## Read API for Data Retrieval

### Endpoint: Query a Table

- **Method**: `POST`
- **Path**: `/v1/table/{table_name}/query/` (URL-encode `table_name`)
- **Auth**: `x-api-key` header
- **Request Content-Type**: `application/json`
- **Response Content-Type**: `application/vnd.apache.arrow.file` (whole-table-query responses are served as an **Apache Arrow IPC** payload — in practice this may arrive as either the IPC *file* format or the IPC *streaming* format depending on server version/path; the connector should try `pyarrow.ipc.open_stream` first and fall back to `pyarrow.ipc.open_file` on failure, per the production reference implementation. A very small number of edge responses may fall back to plain JSON with a `data` array — check `Content-Type` before choosing a parser.)

#### Request body fields

| Field | Type | Required | Description |
|-------|------|----------|--------------|
| `vector` | object | **Yes** — LanceDB's query model is vector-first | `{"single_vector": [float, ...]}` or `{"multi_vector": [[float,...], ...]}`. For a non-similarity "full scan" read, supply a dummy all-zero vector whose length equals the table's vector-column dimension (discovered via `describe`, see above). |
| `k` | integer (≥0) | Yes | Number of nearest-neighbor results to return — LanceDB's REST API is fundamentally top-K; use this as the **batch/page size** for full scans (e.g. `k = batch_size`). |
| `columns` | object | No | `{"column_names": [...]}` for a plain projection, or `{"column_aliases": {alias: field_path}}` to rename. Used for column pruning/performance. |
| `filter` | string | No | SQL-like filter expression (Lance field syntax), e.g. `"price > 100 AND category = 'electronics'"`. Combine with a cursor predicate for incremental reads: `"{filter} AND {cursor_field} > '{last_value}'"`. |
| `vector_column` | string | No | Name of the vector column to search, if the table has more than one or auto-detection is undesired. |
| `distance_type` | string | No | `cosine`, `l2`, or `dot`. |
| `nprobes` | integer (≥0) | No | IVF index probe count (accuracy/speed tradeoff). |
| `ef` | integer (≥0) | No | HNSW search effort. |
| `refine_factor` | integer (≥0) | No | Re-ranking refinement multiplier. |
| `fast_search` | boolean | No | Enables an accelerated search path. |
| `bypass_vector_index` | boolean | No | Forces a full scan, ignoring any vector index — useful together with a dummy vector for pure table scans. |
| `prefilter` | boolean | No, default `true` recommended | Apply `filter` before the vector search rather than after; recommended for large tables to avoid over-fetching. |
| `lower_bound` / `upper_bound` | float | No | Bound the returned distance range. |
| `with_row_id` | boolean | No | Include a `_rowid` system column in results. |
| `offset` | integer (≥0) | No | Skip the first N results — used for **offset-based pagination** across pages within a single logical read. |
| `version` | integer (≥0) | No | Pin the query to a specific table version (time travel) instead of latest. |
| `branch` | string | No | Target branch name; defaults to `main`. Only relevant if the table uses Lance branching. |
| `full_text_query` | object | No | `{"string_query": "..."}` or a structured FTS query, for BM25/keyword search over indexed text columns. Not used by default table-scan reads. |

**Example — full table scan (batch read)**:
```
POST /v1/table/documents/query/ HTTP/1.1
Host: my-lancedb-project.us-east-1.api.lancedb.com
x-api-key: sk_abc123...
Content-Type: application/json

{
  "vector": {"single_vector": [0.0, 0.0, 0.0, 0.0]},
  "k": 1000,
  "bypass_vector_index": true,
  "offset": 0
}
```

**Example — vector similarity search**:
```json
{
  "vector": {"single_vector": [0.12, -0.03, 0.55, ...]},
  "k": 10,
  "distance_type": "cosine",
  "nprobes": 20,
  "refine_factor": 10
}
```

**Example — incremental read (cursor filter) with column projection**:
```json
{
  "vector": {"single_vector": [0.0, 0.0, 0.0, 0.0]},
  "k": 1000,
  "bypass_vector_index": true,
  "filter": "updated_at > '2026-07-01T00:00:00Z'",
  "columns": {"column_names": ["id", "text", "embedding", "updated_at"]},
  "prefilter": true
}
```

#### Response

`200 OK`, body = Arrow IPC binary. Decode with `pyarrow`:

```python
import io, pyarrow as pa

resp = session.post(url, json=query_payload)
try:
    table = pa.ipc.open_stream(io.BytesIO(resp.content)).read_all()
except Exception:
    table = pa.ipc.open_file(io.BytesIO(resp.content)).read_all()
records = table.to_pylist()   # list[dict]
```

Each returned row is a dict of column name → value, plus system columns when requested:
- `_distance` (float) — present whenever a real (non-dummy) `vector` similarity search is performed; indicates the query→row distance. Omit/ignore for full-scan reads with a dummy vector, or filter it out downstream if the schema doesn't declare it.
- `_rowid` (int64) — present only when `with_row_id: true`.

#### Pagination strategy for full-table reads

LanceDB's query API has no native "get next page" token; the connector must paginate manually:

1. Set `k = batch_size` (e.g. 1,000–10,000; hard practical ceiling ~10,000 rows/request based on production usage) and `offset = current_offset` (starting at `0`).
2. Issue the request, decode Arrow rows.
3. If the number of returned rows is `0` or less than `k`, treat this as the last page.
4. Otherwise, advance `offset += k` and repeat.
5. Track the connector's own pagination state as `{"offset": <int>, "cursor_value": <last-seen cursor column value or null>}` in the Lakeflow read-offset object.

**Column projection caveat**: some LanceDB Cloud API versions may not honor `columns` server-side and instead return all columns. The connector should defensively re-filter the decoded records to the requested column set client-side (a no-op if the server already filtered) — verified necessary by the production reference implementation and its accompanying test (`test_column_projection_api_support`).

#### Incremental read (cursor-based) — endpoint reuse

There is no separate "changes" endpoint. Incremental reads reuse the Query endpoint above with an appended `filter` predicate on the user-supplied `cursor_field`:

```
filter = "<cursor_field> > '<last_cursor_value>'"
```
(AND-combined with any user-supplied `filter_expression`.) The connector must track the maximum observed `cursor_field` value across all rows in a batch and persist it as the next run's `cursor_value`.

#### Deleted records

**Not supported.** LanceDB soft-deletes rows (tombstones them until `optimize`/compaction), but no REST endpoint exposes a deleted-rows/tombstone feed. Do not implement delete-sync for this source; ingestion type must never claim `cdc_with_deletes`.

#### Rate limits

No published rate-limit numbers were found in current public LanceDB Cloud documentation (checked docs.lancedb.com API reference, cloud FAQ, and enterprise auth pages). `TBD: rate limits are not publicly documented — implement generic 429-triggered exponential backoff (as the reference implementation does, honoring `Retry-After`) rather than a fixed request budget.` Treat `429` and `5xx` as retryable with exponential backoff; treat `400`/`401`/`403`/`404` as non-retryable.

#### Comparison of read approaches

| Approach | When to use | Trade-off |
|----------|-------------|-----------|
| Full scan (dummy vector + `bypass_vector_index: true` + `offset`/`k` pagination) | Default table sync / snapshot ingestion | Simple, complete; no vector-index acceleration benefit but not needed for a scan. |
| Real vector similarity search (`query_vector` + `k`) | User explicitly wants top-K nearest neighbors (rare for a lakehouse-offload use case, but supported) | Not a full-table read — returns only the closest K rows to the given vector, not suitable as the primary sync strategy. |
| Cursor filter on full scan | Incremental sync when a suitable monotonic column exists and is user-declared | Requires the user to know/declare a cursor column; LanceDB doesn't expose one automatically. |

---

## Field Type Mapping

The `describe` response's `schema.fields[].type` (Arrow-derived) maps to Spark types as follows. Types may arrive as a structured object (`{"type": "int64"}`, `{"type": "fixed_size_list", "length": N}`) or, on some paths, as a flattened string (e.g. `"fixed_size_list<float32>[384]"`); handle both.

| LanceDB / Arrow Type | Spark Data Type | Notes |
|-----------------------|------------------|-------|
| `string`, `utf8` | `StringType` | Text data. |
| `int32`, `int` | `IntegerType` | 32-bit integer. |
| `int64`, `long` | `LongType` | 64-bit integer. Commonly used for auto-increment-style IDs. |
| `float32`, `float` | `FloatType` | Single precision. |
| `float64`, `double` | `DoubleType` | Double precision. |
| `bool`, `boolean` | `BooleanType` | — |
| `binary` | `BinaryType` | Raw bytes (e.g. images/BLOBs). See ingestion-mode caveat below. |
| `date` | `DateType` | — |
| `timestamp` | `TimestampType` | — |
| `fixed_size_list` (with `length` = embedding dimension) | `ArrayType(FloatType)` (reference implementation maps this to `ArrayType(StringType)` generically — prefer `ArrayType(FloatType)`/`ArrayType(DoubleType)` when the inner element type is known, since these columns are numeric embedding vectors) | The **vector/embedding column**. `length` gives the fixed dimension, useful for constructing the dummy query vector for full scans. |
| `list` | `ArrayType` | Variable-length array; element type should be recursively mapped when available. |
| *(any unrecognized/unknown type)* | `StringType` | Safe fallback used by the reference implementation; log a warning when this path is hit so gaps can be tracked. |

### Special field behaviors

- `_distance` (float, query-response-only synthetic column): appended only when a genuine similarity vector is used; not part of the persisted table schema — exclude from the declared Spark schema unless `with_row_id`/similarity mode is explicitly requested by the user.
- `_rowid` (int64, query-response-only synthetic column, opt-in via `with_row_id`): likewise not part of the persisted schema.
- **Binary columns and CDC/SCD ingestion modes**: binary columns (images, large BLOBs) are safe under `APPEND_ONLY` ingestion but should be excluded via the `columns` table option under `SCD_TYPE_1`/`SCD_TYPE_2` ingestion, since CDC row-comparison on large binary payloads can exceed Databricks Serverless's per-UDF memory limit (1024 MB). This is an ingestion-pipeline concern, not an API constraint, but is important enough to flag here since it directly affects which `columns` a table-read should request.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|-----------------|------------|--------------------|
| Prior reference implementation | `lancedb.py` (1374-line prior connector, provided by user) | 2026-07-09 | High | Exact endpoint shapes (`GET /v1/table/`, `POST /v1/table/{name}/describe/`, `POST /v1/table/{name}/query/`), `x-api-key` header, base URL pattern, Arrow IPC parsing with dual stream/file fallback, dummy-vector full-scan strategy, vector-dimension auto-detection from `fixed_size_list.length`, column-projection fallback filtering, cursor-based incremental read via `filter` predicate, primary keys not exposed by API. |
| Prior reference implementation | `test_lancedb_lakeflow_connect.py` (provided by user) | 2026-07-09 | High | Confirms column-projection needs client-side fallback filtering was empirically required; confirms `_distance`/`_rowid` system columns; confirms offset-based pagination behavior and cursor/offset read-state shape. |
| Prior reference implementation | `README.md` (provided by user) | 2026-07-09 | High | Configuration parameters (`api_key`, `project_name`, `region`), full table-option catalog, binary-column/SCD memory-limit caveat, default `batch_size` of 1000 (max 10,000). |
| Official Docs | https://docs.lancedb.com/llms.txt | 2026-07-09 | High | Confirmed current canonical URLs for List Tables, Describe Table, and Query Table REST endpoints, plus authentication doc location. |
| Official Docs | https://docs.lancedb.com/api-reference/rest/table/list-all-tables.md | 2026-07-09 | High | `GET /v1/table` (also works with trailing slash), `limit`/`page_token`/`delimiter`/`include_declared` query params, response shape `{"tables": [...], "page_token": ...}`, auth options (OAuth2 / Bearer / `x-api-key`), error codes 400/401/403/503. No documented default/max `limit` or sort order found. |
| Official Docs | https://docs.lancedb.com/api-reference/rest/table/describe-information-of-a-table.md | 2026-07-09 | High | `POST /v1/table/{id}/describe`, request body optional fields (`version`, `tag`, `branch`, `load_detailed_metadata`, etc.), full response shape including `schema.fields[]` (`name`/`type`/`nullable`), `stats` (`num_deleted_rows`, `num_fragments`), confirms no primary-key field in response. |
| Official Docs | https://docs.lancedb.com/api-reference/rest/table/query-a-table.md | 2026-07-09 | High | `POST /v1/table/{id}/query`, full `QueryTableRequest` field list (`vector`, `k`, `columns`, `filter`, `distance_type`, `nprobes`, `ef`, `refine_factor`, `prefilter`, `offset`, `version`, `with_row_id`, `bypass_vector_index`, `fast_search`, `vector_column`, `full_text_query`, `lower_bound`/`upper_bound`, `branch`), response `Content-Type: application/vnd.apache.arrow.file`. |
| Official Docs | https://docs.lancedb.com/api-reference/rest/table/analyze-query-execution-plan.md | 2026-07-09 | Medium | Cross-confirmed the same request-body field list as the query endpoint (shared schema), confirming field names/types independently. |
| Official Docs | https://docs.lancedb.com/enterprise/authentication.md | 2026-07-09 | High | Confirms two supported auth modes (API key, OAuth 2.0) and that API-key auth is the simple/default path; confirms `x-api-key` header usage elsewhere in docs; Enterprise-specific host_override concept not applicable to Cloud. |
| Third-party (dltHub source docs) | https://dlthub.com/context/source/lancedb | 2026-07-09 | Medium | Independently confirmed base URL pattern `https://{db}.{region}.api.lancedb.com/v1` and `x-api-key` header requirement — cross-checked against reference implementation and official docs. |
| Community / technical article | https://fahadsid1770.medium.com/the-lancedb-administrators-handbook-... | 2026-07-09 | Low | Background confirmation of unenforced single-column primary key concept (`set_unenforced_primary_key`) and `merge_insert` semantics — used only to explain why primary keys aren't exposed via REST, not as a primary source for any endpoint shape. |
| Official Docs (via search snippet; direct fetch returned 404) | https://docs.lancedb.com/tables/update (Lance/LanceDB table update guide) | 2026-07-09 | Medium | Confirms soft-delete semantics (tombstone + compaction) and versioning (MVCC), supporting the "no delete feed over REST" conclusion. |

### Known gaps / TBD

- **Rate limits**: not published anywhere in current public LanceDB Cloud docs. Implement generic retry/backoff on `429`/`5xx` rather than a fixed quota.
- **`limit` default/max on List Tables**: not documented; the reference implementation's value of `100` is carried forward as a validated default.
- **List Tables sort order**: not documented; do not depend on ordering for diffing.
- **Primary key discovery**: LanceDB's unenforced primary key (set via SDK, not REST) is not surfaced by the `describe` endpoint; must remain a user-supplied table option.
- **Exact Arrow IPC sub-format** of query responses (file vs. stream) may vary; implement both-format fallback parsing as the reference implementation does.
