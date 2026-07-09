# Lakeflow LanceDB Community Connector

Ingest tables from **LanceDB Cloud** — including their vector/embedding columns — into Databricks Delta tables via Lakeflow Connect. No coding required.

LanceDB is a vector database. Its tables are user-defined and fully dynamic, so this connector **discovers your tables automatically** at runtime and reads each one as a full-table snapshot (re-read in full on every run). Vector/embedding columns come through as numeric arrays alongside your regular columns.

---

## Prerequisites

- **LanceDB Cloud account and project**: Access to the LanceDB Cloud project (also called a "database") whose tables you want to ingest.
- **API Key**: The connector authenticates with an API key sent in the `x-api-key` header on every request. Generate one in the LanceDB Cloud console under **Settings → API Keys**.
- **Project name and region**: The connector builds its host from these two values: `https://{project_name}.{region}.api.lancedb.com`. Both are shown in your LanceDB Cloud dashboard next to the project.
- **Network access**: The Databricks environment running the connector must be able to reach `https://{project_name}.{region}.api.lancedb.com`.

---

## Step 1: Get Your LanceDB Credentials

1. Sign in to the **LanceDB Cloud** console.
2. Under **Settings → API Keys**, generate (or copy) an API key. Store it securely — you will need it when creating the connection.
3. From the Cloud dashboard, note your **project name** (the database identifier, e.g. `my-lancedb-project`) and its **region** (e.g. `us-east-1`). These become the first two labels of the API host.

> **Note:** `project_name` and `region` must contain only letters, numbers, hyphens, and underscores — they are interpolated directly into the API hostname.

---

## Step 2: Create a Connection in Databricks

1. In your Databricks workspace, go to **Add Data → Lakeflow Community Connectors**.
2. Search for **LanceDB** and click **Configure**.
3. Fill in the connection form:

| Field | Required | What to enter |
|-------|----------|---------------|
| **API Key** | Yes | Your LanceDB Cloud API key (`api_key`). Stored as a secret. |
| **Project Name** | Yes | Your LanceDB project/database identifier (`project_name`), e.g. `my-lancedb-project` |
| **Region** | Yes | The cloud region hosting the project (`region`), e.g. `us-east-1` |
| **External Options Allow List** | Yes | Copy and paste this value exactly: `columns,batch_size,filter_expression` |

> **Why do I need External Options Allow List?**
> This connector supports optional per-table settings (such as which columns to read, batch size, and a filter). The `externalOptionsAllowList` field tells Databricks which option names are allowed to pass through from your pipeline to the connector. Simply paste `columns,batch_size,filter_expression` into the field — you don't need to understand each option unless you want to customize behavior later (see [Optional Per-Table Settings](#optional-per-table-settings)).

4. Click **Test connection** to verify, then **Save**.

---

## Step 3: Configure Your Pipeline

Once the connection is saved, Databricks will guide you through creating an ingestion pipeline where you choose which tables to ingest.

**Tables are discovered dynamically.** LanceDB has no fixed list of tables — the connector lists whatever tables currently exist in your project via `GET /v1/table/`. Use the exact table names as they appear in LanceDB Cloud as the `source_table` value.

The pipeline configuration (stored in `ingest.py` in your workspace) looks like this — **you only need to edit the table list and any optional settings**:

```json
{
  "connection_name": "my_lancedb_connection",
  "objects": [
    { "table": { "source_table": "documents" } },
    { "table": { "source_table": "embeddings" } }
  ]
}
```

> **Tip — Start small:** Add one or two tables on the first run to verify credentials and confirm the data shape (including how vector columns land). Add the rest once you've confirmed the connection is working.

---

## Step 4: Run and Schedule the Pipeline

Run the pipeline from the Databricks UI or schedule it as a recurring job.

- **First run**: each configured table is read in full (a snapshot / full-table scan).
- **Subsequent runs**: every table is re-read in full. This connector is **snapshot-only** — it does not track incremental changes. LanceDB's REST API has no primary keys and no row-level change tracking (no cursor column, no change/delete feed); its table-level version only supports full-snapshot time-travel, not row-level deltas.

---

## Supported Tables

There is **no fixed table list** — every table in your LanceDB project is discoverable and ingestible. Use the exact table name from LanceDB Cloud as `source_table`.

When table discovery returns nothing (for example in offline test mode), the connector falls back to two built-in **example** tables so the pipeline is still exercisable. These illustrate the two common LanceDB shapes:

| Example table | Shape | How it syncs | Notes |
|---------------|-------|--------------|-------|
| `documents` | Text + embedding | Snapshot (full refresh) | Canonical RAG table: `id`, `text`, `category`, `embedding` (vector), `updated_at`. |
| `embeddings` | Bare vector table | Snapshot (full refresh) | `id`, `source`, `vector` (vector), `created_at`. |

Each real table's schema is read from `POST /v1/table/{name}/describe/`, so your actual columns and types are used automatically — the example schemas above are only fallbacks/seeds.

### How reads work

Every table is read as a full-table **snapshot** and re-read in full on each run. Internally this is a "full-table scan": because LanceDB's query API is vector-first and top-K, the connector issues a scan using an all-zero dummy vector (sized to the table's embedding dimension, auto-detected from the schema) with `bypass_vector_index` set, and paginates by offset until all rows are returned. This is **not** a similarity search — every row comes back.

This connector is snapshot-only. LanceDB's REST API exposes no primary keys and no row-level change tracking (no cursor column, no change/delete feed). It does expose a monotonic table-level `version` (time-travel), but that returns full snapshots rather than row-level deltas, so incremental (CDC) reads are not supported.

> **No delete capture:** deleted rows in a table simply stop appearing on the next full read.

---

## Optional Per-Table Settings

These settings are **optional** — all tables work without them. To use them, add them inside the `table_configuration` block for the relevant table in your pipeline configuration:

```json
{ "table": {
    "source_table": "documents",
    "table_configuration": { "batch_size": "2000" }
  }
}
```

| Option | Default | Description |
|--------|---------|-------------|
| `columns` | *(all columns)* | JSON array of column names to read, e.g. `["id", "file_name"]`. Prunes the columns fetched — useful to exclude large vector or binary columns. Applied both in the request and re-checked client-side. |
| `batch_size` | `1000` | Number of rows fetched per request (the query API's top-`k`), also the pagination page size. Clamped to the range `1`–`10000`. |
| `filter_expression` | *(none)* | A Lance SQL filter applied server-side, e.g. `category = 'news'`. |

**Full example with options:**

```json
{
  "connection_name": "my_lancedb_connection",
  "objects": [
    {
      "table": {
        "source_table": "documents",
        "table_configuration": {
          "columns": "[\"id\", \"text\", \"category\", \"updated_at\"]",
          "batch_size": "2000",
          "filter_expression": "category = 'news'"
        }
      }
    },
    {
      "table": {
        "source_table": "embeddings",
        "table_configuration": { "batch_size": "5000" }
      }
    }
  ]
}
```

> **Tip — exclude large binary columns to control cost:** Vector (`embedding`/`vector`) and binary columns can be large. For example, a `people` table with an `image_bytes` blob column can be projected to skip that column entirely — `"columns": "[\"id\", \"file_name\", \"gender\", \"age\"]"` — so the large image payloads are never fetched. Since every run re-reads the table in full, excluding columns you don't need downstream substantially reduces transfer size and cost.

---

## Schema Notes

### How columns are read

- **Schema source:** Each table's Spark schema is derived from the LanceDB `describe` endpoint (`POST /v1/table/{name}/describe/`), which returns the table's Arrow field list. If `describe` returns nothing, a built-in example schema is used as a fallback.
- **Query results:** Rows come back as **Apache Arrow IPC** (the connector tries the streaming sub-format first, then the file sub-format; plain JSON is also accepted for test/simulator paths) and are converted to rows of `column name → value`.
- **Vector / embedding columns:** A `fixed_size_list` column is your embedding/vector column. It is read as an **array of numbers** (`ArrayType(FloatType)`, or `ArrayType(DoubleType)` when the element type is known to be double-precision). The connector also uses this column's declared dimension to size the dummy vector for full-table scans, so you don't need to know the embedding model's output size.
- **System columns:** `_distance` (added only for real similarity searches) and `_rowid` (opt-in) are **not** part of a normal table read and are not included in the schema.

### Data type mapping

The `describe` field types (Arrow-derived) map to Spark/Delta types as follows. Types may arrive as a structured object (`{"type": "int64"}`) or a flattened string (e.g. `"fixed_size_list<float32>[384]"`); both are handled.

| LanceDB / Arrow type | Spark / Delta type | Notes |
|----------------------|--------------------|-------|
| `string`, `utf8`, `large_string` | `StringType` | Text data. |
| `int8`/`int16`/`int32`/`int64` (and unsigned variants) | `LongType` | All integer widths map to `LongType` to prevent overflow. |
| `float32`, `float`, `float16` | `FloatType` | Single precision. |
| `float64`, `double` | `DoubleType` | Double precision. |
| `bool`, `boolean` | `BooleanType` | `true`/`false` flags. |
| `binary`, `large_binary`, `fixed_size_binary` | `BinaryType` | Raw bytes (images, BLOBs). Consider excluding large blob columns via `columns` to control cost. |
| `date`, `date32`, `date64` | `DateType` | — |
| `timestamp` | `TimestampType` | — |
| `fixed_size_list` (embedding/vector column) | `ArrayType(FloatType)` / `ArrayType(DoubleType)` | The vector column. `length` gives the embedding dimension. |
| `list`, `large_list` | `ArrayType(...)` | Variable-length array; element type mapped recursively when available. |
| `struct` | `StructType` | Nested records are mapped field-by-field. |
| *(any unrecognized type)* | `StringType` | Safe fallback. |

---

## Troubleshooting

### Authentication failures (`401` / `403`)
- Verify the `api_key` is correct and has not been revoked. Generate a new one in the LanceDB Cloud console under **Settings → API Keys** if needed.
- `401`/`403` are treated as non-retryable — they indicate a bad or under-privileged key, not a transient error.

### Connection / host errors
- Confirm `project_name` and `region` exactly match what LanceDB Cloud shows for your project. The connector builds its host as `https://{project_name}.{region}.api.lancedb.com`; a wrong project or region resolves to the wrong (or nonexistent) host.
- Both values may contain only letters, numbers, hyphens, and underscores; anything else is rejected before a request is sent.

### A table isn't showing up
- Table discovery reads the current tables in your project on each run. Make sure the table exists in this project (and region) and that the API key has access to it. Use the exact table name from LanceDB Cloud as `source_table`.

### Rate limiting (`429`) or transient errors (`5xx`)
- LanceDB Cloud does not publish rate limits. The connector retries `429`, `500`, `502`, `503`, and `504` responses with exponential backoff, honoring the `Retry-After` header when present.
- If you see persistent throttling, widen the pipeline schedule interval.

### Query is slow or times out on large tables
- Lower or raise `batch_size` (range `1`–`10000`) to tune request size; the default is `1000`.
- Use the `columns` option to read only the columns you need — excluding large vector/binary columns can substantially reduce transfer size.

---

## Reference

| Resource | Link |
|----------|------|
| LanceDB Cloud REST — List Tables | https://docs.lancedb.com/api-reference/rest/table/list-all-tables |
| LanceDB Cloud REST — Describe Table | https://docs.lancedb.com/api-reference/rest/table/describe-information-of-a-table |
| LanceDB Cloud REST — Query a Table | https://docs.lancedb.com/api-reference/rest/table/query-a-table |
| LanceDB authentication docs | https://docs.lancedb.com/enterprise/authentication |
| Connector implementation | `src/databricks/labs/community_connector/sources/lancedb/lancedb.py` |
| Connector schemas | `src/databricks/labs/community_connector/sources/lancedb/lancedb_schemas.py` |
| Connector spec | `src/databricks/labs/community_connector/sources/lancedb/connector_spec.yaml` |
