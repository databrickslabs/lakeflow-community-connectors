# Lakeflow Palantir Foundry Community Connector

This documentation provides setup instructions and reference information for the Palantir Foundry source connector.

The Lakeflow Palantir Foundry Connector allows you to extract data from Palantir Foundry ontologies and load it into Databricks. This connector supports both snapshot (full refresh) and incremental (CDC) synchronization patterns for Palantir object types.

## Features

- **Dynamic Schema Discovery**: Automatically discovers schemas from Palantir object type definitions
- **Flexible Ingestion Modes**: Supports both snapshot and incremental (CDC) sync
- **Memory-Efficient Snapshot Streaming**: Snapshot mode yields records page by page via a generator, avoiding OOM on large datasets. Incremental mode materialises records up to `max_records_per_batch` (default 100,000) so the offset can advance to the last-emitted record; the next microbatch resumes via a server-side composite `(cursor_field, tiebreaker_field)` filter (`cursor > prev` OR `cursor == prev AND tiebreaker > prev`). The tiebreaker (a unique sortable property, defaulting to the declared primary key) ensures rows sharing a non-unique cursor value — e.g. many rows at the same `arrivalTimestamp` — are never skipped or duplicated across a batch boundary.
- **Early-Exit Cursor Peek**: A single `orderBy desc, pageSize=1` call on the search endpoint short-circuits an incremental poll **only when the dataset's max cursor is strictly *below* the checkpoint** (definitively nothing new), skipping the `loadObjects` round-trip. An *equal* max still reads, since un-read rows may share that cursor value. Checkpointing itself is driven by the last emitted record.
- **Complex Type Support**: Handles geopoints, arrays, structs, and nested objects
- **In-Memory Caching**: Caches schemas and metadata for improved performance

## Prerequisites

- Access to a Palantir Foundry instance
- Bearer token with read permissions on the target ontology (`api:ontologies-read` scope)
- Ontology API name from Foundry

## Setup

### Connection Parameters

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `token` | string | Yes | Bearer token for Palantir API authentication | `eyJwbG50ciI6...` |
| `hostname` | string | Yes | Palantir Foundry hostname. Bare form (`yourcompany.palantirfoundry.com`) is preferred; full URLs like `https://yourcompany.palantirfoundry.com/` are also accepted — the connector normalises both. | `yourcompany.palantirfoundry.com` |
| `ontology_api_name` | string | Yes | API name of the target ontology | `ontology-282f1207-a9f0-...` |

### How to Obtain Parameters

#### 1. Bearer Token

Generate a bearer token in Palantir Foundry:

1. Log in to your Palantir Foundry instance
2. Navigate to **Developer Console** or **Account Settings > Tokens**
3. Click **Create New Token**
4. Grant appropriate read permissions
5. Copy the generated token (store securely)

#### 2. Hostname

The hostname is visible in your browser URL when logged into Foundry:
- Example URL: `https://yourcompany.palantirfoundry.com/workspace/...`
- Hostname: `yourcompany.palantirfoundry.com`

#### 3. Ontology API Name

Find your ontology API name:

**Method 1: Via API**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \
     "https://yourcompany.palantirfoundry.com/api/v2/ontologies"
```

Look for the `apiName` field in the response.

**Method 2: Via Ontology Manager**
1. Navigate to **Ontology Manager** in Foundry
2. Select your ontology
3. Look for the "API Name" field in the ontology details

### Create a Unity Catalog Connection

#### Via Databricks UI

1. Go to **Settings** > **Connections**
2. Click **Create Connection**
3. Select **Palantir Foundry**
4. Enter:
   - Connection name: `palantir`
   - Token: Your bearer token
   - Hostname: Your Foundry hostname
   - Ontology API Name: Your ontology identifier
5. Click **Create**

#### Via API/CLI

```python
# Using Databricks SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.connections.create(
    name="palantir",
    connection_type="palantir",
    options={
        "token": "your_bearer_token",
        "hostname": "yourcompany.palantirfoundry.com",
        "ontology_api_name": "ontology-282f1207-..."
    }
)
```

### Table Options

The connector supports the following table-specific options via `table_configuration`:

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `cursor_field` | string | Property name for incremental sync tracking. Omit for snapshot mode. | `"arrivalTimestamp"` |
| `page_size` | string | Number of records per API request (default 1000, max 10000) | `"10000"` |
| `max_records_per_batch` | string | Admission cap per microbatch — **incremental mode only** (snapshot mode streams all records in one framework-driven pass with no resume mechanism, so a cap there would silently drop data). Default 100000. Smaller values reduce driver memory; larger values reduce the number of microbatches needed to drain a backlog. | `"50000"` |
| `tiebreaker_field` | string | **Incremental mode only.** A unique, sortable property used to break ties when `cursor_field` is non-unique (e.g. many rows share one `arrivalTimestamp`). The connector resumes on the composite `(cursor_field, tiebreaker_field)` so rows sharing a boundary cursor value are never skipped or duplicated. Defaults to the ontology's declared primary-key property. Must be a **declared property** — Foundry rejects system fields (`__primaryKey`/`__rid`) in `orderBy`/`where`. If no usable property exists, run the table in snapshot mode. | `"flightId"` |

## Supported Objects

The connector dynamically discovers all object types in your configured ontology. Each object type becomes a table that can be synced.

> **System fields (`__`-prefixed) are not ingested by default.** Every `loadObjects` record carries four Palantir system fields — `__rid` (stable, globally-unique object Resource ID; useful for joins/lineage), `__primaryKey`, `__apiName`, and `__title`. The connector builds each table's schema from the object type's declared **properties** only, so these system fields are excluded from the discovered schema. (`__primaryKey` is still used internally as the merge key when the ontology declares no primary key.) To ingest one — e.g. `__rid` — expose it as a declared property in the ontology or capture it downstream. Note: Foundry rejects `__`-prefixed fields in `orderBy`/`where`, so they cannot be used as a `cursor_field` or `tiebreaker_field`.

### Ingestion Modes

**Snapshot Mode (Default)** — `scd_type: SCD_TYPE_1`
- Full refresh on each sync
- Use when: object types don't have update timestamps or full refresh is preferred
- Configuration: Don't specify `cursor_field`
- Reads all records using a memory-efficient generator (one page at a time)
- **No mid-snapshot checkpoint:** snapshot mode reads the whole object type in a single framework-driven pass (offset is always `{}`), so there is no resume point. If the pass is interrupted partway — e.g. a token expiry (401, which is not retried) or the Foundry snapshot-consistency token timing out on a very long drain — the next trigger **restarts the entire read from the beginning**. For very large object types, prefer incremental (CDC) mode, ensure the token TTL comfortably exceeds the expected drain time, or size compute so the drain completes well within those limits.

**Incremental Mode (CDC)** — `scd_type: SCD_TYPE_2`
- Only syncs new/updated records based on cursor field
- Use when: object types have timestamp fields for tracking changes
- Configuration: Specify `cursor_field` in table options
- Server-side filtering: uses a composite `(cursor_field, tiebreaker_field)` filter via objectSet composition (`cursor > prev` OR `cursor == prev AND tiebreaker > prev`) so the API only returns records strictly after the checkpoint — no full scan, and rows sharing a non-unique cursor value are never skipped or duplicated across a batch boundary
- Early-exit short-circuit: on subsequent runs, peeks at the dataset's current max cursor via `search orderBy desc, pageSize=1` and skips the data fetch when the max is strictly **below** the checkpoint (an *equal* max still proceeds, since un-read rows may share that cursor value)
  - *Eventual-consistency caveat:* the peek uses the **`search`** endpoint while the read uses **`loadObjects`**. Foundry's search index can briefly lag the object store, so a stale (lower) `search` max can defer a microbatch until the index catches up. This delays — never drops — data (the rows arrive on a later tick), and it self-corrects. If you need the lowest possible latency, the early-exit is an optimization you can accept the small staleness from.
- Checkpoint advances to the `(cursor, tiebreaker)` of the last emitted record so admission-capped batches resume exactly where they left off on the next tick
- **Admission cap (`_init_time`) — timestamp cursors only:** the per-trigger cap that defers records arriving *after* the connector started works by comparing the cursor to wall-clock start time, so it applies **only when `cursor_field` is a timestamp**. For non-timestamp cursors (numeric IDs, UUIDs) there is no wall-clock value to compare, so the cap is skipped and the connector drains until the source is caught up — still bounded per microbatch by `max_records_per_batch`, with no data loss. Use a timestamp cursor if you want per-trigger admission control.

**SCD Type Behavior:**

| SCD Type | Same primary key updated | Table result |
|---|---|---|
| `SCD_TYPE_1` | Old row overwritten | 1 row (latest only) |
| `SCD_TYPE_2` | Old row kept + new row added | 2 rows (full history with `__START_AT`, `__END_AT`) |
| `APPEND_ONLY` | New row inserted | Never updates, only inserts |

## Data Type Mapping

| Palantir Type | Spark/Databricks Type | Notes |
|---------------|----------------------|-------|
| string | StringType | |
| integer | LongType | |
| long | LongType | |
| double | DoubleType | |
| float | DoubleType | |
| boolean | BooleanType | |
| timestamp | StringType | Preserved as ISO 8601 format |
| date | StringType | Preserved as ISO date format |
| datetime | StringType | Preserved as ISO 8601 format |
| geopoint | StructType | {latitude: double, longitude: double} |
| array | ArrayType | Recursive type mapping |
| struct | StructType | Recursive type mapping |
| decimal | DecimalType | Precision/scale forwarded from Palantir; capped at Spark's max precision of 38, defaults to (38, 18) if not specified |
| attachment | StringType | JSON representation |

## How to Run

### Step 1: Configure Pipeline Spec

Create an ingestion notebook in your Databricks workspace with the
following content:

```python
from _generated_palantir_python_source import register_lakeflow_source

# Register the Palantir connector with Spark
register_lakeflow_source(spark)

# Define pipeline specification
pipeline_spec = {
    "connection_name": "palantir",
    "objects": [
        {
            "table": {
                "source_table": "FlightsFinal",         # Palantir object type API name
                "destination_catalog": "users",
                "destination_schema": "my_schema",
                "destination_table": "flights_final",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_2",            # Or SCD_TYPE_1, APPEND_ONLY
                    "cursor_field": "arrivalTimestamp",   # For incremental (omit for snapshot)
                    "page_size": "10000"                  # Records per API call
                }
            }
        }
    ]
}

# Run ingestion
from databricks.labs.community_connector.pipeline import ingest
ingest(spark, pipeline_spec)
```

### Step 2: Deploy via SDP Pipeline

1. Upload `_generated_palantir_python_source.py` to your Databricks workspace next to the ingestion notebook from Step 1
2. Create an SDP pipeline pointing to that ingestion notebook
3. Run with **Full refresh** for the initial load
4. Subsequent runs will be incremental (if `cursor_field` is configured)

### Step 3: Schedule (Optional)

Create a Databricks Job to run on a schedule for automated incremental syncs.

## Architecture

### File Structure

| File | Purpose | Where it runs |
|---|---|---|
| `palantir.py` | Source code (edit this) | Local dev + tests |
| `_generated_palantir_python_source.py` | Auto-generated deployable (all code merged) | Databricks SDP pipeline |
| `connector_spec.yaml` | Connection parameter definitions | Framework reference |

### Data Flow

```
Palantir Foundry Ontology
    |
    | POST /objectSets/loadObjects?snapshot=true
    | (consistent pagination, up to 10K/page, server-side filtering for CDC)
    v
PalantirLakeflowConnect (palantir.py)
    |
    | Generator yields records page by page (memory-efficient)
    | Jittered exp backoff on 429/503/network errors; honours Retry-After
    v
Lakeflow Framework (Spark PDS + SDP)
    |
    | apply_changes / apply_changes_from_snapshot
    v
Databricks Delta Table (Unity Catalog)
```

### API Endpoints Used

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/v2/ontologies/{ontology}/objectTypes` | GET | List object types; the connector indexes the response by `apiName` to serve both `list_tables()` and per-type schema discovery — no per-type GET call is made |
| `/api/v2/ontologies/{ontology}/objectSets/loadObjects?snapshot=true` | POST | Fetch data with consistent pagination |
| `/api/v2/ontologies/{ontology}/objects/{type}/search` | POST | Peek max cursor value for the early-exit short-circuit (orderBy desc, pageSize=1) |

## Best Practices

### 1. Choose the Right Cursor Field
- Use a field that **changes when the record is modified** (e.g., `updatedAt`, `lastModifiedTimestamp`)
- Business fields like `departureTimestamp` won't capture updates to other fields
- If no suitable cursor field exists, use snapshot mode

### 2. Optimize Page Size
- Default: 1,000 records per page
- Recommended for large datasets: 10,000 records per page
- Maximum: 10,000 records per page (loadObjects endpoint limit)
- Larger page sizes = fewer API calls but longer individual requests

### 3. Start with Snapshot, Then CDC
- Test with `SCD_TYPE_1` (snapshot) first to verify data loads correctly
- Switch to `SCD_TYPE_2` (CDC) with `cursor_field` once validated

### 4. Monitor API Usage
- Palantir rate limits: 5,000 requests/minute for individual users
- The connector retries on `429` / `503` and on connection / timeout errors with **jittered exponential backoff** (base `2^attempt` × uniform `[0.5, 1.5)`) so concurrent Spark tasks don't unblock in lockstep. When the server sets a `Retry-After` header on 429/503, the connector honours it as the base wait (with jitter on top). Non-transient 4xx/5xx responses (401, 404, etc.) propagate on the first attempt so misconfiguration surfaces immediately
- For large datasets, consider scheduling during off-peak hours
- Use a service user token for higher rate limits

### 5. Scalability Guidelines
- **< 10M records**: Current approach works well
- **10M - 100M**: Use `page_size=10000` to minimise API round-trips; CDC mode already uses a server-side composite `(cursor_field, tiebreaker)` filter on every incremental run
- **100M+**: Consider the Foundry Dataset API for bulk export

## Troubleshooting

### Error: "Missing required parameter 'token'"

**Cause**: Bearer token not provided in connection configuration

**Solution**:
- Verify token is set in Unity Catalog connection
- Check token hasn't expired
- Regenerate token if needed

### Error: "Failed to list object types"

**Cause**: Authentication failure or incorrect ontology API name

**Solution**:
- Verify bearer token has read permissions on the ontology
- Check `ontology_api_name` is correct
- Test API access manually:
  ```bash
  curl -H "Authorization: Bearer YOUR_TOKEN" \
       "https://yourhost.palantirfoundry.com/api/v2/ontologies"
  ```

### Error: "409 Conflict / OntologySyncingObjectTypes"

**Cause**: Object type is still being indexed by Palantir

**Solution**:
- Wait for the sync to complete (check Ontology Manager in Foundry)
- Retry the pipeline after sync finishes

### Error: "Table 'XYZ' is not supported"

**Cause**: Object type doesn't exist in the configured ontology

**Solution**:
- Verify object type API name spelling (case-sensitive)
- List available object types:
  ```bash
  curl -H "Authorization: Bearer YOUR_TOKEN" \
       "https://yourhost.palantirfoundry.com/api/v2/ontologies/YOUR_ONTOLOGY/objectTypes"
  ```

### Error: "Function exceeded the limit of 1024 megabytes"

**Cause**: Attempting to load too much data into memory on serverless

**Solution**:
- The connector uses generators to stream data page by page
- If this error persists, reduce `page_size`
- Ensure you're using the latest `_generated_palantir_python_source.py`

### Pipeline Shows "Output records = N/A"

**Cause**: Normal for snapshot mode — the SDP UI doesn't report record counts for `apply_changes_from_snapshot`

**Solution**: Query the destination table directly:
```sql
SELECT COUNT(*) FROM catalog.schema.table_name
```

### Incremental Run Shows No New Records

**Cause**: The max cursor value hasn't changed since the last run

**Solution**:
- The connector compares the current max cursor with the checkpoint
- If they're equal, it returns immediately with no data (correct behavior)
- Verify that new/updated records have a cursor field value **strictly greater than** the previous max

## Limitations

1. **Single Ontology Per Connection**: Each Unity Catalog connection targets one Palantir ontology. Create multiple connections for multiple ontologies.

2. **Server-Side Filtering on Incremental Only**: The initial full load fetches all records. Subsequent incremental runs use a server-side composite `(cursor_field, tiebreaker)` filter to fetch only new records.

3. **Object Types Only**: Currently supports object types. Link types and action types are not supported.

4. **No Delete Detection**: CDC mode doesn't detect deleted records.

5. **Cursor Field Requirement**: For incremental sync to work correctly, the cursor field must increase when records are created or modified. Business timestamp fields (e.g., `departureTimestamp`) may not capture all changes.

## References

- [Palantir Foundry API Documentation](https://www.palantir.com/docs/foundry/api/v2/general/overview/introduction/)
- [Lakeflow Community Connectors Repository](https://github.com/databricks-labs/lakeflow-community-connectors)
- [Databricks Unity Catalog Connections](https://docs.databricks.com/en/connect/unity-catalog/connections.html)
- [Databricks Lakeflow Connect](https://docs.databricks.com/en/ingestion/lakeflow-connect.html)

## Support

For issues or questions:
- File an issue in the [lakeflow-community-connectors repository](https://github.com/databricks-labs/lakeflow-community-connectors/issues)
- Check existing connector documentation for similar patterns
- Review Palantir API documentation for API-specific questions
