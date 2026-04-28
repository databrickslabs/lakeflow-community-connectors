# Lakeflow Palantir Foundry Community Connector

This documentation provides setup instructions and reference information for the Palantir Foundry source connector.

The Lakeflow Palantir Foundry Connector allows you to extract data from Palantir Foundry ontologies and load it into Databricks. This connector supports both snapshot (full refresh) and incremental (CDC) synchronization patterns for Palantir object types.

## Features

- **Dynamic Schema Discovery**: Automatically discovers schemas from Palantir object type definitions
- **Flexible Ingestion Modes**: Supports both snapshot and incremental (CDC) sync
- **Memory-Efficient Streaming**: Uses generators to yield records page by page, avoiding OOM on large datasets
- **Pre-Computed Cursor Lookup**: Single API call (orderBy desc, limit 1) to determine max cursor value for CDC checkpointing
- **Search API for Large Datasets**: Uses the POST `/objects/{objectType}/search` endpoint which supports full pagination without cross-page limits
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
| `hostname` | string | Yes | Palantir Foundry hostname (without https://) | `yourcompany.palantirfoundry.com` |
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

## Supported Objects

The connector dynamically discovers all object types in your configured ontology. Each object type becomes a table that can be synced.

### Ingestion Modes

**Snapshot Mode (Default)** — `scd_type: SCD_TYPE_1`
- Full refresh on each sync
- Use when: object types don't have update timestamps or full refresh is preferred
- Configuration: Don't specify `cursor_field`
- Reads all records using a memory-efficient generator (one page at a time)

**Incremental Mode (CDC)** — `scd_type: SCD_TYPE_2`
- Only syncs new/updated records based on cursor field
- Use when: object types have timestamp fields for tracking changes
- Configuration: Specify `cursor_field` in table options
- Pre-computes max cursor value via aggregate endpoint (falls back to orderBy desc)
- On subsequent runs, skips data fetch entirely if no new records exist
- Server-side filtering: uses `where: gt` via objectSet composition so the API only returns records newer than the checkpoint — no full scan needed on incremental runs

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
| geopoint | StructType | {latitude: double, longitude: double} |
| array | ArrayType | Recursive type mapping |
| struct | StructType | Recursive type mapping |
| decimal | DoubleType | Simplified for MVP |
| attachment | StringType | JSON representation |

## How to Run

### Step 1: Configure Pipeline Spec

Create an ingestion notebook (see `ingest_notebook.py`):

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

1. Upload `_generated_palantir_python_source.py` and `ingest_notebook.py` to your Databricks workspace
2. Create an SDP pipeline pointing to `ingest_notebook.py`
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
| `ingest_notebook.py` | Pipeline notebook (configures what to ingest) | Databricks SDP pipeline |
| `connector_spec.yaml` | Connection parameter definitions | Framework reference |
| `pipeline_spec.example.py` | Pipeline configuration examples | Developer reference |

### Data Flow

```
Palantir Foundry Ontology
    |
    | POST /objectSets/loadObjects?snapshot=true
    | (consistent pagination, 10K/page, server-side filtering for CDC)
    v
PalantirLakeflowConnect (palantir.py)
    |
    | Generator yields records page by page (memory-efficient)
    | Retry with exponential backoff on 429/503
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
| `/api/v2/ontologies/{ontology}/objectTypes` | GET | List available tables |
| `/api/v2/ontologies/{ontology}/objectTypes/{type}` | GET | Get schema and primary key |
| `/api/v2/ontologies/{ontology}/objectSets/loadObjects?snapshot=true` | POST | Fetch data with consistent pagination |
| `/api/v2/ontologies/{ontology}/objectSets/aggregate` | POST | Get max cursor value (primary) |
| `/api/v2/ontologies/{ontology}/objects/{type}/search` | POST | Get max cursor value (fallback: orderBy desc, limit 1) |

## Best Practices

### 1. Choose the Right Cursor Field
- Use a field that **changes when the record is modified** (e.g., `updatedAt`, `lastModifiedTimestamp`)
- Business fields like `departureTimestamp` won't capture updates to other fields
- If no suitable cursor field exists, use snapshot mode

### 2. Optimize Page Size
- Default: 1,000 records per page
- Recommended for large datasets: 10,000 records per page
- Maximum: 10,000 records per page (search endpoint limit)
- Larger page sizes = fewer API calls but longer individual requests

### 3. Start with Snapshot, Then CDC
- Test with `SCD_TYPE_1` (snapshot) first to verify data loads correctly
- Switch to `SCD_TYPE_2` (CDC) with `cursor_field` once validated

### 4. Monitor API Usage
- Palantir rate limits: 5,000 requests/minute for individual users
- The connector includes a 0.1s delay between page fetches
- For large datasets, consider scheduling during off-peak hours
- Use a service user token for higher rate limits

### 5. Scalability Guidelines
- **< 10M records**: Current approach works well
- **10M - 100M**: Use page_size=10000, consider server-side filtering
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

2. **Server-Side Filtering on Incremental Only**: The initial full load fetches all records. Subsequent incremental runs use server-side `where: gt` filtering to fetch only new records.

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
