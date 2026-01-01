# OSI PI Lakeflow Community Connector

Lakeflow OSIPI Community Connector
This documentation provides setup instructions and reference information for the OSI PI (PI Web API) source connector.

## What is OSI PI?

OSI PI (often referred to as the **PI System**) is an industrial time-series historian platform used to collect, store, and serve high-frequency operational data from industrial assets (sensors, PLC/SCADA systems, DCS, lab systems, etc.). PI Web API provides a modern HTTP interface to query that historian data, as well as related operational context like asset hierarchies and events.

### Who typically uses it?

OSI PI is commonly deployed by customers operating industrial assets, including:

- Manufacturing (process and discrete)
- Oil & gas, chemicals, mining, and metals
- Utilities (power, water, renewables) and energy operators
- Pharma / life sciences (regulated operations)
- Large infrastructure operators (pipelines, transport, facilities)

### Typical ingestion use cases for this community connector

This community connector is typically used to ingest PI data into Databricks/Lakehouse for:

- **Operational analytics**: KPIs, OEE, quality, downtime, yield, energy intensity
- **Predictive maintenance & anomaly detection**: modeling from time-series + context
- **Asset / topology enrichment**: bringing PI AF hierarchy and attributes into the lakehouse
- **Event-based analysis**: batches/campaigns, event frames, and root-cause investigations
- **Bronze-layer landing for downstream pipelines**: standardizing and incrementally refreshing PI extracts

## Features

- **39 tables** covering PI Data Archive, Asset Framework, and Event Frames
- **Multiple authentication methods**: OIDC, Bearer token, Basic auth
- **UC Connections integration**: Secure credential management
- **Incremental loading**: Automatic time-based cursors for time-series data
- **Production-ready**: Error handling, retry logic, token refresh

## Quick Start

### 1. Create UC Connection

#### Current Method (With UC Bug Workaround)

Due to a known UC bug (being fixed Dec 2024), use workaround property names:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()

w.connections.create(
    name="osipi_connection",
    connection_type=ConnectionType.DATABRICKS,
    options={
        "pi_base_url": "https://your-pi-webapi-host",
        "workspace_host": "https://your-workspace.cloud.databricks.com",
        "client_id": "{{secrets/your-scope/client-id}}",
        "client_value_tmp": "{{secrets/your-scope/client-secret}}",  # ⚠️ Workaround name
        "verify_ssl": "true"
    },
    comment="OSI PI connection for industrial data"
)
```

#### Future Method (After UC Fix)

Once Databricks merges the UC fix, use normal property names:

```python
w.connections.create(
    name="osipi_connection",
    connection_type=ConnectionType.DATABRICKS,
    options={
        "pi_base_url": "https://your-pi-webapi-host",
        "workspace_host": "https://your-workspace.cloud.databricks.com",
        "client_id": "{{secrets/your-scope/client-id}}",
        "client_secret": "{{secrets/your-scope/client-secret}}",  # ✅ Normal name
        "verify_ssl": "true"
    }
)
```

**Note**: The connector automatically supports both naming patterns. No code changes needed after UC fix.

### 2. Register Connector

```python
# In Databricks notebook
exec(open("/path/to/_generated_osipi_python_source.py").read())
register_lakeflow_source(spark)
```

### 3. Read Data

```python
# Read PI data servers
df = (
    spark.read.format("lakeflow_connect")
    .option("databricks.connection", "osipi_connection")
    .option("tableName", "pi_dataservers")
    .load()
)

# Read time-series data
df = (
    spark.read.format("lakeflow_connect")
    .option("databricks.connection", "osipi_connection")
    .option("tableName", "pi_timeseries")
    .option("tag_webids", "I9zHPD3w-2pki0OYUCdU-RnQQgAQABAC1...")
    .option("startTime", "*-24h")
    .option("endTime", "*")
    .load()
)
```

## Authentication Methods

### 1. OIDC Client Credentials (Recommended)

**Required properties:**
- `client_id` (or use workaround name)
- `client_secret` → use `client_value_tmp` (workaround for current UC bug)
- `workspace_host`

**How it works:**
1. Connector exchanges client credentials for access token
2. Token cached and auto-refreshed
3. Supports long-running pipelines

### 2. Bearer Token

**Required properties:**
- `access_token`

**Use when:**
- You manage token acquisition separately
- Short-lived queries only

### 3. Basic Authentication

**Required properties:**
- `username`
- `password`

**Use only if:**
- PI Web API has basic auth enabled
- No OIDC available

## Known Issues

### UC Connection Bug (Dec 2024)

**Issue**: UC Connections strips properties containing `secret` or `token` from connection object retrieval.

**Status**: Confirmed by Databricks (Sheldon Tauro). Fix identified and will be merged soon.

**Impact**: Requires using workaround property names temporarily.

**Workaround**: Use alternative property names:
- `client_secret` → `client_value_tmp`
- `refresh_token` → `refresh_value_tmp`

**Resolution**: After UC fix is deployed, normal property names will work automatically. No connector changes needed.

## Available Tables

### Discovery & Inventory (4 tables)
- `pi_dataservers` - PI Data Archive servers
- `pi_points` - PI tags/points
- `pi_point_attributes` - Point metadata
- `pi_point_type_catalog` - Point type statistics

### Time Series (14 tables)
- `pi_timeseries` - Recorded values
- `pi_streamset_recorded` - Multi-tag recorded values
- `pi_interpolated` - Interpolated values
- `pi_current_value` - Latest values
- `pi_summary` - Statistical summaries
- And 9 more...

### Asset Framework (13 tables)
- `pi_assetservers` - AF servers
- `pi_af_hierarchy` - Asset hierarchy
- `pi_element_attributes` - Asset attributes
- `pi_element_templates` - Templates
- And 9 more...

### Event Frames (7 tables)
- `pi_event_frames` - Events/batches
- `pi_eventframe_attributes` - Event metadata
- And 5 more...

See full documentation for table schemas and options.

## Examples

### Incremental Time-Series Pipeline

```python
# DLT Pipeline
import dlt

@dlt.table
def pi_temperature_data():
    return (
        spark.readStream
        .format("lakeflow_connect")
        .option("databricks.connection", "osipi_connection")
        .option("tableName", "pi_timeseries")
        .option("tag_webids", "temp_sensor_webids")
        .load()
    )
```

## Performance: Chunking / Batching (Why it’s needed)

OSI PI time-series APIs can return very large payloads (high-frequency samples × many tags). In practice, requests that try to read “too much” can:

- Time out or hit gateway / proxy limits
- Overwhelm PI Web API with very large JSON responses
- Reduce parallelism (one huge request blocks progress)

This connector supports **connector-side chunking** to keep requests bounded and predictable.

### Tag batching: `tags_per_request`

For multi-tag tables (StreamSet-based reads), the connector can split a long `tag_webids` list into multiple smaller requests:

- **`tags_per_request`**: max number of tag webIds to include per request
  - `0` (default) means “do not split”
  - Example: `tags_per_request=25` batches 250 tags into 10 requests

### Time-window chunking: `window_seconds`

For incremental time-series reads, the connector can enforce a maximum time window per micro-batch:

- **`window_seconds`**: caps the effective `(startTime, endTime)` window per read
  - `0` (default) means “no enforced window”
  - Example: `window_seconds=300` reads at most 5 minutes per batch

### Related knobs

- **`maxCount`**: limits number of returned events per request (PI Web API parameter)
- **`prefer_streamset`**: when multiple tags are requested, prefer StreamSet endpoints for efficiency

All of these knobs are configured via `table_configuration` / `table_options` in your pipeline spec.

## Multi-pipeline ingestion (recommended for scale)

When ingesting many OSIPI tables (or many “slices” of the same table, e.g. different tag groups), a **single pipeline** can become slow and fragile. A **multi-pipeline** layout provides:

- Better parallelism (pipelines run independently)
- Failure isolation (one heavy group won’t block others)
- Easier scheduling (different groups can run at different cadences)

This repo includes a connector-agnostic DAB generator:

- `tools/notebook_based_deployment/generate_dab_yaml_notebooks.py`

It converts a CSV (what to ingest) into DAB YAML with one pipeline per group.

For OSIPI (and any connector that provides reliable `read_table_metadata`), you can generate that CSV automatically based on table characteristics (e.g. `ingestion_type`) and optional schedules:

- `tools/notebook_based_deployment/discover_and_classify_tables.py`

Example (group by ingestion type, give snapshots a daily schedule and append tables a 15-min schedule):

```bash
python3 tools/notebook_based_deployment/discover_and_classify_tables.py \
  --connector-name osipi \
  --output-csv /tmp/osipi_classified.csv \
  --dest-catalog main \
  --dest-schema bronze \
  --group-by category_and_ingestion_type \
  # Produces groups like: time_series_append, asset_framework_snapshot, event_frames_append, ...
  --schedule-snapshot "0 0 * * *" \
  --schedule-append "*/15 * * * *"

python3 tools/notebook_based_deployment/generate_dab_yaml_notebooks.py \
  --input-csv /tmp/osipi_classified.csv \
  --output-yaml /tmp/osipi_pipelines.yml \
  --connector-name osipi \
  --dest-catalog main \
  --dest-schema bronze \
  --emit-jobs \
```

### Option A: Provide `pipeline_group` in CSV (explicit grouping)

Rows with the same `pipeline_group` end up in the same Lakeflow pipeline.

### Option B: Auto-balance into N pipelines (connector-agnostic)

If your CSV omits `pipeline_group`, you can ask the generator to auto-assign groups:

- **`--num-pipelines N`**: spreads rows across N pipelines using weights

### Option C: Chunk (split) large groups into multiple pipelines

Even when you provide `pipeline_group`, you can cap how many items land in one pipeline:


### Asset Hierarchy Analysis

```python
# Join hierarchy with attributes
hierarchy = spark.table("osipi.bronze.pi_af_hierarchy")
attributes = spark.table("osipi.bronze.pi_element_attributes")

assets = (
    hierarchy
    .join(attributes, "element_webid")
    .select("name", "path", "template_name", "attribute_name", "value")
)
```

## Support

- **Issues**: Report via GitHub
- **Slack**: #lakeflow-community-connectors
- **Documentation**: See `/docs` folder

## License

[Your License]
