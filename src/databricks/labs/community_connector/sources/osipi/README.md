# OSI PI Lakeflow Community Connector

This documentation provides setup instructions and reference information for the OSI PI (PI Web API) source connector.

## What is OSI PI?

OSI PI (often referred to as the **PI System**) is an industrial time-series historian platform used to collect, store, and serve high-frequency operational data from industrial assets (sensors, PLC/SCADA systems, DCS, lab systems, etc.). PI Web API provides a modern HTTP interface to query that historian data, as well as related operational context like asset hierarchies and events.

**Who typically uses it?**

OSI PI is commonly deployed by customers operating industrial assets, including:
- Manufacturing (process and discrete)
- Oil & gas, chemicals, mining, and metals
- Utilities (power, water, renewables) and energy operators
- Pharma / life sciences (regulated operations)
- Large infrastructure operators (pipelines, transport, facilities)

**Typical use cases:**
- Operational analytics (KPIs, OEE, quality, downtime, yield, energy intensity)
- Predictive maintenance & anomaly detection
- Asset/topology enrichment with PI AF hierarchy
- Event-based analysis (batches, campaigns, root-cause investigations)
- Bronze-layer landing for downstream pipelines

## Prerequisites

Before using this connector, ensure you have:

1. **PI Web API access** - Access to OSI PI Web API endpoint
2. **Authentication credentials** - One of:
   - OIDC client credentials (client_id + client_secret) - Recommended
   - Bearer token
   - Basic authentication (username + password)
3. **Network connectivity** - Ability to reach PI Web API from Databricks
4. **Databricks workspace** - With Unity Catalog enabled
5. **Permissions** - Ability to create UC Connections and Delta Live Tables pipelines

## Setup

### Required Connection Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `pi_base_url` | PI Web API endpoint (e.g., `https://piwebapi.example.com`) | Yes |
| `access_token` | Bearer token for authentication | For token auth |
| `client_id` | OIDC client ID | For OIDC auth |
| `client_secret` | OIDC client secret | For OIDC auth |
| `username` | Username for basic auth | For basic auth |
| `password` | Password for basic auth | For basic auth |
| `workspace_host` | Databricks workspace URL (required for OIDC) | For OIDC auth |
| `verify_ssl` | Verify SSL certificates (default: `true`) | No |

**Note**: Choose one authentication method - Token, OIDC, or Basic Auth. OIDC is recommended for production use.

### Obtaining Required Parameters

**For OIDC Authentication (Recommended):**
1. Contact your PI administrator to set up OIDC application
2. Obtain `client_id` and `client_secret`
3. Store credentials in Databricks secrets

**For Token Authentication:**
1. Generate bearer token from PI Web API or identity provider
2. Store token in Databricks secrets

### Creating Unity Catalog Connection

Store credentials in Databricks secrets, then create UC Connection:

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
        "client_secret": "{{secrets/your-scope/client-secret}}",
        "verify_ssl": "true"
    },
    comment="OSI PI connection for industrial data"
)
```

## Supported Objects

This connector supports **39 tables** across PI Data Archive, Asset Framework, and Event Frames.

### Object Summary, Primary Keys, and Ingestion Mode

| Table Name | Ingestion Type | Primary Keys | Cursor Field | Category |
|------------|----------------|--------------|--------------|----------|
| `pi_dataservers` | Snapshot | `webid` | - | Discovery |
| `pi_points` | Snapshot | `webid` | - | Discovery |
| `pi_point_attributes` | Snapshot | `point_webid`, `name` | - | Discovery |
| `pi_point_type_catalog` | Snapshot | `point_type`, `engineering_units` | - | Discovery |
| `pi_timeseries` | Append | `tag_webid`, `timestamp` | `timestamp` | Time Series |
| `pi_streamset_recorded` | Append | `tag_webid`, `timestamp` | `timestamp` | Time Series |
| `pi_streamset_plot` | Append | `tag_webid`, `timestamp` | `timestamp` | Time Series |
| `pi_streamset_interpolated` | Append | `tag_webid`, `timestamp` | `timestamp` | Time Series |
| `pi_streamset_summary` | Append | `tag_webid`, `summary_type`, `timestamp` | `timestamp` | Time Series |
| `pi_interpolated` | Append | `tag_webid`, `timestamp` | `timestamp` | Time Series |
| `pi_plot` | Append | `tag_webid`, `timestamp` | `timestamp` | Time Series |
| `pi_calculated` | Append | `tag_webid`, `timestamp`, `calculation_type` | `timestamp` | Time Series |
| `pi_current_value` | Snapshot | `tag_webid` | - | Time Series |
| `pi_end` | Snapshot | `tag_webid` | - | Time Series |
| `pi_summary` | Snapshot | `tag_webid`, `summary_type` | - | Time Series |
| `pi_recorded_at_time` | Snapshot | `tag_webid`, `query_time` | - | Time Series |
| `pi_value_at_time` | Snapshot | `tag_webid`, `timestamp` | - | Time Series |
| `pi_streamset_end` | Snapshot | `tag_webid` | - | Time Series |
| `pi_assetservers` | Snapshot | `webid` | - | Asset Framework |
| `pi_assetdatabases` | Snapshot | `webid` | - | Asset Framework |
| `pi_af_hierarchy` | Snapshot | `element_webid` | - | Asset Framework |
| `pi_element_templates` | Snapshot | `webid` | - | Asset Framework |
| `pi_element_template_attributes` | Snapshot | `webid` | - | Asset Framework |
| `pi_element_attributes` | Snapshot | `element_webid`, `attribute_webid` | - | Asset Framework |
| `pi_attribute_templates` | Snapshot | `webid` | - | Asset Framework |
| `pi_categories` | Snapshot | `webid` | - | Asset Framework |
| `pi_units_of_measure` | Snapshot | `webid` | - | Asset Framework |
| `pi_analyses` | Snapshot | `webid` | - | Asset Framework |
| `pi_analysis_templates` | Snapshot | `webid` | - | Asset Framework |
| `pi_af_tables` | Snapshot | `webid` | - | Asset Framework |
| `pi_af_table_rows` | Snapshot | `table_webid`, `row_index` | - | Asset Framework |
| `pi_event_frames` | Append | `event_frame_webid`, `start_time` | `start_time` | Event Frames |
| `pi_eventframe_referenced_elements` | Append | `event_frame_webid`, `element_webid` | `start_time` | Event Frames |
| `pi_eventframe_templates` | Snapshot | `webid` | - | Event Frames |
| `pi_eventframe_template_attributes` | Snapshot | `webid` | - | Event Frames |
| `pi_eventframe_attributes` | Snapshot | `event_frame_webid`, `attribute_webid` | - | Event Frames |
| `pi_eventframe_annotations` | Snapshot | `event_frame_webid`, `annotation_id` | - | Event Frames |
| `pi_eventframe_acknowledgements` | Snapshot | `event_frame_webid`, `ack_id` | - | Event Frames |
| `pi_links` | Snapshot | `entity_type`, `webid`, `rel` | - | Governance |

**Ingestion Types:**
- **Snapshot**: Full table refresh on each run
- **Append**: Incremental loading with cursor-based tracking

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

#### Time-Series Tables (Append Mode)

**Required:**
- `tag_webids`: Comma-separated list of PI tag WebIDs to query

**Optional:**
- `startTime`: Start time for data retrieval (default: `*-24h`)
- `endTime`: End time for data retrieval (default: `*`)
- `maxCount`: Maximum events per request (default: API limit)
- `tags_per_request`: Batch tag requests (0 = no batching)
- `window_seconds`: Cap time window per batch (0 = unlimited)
- `prefer_streamset`: Use StreamSet endpoints for multi-tag queries (default: `true`)

### Discovery Tables (Snapshot Mode)

Most discovery tables require no additional options. Query all available data by default.

### Asset Framework Tables (Snapshot Mode)

**Optional:**
- `database_webids`: Comma-separated AF database WebIDs to scope queries
- `element_webids`: Specific element WebIDs for targeted queries

### Event Frames Tables (Append Mode)

**Optional:**
- `startTime`: Event frame start time filter
- `endTime`: Event frame end time filter
- `searchMode`: Search mode (e.g., `StartInclusive`, `EndInclusive`)

## How to Run

### Using Load-Balanced Deployment Notebook (Recommended)

For production deployments with many tables, use the load-balanced deployment notebook:

**Location**: `tools/load_balanced_deployment/notebooks/00_load_balanced_deployment_all_in_one.py`

This notebook provides:
- Auto-discovery of all connector tables with metadata
- Load-balanced grouping into multiple pipelines
- Automated pipeline and job deployment
- Scheduled execution with configurable cron expressions

**Steps:**

1. **Sync connector code** to your Databricks workspace at:
   ```
   /Workspace/Users/{your-email}/lakeflow-community-connectors/
   ```

2. **Import and run the notebook**:
   - Open `tools/load_balanced_deployment/notebooks/00_load_balanced_deployment_all_in_one.py`
   - Configure widgets:
     - Connector Name: `osipi`
     - UC Connection: Your connection name
     - Use Preset: `true` (uses pre-classified tables) or `false` (auto-discover)
     - Destination Catalog/Schema
     - Schedules (cron expressions)
     - Secrets scope and mapping (for auto-discovery only)

3. **Run all cells** to:
   - Discover/load table metadata
   - Generate ingest files
   - Deploy pipelines to workspace
   - Create scheduled jobs (optional)
   - Start pipelines (optional)

4. **Monitor pipelines** at: Workflows > Delta Live Tables

### Best Practices

- **Start small**: Begin with a few tables to validate connectivity and performance
- **Use incremental loading**: Enable cursor-based tracking for time-series and event tables
- **Monitor API limits**: PI Web API has rate limits - use `tags_per_request` and `window_seconds` for large-scale ingestion
- **Group by category**: Use `category_and_ingestion_type` grouping for optimal parallelism
- **Schedule appropriately**:
  - Snapshot tables: Daily (e.g., `0 0 * * *`)
  - Append tables: 15-30 minutes (e.g., `*/15 * * * *`)

### Performance Tuning

**For high-frequency time-series data:**
- Set `tags_per_request=25` to batch tag queries
- Set `window_seconds=300` to limit time windows per batch
- Use `prefer_streamset=true` for multi-tag efficiency

**For large-scale deployments:**
- Group tables by category and ingestion type
- Deploy multiple pipelines for parallelism
- Use scheduled jobs with staggered execution

### Troubleshooting

**Authentication Issues:**
- Verify UC Connection credentials are correct
- Check that secrets scope and keys exist
- Confirm OIDC client has necessary permissions

**Rate Limiting:**
- Reduce `tags_per_request` value
- Increase `window_seconds` to smaller time windows
- Add delays between scheduled job executions

**Missing Data:**
- Verify `tag_webids` are valid and accessible
- Check time ranges (`startTime`, `endTime`) are correct
- Confirm PI Web API permissions for data access

**Schema Changes:**
- Re-run discovery to update table metadata
- Check for custom attributes in AF elements
- Verify data type mappings

## References

- **PI Web API Documentation**: https://docs.osisoft.com/bundle/pi-web-api
- **Connector Implementation**: `src/databricks/labs/community_connector/sources/osipi/osipi.py`
- **Load-Balanced Deployment**: `tools/load_balanced_deployment/`
