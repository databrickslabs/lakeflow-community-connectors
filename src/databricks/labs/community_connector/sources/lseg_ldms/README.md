# Lakeflow LSEG LDMS (RDMS) Community Connector

This documentation describes how to configure and use the **LSEG LDMS** Lakeflow community connector to ingest data from the LSEG Data Management System (LDMS, branded **RDMS** in its Swagger UI) REST API into Databricks.

LDMS is a single, generic **curve / time-series** REST API that serves oil, gas, power, freight, refinery, OPIS rack, options and tabular datasets. Rather than a fixed table list, real data lives in hundreds of thousands of individual **curves** that are discovered by metadata search. The connector exposes a small set of **logical tables** (`curve_values`, `curve_metadata`, `tabular_data`) and lets you choose *which* curves or datasets each returns via per-table options.

## One connection per host

There is **no separate "oil" vs "freight" API** — they are the same API surface exposed on different hosts with different permissioning:

| Deployment | Host (`base_url`) |
|---|---|
| Oil | `https://oilprod1.rdms.refinitiv.com` |
| Freight | `https://freightprod1.rdms.refinitiv.com` |

Because permissioning is tied to the host and to the API key, the connector is deployed as **one Unity Catalog connection per host**, each with its own `base_url` and `api_key`. This mirrors the GitHub connector's `base_url` override for GitHub Enterprise.

## Prerequisites

- **LSEG LDMS / RDMS access**: An entitled LDMS deployment (oil, freight, or another host) reachable over HTTPS.
- **API key**: A valid LDMS API key. All data permissions (DACS) are tied to the key — a curve the key cannot see returns an `Error` status rather than data. You can verify a key with `GET /api/v1/KeyStatus`, which returns the number of hours until the key expires.
- **Network access**: The environment running the connector must be able to reach the LDMS host you are targeting (e.g. `https://oilprod1.rdms.refinitiv.com`).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `base_url` | string | yes | Host-specific base URL for the target LDMS deployment. There is no default — you must set it per host. | `https://oilprod1.rdms.refinitiv.com` |
| `api_key` | string | yes | The **entire** `Authorization` header value, **including the `apikey-v1 ` prefix**. Sent verbatim on every request; there is no OAuth or token exchange. | `apikey-v1 66kCwNWtbJvdBbcSJvjTzVm4wiKELIiyx58axrC6M71` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector requires table-specific options, so this parameter must be set. | see list below |

The full list of supported table-specific options for `externalOptionsAllowList` is:

`curve_ids,metadata_query,scenario_id,start_date,result_timezone,ingestion_mode,window_days,max_partitions,max_results,data_type,fields,filter,order_by,page_size`

> **Note**: Table-specific options such as `curve_ids`, `metadata_query`, or `data_type` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **API key**: Obtain an entitled API key from your LSEG/RDMS administrator for the specific host you will connect to. Store it securely and supply it — including the `apikey-v1 ` prefix — as the `api_key` connection option.
- **Base URL**: Use the host that matches your entitlement (e.g. `https://oilprod1.rdms.refinitiv.com` for oil, `https://freightprod1.rdms.refinitiv.com` for freight). Confirm the exact host in the per-host Swagger UI at `https://<host>/api/swagger`.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one, supplying `base_url` and `api_key`.
3. Set `externalOptionsAllowList` to `curve_ids,metadata_query,scenario_id,start_date,result_timezone,ingestion_mode,window_days,max_partitions,max_results,data_type,fields,filter,order_by,page_size` (required for this connector to pass table-specific options).

Create **one connection per host** you need to ingest from (e.g. one for oil, one for freight).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of three logical tables:

- `curve_values`
- `curve_metadata`
- `tabular_data`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `curve_values` | Time-series / forecast values, one row per observation. | `append` (default) or `cdc` | `curve_id, scenario_id, forecast_date, value_date` | `value_date` (append) / `last_update_time` (cdc) |
| `curve_metadata` | Curve catalog (metadata tags) matching a query. | `snapshot` | `curve_id` | n/a |
| `tabular_data` | Provider tabular datasets (JODI, OPIS, IIR flows/fixtures, etc.). | `snapshot` | `data_type, country, product, flow, period` | n/a |

Notes:

- **Every LDMS value is uniquely keyed by `curve_id + scenario_id + forecast_date + value_date`.** Actuals use `forecast_date = 2000-01-01T00:00:00`; `scenario_id = 0` is standard PointConnect data.
- **`curve_values` ingestion mode is selectable** via the `ingestion_mode` table option:
  - `append` (default, suited to POC): cursors on `value_date` — actuals stream forward by observation date.
  - `cdc` (recommended for production): cursors on `last_update_time` so restatements (corrections, new forecasts) upsert on the primary key. LDMS publishes **no delete feed** — corrections arrive as revised values — so `cdc` (not `cdc_with_deletes`) is the correct mode.
- **`curve_values` is read in parallel.** It is a partitioned stream: the connector splits the requested `value_date` range into windowed partitions read concurrently by executors via `POST /api/v1/CurveValuesBatch`. The other two tables are read on a single driver. The read is bounded by an init-time snapshot so `Trigger.AvailableNow` runs terminate; data arriving after a run starts is picked up on the next trigger.
- **`tabular_data` columns are data-type dependent.** The bootstrap schema models the canonical JODI shape (`country`, `product`, `flow`, `period`, `value`, `unit`); always pin an explicit `fields` list, since columns can be added between releases and default column order is not guaranteed.
- **`curve_metadata`** returns a small typed core (`curve_id`, `alias`, `name`) plus a `metadata_json` string holding the full tag set as JSON, since tags vary per deployment.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (`curve_values`, `curve_metadata`, or `tabular_data`) |
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
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

Table-specific options are passed via the pipeline spec under `table_configuration`. All defaults below reflect the connector implementation.

#### `curve_values`

| Option | Required | Default | Description |
|---|---|---|---|
| `curve_ids` | No* | — | Comma-separated `CurveID`s to fetch. If omitted, curves are resolved from `metadata_query`. |
| `metadata_query` | No* | `*` | `Metadata/Search` query used to resolve curves when `curve_ids` is not given (supports `+`/`-` weighting and `Tag.is=Value`, e.g. `Geography.is=Belgium`). |
| `scenario_id` | No | `0` | `ScenarioID` to request. `0` is standard PointConnect data. |
| `start_date` | No | epoch | ISO 8601 lower bound for the first-run `value_date` backfill (used when there is no stored cursor yet). |
| `result_timezone` | No | `UTC` | `ResultTimezone` applied to returned values. |
| `ingestion_mode` | No | `append` | `append` (cursor `value_date`) or `cdc` (cursor `last_update_time`). |
| `window_days` | No | `30` | Size in days of each parallel `value_date` partition window. |
| `max_partitions` | No | `500` | Upper bound on the number of windows produced per micro-batch. If a bounded range would exceed this, the effective window is widened so the partition count stays bounded. |
| `max_results` | No | `1000` | Page size used when resolving curves via `metadata_query`. |

\* Provide either `curve_ids` or rely on `metadata_query` (which defaults to `*`, matching all curves the key can see).

#### `curve_metadata`

| Option | Required | Default | Description |
|---|---|---|---|
| `metadata_query` | No | `*` | `Metadata/Search` query selecting which curves the catalog returns. |
| `max_results` | No | `1000` | Page size for `Metadata/Search` (paged via `MaxResults` / `SkipRows`). |

#### `tabular_data`

| Option | Required | Default | Description |
|---|---|---|---|
| `data_type` | **Yes** | — | Provider dataset type, e.g. `JODI`. Discoverable via `GET /api/v1/TabularData/DataTypes`. |
| `fields` | No (recommended) | all | Comma-separated column projection. Pin this explicitly; it also restricts the reported schema. |
| `filter` | No | — | `TabularData` `Filter` expression. |
| `order_by` | No | — | `TabularData` `OrderBy` expression (`+`/`-` prefixes). |
| `page_size` | No | `1000` | Page size for offset paging (`PageSize` / `SkipSize`). |

## Data Type Mapping

LDMS / JSON fields are mapped to Spark `StructType` types as follows:

| LDMS / JSON | Example Fields | Spark Type |
|---|---|---|
| number (value) | `value` | `DoubleType` |
| date / datetime (ISO 8601, UTC) | `value_date`, `forecast_date`, `last_update_time` | `TimestampType` |
| CurveID, alias, tag values, status | `curve_id`, `alias`, `name` | `StringType` |
| ScenarioID | `scenario_id` | `IntegerType` |
| tabular fields | `country`, `product`, `flow`, `period`, `unit` | `StringType`; numeric → `DoubleType` |

Notes: all dates are UTC. Actuals use `forecast_date = 2000-01-01T00:00:00`. `scenario_id = 0` is standard data (non-zero returns no data for PointConnect). Values are **not** coerced in the connector — the framework casts records to the declared schema.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

Configure a `pipeline_spec` that references a Unity Catalog connection using this connector (pointed at the correct host) and one or more tables with their `table_configuration` options.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "lseg_ldms_oil_connection",
    "object": [
      {
        "table": {
          "source_table": "curve_values",
          "table_configuration": {
            "curve_ids": "700350000,700350002",
            "scenario_id": "0",
            "start_date": "2021-01-01T00:00:00Z",
            "result_timezone": "UTC",
            "ingestion_mode": "cdc",
            "window_days": "30"
          }
        }
      },
      {
        "table": {
          "source_table": "curve_metadata",
          "table_configuration": {
            "metadata_query": "Geography.is=Belgium",
            "max_results": "1000"
          }
        }
      },
      {
        "table": {
          "source_table": "tabular_data",
          "table_configuration": {
            "data_type": "JODI",
            "fields": "country,product,flow,period,value,unit",
            "order_by": "+period"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your LDMS `base_url` and `api_key` for the target host.
- For `curve_values`, provide either `curve_ids` or a `metadata_query` that resolves to the curves you want.
- For `tabular_data`, `data_type` is required.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g. a scheduled job or workflow).

#### Best Practices

- **Start small**: Begin with an explicit short `curve_ids` list and a recent `start_date` to validate configuration and data shape before broadening `metadata_query`.
- **Use `cdc` for production `curve_values`**: Set `ingestion_mode` to `cdc` so corrections and new forecasts upsert on the primary key. Use `append` only for simple forward-only POCs.
- **Bound the first run**: Set `start_date` to a recent cutoff to avoid an unbounded full-history backfill; the first run without a cursor reads from the epoch.
- **Tune parallelism**: Adjust `window_days` and `max_partitions` to balance executor fan-out against per-request volume. Very long backfills automatically widen the window to respect `max_partitions`.
- **Pin `fields` for `tabular_data`**: Column sets can change between releases; an explicit `fields` list keeps the schema stable.
- **Respect rate limits**: LDMS enforces per-**user** rate limits aggregated across all of that user's keys. Breaches return HTTP 403; the connector retries 403/429/5xx with exponential backoff (honoring `Retry-After`). Stagger schedules if you see sustained rate limiting.

#### Troubleshooting

**Common Issues:**

- **Authentication failures (`401`)**: Verify the `api_key` is correct, not expired, and includes the `apikey-v1 ` prefix. Check `GET /api/v1/KeyStatus` for remaining key validity.
- **Missing curves / empty results**: All data permissions are tied to the key (DACS). A curve the key cannot see returns an `Error` status and is silently skipped rather than failing the read — confirm entitlements with your LSEG administrator, and that you are pointed at the correct host.
- **Rate limiting (`403`)**: Rate limits are per user and aggregated across keys. Reduce concurrency, widen schedule intervals, or increase `window_days` to reduce the number of parallel requests.
- **Truncated batches**: When a `CurveValuesBatch` window exceeds the value cap it returns status `Truncated`; the connector automatically splits the window and re-reads so no observations are dropped. Very dense curves over long windows may need a smaller `window_days`.
- **`400 Bad Request`**: Usually a bad date format — ensure `start_date` and related values are ISO 8601.
- **`tabular_data` requires `data_type`**: The read fails fast if `data_type` is not provided.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/lseg_ldms/lseg_ldms.py`
- Connector schemas: `src/databricks/labs/community_connector/sources/lseg_ldms/lseg_ldms_schemas.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/lseg_ldms/lseg_ldms_api_doc.md`
- Official vendor documentation: `LDMS REST API Interface Guide v25.0.0.pdf` (LSEG, Issue 25.0.0, 31 Mar 2025)
- Per-host Swagger UI: `https://<host>/api/swagger` ("RDMS API v1") — authoritative for field-level request/response models, exact parameter casing, and numeric rate-limit/batch caps.
