# Lakeflow remberg Community Connector

This documentation provides setup instructions and reference information for the remberg source connector.

remberg (https://remberg.de) is an asset-centric maintenance / field-service
platform ("XRM"). The connector ingests assets, work orders, tickets, work
requests, organizations, parts, contacts, users and forms from the remberg
public REST API (`https://api.remberg.de`).

## Prerequisites

- A remberg account whose user has API access rights (reach out to
  support@remberg.de or your customer success contact if the API option is
  not visible in your account).
- A remberg API key, created under **Settings > Data > API** in the remberg
  web app. The key inherits the access rights of the user who created it, so
  create it as a user who can read all the objects you want to ingest.
  **Note:** API keys expire after one year by default — plan a rotation.

## Setup

### Required Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `api_key` | string | Yes | remberg API key from Settings > Data > API. Store as a Databricks secret. | `rmbg_…` |
| `base_url` | string | No | API root, no trailing slash. Defaults to `https://api.remberg.de`. | `https://api.remberg.de` |

The connector supports extra table-specific options (see
[Table Configurations](#table-configurations)), so `externalOptionsAllowList`
is a **required** connection option. Set it to exactly:

```
start_timestamp,lookback_seconds,limit,max_records_per_batch
```

### How to obtain the API key

1. Sign in to remberg as a user with API access rights.
2. Go to **Settings > Data > API** and click **Add API Key** (top right).
3. Give the key a name and copy the secret to your clipboard — it is the
   value for `api_key`.
4. Store it as a Databricks secret and reference it from the connection.

The key is sent by the connector in an HTTP header literally named
`authorization` (raw key, no `Bearer` prefix) — this is remberg's documented
authentication scheme; no other method is supported.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Include `start_timestamp,lookback_seconds,limit,max_records_per_batch` in
   the connection's `externalOptionsAllowList`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

All primary keys are the remberg object `id` (a stable hex string). No
object exposes deleted records, so deletes do not propagate
(re-ingest with a fresh pipeline if you need hard-delete reconciliation).

| Object | Ingestion | Cursor | Notes |
|---|---|---|---|
| `assets` | CDC (incremental) | `updatedAt` | machines / installed base |
| `work_orders` | CDC (incremental) | `updatedAt` | |
| `tickets` | CDC (incremental) | `updatedAt` | includes related-object reference arrays and `customPropertyValues` |
| `work_requests` | CDC (incremental) | `updatedAt` | |
| `organizations` | CDC (incremental) | `updatedAt` | |
| `parts` | CDC (incremental) | `updatedAt` | |
| `contacts` | Snapshot (full refresh) | — | list records carry no `updatedAt`, so no usable cursor |
| `users` | Snapshot (full refresh) | — | records carry no timestamps |
| `forms` | Snapshot (full refresh) | — | the endpoint cannot filter on `updatedAt` |

Incremental strategy: CDC tables are read as bounded `updatedAt` ranges
(`updatedAtFrom`/`updatedAtUntil` server-side filters, inclusive) from the
stored cursor (minus a small read-time lookback) up to the trigger's start
time, page by page until drained. The first sync is a full backfill unless
`start_timestamp` is set. Snapshot tables are re-listed in full each run and
upserted on `id`.

Special columns:
- `tickets.customPropertyValues[].value` and `[].associationValue[]` are
  user-defined and untyped in remberg; non-string values arrive
  JSON-serialized as strings.
- Column names are kept exactly as the remberg API returns them (camelCase),
  so rows map 1:1 to the official API documentation.

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
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Special `table_configuration` options

| Option | Applies to | Required | Description |
|---|---|---|---|
| `start_timestamp` | CDC tables | No | ISO-8601 UTC lower bound for the very first sync (e.g. `2024-01-01T00:00:00.000Z`). Default: unbounded full backfill. |
| `lookback_seconds` | CDC tables | No | Seconds subtracted from the cursor at read time to re-capture records updated while a range was being paginated. Default `300`. |
| `limit` | All tables | No | Page size for the remberg list endpoints. Default `1000` (the server maximum). |
| `max_records_per_batch` | CDC tables | No | Per-microbatch cap on emitted rows, applied at page granularity. Default: drain the whole range in one microbatch. |

## Data Type Mapping

| remberg (OpenAPI) type | Databricks type |
|---|---|
| `string` (ids, enums, free text) | `STRING` |
| `string, format: date-time` | `TIMESTAMP` |
| `number` | `DOUBLE` (`forms.counter` is `BIGINT`) |
| nested object | `STRUCT` |
| array | `ARRAY` of the mapped element type |
| untyped custom-property values | JSON-serialized `STRING` |

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Optionally set the table-specific options described above, e.g.:

```json
{
  "pipeline_spec": {
      "connection_name": "remberg_connection",
      "object": [
        {
            "table": {
                "source_table": "assets"
            }
        },
        {
            "table": {
                "source_table": "work_orders",
                "table_configuration": {
                    "start_timestamp": "2024-01-01T00:00:00.000Z",
                    "max_records_per_batch": "50000"
                }
            }
        }
      ]
  }
}
```
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline.
- **Use Incremental Sync**: The six CDC tables only fetch changes after the
  first backfill — prefer them over re-snapshotting where possible.
- **Set Appropriate Schedules**: remberg rate limits are strict (10 requests
  per second burst and 25 requests per 5 seconds sustained, per endpoint).
  The connector self-throttles to ~4 requests/second per endpoint and honors
  `Retry-After` on 429s, but avoid running many concurrent pipelines against
  the same remberg user's API key.
- **Rotate the API key**: remberg API keys expire after one year by default;
  schedule a rotation before expiry to avoid authentication failures.

#### Troubleshooting

**Common Issues:**

- **HTTP 401/403**: the API key is wrong, expired (1-year default lifetime),
  or its creating user lacks read rights on the requested object.
- **HTTP 429**: rate limiting. The connector backs off automatically; if it
  persists, reduce pipeline concurrency against the same remberg user.
- **Missing objects/rows**: the API key applies the access rights of the user
  who created it — records that user cannot see are not returned.
- **Deletes not reflected**: remberg exposes no deletions feed; deleted
  records simply stop appearing in the source and remain in the destination.

## References

- Developer portal: https://developers.remberg.de
- Getting started (API keys): https://developers.remberg.de/update/docs/getting-started
- Rate limiting: https://developers.remberg.de/update/docs/rate-limiting
- OpenAPI specs: https://developers.remberg.de/openapi
- Connector API research notes: [`remberg_api_doc.md`](remberg_api_doc.md)
