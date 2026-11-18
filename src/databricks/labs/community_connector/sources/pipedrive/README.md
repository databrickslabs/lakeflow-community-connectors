# Lakeflow Pipedrive Community Connector

This documentation describes how to configure and use the **Pipedrive** Lakeflow community connector to ingest CRM data from the Pipedrive REST API (v1 and v2) into Databricks.

## Prerequisites

- **Pipedrive account**: A user with access to the data you want to ingest. The connector can only read records visible to the user who owns the API token.
- **Personal API token**: Created in Pipedrive and supplied to the connector as `api_token`.
- **Network access**: The environment running the connector must be able to reach `https://api.pipedrive.com` (or `https://<company_domain>.pipedrive.com`).
- **Optional add-ons**: The `projects` and `tasks` tables need the Projects feature. If it is not enabled on your account, these tables return no rows instead of failing.

## Setup

### Required Connection Parameters

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `api_token` | string | yes | Pipedrive personal API token, sent in the `x-api-token` header. | `abc123...` |
| `company_domain` | string | no | Company subdomain. Accepts `mycompany`, `mycompany.pipedrive.com` or a full URL. If omitted, requests go to `https://api.pipedrive.com`. | `mycompany` |
| `start_date` | string | no | RFC 3339 timestamp used as the initial cursor for incremental tables on the first read. Defaults to the connector's built-in start date. | `2020-01-01T00:00:00Z` |
| `burst_limit` | integer | no | Maximum requests per 2-second window used by the client-side throttle. Lower it if you see HTTP 429 responses (Pipedrive allows 20 requests per 2 seconds on the lowest plan). | `20` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that may be passed to the connector. This connector supports table-specific options, so this parameter must be set to the full list below. | `max_records_per_batch,page_size,lookback_seconds,include_fields` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`max_records_per_batch,page_size,lookback_seconds,include_fields`

### Obtaining the Required Parameters

- **API token**:
  1. Sign in to Pipedrive.
  2. Open **Settings > Personal preferences > API**.
  3. Copy your personal API token and use it as the `api_token` connection option.
  4. Store the token securely. It carries the permissions of the user it belongs to.
- **Company domain** (optional): the subdomain in your Pipedrive URL, for example `mycompany` from `https://mycompany.pipedrive.com`.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `max_records_per_batch,page_size,lookback_seconds,include_fields`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of 25 tables. Table names are lowercase and must be used exactly as shown.

| Table | Description | Ingestion Type | Primary Key | Cursor |
|---|---|---|---|---|
| `deals` | Open, won and lost deals | `cdc_with_deletes` | `id` | `update_time` |
| `deals_archived` | Archived deals | `cdc_with_deletes` | `id` | `update_time` |
| `persons` | Contacts | `cdc` | `id` | `update_time` |
| `organizations` | Organizations | `cdc` | `id` | `update_time` |
| `activities` | Activities (calls, meetings, tasks, and so on) | `cdc` | `id` | `update_time` |
| `products` | Products | `cdc` | `id` | `update_time` |
| `notes` | Notes | `cdc` | `id` | `update_time` |
| `leads` | Active leads | `cdc` | `id` | `update_time` |
| `deal_flow` | Change log of each deal (field changes, stage moves) | `append` | `id` | `log_time` |
| `leads_archived` | Archived leads | `snapshot` | `id` | n/a |
| `pipelines` | Pipelines | `snapshot` | `id` | n/a |
| `stages` | Pipeline stages | `snapshot` | `id` | n/a |
| `users` | Users | `snapshot` | `id` | n/a |
| `lead_labels` | Lead labels | `snapshot` | `id` | n/a |
| `files` | File metadata (content is not downloaded) | `snapshot` | `id` | n/a |
| `projects` | Projects, including archived ones | `snapshot` | `id` | n/a |
| `tasks` | Tasks | `snapshot` | `id` | n/a |
| `activity_types` | Activity types | `snapshot` | `id` | n/a |
| `currencies` | Currencies | `snapshot` | `id` | n/a |
| `deal_fields` | Deal field definitions | `snapshot` | `key` | n/a |
| `person_fields` | Person field definitions | `snapshot` | `key` | n/a |
| `organization_fields` | Organization field definitions | `snapshot` | `key` | n/a |
| `product_fields` | Product field definitions | `snapshot` | `key` | n/a |
| `activity_fields` | Activity field definitions | `snapshot` | `key` | n/a |
| `deal_products` | Products attached to deals (all non-deleted deals, including archived) | `snapshot` | `id` | n/a |

### Incremental behavior

- **`deals`, `deals_archived`, `persons`, `organizations`, `activities`, `products`** use the v2 API with `updated_since`, sorted by `update_time` ascending. Reads are bounded by the time the pipeline update started, so triggered runs terminate. Each micro-batch stops after roughly `max_records_per_batch` records and resumes from the last cursor. Boundary rows can be re-read; the primary-key upsert de-duplicates them. For `products`, the upper bound is applied client-side.
- **`notes` and `leads`** use the v1 API with `updated_since`, sorted by `id`. Each micro-batch reads the whole range up to the start of the run, so `max_records_per_batch` does not apply. A one-hour lookback is applied by default.
- **`deal_flow`** is append-only. It selects deals updated since the cursor (minus a one-hour lookback), then fetches each deal's change log and keeps entries whose `log_time` falls in the window. Batches split between deals, never inside one deal's log.
- **Snapshot tables** are fully re-read on every run.

### Delete handling

`deals` and `deals_archived` support `cdc_with_deletes`. Deleted deals are detected through a separate read with `status=deleted`. Pipedrive keeps deleted deals for 30 days, so a deal must be seen as deleted within that window. Other tables do not propagate deletes. Snapshot tables replace their contents each run.

### Special columns

- **`custom_fields`** (deals, persons, organizations, products, projects, leads) is stored as a **JSON string**. Parse it downstream. Custom field keys are Pipedrive's hashed identifiers; use the `*_fields` tables to map keys to names.
- `options`, `options_deleted` and `subfields` in the `*_fields` tables, and `old_value`, `new_value`, `additional_data` and `origin_id` in `deal_flow`, are also JSON strings.
- `persons.emails`, `phones` and `im` are arrays of `{value, primary, label}`. `postal_address` is a struct.
- Timestamps and dates are strings as returned by Pipedrive. Cast them downstream.
- `deals` and `deals_archived` include `source_lead_id` by default.

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
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables (`deal_flow`) do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Pipedrive-specific options

All of these are optional and must be listed in `externalOptionsAllowList`.

| Option | Applies to | Description |
|---|---|---|
| `max_records_per_batch` | `deals`, `deals_archived`, `persons`, `organizations`, `activities`, `products`, `deal_flow` | Soft cap on records per micro-batch. Default `5000`. |
| `page_size` | Incremental tables | Page size for incremental reads. Capped at 500 for v2 tables and at the endpoint maximum for v1 tables. |
| `lookback_seconds` | Incremental tables | Seconds subtracted from the query lower bound on the first read of each trigger. The stored cursor is not shifted. Defaults: `0` for v2 tables, `3600` for `notes`, `leads` and `deal_flow`. |
| `include_fields` | v2 incremental tables | Comma-separated optional fields to request from the API. Defaults to `source_lead_id` for deals. |

## Data Type Mapping

| Pipedrive type | Databricks type |
|---|---|
| Integer IDs (`id`, `owner_id`, `org_id`, and so on) | `BIGINT` |
| Decimal numbers (`value`, `probability`, `arr`, `mrr`, `acv`) | `DOUBLE` |
| Boolean | `BOOLEAN` |
| Text, timestamps, dates | `STRING` |
| Lists of IDs (`label_ids`) | `ARRAY<BIGINT>` |
| Contact entries (`emails`, `phones`, `im`) | `ARRAY<STRUCT<value, primary, label>>` |
| Address | `STRUCT` |
| Custom fields, field options, flow values | `STRING` (JSON) |

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Add table-specific options under `table_configuration` where needed:

```json
{
  "pipeline_spec": {
    "connection_name": "my_pipedrive_connection",
    "object": [
      {
        "table": {
          "source_table": "deals",
          "table_configuration": {
            "max_records_per_batch": "5000",
            "lookback_seconds": "300"
          }
        }
      },
      {
        "table": {
          "source_table": "persons"
        }
      },
      {
        "table": {
          "source_table": "deal_flow",
          "table_configuration": {
            "max_records_per_batch": "2000"
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

- **Start small**: Begin with a few tables such as `users`, `pipelines`, `stages` and `deals` to validate the setup.
- **Use incremental sync**: Incremental tables read only changed records after the first run.
- **Mind API limits**: Pipedrive enforces a per-token burst limit and a daily token budget. The connector reads sequentially through one throttled client. Schedule large tables (`deal_flow`, `deal_products`) away from other API users of the same token.
- **`deal_flow` and `deal_products` are expensive**: `deal_flow` makes one request per changed deal, and `deal_products` walks every non-deleted deal. Run them less often than the core tables.
- **Set appropriate schedules**: Balance freshness against API usage.

#### Troubleshooting

**Common Issues:**

- **HTTP 401 / 403**: The token is invalid, or the owning user lacks permission to the object. Regenerate the token and check user permissions. An HTML 403 page usually means the request was blocked by Cloudflare, for example from repeated rate-limit violations.
- **HTTP 429 (rate limited)**: Lower `burst_limit` on the connection and reduce how many tables run at the same time.
- **`projects` or `tasks` are empty**: The Projects add-on is probably not enabled for your account.
- **Deleted deals missing**: Pipedrive retains deleted deals for only 30 days. Run `deals` at least that often.
- **Missing records**: The user who owns the token sees only the records their visibility settings allow.
- **Invalid `start_date`**: It must be a valid RFC 3339 timestamp.

## References

- [Pipedrive API reference](https://developers.pipedrive.com/docs/api/v1)
- [Pipedrive API v2 overview and migration guide](https://pipedrive.readme.io/docs/pipedrive-api-v2)
- [Pipedrive rate limiting](https://pipedrive.readme.io/docs/core-api-concepts-rate-limiting)
- [Airbyte `source-pipedrive` connector](https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-pipedrive)
