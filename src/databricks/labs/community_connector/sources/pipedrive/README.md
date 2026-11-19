# Lakeflow Pipedrive Community Connector

This documentation describes how to configure and use the **Pipedrive** Lakeflow community connector to ingest CRM data from the Pipedrive REST API (v1 and v2) into Databricks.

## Prerequisites

- **Pipedrive account**: A user with access to the data you want to ingest. The connector can only read records visible to the user who owns the API token.
- **Personal API token**: Created in Pipedrive and supplied to the connector as `api_token`.
- **Network access**: The environment running the connector must be able to reach `https://api.pipedrive.com` (or `https://<company_domain>.pipedrive.com`).
- **Optional add-ons**: The `projects` and `tasks` tables need the Projects feature. If it is not enabled on your account, these tables return no rows and log a warning instead of failing.

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
| `deals` | Live deals: open, won and lost, not archived | `cdc_with_deletes` | `id` | `update_time` |
| `deals_archived` | Archived deals | `cdc_with_deletes` | `id` | `update_time` |
| `persons` | Contacts | `cdc` | `id` | `update_time` |
| `organizations` | Organizations | `cdc` | `id` | `update_time` |
| `activities` | Activities (calls, meetings, tasks, and so on) | `cdc` | `id` | `update_time` |
| `products` | Products | `cdc` | `id` | `update_time` |
| `notes` | Notes | `cdc` | `id` | `update_time` |
| `leads` | Live (not archived) leads | `cdc_with_deletes` | `id` | `update_time` |
| `deal_flow` | Change log of each deal (field changes, stage moves) | `cdc` | `id` | `log_time` |
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

- **`deals`, `deals_archived`, `persons`, `organizations`, `activities`, `products`** use the v2 API with `updated_since`, sorted by `update_time` ascending. Reads stop at the pipeline's start time minus `lookback_seconds` (the settle margin), so a change is read once it is at least that old and triggered runs terminate. The margin covers writes that Pipedrive commits up to `lookback_seconds` after their `update_time`; a write that becomes visible later than that, with an `update_time` older than the cursor, is not picked up until a full refresh. Each micro-batch holds at most `max_records_per_batch` records and resumes from the cursor. The cursor is the newest `update_time` read, plus the ids already read at that exact second, so rows that share a second are neither lost nor read twice. For `products`, the upper bound is applied client-side.
- **`notes` and `leads`** use the v1 API with `updated_since`, sorted by `update_time` and then `id`. Every page request is keyed on the last `update_time` read, so a row archived, deleted or edited during a long walk cannot shift other rows out of the read (only rows inside one same-second group are re-read). Each micro-batch holds at most `max_records_per_batch` records. The settle margin defaults to five minutes for these tables.
- **`deal_flow`** selects deals updated since the cursor (minus the lookback), then fetches each deal's change log and keeps entries whose `log_time` falls in the window (also widened by the lookback, so an entry logged just before the cursor is picked up). It walks `/deals`, then the archived deals, then re-scans deals changed since the archived pass started. The deal query has no upper time bound, so a deal that is edited, archived or unarchived while a long walk is running is still visited. Such a deal can be visited twice. Change-log rows don't change and are upserted on `id`, so a second visit doesn't create duplicates. Batches split between deals, never inside one deal's log.
- **Snapshot tables** are fully re-read on every run.

### Delete handling

`deals`, `deals_archived` and `leads` support `cdc_with_deletes`. Each has its own delete stream and offset, and emits tombstones that carry the primary key and `update_time`:

- `deals`: deals with `status=deleted` (soft deletes, kept by Pipedrive for 30 days, so run the pipeline at least that often), and deals archived since the last run (they move to `deals_archived`).
- `deals_archived`: archived deals with `status=deleted`, and deals unarchived since the last run (they move back to `deals`). The unarchive source lists every live deal edited since the last run, so this stream tombstones all of them, not only the unarchived ones. Expect it to carry a large share of deal edits.
- `leads`: leads archived since the last run (they move to `leads_archived`). Pipedrive does not document `updated_since` for `/v1/leads/archived`. If the server ignores it, every micro-batch of that stream scans the archived leads from the start, which is correct but costs more requests.

Tombstones for keys that are not in the table are harmless. Moves rely on archiving and unarchiving bumping `update_time`, and the `status=deleted` rows are re-checked client-side. Do not union `deals` and `deals_archived` to count deals: a deal that has just moved can appear in both until the delete stream runs. Other tables do not propagate deletes. Snapshot tables replace their contents each run.

### Special columns

- **`custom_fields`** (deals, persons, organizations, products, projects, leads) is stored as a **JSON string**. Parse it downstream. Custom field keys are Pipedrive's hashed identifiers; use the `*_fields` tables to map keys to names.
- `options`, `options_deleted` and `subfields` in the `*_fields` tables are `ARRAY<STRING>`, where each element is a JSON-encoded object. `old_value`, `new_value`, `additional_data` and `origin_id` in `deal_flow` are JSON strings.
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
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode (every table in this connector). |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Pipedrive-specific options

All of these are optional and must be listed in `externalOptionsAllowList`.

| Option | Applies to | Description |
|---|---|---|
| `max_records_per_batch` | `deals`, `deals_archived`, `persons`, `organizations`, `activities`, `products`, `notes`, `leads`, `deal_flow` | Maximum records per micro-batch. Default `5000`. Exact for every table except `deal_flow`, where one deal's change log is never split, so a batch can exceed the cap. |
| `page_size` | Incremental tables | Page size for incremental reads. Capped at 500 for v2 tables and at the endpoint maximum for v1 tables. |
| `lookback_seconds` | Incremental tables | Settle margin: a change is read only once it is at least this many seconds old, which covers writes committed up to this long after their `update_time`. Raise it if your account sees later commits. For `deal_flow` it instead widens the log-time window backwards, so entries logged shortly before the cursor are re-read (rows are upserted on `id`). Defaults: `60` for v2 tables, `300` for `notes`, `leads` and the `leads` delete stream, `3600` for `deal_flow`. |
| `include_fields` | v2 incremental tables | Comma-separated optional fields to request from the API. Defaults to `source_lead_id` for deals. |

## Data Type Mapping

| Pipedrive type | Databricks type |
|---|---|
| Integer IDs (`id`, `owner_id`, `org_id`, and so on) | `BIGINT`. The exceptions are the lead `id` and the lead `label_ids`, which are UUID strings (`STRING`, `ARRAY<STRING>`). |
| Decimal numbers (`value`, `probability`, `arr`, `mrr`, `acv`) | `DOUBLE` |
| Boolean | `BOOLEAN` |
| Text, timestamps, dates | `STRING` |
| Lists of numeric IDs (`label_ids`, `deal_ids`, `assignee_ids`) | `ARRAY<BIGINT>` (`leads.label_ids` is `ARRAY<STRING>`) |
| Contact entries (`emails`, `phones`, `im`) | `ARRAY<STRUCT<value, primary, label>>` |
| Address | `STRUCT` |
| Custom fields and deal-flow values | `STRING` (JSON) |
| Field options and subfields (`options`, `options_deleted`, `subfields`) | `ARRAY<STRING>`, each element JSON |

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
