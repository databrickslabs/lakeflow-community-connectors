# Lakeflow Mailchimp Community Connector

This documentation describes how to configure and use the **Mailchimp** Lakeflow community connector to ingest marketing campaign-performance data from the **Mailchimp Marketing API v3.0** into Databricks.

The connector ingests four tables — `campaigns`, `reports`, `lists` (audiences), and `members` — all as change-data-capture (`cdc`) tables suitable for consolidating your email marketing analytics in Unity Catalog.

## Prerequisites

- **Mailchimp account**: You need a Mailchimp account with access to the audiences (lists), campaigns, and reports you want to read.
- **Marketing API key**:
  - Must be created in Mailchimp and supplied to the connector as the `api_key` option.
  - The key ends with a data-center suffix (e.g. `...-us21`), which the connector uses to find your account's API host.
- **Network access**: The environment running the connector must be able to reach `https://<dc>.api.mailchimp.com` (where `<dc>` is your data center, e.g. `us21`).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection options exposed by the connector.

| Name        | Type   | Required | Description                                                                                                                        | Example                                  |
|-------------|--------|----------|------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| `api_key`   | string | yes      | Mailchimp Marketing API key used for HTTP Basic authentication. The account data center is derived from the key's `-<dc>` suffix.  | `0123456789abcdef0123456789abcde-us21`   |
| `server_prefix` | string | no   | Data center / server prefix (e.g. `us21`). Only needed for **legacy** keys that have no `-<dc>` suffix; otherwise it is derived automatically from `api_key`. | `us21`                                   |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector supports table-specific options, so this parameter must be set. | `max_records_per_batch,window_seconds,start_timestamp,lookback_seconds` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`max_records_per_batch,window_seconds,start_timestamp,lookback_seconds`

> **Note**: Table-specific options such as `window_seconds` and `start_timestamp` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### How Authentication Works

Mailchimp authenticates a stored, long-lived **API key** using HTTP Basic authentication. There is no interactive browser login and no OAuth app to register for this connector — you supply the key once and the connection uses it on every request. (Mailchimp also offers OAuth 2 for agencies accessing *other* accounts; this connector does not use it.)

The connector derives your account's API host from the key itself:

- Every Mailchimp API key ends with a data-center suffix after the final hyphen, e.g. `0123456789abcdef0123456789abcde-us21`.
- The suffix (`us21`) is your account's data center and becomes the subdomain of the API root: `https://us21.api.mailchimp.com/3.0`.

If you have a rare, legacy key with **no** `-<dc>` suffix, the data center cannot be inferred; in that case supply it explicitly with the `server_prefix` option (e.g. `us21`). You can also find your data center in the URL of your account's API-keys page (`https://<dc>.mailchimp.com/account/api/`).

### Obtaining the API Key

1. Log in to your Mailchimp account.
2. Click your profile name and go to **Account & billing → Extras → API keys**.
3. Under **Your API keys**, click **Create A Key**.
4. Copy the generated key and store it securely.
5. Use this value as the `api_key` connection option. Note the data-center suffix (the text after the last hyphen, e.g. `us21`).

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `max_records_per_batch,window_seconds,start_timestamp,lookback_seconds` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Mailchimp connector exposes a **static list** of four tables (use the exact lowercase names shown):

- `campaigns`
- `reports`
- `lists`
- `members`

### Object summary, primary keys, and ingestion mode

All four tables are ingested as `cdc` (upsert by primary key). The connector defines the primary key and incremental cursor for each table:

| Table       | Description                                                       | Ingestion Type | Primary Key            | Incremental Cursor |
|-------------|-------------------------------------------------------------------|----------------|------------------------|--------------------|
| `campaigns` | All campaigns in the account                                      | `cdc`          | `id`                   | `create_time`      |
| `reports`   | One performance report per **sent** campaign (`id` = campaign id) | `cdc`          | `id`                   | `send_time`        |
| `lists`     | Audiences (a.k.a. lists) in the account                           | `cdc`          | `id`                   | `date_created`     |
| `members`   | Contacts, **fanned out per audience** (nested under each list)    | `cdc`          | `["list_id", "id"]`    | `last_changed`     |

### Important behaviors to know before you sync

These are the gotchas most likely to surprise you. Read them before scheduling large syncs.

- **`members` is nested per audience.** Mailchimp has no account-wide "all members" endpoint. On every read the connector enumerates every audience with `GET /lists` and then pages `GET /lists/{list_id}/members` for each one, unioning the results. Each member row carries its own `list_id` so provenance is preserved, and the effective primary key is the composite `(list_id, id)`. The same contact subscribed to two audiences appears as two rows (one per `list_id`); `unique_email_id` identifies the same email across audiences.
- **Creation-time-only cursors on `campaigns`, `reports`, and `lists`.** These three tables expose only a creation-time filter (`create_time` / `send_time` / `date_created`) — there is no `updated_at`. Incremental windows reliably capture **new** rows, but mutable aggregate metrics on already-synced rows (opens, clicks, `report_summary`, `stats`, `list_stats`) keep accumulating after the cursor timestamp and are **not** refreshed by incremental runs. To recapture late engagement on older campaigns/reports/audiences, schedule a periodic full re-read. Audiences are few, so a full `lists` refresh is cheap.
- **`members` is the one true change-cursor table.** Its `last_changed` cursor is a genuine last-modified timestamp, so status changes, merge-field edits, and other mutations are captured on the next incremental window. Departures (unsubscribes, cleaned, archived) are surfaced as `status` = `unsubscribed` / `cleaned` / `archived` — **soft deletes** read through the normal window. Mailchimp exposes no hard-delete change feed, so no table is `cdc_with_deletes`.
- **`merge_fields` and `interests` are stored as raw-JSON string columns.** On `members`, these two fields have a key set that varies per audience and cannot map to a fixed schema, so the connector serializes each to a JSON string. Parse them downstream (e.g. `from_json`) when you need their contents.
- **Rate limit: 10 simultaneous connections per key.** Mailchimp allows a maximum of 10 concurrent requests per API key and returns HTTP `429` if you exceed it. The connector retries `429` (and transient `500`/`503`) with exponential backoff. There is no documented daily request cap — only the concurrency limit — but keep sync parallelism conservative if you share the key with other tools.

### Schema highlights

Schemas are static (Mailchimp has no schema-discovery endpoint) and are defined by the connector from the documented v3.0 response shapes:

- Nested objects are preserved as Spark `StructType` rather than flattened — e.g. `campaigns.recipients` / `settings` / `tracking` / `report_summary`, `reports.bounces` / `opens` / `clicks` / `list_stats` / `ecommerce`, `lists.contact` / `campaign_defaults` / `stats`, `members.stats` / `location`.
- `members.tags` is an array of `{id, name}` structs.
- `members.merge_fields` and `members.interests` are `string` columns holding raw JSON (dynamic per-audience keys — see above).
- All timestamp fields (`create_time`, `send_time`, `date_created`, `last_changed`, `timestamp_signup`, `timestamp_opt`) are stored as UTC ISO 8601 strings.
- `members.id` is the MD5 hex digest of the lowercased `email_address` (unique within a list); the connector keys the unioned table on `(list_id, id)`.

You usually do not need to customize the schema; it is static and driven by the connector implementation.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (`campaigns`, `reports`, `lists`, or `members`) |
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

These table-specific options apply to all four tables. They must be listed in the connection's `externalOptionsAllowList` to be passed through.

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `start_timestamp` | ISO 8601 string | No | Auto-discovered | Initial cursor lower bound used when there is no stored offset yet (first run). If omitted, the connector probes the source for the oldest cursor value and starts there — which backfills all history and can be heavy. Set this to a recent cutoff (e.g. `2026-01-01T00:00:00Z`) to limit initial backfill. |
| `window_seconds` | integer | No | `86400` (1 day) | Size of the incremental sliding time-window in seconds. Each `read_table` call advances the cursor by at most this much. **On large accounts, start with a small value** to keep per-batch volume bounded — this is the primary sizing knob for `members`. |
| `max_records_per_batch` | integer | No | `200` | Caps the number of records returned per `read_table` call. Applies only to **sorted** endpoints (`campaigns`, `members` list-fan-out), where the batch is truncated at this cap and the cursor resumes mid-window in ascending cursor order. For the **unsorted** endpoints (`reports`, `lists`) and the `members` window as a whole, each window is drained completely so no row is skipped, making this value best-effort there — use `window_seconds` to bound volume. |
| `lookback_seconds` | integer | No | `1` | Seconds subtracted from the queried `since_*` lower bound. Mailchimp's `since_*` filters are **exclusive** (strictly greater-than), so a window seeded at exactly the boundary cursor would drop that row; the lookback shifts the query back so the boundary is included (CDC upsert dedupes the small re-fetched overlap). The stored cursor keeps the raw value — only the query is shifted. Set to `0` to disable. |

> **Tip**: `members` is the highest-volume table. Because its window is always drained fully (never truncated client-side), control its batch size with a small `window_seconds` rather than `max_records_per_batch`.

## Data Type Mapping

Mailchimp JSON fields are mapped to Spark types as follows:

| Mailchimp API Type            | Example Fields                                              | Connector Spark Type          | Notes |
|-------------------------------|-------------------------------------------------------------|-------------------------------|-------|
| string                        | `id`, `name`, enums, URLs                                   | `StringType`                  | Ids, names, enum values. |
| string (ISO 8601 / RFC 3339)  | `create_time`, `send_time`, `date_created`, `last_changed`  | `StringType`                  | Stored as UTC strings (`YYYY-MM-DDThh:mm:ss+00:00`); cast to timestamp downstream. Nullable where the event has not occurred (e.g. `send_time` for unsent campaigns). |
| integer                       | `web_id`, `emails_sent`, `member_count`, counts             | `LongType`                    | All integers use `LongType` to avoid overflow. |
| number (fractional)           | `open_rate`, `click_rate`, `avg_sub_rate`                   | `DoubleType`                  | Rates are decimal fractions (0.0–1.0); multiply by 100 for percentages. |
| boolean                       | `vip`, `email_type_option`, `double_optin`, tracking flags  | `BooleanType`                 | Standard `true`/`false`. |
| object                        | `settings`, `recipients`, `stats`, `opens`, `clicks`, `location` | `StructType`             | Nested objects are preserved, not flattened. |
| array                         | `tags`                                                      | `ArrayType(StructType)`       | Array of `{id, name}` structs. |
| object with dynamic keys      | `members.merge_fields`, `members.interests`                 | `StringType` (raw JSON)       | Key set varies per audience; stored as a JSON string and parsed downstream. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the Mailchimp source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Reference the Unity Catalog connection configured with your Mailchimp `api_key`, and add one `table` entry per object you want to ingest. Place table-specific options under `table_configuration`.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "mailchimp_connection",
    "object": [
      {
        "table": {
          "source_table": "campaigns",
          "table_configuration": {
            "start_timestamp": "2026-01-01T00:00:00Z",
            "window_seconds": 604800
          }
        }
      },
      {
        "table": {
          "source_table": "reports",
          "table_configuration": {
            "start_timestamp": "2026-01-01T00:00:00Z"
          }
        }
      },
      {
        "table": {
          "source_table": "lists"
        }
      },
      {
        "table": {
          "source_table": "members",
          "table_configuration": {
            "start_timestamp": "2026-06-01T00:00:00Z",
            "window_seconds": 3600
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Mailchimp `api_key` (and, for legacy keys, `server_prefix`).
- For each `table`, `source_table` must be one of `campaigns`, `reports`, `lists`, or `members`.
- On the **first run**, set `start_timestamp` to a recent cutoff to limit initial backfill; omit it only if you intend to backfill all history.
- On **subsequent runs**, the connector resumes from the stored `cursor` and advances the window automatically.

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

#### Best Practices

- **Start small**: Begin with `lists` and `campaigns` to validate configuration and data shape before adding the high-volume `members` table.
- **Use a bounded initial window**: Set `start_timestamp` to a recent date so the first run does not backfill the full account history.
- **Tune `members` with `window_seconds`**: On large accounts, start with a small `window_seconds` (e.g. 3600) so each microbatch stays bounded across the per-audience fan-out.
- **Refresh mutable metrics periodically**: Because `campaigns` / `reports` / `lists` have creation-time-only cursors, schedule an occasional full re-read to recapture opens/clicks/stats that changed after the row was first synced.
- **Respect the concurrency limit**: Mailchimp allows a maximum of 10 simultaneous connections per API key. Keep parallelism conservative, especially if the key is shared with other tools.

#### Troubleshooting

**Common Issues:**

- **`ValueError: Mailchimp connector requires an 'api_key' option`**: The connection is missing the `api_key`. Provide it as a connection parameter.
- **`Could not determine the Mailchimp data center`**: Your API key has no `-<dc>` suffix (a rare legacy key). Supply the `server_prefix` option (e.g. `us21`).
- **Authentication failures (`401`)**: Verify the `api_key` is correct and has not been revoked.
- **Rate limiting (`429`)**: You have exceeded the 10-concurrent-connection limit. The connector retries with exponential backoff; if it persists, reduce sync parallelism or stagger schedules.
- **`Cannot determine a starting cursor`**: The connector could not find any cursor value and no `start_timestamp` was provided. Set `start_timestamp` in `table_configuration`.
- **Missing mutable metrics on old rows**: Expected for `campaigns` / `reports` / `lists` (creation-time cursors). Run a full re-read to refresh them.
- **`merge_fields` / `interests` look like text**: These are stored as raw JSON strings by design; parse them downstream (e.g. `from_json`).

## Deferred Tables

The following objects are supported by other Mailchimp integrations (Airbyte / Fivetran) and are useful for a fuller analytics model, but are **out of scope** for this initial connector (which targets campaign-performance consolidation). They are candidates for a later iteration:

| Deferred table                       | Endpoint                                             | Notes |
|--------------------------------------|------------------------------------------------------|-------|
| `automations`                        | `GET /automations`                                   | Classic automations (legacy); incremental via `create_time`. |
| `segments`                           | `GET /lists/{list_id}/segments`                      | Nested under a list; incremental via `updated_at`; PK `id`. |
| `segment_members`                    | `GET /lists/{list_id}/segments/{segment_id}/members` | Doubly nested; large volume. |
| `unsubscribes`                       | `GET /reports/{campaign_id}/unsubscribed`            | Nested under a report; composite key `(campaign_id, email_id)`; per-campaign unsubscribe events. |
| `email_activity`                     | `GET /reports/{campaign_id}/email-activity`          | Nested under a report; very high volume; composite key `(email_id, action, timestamp)`. |
| `interests` / `interest_categories`  | `GET /lists/{list_id}/interest-categories[...]`      | Reference data; snapshot only. |
| `tags`                               | member-level `tags` array; `GET /lists/{list_id}/tag-search` | Reference data. |

Adding any of these follows the same auth, pagination, and rate-limit patterns described above; the nested ones require an extra fan-out level (per campaign or per list/segment).

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/mailchimp/mailchimp.py`
- Connector schemas: `src/databricks/labs/community_connector/sources/mailchimp/mailchimp_schemas.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/mailchimp/mailchimp_api_doc.md`
- Official Mailchimp Marketing API documentation:
  - `https://mailchimp.com/developer/marketing/docs/fundamentals/`
  - `https://mailchimp.com/developer/marketing/docs/methods-parameters/`
  - `https://mailchimp.com/developer/marketing/api/campaigns/list-campaigns/`
  - `https://mailchimp.com/developer/marketing/api/reports/list-campaign-reports/`
  - `https://mailchimp.com/developer/marketing/api/lists/get-lists-info/`
  - `https://mailchimp.com/developer/marketing/api/list-members/list-members-info/`
