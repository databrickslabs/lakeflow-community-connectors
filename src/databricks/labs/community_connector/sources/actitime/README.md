# Lakeflow actiTIME Community Connector

This documentation describes how to configure and use the **actiTIME** Lakeflow community connector to ingest data from the actiTIME REST API v1 into Databricks.

The connector targets both actiTIME Online and self-hosted actiTIME (Self-Hosted 2019.2+). It exposes 16 tables covering customers, projects, tasks, time tracking, leave tracking, users and organizational reference data, plus the weekly time-approval workflow.

## Prerequisites

- **actiTIME instance**: An accessible actiTIME tenant. The base URL pattern is **per tenant**:
  - actiTIME Online: `https://online.actitime.com/<tenant>`
  - Self-hosted: `https://<your-actitime-host>` (requires actiTIME Self-Hosted 2019.2 or later)
- **Service-account user**: A dedicated actiTIME user account used for API access. The account must have:
  - The **"Allow access to the actiTIME REST API"** permission.
  - Sufficient role (typically **Admin** or a **Manager** with broad scope) to read every object you intend to ingest.
  - The **"Manage Accounts & Permissions"** permission if you need PII columns from `users` (`username`, `email`, `hired`, `release_date`). Without it, those fields are simply omitted by the API.
- **Credentials**: The username and password of the service-account user. The connector authenticates with HTTP Basic auth; there is no OAuth flow.
- **Network access**: The environment running the pipeline must be able to reach the actiTIME base URL over HTTPS.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These are the exact names exposed by the connector.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `base_url` | string | yes | Base URL of your actiTIME instance, **without a trailing slash**. The connector automatically appends `/api/v1`. | `https://online.actitime.com/acme` |
| `username` | string | yes | Username (or email) of the actiTIME service-account user used for API access. | `lakeflow-ingest` |
| `password` | string | yes | Password for the actiTIME service-account user. **Store as a Databricks secret** rather than inline. | `<DATABRICKS_SECRET>` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector supports table-specific options, so this parameter must be set. | `start_date,window_days,lookback_days,forward_days,stopAfter,user_batch_size,dateFrom,dateTo,limit,max_records_per_batch` |

The full list of supported table-specific options for `externalOptionsAllowList` is:

`start_date,window_days,lookback_days,forward_days,stopAfter,user_batch_size,dateFrom,dateTo,limit,max_records_per_batch`

> **Note**: Options such as `start_date`, `window_days`, `lookback_days`, `forward_days`, `stopAfter`, `user_batch_size`, `dateFrom`, `dateTo`, `limit`, and `max_records_per_batch` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline spec. Their names must be listed in `externalOptionsAllowList` for the connection to allow them through.

### Obtaining the Required Parameters

1. **Identify your actiTIME base URL**:
   - For actiTIME Online, locate your tenant slug from the browser address bar (e.g. `https://online.actitime.com/<tenant>`).
   - For self-hosted, use the public hostname of your actiTIME server, e.g. `https://actitime.mycompany.com`.
2. **Create a service-account user in actiTIME**:
   - Sign in as an Admin.
   - Create a dedicated user (e.g. `lakeflow-ingest`).
   - Grant the **"Allow access to the actiTIME REST API"** permission.
   - Assign **Admin** or a **Manager** role with read scope wide enough to see every customer, project, task, user, and time entry you want to ingest.
   - If PII columns from `users` are required, also grant **"Manage Accounts & Permissions"**.
3. **Set a stable password** for the service account and store it in **Databricks Secrets** (e.g. `dbutils.secrets.get(scope="actitime", key="password")`). Reference the secret as the `password` connection option.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `start_date,window_days,lookback_days,forward_days,stopAfter,user_batch_size,dateFrom,dateTo,limit,max_records_per_batch` (required so table-specific options can be passed through).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The actiTIME connector exposes a **static list of 16 tables**. Names must be used with the exact casing shown below (the table set uses camelCase for several names — `userGroups`, `userRates`, `typesOfWork`, `leaveTypes`, `workflowStatuses`, `timeZoneGroups`, `approvalStatus`).

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `customers` | Customer/client entities | `cdc` | `id` | `created` (proxy; full-list refresh — see note below) |
| `projects` | Projects belonging to customers | `cdc` | `id` | `created` (proxy; full-list refresh) |
| `tasks` | Tasks within projects | `cdc` | `id` | `created` (proxy; full-list refresh) |
| `users` | User accounts | `snapshot` | `id` | n/a |
| `timetrack` | Time-track entries per user per date | `append` | `["user_id", "date", "task_id"]` | `date` (sliding window) |
| `leavetime` | Leave-time entries per user per date | `append` | `["user_id", "date", "leave_type_id"]` | `date` (sliding window) |
| `approvalStatus` | Per-user-week time approval status | `append` | `["user_id", "week_start_date"]` | `week_start_date` (sliding window) |
| `departments` | Organizational departments | `snapshot` | `id` | n/a |
| `userGroups` | User groups (permission groupings) | `snapshot` | `id` | n/a |
| `userRates` | Per-user billing/cost rate history | `snapshot` | `["user_id", "date_from"]` | n/a |
| `typesOfWork` | Work-type categories | `snapshot` | `id` | n/a |
| `leaveTypes` | Leave-type definitions | `snapshot` | `id` | n/a |
| `workflowStatuses` | Task workflow status definitions | `snapshot` | `id` | n/a |
| `timeZoneGroups` | Time-zone groups | `snapshot` | `id` | n/a |
| `settings` | Single-record system settings | `snapshot` | `id` (synthesised as `1`) | n/a |
| `holidays` | Holiday calendar entries | `snapshot` | `["date", "time_zone_group_id"]` | n/a |

### CDC tables: full-refresh with upsert (important)

The actiTIME REST API **does not expose a `modifiedAt` field** on `customers`, `projects`, or `tasks`, and there is no "modified since" filter. A strict incremental sync is therefore not possible. On every trigger, the connector performs a **full list refresh** of each CDC table and relies on **upsert on the primary key (`id`)** downstream:

- New rows appear.
- Updates and archive flips are picked up because the connector re-lists everything.
- **Deletes are not surfaced** — actiTIME has no delete feed. Hard-deleted rows simply stop appearing, but their previous version remains in the destination table.

If physical delete tracking is essential, schedule a periodic reconciliation against the latest full snapshot.

`users` is treated as a `snapshot` table rather than CDC because the actiTIME API does not expose any timestamp field on a user (no `created`, no `modifiedAt`). The same full-refresh + upsert-on-`id` behaviour applies, and the same deletes-not-surfaced caveat holds.

### Append tables: sliding date window

`timetrack`, `leavetime`, and `approvalStatus` are append-only. The connector slices the requested date range into sliding windows and re-reads the recent past on each run to capture late edits within the editable window:

- `timetrack`, `leavetime`: cursor on the `date` column. Default window: **7 days**. Default lookback: **7 days**. The connector fans out by user batch (`user_batch_size`, default **100**) so the per-call response size stays bounded regardless of tenant scale. `stopAfter` is an opt-in cap and is not sent by default.
- `approvalStatus`: cursor on `week_start_date`. Default window: **28 days**. Default lookback: **14 days**.

Deletes of `timetrack` / `leavetime` entries are **not** surfaced. Because data is appended, history is treated as immutable downstream; if you need to reconcile deletes, periodically re-sync the open editable window.

### Snapshot tables

Snapshot tables are full-refresh on every run. `userRates` is a per-user sub-resource: the connector iterates the user list internally and fans out one request per user, then emits a synthetic `(user_id, date_from)` composite key.

### Schema notes

- `id`, `customer_id`, `project_id`, etc. are 64-bit integers (`LongType`).
- Source `created`, `submittedAt`, and `approvedAt` fields are Unix epoch milliseconds — the connector converts them to ISO-8601 UTC timestamps.
- Calendar dates (`date`, `dateFrom`, `deadline`, `hired`, `release_date`, `week_start_date`) are kept as `DateType`.
- `allowedActions.*` API objects are flattened to `allowed_actions_can_modify`, `allowed_actions_can_delete`, and (for `tasks`) `allowed_actions_can_complete` / (for `users`) `allowed_actions_can_submit_timetrack`.
- camelCase API field names are renamed to snake_case in the output schema.
- `userRates.leave_rates` is an `array<struct<leave_type_id: long, rate: decimal(18,4)>>` — the API returns the rates as an array of `{leaveTypeId, rate}` objects, not a map.
- `settings` is a singleton; the connector assigns `id = 1` so the row is addressable downstream.
- `timetrack` and `leavetime` rows do not carry a record-level id on the API; the connector exposes them with composite primary keys (`user_id, date, task_id` and `user_id, date, leave_type_id` respectively). The wire field `dayOffset` is intentionally dropped — it is computed server-side as `(entry.date - request.dateFrom).days` and so changes across overlapping fetches caused by `lookback_days > 0`. `date` is the canonical day attribute.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (e.g. `customers`, `timetrack`). |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default). |
| `destination_schema` | No | Target schema (defaults to pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. |

### Source-specific `table_configuration` options

All source-specific options below are passed via `table_configuration` in the pipeline spec. They must also be listed in the connection's `externalOptionsAllowList`.

#### Time-window options (apply to `timetrack`, `leavetime`, `approvalStatus`)

| Option | Type | Default | Applies to | Description |
|---|---|---|---|---|
| `start_date` | ISO date (`YYYY-MM-DD`) | 30 days before pipeline init time | `timetrack`, `leavetime`, `approvalStatus` | Initial cursor used when there is no stored offset yet. Controls how far back the very first run reaches. |
| `window_days` | integer | `7` for `timetrack`/`leavetime`, `28` for `approvalStatus` | `timetrack`, `leavetime`, `approvalStatus` | Size of each sliding date window the connector requests per microbatch. |
| `lookback_days` | integer | `7` for `timetrack`/`leavetime`, `14` for `approvalStatus` | `timetrack`, `leavetime`, `approvalStatus` | Days subtracted from the cursor at read time to catch late edits to records that fall inside actiTIME's editable window. Does **not** modify the stored offset. |
| `forward_days` | integer | `365` | `leavetime`, `approvalStatus` | How many days past today the connector will fetch. Captures planned leave (a user enters vacation months ahead) and future-week approvals. Ignored for `timetrack` — logged hours don't exist in the future, and the connector clamps the upper bound at today regardless of this setting. Set to `0` to revert to today-only behavior. |
| `user_batch_size` | integer | `100` | `timetrack`, `leavetime` | Number of `userIds` the connector fans out per actiTIME request. Bounds the per-call response size on large tenants without relying on `stopAfter` truncation. Lower this on tenants where a single user accumulates an unusually high entry count per window; raise it (up to ~200) on small tenants to reduce call count. |
| `stopAfter` | integer | unset (opt-in) | `timetrack`, `leavetime` | Optional server-side result cap forwarded as the `stopAfter` query parameter. The connector does **not** send this by default — the per-user batch is the natural bound. Set only if you have a specific need to cap response size further. `approvalStatus` does not accept this option (its `/approvalStatus` endpoint does not support `stopAfter`). |

#### Snapshot options (apply to `holidays`)

| Option | Type | Default | Applies to | Description |
|---|---|---|---|---|
| `dateFrom` | ISO date (`YYYY-MM-DD`) | none (open lower bound) | `holidays` | Optional lower bound on the holiday window. |
| `dateTo` | ISO date (`YYYY-MM-DD`) | none (open upper bound) | `holidays` | Optional upper bound on the holiday window. |

#### List pagination options (apply to all offset/limit list endpoints)

These apply to `customers`, `projects`, `tasks`, and `users` (and are used internally when fanning out `userRates` over the user list).

| Option | Type | Default | Description |
|---|---|---|---|
| `limit` | integer | `1000` | Page size used for offset/limit pagination against the actiTIME server. |
| `max_records_per_batch` | integer | unbounded (drain) | **Opt-in** per-microbatch cap on rows emitted from offset/limit list endpoints. By default the connector drains the endpoint fully. Set this only as a memory-hygiene knob on very large tables — note that the current snapshot / CDC reads return `{"done": True}` after one call and do not yet support partial-offset resumption, so setting this below your table's actual row count **will silently truncate** the remainder until a resumable cursor model lands. The internal `/users` discovery walk used for `userRates`, `timetrack`, and `leavetime` fan-out is always uncapped regardless of this setting. |

Snapshot tables that do not list above (`departments`, `userGroups`, `userRates`, `typesOfWork`, `leaveTypes`, `workflowStatuses`, `timeZoneGroups`, `settings`) do not require any source-specific options.

## Data Type Mapping

actiTIME JSON fields are mapped to Spark types as follows:

| actiTIME JSON Type | Example Fields | Connector Spark Type | Notes |
|---|---|---|---|
| integer | `id`, `customerId`, `projectId`, `time`, `estimatedTime` | `LongType` | All numeric identifiers are stored as 64-bit integers for safety. Durations such as `time` and `estimated_time` are kept in seconds. |
| string | `name`, `description`, `username`, `email`, `url`, `status` | `StringType` | UTF-8. |
| boolean | `archived`, `active`, `billable`, `paid_leave`, `auto_accrual`, `workflow_enabled` | `BooleanType` | |
| long (epoch ms) | `created`, `submittedAt`, `approvedAt` | `TimestampType` | Connector converts epoch-ms longs to ISO-8601 UTC timestamps. |
| string (ISO date) | `date`, `dateFrom`, `deadline`, `hired`, `releaseDate`, `weekStartDate` | `DateType` | `YYYY-MM-DD`. |
| object | `allowedActions` | Flattened to scalar booleans | E.g. `allowed_actions_can_modify`, `allowed_actions_can_delete`. |
| array<object> | `leaveRates` (inside `/userRates/{id}`) | `ArrayType(StructType([leave_type_id: LongType, rate: DecimalType(18,4)]))` | Modelled as an array of structs, not a map. |
| decimal | `regularRate`, `overtimeRate` | `DecimalType(18, 4)` | |
| null / absent | optional fields | nullable target type | Missing keys and explicit `null` are both treated as SQL `NULL`. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the actiTIME connector source in your workspace. This will typically place the connector code under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g. `ingestion_pipeline.py`), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this actiTIME connector.
- One or more **tables** to ingest, each with optional `table_configuration` options.

Example `pipeline_spec`:

```json
{
  "pipeline_spec": {
    "connection_name": "actitime_connection",
    "object": [
      {
        "table": {
          "source_table": "customers"
        }
      },
      {
        "table": {
          "source_table": "projects",
          "table_configuration": {
            "limit": "1000",
            "max_records_per_batch": "100000"
          }
        }
      },
      {
        "table": {
          "source_table": "timetrack",
          "table_configuration": {
            "start_date": "2026-01-01",
            "window_days": "7",
            "lookback_days": "7",
            "user_batch_size": "100"
          }
        }
      },
      {
        "table": {
          "source_table": "leavetime",
          "table_configuration": {
            "start_date": "2026-01-01",
            "window_days": "14",
            "lookback_days": "7"
          }
        }
      },
      {
        "table": {
          "source_table": "approvalStatus",
          "table_configuration": {
            "start_date": "2026-01-01",
            "window_days": "28",
            "lookback_days": "14"
          }
        }
      },
      {
        "table": {
          "source_table": "holidays",
          "table_configuration": {
            "dateFrom": "2026-01-01",
            "dateTo": "2026-12-31"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your actiTIME `base_url`, `username`, and `password`.
- For each `table`:
  - `source_table` must be one of the supported table names listed above (mind the camelCase: `userGroups`, `userRates`, `typesOfWork`, `leaveTypes`, `workflowStatuses`, `timeZoneGroups`, `approvalStatus`).
  - Source-specific options go inside `table_configuration` and must be allowed via the connection's `externalOptionsAllowList`.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g. a scheduled job).

- On the **first run** of `timetrack`, `leavetime`, or `approvalStatus`, set `start_date` to a recent cutoff to limit history. Without `start_date`, the connector defaults to 30 days before pipeline init time.
- On **subsequent runs**, the connector advances the stored cursor automatically and applies `lookback_days` at read time to re-fetch the recent past for late edits.
- CDC tables (`customers`, `projects`, `tasks`) and all snapshot tables (including `users`) refresh fully on every trigger.

#### Best Practices

- **Start small**: Begin with one or two tables (e.g. `customers`, `projects`) to validate connectivity and credentials, then add `tasks`, `users`, and the append tables.
- **Use incremental sync where possible**: For `timetrack`, `leavetime`, and `approvalStatus`, the sliding-window strategy keeps API usage bounded once the initial backfill completes.
- **Tune the window and lookback**: Smaller `window_days` reduces per-microbatch payload size but increases the number of triggers needed to catch up; larger `lookback_days` improves correctness for late edits at the cost of re-reading data.
- **Set appropriate schedules**: Balance data freshness against the API rate limits (see below).
- **Respect API rate limits**: actiTIME enforces **100 requests/second** and **1000 requests/minute** per user account. The connector honors `Retry-After` on `429` responses and applies exponential backoff with capped retries.
- **Protect against auth bans**: After **3 failed authentication attempts within 10 seconds**, the source IP is banned for 1 minute. Make sure the configured `password` is correct before deploying.

#### Troubleshooting

**Common Issues:**

- **`401 Unauthorized`**:
  - Verify `username` and `password`. The account password may have been reset or the user may have been disabled.
  - Confirm the service account has the **"Allow access to the actiTIME REST API"** permission.
  - If you hit `401` repeatedly within 10 seconds, the source IP may have been temporarily banned — wait at least 1 minute before retrying.
- **`403 Forbidden`**:
  - The service account lacks read permission on the requested resource. Increase the role/scope of the account.
  - To read PII columns from `users` (`username`, `email`, `hired`, `release_date`), grant **"Manage Accounts & Permissions"**.
- **`404 Not Found`**:
  - The `base_url` is incorrect or missing the tenant slug (e.g. `https://online.actitime.com/<tenant>`). Do not include `/api/v1` — the connector appends it automatically.
- **`429 Too Many Requests`**:
  - Rate limit exceeded. The connector backs off automatically; if it persists, lengthen the pipeline schedule, reduce concurrent tables, or split tables across separate pipelines.
- **Missing updates on `customers`, `projects`, `tasks`, or `users`**:
  - These tables are full-refresh + upsert; updates and archive flips appear on the next trigger. **Deletes are not surfaced**, so previously deleted rows will remain in the destination.
- **Missing PII columns on `users`**:
  - The actiTIME API silently omits `username`, `email`, `hired`, and `releaseDate` when the caller lacks the "Manage Accounts & Permissions" permission. Grant the permission and re-run.
- **`timetrack` / `leavetime` deletions not reflected**:
  - actiTIME does not expose a delete feed for time entries. Periodically re-sync the open editable window if you need to reconcile deletes.
- **First `timetrack` / `leavetime` / `approvalStatus` run pulls only 30 days**:
  - That is the connector's default `start_date`. Override `start_date` to backfill further history on the first run.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/actitime/actitime.py`
- Connector specification: `src/databricks/labs/community_connector/sources/actitime/connector_spec.yaml`
- Connector API documentation: `src/databricks/labs/community_connector/sources/actitime/actitime_api_doc.md`
- Official actiTIME documentation:
  - `https://www.actitime.com/api-documentation`
  - `https://www.actitime.com/api-documentation/api-usage`
  - `https://www.actitime.com/api-documentation/api-authorization`
