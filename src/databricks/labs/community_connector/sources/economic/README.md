# Lakeflow e-conomic Community Connector

This documentation describes how to configure and use the **Visma e-conomic** Lakeflow community connector to ingest accounting and project data from the [e-conomic OpenAPI](https://apis.e-conomic.com/) into Databricks.

The connector is **read-only**. It reads from the newer e-conomic OpenAPI family (`https://apis.e-conomic.com/`), not the legacy REST API (`restapi.e-conomic.com`). In this family each domain is a separate "resource API" with its own version. The connector reads from two of them:

| Resource API | Default version | Tables |
|---|---|---|
| Projects (`projectsapi`) | `1.1.0` | `employees`, `projects`, `project_time_entries` |
| Customers (`customersapi`) | `3.1.0` | `customers` |

> **Not affiliated with Visma.** This is a community-maintained Lakeflow connector. It is not affiliated with, approved, endorsed, or sponsored by Visma or e-conomic. "Visma" and "e-conomic" are trademarks of their respective owners and are used here only to identify the source system.

## Prerequisites

- **An e-conomic agreement (company)** that holds the data you want to ingest. One connection reads one agreement. To ingest several companies, create one connection per agreement.
- **An e-conomic app with two tokens**:
  - **App secret token**: identifies your integrating app. You get it when you register the app in your e-conomic developer agreement.
  - **Agreement grant token**: issued when the customer agreement grants your app access. It identifies the agreement and the access it was granted.
- **Required roles**: the role the agreement granted to the app must allow reading each table you plan to ingest. Without the role, the API returns `403 Forbidden` for that table.

  | Table | Required role (any of) |
  |---|---|
  | `employees` | `Sales`, `SuperUser` |
  | `customers` | `Sales`, `SuperUser` |
  | `projects` | `ProjectEmployee`, `SuperUser` |
  | `project_time_entries` | `ProjectEmployee`, `SuperUser` |

  To ingest all four tables, the app needs either `SuperUser`, or both `Sales` and `ProjectEmployee`.
- **Network access**: the environment running the connector must be able to reach `https://apis.e-conomic.com` over HTTPS.
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

> **Try it without an account.** e-conomic hosts a public demo agreement. Set **both** `app_secret_token` and `agreement_grant_token` to the literal value `demo` to read sample data from all four tables. The demo agreement supports read (GET) requests only and holds very little data. Use it for smoke tests, not for production pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when you create the connection.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `app_secret_token` | string (secret) | yes | e-conomic app secret token. The connector sends it in the `X-AppSecretToken` header on every request. Store it as a Databricks secret, not inline. Use `demo` (together with `agreement_grant_token = demo`) to read the public demo agreement. | `<your-app-secret-token>` or `demo` |
| `agreement_grant_token` | string (secret) | yes | e-conomic agreement grant token for the agreement you want to read. The connector sends it in the `X-AgreementGrantToken` header. The grant must include the roles listed under [Prerequisites](#prerequisites). Store it as a Databricks secret. | `<your-agreement-grant-token>` or `demo` |
| `base_url` | string | no | Base URL for the e-conomic resource APIs. Defaults to `https://apis.e-conomic.com`. Override it only if e-conomic gives you a different host for your environment. | `https://apis.e-conomic.com` |
| `projects_api_version` | string | no | Version of the Projects resource API, used for `employees`, `projects` and `project_time_entries`. Accepts the version with or without a leading `v`. Defaults to `1.1.0`. | `1.1.0` |
| `customers_api_version` | string | no | Version of the Customers resource API, used for `customers`. Accepts the version with or without a leading `v`. Defaults to `3.1.0`. | `3.1.0` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names the connection may pass through to the connector. This connector supports table-specific options, so you must set this parameter to the exact value below. | `max_records_per_batch,lookback_seconds,start_timestamp` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`max_records_per_batch,lookback_seconds,start_timestamp`

> **Note**: Table-specific options are **not** connection parameters. You set them per table under `table_configuration` in the pipeline spec. The connection only lets them through if their names are in `externalOptionsAllowList`.

There is no OAuth flow. Both tokens are long-lived static values, and the connector sends them as headers on every request.

### Obtaining the Required Parameters

The exact screens in e-conomic change over time. Treat the steps below as an outline, and check the e-conomic developer documentation at <https://www.e-conomic.com/developer/connect> for the current procedure.

1. **Get a developer agreement.** Sign up for an e-conomic developer agreement through the e-conomic developer portal. This is where you register and manage apps.
2. **Create an app.** In the developer agreement, create a new app. When you create it, choose the role the app requires. To read all four tables, choose `SuperUser`, or a role set that covers both `Sales` and `ProjectEmployee` (see [Prerequisites](#prerequisites)). Creating the app gives you:
   - the **app secret token**, which is private. Use it as `app_secret_token`.
   - the **app public token**, which is only used to build the installation link in the next step. The connector does not use it.
3. **Have the customer agreement grant access.** An administrator of the e-conomic company you want to read must install (grant) the app. The developer portal gives you an installation link that contains the app public token. When the administrator opens the link and approves access, e-conomic issues the **agreement grant token**. Use it as `agreement_grant_token`. If you are reading your own developer agreement, the developer portal may let you create a grant token for it directly.
4. **Store both tokens as Databricks secrets**, for example in a secret scope named `economic`, and reference them from the connection.

If the grant is later revoked in e-conomic, requests fail with `401 Unauthorized`, and you need a new agreement grant token.

### Create a Unity Catalog Connection

You can create a Unity Catalog connection for this connector in the UI in either of these ways:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select an existing Lakeflow Community Connector connection for this source, or create a new one.

Either way, set `externalOptionsAllowList` to `max_records_per_batch,lookback_seconds,start_timestamp`. The connector needs it to receive table-specific options.

You can also create the connection with the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of four tables. Use these exact (lowercase) names as `source_table`:

- `employees`
- `customers`
- `projects`
- `project_time_entries`

### Object summary, primary keys, and ingestion mode

| Table | Description | e-conomic endpoint | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|---|
| `employees` | Employees registered in the agreement | Projects API `/Employees` | `snapshot` | `number` | n/a |
| `customers` | Customer master data (debtors) | Customers API `/Customers` | `snapshot` | `customerNumber` | n/a |
| `projects` | Projects, including price and invoicing totals | Projects API `/Projects` | `cdc` | `number` | `lastUpdated` |
| `project_time_entries` | Hours registered on projects | Projects API `/TimeEntries` | `cdc` | `number` | `lastUpdated` |

All primary keys are integers that are unique within an agreement.

- **Snapshot tables** (`employees`, `customers`): e-conomic offers no change-tracking field for these resources (no `lastUpdated`, and `customers` can only be filtered on `customerNumber`). The connector reads the full table on every run. Inserts, updates and deletes are all reflected after each run.
- **CDC tables** (`projects`, `project_time_entries`): the connector reads incrementally with the `lastUpdated` field and upserts by primary key. See [Incremental sync behavior](#incremental-sync-behavior).
- **No delete synchronization**: no table uses `cdc_with_deletes`. e-conomic has no deleted-records feed and no soft-delete flag for these resources, so a deleted project or time entry is never reported to the connector. See [Handling deletes](#handling-deletes-periodic-full-refresh).

### Schema highlights

The schemas are static and defined by the connector. All records are flat, with no nested objects or arrays.

- **Missing values are null.** e-conomic leaves empty properties out of its responses instead of returning `null`, and every column is nullable.
- **Boolean `false` may arrive as null.** According to the e-conomic API documentation, a `false` boolean can be left out of the response body. In practice the behavior is inconsistent. When you filter on flags such as `isBarred`, `isClosed`, `isMainProject`, `isApproved`, `isReconciled`, `access` or `defaultDisableEInvoicing`, treat null as `false`, for example `coalesce(isBarred, false)`.
- **`objectVersion`** (all tables) is an opaque concurrency token. It changes when a record changes, but it is not ordered and cannot be used as a change cursor.
- **`projects`**:
  - `lastUpdated` is the incremental cursor.
  - `costPrice`, `salesPrice` and `invoicedTotal` are read-only totals that e-conomic calculates from registrations on the project. See [Known Limitations](#known-limitations) for how fresh they are.
  - `status` is an integer code (for example `1` = Quote sent, `2` = Realised).
  - `customerNumber` links to `customers.customerNumber`. `responsibleEmployeeNumber` and `otherResponsibleEmployeeNumber` link to `employees.number`. `mainProjectNumber` links to `projects.number`.
- **`project_time_entries`**:
  - `lastUpdated` is the incremental cursor.
  - `date` is the registration date, stored as a timestamp with a `00:00:00` time part.
  - `projectNumber` links to `projects.number`, and `employeeNumber` links to `employees.number`. `activityNumber` refers to e-conomic Activities, which this connector does not ingest.
  - Approved entries (`isApproved = true`) cannot be changed in e-conomic. Unapproved entries can still be edited or deleted.
  - Time entry prices are stored in a separate e-conomic resource and are not part of this table.
- **`employees`**: `groupNumber` refers to the employee group, which this connector does not ingest.

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
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. For `projects` and `project_time_entries`, `lastUpdated` is the natural choice. |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

The source-specific options below apply only to the **CDC tables** `projects` and `project_time_entries`. The snapshot tables `employees` and `customers` ignore them.

| Option | Type | Default | Applies to | Description |
|---|---|---|---|---|
| `start_timestamp` | ISO-8601 string | none (full history) | `projects`, `project_time_entries` | Lower bound on `lastUpdated` for the **first** incremental run, for example `2024-01-01T00:00:00`. Used only when the table has no stored progress yet. If you omit it, the first run reads the full history. Values with `Z` or an offset are converted to UTC. Fractional seconds are dropped. An unparsable value fails the run. |
| `lookback_seconds` | integer, `>= 0` | `600` | `projects`, `project_time_entries` | Number of seconds subtracted from the stored `lastUpdated` high-water mark at the start of each new scan, so that recently changed rows are read again. Re-read rows are de-duplicated by primary key. e-conomic stores `lastUpdated` with whole-minute precision, so keep this value at 60 or higher. |
| `max_records_per_batch` | integer, `> 0` | `5000` | `projects`, `project_time_entries` | Maximum number of records the connector returns in one micro-batch. If a scan has more matching records, it continues in the next micro-batch from the last primary key it returned. Lower the value to reduce memory per batch. |

## Incremental sync behavior

e-conomic returns list results in **primary-key order**, and its list endpoints cannot sort by `lastUpdated`. For that reason, the CDC tables use a resumable scan ordered by primary key:

1. **First run.** If `start_timestamp` is set, the connector reads every record with `lastUpdated >= start_timestamp`. Otherwise it reads the whole table.
2. **Later runs.** The connector reads every record with `lastUpdated >= (stored high-water mark - lookback_seconds)`. The filter runs on the e-conomic side, so only changed records cross the network.
3. **Batching.** If a scan has more than `max_records_per_batch` matching records, it is split across several micro-batches. Each batch continues from the last primary key returned.
4. **Committing progress.** The `lastUpdated` high-water mark advances only after a scan finishes. If a run fails partway, the next run repeats the scan and does not skip records. The new high-water mark is the largest `lastUpdated` seen, capped at the time the run started. Records that change while a run is in progress are picked up by the next run.
5. **Upserts.** Records are merged into the destination by primary key (`number`). The lookback window and repeated scans therefore never create duplicates.

Snapshot tables (`employees`, `customers`) read the full table on every run, using cursor pagination of up to 1,000 records per request.

### Handling deletes (periodic full refresh)

e-conomic does not report deletions. Records that can be deleted in e-conomic include:

- unapproved time entries,
- projects that nothing else references.

After a deletion, the row stays in the destination tables for `projects` and `project_time_entries` until you run a full refresh. Snapshot tables (`employees`, `customers`) pick up deletions automatically on the next run.

If you need the destination to match e-conomic exactly, schedule a **periodic full refresh** of `projects` and `project_time_entries`. Use the pipeline UI (**Full refresh selection**) or the pipelines API (`full_refresh_selection`). Weekly or monthly is usually enough for accounting data volumes. A full refresh clears the stored progress, so the table is reloaded from `start_timestamp` if one is set, or from the full history otherwise. To keep all history after a full refresh, leave `start_timestamp` unset on tables you refresh fully.

## Data Type Mapping

| e-conomic API type | Example fields | Databricks type | Notes |
|---|---|---|---|
| integer (int32) | `number`, `customerNumber`, `projectNumber`, `groupNumber`, `status` | `BIGINT` (`LongType`) | All identifiers and integer codes are stored as 64-bit integers. |
| number (double) | `numberOfHours`, `fixedPrice`, `costPrice`, `salesPrice`, `invoicedTotal`, `creditMaximum`, `mileage` | `DOUBLE` (`DoubleType`) | e-conomic sends amounts as JSON decimals (for example `4080.00`). If you need exact precision, cast to `DECIMAL` downstream. |
| boolean | `isBarred`, `isClosed`, `isApproved`, `access` | `BOOLEAN` (`BooleanType`) | A missing value is null and may mean `false`. See [Schema highlights](#schema-highlights). |
| string | `name`, `email`, `currency`, `description`, `text`, `objectVersion` | `STRING` (`StringType`) | |
| date-time (ISO-8601, no offset) | `lastUpdated`, `date`, `deliveryDate`, `closedDate`, `directDebitMandateDate` | `TIMESTAMP` (`TimestampType`) | e-conomic sends timestamps without a time zone offset (for example `2024-12-07T23:14:00`) and does not document their time zone. Spark reads these values in the session time zone. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Add one `table` entry for each table you want to ingest. Put the source-specific options for the CDC tables under `table_configuration`:

```json
{
  "pipeline_spec": {
    "connection_name": "economic_connection",
    "object": [
      {
        "table": {
          "source_table": "employees"
        }
      },
      {
        "table": {
          "source_table": "customers"
        }
      },
      {
        "table": {
          "source_table": "projects",
          "table_configuration": {
            "lookback_seconds": "600"
          }
        }
      },
      {
        "table": {
          "source_table": "project_time_entries",
          "table_configuration": {
            "start_timestamp": "2024-01-01T00:00:00",
            "lookback_seconds": "900",
            "max_records_per_batch": "5000"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the Unity Catalog connection that holds your `app_secret_token` and `agreement_grant_token`.
- `source_table` must be one of `employees`, `customers`, `projects`, `project_time_entries`.
- Write option values as strings.

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

Run the pipeline with your usual Databricks orchestration, such as a scheduled job.

- **First run**: snapshot tables load in full. CDC tables load either the full history or everything from `start_timestamp`.
- **Later runs**: snapshot tables are reloaded in full. CDC tables read only the records changed since the last run, plus the lookback window.

#### Best Practices

- **Start small**: validate the connection and data shape with the `demo` tokens or a single table, such as `employees`, before you add the rest.
- **Use incremental sync**: for large agreements, rely on the CDC tables. Set `start_timestamp` only if you don't need older history.
- **Schedule periodic full refreshes** of `projects` and `project_time_entries` to remove deleted records (see [Handling deletes](#handling-deletes-periodic-full-refresh)).
- **Set appropriate schedules**: accounting data changes slowly. Hourly or daily runs are usually enough, and they use far fewer API calls than very frequent runs.
- **Respect rate limits**: e-conomic rate limits API calls **per app and agreement** with a token bucket. Each call has a cost, which e-conomic reports in the `X-CallCost` response header. Pipelines, scripts and other integrations that use the same app and agreement share the same budget. A full snapshot needs about one call per 1,000 records, so very frequent runs on large agreements can use up the budget.
- **Keep `lookback_seconds` at 60 or more**, because `lastUpdated` has whole-minute precision.

#### Rate limits and retries

The connector handles e-conomic throttling and transient errors automatically:

- It retries HTTP `429`, `500`, `502`, `503` and `504` responses, connection errors and timeouts, with up to 6 attempts per request.
- On `429`, it waits for the number of seconds in the `Retry-After` header, up to 60 seconds. Without the header, it waits 1, 2, 4, 8, then 16 seconds between attempts.
- Each request times out after 60 seconds.
- Requests run one at a time from a single reader. The connector never sends parallel requests that could drain the bucket faster.

e-conomic does not publish fixed bucket sizes or refill rates, and the actual limits may vary per agreement. If you see frequent retries in the logs, run the pipeline less often.

#### Troubleshooting

**Common Issues:**

- **`401 Unauthorized` (for example `ApplicationSecretTokenMissing`)**
  - Check that `app_secret_token` and `agreement_grant_token` are both set and correct, and that the secret references resolve.
  - Make sure you have not swapped the two tokens, or used the app *public* token instead of the app *secret* token.
  - The grant may have been revoked in e-conomic. Ask the agreement administrator to grant access again, and update `agreement_grant_token`.
- **`403 Forbidden` on some tables only**
  - The grant lacks the role for that resource. `employees` and `customers` need `Sales` or `SuperUser`. `projects` and `project_time_entries` need `ProjectEmployee` or `SuperUser`. Update the app's required role, and have the agreement grant access again if needed.
- **`Invalid 'start_timestamp'` error**
  - `start_timestamp` must be a valid ISO-8601 value such as `2024-01-01T00:00:00`. The run fails before any request is sent to e-conomic.
- **`400 Bad Request`**
  - e-conomic rejected the request parameters. Check that `projects_api_version` and `customers_api_version` point to versions that support the filters the connector uses (the defaults do), and see the `errorCode` / `detail` text in the error message.
- **`404 Not Found` on every table**
  - Check `base_url`, `projects_api_version` and `customers_api_version`. An API version that does not exist on e-conomic returns `404`. Leave these options unset to use the tested defaults.
- **Deleted projects or time entries still appear in Databricks**
  - This is expected. Run a full refresh of the affected table (see [Handling deletes](#handling-deletes-periodic-full-refresh)).
- **Table options seem to be ignored**
  - Check that `externalOptionsAllowList` on the connection is exactly `max_records_per_batch,lookback_seconds,start_timestamp`.
  - `start_timestamp` only affects the first run. Changing it later has no effect until you run a full refresh of that table.
  - Options set on `employees` or `customers` are ignored, because those are snapshot tables.
- **Very few rows with the `demo` tokens**
  - This is expected. The public demo agreement holds only a small sample data set.

## Known Limitations

- **Read-only.** The connector never writes back to e-conomic.
- **Four tables only.** Related e-conomic resources are not covered, including Activities, Activity Groups, Employee Groups, Project Groups, Project Statuses, Mileages, Time Entry Prices, Cost Types, customer Contacts and Delivery Locations. Neither are other resource APIs such as products or invoices.
- **No delete synchronization.** e-conomic exposes no deleted-records feed or soft-delete flag. The CDC tables keep deleted rows until you run a full refresh. Snapshot tables reflect deletions on every run.
- **Rows with an empty `lastUpdated`.** The e-conomic schema allows `lastUpdated` to be null on projects. Such rows are loaded by an unfiltered first run, but later incremental runs cannot detect changes to them. A periodic full refresh also covers these rows.
- **Freshness of project totals.** e-conomic documents that `lastUpdated` changes when a property of the record changes, and that changes to related records can also update it. It is not confirmed whether new time registrations always update `lastUpdated` on the parent project, so `costPrice`, `salesPrice` and `invoicedTotal` in `projects` may be stale between full refreshes. If these totals must always be current, run frequent full refreshes of `projects`. The table is usually small.
- **Time zone of timestamps.** e-conomic sends timestamps without a time zone offset and does not document their time zone. Convert them downstream if you need UTC.
- **Monetary values as `DOUBLE`.** Amounts are stored as floating-point numbers. Cast them to `DECIMAL` downstream for exact financial arithmetic.
- **Single sequential reader.** e-conomic list endpoints cannot sort, the rate limit is per agreement, and accounting data sets are usually small. For these reasons the connector reads each table from a single reader, without partitioning.
- **Safety limit on pages.** As a guard against a misbehaving cursor, one read follows at most 10,000 pages of up to 1,000 records each, which is about 10 million rows. A read also stops if e-conomic returns the same pagination cursor twice.
- **One agreement per connection.** To ingest several e-conomic companies, create a separate connection and pipeline for each agreement.

## References

- e-conomic OpenAPI documentation index: <https://apis.e-conomic.com/>
- e-conomic Projects API reference (employees, projects, time entries): <https://apis.e-conomic.com/projectsapi/redoc.html>
- e-conomic Customers API reference: <https://apis.e-conomic.com/customersapi/redoc.html>
- e-conomic developer connection and token documentation: <https://www.e-conomic.com/developer/connect>
- e-conomic rate limiting help article: <https://www.e-conomic.dk/support/artikler/rate-limiting-on-the-e-conomic-public-api>
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)
