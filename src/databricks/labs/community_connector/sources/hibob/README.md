# Lakeflow HiBob Community Connector

This documentation describes how to configure and use the **HiBob (Bob)** Lakeflow community connector to ingest HR data from the HiBob Public API (`https://api.hibob.com/v1`) into Databricks.

## Prerequisites

- **HiBob account with admin access**: A Bob admin is needed to create a Service User and assign it permissions.
- **HiBob Service User**: The connector authenticates with a Service User ID and token (HTTP Basic authentication). Service users do not count toward your headcount.
- **Service User permissions**: Service users have **no permissions by default**. The admin must add the service user to a permission group that grants, for each field category you want to read:
  - **View** on the category (for example, Basic info, Work, Employment, Lifecycle, Payroll/Salary, Time off).
  - **View history** on the category, for the history tables. With only **View**, the history tables return just the current entry for each employee.
  - An **"Access data for"** audience that covers **all employees, including inactive (non-employed) people**. The default audience covers employed people only, which means leavers are missing from `employees` and from the history tables.
- **Network access**: The environment running the connector must be able to reach `https://api.hibob.com` (or `https://api.sandbox.hibob.com` for the sandbox).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `service_user_id` | string | yes | Service User ID. Used as the Basic auth username. | `SERVICE-12345` |
| `service_user_token` | string (secret) | yes | Service User token. Used as the Basic auth password. | `aBcD...` |
| `use_sandbox` | string | no | Set to `true` to connect to the HiBob sandbox (`https://api.sandbox.hibob.com/v1`). Leave empty or set to `false` (the default) to connect to production (`https://api.hibob.com/v1`). | `false` |
| `base_url` | string | no | Explicit API base URL, including the `/v1` suffix. When set, it takes precedence over `use_sandbox`. Only needed for advanced cases. | `https://api.hibob.com/v1` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that the connection passes through to the connector. This connector supports table-specific options, so this parameter is required. | `fields,show_inactive,human_readable,include_archived,employee_ids,page_size,start_date,window_days,max_lookback_days,include_pending,max_records_per_batch` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`fields,show_inactive,human_readable,include_archived,employee_ids,page_size,start_date,window_days,max_lookback_days,include_pending,max_records_per_batch`

> **Note**: Table-specific options such as `fields` or `start_date` are **not** connection parameters. You set them per table in the pipeline specification. Their names must be listed in `externalOptionsAllowList` or the connection will not pass them to the connector.

### Creating a HiBob Service User

1. Sign in to Bob as an admin.
2. Go to **Settings > Integrations > Service users** and create a new service user.
3. Copy the **Service User ID** and **token**. Store the token securely. Use these values as `service_user_id` and `service_user_token`.
4. Assign the service user to a permission group (from the service user's permissions settings, or from the permission groups settings in Bob). In that group:
   - Under **People's data**, grant **View** on every field category the connector reads: Basic info (`root`), Work, Employment, Lifecycle / Internal, Personal (for birth date), and Payroll/Salary (for `employee_salary_history`).
   - Grant **View history** on Work, Employment, Lifecycle, and Salary so the history tables return every entry, not only the current one.
   - Grant access to **Time off** data for `time_off_request_changes` and `time_off_policy_types`.
   - Set **Access data for** to an audience that includes **everyone, including inactive employees**, so leavers are ingested.
   - If you plan to ingest custom fields through the `fields` option, grant **View** on the custom categories that hold them.
5. Save the permission group.

Sandbox and production are separate environments with separate service users. To use the sandbox, create the service user in your sandbox account and set `use_sandbox` to `true`.

> **Tip**: HiBob does not return an error when the service user lacks permission for a field. The field is left out of the response and appears as `null` in Databricks. If a column is unexpectedly empty, check the permission group first.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `fields,show_inactive,human_readable,include_archived,employee_ids,page_size,start_date,window_days,max_lookback_days,include_pending,max_records_per_batch`. This is required for the connector to receive table-specific options.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The HiBob connector exposes a **static list** of tables. Use these names exactly (lowercase, snake_case) as `source_table`:

- `employees`
- `employee_work_history`
- `employee_employment_history`
- `employee_lifecycle_history`
- `employee_salary_history`
- `time_off_request_changes`
- `named_lists`
- `time_off_policy_types`
- `employee_fields`
- `custom_tables_metadata`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `employees` | Core people data (name, email, work, internal status, birth date), including inactive employees by default | `snapshot` | `id` | n/a |
| `employee_work_history` | Work entries: department, title, site, manager (`reportsTo`) over time | `snapshot` | `["employeeId", "id"]` | n/a |
| `employee_employment_history` | Employment entries: contract, type, weekly hours, FTE, working pattern | `snapshot` | `["employeeId", "id"]` | n/a |
| `employee_lifecycle_history` | Lifecycle entries: status (Hired, Employed, Terminated, ...), leave reasons | `snapshot` | `["employeeId", "id"]` | n/a |
| `employee_salary_history` | Salary entries: base amount and currency, pay period, pay frequency | `snapshot` | `["employeeId", "id"]` | n/a |
| `time_off_request_changes` | Change log of time off requests (Created, Canceled, Deleted, Pending) | `append` | `["requestId", "changeType"]` | `createdOn` |
| `named_lists` | Company list values (for example departments, sites), flattened to one row per list item | `snapshot` | `["list_name", "item_id"]` | n/a |
| `time_off_policy_types` | Names of time off policy types (for example Holiday, Sick) | `snapshot` | `name` | n/a |
| `employee_fields` | Metadata for every people field, including custom fields | `snapshot` | `id` | n/a |
| `custom_tables_metadata` | Definitions of custom tables and their columns | `snapshot` | `id` | n/a |

### Ingestion behavior

- **Snapshot tables**: The HiBob endpoints behind these tables have no "modified since" filter, so each run reads the full data set and the pipeline reconciles it against the destination table. Rows that disappear from the source are removed on the next snapshot.
- **`time_off_request_changes` (append)**: The connector reads the change log by time range (`createdOn`) and appends new changes on each run. It stores the end of the last range read and resumes from there on the next run. Each row is an immutable record of one change. A deleted request appears as a row with `changeType = Deleted`. A request that is modified gets a new `requestId`; use `originalRequestId` and `previousRequestId` to follow the chain.
- **Delete synchronization**: No table uses CDC with deletes. HiBob has no delete feed. Terminated employees stay in `employees` (with `internal.status` / `internal.lifecycleStatus` showing their state) as long as `show_inactive` is `true` and the service user's audience includes inactive employees.

### 6-month lookback limit for time off changes

The HiBob API rejects time off change requests that start more than about **6 months** in the past. Because of this:

- On the **first run**, the connector loads at most the last **180 days** of changes, even if you set an earlier `start_date`. Changes older than that cannot be retrieved through the API.
- If a pipeline is paused for longer than 180 days, the stored position falls outside the allowed range. The connector then resumes from 180 days ago and logs a warning; changes in the gap are lost.
- To avoid gaps, schedule the pipeline to run well within the 6-month window (daily or weekly is typical).

### Schema highlights

- **`employees`**
  - `id` is a **string**. HiBob employee IDs are large numeric-like values (for example `3332883884017713938`) that must not be treated as numbers.
  - `work`, `internal`, and `personal` are nested structs. `work.reportsTo` is a struct with `id`, `firstName`, `surname`, `email`, and `displayName`.
  - `humanReadable` is a JSON string with display labels for list-type fields (populated when `human_readable` is `APPEND`, the default).
  - `raw_json` holds the full employee record as returned by HiBob, as a JSON string. Custom fields and any extra fields you request with the `fields` option are available here. Use `employee_fields` to find their IDs and names.
- **History tables** (`employee_*_history`)
  - `employeeId` is added to each entry so every row is self-contained.
  - Shared columns: `id`, `effectiveDate`, `activeEffectiveDate`, `endEffectiveDate`, `isCurrent`, `creationDate`, `modificationDate`, and `change` (struct with `reason`, `changedBy`, `changedById`).
  - `customColumns` is a JSON string keyed by internal column IDs (for example `column_1666178477233`). Map them to names with `custom_tables_metadata` or `employee_fields`.
  - `employee_employment_history.actualWorkingPattern` is a JSON string because its shape depends on the pattern type.
  - `employee_salary_history.base` is a struct with `value` (double) and `currency` (string). This table contains **sensitive compensation data**; restrict access to it in Unity Catalog.
  - In `employee_lifecycle_history`, `endEffectiveDate` is a string, as HiBob documents it.
- **`time_off_request_changes`**
  - `additional_fields` is a JSON string holding type-specific attributes (for example start/end times, day portions, per-day durations) that vary with the request `type`.
- **`named_lists`**
  - Nested list items are flattened. `parent_id` points to the parent item for hierarchical lists and is `null` for top-level items.
- **`employee_fields` / `custom_tables_metadata`**
  - `typeData` is a JSON string (for example `{"listId": "..."}` for list fields). `custom_tables_metadata.columns` is an array of structs.

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
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

All source-specific options are optional.

**`employees`**

| Option | Default | Description |
|---|---|---|
| `fields` | (none) | Comma-separated HiBob field IDs to request in addition to the defaults, for example `work.employeeIdInCompany,payroll.employment.type` or custom fields such as `work.custom.field_1690000000000`. Custom fields are only returned when listed here. Extra fields appear in `raw_json`. The total number of fields (defaults plus extras) cannot exceed 400. |
| `show_inactive` | `true` | Include non-employed (inactive) people. Requires an "Access data for" audience that covers inactive employees. |
| `human_readable` | `APPEND` | `APPEND` adds display labels in the `humanReadable` column; `REPLACE` returns labels instead of internal values; an empty value returns internal values only. |

The default field set is: `root.id`, `root.firstName`, `root.surname`, `root.email`, `root.displayName`, `root.fullName`, `root.creationDateTime`, `work.department`, `work.title`, `work.startDate`, `work.manager`, `work.site`, `work.siteId`, `work.reportsTo`, `internal.status`, `internal.lifecycleStatus`, `internal.terminationDate`, `personal.birthDate`.

**`employee_work_history`, `employee_employment_history`, `employee_lifecycle_history`, `employee_salary_history`**

| Option | Default | Description |
|---|---|---|
| `include_archived` | `false` | Include archived entries. |
| `employee_ids` | (all accessible employees) | Comma-separated employee IDs to restrict the read to. Long lists are split into requests of 200 IDs. |
| `page_size` | `200` | Entries per page, between 1 and 200. |

**`named_lists`**

| Option | Default | Description |
|---|---|---|
| `include_archived` | `false` | Include archived list items. |

**`time_off_request_changes`**

| Option | Default | Description |
|---|---|---|
| `start_date` | 180 days before the run | ISO 8601 date or timestamp for the first load, for example `2026-06-01T00:00:00Z`. Values older than `max_lookback_days` are moved forward to that limit. Ignored after the first run. |
| `window_days` | `7` | Size, in days, of each time range the connector requests. Smaller windows mean smaller responses and more requests. |
| `max_lookback_days` | `180` | Oldest point, in days before the run, the connector will request. Do not set it above 180, because HiBob rejects requests older than about 6 months. |
| `include_pending` | `false` | Include changes for requests that are still pending approval. |
| `max_records_per_batch` | `1000` | Approximate number of records to read per batch. The connector always finishes the current time window, so a batch can exceed this number. |

`time_off_policy_types`, `employee_fields`, and `custom_tables_metadata` have no source-specific options.

## Data Type Mapping

| HiBob type | Example fields | Databricks type | Notes |
|---|---|---|---|
| ID (numeric-like string) | `employees.id`, `employeeId`, `reportsTo.id` | `STRING` | Employee IDs are too large to handle safely as numbers. |
| integer | history `id`, `siteId`, `calendarId`, `requestId` | `BIGINT` (`LongType`) | |
| number | `fte`, `weeklyHours`, `totalDuration`, `totalCost`, `base.value` | `DOUBLE` | |
| boolean | `isCurrent`, `archived`, `historical`, `mandatory` | `BOOLEAN` | |
| date (`yyyy-MM-dd`) | `effectiveDate`, `work.startDate`, `startDate`, `personal.birthDate` | `DATE` | `employee_lifecycle_history.endEffectiveDate` is `STRING`. |
| datetime (ISO 8601) | `creationDateTime`, `creationDate`, `modificationDate`, `createdOn` | `TIMESTAMP` | |
| string / list value | `department`, `title`, `status`, `payPeriod` | `STRING` | List fields hold the item value; labels are available via `humanReadable`. |
| object with a fixed shape | `work`, `reportsTo`, `change`, `base` | `STRUCT` | Empty objects are stored as `null`. |
| object with a variable shape | `customColumns`, `actualWorkingPattern`, `typeData`, `humanReadable`, `additional_fields`, `raw_json` | `STRING` (JSON) | Parse with `from_json` or the `:` JSON path operator. |
| array | `custom_tables_metadata.columns` | `ARRAY<STRUCT>` | |

All columns except primary keys are nullable. Fields the service user is not permitted to see are returned as `null`.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (for example, `ingest.py`).
2. Add a `table` entry for each HiBob table you want to ingest, with any table-specific options under `table_configuration`:

```json
{
  "pipeline_spec": {
    "connection_name": "hibob_connection",
    "object": [
      {
        "table": {
          "source_table": "employees",
          "table_configuration": {
            "fields": "work.employeeIdInCompany,work.custom.field_1690000000000",
            "show_inactive": "true",
            "human_readable": "APPEND"
          }
        }
      },
      {
        "table": {
          "source_table": "employee_work_history",
          "table_configuration": {
            "include_archived": "false",
            "page_size": "200"
          }
        }
      },
      {
        "table": {
          "source_table": "employee_salary_history",
          "destination_schema": "hr_restricted"
        }
      },
      {
        "table": {
          "source_table": "time_off_request_changes",
          "table_configuration": {
            "start_date": "2026-06-01T00:00:00Z",
            "window_days": "7",
            "include_pending": "true"
          }
        }
      },
      {
        "table": {
          "source_table": "named_lists"
        }
      },
      {
        "table": {
          "source_table": "employee_fields"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your `service_user_id` and `service_user_token`.
- Option values are strings, including booleans and numbers.
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (for example, a scheduled job). On the first run, `time_off_request_changes` loads up to the last 180 days (or from `start_date`, if later); later runs pick up only new changes. Snapshot tables are read in full on every run.

#### Best Practices

- **Start small**: Begin with `employees` and `employee_fields` to confirm that permissions and field coverage look right before adding history and time off tables.
- **Schedule within the 6-month window**: Run `time_off_request_changes` at least every few weeks so the stored position never falls outside HiBob's lookback limit.
- **Set appropriate schedules**: Snapshot tables re-read all data on every run. Reference tables (`named_lists`, `time_off_policy_types`, `employee_fields`, `custom_tables_metadata`) change rarely and can run less often than `employees` or the history tables.
- **Use `employee_fields` to find field IDs**: Look up the `id` / `jsonPath` of custom fields there before adding them to the `employees` `fields` option.
- **Protect sensitive data**: `employee_salary_history` and parts of `employees` contain personal and compensation data. Land them in a restricted schema and apply Unity Catalog grants.
- **Respect rate limits**: HiBob applies rate limits per endpoint and per service user. The only published limit is **50 requests per minute** for people search (used by `employees`). The connector reads `employees` with a single request, uses the bulk endpoints for history tables (up to 200 entries per page), and automatically retries `429` and `5xx` responses with exponential backoff, waiting until the time given in the `X-RateLimit-Reset` (or `Retry-After`) header. Avoid running several pipelines against the same service user at the same time.
- **Allow for short delays**: HiBob can return data that is up to about 20 seconds stale after a change in Bob.

#### Troubleshooting

**Common Issues:**

- **`401` (invalid service user credentials)**: Check `service_user_id` and `service_user_token`. Make sure you use sandbox credentials with `use_sandbox = true` and production credentials otherwise.
- **`403` (missing category permission or audience)**: The service user's permission group is missing **View** on a required category, or the audience does not include the employees being read. Update the permission group as described in [Creating a HiBob Service User](#creating-a-hibob-service-user).
- **Columns are `null` or custom fields are missing**: HiBob silently drops fields the service user cannot see and fields with invalid IDs. Check the permissions for that category and confirm the field ID in `employee_fields`. Custom fields are only returned when listed in the `fields` option, and they appear in `raw_json`.
- **Terminated employees are missing**: Set `show_inactive` to `true` (the default) and make sure the service user's **Access data for** audience includes inactive employees.
- **History tables contain only one entry per employee**: Grant **View history** on the Work, Employment, Lifecycle, and Salary categories.
- **Some employees are missing from a history table**: HiBob reports per-employee errors (for example `MISSING_PERMISSION`) instead of failing the request. The connector skips those employees and logs a warning. Review the audience and category permissions.
- **Time off changes older than 6 months are missing**: This is a HiBob API limit. Earlier changes cannot be loaded through this connector.
- **`400` (bad request)**: Usually an invalid option value, such as an unsupported `human_readable` value, or more than 400 fields in total for `employees`.
- **`429` (rate limit exceeded after retries)**: Reduce how often pipelines run, or avoid running several pipelines against the same service user at once.

## References

- HiBob API getting started: https://apidocs.hibob.com/docs/getting-started
- Rate limiting: https://apidocs.hibob.com/docs/rate-limit
- People search: https://apidocs.hibob.com/reference/post_people-search
- Bulk work history: https://apidocs.hibob.com/reference/get_bulk-people-work
- Bulk employment history: https://apidocs.hibob.com/reference/get_bulk-people-employment
- Bulk lifecycle history: https://apidocs.hibob.com/reference/get_bulk-people-lifecycle
- Bulk salary history: https://apidocs.hibob.com/reference/get_bulk-people-salaries
- Time off request changes: https://apidocs.hibob.com/reference/get_timeoff-requests-changes
- Named lists: https://apidocs.hibob.com/reference/get_company-named-lists
- People fields metadata: https://apidocs.hibob.com/reference/get_company-people-fields
- Custom tables metadata: https://apidocs.hibob.com/reference/get_people-custom-tables-metadata
