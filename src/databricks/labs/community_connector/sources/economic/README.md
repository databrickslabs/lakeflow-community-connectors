# Lakeflow e-conomic Community Connector

This documentation provides setup instructions and reference information for the **Visma e-conomic** source connector. The connector ingests accounting and project data from the [e-conomic OpenAPI](https://apis.e-conomic.com/) into Databricks.

The connector is **read-only**. It reads from the newer e-conomic OpenAPI family (`https://apis.e-conomic.com/`), not the legacy REST API (`restapi.e-conomic.com`). In this family each domain is a separate "resource API" with its own version. The connector reads 16 tables from two of them:

| Resource API | Default version | Tables |
|---|---|---|
| Projects (`projectsapi`) | `1.1.0` | `employees`, `employee_groups`, `projects`, `project_time_entries`, `activities`, `activity_groups`, `project_groups`, `project_statuses`, `mileages`, `project_employees`, `project_customers`, `time_entry_prices`, `cost_types` |
| Customers (`customersapi`) | `3.1.0` | `customers`, `customer_contacts`, `customer_delivery_locations` |

> **Not affiliated with Visma.** This is a community-maintained Lakeflow connector. It is not affiliated with, approved, endorsed, or sponsored by Visma or e-conomic. "Visma" and "e-conomic" are trademarks of their respective owners and are used here only to identify the source system.

## Prerequisites

- **An e-conomic agreement (company)** that holds the data you want to ingest. One connection reads one agreement. To ingest several companies, create one connection per agreement.
- **An e-conomic app with two tokens**:
  - **App secret token**: identifies your integrating app. You get it when you register the app in your e-conomic developer agreement.
  - **Agreement grant token**: issued when the customer agreement grants your app access. It identifies the agreement and the access it was granted.
- **Required roles**: the role the agreement granted to the app must allow reading each table you plan to ingest. Without the role, the API returns `403 Forbidden` for that table only. The other tables still load.

  | Role (any of) | Tables |
  |---|---|
  | `SuperUser` | All 16 tables |
  | `Sales` | `employees`, `employee_groups`, `customers`, `customer_contacts`, `customer_delivery_locations` |
  | `ProjectEmployee` | `projects`, `project_time_entries`, `activities`, `activity_groups`, `project_groups`, `project_statuses`, `mileages`, `project_employees`, `project_customers`, `time_entry_prices`, `cost_types` |

  To ingest all 16 tables, the app needs either `SuperUser`, or both `Sales` and `ProjectEmployee`. Note that `employees` and `employee_groups` come from the Projects API but need `Sales`, not `ProjectEmployee`.

  With a `ProjectEmployee`-only grant, you cannot read `employees` or any Customers API table. Use `project_employees` for employee data and `project_customers` for customer numbers and names instead (see [Schema highlights](#schema-highlights)).
- **Network access**: the environment running the connector must be able to reach `https://apis.e-conomic.com` over HTTPS.
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

> **Try it without an account.** e-conomic hosts a public demo agreement. Set **both** `app_secret_token` and `agreement_grant_token` to the literal value `demo` to read sample data from all 16 tables (the demo grant has every role). The demo agreement supports read (GET) requests only and holds very little data. Use it for smoke tests, not for production pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when you create the connection.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `app_secret_token` | string (secret) | yes | e-conomic app secret token. The connector sends it in the `X-AppSecretToken` header on every request. Store it as a Databricks secret, not inline. Use `demo` (together with `agreement_grant_token = demo`) to read the public demo agreement. | `<your-app-secret-token>` or `demo` |
| `agreement_grant_token` | string (secret) | yes | e-conomic agreement grant token for the agreement you want to read. The connector sends it in the `X-AgreementGrantToken` header. The grant must include the roles for the tables you read, as listed under [Prerequisites](#prerequisites). Store it as a Databricks secret. | `<your-agreement-grant-token>` or `demo` |
| `base_url` | string | no | Base URL for the e-conomic resource APIs. Defaults to `https://apis.e-conomic.com`. Override it only if e-conomic gives you a different host for your environment. | `https://apis.e-conomic.com` |
| `projects_api_version` | string | no | Version of the Projects resource API, used by the 13 tables `employees`, `employee_groups`, `projects`, `project_time_entries`, `activities`, `activity_groups`, `project_groups`, `project_statuses`, `mileages`, `project_employees`, `project_customers`, `time_entry_prices` and `cost_types`. Accepts the version with or without a leading `v`. Defaults to `1.1.0`. | `1.1.0` |
| `customers_api_version` | string | no | Version of the Customers resource API, used by the 3 tables `customers`, `customer_contacts` and `customer_delivery_locations`. Accepts the version with or without a leading `v`. Defaults to `3.1.0`. | `3.1.0` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names the connection may pass through to the connector. This connector supports table-specific options, so you must set this parameter to the exact value below. | `max_records_per_batch,lookback_seconds,start_timestamp` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`max_records_per_batch,lookback_seconds,start_timestamp`

> **Note**: Table-specific options are **not** connection parameters. You set them per table under `table_configuration` in the pipeline spec. The connection only lets them through if their names are in `externalOptionsAllowList`.

There is no OAuth flow. Both tokens are long-lived static values, and the connector sends them as headers on every request.

### Obtaining the Required Parameters

e-conomic's official developer documentation at <https://www.e-conomic.com/developer/connect> is the authoritative guide for issuing tokens. The screens in the e-conomic developer portal change over time, and the steps below have not been verified click by click against the current portal. Treat them as an outline of the process, and follow e-conomic's documentation for the exact procedure.

1. **Get a developer agreement.** Sign up for an e-conomic developer agreement through the e-conomic developer portal. This is where you register and manage apps.
2. **Create an app.** In the developer agreement, create a new app. When you create it, choose the role the app requires. To read all 16 tables, choose `SuperUser`, or a role set that covers both `Sales` and `ProjectEmployee` (see [Prerequisites](#prerequisites)). Creating the app gives you:
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

The connector exposes a **static list** of 16 tables. Use these exact (lowercase) names as `source_table`:

- Projects API: `employees`, `employee_groups`, `projects`, `project_time_entries`, `activities`, `activity_groups`, `project_groups`, `project_statuses`, `mileages`, `project_employees`, `project_customers`, `time_entry_prices`, `cost_types`
- Customers API: `customers`, `customer_contacts`, `customer_delivery_locations`

### Object summary, primary keys, and ingestion mode

| Table | Description | e-conomic endpoint | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|---|
| `employees` | Employees registered in the agreement, with contact details | Projects API `/Employees` | `snapshot` | `number` | n/a |
| `employee_groups` | Employee groups | Projects API `/EmployeeGroups` | `snapshot` | `number` | n/a |
| `projects` | Projects, including price and invoicing totals | Projects API `/Projects` | `cdc` | `number` | `lastUpdated` |
| `project_time_entries` | Hours registered on projects | Projects API `/TimeEntries` | `cdc` | `number` | `lastUpdated` |
| `activities` | Activities that time is registered against, with sales prices | Projects API `/Activities` | `snapshot` | `number` | n/a |
| `activity_groups` | Activity groups and their finance accounts | Projects API `/ActivityGroups` | `snapshot` | `number` | n/a |
| `project_groups` | Project groups and their finance accounts | Projects API `/ProjectGroups` | `snapshot` | `number` | n/a |
| `project_statuses` | Project statuses (quote sent, realised) | Projects API `/ProjectStatuses` | `snapshot` | `number` | n/a |
| `mileages` | Mileage registered on projects | Projects API `/Mileages` | `snapshot` | `number` | n/a |
| `project_employees` | Employees as seen by the Projects module, with user link, employee type and cost and sales prices | Projects API `/project-employees` | `snapshot` | `number` | n/a |
| `project_customers` | Customers that can be used on projects (number and name only) | Projects API `/project-customers` | `snapshot` | `number` | n/a |
| `time_entry_prices` | Calculated cost and sales price of each time entry | Projects API `/TimeEntryPrices` | `snapshot` | `number` | n/a |
| `cost_types` | Cost types used for project costs | Projects API `/CostTypes` | `cdc` | `number` | `lastUpdated` |
| `customers` | Customer master data (debtors) | Customers API `/Customers` | `snapshot` | `customerNumber` | n/a |
| `customer_contacts` | Contact persons of all customers | Customers API `/Contacts` | `cdc` | `number` | `lastUpdated` |
| `customer_delivery_locations` | Delivery addresses of all customers | Customers API `/DeliveryLocations` | `cdc` | `number` | `lastUpdated` |

All primary keys are single integer columns that are unique within an agreement. Every table is a flat, top-level list in e-conomic, so the connector never has to loop over customers or projects to read child records. For example, `customer_contacts` returns the contacts of every customer in one list.

- **Snapshot tables** (the 11 tables `employees`, `employee_groups`, `customers`, `activities`, `activity_groups`, `project_groups`, `project_statuses`, `mileages`, `project_employees`, `project_customers`, `time_entry_prices`): e-conomic offers no filterable change-tracking field for these resources (no `lastUpdated`, and `customers` can only be filtered on `customerNumber`). The connector reads the full table on every run. Inserts, updates and deletes are all reflected after each run.
- **CDC tables** (the 5 tables `projects`, `project_time_entries`, `cost_types`, `customer_contacts`, `customer_delivery_locations`): the connector reads incrementally with the `lastUpdated` field and upserts by primary key. See [Incremental sync behavior](#incremental-sync-behavior).
- **`time_entry_prices` is a snapshot on purpose.** It has one row per time entry, but its prices are calculated by e-conomic from activity and employee prices. A price can change without the time entry's `lastUpdated` changing, so the connector reads the whole table on every run to keep prices current.
- **No delete synchronization**: no table uses `cdc_with_deletes`. e-conomic has no deleted-records feed, so a deleted record in a CDC table is never reported to the connector. See [Handling deletes](#handling-deletes-periodic-full-refresh).

### Incremental sync behavior

e-conomic returns list results in **primary-key order**, and its list endpoints cannot sort by `lastUpdated`. For that reason, the CDC tables (`projects`, `project_time_entries`, `cost_types`, `customer_contacts`, `customer_delivery_locations`) use a resumable scan ordered by primary key:

1. **First run.** If `start_timestamp` is set, the connector reads every record with `lastUpdated >= start_timestamp`. Otherwise it reads the whole table.
2. **Later runs.** The connector reads every record with `lastUpdated >= (stored high-water mark - lookback_seconds)`. The filter runs on the e-conomic side, so only changed records cross the network. The live e-conomic API accepts the full date-and-time filter the connector sends (for example `lastUpdated$gte:2024-12-07T23:04:00`) on time entries, cost types, contacts and delivery locations.
3. **Batching.** If a scan has more than `max_records_per_batch` matching records, it is split across several micro-batches. Each batch continues from the last primary key returned, by adding a primary-key condition to the same `lastUpdated` filter. The live API accepts this combined filter.
4. **Committing progress.** The `lastUpdated` high-water mark advances only after a scan finishes. If a run fails partway, the next run repeats the scan and does not skip records. The new high-water mark is the largest `lastUpdated` seen, capped at the current UTC time when the run started. Records that change while a run is in progress are picked up by the next run.
5. **Upserts.** Records are merged into the destination by primary key (`number`). The lookback window and repeated scans therefore never create duplicates.

Each CDC table keeps its own high-water mark, so tables in the same pipeline progress independently.

**Expect some rows to be re-read on every run.** e-conomic's `lastUpdated` values carry no time zone and appear to be Danish local time (CET/CEST, one or two hours ahead of UTC), while the high-water mark is capped at UTC "now". Rows changed in roughly the last one to two hours therefore have a `lastUpdated` later than the cap, and each run reads them again, on top of the `lookback_seconds` window. This is harmless: the upsert by primary key absorbs the repeated rows, and no changes are lost.

Snapshot tables read the full table on every run, using cursor pagination of up to 1,000 records per request.

#### Handling deletes (periodic full refresh)

e-conomic does not report deletions. Records that can be deleted in e-conomic include:

- unapproved time entries,
- projects and cost types that nothing else references,
- customer contacts that are not in use, and customer delivery locations.

After a deletion, the row stays in the destination tables for the CDC tables (`projects`, `project_time_entries`, `cost_types`, `customer_contacts`, `customer_delivery_locations`) until you run a full refresh. Snapshot tables, including `mileages` and `time_entry_prices`, pick up deletions automatically on the next run.

If you need the destination to match e-conomic exactly, schedule a **periodic full refresh** of the CDC tables. Use the pipeline UI (**Full refresh selection**) or the pipelines API (`full_refresh_selection`). Weekly or monthly is usually enough for accounting data volumes. A full refresh clears the stored progress, so the table is reloaded from `start_timestamp` if one is set, or from the full history otherwise. To keep all history after a full refresh, leave `start_timestamp` unset on tables you refresh fully.

`customer_contacts` has an `isDeleted` column, but it does not replace a full refresh. See [Schema highlights](#schema-highlights).

### Schema highlights

The schemas are static and defined by the connector. All records are flat, with no nested objects or arrays.

**General rules (all tables)**

- **Missing values are null.** e-conomic leaves empty properties out of its responses instead of returning `null`. Every column except the primary key is nullable.
- **Timestamps appear to be Danish local time, with no time zone.** `lastUpdated` and the other date-time columns arrive without an offset, for example `2024-12-07T23:14:00`. Live checks against the e-conomic demo agreement show that they appear to be Danish local time (CET in winter, CEST in summer), not UTC. For example, a time entry whose text records `07-12-2024 23:13:32 +01:00` has `lastUpdated = 2024-12-07T23:14:00`. This was checked on time entries. The connector treats the timestamps of every table the same way. `lastUpdated` has whole-minute precision. See [Data Type Mapping](#data-type-mapping) for how to convert these values to UTC.
- **Boolean `false` may arrive as null.** According to the e-conomic API documentation, a `false` boolean can be left out of the response body. In practice the behavior is inconsistent. When you filter on flags such as `isBarred`, `isClosed`, `isMainProject`, `isApproved`, `isReconciled`, `access`, `defaultDisableEInvoicing`, `canApprove`, `canInvoice`, `isUser`, `hideInSearch`, `includeCostPriceInFinance`, `includeSalesPriceInFinance`, the `receive...` flags, `isDeleted` or `active`, treat null as `false`, for example `coalesce(isBarred, false)`.
- **`objectVersion`** (all tables except `time_entry_prices`) is an opaque concurrency token. It changes when a record changes, but it is not ordered and cannot be used as a change cursor.
- **Integer codes.** Several columns are integer codes that you map to names downstream:
  - `project_statuses.type`: `1` = Quote sent, `2` = Realised.
  - `activity_groups.type`: `1` = Productive, invoiceable; `2` = Productive, not invoiceable; `3` = Unproductive.
  - `project_groups.type`: `1` = Internal; `2` = External, invoiceable; `3` = External, not invoiceable.
  - `costAccountOngoingType` and `salesAccountOngoingType` (activity groups and project groups): `0` = Debit, `1` = Credit.
  - `project_employees.employeeType`: `0` = Not an employee, `1` = Time logger, `2` = Project manager, `3` = Mobile time logger.

**Projects and time**

- **`projects`**:
  - `lastUpdated` is the incremental cursor.
  - `costPrice`, `salesPrice` and `invoicedTotal` are read-only totals that e-conomic calculates from registrations on the project. See [Known Limitations](#known-limitations) for how fresh they are.
  - `status` is an integer code (`1` = Quote sent, `2` = Realised), matching `project_statuses`. In the demo agreement, status `number` and `type` coincide, so it is not confirmed whether `projects.status` refers to `project_statuses.number` or `project_statuses.type`.
  - `customerNumber` links to `customers.customerNumber` (and `project_customers.number`). `responsibleEmployeeNumber` and `otherResponsibleEmployeeNumber` link to `employees.number`. `mainProjectNumber` links to `projects.number`. `projectGroupNumber` links to `project_groups.number`.
  - `contactPersonId` appears to link to `customer_contacts.number`, and `deliveryLocationNumber` to `customer_delivery_locations.number`. These links are not documented by e-conomic.
- **`project_time_entries`**:
  - `lastUpdated` is the incremental cursor.
  - `date` is the registration date, stored as a timestamp with a `00:00:00` time part.
  - `projectNumber` links to `projects.number`, `employeeNumber` to `employees.number`, and `activityNumber` to `activities.number`.
  - Approved entries (`isApproved = true`) cannot be changed in e-conomic. Unapproved entries can still be edited or deleted.
  - Prices are not part of this table. Join `time_entry_prices` on `number`.
- **`time_entry_prices`**:
  - One row per time entry. `number` is the time entry number, so join it 1:1 to `project_time_entries.number`.
  - `costPrice` is calculated as (employee cost price + activity markup percentage) x hours. `salesPrice` is the activity sales price x hours, falling back to the employee sales price.
  - Because the prices are calculated, they can change when activity or employee prices change, even though the time entry itself and its `lastUpdated` do not change. That is why this table is a full snapshot on every run. It is as large as the time entry table, so it is the most expensive snapshot table to read.
  - It has no `objectVersion` column.
- **`mileages`**:
  - The start and end places are in columns literally named **`from`** and **`to`**. Both are reserved words in SQL, so quote them with backticks, for example ``SELECT `from`, `to`, distance FROM mileages``.
  - `date` is the travel date (time part `00:00:00`), not an update stamp. `distance` is a decimal number.
  - `projectNumber` links to `projects.number`, and `employeeNumber` to `employees.number`.
  - Approved mileage (`isApproved = true`) cannot be changed. Unapproved mileage can be edited or deleted, which the snapshot picks up on the next run.
- **`activities`**: `groupNumber` links to `activity_groups.number`. `salesPriceBefore` applies until `cutoffDate`, and `salesPriceAfter` applies from then on.
- **`activity_groups`** and **`project_groups`**: the finance account columns (`costAccount...`, `salesAccount...`) are only set when the matching `includeCostPriceInFinance` or `includeSalesPriceInFinance` flag is true. Otherwise they are null.
- **`cost_types`**:
  - `lastUpdated` is the incremental cursor.
  - `vatCode` is a fixed-width code and keeps its **trailing spaces** exactly as e-conomic returns them, for example `'I25  '`. Use `trim(vatCode)` before you compare or join on it.
  - `costGroupNumber` refers to e-conomic cost type groups, which this connector does not ingest.

**Employees**

- **`employees`**: `groupNumber` links to `employee_groups.number`. Includes `phone` and `email`.
- **`project_employees`**:
  - Holds the **same people as `employees`**, with the same `number`, seen from the Projects module. It adds the e-conomic user link (`isUser`, `userId`), `employeeType`, approval and invoicing rights (`canApprove`, `canInvoice`), address fields, and employee cost and sales prices. It does not include phone or email.
  - Use it instead of `employees` when your grant has `ProjectEmployee` but not `Sales`. If you ingest both, treat them as two views of one employee list, joined on `number`, rather than as separate people.
  - `cutOffDate` is a **date** (`DATE`), not a timestamp. It is the only date-only column in the connector. The `...Before` prices apply until `cutOffDate`, and the `...After` prices apply from then on.
  - The cost and sales price columns may be sensitive. Restrict access to this table in Unity Catalog if needed.
  - `groupNumber` links to `employee_groups.number`.
- **`employee_groups`**: needs the `Sales` role, unlike the other Projects API configuration tables.

**Customers**

- **`customers`**: the primary key is `customerNumber`. `defaultDeliveryLocation` refers to a delivery location number in `customer_delivery_locations`.
- **`project_customers`**: a thin projection of customers (`number`, `name`). `number` equals `customers.customerNumber`. It is the **only source of customer names for a grant that has `ProjectEmployee` but not `Sales`**, because the Customers API tables need `Sales`.
- **`customer_contacts`** and **`customer_delivery_locations`**:
  - `lastUpdated` is the incremental cursor.
  - `number` is a **global id**, unique across all customers. It is **not** the number shown per customer in the e-conomic UI. That number is `userInterfaceNumber`, which starts at 1 for each customer. The pair (`customerNumber`, `userInterfaceNumber`) identifies a record as users see it, but `number` is the primary key.
  - `customerNumber` links to `customers.customerNumber`.
- **`customer_contacts.isDeleted`** and **`active`** are ordinary columns. In live checks against the demo agreement, the API never returned a contact with `isDeleted = true`, so it is not known whether e-conomic lists soft-deleted contacts at all. If such a row does arrive, it is upserted with `isDeleted = true`, so filter on `coalesce(isDeleted, false) = false` downstream. Contacts that disappear from e-conomic are still only removed by a [full refresh](#handling-deletes-periodic-full-refresh).

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
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. For the CDC tables (`projects`, `project_time_entries`, `cost_types`, `customer_contacts`, `customer_delivery_locations`), `lastUpdated` is the natural choice. |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

The source-specific options below apply only to the 5 **CDC tables**: `projects`, `project_time_entries`, `cost_types`, `customer_contacts` and `customer_delivery_locations`. The 11 snapshot tables (`employees`, `employee_groups`, `customers`, `activities`, `activity_groups`, `project_groups`, `project_statuses`, `mileages`, `project_employees`, `project_customers`, `time_entry_prices`) ignore them.

| Option | Type | Default | Applies to | Description |
|---|---|---|---|---|
| `start_timestamp` | ISO-8601 string | none (full history) | CDC tables | Lower bound on `lastUpdated` for the **first** incremental run, for example `2024-01-01T00:00:00`. Used only when the table has no stored progress yet. If you omit it, the first run reads the full history. Because `lastUpdated` is Danish local time, write the value **without** an offset, in Danish local time. A value with `Z` or an offset is converted to UTC before it is compared with `lastUpdated`, which shifts the bound by one to two hours. Fractional seconds are dropped. An unparsable value fails the run. |
| `lookback_seconds` | integer, `>= 0` | `600` | CDC tables | Number of seconds subtracted from the stored `lastUpdated` high-water mark at the start of each new scan, so that recently changed rows are read again. Re-read rows are de-duplicated by primary key. e-conomic stores `lastUpdated` with whole-minute precision, so keep this value at 60 or higher. Each run also re-reads roughly the last one to two hours of changes because of the time zone difference described in [Incremental sync behavior](#incremental-sync-behavior). |
| `max_records_per_batch` | integer, `> 0` | `5000` | CDC tables | Maximum number of records the connector returns in one micro-batch. If a scan has more matching records, it continues in the next micro-batch from the last primary key it returned. Lower the value to reduce memory per batch. |

The small CDC tables (`cost_types`, `customer_contacts`, `customer_delivery_locations`) usually work well with the defaults. `start_timestamp` is mostly useful on `project_time_entries` and `projects`, where history can be large.

## Data Type Mapping

| e-conomic API type | Example fields | Databricks type | Notes |
|---|---|---|---|
| integer (int32) | `number`, `customerNumber`, `projectNumber`, `groupNumber`, `userInterfaceNumber`, `status`, `type`, `priority`, `employeeType` | `BIGINT` (`LongType`) | All identifiers and integer codes are stored as 64-bit integers. |
| number (double) | `numberOfHours`, `fixedPrice`, `costPrice`, `salesPrice`, `invoicedTotal`, `creditMaximum`, `mileage`, `distance`, `salesPriceBefore`, `costPriceMarkupPercentage` | `DOUBLE` (`DoubleType`) | e-conomic sends amounts as JSON decimals (for example `4080.00`). If you need exact precision, cast to `DECIMAL` downstream. |
| boolean | `isBarred`, `isClosed`, `isApproved`, `access`, `canApprove`, `isDeleted`, `active` | `BOOLEAN` (`BooleanType`) | A missing value is null and may mean `false`. See [Schema highlights](#schema-highlights). |
| string | `name`, `email`, `currency`, `description`, `text`, `vatCode`, `from`, `to`, `objectVersion` | `STRING` (`StringType`) | Stored exactly as returned, so `cost_types.vatCode` keeps its trailing spaces. `mileages.from` and `mileages.to` must be quoted with backticks in SQL. |
| date-time (ISO-8601, no offset) | `lastUpdated`, `date`, `deliveryDate`, `closedDate`, `directDebitMandateDate`, `cutoffDate` (activities) | `TIMESTAMP` (`TimestampType`) | e-conomic sends timestamps without a time zone offset (for example `2024-12-07T23:14:00`). The values appear to be Danish local time (CET/CEST), not UTC. The connector stores the wall-clock value unchanged, and Spark interprets it in the session time zone. With a UTC session time zone, convert to true UTC with `to_utc_timestamp(lastUpdated, 'Europe/Copenhagen')`. |
| date | `cutOffDate` (project_employees) | `DATE` (`DateType`) | The only date-only column. e-conomic sends it as `YYYY-MM-DD` or with a `T00:00:00` suffix, and both are stored as a date. Note the different casing from `activities.cutoffDate`, which is a timestamp. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Add one `table` entry for each table you want to ingest. Snapshot tables need only `source_table`. Put the source-specific options for the CDC tables under `table_configuration`. The example below ingests all 16 tables, which requires a `SuperUser` grant or both `Sales` and `ProjectEmployee`:

```json
{
  "pipeline_spec": {
    "connection_name": "economic_connection",
    "object": [
      { "table": { "source_table": "employees" } },
      { "table": { "source_table": "employee_groups" } },
      { "table": { "source_table": "activities" } },
      { "table": { "source_table": "activity_groups" } },
      { "table": { "source_table": "project_groups" } },
      { "table": { "source_table": "project_statuses" } },
      { "table": { "source_table": "mileages" } },
      { "table": { "source_table": "project_employees" } },
      { "table": { "source_table": "project_customers" } },
      { "table": { "source_table": "time_entry_prices" } },
      { "table": { "source_table": "customers" } },
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
      },
      { "table": { "source_table": "cost_types" } },
      {
        "table": {
          "source_table": "customer_contacts",
          "destination_table": "economic_customer_contacts",
          "table_configuration": {
            "lookback_seconds": "600"
          }
        }
      },
      { "table": { "source_table": "customer_delivery_locations" } }
    ]
  }
}
```

- `connection_name` must point to the Unity Catalog connection that holds your `app_secret_token` and `agreement_grant_token`.
- `source_table` must be one of the 16 table names listed under [Supported Objects](#supported-objects).
- To add a table to an existing pipeline, append another `{ "table": { "source_table": "<name>" } }` entry. A new CDC table starts with a full first load (or from its own `start_timestamp`), independent of the other tables.
- Include only the tables your grant's roles allow. With a `ProjectEmployee`-only grant, remove `employees`, `employee_groups`, `customers`, `customer_contacts` and `customer_delivery_locations`, and use `project_employees` and `project_customers` instead. With a `Sales`-only grant, keep only those five tables.
- Write option values as strings.

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

Run the pipeline with your usual Databricks orchestration, such as a scheduled job.

- **First run**: snapshot tables load in full. CDC tables load either the full history or everything from `start_timestamp`.
- **Later runs**: snapshot tables are reloaded in full. CDC tables read only the records changed since the last run, plus the lookback window.

#### Best Practices

- **Start small**: validate the connection and data shape with the `demo` tokens or a single table, such as `employees` or `project_statuses`, before you add the rest.
- **Use incremental sync**: for large agreements, rely on the CDC tables. Set `start_timestamp` only if you don't need older history.
- **Mind `time_entry_prices`**: it is a full snapshot as large as `project_time_entries`. On agreements with many time entries, it dominates the API calls of each run. Ingest it only if you need the calculated prices, and consider a less frequent schedule for it.
- **Schedule periodic full refreshes** of the CDC tables (`projects`, `project_time_entries`, `cost_types`, `customer_contacts`, `customer_delivery_locations`) to remove deleted records (see [Handling deletes](#handling-deletes-periodic-full-refresh)).
- **Set appropriate schedules**: accounting data changes slowly. Hourly or daily runs are usually enough, and they use far fewer API calls than very frequent runs.
- **Respect rate limits**: e-conomic rate limits API calls **per app and agreement** with a token bucket. Each call has a cost, which e-conomic reports in the `X-CallCost` response header. Pipelines, scripts and other integrations that use the same app and agreement share the same budget. Each table needs about one call per 1,000 records it reads, so a run of all 16 tables needs at least 16 calls even when nothing changed. Very frequent runs on large agreements can use up the budget.
- **Keep `lookback_seconds` at 60 or more**, because `lastUpdated` has whole-minute precision.

#### Rate limits and retries

The connector handles e-conomic throttling and transient errors automatically:

- It retries HTTP `429`, `500`, `502`, `503` and `504` responses, connection errors and timeouts, with up to 6 attempts per request.
- On `429`, it waits for the number of seconds in the `Retry-After` header, up to 60 seconds. Without the header, it waits 1, 2, 4, 8, then 16 seconds between attempts.
- Each request times out after 60 seconds.
- Requests run one at a time from a single reader per table. The connector never sends parallel requests for a table that could drain the bucket faster.

e-conomic does not publish fixed bucket sizes or refill rates, and the actual limits may vary per agreement. If you see frequent retries in the logs, run the pipeline less often.

#### Troubleshooting

**Common Issues:**

- **`401 Unauthorized` (for example `ApplicationSecretTokenMissing`)**
  - Check that `app_secret_token` and `agreement_grant_token` are both set and correct, and that the secret references resolve.
  - Make sure you have not swapped the two tokens, or used the app *public* token instead of the app *secret* token.
  - The grant may have been revoked in e-conomic. Ask the agreement administrator to grant access again, and update `agreement_grant_token`.
- **`403 Forbidden` on some tables only**
  - The grant lacks the role for that resource. `employees`, `employee_groups`, `customers`, `customer_contacts` and `customer_delivery_locations` need `Sales` or `SuperUser`. The other 11 tables need `ProjectEmployee` or `SuperUser`. Either remove the affected tables from the pipeline, or update the app's required role and have the agreement grant access again.
  - `employee_groups` fails with `403` on a `ProjectEmployee`-only grant even though it comes from the Projects API, because it needs `Sales`.
- **`Invalid 'start_timestamp'` error**
  - `start_timestamp` must be a valid ISO-8601 value such as `2024-01-01T00:00:00`. The run fails before any request is sent to e-conomic.
- **First run starts one to two hours off from `start_timestamp`**
  - Remove the `Z` or offset from `start_timestamp` and give the value in Danish local time. Values with an offset are converted to UTC, while `lastUpdated` is Danish local time.
- **`400 Bad Request`**
  - e-conomic rejected the request parameters. Check that `projects_api_version` and `customers_api_version` point to versions that support the filters the connector uses. The default versions accept these filters on the live API. See the `errorCode` / `detail` text in the error message.
- **`404 Not Found` on every table**
  - Check `base_url`, `projects_api_version` and `customers_api_version`. An API version that does not exist on e-conomic returns `404`. Leave these options unset to use the tested defaults.
- **Deleted records still appear in Databricks**
  - This is expected for the CDC tables (`projects`, `project_time_entries`, `cost_types`, `customer_contacts`, `customer_delivery_locations`). Run a full refresh of the affected table (see [Handling deletes](#handling-deletes-periodic-full-refresh)).
- **Table options seem to be ignored**
  - Check that `externalOptionsAllowList` on the connection is exactly `max_records_per_batch,lookback_seconds,start_timestamp`.
  - `start_timestamp` only affects the first run. Changing it later has no effect until you run a full refresh of that table.
  - Options set on the 11 snapshot tables are ignored. Only the 5 CDC tables read them.
- **SQL syntax error when querying `mileages`**
  - The columns `from` and `to` are reserved words. Quote them with backticks: ``SELECT `from`, `to` FROM mileages``.
- **Joins or filters on `cost_types.vatCode` find no match**
  - `vatCode` keeps the trailing spaces e-conomic returns. Use `trim(vatCode)`.
- **Contact or delivery location numbers don't match the e-conomic UI**
  - `number` is a global id. The per-customer number shown in the UI is `userInterfaceNumber`.
- **`time_entry_prices` values changed although no time entry changed**
  - This is expected. Prices are calculated from activity and employee prices, which can change independently. The table is re-read in full on every run.
- **The same recent rows are processed on every run**
  - This is expected. `lastUpdated` is Danish local time, while the connector caps its progress marker at UTC, so each run re-reads roughly the last one to two hours of changes plus `lookback_seconds`. The rows are upserted by primary key, so the destination has no duplicates (see [Incremental sync behavior](#incremental-sync-behavior)).
- **Very few rows with the `demo` tokens**
  - This is expected. The public demo agreement holds only a small sample data set.

## Known Limitations

- **Read-only.** The connector never writes back to e-conomic.
- **16 tables only.** Some related e-conomic resources are not covered, including Cost Type Groups, Project Activities, Mileage Prices and Project Employee Groups (a copy of employee groups that needs `ProjectEmployee` instead of `Sales`). Neither are other resource APIs such as products or invoices.
- **No delete synchronization.** e-conomic exposes no deleted-records feed. The CDC tables keep deleted rows until you run a full refresh. Snapshot tables reflect deletions on every run.
- **`customer_contacts.isDeleted` is not a delete feed.** It is unproven whether e-conomic ever returns soft-deleted contacts in the list, so the connector keeps `isDeleted` as an ordinary column and does not delete rows based on it.
- **Rows with an empty `lastUpdated`.** The e-conomic schema allows `lastUpdated` to be null on projects, and e-conomic notes that not all resources track it. In live checks against the demo agreement, `lastUpdated` was set on every project and every time entry, so this looks rare. If such a row does exist, an unfiltered first run loads it, but later incremental runs cannot detect changes to it. A periodic full refresh also covers these rows.
- **Freshness of project totals.** e-conomic documents that `lastUpdated` changes when a property of the record changes, and that changes to related records can also update it. It is not confirmed whether new time registrations always update `lastUpdated` on the parent project, so `costPrice`, `salesPrice` and `invoicedTotal` in `projects` may be stale between full refreshes. If these totals must always be current, run frequent full refreshes of `projects`. The table is usually small. Per-entry prices in `time_entry_prices` are always current, because that table is a full snapshot.
- **Time zone of timestamps.** e-conomic sends timestamps without a time zone offset and does not document their time zone. Live checks indicate Danish local time (CET/CEST). The connector does not convert them, so convert downstream if you need UTC (see [Data Type Mapping](#data-type-mapping)). As a side effect, each incremental run re-reads roughly the last one to two hours of changes. The upsert by primary key absorbs these rows, and no data is lost.
- **Multi-page reads not yet verified live.** e-conomic returns at most 1,000 records per page and the connector follows the pagination cursor across pages. The public demo agreement fits in a single page, so reads that span more than 1,000 records have not yet been verified against a live agreement. If a large table loads fewer rows than e-conomic shows, or a read fails with a pagination error, report it to the connector maintainers.
- **Monetary values as `DOUBLE`.** Amounts are stored as floating-point numbers. Cast them to `DECIMAL` downstream for exact financial arithmetic.
- **Single sequential reader.** e-conomic list endpoints cannot sort, the rate limit is per agreement, and accounting data sets are usually small. For these reasons the connector reads each table from a single reader, without partitioning.
- **Safety limit on pages.** As a guard against a misbehaving cursor, one read follows at most 10,000 pages of up to 1,000 records each, which is about 10 million rows, and fails with an error beyond that. A read also fails with an error if e-conomic returns the same pagination cursor twice.
- **One agreement per connection.** To ingest several e-conomic companies, create a separate connection and pipeline for each agreement.

## References

- e-conomic OpenAPI documentation index: <https://apis.e-conomic.com/>
- e-conomic Projects API reference (employees, employee groups, projects, time entries, activities, groups, statuses, mileage, project employees and customers, time entry prices, cost types): <https://apis.e-conomic.com/projectsapi/redoc.html>
- e-conomic Customers API reference (customers, contacts, delivery locations): <https://apis.e-conomic.com/customersapi/redoc.html>
- e-conomic developer connection and token documentation: <https://www.e-conomic.com/developer/connect>
- e-conomic rate limiting help article: <https://www.e-conomic.dk/support/artikler/rate-limiting-on-the-e-conomic-public-api>
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)
