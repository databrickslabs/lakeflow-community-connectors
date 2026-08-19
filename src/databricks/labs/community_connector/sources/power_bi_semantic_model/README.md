# Lakeflow Power BI Semantic Model Community Connector

This documentation describes how to configure and use the **Power BI semantic model** Lakeflow community connector to ingest semantic-model *metadata* from the Power BI REST API into Databricks.

> **Naming note**: "Semantic model" is Microsoft's current product name for what the Power BI REST API still calls a **dataset**. The connector's table and column names deliberately mirror the REST API's `dataset` / `datasets` vocabulary so every field maps back to the official reference; only the connector's display name says "semantic model".

## What This Connector Does

The connector builds a governance/observability picture of the semantic models in a Power BI (Fabric) tenant. It reads **metadata about models** — which workspaces exist, which semantic models live in them, the tables/columns/DAX measures defined inside each model, and the refresh run history of each model.

It reads the rows stored *inside* a model only when you explicitly ask it to, via the opt-in `dax_query_result` table and a DAX query you supply. See [Reading row data with `dax_query_result`](#reading-row-data-with-dax_query_result).

Typical uses:

- Catalog every semantic model in the tenant and who owns it (`datasets.configuredBy`).
- Track model structure over time — tables, columns, DAX measure expressions.
- Monitor refresh reliability: success/failure rates, durations, and error payloads.

Data is retrieved from three API surfaces:

| Surface | Endpoints | Used for |
|---|---|---|
| Admin list APIs (tenant-wide) | `GET /admin/groups`, `GET /admin/datasets` | `workspaces`, `datasets` |
| Admin metadata scanner ("Scanner API") | `POST /admin/workspaces/getInfo` → `GET /admin/workspaces/scanStatus/{id}` → `GET /admin/workspaces/scanResult/{id}` | `dataset_tables`, `dataset_columns`, `dataset_measures` |
| Workspace-scoped (non-admin) API | `GET /groups/{groupId}/datasets/{datasetId}/refreshes` | `dataset_refresh_history` |

The connector implements partitioned reads. Because the Power BI REST API exposes no `since`/`until` range filters, there is no time axis to split on — instead the connector partitions along the fan-out the API forces on it: batches of workspace IDs for the scanner (max 100 per scan) and batches of `(workspace, dataset)` pairs for refresh history. These parallelize across executors.

## Prerequisites

- **A Power BI / Fabric tenant** with at least one workspace containing a semantic model.
- **Credentials for one of two auth methods** — Power BI's own docs describe both:
  - `service_principal` (preferred): a Microsoft Entra ID service principal (app registration plus a client secret). The connector authenticates non-interactively with the OAuth 2.0 client-credentials flow.
  - `user`: a specific Power BI user's Entra ID credentials (the classic "master user" pattern), via the Resource Owner Password Credentials (ROPC) grant. The account must have a Power BI Pro/PPU license, workspace access, and **no MFA** (ROPC can't satisfy an interactive challenge). Microsoft treats ROPC as legacy — prefer `service_principal` unless you specifically need to run as a real user.
- **Tenant admin cooperation.** Several settings must be turned on **once, by a Fabric/Power BI tenant administrator, in the Power BI Admin Portal**. The connector cannot self-serve these, and there is no runtime workaround — see [Tenant Setup](#tenant-setup-required-admin-actions). Plan for this: it is the single most common reason a first pipeline run fails or returns empty schema tables.
- **Network access**: the environment running the connector must reach `https://login.microsoftonline.com` (token issuance) and `https://api.powerbi.com` (data).
- **Lakeflow / Databricks environment**: a workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `tenant_id` | string | yes | Microsoft Entra ID tenant (directory) ID. Selects the per-tenant token endpoint. | `72f988bf-86f1-41af-91ab-2d7cd011db47` |
| `client_id` | string | yes | Application (client) ID of the Entra ID app registration. | `1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d` |
| `client_secret` | string | yes for `service_principal` | Client secret for the service principal. Store as a secret. | `abc~1Xy...` |
| `username` | string | yes for `user` | Power BI user's UPN/email (the `user` / ROPC method). | `analyst@tenant.onmicrosoft.com` |
| `password` | string | yes for `user` | That user's password. Store as a secret. | — |
| `use_admin_api` | boolean | no | `true` (default) prefers the tenant-wide `/admin` endpoints; `false` forces the workspace-membership-scoped endpoints. See [Admin API vs. membership-scoped access](#admin-api-vs-membership-scoped-access). | `true` |
| `base_url` | string | no | Override the API root. Defaults to `https://api.powerbi.com/v1.0/myorg`. Only needed for sovereign/government clouds. | `https://api.powerbigov.us/v1.0/myorg` |
| `timeout_seconds` | integer | no | Per-request HTTP timeout in seconds. Defaults to `60`. | `120` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector supports table-specific options, so this parameter must be set. | see below |

This connector requires `externalOptionsAllowList`. The full, definitive list of supported table-specific options is:

```
workspace_ids,dataset_ids,workspace_filter,use_admin_api,page_size,max_pages,scan_batch_size,scan_poll_seconds,scan_timeout_seconds,workspaces_per_partition,datasets_per_partition,top,max_records_per_batch,dax_query,workspace_id,dataset_id,dax_columns,include_nulls,impersonated_user_name
```

> **Note**: Options such as `workspace_ids` and `dataset_ids` are **not** connection parameters. They are supplied per table under `table_configuration` in the pipeline spec, and their names must appear in `externalOptionsAllowList` for the connection to allow them through.

The connector performs the token exchange itself — `client_credentials` grant for `service_principal` (`tenant_id`/`client_id`/`client_secret`), `password` grant for `user` (`tenant_id`/`client_id`/`username`/`password`) — there is no interactive browser sign-in and you never paste an access token. `client_secret` wins if both a secret and a username/password are supplied.

The `user` method does **not** need the tenant-admin "Allow service principals to use Power BI APIs" steps in [Tenant Setup](#tenant-setup-required-admin-actions) — those apply only to `service_principal`. Instead, the Power BI user must already have the workspace access (and, for `dataset_refresh_history`, the Member/Admin role) that section describes granting to the service principal.

### Obtaining the Required Parameters

1. Sign in to the [Azure Portal](https://portal.azure.com) → **Microsoft Entra ID** → **App registrations** → **New registration**. Give the app a name; no redirect URI is needed (this is a non-interactive, server-to-server app).
2. On the app's **Overview** page, copy:
   - **Directory (tenant) ID** → use as `tenant_id`
   - **Application (client) ID** → use as `client_id`
3. Go to **Certificates & secrets** → **New client secret**. Copy the secret **Value** (not the Secret ID) immediately — it is shown only once. Use it as `client_secret`. Note the expiry date and plan rotation.
4. **Do not add Power BI API permissions in the Entra "API permissions" blade.** This is counter-intuitive but important: when authenticating as a service principal, the Entra app registration must **not** have admin-consent-required Power BI delegated/application permissions configured. Power BI grants service-principal access entirely through the Power BI Admin Portal (next section). Adding Entra-side Power BI permissions to a service-principal app causes confusing failures.

### Tenant Setup (Required Admin Actions)

**These steps must be performed once by a Fabric/Power BI tenant administrator. The connector cannot perform or work around them.** Without them the connector will either fail with `401`/`403` or silently return empty schema tables.

1. **Create an Entra ID security group** (e.g. `pbi-api-service-principals`) and add the service principal from the previous section as a member. This is the recommended way to scope access rather than enabling it tenant-wide.

2. **Enable service principal API access.** In the **Power BI Admin Portal → Tenant settings → Developer settings → "Allow service principals to use Power BI APIs"**: enable it and scope it to the security group created above.

   This is mandatory for *every* table. Without it, the service principal cannot call any Power BI REST endpoint. This is not something the connector can self-serve — a tenant admin has to flip it.

3. **Enable metadata scanning.** In the **Power BI Admin Portal → Tenant settings → Admin API settings**, enable both:
   - **"Enhance admin APIs responses with detailed metadata"**
   - **"Enhance admin APIs responses with DAX and mashup expressions"**

   These are required for `dataset_tables`, `dataset_columns`, and `dataset_measures`. This is the most commonly missed step, and its failure mode is quiet rather than loud: the scan **succeeds**, but the schema arrays come back empty, so those three tables ingest **zero rows** (or rows with `null` `expression` values) with no error raised. If those tables look empty, check these two settings first.

   Also ensure **"Allow service principals to use read-only admin APIs"** is enabled and scoped to the same security group, so the `/admin` endpoints are reachable.

4. **Grant workspace access for refresh history.** `dataset_refresh_history` uses the *non-admin*, workspace-scoped refreshes endpoint even when `use_admin_api` is `true` — the Admin API has no refresh-history equivalent. Add the service principal as a **Member** or **Admin** of every workspace whose refresh history you want to ingest. Contributor-level (write) access on the dataset is required for the endpoint to return the full field set; Viewer is not sufficient.

   If you only need `workspaces`, `datasets`, and the three scanner tables, per-workspace membership is not required — Admin API access covers the whole tenant.

5. **Only if you use `dax_query_result`: enable DAX query execution.** In the **Power BI Admin Portal → Tenant settings → Integration settings → "Dataset Execute Queries REST API"**: enable it and scope it to the same security group. The service principal also needs at least read access to the workspace holding the model — this endpoint is workspace-scoped, not an Admin API. Skip this step entirely if you are not ingesting `dax_query_result`.

Setting expectations: allow up to ~15 minutes for tenant setting changes to take effect.

### Admin API vs. membership-scoped access

The connector defaults to the tenant-wide `/admin` endpoints (`use_admin_api = true`) because that avoids adding the service principal to every workspace individually. Behavior when Admin access is unavailable differs by table:

| Table | Behavior on `401`/`403` from `/admin` |
|---|---|
| `workspaces` | Falls back automatically to `GET /groups` — returns only workspaces the service principal is a member of. |
| `datasets` | Falls back automatically to per-workspace `GET /groups/{groupId}/datasets`. Note the non-admin endpoint returns a **reduced field set** (essentially `id` and `name`) unless the caller has write access on the dataset, and does not return `workspaceId` — the connector fills it in from the calling context. |
| `dataset_tables`, `dataset_columns`, `dataset_measures` | **No fallback.** The Scanner API is Admin-only; the read fails with an error. These three tables hard-require Admin API access. |
| `dataset_refresh_history` | Always uses the non-admin endpoint; requires workspace membership regardless of `use_admin_api`. |

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source, or create a new one, supplying `tenant_id`, `client_id`, and `client_secret`.
3. Set `externalOptionsAllowList` to:
   `workspace_ids,dataset_ids,workspace_filter,use_admin_api,page_size,max_pages,scan_batch_size,scan_poll_seconds,scan_timeout_seconds,workspaces_per_partition,datasets_per_partition,top,max_records_per_batch,dax_query,workspace_id,dataset_id,dax_columns,include_nulls,impersonated_user_name`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes a **static list** of seven tables. Table names are lowercase with underscores and must be spelled exactly as shown:

- `workspaces`
- `datasets`
- `dataset_tables`
- `dataset_columns`
- `dataset_measures`
- `dataset_refresh_history`
- `dax_query_result` — **opt-in**; see [Reading row data with `dax_query_result`](#reading-row-data-with-dax_query_result)

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Cursor |
|---|---|---|---|---|
| `workspaces` | Workspaces ("groups") in the tenant — the container for semantic models | `snapshot` | `id` | n/a |
| `datasets` | Semantic models (datasets), one row per model across all workspaces | `snapshot` | `id` | n/a |
| `dataset_tables` | Tables defined inside each semantic model | `snapshot` | `["dataset_id", "name"]` | n/a |
| `dataset_columns` | Columns defined inside each semantic model table | `snapshot` | `["dataset_id", "table_name", "name"]` | n/a |
| `dataset_measures` | DAX measures defined inside each semantic model table | `snapshot` | `["dataset_id", "table_name", "name"]` | n/a |
| `dataset_refresh_history` | Refresh runs (scheduled, on-demand, and API-triggered) per semantic model | `append` | `["dataset_id", "requestId"]` | `startTime` |
| `dax_query_result` | Rows returned by **your own** DAX query against **one** semantic model. Empty unless configured. | `snapshot` | `["dataset_id", "query_hash", "row_index"]` | n/a |

**Why five of six are snapshots.** None of the Power BI list endpoints expose a `lastModifiedDateTime` or delta-link mechanism, and the Scanner API always returns a full metadata tree. There is no way to ask "what changed since X", so each run re-lists everything. `dataset_refresh_history` is the exception: each refresh run is an immutable, timestamped, uniquely identified (`requestId`) event, so runs accumulate and can be appended.

**Delete detection.** No object supports `cdc_with_deletes`. Deleted workspaces and models are only detectable by diffing successive snapshots downstream. (The Admin `groups` endpoint does support `$filter=state eq 'Deleted'` to surface soft-deleted workspaces — pass it via the `workspace_filter` table option on `workspaces` if you need this — but it is not wired up as a distinct ingestion mode.)

### How the tables join

```
workspaces.id ─┬─< datasets.workspaceId
               │
               └─< dataset_tables.workspace_id, dataset_columns.workspace_id,
                   dataset_measures.workspace_id, dataset_refresh_history.workspace_id

datasets.id ───┬─< dataset_tables.dataset_id
               ├─< dataset_columns.dataset_id  (+ table_name → dataset_tables.name)
               ├─< dataset_measures.dataset_id (+ table_name → dataset_tables.name)
               └─< dataset_refresh_history.dataset_id
```

`workspace_id`, `dataset_id`, and `table_name` on the scanner-derived tables are **connector-derived** foreign keys — the raw scan response nests these entities rather than repeating the parent IDs on each row.

### Schema highlights

Full schemas are defined in `power_bi_semantic_model_schemas.py`. Columns worth calling out:

- **`datasets.workspaceId`** — native on Admin API rows, connector-derived on non-admin rows. Always populated when present.
- **`datasets.queryScaleOutSettings` / `datasets.encryption`** — nested structs. The connector normalizes empty objects (`{}`) to `null`.
- **`dataset_columns.dataType`** — this is the *semantic model's own* type label from the Tabular engine (`Int64`, `String`, `Double`, `Decimal`, `DateTime`, `Boolean`, …), passed through verbatim as a string. It is **not** a Spark type, and the connector deliberately does not map it to one: these tables describe metadata *about* a model, not rows conforming to that model's schema.
- **`dataset_measures.expression`** — the raw DAX text of the measure. Populated only when the tenant's metadata-scanning settings (step 3 above) are enabled; otherwise `null`.
- **`dataset_tables.source`** — array of `{expression}` structs holding the raw Power Query (M) text. Same tenant-setting dependency. Entries without an expression are dropped rather than emitted as empty structs.
- **`dataset_refresh_history.status`** — `Unknown` (still running), `Completed`, `Failed`, or `Disabled`. See the caveat about in-progress rows under [Limitations](#limitations).
- **`dataset_refresh_history.refreshAttempts`** — array of structs describing Power BI's automatic retry attempts within a single refresh request. Its `executionMetrics` field is a free-form blob that the connector JSON-encodes into a string.
- **`dataset_refresh_history.serviceExceptionJson`** — JSON-encoded error code/details, populated when `status = Failed`.

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
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Applies to the five snapshot tables only; `dataset_refresh_history` is APPEND_ONLY and does not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

All are optional. Every option name used here must also appear in the connection's `externalOptionsAllowList`.

**Scoping options — apply to all tables:**

| Option | Type | Default | Description |
|---|---|---|---|
| `workspace_ids` | comma-separated string | *(all)* | Allow-list of workspace IDs. Scopes every fan-out — list, scan, and refresh-history reads. **Strongly recommended for large tenants**, both to control cost and to stay inside the Admin API rate limits. |
| `dataset_ids` | comma-separated string | *(all)* | Allow-list of dataset (semantic model) IDs. |
| `use_admin_api` | boolean | connection value | Per-table override of the connection-level setting. |

**`workspaces` only:**

| Option | Type | Default | Description |
|---|---|---|---|
| `workspace_filter` | string | *(none)* | OData `$filter` passed to `/admin/groups`, e.g. `state eq 'Active'` to exclude soft-deleted workspaces. Ignored on the non-admin fallback path. |

**Admin list paging — `workspaces` and `datasets`:**

| Option | Type | Default | Description |
|---|---|---|---|
| `page_size` | integer | `1000` | `$top` used when paging Admin list endpoints. Clamped to a maximum of `5000` (the service cap). |
| `max_pages` | integer | `50` | Safety cap on the number of pages fetched, guarding against a server that ignores `$skip`. |
| `workspaces_per_partition` | integer | `10` | Workspaces per partition when reading `datasets` via the non-admin path (`use_admin_api = false`). No effect in Admin mode, which is a single partition. |

**Scanner tables — `dataset_tables`, `dataset_columns`, `dataset_measures`:**

| Option | Type | Default | Description |
|---|---|---|---|
| `scan_batch_size` | integer | `100` | Workspace IDs per metadata scan. Clamped to a maximum of `100` (the API's hard limit per `getInfo` call). Each batch becomes one partition. |
| `scan_poll_seconds` | integer | `3` | Initial interval between `scanStatus` polls. The connector backs off exponentially up to 30s. |
| `scan_timeout_seconds` | integer | `600` | How long to wait for a single scan to reach `Succeeded` before failing. Raise it for very large workspace batches. |

**`dataset_refresh_history` only:**

| Option | Type | Default | Description |
|---|---|---|---|
| `top` | integer | `60` | `$top` on the refreshes endpoint — how many most-recent refresh entries to request per dataset. |
| `datasets_per_partition` | integer | `20` | `(workspace, dataset)` pairs per partition. |
| `max_records_per_batch` | integer | `1000` | Caps a single non-partitioned `read_table` micro-batch. Batches are bounded by whole datasets, never by truncating one dataset's response — truncating would duplicate rows on an append-only table. |

**`dax_query_result` only:**

| Option | Type | Default | Description |
|---|---|---|---|
| `dax_query` | string | *(none)* | The DAX `EVALUATE` statement to execute. **Without it the table yields no rows.** |
| `workspace_id` | string | from `workspace_ids` | Workspace (group) containing the model. Required once `dax_query` is set; inferred from `workspace_ids` only when that names exactly one workspace. |
| `dataset_id` | string | from `dataset_ids` | Semantic model to query. Required once `dax_query` is set; inferred from `dataset_ids` only when that names exactly one model. |
| `dax_columns` | JSON array | *(none)* | Declares the query's result columns so they land as properly typed Spark columns. See below. |
| `include_nulls` | boolean | `true` | Sends `serializerSettings.includeNulls`, so `BLANK()` cells arrive as JSON nulls instead of being omitted from the row object. |
| `impersonated_user_name` | string | *(none)* | UPN to evaluate the query as. Needed for models with row-level security. |

### Reading row data with `dax_query_result`

Every other table in this connector describes semantic models. `dax_query_result` reads the data *inside* one, by POSTing a DAX query to `/groups/{workspaceId}/datasets/{datasetId}/executeQueries`.

It is deliberately **one configured query, one table**. A DAX query is written against a specific model's tables and measures, so there is nothing to fan out across models; point the table at exactly one `workspace_id` + `dataset_id`. To ingest several queries, define several tables in your pipeline spec, each with its own `table_configuration`.

Minimal `table_configuration`:

```json
{
  "dax_query": "EVALUATE SUMMARIZECOLUMNS(Sales[Region], \"Total Amount\", SUM(Sales[Amount]))",
  "workspace_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
  "dataset_id": "11111111-2222-3333-4444-555555555555"
}
```

**Column typing.** A DAX result's column set comes from your query, so the connector cannot type it ahead of time. Two modes:

- **Declared (recommended).** Supply `dax_columns` and each column becomes a real, typed Spark column. `dax` is the key exactly as the API returns it (`Sales[Region]`, `[Total Amount]`); `name` is the Spark column to land it in — they are separate because DAX names carry brackets that make poor Delta identifiers. `type` is one of `string` (default), `long`, `double`, `boolean`, `timestamp`, `date`.

  ```json
  "dax_columns": "[{\"dax\": \"Sales[Region]\", \"name\": \"region\", \"type\": \"string\"}, {\"dax\": \"[Total Amount]\", \"name\": \"total_amount\", \"type\": \"double\"}]"
  ```

- **Undeclared (fallback).** Omit `dax_columns` and the query's columns arrive in a `columns` `map<string,string>` keyed by their DAX names.

In both modes each row also carries `workspace_id`, `dataset_id`, `query_hash`, `row_index`, `truncated`, `ingestion_timestamp`, and `row_json` — the untouched JSON object for the row, which is where undeclared columns survive and where the original numeric/date types can be recovered with `from_json`.

`query_hash` is a fingerprint of the query text (whitespace-insensitive). Editing your query changes the hash, so revised results land as new rows rather than silently overwriting the old ones.

**Constraints you must plan for** (all imposed by the API, not the connector):

- **120 requests per minute, per user, tenant-wide** — not per model. This is roughly an order of magnitude tighter than the metadata endpoints. The connector spends exactly one request per micro-batch on this table and never parallelizes it, but if you configure many DAX tables in one pipeline you are budgeting against this shared ceiling yourself. `429`s are retried with the server's `Retry-After`.
- **Silent truncation** at 100,000 rows, 1,000,000 total values, or 15 MB of response — whichever comes first. The API reports this as a warning, not an error, so a truncated result looks exactly like a complete one. The connector sets `truncated = true` when a response lands on one of the countable ceilings; treat that as "narrow the query", not as a warning you can ignore. Aggregate in DAX rather than pulling raw fact tables.
- **`EVALUATE` only.** No MDX, no DMV/`INFO` functions.
- **Unsupported for models hosted in or live-connected to Azure Analysis Services**, and for RLS/SSO-enabled models under service-principal auth — use `impersonated_user_name` for the RLS case.
- Requires the **"Dataset Execute Queries REST API"** tenant setting (Integration settings) in addition to the service-principal settings the other tables need.

## Data Type Mapping

| Power BI / REST API Type | Example Fields | Spark Type | Notes |
|---|---|---|---|
| `string` | `name`, `description`, `status`, `refreshType` | `StringType` | |
| `string (uuid)` | `id`, `workspaceId`, `capacityId`, `requestId` | `StringType` | IDs are kept as strings, not a native UUID type. |
| `boolean` | `isReadOnly`, `isRefreshable`, `isHidden` | `BooleanType` | |
| `integer` | `attemptId`, `maxReadOnlyReplicas` | `LongType` | `LongType` is used for all numeric fields to avoid 32-bit overflow. |
| `string (date-time)` | `createdDate`, `startTime`, `endTime` | `TimestampType` | ISO 8601 UTC strings (e.g. `2017-06-13T09:25:43.153Z`), parsed to timestamps. |
| Nested JSON object | `queryScaleOutSettings`, `encryption` | `StructType` | Preserved as nested structs rather than flattened. Empty objects (`{}`) are normalized to `null`. |
| Array of objects | `upstreamDataflows`, `refreshAttempts`, `source` | `ArrayType(StructType)` | Preserved as nested collections. |
| Free-form / untyped blob | `refreshAttempts[].executionMetrics` | `StringType` | JSON-encoded by the connector, since the shape is not documented or stable. |
| Model-level type label | `dataset_columns.dataType` | `StringType` | Pass-through of the Tabular engine's own type name — see [Schema highlights](#schema-highlights). |
| DAX / M expression text | `dataset_measures.expression`, `dataset_tables.source[].expression` | `StringType` | Raw text. `null` unless the metadata-scanning tenant settings are enabled. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code. This places the connector code under a project path Lakeflow can load.

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` in the main pipeline file (e.g. `ingest.py`) to reference your Unity Catalog connection and the tables to ingest.

Minimal example — full tenant, all six tables:

```json
{
  "pipeline_spec": {
    "connection_name": "power_bi_connection",
    "object": [
      { "table": { "source_table": "workspaces" } },
      { "table": { "source_table": "datasets" } },
      { "table": { "source_table": "dataset_tables" } },
      { "table": { "source_table": "dataset_columns" } },
      { "table": { "source_table": "dataset_measures" } },
      { "table": { "source_table": "dataset_refresh_history" } }
    ]
  }
}
```

Scoped example — restrict to two workspaces and tune the fan-out:

```json
{
  "pipeline_spec": {
    "connection_name": "power_bi_connection",
    "object": [
      {
        "table": {
          "source_table": "workspaces",
          "table_configuration": {
            "workspace_filter": "state eq 'Active'",
            "page_size": "1000"
          }
        }
      },
      {
        "table": {
          "source_table": "datasets",
          "table_configuration": {
            "workspace_ids": "e380d1d0-1fa6-460b-9a90-1a5c6b02414c,5c968528-70b6-4588-809f-ce811ffa5c23"
          }
        }
      },
      {
        "table": {
          "source_table": "dataset_measures",
          "table_configuration": {
            "workspace_ids": "e380d1d0-1fa6-460b-9a90-1a5c6b02414c,5c968528-70b6-4588-809f-ce811ffa5c23",
            "scan_batch_size": "100",
            "scan_timeout_seconds": "900"
          }
        }
      },
      {
        "table": {
          "source_table": "dataset_refresh_history",
          "table_configuration": {
            "workspace_ids": "e380d1d0-1fa6-460b-9a90-1a5c6b02414c",
            "top": "100",
            "datasets_per_partition": "20"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection holding your `tenant_id` / `client_id` / `client_secret`.
- `source_table` must be one of the six supported names.
- Ingesting `dataset_tables`, `dataset_columns`, and `dataset_measures` together triggers a **separate scan per table**. If you need all three, consider whether scanning three times per run fits inside the Scanner API's 500 requests/hour budget — for most tenants it comfortably does.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g. a scheduled job or workflow).

The five snapshot tables fully re-read on every run. `dataset_refresh_history` appends only refresh runs whose `startTime` is newer than the stored cursor; on the very first run it backfills whatever the refreshes endpoint returns (bounded by `top`, default the 60 most recent entries per model).

#### Best Practices

- **Start small.** Begin with `workspaces` and `datasets` scoped to one or two `workspace_ids`. Confirm rows land, then add the scanner tables, then refresh history.
- **Scope with `workspace_ids` on large tenants.** This is the single most effective control on both runtime and API budget. The Admin list endpoints allow only **50 requests/hour**, which is tight.
- **Schedule conservatively.** Metadata changes slowly; daily or twice-daily is usually plenty for the five snapshot tables. Refresh history can run more frequently if you are monitoring refresh failures, but each run costs one refreshes call per semantic model.
- **Mind the rate limits.** Documented ceilings for the endpoints this connector uses:

  | Endpoint | Limit |
  |---|---|
  | `GET /admin/groups` | 50/hour or 15/minute per tenant |
  | `GET /admin/datasets` | 50/hour or 5/minute per tenant |
  | `POST /admin/workspaces/getInfo` | 500/hour; max 16 concurrent; max 100 workspace IDs per call |
  | `GET /admin/workspaces/scanStatus/{id}` | 10,000/hour |
  | `GET /admin/workspaces/scanResult/{id}` | 500/hour; result available for only 24 hours |
  | `GET /groups/{id}/datasets/{id}/refreshes` | Subject to general per-user throttling |

  The connector honors `Retry-After` on `429` and backs off exponentially on `5xx`, retrying up to 5 times. It also caches workspace and dataset enumerations for the lifetime of a run so the same list is not re-fetched per table.
- **Raise `scan_timeout_seconds` for large batches.** A scan of 100 busy workspaces can take several minutes; the 600s default is generous for typical batches but not unbounded.
- **Rotate the client secret before it expires.** Entra client secrets have a fixed lifetime; an expired secret fails every table at the token step.

#### Troubleshooting

**`dataset_tables` / `dataset_columns` / `dataset_measures` ingest zero rows, but the pipeline succeeds**

The most common issue by far. The metadata-scanning tenant settings are not enabled, so the scan succeeds but returns no schema data. Have a tenant admin enable both **"Enhance admin APIs responses with detailed metadata"** and **"Enhance admin APIs responses with DAX and mashup expressions"** (Admin Portal → Tenant settings → Admin API settings), then wait ~15 minutes and re-run. See [Tenant Setup](#tenant-setup-required-admin-actions) step 3.

**`dataset_measures.expression` and `dataset_tables.source` are `null` while other columns populate**

Same root cause, narrower: **"Enhance admin APIs responses with DAX and mashup expressions"** specifically controls the expression text.

**`Entra ID token request failed with status 400 / 401`**

The token exchange itself failed, before any Power BI call. Verify `tenant_id`, `client_id`, and `client_secret` — most often the secret has expired, or the secret **ID** was copied instead of the secret **Value**.

**`... was rejected with 401` / `403`, message mentioning "Allow service principals to use Power BI APIs"**

The service principal is not enabled for the Power BI APIs, or not for the `/admin` family. Check that:
- **"Allow service principals to use Power BI APIs"** is enabled and the security group containing the service principal is in its scope.
- **"Allow service principals to use read-only admin APIs"** is enabled for the `/admin` endpoints.
- The Entra app registration has **no** Power BI API permissions configured in the Azure Portal API permissions blade — these break service-principal auth rather than help it.

Note that `workspaces` and `datasets` fall back automatically to membership-scoped endpoints on `401`/`403`, so a partial failure where those two tables return a suspiciously small number of rows while the scanner tables error outright points at exactly this problem.

**`dataset_refresh_history` fails with `403` while other tables succeed**

Refresh history always uses the workspace-scoped endpoint. Add the service principal as a **Member** or **Admin** of the affected workspaces — Admin API access does not cover this endpoint. Read-only (Viewer) access is not sufficient for the full response.

**`datasets` rows only have `id` and `name` populated**

You are on the non-admin fallback path with read-only access. Either enable Admin API access (preferred) or grant the service principal write access on the workspaces.

**`Metadata scan {id} did not complete within {n}s`**

The scan is asynchronous and large batches take longer. Raise `scan_timeout_seconds`, or lower `scan_batch_size` to shrink each scan.

**`Metadata scan {id} failed`**

The Power BI service reported a scan failure. Retry; if it persists, reduce `scan_batch_size` to isolate a problematic workspace.

**Refresh rows stuck at `status = "Unknown"`**

Expected, and not fixable by re-running. See [Limitations](#limitations).

**`429 Too Many Requests` in logs**

The connector retries these automatically. Persistent throttling means the schedule is too aggressive for the tenant's 50/hour Admin list budget — reduce frequency or scope with `workspace_ids`.

## Limitations

**Row-level data is opt-in and hand-written, not discovered.** Six of the seven tables are metadata. Row data is available only through `dax_query_result`, and only for a DAX query you write yourself against one model you name explicitly — the connector never derives a query from a model's metadata, so there is no "ingest every table in every model" mode. The reasons are in [Reading row data with `dax_query_result`](#reading-row-data-with-dax_query_result): a 120 request/minute tenant-wide budget, silent truncation at 100,000 rows / 1,000,000 values / 15 MB, `EVALUATE`-only support, no support for Azure Analysis Services-hosted or RLS/SSO models under service-principal auth, and a separate tenant setting.

**A DAX result's schema cannot be validated ahead of time.** Column names and types come from your query. Declare them with `dax_columns` to get typed Spark columns; otherwise they land in a `columns` `map<string,string>`. A name in `dax_columns` that the query never returns produces a null column rather than an error, so mistyped DAX column names fail quietly — check `row_json` on a sample row to see the keys the API actually returned.

**No `dataset_relationships` table.** Relationships between tables inside a semantic model are present in the scanner response (`datasets[].relationships[]`), but Microsoft Learn documents the field only as `relationships: []` — unlike `Table`, `Column`, and `Measure`, it publishes no object definition with field names. Rather than ship a guessed schema, the table is deferred until the shape can be confirmed against a live tenant scan.

**No `reports`, `dashboards`, or `dataflows` tables.** These are separate Power BI artifact types with their own Admin endpoints (`GET /admin/reports`, `GET /admin/dashboards`, `GET /admin/dataflows`) and materially different schemas — they are not part of a semantic model. Deferred to keep this connector focused. Also deferred for the same reason: `datasources` / gateway bindings, `activity_events` (the audit log, which uses a completely different time-windowed, continuation-token API with its own 200/hour limit), `dataset_users` / `workspace_users` access-control lists, and `datamarts`.

**Refresh-history terminal status is not backfilled.** A refresh run can be read once with `status: "Unknown"` (in progress) and only reach `Completed` or `Failed` later, against the same `requestId`. Because `dataset_refresh_history` is append-only and filtered on `startTime`, a run already ingested as `Unknown` will not be re-read, so its terminal status is never captured for that row. Mitigations: schedule the pipeline less frequently than the typical refresh duration so runs have finished before they are first read, or deduplicate downstream on `(dataset_id, requestId)` keeping the latest non-`Unknown` row.

**No incremental reads for metadata.** Five of six tables re-read fully on each run. This is an API constraint, not a connector choice — Power BI exposes no `lastModifiedDateTime`, delta link, or change feed on any of these endpoints.

**No delete detection.** Deleted workspaces and models disappear from later snapshots but are not tombstoned. Diff successive snapshots downstream if you need this.

**OneDrive-triggered refreshes are excluded** from the refreshes endpoint response by Power BI itself, so they never appear in `dataset_refresh_history`.

**Scan results expire after 24 hours.** Not usually visible to users, since the connector polls and reads each scan within a single run, but it means scan results cannot be re-read from a prior run.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/power_bi_semantic_model/power_bi_semantic_model.py`
- Schemas and tuning constants: `src/databricks/labs/community_connector/sources/power_bi_semantic_model/power_bi_semantic_model_schemas.py`
- Connector API research doc: `src/databricks/labs/community_connector/sources/power_bi_semantic_model/power_bi_semantic_model_api_doc.md`
- Official Power BI REST API documentation:
  - https://learn.microsoft.com/en-us/rest/api/power-bi/
  - https://learn.microsoft.com/en-us/rest/api/power-bi/admin/groups-get-groups-as-admin
  - https://learn.microsoft.com/en-us/rest/api/power-bi/admin/datasets-get-datasets-as-admin
  - https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-post-workspace-info
  - https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-status
  - https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result
  - https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group
  - https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries-in-group
- Service principal setup and tenant settings:
  - https://learn.microsoft.com/en-us/power-bi/enterprise/read-only-apis-service-principal-authentication
  - https://learn.microsoft.com/en-us/fabric/governance/metadata-scanning-setup
  - https://learn.microsoft.com/power-bi/connect-data/service-datasets-understand
