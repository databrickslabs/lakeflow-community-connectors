# Lakeflow Linear Community Connector

This documentation describes how to configure and use the **Linear** Lakeflow community connector to ingest data from the [Linear GraphQL API](https://linear.app/developers/graphql) into Databricks.

The connector reads `issues` and `projects` from a Linear workspace using incremental CDC (change data capture) over the `updatedAt` timestamp.

## Prerequisites

- **Linear account**: You need a Linear account with access to the workspace whose issues and projects you want to read. The connector can only read data that the credential's owner can see in the Linear application.
- **Authentication credential** — one of:
  - **Personal API key** (preferred for data pipelines): created in Linear under **Settings → Security & access**. The key inherits the permissions of the user who created it.
  - **OAuth 2.0 access token** (for multi-tenant / user-delegated access): a short-lived access token obtained from Linear's OAuth flow.
- **Network access**: The environment running the connector must be able to reach `https://api.linear.app`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. Supply **exactly one** authentication credential — either `api_key` (preferred) or `access_token`.

| Name      | Type   | Required | Description | Example |
|-----------|--------|----------|-------------|---------|
| `api_key` | string | conditional | Linear **personal API key**. Sent verbatim in the `Authorization` header (no `Bearer` prefix). Preferred for server-side / pipeline use. Required if `access_token` is not supplied. | `lin_api_xxx...` |
| `access_token` | string | conditional | Linear **OAuth 2.0 access token**. Sent as `Bearer <token>`. Use for multi-tenant or user-delegated access. Required if `api_key` is not supplied. | `lin_oauth_xxx...` |

> **Note**: The connector raises an error at initialization if neither `api_key` nor `access_token` is provided. Personal API keys are sent raw (no prefix); OAuth access tokens are automatically prefixed with `Bearer`. OAuth access tokens expire (~24 hours), so the personal API key is recommended for unattended pipelines.

#### `externalOptionsAllowList`

This connector supports table-specific options (see [Table Configurations](#table-configurations)). To allow these options to be passed through per table, set the `externalOptionsAllowList` connection option to the full list below:

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to be passed through to the connector. |

The full, definitive list of supported table-specific options is:

```
max_records_per_batch,page_size
```

> Table-specific options (`max_records_per_batch`, `page_size`) are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline spec, and their names must be listed in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

#### Personal API Key (preferred)

1. Log in to Linear and click your workspace name in the sidebar.
2. Navigate to **Settings → Security & access**.
3. Under **Personal API keys**, click **Create key**, give it a label, and click **Create**.
4. **Copy the key immediately** — Linear displays it only once.
5. Use this value as the `api_key` connection option.

The default `read` scope is sufficient for `issues` and `projects`. (If you later extend the connector to read customer-related data, the key owner also needs `customer:read` access.)

#### OAuth 2.0 Access Token (alternative)

For multi-tenant or user-delegated access, register an OAuth application in Linear and run the Authorization Code flow:

1. Direct the user to `https://linear.app/oauth/authorize` with your `client_id`, `redirect_uri`, `response_type=code`, and `scope=read`.
2. Exchange the returned authorization code for tokens at `https://api.linear.app/oauth/token`.
3. Supply the resulting access token as the `access_token` connection option. Because access tokens expire in ~24 hours, you will need to refresh them out-of-band before they lapse.

See the [Linear OAuth 2.0 documentation](https://linear.app/developers/oauth-2-0-authentication) for the full flow.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one. Provide either `api_key` or `access_token`.
3. Set `externalOptionsAllowList` to `max_records_per_batch,page_size` so the connection allows the connector's table-specific options.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Linear connector exposes a **static list** of two tables (use the exact lowercase names):

- `issues`
- `projects`

### Object summary, primary keys, and ingestion mode

| Table      | Description                                              | Ingestion Type | Primary Key | Incremental Cursor |
|------------|----------------------------------------------------------|----------------|-------------|--------------------|
| `issues`   | Issues and tasks across every team in the workspace, including archived issues. | `cdc`          | `id` (UUID) | `updatedAt`        |
| `projects` | Projects across all teams in the workspace, including archived projects. | `cdc`          | `id` (UUID) | `updatedAt`        |

### Incremental sync behavior

Both tables use **CDC (upsert-only) incremental ingestion** keyed on the `id` primary key with `updatedAt` as the cursor:

- Each read fetches records ordered ascending by `updatedAt`, filtered to the range `updatedAt >= last_cursor` (inclusive) and capped at the connector's start time. Results are paginated using Linear's Relay-style cursor (`first` / `after`) until the page-size or `max_records_per_batch` limit is reached, or the source reports no further pages.
- The checkpointed offset is the maximum `updatedAt` observed. The next run resumes from that watermark (inclusive), and CDC upsert on `id` dedupes any boundary overlap, so records are never lost or double-counted at the seam.
- On the **first run** (no stored offset), the connector backfills all matching records from the beginning of the workspace's history up to the run's start time. Archived records are included (`includeArchived: true`).
- Because Linear caps incremental cursors at the connector's initialization time, a single trigger only drains data that existed when it started; records updated afterward are picked up by the following trigger.

> **Delete handling**: Linear does **not** expose a soft-delete flag or a deleted-records feed for `issues` or `projects` — deleted records simply disappear from the API. The connector therefore uses upsert-only CDC (`cdc`, not `cdc_with_deletes`); hard deletes are **not** propagated to the destination. If you need to detect deletions, perform a periodic full-snapshot reconciliation downstream.

### Schema highlights

Full schemas are defined by the connector (see `linear_schemas.py`) and mirror the raw GraphQL response shape. Notable columns:

- **Nested foreign keys** are kept as structs with a single `id` field rather than being flattened. For example `team`, `state`, `assignee`, `creator`, `parent`, `project`, `projectMilestone`, and `cycle` on `issues` are each `struct<id: string>`. On `projects`: `status`, `lead`, `creator`, `convertedFromIssue`, and `lastUpdate`.
- **`updatedAt`** is the incremental cursor and is always populated (non-nullable) for existing records.
- **`priority`** (both tables) is an integer enum: `0` = No priority, `1` = Urgent, `2` = High, `3` = Medium, `4` = Low. `priorityLabel` provides the human-readable label.
- **`identifier`** (issues) is the human-readable key such as `ENG-123`; `id` is the stable UUID primary key.
- **`reactionData`** (issues) is a JSON blob stored as a serialized string.
- **`labelIds`** is an array of label UUID strings (a denormalized convenience copy).
- **History arrays** on `projects` (`completedScopeHistory`, `scopeHistory`, `issueCountHistory`, etc.) are ordered arrays with one entry per week since project inception.
- **`progress`** (projects) is a float from `0.0` to `1.0` representing percent complete.
- The `state` string field on `projects` is **deprecated** by Linear in favor of `status` (FK to ProjectStatus); both are exposed for backward compatibility.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (`issues` or `projects`). |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default). |
| `destination_schema` | No | Target schema (defaults to pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Both `issues` and `projects` are CDC tables, so both SCD types are applicable. |
| `primary_keys` | No | List of columns to override the connector's default primary keys (defaults to `id`). |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking (use `updatedAt`). |

### Source-specific `table_configuration` options

Both `issues` and `projects` accept the following optional options. Their names must appear in the connection's `externalOptionsAllowList`.

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `page_size` | integer | No | `50` | Number of records requested per GraphQL page (Relay `first` argument). Clamped to the range `1`–`250`. Keep this moderate for nested queries to stay within Linear's per-request complexity limit. |
| `max_records_per_batch` | integer | No | unset (no cap) | Caps the number of records returned per `read_table` call. Records are strictly truncated and the cursor resumes from the last-seen watermark on the next batch. Useful for smoothing API usage across triggers. |

## Data Type Mapping

Linear GraphQL types are mapped to Spark types as follows:

| Linear / GraphQL Type | Example Fields | Spark Type | Notes |
|---|---|---|---|
| `ID` / `UUID` | `id`, `labelIds` elements | `StringType` | UUID strings. |
| `String` | `title`, `description`, `name`, `health` | `StringType` | Plain text or Markdown content. |
| `Int` / Enum int | `number`, `priority`, `customerTicketCount` | `LongType` | All integers use `LongType` to avoid overflow, including enum integers like `priority`. |
| `Float` | `estimate`, `progress`, `scope`, sort orders | `DoubleType` | |
| `Boolean` | `trashed`, `slackNewIssue`, `inheritsSharedAccess` | `BooleanType` | |
| `DateTime` | `createdAt`, `updatedAt`, `completedAt`, `archivedAt` | `TimestampType` | ISO 8601 with milliseconds (UTC). |
| `Date` | `dueDate`, `targetDate`, `startDate` | `DateType` | Calendar date, no time component. |
| `JSON` | `reactionData` | `StringType` | Serialized JSON blob. |
| `[UUID]` | `labelIds`, `previousIdentifiers` | `ArrayType(StringType)` | Array of strings. |
| `[Float]` | `completedScopeHistory`, `scopeHistory`, `inProgressScopeHistory` | `ArrayType(DoubleType)` | Weekly history arrays. |
| `[Int]` | `issueCountHistory`, `completedIssueCountHistory` | `ArrayType(LongType)` | Weekly history arrays. |
| Nested object reference | `team`, `state`, `assignee`, `status`, `lead` | `StructType` with `id` field | FK references kept as `struct<id: string>` rather than flattened. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Linear connector source in your workspace. This will place the connector code (`linear.py` and its schemas) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline entrypoint, configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Linear connector (configured with `api_key` or `access_token`, plus `externalOptionsAllowList`).
- One or more **tables** to ingest, each with optional `table_configuration`.

Example `pipeline_spec` ingesting both supported tables:

```json
{
  "pipeline_spec": {
    "connection_name": "linear_connection",
    "object": [
      {
        "table": {
          "source_table": "issues",
          "table_configuration": {
            "page_size": "50",
            "max_records_per_batch": "5000"
          }
        }
      },
      {
        "table": {
          "source_table": "projects",
          "table_configuration": {
            "page_size": "50"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Linear credential.
- For each `table`, `source_table` must be `issues` or `projects`.
- The `page_size` and `max_records_per_batch` options are optional; omit them to use the connector defaults.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job). For these CDC tables:

- On the **first run**, the connector backfills all matching records (including archived) up to the run's start time. This can be heavy for large workspaces — consider a lower `page_size` and a `max_records_per_batch` cap to spread the initial load.
- On **subsequent runs**, the connector resumes from the stored `updatedAt` watermark and upserts changed records on the `id` primary key.

#### Best Practices

- **Start small**: Begin by syncing a single table (e.g. `projects`) to validate configuration and data shape before adding `issues`.
- **Use incremental sync**: Both tables are CDC by default — let the connector track `updatedAt` to minimize API usage instead of re-reading full history.
- **Respect Linear's rate limits**: Linear enforces a strict **30 requests/minute per-endpoint burst limit**, plus hourly request (≈2,500–5,000 req/hr) and query-complexity budgets. The connector is intentionally single-driver (sequential reads) for this reason — avoid running many concurrent Linear pipelines against the same workspace. The connector automatically retries on HTTP 429/5xx and GraphQL `RATELIMITED` errors with exponential backoff.
- **Tune `page_size`**: Keep `page_size` moderate (the default `50` is a safe choice) so each query stays under Linear's per-request complexity ceiling. Very large pages on the heavily nested `issues` query can trip the complexity limit.
- **Set appropriate schedules**: Balance data freshness with API usage; CDC means each run only fetches what changed since the last watermark.

#### Troubleshooting

**Common issues:**

- **Authentication errors**: If initialization fails with a missing-credential error, confirm that the connection has either `api_key` or `access_token` set. If requests are rejected by Linear, verify the credential is valid and not revoked. Remember that personal API keys are sent **without** a `Bearer` prefix while OAuth tokens use `Bearer` — the connector handles this automatically based on which option you supplied.
- **Expired OAuth token**: OAuth access tokens expire (~24 hours). If you authenticate with `access_token`, refresh it before it lapses, or switch to a personal API key for unattended pipelines.
- **Rate limiting (`RATELIMITED` / HTTP 429)**: If you see repeated rate-limit retries or failures, reduce `page_size`, widen the schedule interval, and avoid running multiple Linear pipelines concurrently. Linear's 30 req/min per-endpoint burst limit is the most common bottleneck.
- **Missing deletes**: Deleted issues/projects are not removed from the destination (Linear exposes no delete feed). Run periodic full-snapshot reconciliation downstream if delete detection is required.
- **`table not supported` error**: Confirm `source_table` is exactly `issues` or `projects` (lowercase).
- **Rejected table option**: If a table option appears to be ignored or rejected, ensure its name is listed in the connection's `externalOptionsAllowList` (`max_records_per_batch,page_size`).

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/linear/linear.py`
- Connector schemas and GraphQL query builder: `src/databricks/labs/community_connector/sources/linear/linear_schemas.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/linear/linear_api_doc.md`
- Official Linear developer documentation:
  - GraphQL API: `https://linear.app/developers/graphql`
  - Pagination: `https://linear.app/developers/pagination`
  - Filtering: `https://linear.app/developers/filtering`
  - Rate limiting: `https://linear.app/developers/rate-limiting`
  - OAuth 2.0 authentication: `https://linear.app/developers/oauth-2-0-authentication`
