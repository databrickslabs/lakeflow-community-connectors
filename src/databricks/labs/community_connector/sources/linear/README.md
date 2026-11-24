# Lakeflow Linear Community Connector

This documentation describes how to configure and use the **Linear** Lakeflow community connector to ingest data from the [Linear GraphQL API](https://linear.app/developers/graphql) into Databricks.

Linear is a single-endpoint GraphQL API. The connector reads the `issues` and `projects` objects incrementally using the `updatedAt` cursor, paginating with Relay-style cursors and always passing `includeArchived: true` so archived records are captured.

## Prerequisites

- **Linear account**: A Linear workspace account with access to the issues and projects you want to read. The data returned is scoped to the permissions of the authenticating user.
- **Credentials** (one of the following):
  - **Personal API Key** (recommended): A long-lived, user-scoped key beginning with `lin_api_`. Inherits the permissions of the user who created it.
  - **OAuth 2.0 application**: `client_id`, `client_secret`, and a `refresh_token` for an OAuth app, with at least the `read` scope. The connector exchanges the refresh token for a short-lived access token at runtime.
- **Network access**: The environment running the connector must be able to reach `https://api.linear.app`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. You must supply **either** a Personal API Key **or** the full OAuth trio.

| Name             | Type   | Required | Description                                                                                                   | Example                  |
|------------------|--------|----------|---------------------------------------------------------------------------------------------------------------|--------------------------|
| `api_key`        | string | Conditional | Personal API Key used for authentication. Sent verbatim (no `Bearer` prefix). Required unless the OAuth trio is supplied. `personal_api_key` is accepted as an alias. | `lin_api_xxx...`         |
| `client_id`      | string | Conditional | OAuth application client ID. Required (with `client_secret` and `refresh_token`) when `api_key` is not supplied. | `abc123...`              |
| `client_secret`  | string | Conditional | OAuth application client secret. Required alongside `client_id` and `refresh_token`.                          | `def456...`              |
| `refresh_token`  | string | Conditional | OAuth refresh token exchanged at runtime for a short-lived access token. Required alongside `client_id` and `client_secret`. | `ghi789...`              |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector supports table-specific options, so this parameter must be set. | `page_size,max_records_per_batch,lookback_seconds` |

The full, definitive list of supported table-specific options for `externalOptionsAllowList` is:

`page_size,max_records_per_batch,lookback_seconds`

> **Note**: Table-specific options such as `page_size`, `max_records_per_batch`, and `lookback_seconds` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

#### Personal API Key (recommended)

1. Log in to Linear and open **Settings** (click your workspace name in the sidebar).
2. Select **Security & access**.
3. Scroll to **Personal API keys** and click **Create key**.
4. Copy the key immediately — Linear shows it only once. Keys begin with `lin_api_`.
5. Use this value as the `api_key` connection option.

You can verify the key with:

```bash
curl -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: lin_api_XXXXXXXXXXXXX" \
  --data '{"query": "{ viewer { id name email } }"}'
```

#### OAuth 2.0 (alternative)

For applications acting on behalf of users, create an OAuth application in Linear and obtain its `client_id` and `client_secret`. Complete the OAuth authorization flow (requesting at least the `read` scope) to obtain a `refresh_token`. Supply all three values to the connector; it performs the refresh-token exchange against `https://api.linear.app/oauth/token` automatically.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one, supplying either `api_key` or the OAuth trio.
3. Set `externalOptionsAllowList` to `page_size,max_records_per_batch,lookback_seconds` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Linear connector exposes a **static list** of tables (use the exact lowercase names below):

- `issues`
- `projects`

### Object summary, primary keys, and ingestion mode

| Table      | Description                                                          | Ingestion Type | Primary Key       | Incremental Cursor |
|------------|---------------------------------------------------------------------|----------------|-------------------|--------------------|
| `issues`   | Work items (bugs, features, tasks) assigned to teams                | `cdc`          | `id` (UUID)       | `updatedAt`        |
| `projects` | High-level initiatives grouping related issues with goals/timelines | `cdc`          | `id` (UUID)       | `updatedAt`        |

Both tables are read incrementally: each run filters `updatedAt { gte: <cursor> }`, orders ascending by `updatedAt`, and checkpoints the maximum `updatedAt` seen.

### Delete handling

Linear does **not** expose a hard-delete event stream, so the connector uses `cdc` (no delete synchronization). To capture lifecycle state, the connector always passes `includeArchived: true` and surfaces soft-state fields:

- **`trashed`** (issues): `true` for soft-deleted issues. Trashed issues remain retrievable for ~30 days, after which permanent deletion removes them from the API entirely (and they cannot be detected).
- **`archivedAt`** (issues and projects): non-null timestamp for archived records; `null` otherwise.

### Schema highlights

The full schemas are static and defined by the connector. Notable columns:

- **`issues`**: `id` (primary key), `identifier` (human-readable, e.g. `ENG-123`), `priority` (`0`=None, `1`=Urgent, `2`=High, `3`=Normal, `4`=Low) with `priorityLabel`, lifecycle timestamps (`createdAt`, `updatedAt`, `archivedAt`, `startedAt`, `completedAt`, `canceledAt`, `autoArchivedAt`, `autoClosedAt`), and nested structs `state`, `assignee`, `team`, `project`, `parent`, `creator`, `cycle`, plus the `labels` array.
- **`projects`**: `id` (primary key), `name`, `slugId`, `state` (`planned`/`started`/`paused`/`completed`/`cancelled`), `progress` (0.0–1.0), `scope`, history arrays `issueCountHistory` / `scopeHistory`, plus nested `lead`, `creator`, and the `members`, `teams`, and `projectMilestones` arrays.

Connection-type fields (`labels`, `members`, `teams`, `projectMilestones`) are returned by the GraphQL API wrapped in a `{ "nodes": [...] }` envelope; the connector flattens them to plain arrays of structs.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (`issues` or `projects`) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Both `issues` and `projects` use CDC ingestion, so both support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking (use `updatedAt`) |

### Source-specific `table_configuration` options

These options apply to both `issues` and `projects` and are optional. They must be listed in the connection's `externalOptionsAllowList`.

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `page_size` | integer | No | `50` | Relay page size (`first`) per GraphQL request. Capped at `250`. Linear recommends `50` to keep per-query complexity low; reduce if you hit complexity errors. |
| `max_records_per_batch` | integer | No | `200` | Caps the number of records returned per `read_table` call. Records are sorted ascending by `updatedAt` and truncated; the next run resumes from the checkpointed cursor. |
| `lookback_seconds` | integer | No | `300` | Window subtracted from the stored cursor at query time (only on the first read of a table per run) to safely re-capture records updated around the previous high-water mark (clock skew / late writes). Set to `0` to disable. |

> **Note**: There is no `start_date` option. On the first run (no stored offset), the connector reads all available records; subsequent runs resume from the checkpointed `updatedAt` cursor.

## Data Type Mapping

Linear GraphQL types are mapped to Spark types as follows:

| Linear Type | Spark Type | Example Fields | Notes |
|---|---|---|---|
| `String` | `StringType` | `id`, `identifier`, `title`, `url` | UUIDs, identifiers, and text. |
| `Int` | `LongType` | `priority` | Stored as 64-bit integers. |
| `Float` | `DoubleType` | `progress`, `scope`, `estimate` | |
| `Boolean` | `BooleanType` | `trashed` | |
| `DateTime` (ISO 8601 UTC) | `TimestampType` | `createdAt`, `updatedAt`, `archivedAt`, `completedAt` | Always UTC (`...Z`). Nullable lifecycle timestamps are `null` when not applicable. |
| `TimelessDate` (YYYY-MM-DD) | `DateType` | `dueDate`, `startDate`, `targetDate` | Date only, no time component. |
| Nested object | `StructType` | `state`, `assignee`, `team`, `lead`, `creator` | Preserved as nested structs (e.g. `{ id, name, email }`). |
| Connection (`{ nodes: [...] }`) | `ArrayType(StructType)` | `labels`, `members`, `teams`, `projectMilestones` | Flattened from the GraphQL envelope into arrays of structs. |
| Array | `ArrayType(...)` | `issueCountHistory`, `scopeHistory` | Numeric history arrays. |

**Key behaviors:**

- `priority = 0` means *no priority assigned* (not the highest urgency).
- Nullable timestamps (`archivedAt`, `completedAt`, `canceledAt`, `startedAt`) are `null` for records not in the corresponding state.
- `identifier` (e.g. `ENG-123`) is human-readable, but the stable primary key is `id` (UUID).

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Linear connector source in your workspace. This will place the connector code (`linear.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g. `ingest.py`), configure a `pipeline_spec` that references a Unity Catalog connection using this connector and one or more tables to ingest.

Example `pipeline_spec`:

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
            "lookback_seconds": "300"
          }
        }
      },
      {
        "table": {
          "source_table": "projects",
          "table_configuration": {
            "page_size": "50",
            "max_records_per_batch": "200"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Linear `api_key` (or OAuth trio) and the required `externalOptionsAllowList`.
- For each `table`, `source_table` must be `issues` or `projects`.
- Table options such as `page_size`, `max_records_per_batch`, and `lookback_seconds` go under `table_configuration` (and must appear in `externalOptionsAllowList`).

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g. a scheduled job or workflow).

- On the **first run**, the connector backfills all available `issues` / `projects` (no `start_date` is supported).
- On **subsequent runs**, it resumes from the stored `updatedAt` cursor, applying `lookback_seconds` to safely re-capture late updates. Each run only reads up to the snapshot time at which it started, so newly arriving data is picked up by the next run.

#### Best Practices

- **Start small**: Begin with a single table (e.g. `issues`) to validate configuration and data shape before adding `projects`.
- **Use incremental sync**: Both tables are `cdc` with `updatedAt`; rely on the stored cursor to minimize API calls.
- **Respect rate limits**: Linear allows 2,500 requests/hour (Personal API Key) or 5,000 requests/hour (OAuth app) per user, plus a complexity budget (3M / 2M points per hour) and a 10,000-point hard cap per query. Keep `page_size` at `50` and stagger schedules to stay within limits.
- **Tune batch limits**: Use `max_records_per_batch` to bound the number of records per microbatch for large backfills.

#### Troubleshooting

**Common issues:**

- **Authentication failures (`401`)**:
  - For Personal API Keys, ensure the key is passed as `api_key` and is current — it is sent verbatim with **no** `Bearer` prefix.
  - For OAuth, verify `client_id`, `client_secret`, and `refresh_token` are correct and the app has the `read` scope. A failed token exchange surfaces as an OAuth error.
- **Rate limiting (`429`)**:
  - The connector retries `429`/`5xx` responses with exponential backoff (up to 5 attempts). Persistent `429`s mean you are exceeding the hourly request or complexity budget — widen schedule intervals or reduce concurrency.
- **GraphQL complexity errors**:
  - Reduce `page_size` (e.g. to `25`) if individual queries exceed the 10,000-point per-query complexity cap.
- **Missing recent updates**:
  - Increase `lookback_seconds` if records updated around a sync boundary are occasionally missed due to clock skew.
- **Deleted records still present / missing**:
  - Trashed issues (`trashed: true`) remain for ~30 days then disappear permanently; hard deletes are not detectable. Archived records (`archivedAt` non-null) are always included.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/linear/linear.py`
- Connector schemas and constants: `src/databricks/labs/community_connector/sources/linear/linear_schemas.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/linear/linear_api_doc.md`
- Official Linear API documentation:
  - `https://linear.app/developers/graphql`
  - `https://linear.app/developers/pagination`
  - `https://linear.app/developers/rate-limiting`
