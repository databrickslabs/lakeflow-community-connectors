# Lakeflow Notion Community Connector

This documentation describes how to configure and use the **Notion** Lakeflow community connector to ingest data from the Notion REST API into Databricks.


## Prerequisites

- **Notion workspace**: You need access to a Notion workspace where you have permission to create integrations.
- **Internal Integration Token**:
  - Must be created at [https://www.notion.so/my-integrations](https://www.notion.so/my-integrations) and supplied as the `api_key` connection option.
  - The integration must have the following capabilities enabled:
    - **Read content** -- required for databases, pages, and blocks.
    - **Read user information** -- required for the `users` table.
    - **Read comments** -- required for the `comments` table.
  - All Notion pages and databases that the connector should access must be explicitly shared with the integration. Pages are not automatically accessible.
- **Network access**: The environment running the connector must be able to reach `https://api.notion.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_key` | string | yes | Notion Internal Integration Token for API authentication. | `ntn_xxx...` |
| `base_url` | string | no | Base URL for the Notion API. Override to use an alternative endpoint. Defaults to `https://api.notion.com`. | `https://api.notion.com` |
| `notion_version` | string | no | Notion API version in `YYYY-MM-DD` format. Defaults to `2026-03-11`. | `2026-03-11` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector supports table-specific options, so this parameter must be set. | `max_records_per_batch,max_pages_per_batch` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`max_records_per_batch,max_pages_per_batch`

> **Note**: Table-specific options such as `max_records_per_batch` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

1. Sign in to [Notion](https://www.notion.so/) and navigate to [https://www.notion.so/my-integrations](https://www.notion.so/my-integrations).
2. Click **New integration**.
3. Set a name and select the target workspace.
4. Under **Capabilities**, enable the required permissions:
   - **Read content** -- required for all read operations.
   - **Read user information** -- required for the `users` table.
   - **Read comments** -- required for the `comments` table.
5. Copy the **Internal Integration Token** from the **Secrets** section. Use this as the `api_key` connection option.
6. Share each Notion page and database you want to ingest with the integration. Open the page or database in Notion, click the three-dot menu, select **Connect to**, and choose your integration.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `max_records_per_batch,max_pages_per_batch` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Notion connector exposes a **static list** of tables:

- `databases`
- `pages`
- `blocks`
- `users`
- `comments`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|-------|-------------|----------------|-------------|---------------------|
| `databases` | Notion databases (metadata and property schema) | `cdc` | `id` | `last_edited_time` |
| `pages` | Pages and database items | `cdc` | `id` | `last_edited_time` |
| `blocks` | Content blocks within pages (fetched recursively up to 30 levels deep) | `cdc` | `id` | `last_edited_time` |
| `users` | Workspace members (people and bots) | `snapshot` | `id` | n/a |
| `comments` | Comments on pages and blocks | `cdc` | `id` | `created_time` |

### Incremental sync details

- **databases** and **pages**: Discovered via the Notion Search API (`POST /v1/search`) sorted by `last_edited_time` ascending. The connector applies a 60-second lookback window on each incremental read to account for Notion search indexing delays.
- **blocks**: The connector finds recently modified pages via Search, then recursively fetches all child blocks for those pages. Only blocks with `last_edited_time` newer than the stored cursor are emitted.
- **comments**: The connector finds recently modified pages via Search, then polls each page for comments. Only comments with `created_time` newer than the stored cursor are emitted.
- **users**: Full refresh on every run. All workspace users are fetched from `GET /v1/users`.

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
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

These options control how the connector reads data from the Notion API. All are optional.

| Option | Applies To | Default | Description |
|--------|-----------|---------|-------------|
| `max_records_per_batch` | `databases`, `pages`, `blocks`, `comments` | `200` | Maximum number of records to return per `read_table` call. Useful for controlling batch size and API usage. |
| `max_pages_per_batch` | `blocks`, `comments` | `5` (blocks), `10` (comments) | Maximum number of modified pages to scan per batch when fetching blocks or comments. Controls how many pages are traversed for child content per pipeline trigger. |

### Schema highlights

- **databases**: Includes `title` and `description` extracted as plain text from Notion rich text arrays. The `properties` field contains the full database property schema as a JSON string. `icon` and `cover` are serialized as JSON strings.
- **pages**: Similar structure to databases. The `properties` field contains property values as a JSON string. Page body content is not included -- use the `blocks` table for content.
- **blocks**: Each block includes a `type` field (e.g., `paragraph`, `heading_1`, `to_do`) and a `content` field containing the type-specific payload as a JSON string. The `page_id` field links each block back to its parent page.
- **users**: Includes `type` (`person` or `bot`), `name`, `avatar_url`, and nested `person` (with `email`) or `bot` details. The `bot` field is serialized as a JSON string.
- **comments**: The `rich_text` field contains the comment body as a JSON string (Notion rich text array). The `page_id` field links each comment to its parent page.

## Data Type Mapping

Notion JSON fields are mapped to Spark types as follows:

| Notion JSON Type | Example Fields | Spark Type | Notes |
|------------------|---------------|------------|-------|
| string (UUID) | `id`, `discussion_id` | `StringType` | All identifiers are stored as strings. |
| string | `title`, `description`, `url`, `type` | `StringType` | Plain string fields. |
| boolean | `in_trash`, `is_inline`, `has_children`, `is_archived`, `is_locked` | `BooleanType` | Standard true/false values. |
| ISO 8601 datetime | `created_time`, `last_edited_time` | `TimestampType` | Parsed as Spark timestamps. |
| Partial User object | `created_by`, `last_edited_by` | `StructType(object, id)` | Nested struct with `object` and `id` fields. |
| Parent object | `parent` | `StructType(type, page_id, database_id, block_id, workspace, data_source_id)` | Nested struct representing the parent container. |
| Person object | `person` (in users table) | `StructType(email)` | Nested struct with email field. |
| Complex JSON | `properties`, `icon`, `cover`, `content`, `rich_text`, `bot` | `StringType` | Serialized as JSON strings to preserve full fidelity. Parse downstream as needed. |
| nullable fields | `public_url`, `cover`, `icon`, `bot` | Same as base type + `null` | Missing fields are surfaced as `null`. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`). Below is an example that ingests all five Notion tables:

```json
{
  "pipeline_spec": {
    "connection_name": "notion_connection",
    "object": [
      {
        "table": {
          "source_table": "databases"
        }
      },
      {
        "table": {
          "source_table": "pages"
        }
      },
      {
        "table": {
          "source_table": "blocks",
          "table_configuration": {
            "max_records_per_batch": "200",
            "max_pages_per_batch": "5"
          }
        }
      },
      {
        "table": {
          "source_table": "users"
        }
      },
      {
        "table": {
          "source_table": "comments",
          "table_configuration": {
            "max_records_per_batch": "200",
            "max_pages_per_batch": "10"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your Notion `api_key`.
- For each `table`:
  - `source_table` must be one of the supported table names listed above.
  - Table options such as `max_records_per_batch` and `max_pages_per_batch` are placed under `table_configuration` and used to control how data is read.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

- On the **first run**, the connector performs an initial sync of all accessible data up to the pipeline start time.
- On **subsequent runs**, incremental tables (`databases`, `pages`, `blocks`, `comments`) use stored cursors to pick up only new or modified records. The `users` table always performs a full refresh.

#### Best Practices

- **Start small**: Begin by syncing one or two tables (e.g., `pages` and `databases`) to validate configuration and data shape before adding `blocks` and `comments`.
- **Use incremental sync**: Four of the five tables support CDC, reducing API calls on subsequent runs.
- **Tune batch sizes**: Adjust `max_records_per_batch` and `max_pages_per_batch` to balance throughput against API usage. Smaller batches mean more frequent but lighter pipeline triggers.
- **Share pages with the integration**: The connector can only access pages and databases that have been explicitly shared with the Notion integration. If data is missing, verify that the relevant content has been shared.
- **Respect rate limits**: Notion enforces rate limits (averaging 3 requests per second). The connector has built-in retry logic with exponential backoff for 429 and server errors, but consider staggering syncs if you are running multiple pipelines against the same workspace.

#### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`401 Unauthorized`)**:
  - Verify that the `api_key` is correct and has not been revoked.
  - Ensure the integration has the required capabilities enabled (Read content, Read user information, Read comments).
- **Missing data (pages or databases not appearing)**:
  - Notion integrations can only access content that has been explicitly shared with them. Open each page or database, click the three-dot menu, select **Connect to**, and choose your integration.
- **Rate limiting (`429 Too Many Requests`)**:
  - The connector retries automatically with exponential backoff. If rate limiting persists, reduce `max_records_per_batch` and `max_pages_per_batch`, or widen the schedule interval between pipeline runs.
- **Server errors (`500`, `502`, `503`)**:
  - The connector retries these automatically (up to 10 attempts with exponential backoff). Transient Notion API issues typically resolve on their own.
- **Empty `blocks` or `comments` tables**:
  - These tables depend on finding recently modified pages via Search. If no pages have been modified since the last cursor, no blocks or comments will be emitted. This is expected behavior.
- **JSON string fields**:
  - Fields such as `properties`, `content`, `rich_text`, `icon`, `cover`, and `bot` are stored as JSON strings. Use Spark's `from_json()` or similar functions downstream to parse them into structured columns.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/notion/notion.py`
- Connector schemas and constants: `src/databricks/labs/community_connector/sources/notion/notion_schemas.py`
- Connector API documentation: `src/databricks/labs/community_connector/sources/notion/notion_api_doc.md`
- Official Notion API documentation:
  - [https://developers.notion.com/reference](https://developers.notion.com/reference)
  - [https://developers.notion.com/docs/authorization](https://developers.notion.com/docs/authorization)
  - [https://developers.notion.com/reference/post-search](https://developers.notion.com/reference/post-search)
  - [https://developers.notion.com/reference/get-users](https://developers.notion.com/reference/get-users)
  - [https://developers.notion.com/reference/retrieve-a-comment](https://developers.notion.com/reference/retrieve-a-comment)
  - [https://developers.notion.com/reference/get-block-children](https://developers.notion.com/reference/get-block-children)
