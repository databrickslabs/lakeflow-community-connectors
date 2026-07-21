# Lakeflow Notion Community Connector

This documentation provides setup instructions and reference information for the Notion source connector.

## Prerequisites

- A Notion account with access to the pages, data_sources, and blocks you want to sync
- A Notion Integration (API token) with the following capabilities:
  - Read access to pages and data_sources
  - Read access to block content
  - Read access to comments (if syncing comments)
- The integration must be shared with the pages/data_sources you want to ingest

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `api_token` | string | Yes | Your Notion Integration token (starts with `secret_`) | `secret_xxxxxxxxxxxxxxxxxxxxx` |

**Note:** This connector does not require `externalOptionsAllowList` as there are no extra table-specific options. You do not need to include this parameter.

### How to Obtain Required Parameters

#### 1. Create a Notion Integration

1. Go to the [Notion Developer Portal](https://www.notion.so/my-integrations)
2. Click **"+ New integration"**
3. Fill in the required fields:
   - **Name**: A descriptive name for your integration
   - **Logo**: (Optional) Upload a logo
   - **Associated workspace**: Select your workspace
4. Click **Submit**
5. Copy the **Internal Integration Token** (starts with `secret_`) - you'll need this for the `api_token` parameter

#### 2. Share Pages/Data sources with Your Integration

For each page or database you want to sync:

1. Open the page/database in Notion
2. Click the **Share** button (top right)
3. Click **Invite**
4. Search for and select your integration name
5. Grant **Can read** permissions (or higher if needed)

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide your Notion Integration token in the `api_token` field.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Notion connector supports the following tables:

| Table | Ingestion Type | Primary Key | Cursor Field | Description |
|-------|---------------|-------------|--------------|-------------|
| `pages` | CDC | `id` | `last_edited_time` | Notion pages with their properties |
| `data_sources` | CDC | `id` | `last_edited_time` | Notion data_sources with their schema |
| `blocks` | CDC | `id` | `last_edited_time` | Content blocks within pages (paragraphs, headings, lists, etc.) |
| `users` | Snapshot | `id` | N/A | Workspace users and bots |
| `comments` | CDC | `id` | `created_time` | Comments on pages and blocks |

### Incremental Ingestion

- **CDC tables** (`pages`, `data_sources`, `blocks`, `comments`): The connector tracks changes using the cursor field and only retrieves records modified since the last sync.
- **Snapshot table** (`users`): The full user list is retrieved on each sync.

### Block Types

The `blocks` table supports all Notion block types:
- `paragraph`, `heading_1`, `heading_2`, `heading_3`
- `bulleted_list_item`, `numbered_list_item`, `to_do`, `toggle`
- `child_page`, `child_database`
- `image`, `video`, `file`, `pdf`, `bookmark`
- `code`, `quote`, `divider`, `callout`
- `embed`, `link_preview`, `table`, `table_row`

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

## Data Type Mapping

| Notion Type | Databricks/Spark Type |
|-------------|----------------------|
| `string` (IDs, URLs, timestamps) | `StringType` |
| `boolean` | `BooleanType` |
| `rich_text[]` | `ArrayType(MapType(StringType, StringType))` |
| `object` (nested structures) | `MapType(StringType, StringType)` |
| `number` | `DoubleType` |

### Special Column Notes

- **Timestamps**: All timestamp fields (`created_time`, `last_edited_time`, etc.) are returned as ISO 8601 formatted strings.
- **Rich text**: Text content is stored as an array of objects with formatting information.
- **Nested objects**: Complex structures like `properties`, `parent`, `created_by` are flattened to map types.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure the tables you want to sync:

```json
{
  "pipeline_spec": {
    "connection_name": "your-notion-connection",
    "object": [
      {
        "table": {
          "source_table": "pages",
          "table_configuration": {
            "scd_type": "SCD_TYPE_1"
          }
        }
      },
      {
        "table": {
          "source_table": "data_sources"
        }
      },
      {
        "table": {
          "source_table": "users"
        }
      }
    ]
  }
}
```

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a single table (e.g., `pages`) to test your pipeline
- **Use Incremental Sync**: The connector automatically uses CDC for supported tables, reducing API calls and improving performance
- **Set Appropriate Schedules**: Notion API has rate limits; consider scheduling syncs every 15-60 minutes depending on your needs
- **Share Resources with Integration**: Remember to share all pages/data_sources with your Notion integration, or they won't be accessible
- **Block Access Limitation**: The `blocks` table requires parent page access; ensure your integration has read access to parent pages

#### Troubleshooting

**Common Issues:**

| Issue | Possible Cause | Solution |
|-------|---------------|----------|
| "401 Unauthorized" | Invalid or expired API token | Regenerate your integration token and update the connection |
| "403 Forbidden" | Integration lacks access to resource | Share the page/database with your integration |
| "404 Not Found" | Resource no longer exists or integration not shared | Verify the resource exists and integration has access |
| "429 Too Many Requests" | Rate limit exceeded | Wait and retry; the connector has built-in retry logic |
| Empty results from `blocks` table | Integration lacks parent page access | Share parent pages with the integration |

## References

- [Notion API Documentation](https://developers.notion.com/reference/intro)
- [Notion Developer Portal](https://www.notion.so/my-integrations)
- [Lakeflow Community Connectors Documentation](https://github.com/databrickslabs/lakeflow-community-connectors)