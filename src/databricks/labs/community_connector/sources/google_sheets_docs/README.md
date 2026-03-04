# Lakeflow Google Sheets & Docs Community Connector

This documentation describes how to configure and use the **Google Sheets & Docs** Lakeflow community connector to ingest data from Google Drive, Google Sheets, and Google Docs into Databricks.

The connector uses OAuth2 credentials to access the Google Drive API v3, Google Sheets API v4, and Google Docs API v1. It exposes three tables: **spreadsheets** (Drive file list for spreadsheets), **sheet_values** (cell data from a specific sheet), and **documents** (Drive file list for Google Docs, with optional plain-text content).


## Prerequisites

- **Google Cloud project**: A project in [Google Cloud Console](https://console.cloud.google.com/) with the Drive, Sheets, and Docs APIs enabled.
- **OAuth 2.0 credentials**: A client ID and client secret from an OAuth 2.0 application (e.g. “Desktop app” or “Web application”), and a **refresh token** obtained with `access_type=offline` so the connector can obtain access tokens at runtime.
- **Scopes**: The refresh token must have been authorized with at least:
  - `https://www.googleapis.com/auth/spreadsheets.readonly` — read spreadsheets and sheet values
  - `https://www.googleapis.com/auth/documents.readonly` — read document content
  - `https://www.googleapis.com/auth/drive.readonly` — list and get file metadata (required to discover spreadsheets and docs)
- **Network access**: The environment running the connector must be able to reach `https://oauth2.googleapis.com`, `https://www.googleapis.com/drive/v3`, `https://sheets.googleapis.com/v4`, and `https://docs.googleapis.com/v1`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.


## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `client_id` | string | yes | OAuth 2.0 client ID from Google Cloud Console | `xxx.apps.googleusercontent.com` |
| `client_secret` | string | yes | OAuth 2.0 client secret | `GOCSPX-xxx` |
| `refresh_token` | string | yes | Long-lived refresh token (obtained with `access_type=offline` during authorization) | `1//xxx` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to be passed through. This connector uses table-specific options for `sheet_values` and `documents`. | `spreadsheet_id,spreadsheetId,sheet_name,range,include_content` |

The full list of supported table-specific options for `externalOptionsAllowList` is:

`spreadsheet_id,spreadsheetId,sheet_name,range,include_content`

> **Note**: Table-specific options such as `spreadsheet_id`, `sheet_name`, or `include_content` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **OAuth 2.0 client ID and client secret**:
  1. In Google Cloud Console, open **APIs & Services → Credentials**.
  2. Create an **OAuth 2.0 Client ID** (e.g. Desktop app or Web application).
  3. Note the **Client ID** and **Client secret**; these are your `client_id` and `client_secret`.
- **Refresh token**:
  - The connector does **not** run a user-facing OAuth flow. You must obtain a refresh token out-of-band (e.g. via a one-time auth script or a tool that performs the OAuth consent flow with `access_type=offline` and `prompt=consent`).
  - The resulting refresh token is long-lived and is used as the `refresh_token` connection option.
- Ensure the **Google Drive API**, **Google Sheets API**, and **Google Docs API** are enabled for your project.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `spreadsheet_id,spreadsheetId,sheet_name,range,include_content` so that table-specific options (e.g. for `sheet_values` and `documents`) can be passed through.

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The connector exposes a **static list** of tables:

- `spreadsheets`
- `sheet_values`
- `documents`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Notes |
|-------|-------------|----------------|-------------|--------|
| `spreadsheets` | Drive file list filtered by `mimeType=application/vnd.google-apps.spreadsheet` (metadata only) | snapshot | `id` | Paginated via Drive API; excludes trashed files. |
| `sheet_values` | Cell data from a single range in a spreadsheet (one row per sheet row; `values` is an array of cell values) | snapshot | — | Requires `spreadsheet_id` (or `spreadsheetId`) in table options; optional `sheet_name`, `range`. |
| `documents` | Drive file list filtered by `mimeType=application/vnd.google-apps.document`; optionally includes plain-text content via Drive export | snapshot | `id` | Set `include_content=true` to fetch document body as plain text (10 MB export limit per doc). |

- **spreadsheets**: Returns `id`, `name`, `mimeType`, `modifiedTime`, `createdTime`. The `id` is the spreadsheet ID used in the Sheets API.
- **sheet_values**: Returns `row_index` (1-based row number) and `values` (array of strings). Cell values are returned as unformatted strings. You specify which spreadsheet and range via table options.
- **documents**: Returns `id`, `name`, `mimeType`, `modifiedTime`, `createdTime`, and optionally `content` (plain text when `include_content` is enabled). The `id` is the document ID used in the Docs API.


## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|--------|----------|-------------|
| `source_table` | Yes | Table name in the source system (`spreadsheets`, `sheet_values`, or `documents`) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|--------|----------|-------------|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

| Table | Option | Required | Description |
|-------|--------|----------|-------------|
| **spreadsheets** | — | — | No table-specific options. Listing is paginated automatically via Drive API. |
| **sheet_values** | `spreadsheet_id` or `spreadsheetId` | yes | Spreadsheet ID (same as the Drive file `id` for that spreadsheet). |
| **sheet_values** | `sheet_name` | no | Sheet (tab) name. Default: `Sheet1`. |
| **sheet_values** | `range` | no | A1 notation range (e.g. `A:Z`, `A1:D100`). If no `!` is present, it is combined with `sheet_name` (e.g. `Sheet1!A:Z`). Default: `A:Z`. |
| **documents** | `include_content` | no | Set to `true`, `1`, or `yes` to fetch plain-text content for each document via Drive `files.export` (10 MB limit per document). Default: not set (metadata only). |


## Data Type Mapping

| Source (API) | Connector / Spark | Notes |
|--------------|-------------------|--------|
| Drive file `id`, `name`, `mimeType`, etc. | `StringType` | IDs and metadata as strings. |
| Drive `modifiedTime`, `createdTime` | `StringType` | RFC 3339 datetime strings. |
| Sheets cell values (values.get) | `StringType` (in `values` array) | Connector coerces to string for schema compatibility. |
| Document export (plain text) | `StringType` (`content`) | Plain text from Drive export when `include_content` is enabled. |


## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Google Sheets & Docs connector source in your workspace so that the connector code is available under a path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline specification, reference a Unity Catalog connection that uses this connector and configure one or more tables with the required table options where applicable.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "google_sheets_docs_connection",
    "object": [
      {
        "table": {
          "source_table": "spreadsheets"
        }
      },
      {
        "table": {
          "source_table": "sheet_values",
          "table_configuration": {
            "spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            "sheet_name": "Sheet1",
            "range": "A:Z"
          }
        }
      },
      {
        "table": {
          "source_table": "documents",
          "table_configuration": {
            "include_content": "true"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to a UC connection configured with `client_id`, `client_secret`, and `refresh_token` (and `externalOptionsAllowList` as above).
- For **sheet_values**, you must set `spreadsheet_id` (or `spreadsheetId`); `sheet_name` and `range` are optional.
- For **documents**, set `include_content` to `true` (or `1` or `yes`) only if you need the document body as plain text; otherwise omit it for metadata-only ingestion.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your usual Lakeflow or Databricks orchestration (e.g. scheduled job or workflow). All tables use snapshot ingestion; the connector re-reads data according to your schedule.

#### Best Practices

- **Start small**: Begin with `spreadsheets` or `documents` (metadata only) to validate credentials and connectivity, then add `sheet_values` for a single spreadsheet or enable `include_content` for documents if needed.
- **Rate limits**: Google enforces per-project and per-user limits (e.g. Sheets 300 requests/min per project, 60/min per user; Drive 12,000 requests per 60 seconds). The connector uses exponential backoff on 429/5xx; avoid overly frequent or concurrent syncs.
- **Document content**: Enabling `include_content` for `documents` triggers one Drive export per document (10 MB limit per export). Use for smaller or critical docs; for large catalogs, consider metadata-only sync or filtering.

#### Troubleshooting

- **Authentication failures (401 / 403)**:
  - Verify `client_id`, `client_secret`, and `refresh_token` are correct and that the refresh token was created with the required scopes (`spreadsheets.readonly`, `documents.readonly`, `drive.readonly`).
  - If using a service account, ensure target files are shared with the service account email.
- **Missing table options for sheet_values**: You must provide `spreadsheet_id` (or `spreadsheetId`) in `table_configuration`; otherwise the connector will raise an error.
- **404 for sheet or document**: Confirm the spreadsheet or document ID is valid and that the authenticated identity has access.
- **Rate limiting (429)**: Reduce sync frequency or the number of tables/ranges; the connector will retry with backoff, but sustained overuse can still lead to failures.


## References

- Connector implementation: `src/databricks/labs/community_connector/sources/google_sheets_docs/google_sheets_docs.py`
- Connector API documentation (endpoints, schemas, rate limits): [google_sheets_docs_api_doc.md](./google_sheets_docs_api_doc.md)
- Official Google APIs:
  - [Sheets API v4](https://developers.google.com/sheets/api/reference/rest)
  - [Docs API v1](https://developers.google.com/workspace/docs/api/reference/rest)
  - [Drive API v3](https://developers.google.com/drive/api/guides/about-files)
  - [OAuth 2.0 for Web Server Applications](https://developers.google.com/identity/protocols/oauth2/web-server)
