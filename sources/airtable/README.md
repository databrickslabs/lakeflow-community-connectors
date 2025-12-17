# Lakeflow Airtable Community Connector

This documentation provides setup instructions and reference information for the Airtable source connector.

## Prerequisites

- An Airtable account with access to the base(s) you want to sync
- A Personal Access Token (PAT) with the following scopes:
  - `data.records:read` — Read records from bases
  - `schema.bases:read` — Read base schema (tables and fields)

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `personal_access_token` | string | Yes | Airtable Personal Access Token for authentication | `patXXXXXXXXXXXXXX` |
| `base_id` | string | Yes | The Airtable base ID to connect to | `appXXXXXXXXXXXXXX` |

### Table Options

The following table-specific options can be configured when reading data:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `view` | string | No | Name or ID of a view to use (applies view's filters/sorts) | `Grid view` |
| `fields` | string | No | Comma-separated list of field names to return | `Name,Status,Priority` |
| `filter_by_formula` | string | No | Airtable formula to filter records | `{Status}='Done'` |

If you plan to use any table-specific options, configure `externalOptionsAllowList` in your connection with the value: `view,fields,filter_by_formula`.

### Creating a Personal Access Token

1. Go to https://airtable.com/create/tokens
2. Click "Create new token"
3. Give your token a descriptive name (e.g., "Lakeflow Connector")
4. Add required scopes:
   - `data.records:read`
   - `schema.bases:read`
5. Select the bases you want to access (or all bases)
6. Click "Create Token" and save the token securely

### Finding Your Base ID

1. Open your Airtable base in the browser
2. Look at the URL: `https://airtable.com/appXXXXXXXXXXXXXX/...`
3. The base ID is the `appXXXXXXXXXXXXXX` portion

### Create a Unity Catalog Connection 

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one
3. If using table-specific options, set `externalOptionsAllowList` to: `view,fields,filter_by_formula`

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The Airtable connector dynamically discovers all tables in the configured base. Each table in your Airtable base becomes a supported object.

### Object Discovery

Tables are discovered automatically via the Airtable Metadata API. The connector reads the base schema to identify all available tables and their field definitions.

### Primary Keys

All Airtable tables use the `id` field as the primary key. This is a 17-character string prefixed with `rec` (e.g., `recXXXXXXXXXXXXXXX`).

### Ingestion Strategy

**Ingestion Type**: `snapshot`

Airtable does not natively support incremental reads via the API. The connector performs full table refreshes on each sync. This means:

- All records are fetched on each pipeline run
- Deleted records will be removed when the table is fully refreshed
- No cursor field is used

**Note**: If your Airtable tables include a "Last modified time" field, you can optionally use `filter_by_formula` to implement pseudo-incremental filtering, though this requires manual configuration.

### System Fields

Every record includes these system fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique record identifier (primary key) |
| `createdTime` | string | ISO 8601 timestamp when the record was created |


## Data Type Mapping

| Airtable Field Type | Spark Type | Notes |
|---------------------|------------|-------|
| `singleLineText` | StringType | Simple text field |
| `multilineText` | StringType | Multi-line text |
| `richText` | StringType | Rich text with formatting |
| `email` | StringType | Email address |
| `url` | StringType | URL |
| `phoneNumber` | StringType | Phone number |
| `number` | DoubleType | Numeric value |
| `currency` | DoubleType | Currency value |
| `percent` | DoubleType | Percentage (0.5 = 50%) |
| `duration` | LongType | Duration in seconds |
| `rating` | LongType | Rating value |
| `count` | LongType | Count of linked records |
| `autoNumber` | LongType | Auto-incrementing number |
| `checkbox` | BooleanType | True/false |
| `date` | StringType | ISO 8601 date string |
| `dateTime` | StringType | ISO 8601 datetime string |
| `createdTime` | StringType | Record creation timestamp |
| `lastModifiedTime` | StringType | Last modification timestamp |
| `singleSelect` | StringType | Selected option name |
| `multipleSelects` | ArrayType(StringType) | Array of selected options |
| `singleCollaborator` | StructType | Collaborator with id, email, name |
| `multipleCollaborators` | ArrayType(StructType) | Array of collaborators |
| `multipleRecordLinks` | ArrayType(StringType) | Array of linked record IDs |
| `multipleAttachments` | ArrayType(StructType) | Array of attachment objects |
| `barcode` | StructType | Object with text and type |
| `formula` | StringType | Formula result (varies) |
| `rollup` | StringType | Rollup result (varies) |
| `lookup` | ArrayType(StringType) | Looked up values |


## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure each table you want to sync. You can optionally specify a view or filter:

```json
{
  "pipeline_spec": {
      "connection_name": "my_airtable_connection",
      "object": [
        {
            "table": {
                "source_table": "Tasks",
                "view": "Active Tasks",
                "filter_by_formula": "{Status}!='Done'"
            }
        },
        {
            "table": {
                "source_table": "Projects"
            }
        }
      ]
  }
}
```

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of tables to test your pipeline
- **Use Views**: Apply Airtable views to filter and sort data server-side
- **Monitor Rate Limits**: Airtable allows 5 requests/second per base and 50 requests/second per token
- **Attachment URLs Expire**: URLs in attachment fields expire after a few hours; download files promptly if needed

#### Troubleshooting

**Common Issues:**

1. **Authentication Errors (401/403)**
   - Verify your Personal Access Token is valid and not expired
   - Ensure the token has the required scopes (`data.records:read`, `schema.bases:read`)
   - Check that the token has access to the specified base

2. **Rate Limiting (429)**
   - The connector implements automatic retry with exponential backoff
   - If issues persist, reduce the sync frequency or split into multiple pipelines

3. **Table Not Found**
   - Verify the table name matches exactly (case-sensitive)
   - Use table IDs instead of names for stability if tables are frequently renamed

4. **Missing Fields in Data**
   - Empty cells in Airtable are not returned in the API response
   - These will appear as `null` values in the synced data


## References

- [Airtable API Documentation](https://airtable.com/developers/web/api/introduction)
- [Personal Access Tokens Guide](https://airtable.com/developers/web/guides/personal-access-tokens)
- [Airtable Rate Limits](https://airtable.com/developers/web/api/rate-limits)
- [Airtable Field Types](https://airtable.com/developers/web/api/field-model)

