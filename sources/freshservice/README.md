# Lakeflow Freshservice Community Connector

This documentation provides setup instructions and reference information for the Freshservice source connector to ingest data from the Freshservice API v2 into Databricks.

## Prerequisites

- **Freshservice account**: You need a Freshservice account with appropriate permissions to access the data you want to read.
- **API Key**: You must generate an API key from your Freshservice account. The API key is used for authentication.
- **Freshservice domain**: Your Freshservice subdomain (e.g., `yourcompany` for `yourcompany.freshservice.com`).
- **Network access**: The environment running the connector must be able to reach `https://{domain}.freshservice.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

To configure the connector, you must set up connection parameters in your Unity Catalog connection and table-specific parameters in your pipeline specification.

#### Unity Catalog Connection Parameters

The following parameters must be configured in your Unity Catalog connection:

| Name      | Type   | Required | Description                                                                                 | Example                            |
|-----------|--------|----------|---------------------------------------------------------------------------------------------|------------------------------------|
| `api_key` | string | yes      | Freshservice API Key for authentication.                                                    | `your_api_key_here`                |
| `domain`  | string | yes      | Freshservice subdomain (without `.freshservice.com`).                                       | `yourcompany`                      |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed from the pipeline spec to the connector. **This must be added to your UC connection.** | `per_page,max_pages_per_batch,lookback_seconds,start_date` |

> **Important**: The `externalOptionsAllowList` parameter **must be set in your Unity Catalog connection** to allow table-specific options to be passed through from your pipeline specification.

#### Pipeline-Level Table Configuration Parameters

The following parameters control pagination and incremental sync behavior. These parameters are **NOT** connection parameters. They **must be configured in your pipeline specification** under `table_configuration` for each table you want to ingest:

| Parameter | Type   | Required | Description                                                                                 | Example                            |
|-----------|--------|----------|---------------------------------------------------------------------------------------------|------------------------------------|
| `per_page` | integer | no | Number of records to fetch per API page. Default: `100`. Maximum: `100`. | `10` |
| `max_pages_per_batch` | integer | no | Maximum number of pages to fetch in a single read operation. Default: `50`. | `50` |
| `lookback_seconds` | integer | no | Lookback window in seconds for incremental tables to handle late-arriving updates. Default: `300` (5 minutes). | `300` |
| `start_date` | string (ISO 8601) | no | Initial starting point for incremental sync when there is no stored offset yet. Format: `YYYY-MM-DDTHH:MM:SSZ`. | `2025-12-01T00:00:00Z` |

> **Note**: These table-specific parameters should be specified in your `pipeline_spec` under each table's `table_configuration` section (see example below).

### Obtaining the Required Parameters

**Freshservice API Key:**
1. Log in to your Freshservice account as an admin.
2. Navigate to **Profile Settings** by clicking your profile picture in the top-right corner.
3. Your API key will be displayed in the profile page under the **API Key** section.
4. Copy the API key and store it securely. Use this as the `api_key` connection option.

**Freshservice Domain:**
- Your domain is the subdomain portion of your Freshservice URL. For example, if your Freshservice URL is `https://acme-corp.freshservice.com`, your domain is `acme-corp`.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Provide your `api_key` and `domain` as connection parameters.
3. **Important**: Set `externalOptionsAllowList` to `per_page,max_pages_per_batch,lookback_seconds,start_date` in your Unity Catalog connection. This is **required** for the connector to accept table-specific configuration from your pipeline specification.

The connection can also be created using the standard Unity Catalog API.

> **Critical**: Without setting `externalOptionsAllowList` in your UC connection, table-specific options like `per_page`, `start_date`, etc., will be rejected by the connector.

## Supported Objects

The Freshservice connector exposes a **static list** of tables covering IT service management objects:

- `tickets` - Support tickets and service requests
- `problems` - Problem records for root cause analysis
- `releases` - Release management records
- `locations` - Physical or virtual locations
- `products` - Product catalog items
- `vendors` - Vendor information
- `assets` - Hardware and software assets
- `purchase_orders` - Purchase orders for assets
- `software` - Software application records
- `requested_items` - Service catalog requested items

### Object Summary, Primary Keys, and Ingestion Mode

The connector defines the ingestion mode and primary key for each table:

| Table                           | Description                                                  | Ingestion Type | Primary Key(s)    | Incremental Cursor (if any) |
|---------------------------------|--------------------------------------------------------------|----------------|-------------------|-----------------------------|
| `tickets`                       | Support tickets and incidents                                | `cdc`          | `id`              | `updated_at`                |
| `problems`                      | Problem records for root cause analysis                      | `cdc`          | `id`              | `updated_at`                |
| `releases`                      | Release management records                                   | `cdc`          | `id`              | `updated_at`                |
| `locations`                     | Physical or virtual locations                                | `snapshot`     | `id`              | n/a                         |
| `products`                      | Product catalog                                              | `snapshot`     | `id`              | n/a                         |
| `vendors`                       | Vendor information                                           | `snapshot`     | `id`              | n/a                         |
| `assets`                        | Hardware and software asset inventory                        | `snapshot`     | `id`              | n/a                         |
| `purchase_orders`               | Purchase orders                                              | `snapshot`     | `id`              | n/a                         |
| `software`                      | Software applications                                        | `snapshot`     | `id`              | n/a                         |
| `requested_items`               | Service catalog items requested in service request tickets   | `snapshot`     | `id`, `ticket_id` | n/a                         |

**Ingestion Type Definitions:**
- **`cdc` (Change Data Capture)**: Supports incremental reads using the `updated_at` timestamp. The connector tracks which records have been updated since the last sync. These tables support soft deletes (deleted records have `deleted=true`).
- **`snapshot`**: Requires full table reads. All records are fetched on each sync with no incremental capability.

### Required and Optional Table Options

Table-specific options are passed via the **pipeline specification** under `table_configuration` for each table in `objects`. These options control pagination, incremental sync behavior, and other read characteristics.

> **Important**: These options must be specified in your pipeline spec (see example below), NOT in your Unity Catalog connection. The UC connection only needs `externalOptionsAllowList` to allow these options to be passed through.

**Common options for all tables:**
- `per_page` (integer, optional): Number of records to fetch per page. Default: `100`. Maximum: `100`.
- `max_pages_per_batch` (integer, optional): Maximum number of pages to fetch in a single read operation. Default: `50`. This helps control memory usage and runtime.

**Additional options for incremental tables** (`tickets`, `problems`, `releases`):
- `start_date` (ISO 8601 string, optional): Initial starting point for incremental sync when there is no stored offset yet. Format: `YYYY-MM-DDTHH:MM:SSZ` (e.g., `2025-12-01T00:00:00Z`).
- `lookback_seconds` (integer, optional): Lookback window in seconds when computing the next cursor to handle late-arriving updates. Default: `300` (5 minutes).

**Special notes:**
- **`requested_items`**: This is a nested resource under service request tickets. The connector automatically fetches all service request tickets first, then retrieves requested items for each service request. No additional configuration is needed beyond the common options.

### Schema Highlights

Full schemas are defined by the connector and align with the Freshservice API v2 documentation:

- **`tickets`**: Includes fields such as `subject`, `description`, `priority`, `status`, `requester_id`, `responder_id`, `created_at`, `updated_at`, and arrays like `cc_emails`, `tags`. Also includes nested `custom_fields` for instance-specific custom data.

- **`problems`** and **`releases`**: Similar structure with IT service management specific fields like `impact`, `risk`, `planned_start_date`, `planned_end_date`, and nested planning/analysis fields.

- **`locations`** and **`vendors`**: Include structured address information stored as nested objects with fields like `line1`, `line2`, `city`, `state`, `country`, `zipcode`.

- **`assets`**: Include asset tracking information such as `asset_tag`, `asset_state`, `usage_type`, along with nested `type_fields` containing asset-type-specific attributes.

- **`purchase_orders`**: Include vendor details, billing/shipping addresses, line items in the `purchase_items` array, and financial fields with decimal precision.

- **`requested_items`**: Include service catalog item details like `service_item_id`, `quantity`, `stage`, `cost_per_request`, and fulfillment information.

You usually do not need to customize the schema; it is static and driven by the connector implementation. Nested objects are preserved as structured types rather than being flattened.

## Data Type Mapping

Freshservice API fields are mapped to logical types as follows:

| Freshservice JSON Type          | Example Fields                                               | Connector Logical Type         | Notes                                                                                      |
|---------------------------------|--------------------------------------------------------------|--------------------------------|--------------------------------------------------------------------------------------------|
| integer (ID fields)             | `id`, `requester_id`, `agent_id`, `department_id`           | 64-bit integer (`LongType`)    | All numeric IDs are stored as `LongType` to avoid overflow.                                |
| string                          | `subject`, `description`, `name`, `email`, `status`         | string (`StringType`)          | Supports long text fields including HTML descriptions.                                     |
| boolean                         | `is_escalated`, `spam`, `deleted`, `active`, `known_error`  | boolean (`BooleanType`)        | Standard `true`/`false` values.                                                            |
| ISO 8601 datetime (string)      | `created_at`, `updated_at`, `due_by`, `planned_start_date`  | string in schema               | Stored as UTC ISO 8601 strings; can be cast to timestamp downstream.                       |
| decimal                         | `cost_per_request`, `conversion_rate`, `tax_percentage`     | decimal (`DecimalType(10,2)`)  | Used for currency and percentage fields with precise decimal representation.               |
| array                           | `tags`, `cc_emails`, `secondary_emails`, `department_ids`   | array of primitive types       | Arrays are preserved as nested collections.                                                |
| object/struct                   | `custom_fields`, `address`, `planning_fields`, `type_fields`| struct (`StructType`)          | Nested objects are preserved instead of flattened. Absent fields are represented as `null`.|
| nullable fields                 | Most optional fields                                        | same as base type + `null`     | Missing values and absent nested objects are surfaced as `null`, not empty objects.        |

The connector is designed to:

- Use `LongType` for all identifier fields to prevent integer overflow.
- Preserve nested JSON structures instead of flattening them into separate tables.
- Treat absent nested fields as `null` to maintain data integrity and schema consistency.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the Freshservice connector code. This typically places the connector code (e.g., `freshservice.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingest.py` or similar entrypoint), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** configured with your Freshservice `api_key`, `domain`, and `externalOptionsAllowList`.
- One or more **tables** to ingest, each with optional table-specific options in `table_configuration`.

> **Critical**: Table-specific parameters like `per_page`, `max_pages_per_batch`, `lookback_seconds`, and `start_date` must be specified in the `table_configuration` section of your pipeline spec, NOT in the Unity Catalog connection.

Example `pipeline_spec` for ingesting Freshservice tables:

```python
# Please update the spec below to configure your ingestion pipeline.

pipeline_spec = {
    "connection_name": "freshservice-new",
    "objects": [
        # Full config: customize destination and behavior
        {
            "table": {
                "source_table": "tickets",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "lakeflow",
                "destination_table": "freshservice_tickets",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "start_date": "2025-12-01T00:00:00Z",
                    "per_page": 10,
                    "max_pages_per_batch": 50,
                    "lookback_seconds": 300
                },
            }
        },
        # Minimal config: use defaults for everything
        {
            "table": {
                "source_table": "problems",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "lakeflow",
                "destination_table": "freshservice_problems",
                "table_configuration": {
                    "start_date": "2025-12-01T00:00:00Z"
                },
            }
        },
        # Snapshot table example
        {
            "table": {
                "source_table": "assets",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "lakeflow",
                "destination_table": "freshservice_assets",
                "table_configuration": {
                    "per_page": 100
                },
            }
        }
    ],
}
```

**Key points:**
- `connection_name` must point to the Unity Catalog connection configured with your Freshservice `api_key`, `domain`, and `externalOptionsAllowList`.
- For each `table`:
  - `source_table` must be one of the supported table names listed above (exact casing required).
  - `table_configuration` contains the table-specific options like `per_page`, `start_date`, etc.
  - For incremental tables (`tickets`, `problems`, `releases`), you can specify `start_date`, `lookback_seconds`, etc.
  - For snapshot tables, you typically only need `per_page` and `max_pages_per_batch`.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

For incremental tables (`tickets`, `problems`, `releases`):
- On the **first run**:
  - Omit `start_date` to backfill all historical data (may be heavy for large datasets), or
  - Set `start_date` to a recent cutoff to limit the initial history.
- On **subsequent runs**, the connector uses the stored cursor (based on `updated_at`) plus the lookback window to safely pick up any late-arriving updates.

For snapshot tables:
- All records are fetched on each run. No cursor or incremental sync is used.

#### Best Practices

- **Start Small**: Begin by syncing a subset of tables (e.g., `tickets` and `assets`) to validate your configuration and understand the data structure.
- **Use Incremental Sync**: For tables that support it (`tickets`, `problems`, `releases`), leverage incremental sync to reduce API calls and improve performance.
- **Configure Appropriate Page Sizes**: Use `per_page=100` (the maximum) for optimal performance. Adjust `max_pages_per_batch` based on your runtime requirements and memory constraints.
- **Set Appropriate Schedules**: Balance data freshness requirements with API rate limits. For example, sync incremental tables every hour and snapshot tables daily or weekly.
- **Monitor Rate Limits**: Freshservice enforces a rate limit of **1000 requests per hour per API key**. Monitor the `X-RateLimit-*` response headers to track usage. If you hit rate limits, consider:
  - Increasing the sync interval.
  - Reducing the number of tables synced concurrently.
  - Using larger page sizes (`per_page=100`) to reduce the number of requests.
- **Handle Soft Deletes**: For incremental tables, the connector automatically surfaces deleted records (where `deleted=true`). Ensure your downstream processing handles these appropriately.

#### Troubleshooting

**Common Issues:**

- **Authentication failures (`401 Unauthorized`)**:
  - Verify that your `api_key` is correct and active.
  - Check that the API key has the necessary permissions to read the data.
  - Ensure you haven't regenerated the API key in Freshservice without updating the connection configuration.

- **Invalid domain errors**:
  - Verify that the `domain` is correct (it should be just the subdomain, without `.freshservice.com`).
  - Example: For `https://acme-corp.freshservice.com`, use `acme-corp` as the domain.

- **Rate limiting (`429 Too Many Requests`)**:
  - The Freshservice API has a rate limit of 1000 requests per hour.
  - Check the `X-RateLimit-Remaining` and `Retry-After` headers in the API response.
  - Reduce sync frequency, use larger page sizes, or sync fewer tables concurrently.

- **Missing data for `requested_items`**:
  - `requested_items` are nested under service request tickets (tickets with `ticket_type = "Service Request"`).
  - If you have no service request tickets, the `requested_items` table will be empty.
  - Verify that your Freshservice instance has service catalog items being requested.

- **Schema mismatches downstream**:
  - The connector preserves nested structures (`custom_fields`, `address`, etc.) as structs.
  - Ensure downstream tables are defined to accept nested types, or explicitly flatten them in your transformations.

- **Slow performance on first sync**:
  - Initial syncs for large datasets (especially without `start_date`) can be slow.
  - Consider setting `start_date` to a recent date to limit the initial backfill.
  - Adjust `max_pages_per_batch` to control how much data is fetched per batch.

- **Custom fields appear empty**:
  - The `custom_fields` object contains instance-specific custom fields defined in your Freshservice account.
  - If no custom fields are configured or populated, this will appear as an empty struct.
  - Custom field schemas are dynamic and cannot be statically defined by the connector.

## References

- Connector implementation: `sources/freshservice/freshservice.py`
- Connector API documentation and schemas: `sources/freshservice/freshservice_api_doc.md`
- Official Freshservice API Documentation:
  - API Overview: https://api.freshservice.com/
  - Developer Portal: https://developers.freshservice.com/
  - Tickets API: https://api.freshservice.com/#tickets
  - Problems API: https://api.freshservice.com/#problems
  - Releases API: https://api.freshservice.com/#releases
  - Assets API: https://api.freshservice.com/#assets
  - Products API: https://api.freshservice.com/#products
  - Vendors API: https://api.freshservice.com/#vendors
  - Locations API: https://api.freshservice.com/#locations
  - Purchase Orders API: https://api.freshservice.com/#purchase_orders
  - Software API: https://api.freshservice.com/#applications

