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
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed from the pipeline spec to the connector. **This must be added to your UC connection.** | `per_page,max_pages_per_batch,lookback_seconds,start_date,include` |

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
3. **Important**: Set `externalOptionsAllowList` to `per_page,max_pages_per_batch,lookback_seconds,start_date,include` in your Unity Catalog connection. This is **required** for the connector to accept table-specific configuration from your pipeline specification.

The connection can also be created using the standard Unity Catalog API.

> **Critical**: Without setting `externalOptionsAllowList` in your UC connection, table-specific options like `per_page`, `start_date`, etc., will be rejected by the connector.

## Supported Objects

The Freshservice connector exposes a **static list** of tables covering IT service management objects:

- `tickets` - Support tickets and service requests
- `problems` - Problem records for root cause analysis
- `changes` - Change management records
- `releases` - Release management records
- `agents` - Support agents and their information
- `requesters` - End users and requesters
- `locations` - Physical or virtual locations
- `products` - Product catalog items
- `vendors` - Vendor information
- `assets` - Hardware and software assets
- `asset_types` - Asset type definitions and configurations
- `purchase_orders` - Purchase orders for assets
- `software` - Software application records
- `service_catalog` - Service catalog items available for requests
- `requested_items` - Service catalog items requested in service request tickets
- `conversations` - Ticket conversations, notes, and replies

### Object Summary, Primary Keys, and Ingestion Mode

The connector defines the ingestion mode and primary key for each table:

| Table                           | Description                                                  | Ingestion Type | Primary Key(s)    | Incremental Cursor (if any) |
|---------------------------------|--------------------------------------------------------------|----------------|-------------------|-----------------------------|
| `tickets`                       | Support tickets and incidents                                | `cdc`          | `id`              | `updated_at`                |
| `problems`                      | Problem records for root cause analysis                      | `cdc`          | `id`              | `updated_at`                |
| `changes`                       | Change management records                                    | `cdc`          | `id`              | `updated_at`                |
| `releases`                      | Release management records                                   | `cdc`          | `id`              | `updated_at`                |
| `agents`                        | Support agents and their information                         | `snapshot`     | `id`              | n/a                         |
| `requesters`                    | End users and requesters                                     | `snapshot`     | `id`              | n/a                         |
| `locations`                     | Physical or virtual locations                                | `snapshot`     | `id`              | n/a                         |
| `products`                      | Product catalog                                              | `snapshot`     | `id`              | n/a                         |
| `vendors`                       | Vendor information                                           | `snapshot`     | `id`              | n/a                         |
| `assets`                        | Hardware and software asset inventory                        | `snapshot`     | `id`              | n/a                         |
| `asset_types`                   | Asset type definitions                                       | `snapshot`     | `id`              | n/a                         |
| `purchase_orders`               | Purchase orders                                              | `snapshot`     | `id`              | n/a                         |
| `software`                      | Software applications                                        | `snapshot`     | `id`              | n/a                         |
| `service_catalog`               | Service catalog items available for requests                 | `snapshot`     | `id`              | n/a                         |
| `requested_items`               | Service catalog items requested in service request tickets   | `snapshot`     | `id`, `ticket_id` | n/a                         |
| `conversations`                 | Ticket conversations, notes, and replies                     | `snapshot`     | `id`, `ticket_id` | n/a                         |

**Ingestion Type Definitions:**
- **`cdc` (Change Data Capture)**: Supports incremental reads using the `updated_at` timestamp. The connector tracks which records have been updated since the last sync. These tables support soft deletes (deleted records have `deleted=true`).
- **`snapshot`**: Requires full table reads. All records are fetched on each sync with no incremental capability.

### Required and Optional Table Options

Table-specific options are passed via the **pipeline specification** under `table_configuration` for each table in `objects`. These options control pagination, incremental sync behavior, and other read characteristics.

> **Important**: These options must be specified in your pipeline spec (see example below), NOT in your Unity Catalog connection. The UC connection only needs `externalOptionsAllowList` to allow these options to be passed through.

**Common options for all tables:**
- `per_page` (integer, optional): Number of records to fetch per page. Default: `100`. Maximum: `100`.
- `max_pages_per_batch` (integer, optional): Maximum number of pages to fetch in a single read operation. Default: `50`. This helps control memory usage and runtime.

**Additional options for incremental tables** (`tickets`, `problems`, `changes`, `releases`):
- `start_date` (ISO 8601 string, optional): Initial starting point for incremental sync when there is no stored offset yet. Format: `YYYY-MM-DDTHH:MM:SSZ` (e.g., `2025-12-01T00:00:00Z`).
- `lookback_seconds` (integer, optional): Lookback window in seconds when computing the next cursor to handle late-arriving updates. Default: `300` (5 minutes).

**Tickets-specific options:**
- `include` (string, optional): Comma-separated list of related data to include in ticket responses. This enhances ticket data by including additional fields in a single API call. Supported values:
  - `stats` - Includes ticket statistics (`closed_at`, `resolved_at`, `first_responded_at`)
  - `requester` - Includes requester details (email, name, phone, mobile)
  - `requested_for` - Includes details about the user for whom the ticket was requested
  - `onboarding_context` - Includes onboarding-related information
  - `offboarding_context` - Includes offboarding-related information
  - Example: `"include": "stats,requester,requested_for"`

**Special notes:**
- **`requested_items`**: This is a nested resource under service request tickets. The connector automatically fetches all service request tickets first, then retrieves requested items for each service request. No additional configuration is needed beyond the common options.
- **`conversations`**: This is a nested resource under tickets. The connector automatically fetches all tickets first, then retrieves conversations for each ticket. No additional configuration is needed beyond the common options.

### Schema Highlights

Full schemas are defined by the connector and align with the Freshservice API v2 documentation:

- **`tickets`**: Includes fields such as `subject`, `description`, `priority`, `status`, `requester_id`, `responder_id`, `created_at`, `updated_at`, and arrays like `cc_emails`, `tags`. Also includes `custom_fields` stored as a JSON string for instance-specific custom data. When using the `include` parameter, additional fields like `stats` and `requester` are added to the response.

- **`problems`**, **`changes`**, and **`releases`**: Similar structure with IT service management specific fields like `impact`, `risk`, `planned_start_date`, `planned_end_date`, and planning/analysis fields stored as JSON strings.

- **`agents`** and **`requesters`**: Include user information such as names, email addresses, phone numbers, department associations, and `custom_fields` stored as JSON strings.

- **`locations`** and **`vendors`**: Include structured address information stored as nested objects with fields like `line1`, `line2`, `city`, `state`, `country`, `zipcode`.

- **`assets`**: Include asset tracking information such as `asset_tag`, `asset_state`, `usage_type`, along with `type_fields` stored as a JSON string containing asset-type-specific attributes.

- **`asset_types`**: Include type definitions with fields like `name`, `description`, and hierarchical relationships via `parent_asset_type_id`.

- **`purchase_orders`**: Include vendor details, billing/shipping addresses, line items in the `purchase_items` field (stored as JSON string), and financial fields with decimal precision.

- **`service_catalog`**: Include catalog item details such as `name`, `delivery_time`, `visibility`, `item_type`, and configuration options.

- **`requested_items`**: Include service catalog item details like `service_item_id`, `quantity`, `stage`, `cost_per_request`, and fulfillment information. Each record includes a `ticket_id` reference to the parent service request.

- **`conversations`**: Include conversation details such as `body`, `body_text`, `incoming`, `private`, `user_id`, and email information. Each record includes a `ticket_id` reference to the parent ticket.

You usually do not need to customize the schema; it is static and driven by the connector implementation. Well-defined nested objects (like `address`) are preserved as structured types, while dynamic/custom fields are stored as JSON strings to ensure compatibility with Spark/Databricks.

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
| object/struct (well-defined)    | `address` (locations, vendors)                              | struct (`StructType`)          | Well-defined nested objects with known schemas are preserved as structs.                   |
| object/struct (dynamic)         | `custom_fields`, `planning_fields`, `type_fields`, `roles`, `ratings`, `purchase_items` | JSON string (`StringType`) | Dynamic/custom fields are stored as JSON strings for Spark compatibility. Use `from_json()` or `get_json_object()` to parse. |
| nullable fields                 | Most optional fields                                        | same as base type + `null`     | Missing values and absent nested objects are surfaced as `null`, not empty strings.        |

The connector is designed to:

- Use `LongType` for all identifier fields to prevent integer overflow.
- Preserve well-defined nested structures (like `address`) as structs for type safety.
- Store dynamic/custom fields as JSON strings to ensure compatibility with Spark/Databricks and avoid empty struct errors.
- Treat absent fields as `null` to maintain data integrity and schema consistency.

### Working with JSON String Fields

Dynamic fields like `custom_fields`, `planning_fields`, `type_fields`, and others are stored as JSON strings. To work with these fields in your Spark queries, use Spark's JSON functions:

```python
from pyspark.sql.functions import from_json, get_json_object, schema_of_json

# Extract a specific value from a JSON string field
df = df.withColumn(
    "priority_reason", 
    get_json_object("custom_fields", "$.priority_reason")
)

# Parse the entire JSON string into a struct with a known schema
custom_schema = "priority_reason STRING, escalation_notes STRING"
df = df.withColumn(
    "custom_fields_parsed", 
    from_json("custom_fields", custom_schema)
)

# Automatically infer schema from sample data
sample_json = df.select("custom_fields").filter("custom_fields IS NOT NULL").first()[0]
inferred_schema = schema_of_json(sample_json)
df = df.withColumn(
    "custom_fields_parsed", 
    from_json("custom_fields", inferred_schema)
)
```

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
        # Tickets with include parameter for enhanced data
        {
            "table": {
                "source_table": "tickets",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "lakeflow",
                "destination_table": "freshservice_tickets",
                "table_configuration": {
                    "scd_type": "APPEND_ONLY",
                    "start_date": "2025-12-01T00:00:00Z",
                    "per_page": 100,
                    "max_pages_per_batch": 50,
                    "lookback_seconds": 300,
                    "include": "stats,requester,requested_for"
                },
            }
        },
        # Changes - new CDC table
        {
            "table": {
                "source_table": "changes",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "lakeflow",
                "destination_table": "freshservice_changes",
                "table_configuration": {
                    "start_date": "2025-12-01T00:00:00Z"
                },
            }
        },
        # Agents - user management
        {
            "table": {
                "source_table": "agents",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "lakeflow",
                "destination_table": "freshservice_agents",
                "table_configuration": {
                    "per_page": 100
                },
            }
        },
        # Conversations - ticket discussions
        {
            "table": {
                "source_table": "conversations",
                "destination_catalog": "fabio_goncalves",
                "destination_schema": "lakeflow",
                "destination_table": "freshservice_conversations",
                "table_configuration": {
                    "per_page": 100,
                    "max_pages_per_batch": 20
                },
            }
        },
        # Assets - snapshot table
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
  - `table_configuration` contains the table-specific options like `per_page`, `start_date`, `include`, etc.
  - For incremental tables (`tickets`, `problems`, `changes`, `releases`), you can specify `start_date`, `lookback_seconds`, etc.
  - For the `tickets` table, you can use the `include` parameter to retrieve additional related data like `stats`, `requester`, and more.
  - For snapshot tables, you typically only need `per_page` and `max_pages_per_batch`.
  - For nested resources (`requested_items`, `conversations`), the connector automatically handles fetching parent records.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

For incremental tables (`tickets`, `problems`, `changes`, `releases`):
- On the **first run**:
  - Omit `start_date` to backfill all historical data (may be heavy for large datasets), or
  - Set `start_date` to a recent cutoff to limit the initial history.
- On **subsequent runs**, the connector uses the stored cursor (based on `updated_at`) plus the lookback window to safely pick up any late-arriving updates.
- For `tickets`, consider using the `include` parameter to enrich data with statistics and requester information in a single API call.

For snapshot tables:
- All records are fetched on each run. No cursor or incremental sync is used.
- For nested resources (`requested_items`, `conversations`), the connector automatically fetches all parent records and their children.

#### Best Practices

- **Start Small**: Begin by syncing a subset of tables (e.g., `tickets`, `agents`, and `assets`) to validate your configuration and understand the data structure.
- **Use Incremental Sync**: For tables that support it (`tickets`, `problems`, `changes`, `releases`), leverage incremental sync to reduce API calls and improve performance.
- **Use the Include Parameter for Tickets**: When syncing `tickets`, consider using the `include` parameter to fetch related data (like `stats` and `requester`) in a single API call, reducing the need for additional lookups.
- **Configure Appropriate Page Sizes**: Use `per_page=100` (the maximum) for optimal performance. Adjust `max_pages_per_batch` based on your runtime requirements and memory constraints.
- **Set Appropriate Schedules**: Balance data freshness requirements with API rate limits. For example:
  - Sync incremental tables (`tickets`, `problems`, `changes`, `releases`) every hour or more frequently.
  - Sync user/configuration tables (`agents`, `requesters`, `asset_types`, `service_catalog`) daily.
  - Sync asset/inventory tables (`assets`, `products`, `vendors`, `locations`) daily or weekly.
  - Sync nested resources (`conversations`) based on your needs, as they can generate many API calls.
- **Monitor Rate Limits**: Freshservice enforces a rate limit of **1000 requests per hour per API key**. Monitor the `X-RateLimit-*` response headers to track usage. If you hit rate limits, consider:
  - Increasing the sync interval.
  - Reducing the number of tables synced concurrently.
  - Using larger page sizes (`per_page=100`) to reduce the number of requests.
  - For nested resources (`conversations`, `requested_items`), adjust `max_pages_per_batch` to limit parent records fetched.
- **Handle Soft Deletes**: For incremental tables, the connector automatically surfaces deleted records (where `deleted=true`). Ensure your downstream processing handles these appropriately.
- **Plan for Nested Resources**: The `conversations` and `requested_items` tables fetch data by iterating through parent tickets. This can result in many API calls. Consider limiting the number of parent tickets processed per batch using `max_pages_per_batch`.

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

- **Missing data for `conversations`**:
  - `conversations` are nested under tickets.
  - If your tickets have no notes, replies, or comments, the `conversations` table will be empty or have very few records.
  - This is normal for tickets that haven't had any discussion or updates.

- **Include parameter not working for tickets**:
  - Verify that `include` is listed in `externalOptionsAllowList` in your Unity Catalog connection.
  - Ensure the `include` parameter is specified in `table_configuration` for the tickets table, not in the connection parameters.
  - Check that you're using valid include values: `stats`, `requester`, `requested_for`, `onboarding_context`, `offboarding_context`.
  - The API response may not include these fields if they're not available for certain tickets.

- **Schema mismatches downstream**:
  - The connector preserves well-defined nested structures (like `address`) as structs.
  - Dynamic fields (`custom_fields`, `planning_fields`, etc.) are stored as JSON strings.
  - Ensure downstream tables are defined to accept these types, or parse/flatten them in your transformations using Spark JSON functions.

- **Slow performance on first sync**:
  - Initial syncs for large datasets (especially without `start_date`) can be slow.
  - Consider setting `start_date` to a recent date to limit the initial backfill.
  - Adjust `max_pages_per_batch` to control how much data is fetched per batch.

- **Custom fields appear empty or null**:
  - The `custom_fields` field contains instance-specific custom fields defined in your Freshservice account, stored as a JSON string.
  - If no custom fields are configured or populated, this will be `null` or an empty JSON object (`{}`).
  - Custom field schemas are dynamic and cannot be statically defined by the connector.
  - Use Spark's `from_json()` or `get_json_object()` functions to parse custom fields (see "Working with JSON String Fields" section above).

## References

- Connector implementation: `sources/freshservice/freshservice.py`
- Connector API documentation and schemas: `sources/freshservice/freshservice_api_doc.md`
- Test results: `sources/freshservice/TEST_RESULTS.md`
- Official Freshservice API Documentation:
  - API Overview: https://api.freshservice.com/
  - Developer Portal: https://developers.freshservice.com/
  - Tickets API (with include parameter): https://developers.freshworks.com/api-sdk/freshservice/tickets.html
  - Problems API: https://api.freshservice.com/#problems
  - Changes API: https://api.freshservice.com/#changes
  - Releases API: https://api.freshservice.com/#releases
  - Agents API: https://api.freshservice.com/#agents
  - Requesters API: https://api.freshservice.com/#requesters
  - Assets API: https://api.freshservice.com/#assets
  - Asset Types API: https://api.freshservice.com/#asset_types
  - Products API: https://api.freshservice.com/#products
  - Vendors API: https://api.freshservice.com/#vendors
  - Locations API: https://api.freshservice.com/#locations
  - Purchase Orders API: https://api.freshservice.com/#purchase_orders
  - Software API: https://api.freshservice.com/#applications
  - Service Catalog API: https://api.freshservice.com/#service_catalog

