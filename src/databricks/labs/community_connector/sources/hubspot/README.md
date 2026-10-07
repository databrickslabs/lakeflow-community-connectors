# Lakeflow HubSpot Connector

The Lakeflow HubSpot Connector allows you to extract data from your HubSpot instance and load it into your data lake or warehouse. This connector supports both incremental and full refresh synchronization patterns for HubSpot objects.

## Set up

### Prerequisites
- Access to a HubSpot instance with API permissions
- HubSpot Private App with appropriate read permissions
- Developer or admin role in HubSpot (depending on the data you want to access)

### Required Parameters

To configure the HubSpot connector, you'll need to provide the following parameters in your connector options:

1. access_token

### Getting Your Private App Access Token

1. Log in to your HubSpot account as an admin
2. Navigate to Settings → Integrations → Private Apps
3. Click "Create a private app"
4. Configure the app name and description
5. Set the required scopes (contacts read permissions)
6. Create the app and copy the access token
7. Store the token securely (you won't be able to see it again)

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.

The connection can also be created using the standard Unity Catalog API.

## Objects Supported

The HubSpot connector supports the following CRM objects with dynamic schema discovery and incremental synchronization:

| Table Name | Primary Key | Cursor Field | Associations |
|------------|-------------|--------------|--------------|
| `contacts` | `id` | `updatedAt` | companies |
| `companies` | `id` | `updatedAt` | contacts |
| `deals` | `id` | `updatedAt` | contacts, companies, tickets |
| `tickets` | `id` | `updatedAt` | contacts, companies, deals |
| `calls` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `emails` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `meetings` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `tasks` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `notes` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `contacts_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `companies_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `deals_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `tickets_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |
| `<custom_object>_property_history` | `object_id`, `property`, `timestamp` | `timestamp` | – |

> **Note**: Table names are case-sensitive. Use the exact names shown above (lowercase with underscores). Custom objects are also supported and will be discovered automatically, each with a matching `<custom_object>_property_history` table.

### Object Details

### Standard CRM Objects

#### `contacts`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: companies, deals, tickets
- **Schema**: All contact properties (discovered dynamically) including standard fields like email, firstname, lastname, and all custom properties

#### `companies`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, deals, tickets
- **Schema**: All company properties (discovered dynamically) including standard fields like name, domain, and all custom properties

#### `deals`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, tickets
- **Schema**: All deal properties (discovered dynamically) including standard fields like dealname, amount, stage, and all custom properties

#### `tickets`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals
- **Schema**: All ticket properties (discovered dynamically) including standard fields like subject, content, priority, and all custom properties

### Engagement Objects

#### `calls`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All call properties (discovered dynamically) including standard fields like duration, outcome, recording details, and all custom properties

#### `emails`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All email properties (discovered dynamically) including standard fields like subject, body, sender, recipient, and all custom properties

#### `meetings`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All meeting properties (discovered dynamically) including standard fields like title, start time, end time, location, and all custom properties

#### `tasks`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All task properties (discovered dynamically) including standard fields like subject, due date, status, priority, and all custom properties

#### `notes`
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Associations**: contacts, companies, deals, tickets
- **Schema**: All note properties (discovered dynamically) including standard fields like body, timestamp, and all custom properties

### Custom Objects
- **Dynamic Discovery**: The connector automatically discovers and supports any custom objects in your HubSpot instance
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updatedAt`
- **Schema**: All custom object properties discovered dynamically via HubSpot Properties API

### Property History Tables

`contacts_property_history`, `companies_property_history`, `deals_property_history`, `tickets_property_history`, and `<custom_object>_property_history` contain one row per historical value of each property, from HubSpot's `propertiesWithHistory`.

- **Ingestion type**: `cdc` (upsert on the primary key, so rows that are read again by the lookback window are de-duplicated)
- **Primary Key**: `object_id`, `property`, `timestamp`
- **Cursor Field**: `timestamp`
- **Schema**: `object_id`, `property`, `value`, `timestamp`, `source_type`, `source_id`, `source_label`, `updated_by_user_id`
- **How it reads**: the Search API finds objects whose last-modified date changed since the last run (Search does not return history), then the Batch Read API (`POST /crm/v3/objects/{object}/batch/read`) fetches `propertiesWithHistory` for those objects, up to 50 IDs per call.
- **Rate limiting**: Search calls are spaced to stay under HubSpot's 5 requests/second Search limit, and HTTP 429 responses are retried (honouring `Retry-After`, otherwise exponential backoff, up to 5 retries). A single read stops before reaching the Search API's 10,000-result cap and resumes from the latest `updatedAt` on the next microbatch.
- **Scopes**: the same `crm.objects.<object>.read` scope as the parent object.
- **Known limitation**: changes that do not update the object's last-modified date (some calculated, rollup, or formula properties) are not detected until the object is next modified.

### Common Schema Structure
All objects follow the same base structure:
- **Base Fields**: `id`, `createdAt`, `updatedAt`, `archived`
- **Associations**: Arrays of associated object IDs (for standard objects)
- **Properties**: All object properties flattened with `properties_` prefix
- **Dynamic Discovery**: Schema adapts automatically to your HubSpot configuration

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

### Source-specific `table_configuration` options

| Option | Applies to | Default | Description |
|---|---|---|---|
| `max_records_per_batch` | all incremental tables | no cap | Maximum records returned per microbatch. For property history tables the cap is checked after each Batch Read call, so a microbatch can exceed it by up to one call's worth of rows. |
| `history_properties` | `*_property_history` | all properties | Comma-separated property names to fetch history for (e.g. `lifecyclestage,hs_lead_status,dealstage`). Recommended for portals with many properties, because requesting history for every property makes each Batch Read response larger. |
| `include_properties` | all tables | all properties | Comma-separated allowlist of properties to ingest. If set, only these discovered properties are read and exposed in the schema (unknown names are ignored, matching HubSpot's behavior). Required for objects with very many properties — HubSpot's Search API rejects requests over 3,000 characters, which unfiltered wide objects exceed. The object's cursor property is always retained. |
| `exclude_properties` | all tables | none | Comma-separated list of properties to drop from ingestion. Applied after `include_properties`. |
| `history_since` | `*_property_history` | none | ISO-8601 timestamp (e.g. `2025-01-01T00:00:00Z`). Only objects modified on or after this time are read, and older history entries are dropped. Caps the initial backfill. |
| `history_lookback_minutes` | `*_property_history` | `10` | Minutes subtracted from the saved cursor once per pipeline update, to pick up objects that were not yet indexed by the Search API in the previous run. Set to `0` to disable. |

`include_properties` / `exclude_properties` also apply to `*_property_history` tables (they bound which properties are requested in `propertiesWithHistory`; `history_properties` still narrows further for history reads). Changing either option changes the target table's columns and future ingestion only — previously ingested rows remain until the table is full-refreshed.

These options must be included in the connection's `externalOptionsAllowList` (see `connector_spec.yaml`).

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing contacts to test your pipeline
- **Monitor API Limits**: HubSpot has rate limits (100 requests per 10 seconds for most endpoints)
- **Use Incremental Sync**: Reduces API calls and improves performance
- **Set Appropriate Schedules**: Balance data freshness needs with API usage
- **Test Thoroughly**: Validate data accuracy and completeness after initial setup

#### Troubleshooting

**Common Issues:**
- **Authentication Errors**: Verify access token is correct and has proper scopes
- **Rate Limiting**: Reduce sync frequency or implement exponential backoff
- **Missing Data**: Check Private App permissions and scopes
- **Schema Changes**: Update connector code if HubSpot adds new fields

**Error Handling:**
The connector includes built-in error handling for common scenarios like rate limiting, network issues, and API errors. Check the pipeline logs for detailed error information and recommended actions.