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

> **Note**: Table names are case-sensitive. Use the exact names shown above (lowercase with underscores). Custom objects are also supported and will be discovered automatically.

### Resource snapshots

These additive tables read complete snapshots. Each page is yielded before the
next request, while the batch fails closed if a partition or later page cannot be
read. The snapshot consumer reconciles removals only after the complete read
succeeds.

| Table | Primary key | Coverage |
|---|---|---|
| `owners` | `id` | Active and archived owners |
| `pipelines` | `objectTypeId`, `id` | Pipelines and nested stages |
| `forms` | `id` | Active and archived forms |
| `contact_lists` | `listId` | Lists with current and, when available, legacy IDs |
| `contact_list_memberships` | `listId`, `recordId` | Current membership for every discovered contact list |
| `form_submissions` | `formId`, `conversionId` | Submissions for every discovered form |

Resource snapshots ignore `max_records_per_batch`, because stopping early would
publish an incomplete current set. `pipeline_object_types` may provide an
explicit comma-separated set such as `0-3,0-5`; otherwise the connector reads
the built-in pipeline types plus discovered custom objects. Missing permissions
fail the selected snapshot. The optional legacy-list lookup may instead leave
`legacyListId` null when HubSpot returns 403, 404, or 410.

### Custom objects and associations

Custom tables keep their lowercase schema names while API requests use HubSpot's
canonical `objectTypeId`. Directional schema definitions add association columns
for standard, custom, and self relationships. Association batch reads page each
parent independently and fail before checkpointing if a response is incomplete.
When custom objects exist, their directional definitions can therefore add new
association columns to standard tables as well as custom tables.

Association columns are ID arrays by default. Source-specific table options can
change that behavior:

| Option | Values | Behavior |
|---|---|---|
| `associations` | omitted, empty, or comma-separated names | Fetch all discovered targets, none, or the named subset. Unknown names fail validation. |
| `association_value_mode` | `ids` (default), `typed` | Emit `array<string>` IDs or structs containing `id`, `typeId`, `category`, and `label`. |
| `association_sync_mode` | `incremental` (default), `full_refresh` | Refresh relationships with changed parents or rescan all parents as a snapshot so removals can be reconciled. |

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

The association options above are optional source-specific table configurations.

### Activity reliability and numeric values

Activity incrementals finish every record sharing the current timestamp before
advancing their checkpoint. Archive reads drain the complete frozen window, and
meetings use complete active snapshots because HubSpot does not support their
archive listing. Read-only GETs and POSTs retry `429`, `500`, `502`, and `503`
up to five times and honor `Retry-After`.

Numeric properties remain `BIGINT` by default for backward compatibility. Set
`numeric_value_mode=decimal` to use `DECIMAL(38,18)`. Exact values that fit stay
exact; recognizable long binary-float expansions such as HubSpot's long form of
`0.1` normalize to their shortest representation. Other overflow, nonfinite,
or malformed values fail before checkpointing.

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
