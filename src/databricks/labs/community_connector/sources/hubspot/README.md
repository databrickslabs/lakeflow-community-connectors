# Lakeflow HubSpot Connector

The Lakeflow HubSpot Connector allows you to extract data from your HubSpot instance and load it into your data lake or warehouse. This connector supports both incremental and full refresh synchronization patterns for HubSpot objects.

This is an open-source community connector with no Databricks SLA or certification.
CRM schema, property, object, history and association reads use the pinned HubSpot
`2026-09` API release. Public API documentation is the contract; live acceptance
must also be verified against the target account's permissions and subscription.

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

The HubSpot connector supports the following CRM objects with dynamic schema
discovery and incremental or snapshot synchronization:

| Table Name | Primary Key | Cursor Field | Associations |
|------------|-------------|--------------|--------------|
| `contacts` | `id` | `updatedAt` | companies |
| `companies` | `id` | `updatedAt` | contacts |
| `deals` | `id` | `updatedAt` | contacts, companies, tickets |
| `tickets` | `id` | `updatedAt` | contacts, companies, deals |
| `calls` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `emails` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `meetings` | `id` | – | contacts, companies, deals, tickets |
| `tasks` | `id` | `updatedAt` | contacts, companies, deals, tickets |
| `notes` | `id` | `updatedAt` | contacts, companies, deals, tickets |

> **Note**: Table names are case-sensitive. Use the exact lowercase names shown above. Custom objects are discovered automatically.

### Resource snapshots

The following tables use complete snapshot ingestion. Each read drains every
configured partition and page in one Spark task. An HTTP error, malformed page,
incomplete partition discovery, or conflicting duplicate fails that task. The
downstream snapshot consumer commits rows and reconciles removals only after a
successful complete task.
These tables retain all source fields in a JSON `payload` column alongside typed
common fields. They do not use `max_records_per_batch` or property filters.

| Table | Primary key | Coverage |
|---|---|---|
| `owners` | `id` | Active and archived owners, including teams |
| `pipelines` | `objectTypeId`, `id` | Pipelines and nested stages for configured object types |
| `forms` | `id` | Active and archived forms |
| `contact_lists` | `listId` | Contact lists with current `listId` and mapped v1 `legacyListId`; the legacy value is null when the v1 catalog is unavailable or HubSpot reports no mapping |
| `contact_list_memberships` | `listId`, `recordId` | Current memberships of each discovered contact list |
| `form_submissions` | `formId`, `conversionId` | Submissions for each discovered form |

Meeting and contact-list-membership snapshots stream complete API pages through
one batch-reader invocation and do not expose a continuation offset. They ignore
`max_records_per_batch`, because stopping a snapshot early would publish an
incomplete current set. The connector retains the active page, previous-page
identities, current list identity, and bounded cursor-cycle history rather than
collecting the complete snapshot in connector memory. A later page failure
fails the Spark task, so the snapshot consumer does not commit rows from a
partial pass.

Private apps need `crm.objects.owners.read` for owners, `forms` for forms and
submissions, and an appropriate list read scope (`crm.segments.read` or
`crm.lists.read`) for contact lists and memberships. Pipeline access also depends
on the object permissions and subscription. Missing permission fails a selected
snapshot; it is not interpreted as an empty table.

By default, pipeline snapshots enumerate deals (`0-3`), tickets (`0-5`), tasks
(`0-27`), and discovered custom objects. Schema discovery must succeed for that
inventory to be complete. Use `pipeline_object_types` to supply a comma-separated
explicit set, such as `0-3,0-5`; that set replaces discovery. Add this option to an
existing Unity Catalog connection's external options allowlist before using it.
Every selected partition must be readable; inaccessible partitions are not skipped.

### Custom objects and associations

Custom table names remain the lowercase schema names; API calls use their
canonical `objectTypeId`. Directional schema association definitions add one
target ID-array column per related object, including custom-to-custom and
self-associations. The connector rejects ambiguous names and unsupported target
types instead of silently dropping relationships.

Object snapshot and incremental reads both hydrate relationships through the
2026-09 association batch API. Each parent has its own pagination cursor. IDs are
deduplicated and sorted across pages and labels. Empty arrays mean a successful
empty read. Missing parents are retried; incomplete responses or invalid cursors
fail the read before it returns a new checkpoint.

By default, each CRM table includes every association target listed in its
discovered graph. Set the table option `associations` to a comma-separated subset,
such as `contacts,companies`, to request only those columns and relationships. Set
`associations=` to request none. Targets must exactly match the discovered names;
duplicates, whitespace, empty tokens, and unknown targets are rejected. The same
selection controls schema discovery, metadata, API hydration, and emitted rows, so
unselected targets are absent rather than represented as empty arrays. Changing
this option changes the physical table schema and requires a fresh destination or
an explicit schema migration. Add `associations` to an existing Unity Catalog
connection's external options allowlist before using it.

If a completed batch still reports only `OBJECT_NOT_FOUND` after those retries,
the connector reconciles every unresolved parent through the documented
single-record association GET API. Each read starts at page one and replaces
earlier partial pages only after all GET pages succeed. A completely empty GET
collection also requires a successful parent read with a matching active ID.
Mixed errors, missing parents, malformed responses, and failed GETs still abort
the read. These requests do not provide a transactional snapshot during source
changes.

The default incremental mode refreshes associations only for parents selected by
their modification timestamp. HubSpot does not guarantee that association-only
changes update that timestamp. Set `association_sync_mode=full_refresh` on an
object table to advertise snapshot ingestion and enumerate all current parents
and relationships on each pipeline update. This reconciles association removals
without requiring a parent property change, at the cost of a complete scan.
Property-history tables retain their own ingestion strategy.

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
- **Ingestion Strategy**: Complete active snapshot with no continuation offset
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

### Activity coverage and schema migration

Calls, emails, meetings, tasks, and notes use their current CRM object APIs.
Discovered readable properties include bodies, email headers, attachment IDs,
meeting notes and transcripts, and task fields when present in the portal.
The connector preserves these property values; it does not download attachment
content or reconstruct a legacy `engagements` table. A consumer that still reads
the older engagement export shape should compare its consumed legacy fields
against these tables before switching.

Calls, emails, tasks, and notes use archive reconciliation. Each delete read
scans every archived page in a frozen archive-time window, irrespective of
`max_records_per_batch`, because listing order does not guarantee archive-time
order. Large archive inventories can therefore increase request volume and
memory use. Only archives returned within HubSpot's retention are observable;
hard deletes and expired archives are not guaranteed.

Full-refresh association mode drains all active pages in one batch-reader invocation and
ignores `max_records_per_batch`, preserving authoritative snapshot membership.

Meetings use complete active snapshots by default. HubSpot rejects archived
meeting listing with `VALIDATION_ERROR` for `MEETING_EVENT` (`0-47`). Each read
therefore enumerates the complete current meeting set, even with a saved offset
or record cap. The snapshot consumer reconciles missing IDs after a successful
complete read; there is no `meetings_delete` flow. This preserves current-state
removals without archive timestamps or deleted-meeting history. Meeting reads
remain snapshots when `association_sync_mode=incremental` is supplied.

Active snapshots reject empty pages with continuation, repeated cursors,
malformed records, and duplicate IDs. Contact-list membership snapshots use a
task-local disk-backed uniqueness index, so list parents and composite membership
keys are checked across the complete snapshot without retaining all IDs in memory.
Earlier pages may already have been yielded inside the Spark task when a later
page fails, but the failed task does not commit a partial snapshot and no
continuation offset is exposed. A successful empty snapshot represents an empty
current set. Pagination is not a transactional source snapshot: concurrent
source changes can still affect what a complete enumeration observes. Snapshot
reconciliation and mixed-object
option isolation require validation through the managed consumer.

CRM properties with HubSpot type `number` retain the existing `BIGINT` schema by
default. Set `numeric_value_mode=decimal` on a new destination to use
`DECIMAL(38,18)` and preserve fractional values. In decimal mode, exact long
binary64 expansions such as HubSpot's long form of `0.1` are reduced to the
shortest representable decimal; invalid, nonfinite, and out-of-range values fail
before checkpointing. Changing this option changes the destination schema, so
use a fresh destination or a planned schema migration.

### Common Schema Structure
All objects follow the same base structure:
- **Base Fields**: `id`, `createdAt`, `updatedAt`, `archived`
- **Associations**: Directional ID arrays named for each target object, for standard and discovered custom objects
- **Properties**: Selected object fields nested in the `properties` struct (for example, `properties.amount`)
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

These control destination ingestion behavior in the `table_configuration` map:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific options

For managed ingestion pipelines, put connector options under
`connector_options.community_connector_options.options` on each source table.
The managed Pipelines API can silently drop source options placed directly in
`table_configuration`. Verify the persisted pipeline specification before the
first update. For example:

```yaml
connection_name: your_hubspot_connection
objects:
  - table:
      source_table: deals
      destination_table: deals_decimal
      connector_options:
        community_connector_options:
          options:
            numeric_value_mode: "decimal"
            max_records_per_batch: "60"
```

Include each external option in the UC connection's allowlist. The connector's
Python interface still receives these values as its `table_options` map.

| Option | Applies to | Default | Description |
|---|---|---|---|
| `max_records_per_batch` | incremental object tables | no cap | Soft limit on records returned per microbatch. Reads finish records sharing the last processed `updatedAt` before advancing the checkpoint. |
| `association_sync_mode` | object tables | `incremental` | `incremental` refreshes selected changed parents. `full_refresh` uses snapshot ingestion and reconciles all current relationships on every update. Add this option to an existing UC connection's external options allowlist before using it. |
| `association_value_mode` | object tables | `ids` | `ids` emits `array<string>` target IDs. `typed` emits `array<struct<id,typeId,category,label>>`. Changing modes changes the destination schema and requires a fresh destination or planned migration. |
| `numeric_value_mode` | CRM object tables | `legacy` | `legacy` preserves the existing `BIGINT` schema. `decimal` emits `DECIMAL(38,18)` and normalizes exact binary64 expansions. Changing modes requires a fresh destination or planned migration. |
| `pipeline_object_types` | `pipelines` | deals, tickets, tasks, discovered custom types | Comma-separated canonical object type IDs. Replaces the default partition inventory; each configured partition must succeed. |
| `include_properties` | CRM object tables | all properties | Comma-separated allowlist of properties to ingest. Unknown names are ignored. The object's cursor property is always retained. |
| `exclude_properties` | CRM object tables | none | Comma-separated list of properties to drop after `include_properties` is applied. |

Two operational notes:

- Rows ingested before a filter change are not retroactively removed. To rebuild a table so its contents match the new filter, force a re-ingest (below).
- **Forcing a re-ingest:** changing a connector option recreates that table's ingestion flow and re-reads the source with the new settings. On serverless managed ingestion, a full refresh alone may resume from the pipeline's stored cursor.

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
- **Rate Limiting**: The connector retries transient reads with bounded backoff; reduce sync frequency if the retry budget is repeatedly exhausted
- **Missing Data**: Check Private App permissions and scopes
- **Schema Changes**: Update connector code if HubSpot adds new fields

**Error Handling:**
The connector includes built-in error handling for common scenarios like rate limiting, network issues, and API errors. Check the pipeline logs for detailed error information and recommended actions.
