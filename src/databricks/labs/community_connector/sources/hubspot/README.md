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

### Resource snapshots

The following tables use complete snapshot ingestion. Each read drains every
configured partition and page before returning rows. An HTTP error, malformed
page, incomplete partition discovery, or conflicting duplicate fails the read.
The downstream snapshot consumer reconciles removals after a successful read.
These tables retain all source fields in a JSON `payload` column alongside typed
common fields. They do not use `max_records_per_batch` or property filters.

| Table | Primary key | Coverage |
|---|---|---|
| `owners` | `id` | Active and archived owners, including teams |
| `pipelines` | `objectTypeId`, `id` | Pipelines and nested stages for configured object types |
| `forms` | `id` | Active and archived forms |
| `contact_lists` | `listId` | Contact lists with MANUAL, DYNAMIC, or SNAPSHOT processing type |
| `contact_list_memberships` | `listId`, `recordId` | Current memberships of each discovered contact list |
| `form_submissions` | `formId`, `conversionId` | Submissions for each discovered form |

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
- **How it reads**: the Search API finds objects whose last-modified date changed since the last run (Search does not return history), then the Batch Read API (`POST /crm/objects/2026-09/{object}/batch/read`) fetches `propertiesWithHistory` for those objects, up to 50 IDs per call.
- **Pagination and checkpoints**: Search sorts by numeric `hs_object_id` within a frozen modification-time window. Every page starts a new query with `hs_object_id GT <last hydrated ID>`, avoiding Search's 10,000-result paging window. Checkpoints retain the scan bounds, last ID, completion flag, parent count, and rolling parent-ID digest. Legacy `updatedAt` checkpoints remain readable. Admission can stop after a complete batch of parents, even within a timestamp group.
- **Replay**: A new scan includes the configured lookback before the saved watermark. All selected parents' requested history is emitted, including entries older than that watermark, except where `history_since` explicitly excludes them. CDC keys deduplicate repeated history entries. Failed-batch recovery honors the recorded ending frontier and ignores admission limits. If Search membership changes during recovery, its parent count/digest check fails before returning rows.
- **Rate limiting**: Search calls are paced below HubSpot's Search limit. HTTP429 responses use bounded retries, honoring `Retry-After` or exponential backoff.
- **Partial history responses**: Batch results must have status `COMPLETE`. Missing requested parent IDs are retried, including HTTP200/207 partial responses; unresolved parents fail the read before returning a checkpoint. Records deleted between Search and Batch Read are not silently discarded.

Lookback provides bounded recovery: indexing delays longer than the configured
window can still miss a parent. Search excludes archived objects. The selection
window freezes parent modification times, but Batch Read returns current history,
not a historical snapshot as of that upper bound. Numeric-ID ordering and managed
pipeline recovery require live validation in the target account.

- **Scopes**: the same `crm.objects.<object>.read` scope as the parent object.
- **Known limitation**: changes that do not update the object's last-modified date (some calculated, rollup, or formula properties) are not detected until the object is next modified.

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

Meetings use complete active snapshots by default. HubSpot rejects archived
meeting listing with `VALIDATION_ERROR` for `MEETING_EVENT` (`0-47`). Each read
therefore enumerates the complete current meeting set, even with a saved offset
or record cap. The snapshot consumer reconciles missing IDs after a successful
complete read; there is no `meetings_delete` flow. This preserves current-state
removals without archive timestamps or deleted-meeting history. Meeting reads
remain snapshots when `association_sync_mode=incremental` is supplied.

Active snapshots reject empty pages with continuation, repeated cursors,
malformed records, and duplicate IDs before returning any rows or offset. A
successful empty snapshot represents an empty current set. Pagination is not a
transactional source snapshot: concurrent source changes can still affect what
a complete enumeration observes. Snapshot reconciliation and mixed-object
option isolation require validation through the managed consumer.

**Breaking schema change:** CRM properties with HubSpot type `number` now map
to `DECIMAL(38,18)` instead of `BIGINT`, preserving fractional values. This
applies to every CRM object, including custom objects. Values may use at most
20 integer digits and 18 fractional digits after removing trailing zeros.
Invalid, nonfinite, or out-of-range values fail the read before checkpointing;
they are not rounded or truncated. Property-history values remain strings and
non-CRM resource schemas retain their explicit types.

For an existing pipeline, inspect downstream numeric casts and plan a destination
schema migration before upgrading. Validate a separate destination and fresh
checkpoint first; follow the pipeline's supported schema-change or full-refresh
procedure for the production cutover. Do not assume existing BIGINT columns or
checkpoints migrate automatically. Local source/generated consumer tests cover
exact decimals; managed runtime and Delta serialization remain a required live
validation step.

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
      source_table: deals_property_history
      destination_table: deals_property_history
      connector_options:
        community_connector_options:
          options:
            history_properties: "amount,dealstage"
            max_records_per_batch: "60"
```

Include each external option in the UC connection's allowlist. The connector's
Python interface still receives these values as its `table_options` map.

| Option | Applies to | Default | Description |
|---|---|---|---|
| `max_records_per_batch` | all incremental tables | no cap | Soft limit on records returned per microbatch. History stops after complete parent batches and saves an ID frontier; one parent can exceed the cap. Object reads drain parents sharing the last processed `updatedAt` before stopping. |
| `association_sync_mode` | object tables | `incremental` | `incremental` refreshes selected changed parents. `full_refresh` uses snapshot ingestion and reconciles all current relationships on every update. Add this option to an existing UC connection's external options allowlist before using it. |
| `pipeline_object_types` | `pipelines` | deals, tickets, tasks, discovered custom types | Comma-separated canonical object type IDs. Replaces the default partition inventory; each configured partition must succeed. |
| `history_properties` | `*_property_history` | all properties | Comma-separated property names to fetch history for (e.g. `lifecyclestage,hs_lead_status,dealstage`). Recommended for portals with many properties, because requesting history for every property makes each Batch Read response larger. |
| `include_properties` | CRM object and history tables | all properties | Comma-separated allowlist of properties to ingest. If set, only these discovered properties are read and exposed in the schema (unknown names are ignored, matching HubSpot's behavior). Required for objects with very many properties — full refresh fails with HTTP 414 once the request URL passes ~22,000 characters (objects with 1,000+ properties always exceed this). The object's cursor property is always retained. |
| `exclude_properties` | CRM object and history tables | none | Comma-separated list of properties to drop from ingestion. Applied after `include_properties`. |
| `history_since` | `*_property_history` | none | ISO-8601 timestamp (e.g. `2025-01-01T00:00:00Z`). Only objects modified on or after this time are read, and older history entries are dropped. Caps the initial backfill. |
| `history_lookback_minutes` | `*_property_history` | `10` | Minutes subtracted from the saved watermark when starting a new history scan. Replays late-indexed parents within this window, including their older history entries. A resumed scan retains its original lower bound. Set to `0` to disable the time lookback. |

`include_properties` / `exclude_properties` also apply to `*_property_history` tables (they bound which properties are requested in `propertiesWithHistory`; `history_properties` still narrows further for history reads). Two operational notes:

- Rows ingested before a filter change are not retroactively removed. To rebuild a table so its contents match the new filter, force a re-ingest (below).
- **Forcing a re-ingest:** changing any connector option value on a table — even trivially, e.g. bumping `history_lookback_minutes` by 1 — recreates that table's ingestion flow and re-reads the source from the beginning with the new settings. This is the reliable way to rebuild a table after changing filters: on serverless managed ingestion, a full refresh alone re-processes from the pipeline's stored cursor and may not re-read the source for tables whose options did not change.

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


### Managed per-table option isolation

The CLI repeats one `tableConfigs` JSON string on every explicit HubSpot table.
It maps source table names to their complete connector option dictionaries. This
preserves table-specific filters when native planning shares flat options across
objects. For example:

```json
{"calls":{"include_properties":"hs_call_body"},"notes":{}}
```

With an envelope present, the selected entry is authoritative: `{}` means default
options, and omitted keys never inherit another table's flat options. Missing
entries, duplicate JSON keys, invalid shapes, and undeclared options fail with
sanitized errors. Table names and property names retain their exact spelling.
Only the envelope key accepts case-insensitive spelling for Spark options.
Connection credentials and framework routing options cannot appear inside entries.
`tableConfigs` is a framework connection allowlist option, not a nested HubSpot
option; existing connections must allow it as well as the desired HubSpot options.

The connector validates and retains a constructor envelope before virtual metadata
parses it. This instance-scoped map remains authoritative when adapters extract
an entry. A conflicting replacement map requires a new source instance. Without
a constructor envelope, legacy flat options remain supported and individual SPI
calls can supply their own complete maps. Resolution covers schema, metadata,
reads, deletes, and replay, including history and non-CRM dispatch.

CLI automatic normalization identifies HubSpot by explicit source name or an
exact `lakeflow_community_connectors_hubspot` wheel filename in the final pipeline
dependencies. Renamed/custom packages need explicit HubSpot identification. It
supports explicit tables in the absent/default source schema; schema/report
selectors are rejected because the CLI cannot enumerate their complete map
locally. Other connectors retain their existing behavior.

Local tests do not establish native managed transport: verify that the envelope
and correct table identity reach both planning and execution. The current generic
ingestion-agent dispatcher also loses case-insensitive routing when copying Spark
options: a lowercased `tablename` is rejected as missing `tableName`. Direct agent
calls with canonical routing keys and lowercased `tableconfigs` are covered; this
connector change does not fix generic agent routing.
