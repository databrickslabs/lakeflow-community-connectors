# HubSpot API — Connector Reference

This document maps each connector table to its underlying HubSpot API
endpoint(s), pagination strategy, and rate-limit behavior. The implementation
lives in `hubspot.py`; tables and authentication are documented in `README.md`.

## Authentication

All endpoints use HubSpot Private App tokens, sent as a Bearer token:

```
Authorization: Bearer <access_token>
Content-Type: application/json
```

Private apps must be granted the appropriate CRM scopes for each object type
(`crm.objects.contacts.read`, `crm.objects.companies.read`, etc.).

## Base URL

```
https://api.hubapi.com
```

## Rate Limits

HubSpot limits vary by endpoint family, subscription, and private-app entitlement.
Search has its own limits and a 10,000-result query window. The connector retries
safe GET and read-only POST requests after HTTP429/500/502/503 or request
exceptions, with separate request pacing. Do not treat configured pacing as a
guaranteed account-wide budget when multiple pipelines share a token.

## Pagination

HubSpot paginates with an opaque `paging.next.after` cursor. The connector
follows it until the endpoint returns no continuation. Incremental Search reads
may stop after a complete `updatedAt` group when `max_records_per_batch` is set;
the next microbatch resumes from the timestamp checkpoint.

Snapshot ingestion, including `association_sync_mode=full_refresh`, drains every
object-list page in one read and ignores `max_records_per_batch` so the batch
reader cannot publish a truncated authoritative set.

## Tables

| Table        | Endpoint (full refresh)                  | Endpoint (incremental search)                  | Primary Key | Cursor Field |
|--------------|------------------------------------------|------------------------------------------------|-------------|--------------|
| `contacts`   | `GET /crm/objects/2026-09/contacts`           | `POST /crm/objects/2026-09/contacts/search`         | `id`        | `updatedAt`  |
| `companies`  | `GET /crm/objects/2026-09/companies`          | `POST /crm/objects/2026-09/companies/search`        | `id`        | `updatedAt`  |
| `deals`      | `GET /crm/objects/2026-09/deals`              | `POST /crm/objects/2026-09/deals/search`            | `id`        | `updatedAt`  |
| `tickets`    | `GET /crm/objects/2026-09/tickets`            | `POST /crm/objects/2026-09/tickets/search`          | `id`        | `updatedAt`  |
| `calls`      | `GET /crm/objects/2026-09/calls`              | `POST /crm/objects/2026-09/calls/search`            | `id`        | `updatedAt`  |
| `emails`     | `GET /crm/objects/2026-09/emails`             | `POST /crm/objects/2026-09/emails/search`           | `id`        | `updatedAt`  |
| `meetings`   | `GET /crm/objects/2026-09/meetings`           | `POST /crm/objects/2026-09/meetings/search`         | `id`        | `updatedAt`  |
| `tasks`      | `GET /crm/objects/2026-09/tasks`              | `POST /crm/objects/2026-09/tasks/search`            | `id`        | `updatedAt`  |
| `notes`      | `GET /crm/objects/2026-09/notes`              | `POST /crm/objects/2026-09/notes/search`            | `id`        | `updatedAt`  |

Custom objects discovered via `GET /crm-object-schemas/2026-09/schemas` are exposed as additional
tables using the same patterns as the standard objects above.

### Cursor property mapping

The search API filters on the property-level cursor field, which differs from
the response-level `updatedAt`:

| Table       | Property cursor field   |
|-------------|-------------------------|
| `contacts`  | `lastmodifieddate`      |
| All others  | `hs_lastmodifieddate`   |

### Deletes (CDC)

HubSpot supports `archived=true` queries on the following tables. The
connector emits these via `read_table_deletes`:

- `contacts`, `companies`, `deals`, `tickets`, `calls`, `emails`, `meetings`, `tasks`, `notes`

The connector drains every archived page before committing the frozen archive-time
window. Archive listing is not ordered by archive time, so this scan ignores
`max_records_per_batch`. Invalid records, conflicting duplicate IDs, malformed
paging, and later-page failures fail the read before publishing a checkpoint.
Archive reads cover records retained and returned by HubSpot; they do not detect
hard deletes or records outside the source retention window.

## Schema discovery

Each table's column set is discovered at runtime via:

```
GET /crm/properties/2026-09/{type}
```

The connector caches the schema per `HubspotLakeflowConnect` instance.

Property responses wrap fields in `results`. Custom schema discovery uses
`GET /crm-object-schemas/2026-09/schemas`; returned directional association
definitions resolve columns by canonical objectTypeId after collecting all schemas.

HubSpot `number` properties use the existing `BIGINT` schema by default. The
per-table `numeric_value_mode=decimal` option selects `DECIMAL(38,18)` for a new
destination and normalizes exact long binary64 expansions before serialization.

## Complete association reads

`POST /crm/associations/2026-09/{fromType}/{toType}/batch/read` receives
`inputs: [{id}]`. Each result has `from.id`, `to[].toObjectId` and optional
per-parent `paging.next.after`. Continue with `{id, after}` for that parent.
The connector drains all pages and reconciles every requested parent before
publishing rows. HTTP207 is not sufficient evidence of a complete result.

Association-only changes are not guaranteed to advance a searched parent cursor.
The optional `association_sync_mode=full_refresh` returns snapshot metadata and
performs a complete parent/association scan each update; the default incremental
mode covers only parents selected by Search.

## Search API request shape

Incremental reads use the search API with a filter on the property-level
cursor field. Example request body for `contacts`:

```json
{
  "filterGroups": [{
    "filters": [{
      "propertyName": "lastmodifieddate",
      "operator": "BETWEEN",
      "highValue": "<init_ts>",
      "value": "<cursor>"
    }]
  }],
  "sorts": [{"propertyName": "lastmodifieddate", "direction": "ASCENDING"}],
  "limit": 100,
  "after": "<paging cursor>"
}
```

The high-value bound (`<init_ts>`) is captured once per connector instance to
guarantee `Trigger.AvailableNow` termination — without it, the read window
chases incoming updates forever.

## Property filtering (`include_properties` / `exclude_properties`)

By default CRM object tables discover and request **all** properties. Two table
options bound that set:

- `include_properties` — comma-separated allowlist; unknown names are ignored
  (HubSpot itself ignores unrecognised property names).
- `exclude_properties` — removed after `include_properties` is applied.

The object's cursor property (`lastmodifieddate` / `hs_lastmodifieddate`) is
always retained so incremental reads keep working. Changing the options
changes the schema and future ingestion; previously ingested rows remain
until the table is re-ingested. To force a re-ingest after changing options,
change any connector option value on the table (even trivially) — this
recreates the table's ingestion flow and re-reads the source from the
beginning. On serverless managed ingestion, a full refresh alone resumes
from the pipeline's stored cursor and may not re-read the source for tables
whose options did not change.

**Why this matters — verified against a live HubSpot portal (2026-10-07):**

- **Full refresh (list endpoint)** takes `properties=` in the URL. Requests fail
  with **HTTP 414 Request-URI Too Large** once the URL passes roughly **22,000
  characters** (~630 property parameters). Objects with 1,000+ properties
  produce ~35,000-character URLs — full refresh fails on them, always.
- **Incremental (Search API)** accepts large `properties` bodies (verified
  HTTP 200 at ~29,700 characters / 1,099 entries); HubSpot's documented
  3,000-character "query" limit did not reject the properties array in
  testing. Still, requesting every property inflates every page of every
  response.

Use `include_properties` / `exclude_properties` to bound wide objects to the
properties you need — for 1,000+-property objects it is currently the only
way to ingest them at all. Chunking the all-properties path across multiple
requests (Airbyte-style) is a possible follow-up.

## References

- HubSpot CRM v3 API: https://developers.hubspot.com/docs/api/crm
- Search API: https://developers.hubspot.com/docs/api/crm/search
- Properties API: https://developers.hubspot.com/docs/api/crm/properties
- Rate limits: https://developers.hubspot.com/docs/api/usage-details
- Search limits: https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm#limits
- Batch read (contacts example, same shape for all CRM objects): https://developers.hubspot.com/docs/api-reference/legacy/crm/objects/contacts/batch/get-contacts
- Usage guidelines (current limits table): https://developers.hubspot.com/docs/developer-tooling/platform/usage-guidelines

## Resource snapshot endpoints

| Table | Endpoint | Pagination |
|---|---|---|
| `owners` | `GET /crm/owners/2026-09` | `paging.next.after`, active and archived partitions |
| `pipelines` | `GET /crm/pipelines/2026-09/{objectTypeId}` | `paging.next.after`, every configured object partition |
| `forms` | `GET /marketing/v3/forms` | `paging.next.after`, active and archived partitions, `formTypes=all` |
| `contact_lists` | `POST /crm/lists/2026-09/search`, `GET /contacts/v1/lists`, `POST /crm/lists/2026-09/idmapping` | Search provides the current `listId`; the legacy catalog and ID-mapping endpoint add nullable `legacyListId`. While search `hasMore` is true, pass its top-level integer `offset` in the next request body. |
| `contact_list_memberships` | `GET /crm/lists/2026-09/{listId}/memberships` | `paging.next.after` for each discovered contact list |
| `form_submissions` | `GET /form-integrations/v1/submissions/forms/{formId}` | `paging.next.after` for each discovered form |

Forms intentionally use the supported marketing v3 and submissions v1 endpoints.
The `2027-03-beta` Forms API is Beta and is not selected by this connector.
Lists are restricted to contact object type `0-1`; a list still processing fails
membership publication. All resource tables are full snapshots with no saved
incremental cursor. Failed pages or partitions abort publication of that snapshot.
Implementation: `hubspot_non_crm.py`. See README for keys, scopes, and pipeline
partition selection. Current live validation may require additional account scopes
and fixtures; offline fixtures alone do not establish production completeness.
