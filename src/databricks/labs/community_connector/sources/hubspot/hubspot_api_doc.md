# HubSpot API — Connector Reference

This document maps each connector table to its underlying HubSpot CRM v3 API
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

- 100 requests per 10 seconds per token (default).
- 250,000 requests per day per token (free / starter; varies by plan).
- 429 responses include a `Retry-After` header — the connector respects this
  via exponential backoff.

## Pagination

HubSpot CRM v3 paginates with an opaque `paging.next.after` cursor. The
connector follows it until either:
1. The endpoint returns no `paging.next.after`, or
2. The configured `max_records_per_batch` (admission control) is reached, in
   which case the partial cursor is returned and the next microbatch resumes.

## Tables

| Table        | Endpoint (full refresh)                  | Endpoint (incremental search)                  | Primary Key | Cursor Field |
|--------------|------------------------------------------|------------------------------------------------|-------------|--------------|
| `contacts`   | `GET /crm/v3/objects/contacts`           | `POST /crm/v3/objects/contacts/search`         | `id`        | `updatedAt`  |
| `companies`  | `GET /crm/v3/objects/companies`          | `POST /crm/v3/objects/companies/search`        | `id`        | `updatedAt`  |
| `deals`      | `GET /crm/v3/objects/deals`              | `POST /crm/v3/objects/deals/search`            | `id`        | `updatedAt`  |
| `tickets`    | `GET /crm/v3/objects/tickets`            | `POST /crm/v3/objects/tickets/search`          | `id`        | `updatedAt`  |
| `calls`      | `GET /crm/v3/objects/calls`              | `POST /crm/v3/objects/calls/search`            | `id`        | `updatedAt`  |
| `emails`     | `GET /crm/v3/objects/emails`             | `POST /crm/v3/objects/emails/search`           | `id`        | `updatedAt`  |
| `meetings`   | `GET /crm/v3/objects/meetings`           | `POST /crm/v3/objects/meetings/search`         | `id`        | `updatedAt`  |
| `tasks`      | `GET /crm/v3/objects/tasks`              | `POST /crm/v3/objects/tasks/search`            | `id`        | `updatedAt`  |
| `notes`      | `GET /crm/v3/objects/notes`              | `POST /crm/v3/objects/notes/search`            | `id`        | `updatedAt`  |

Custom objects discovered via `GET /crm/v3/schemas` are exposed as additional
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

- `contacts`, `companies`, `deals`, `tickets`, `emails`, `tasks`, `notes`

`calls` and `meetings` do not support archived queries and are append-only
incremental.

## Schema discovery

Each table's column set is discovered at runtime via:

```
GET /properties/v2/{type}/properties
```

The connector caches the schema per `HubspotLakeflowConnect` instance.

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

## Property history (`{object}_property_history`)

HubSpot returns a property's previous values when it is listed in
`propertiesWithHistory`. Which endpoints accept it (verified against
HubSpot's OpenAPI specs for the CRM objects API):

| Endpoint | `propertiesWithHistory` | Notes |
|---|---|---|
| `GET /crm/v3/objects/{type}/{id}` | yes (query) | one object |
| `GET /crm/v3/objects/{type}` (list) | yes (query) | "will reduce the maximum number of [objects] that can be read by a single request"; long property lists hit URL-length limits |
| `POST /crm/v3/objects/{type}/batch/read` | yes (body) | up to 100 IDs; may return `207` with per-ID errors |
| `POST /crm/v3/objects/{type}/search` | **no** | request schema is `after`, `filterGroups`, `limit`, `properties`, `query`, `sorts` |

Each history entry (`ValueWithTimestamp`) has `value`, `timestamp`,
`sourceType` (required) and `sourceId`, `sourceLabel`, `updatedByUserId`
(optional).

### Read pattern

1. `POST /crm/v3/objects/{type}/search` with `GTE <cursor - lookback>` and
   `LTE <init_ts>` on the cursor property, ascending, `limit: 200`, returning
   only IDs and `updatedAt`.
2. `POST /crm/v3/objects/{type}/batch/read` with up to 50 IDs and
   `propertiesWithHistory` (all properties from the Properties API, or the
   `history_properties` table option).
3. One output row per history entry; entries older than the query's lower
   bound (or `history_since`) are dropped.

Offset: `{"updatedAt": <max parent updatedAt processed>}`. The lookback is
applied once per connector instance (one pipeline update), not stored in the
offset.

### Limits that shape the implementation

- Search: **5 requests/second per account**, max 200 results per page, max
  **10,000 results per query** (paging past it returns 400). The reader spaces
  search calls 250 ms apart and ends a read before the cap, re-anchoring the
  next search at the latest `updatedAt`.
- General burst limits for privately distributed apps: 100 requests / 10 s
  (Free/Starter), 190 / 10 s (Professional/Enterprise), 250 / 10 s with the API
  limit increase. Daily: 250k / 625k / 1M per account.
- 429 responses: retried up to 5 times, honouring `Retry-After` when present,
  else exponential backoff (1, 2, 4, 8, 16 s; capped at 30 s). Retry applies to
  the property history tables only.

### Known gaps (to confirm in live testing)

- HubSpot does not document the per-request cap for batch read *with* history;
  the reader uses 50 IDs per call.
- Calculated / rollup properties may change without updating the object's
  last-modified date (Airbyte documents this as cursor drift); such changes are
  not picked up until the object is next modified.

## References

- HubSpot CRM v3 API: https://developers.hubspot.com/docs/api/crm
- Search API: https://developers.hubspot.com/docs/api/crm/search
- Properties API: https://developers.hubspot.com/docs/api/crm/properties
- Rate limits: https://developers.hubspot.com/docs/api/usage-details
- Search limits: https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm#limits
- Batch read (contacts example, same shape for all CRM objects): https://developers.hubspot.com/docs/api-reference/legacy/crm/objects/contacts/batch/get-contacts
- Usage guidelines (current limits table): https://developers.hubspot.com/docs/developer-tooling/platform/usage-guidelines
