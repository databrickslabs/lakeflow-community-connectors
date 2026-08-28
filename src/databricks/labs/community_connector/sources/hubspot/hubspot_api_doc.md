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

## References

- HubSpot CRM v3 API: https://developers.hubspot.com/docs/api/crm
- Search API: https://developers.hubspot.com/docs/api/crm/search
- Properties API: https://developers.hubspot.com/docs/api/crm/properties
- Rate limits: https://developers.hubspot.com/docs/api/usage-details
