# Mixpanel API — Connector Reference

This document maps each connector table to its Mixpanel API endpoint(s),
pagination strategy, and rate-limit behavior. The implementation lives in
`mixpanel.py`; tables and authentication are documented in `README.md`.

## Authentication

The connector supports two authentication modes:

1. **Service account** (Basic): `Authorization: Basic base64("<username>:<secret>")`
2. **Project secret** (Basic): `Authorization: Basic base64("<api_secret>:")`

Service account is recommended; project secrets are still accepted on legacy
projects. Both require a `project_id` for non-export endpoints.

## Base URLs

- **US**: `https://data.mixpanel.com` (export) and `https://mixpanel.com` (query)
- **EU**: `https://data-eu.mixpanel.com` and `https://eu.mixpanel.com`

The connector picks the region from the `region` connection parameter
(`us` or `eu`).

## Rate Limits

- 3 requests per second per project on most endpoints (Mixpanel-side throttle).
- Raw export endpoint additionally limits to 60 concurrent queries per project.
- 429 responses include a `Retry-After` header — the connector backs off
  exponentially.

## Tables

| Table            | Endpoint                                       | Primary Key                       | Cursor Field             | Ingestion        |
|------------------|------------------------------------------------|-----------------------------------|--------------------------|------------------|
| `events`         | `GET /api/2.0/export`                          | `$insert_id`                      | `properties.time`        | `cdc`            |
| `cohorts`        | `POST /api/query/cohorts/list`                 | `id`                              | —                        | `snapshot`       |
| `cohort_members` | `POST /api/query/engage` (with `filter_by_cohort_id`) | `[cohort_id, distinct_id]` | —                        | `snapshot`       |
| `engage`         | `POST /api/query/engage`                       | `$distinct_id`                    | `$properties.$last_seen` | `cdc`            |

### `events`

The Raw Export endpoint streams JSON-lines. The connector requests fixed-width
date windows (default 7 days) bounded by `_init_date` so a single
`Trigger.AvailableNow` trigger only drains events that existed at trigger
start. The cursor is `properties.time` (Unix epoch seconds in the event body).

### `cohorts`

Returns the full cohort list per call. Snapshot ingestion — every microbatch
re-reads. Small dataset; no admission control needed.

### `cohort_members`

For each cohort, calls `engage` with `filter_by_cohort_id=<cohort_id>` and
yields `(cohort_id, $distinct_id)` pairs. Snapshot per microbatch.

### `engage`

User profile updates. Cursor is `$properties.$last_seen` (Unix epoch seconds).
The connector caps the cursor at `_init_ts` so reads converge under
`Trigger.AvailableNow`.

## Pagination

- **Export**: streamed JSON-lines; the connector consumes the entire response
  body for the requested date window.
- **Engage**: response includes `session_id` and `page` for forward
  pagination; the connector follows them until either the page is empty or
  `max_records_per_batch` is reached.
- **Cohorts**: single response, no pagination.

## Admission control

The connector reads `max_records_per_batch` from `table_options` and stops
yielding records once the cap is hit, returning a partial cursor for resume
on the next microbatch.

## References

- Mixpanel Raw Export: https://developer.mixpanel.com/reference/raw-event-export
- Mixpanel Engage: https://developer.mixpanel.com/reference/engage-query
- Mixpanel Cohorts: https://developer.mixpanel.com/reference/cohorts-list
- Authentication: https://developer.mixpanel.com/reference/service-accounts
- Rate limits: https://developer.mixpanel.com/reference/query-api#rate-limits
