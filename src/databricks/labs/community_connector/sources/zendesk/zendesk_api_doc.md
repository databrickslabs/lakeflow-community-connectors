# Zendesk API — Connector Reference

This document maps each connector table to its Zendesk REST API endpoint(s),
pagination strategy, and rate-limit behavior. The implementation lives in
`zendesk.py`; tables and authentication are documented in `README.md`.

## Authentication

The connector uses Zendesk API Token authentication (HTTP Basic):

```
Authorization: Basic base64("<email>/token:<api_token>")
Content-Type: application/json
```

API tokens are managed under **Admin Center → Apps and integrations → APIs →
Zendesk API → Settings → Token Access**.

## Base URL

```
https://<subdomain>.zendesk.com/api/v2
```

## Rate Limits

- 700 requests per minute on Suite plans (typical).
- 200 requests per minute on lower-tier plans.
- 429 responses include a `Retry-After` header and `X-Rate-Limit` headers.
- Incremental Exports are subject to a **separate** 10-requests-per-minute
  limit and should not be polled aggressively.

## Tables

### Incremental tables (cursor-based on `end_time`)

These tables use Zendesk's
[Incremental Exports API](https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/):
each response contains a `end_time` cursor and `end_of_stream` flag. The
connector resumes via the cursor across microbatches.

| Table             | Endpoint                                        | Response key       | Primary Key | Cursor Field |
|-------------------|-------------------------------------------------|--------------------|-------------|--------------|
| `tickets`         | `GET /api/v2/incremental/tickets.json`          | `tickets`          | `id`        | `end_time`   |
| `organizations`   | `GET /api/v2/incremental/organizations.json`    | `organizations`    | `id`        | `end_time`   |
| `users`           | `GET /api/v2/incremental/users.json`            | `users`            | `id`        | `end_time`   |
| `ticket_comments` | `GET /api/v2/incremental/ticket_events.json`    | `ticket_events`    | `id`        | `end_time`   |

The cursor (`end_time`) is capped at the connector's `_init_time` so a single
`Trigger.AvailableNow` trigger drains only the data that existed at trigger
start. Without this cap, Zendesk's continuously-arriving update stream would
prevent termination.

### Snapshot tables (paginated, full refresh per microbatch)

These tables do not expose an incremental endpoint, so each microbatch reads
all pages. The connector returns a `{"done": true}` sentinel offset after the
first call within a trigger so subsequent calls return immediately.

| Table      | Endpoint                                  | Response key | Primary Key |
|------------|-------------------------------------------|--------------|-------------|
| `articles` | `GET /api/v2/help_center/articles.json`   | `articles`   | `id`        |
| `brands`   | `GET /api/v2/brands.json`                 | `brands`     | `id`        |
| `groups`   | `GET /api/v2/groups.json`                 | `groups`     | `id`        |
| `topics`   | `GET /api/v2/community/topics.json`       | `topics`     | `id`        |

## Pagination

- **Incremental**: response includes `end_time` (Unix epoch seconds) and
  `end_of_stream` (boolean). Use `?start_time=<end_time>` for the next page;
  stop when `end_of_stream == true`.
- **Snapshot**: standard offset pagination via `?page=N&per_page=100`. Stop
  when the response returns 404 or fewer than 100 records.

## Admission control

The connector reads `max_records_per_batch` from `table_options` and stops
fetching pages once the cap is hit, returning the partial cursor for resume
on the next microbatch. Default cap: 100,000 records per batch.

## References

- Zendesk REST API: https://developer.zendesk.com/api-reference/
- Incremental Exports: https://developer.zendesk.com/api-reference/ticketing/ticket-management/incremental_exports/
- Help Center API: https://developer.zendesk.com/api-reference/help_center/help-center-api/articles/
- Rate limits: https://developer.zendesk.com/api-reference/introduction/rate-limits/
