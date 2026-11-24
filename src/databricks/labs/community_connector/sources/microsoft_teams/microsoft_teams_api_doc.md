# Microsoft Teams (Graph API) — Connector Reference

This document maps each connector table to its Microsoft Graph endpoint(s),
pagination strategy, and rate-limit behavior. The implementation lives in
`microsoft_teams.py`; tables and authentication are documented in `README.md`.

## Authentication

The connector uses the OAuth 2.0 Client Credentials flow against
Microsoft Entra ID (formerly Azure AD):

```
POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
  client_id={client_id}
  client_secret={client_secret}
  grant_type=client_credentials
  scope=https://graph.microsoft.com/.default
```

The resulting Bearer token is sent on each Graph call:

```
Authorization: Bearer <access_token>
```

The application must have these Graph application permissions granted with
admin consent: `Team.ReadBasic.All`, `Channel.ReadBasic.All`,
`ChannelMember.Read.All`, `ChannelMessage.Read.All`,
`Group.Read.All`.

## Base URL

```
https://graph.microsoft.com/v1.0
```

## Rate Limits

- Graph throttles per resource and per app+user; typical channel-message
  reads allow up to 60 requests per app per 30 seconds.
- 429 / 503 responses include a `Retry-After` header. The connector backs
  off, retries the call, and inserts a 100 ms inter-request sleep to stay
  under threshold.

## Tables

| Table             | Endpoint                                                                                  | Primary Key | Cursor Field             | Ingestion |
|-------------------|-------------------------------------------------------------------------------------------|-------------|--------------------------|-----------|
| `teams`           | `GET /v1.0/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')`                 | `id`        | —                        | snapshot  |
| `channels`        | `GET /v1.0/teams/{team_id}/channels`                                                      | `id`        | —                        | snapshot  |
| `messages`        | `GET /v1.0/teams/{team_id}/channels/{channel_id}/messages` (delta: `.../messages/delta`)  | `id`        | `lastModifiedDateTime`   | cdc       |
| `members`         | `GET /v1.0/teams/{team_id}/members`                                                       | `id`        | —                        | snapshot  |
| `message_replies` | `GET /v1.0/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies` (delta: `.../replies/delta`) | `id` | `lastModifiedDateTime`   | cdc       |

## Pagination

- **Snapshot tables**: standard `@odata.nextLink` chasing until null.
- **CDC tables (messages, message_replies)**: Graph delta query.
  - First call: `GET .../messages/delta` returns a page with `@odata.nextLink`
    or `@odata.deltaLink`.
  - Subsequent calls: follow `@odata.nextLink` until the response includes
    `@odata.deltaLink` (terminal token).
  - Resume across microbatches: store the `@odata.deltaLink` and use it as
    the next call's URL.

## Termination

Static snapshot tables terminate naturally when pagination ends. Delta-based
CDC tables converge when the response yields a `@odata.deltaLink` and no
further changes were observed. The connector also caps cursors at
`_init_time` for legacy CDC fallback paths to ensure
`Trigger.AvailableNow` termination on active sources.

## Admission control

The connector reads `max_records_per_batch` from `table_options` on the
delta-based tables and stops fetching new pages once the cap is hit,
preserving the current `@odata.nextLink` (or `@odata.deltaLink`) so the
next microbatch resumes cleanly.

## References

- Graph teams: https://learn.microsoft.com/graph/api/resources/team
- Graph channels: https://learn.microsoft.com/graph/api/resources/channel
- Graph channel messages: https://learn.microsoft.com/graph/api/resources/chatmessage
- Delta query: https://learn.microsoft.com/graph/delta-query-overview
- Throttling: https://learn.microsoft.com/graph/throttling
