# Rootly Source API Research

Research dates: 2026-11-03 (schema), 2026-11-11 (pagination and incremental filters). Sources are Rootly's public API overview, the incident list documentation,
and the published OpenAPI document. No credentials or live customer records were used.

## Authorization

Use `Authorization: Bearer <api_token>` against `https://api.rootly.com/v1` and
`Accept: application/vnd.api+json`. Keys are created through Organization Settings > API Keys.
Global, team, and personal keys have different visibility; use least-privilege read roles covering
the selected tables. Authorization does not imply organization-wide completeness. This connector
uses only GET requests, disallows redirects, and never follows response-provided URLs.

## Object List

| Table | Read endpoint | OpenAPI list schema | Resource type |
|---|---|---|---|
| incidents | GET /v1/incidents | incident_list | incidents |
| services | GET /v1/services | service_list | services |
| users | GET /v1/users | user_list | users |
| teams | GET /v1/teams | team_list | groups |
| severities | GET /v1/severities | severity_list | severities |
| escalation_policies | GET /v1/escalation_policies | escalation_policy_list | escalation_policies |
| schedules | GET /v1/schedules | schedule_list | schedules |

These are the seven supported objects, not an inventory of every Rootly API endpoint. Related
incident events, action items, alerts, shifts, escalation levels and on-call assignments are outside
this initial scope. Static discovery does not verify the key's access to each table.

## Object Schema

All seven list endpoints return a JSON:API envelope containing `data`, `links`, and `meta`.
Each `data` item declares `id` (string), `type` (string), and `attributes` (object).
The users list also declares `relationships`, which are preserved as nested structs.
The [field reference](schema_reference.md) inventories every field declared by each list-response
resource schema, including nested fields and the users endpoint's SCIM-shaped attributes.
The connector yields these resource dictionaries without flattening or stringifying nested values.

`links` and `meta` are paging control data. Top-level `included` resources are not independent rows
and are not loaded; no optional `include` expansions are requested. Open-ended nested objects are
VARIANT so arbitrary JSON values are preserved. Newly added fields outside these open-ended
objects are not automatically added to the Spark schema.

Schema provenance: public OpenAPI SHA256
`f3b255f4b64ad713dac2b04d43803adeea9081ea4dbf9ff2caa5140c8cd93355`.
Regenerate using `tests/unit/sources/rootly/generate_schemas.py` with a downloaded OpenAPI file,
then format and review the changes. Do not use a private or credential-bearing schema export.

## Primary Keys

`id` is the primary key for every table. Use the outer JSON:API ID rather than an ID embedded in
attributes or a display/sequential incident number. Keep string IDs as strings, including users.

## Ingestion Type

Incidents use `cdc`, primary key `id`, and cursor field `updated_at` (a top-level copy of
`attributes.updated_at` in fixed-width UTC, so string ordering is chronological). Each call returns
one page plus a JSON-serializable checkpoint. The connector fixes a UTC whole-second cutoff at
initialization and requests half-open windows using `filter[updated_at][gte]` (when a completed
watermark exists), `filter[updated_at][lt]`, and `sort=updated_at`. The first run has no lower bound.
Cursors paginate within that fixed window, including when several incidents share an update time.
The checkpoint retains the lower/upper bounds, page size and opaque next cursor until the window
is drained, then advances to the upper bound. A completed run returns its input offset unchanged.
See the README for restart, full-refresh and deletion limitations.

The other six tables use `snapshot`, streaming every numbered page and returning no offset.
Their reviewed list contracts do not document equivalent updated-at filters. No delete endpoint
is used. Snapshot disappearance can mean deletion or a permission/visibility change. Incidents do
not propagate deletions; a full refresh is necessary to reconcile the current visible set.

## Read API

The configurable `page_size` is 1–1,000, default 100. The current public overview documents a 1,000
maximum; none of the seven selected endpoint OpenAPI parameter definitions documents a lower cap.
This replaces the original connector's conservative 100 cap. The `/v1/alert_events` exception is
outside this connector's scope.

For the six snapshot tables, send `page[number]=1` and `page[size]`, then follow advancing integer
`meta.next_page` values until null. Incidents start without `page[number]`, require `meta.next_cursor`,
and send each opaque continuation as `page[after]`. Every page retains the same filters and sort.
Do not decode or synthesize production cursors. Missing/invalid cursor metadata, an empty page with
a continuation, and an immediately repeated cursor fail the incident batch without advancing its
checkpoint. Incident rows must have timezone-aware `updated_at` values in the requested window
and in ascending order. Source mutation during pagination can move rows into a later window;
there is no documented point-in-time snapshot guarantee.

Illustrative synthetic response (abbreviated attributes):

```json
{
  "data": [{"id": "synthetic-incident-1", "type": "incidents", "attributes": {"title": "Synthetic incident", "updated_at": "2026-01-01T00:00:00Z"}}],
  "links": {"self": "https://api.rootly.com/v1/incidents?page[number]=1", "next": null},
  "meta": {"current_page": 1, "next_page": null, "prev_page": null, "total_count": 1, "total_pages": 1, "next_cursor": null}
}
```

The overview documents default per-key read limits of 3,000 requests per minute using a sliding
window; deployment configuration can differ. Respect 429 and `Retry-After` (seconds or HTTP date),
with `X-RateLimit-Reset` (epoch seconds) as fallback. Retry 500/502/503/504 and connection/timeouts
with exponential backoff, at most five attempts total. A hint above 120 seconds aborts rather than
retrying early. Other statuses fail immediately. Each request has connect/read timeouts of 10/60
seconds. Error messages include status and endpoint but omit response bodies and credentials.

## Field Mapping

| OpenAPI | Spark |
|---|---|
| string (including dates/timestamps) | StringType |
| integer | LongType |
| boolean | BooleanType |
| object with properties | StructType |
| open-ended object | VariantType |
| array | ArrayType of the mapped element |

Nullable Spark fields tolerate omissions; the API's required fields do not imply every role sees
all attributes. No automatic enum, timestamp, or ID conversions are performed.

## Sources and Remaining Validation

- [Rootly overview](https://docs.rootly.com/api-reference/overview): authentication, roles and rate limits.
- [List incidents](https://docs.rootly.com/api-reference/incidents/list-incidents): incident filters and pagination.
- [Public OpenAPI](https://rootly-heroku.s3.amazonaws.com/swagger/v1/swagger.json): all seven endpoint contracts and schemas.

The November 3 sampled live snapshot run covered all seven endpoints (five records each), including
incident continuations; committed reports are historical evidence for that earlier implementation.
On November 11, targeted live GET checks passed for the initial incremental page, its cursor
continuation with unchanged filters/sort, and the inclusive lower updated-at bound. All seven
endpoints accepted `page[size]=1000`; incidents returned 1,000 rows in one response. These are
sampled transport/contract checks, not full-account or Databricks pipeline validation. Restart,
timestamp ties, later updates and future-record termination are covered synthetically. The
Databricks pipeline smoke test remains pending.
