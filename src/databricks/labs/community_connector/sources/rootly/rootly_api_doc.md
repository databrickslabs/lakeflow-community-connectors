# Rootly Source API Research

Research date: 2026-11-03. Sources are Rootly's public API overview, the incident list documentation,
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

All tables use `snapshot`. Each read streams all pages and returns no incremental offset.
Full refreshes capture the latest visible mutable resources without treating creation times as
update cursors. Incidents document `filter[updated_at][gte]` and sorting, but incremental incident
ingestion and delete tracking are not implemented in this first version. The other selected lists
do not document an equivalent updated-at filter in the reviewed specification.

No separate deletion endpoint is used. Snapshot processing reflects the visible set according to
the pipeline's snapshot/SCD semantics; disappearance can also mean a permission or visibility change.
Source changes during pagination can cause inconsistencies because the API offers no documented
point-in-time token for these reads. A subsequent full refresh is required to reconcile changes.

## Read API

Send `page[number]=1` and `page[size]=100` initially (configurable page size 1–100). All selected
endpoints declare these parameters. Follow integer `meta.next_page` until null. Require advancing
page numbers; fail if pagination metadata or resource envelopes are malformed.

Incidents also document `page[after]` with opaque `meta.next_cursor`. If a cursor is returned,
switch to `page[after]`, omit `page[number]`, and follow cursors until `next_cursor` is null.
Never decode or synthesize production cursors. Repeated cursors and an empty page with a
continuation fail the refresh. Numbered pagination remains available when no cursor is returned.
The simulator models these rules from documentation; live pagination compatibility is unverified.

The OpenAPI metadata shape declares `current_page`, `next_page`, `prev_page`, `total_count`, and
`total_pages`; `next_cursor` is optional. Page fields may be null during cursor pagination.
The API's maximum accepted page size is not specified in the reviewed OpenAPI parameter schemas;
100 is a conservative connector cap, not a documented server maximum.

Illustrative synthetic response (abbreviated attributes):

```json
{
  "data": [{"id": "synthetic-incident-1", "type": "incidents", "attributes": {"title": "Synthetic incident"}}],
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

The docs and OpenAPI agree on the API base, bearer authentication and incident paging parameter
names. The six non-incident contracts are sourced from the OpenAPI; independent live verification
is still needed. Synthetic tests cannot confirm actual optional fields, permission behavior, rate
limit headers, cursor termination or Databricks runtime behavior. Record-mode tests with a scoped
test account, schema comparison and a Databricks smoke test remain prerequisites for readiness.
