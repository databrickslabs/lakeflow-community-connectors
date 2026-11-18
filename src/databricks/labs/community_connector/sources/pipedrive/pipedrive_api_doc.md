# **Pipedrive API Documentation**

Scope: read-only ingestion of 25 tables: `deals`, `deals_archived`, `persons`, `organizations`, `activities`,
`products`, `pipelines`, `stages`, `users`, `notes`, `leads`, `leads_archived`, `lead_labels`, `files`,
`projects`, `tasks`, `activity_types`, `currencies`, `deal_fields`, `person_fields`, `organization_fields`,
`product_fields`, `activity_fields`, `deal_products`, `deal_flow`.

Pipedrive exposes two API generations on the same host:

- **API v2** (`/api/v2/...`): cursor pagination, RFC 3339 timestamps, custom fields nested under
  `custom_fields`, server-side `updated_since` / `updated_until` filters and `sort_by=update_time` on the core
  entities. Pipedrive recommends v2 "where available". The v1 list endpoints for deals, persons, organizations,
  activities, products, pipelines and stages are no longer in the official v1 OpenAPI spec (they were replaced
  by v2), so **v2 is mandatory for those tables**.
- **API v1** (`/v1/...`): offset pagination (`start` / `limit`), used for the tables that have no v2 list
  endpoint (users, notes, leads, lead labels, files, activity types, currencies, deal flow) and for the
  `*_fields` metadata tables (see the field-table note below).

---

## **Authorization**

**Chosen method: API token, sent in the `x-api-token` request header.**

- Every Pipedrive user has one personal API token (Pipedrive UI: avatar (top right) > *Company settings* >
  *Personal preferences* > *API*). Only one token is active per user and company; regenerating it invalidates
  the old one. The token gives access to the data that user can see.
- Header: `x-api-token: <api_token>`. This is the only API-token transport the current authentication page
  documents. The legacy `?api_token=` query parameter is no longer documented and would put the secret in URLs,
  so the connector must **not** use it.
- Base URL: `https://api.pipedrive.com` (the server in both official OpenAPI specs: v1 server
  `https://api.pipedrive.com/v1`, v2 server `https://api.pipedrive.com/api/v2`). The authentication page's example
  uses the company host `https://{companydomain}.pipedrive.com/api/v2/...`, and that host also works. The connector
  should default to `https://api.pipedrive.com` and accept an optional `company_domain` override.
- Path prefixes on the shared host: v1 = `/v1/<resource>`, v2 = `/api/v2/<resource>`.

Connection parameters:

| Parameter | Required | Description |
|---|---|---|
| `api_token` | yes | Personal API token (secret). |
| `company_domain` | no | Company subdomain, e.g. `mycompany`. If set, requests go to `https://mycompany.pipedrive.com`. Default host: `https://api.pipedrive.com`. |
| `start_date` | no | Lower bound for the first incremental read (RFC 3339). Defaults to `2010-01-01T00:00:00Z` (Airbyte default). |

Example authenticated requests:

```bash
# v2
curl -s "https://api.pipedrive.com/api/v2/deals?limit=1" \
  -H "x-api-token: $PIPEDRIVE_API_TOKEN"

# v1
curl -s "https://api.pipedrive.com/v1/users" \
  -H "x-api-token: $PIPEDRIVE_API_TOKEN"
```

Alternatives (not used, see Known Quirks): OAuth 2.0 (Marketplace apps). OAuth requests must go to the company
host returned as `api_domain` in the token response, and access tokens are refreshed at
`https://oauth.pipedrive.com/oauth/token` using HTTP Basic `client_id:client_secret`. If OAuth is added later, the
connector stores `client_id`, `client_secret` and `refresh_token`, exchanges them for an access token at runtime,
and sends `Authorization: Bearer <access_token>`. It never runs the user-facing consent flow.

Auth-related error codes:

| HTTP | Meaning | Connector action |
|---|---|---|
| 401 | Invalid / revoked token | Fail (config error). |
| 402 | Company account inactive (trial expired / billing) | Fail (config error). |
| 403 | No permission, plan doesn't include feature, **or** Cloudflare block after repeated 429s (HTML body) | Fail, except where a table is optional (projects/tasks: see below). |
| 415 | Feature not enabled for company | Fail, or skip for optional tables. |
| 429 | Rate limit / daily budget exhausted | Back off and retry. |

---

## **Object List**

The object list is **static**: Pipedrive has no API that enumerates its resource types. The table below gives the
complete list in scope.

| Table | API | Endpoint | Parent / layering | Notes |
|---|---|---|---|---|
| `deals` | v2 | `GET /api/v2/deals` | top-level | Not-archived deals only. |
| `deals_archived` | v2 | `GET /api/v2/deals/archived` | top-level | Archived deals only, same record shape as `deals`. |
| `persons` | v2 | `GET /api/v2/persons` | top-level | |
| `organizations` | v2 | `GET /api/v2/organizations` | top-level | |
| `activities` | v2 | `GET /api/v2/activities` | top-level | |
| `products` | v2 | `GET /api/v2/products` | top-level | |
| `pipelines` | v2 | `GET /api/v2/pipelines` | top-level | |
| `stages` | v2 | `GET /api/v2/stages` | top-level (each stage references `pipeline_id`) | Omitting `pipeline_id` returns stages of all pipelines. |
| `users` | v1 | `GET /v1/users` | top-level | Not paginated. |
| `notes` | v1 | `GET /v1/notes` | top-level | |
| `leads` | v1 | `GET /v1/leads` | top-level | Not-archived leads only. |
| `leads_archived` | v1 | `GET /v1/leads/archived` | top-level | Archived leads only, same record shape as `leads`. |
| `lead_labels` | v1 | `GET /v1/leadLabels` | top-level | Not paginated. |
| `files` | v1 | `GET /v1/files` | top-level | Max page size 100. |
| `projects` | v2 | `GET /api/v2/projects` **and** `GET /api/v2/projects/archived` | top-level, two endpoints unioned | Requires the Projects add-on. |
| `tasks` | v2 | `GET /api/v2/tasks` | top-level (each task references `project_id`) | Requires the Projects add-on. Tagged *Beta* in the spec. |
| `activity_types` | v1 | `GET /v1/activityTypes` | top-level | Not paginated. |
| `currencies` | v1 | `GET /v1/currencies` | top-level | Not paginated. |
| `deal_fields` | v1 | `GET /v1/dealFields` | metadata | |
| `person_fields` | v1 | `GET /v1/personFields` | metadata | |
| `organization_fields` | v1 | `GET /v1/organizationFields` | metadata | |
| `product_fields` | v1 | `GET /v1/productFields` | metadata | |
| `activity_fields` | v1 | `GET /v1/activityFields` | metadata | |
| `deal_products` | v2 | `GET /api/v2/deals/products?deal_ids=...` | **child of deals**: needs ≤100 deal IDs per request, taken from `deals` + `deals_archived` | |
| `deal_flow` | v1 | `GET /v1/deals/{id}/flow?items=dealChange&all_changes=1` | **child of deals**: one call per deal, IDs from `deals` + `deals_archived` | Change log of deal field updates. |

Note on field tables: v2 also has `GET /api/v2/{deal,person,organization,product,activity}Fields` (cursor
paginated, keyed by `field_code`). It omits `id`, `add_time`, `update_time`, `active_flag` and the other v1 flags.
v1 field endpoints are **not** marked deprecated and are what Airbyte reads (PK `key`), so v1 is the documented
choice here. The v2 variant is an alternative if v1 is ever retired.

---

## **Object Schema**

There is no schema-discovery API for entity tables. Schemas are static and listed below. They come from the
official OpenAPI specs (`openapi.yaml` for v1, `openapi-v2.yaml` for v2) and were cross-checked against Airbyte's
`manifest.yaml` schemas.

**Custom fields**: deals, persons, organizations, products and projects (v2) return custom field values in a
`custom_fields` object keyed by a 40-character hash. Leads (v1) inherit **deal** custom fields and return them as
**root-level** hash keys, present only when a value is set. The field-metadata tables (`*_fields`) map each hash
`key` to its name and type. Recommended: store `custom_fields` as a JSON string / map column, not one column per
custom field. For leads, collect any unknown 40-hex root key into a `custom_fields` map column the same way.

Conventions used below: `int` = 64-bit integer, `num` = double, `str` = string, `bool` = boolean,
`ts` = timestamp, `date` = date, `[x]` = array of x, `{...}` = struct, `json` = free-form object (store as
string/map). All fields are nullable unless stated otherwise. API responses omit or null many fields.

### deals / deals_archived (v2, identical shape)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| title | str | |
| creator_user_id | int | In example payload; not in the response schema block |
| owner_id | int | |
| person_id | int | |
| org_id | int | |
| pipeline_id | int | |
| stage_id | int | |
| value | num | |
| currency | str | |
| add_time | ts | RFC 3339 |
| update_time | ts | **cursor** |
| stage_change_time | ts | |
| is_deleted | bool | Soft-delete flag (see ingestion type) |
| is_archived | bool | |
| archive_time | ts | In example payload |
| status | str | `open`, `won`, `lost`, `deleted` |
| probability | num | |
| lost_reason | str | |
| visible_to | int | 1, 3, 5, 7 |
| close_time | ts | |
| won_time | ts | |
| lost_time | ts | |
| local_won_date | date | In example payload |
| local_lost_date | date | In example payload |
| local_close_date | date | In example payload |
| expected_close_date | date | |
| label_ids | [int] | |
| origin | str | Set by Pipedrive at creation |
| origin_id | str | |
| channel | int | |
| channel_id | str | |
| source_lead_id | str (uuid) | Only with `include_fields=source_lead_id` |
| arr | num | Growth+ plans |
| mrr | num | Growth+ plans |
| acv | num | Growth+ plans |
| custom_fields | json | Hash-keyed custom values |

Optional via `include_fields` (comma-separated, not returned by default): `next_activity_id`,
`last_activity_id`, `first_won_time`, `products_count`, `files_count`, `notes_count`, `followers_count`,
`email_messages_count`, `activities_count`, `done_activities_count`, `undone_activities_count`,
`participants_count`, `last_incoming_mail_time`, `last_outgoing_mail_time`, `smart_bcc_email`, `source_lead_id`.
TBD: whether to request these. Each adds server cost, and the docs do not say whether a change to a counter
moves `update_time`. Recommendation: request only `source_lead_id`. Leave the counters out.

### persons (v2)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| name | str | |
| first_name | str | |
| last_name | str | |
| owner_id | int | |
| org_id | int | |
| add_time | ts | |
| update_time | ts | **cursor** |
| emails | [{value: str, primary: bool, label: str}] | |
| phones | [{value: str, primary: bool, label: str}] | |
| is_deleted | bool | |
| visible_to | int | |
| label_ids | [int] | |
| picture_id | int | |
| postal_address | {value, country, admin_area_level_1, admin_area_level_2, locality, sublocality, route, street_number, subpremise, postal_code: all str} | Contact sync only |
| notes | str | Contact sync only |
| im | [{value: str, primary: bool, label: str}] | Contact sync only. The schema calls it `im`; the migration guide says `ims`. Accept both. |
| birthday | str | Contact sync only |
| job_title | str | Contact sync only |
| custom_fields | json | |

Optional `include_fields`: `next_activity_id`, `last_activity_id`, `open_deals_count`, `related_open_deals_count`,
`closed_deals_count`, `related_closed_deals_count`, `participant_open_deals_count`,
`participant_closed_deals_count`, `email_messages_count`, `activities_count`, `done_activities_count`,
`undone_activities_count`, `files_count`, `notes_count`, `followers_count`, `won_deals_count`,
`related_won_deals_count`, `lost_deals_count`, `related_lost_deals_count`, `last_incoming_mail_time`,
`last_outgoing_mail_time`, `marketing_status`, `doi_status`, `smart_bcc_email`. Not requested by default.

### organizations (v2)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| name | str | |
| owner_id | int | |
| add_time | ts | |
| update_time | ts | **cursor** |
| is_deleted | bool | |
| visible_to | int | |
| address | {value, country, admin_area_level_1, admin_area_level_2, locality, sublocality, route, street_number, subpremise, postal_code: all str} | |
| label_ids | [int] | |
| website | str | |
| linkedin | str | |
| industry | int | |
| annual_revenue | int | Spec says integer. Use long/double defensively. |
| employee_count | int | |
| custom_fields | json | |

Optional `include_fields`: `next_activity_id`, `last_activity_id`, `open_deals_count`, `related_open_deals_count`,
`closed_deals_count`, `related_closed_deals_count`, `email_messages_count`, `people_count`, `activities_count`,
`done_activities_count`, `undone_activities_count`, `files_count`, `notes_count`, `followers_count`,
`won_deals_count`, `related_won_deals_count`, `lost_deals_count`, `related_lost_deals_count`, `smart_bcc_email`.

### activities (v2)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| subject | str | |
| type | str | Matches `activity_types.key_string` |
| owner_id | int | |
| creator_user_id | int | |
| is_deleted | bool | |
| add_time | ts | |
| update_time | ts | **cursor** |
| deal_id | int | |
| lead_id | str (uuid) | |
| person_id | int | |
| org_id | int | |
| project_id | int | |
| due_date | str (date) | `YYYY-MM-DD` |
| due_time | str | `HH:MM` |
| duration | str | `HH:MM` |
| busy | bool | |
| done | bool | |
| marked_as_done_time | ts | |
| location | {value, country, admin_area_level_1, admin_area_level_2, locality, sublocality, route, street_number, subpremise, postal_code: all str} | |
| participants | [{person_id: int, primary: bool}] | |
| attendees | [{email: str, name: str, status: str, is_organizer: bool, person_id: int, user_id: int}] | Only with `include_fields=attendees` |
| conference_meeting_client | str | |
| conference_meeting_url | str | |
| conference_meeting_id | str | |
| public_description | str | |
| priority | int | |
| outcome | int | |
| note | str | HTML |

### products (v2)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| name | str | |
| code | str | |
| unit | str | |
| tax | num | |
| is_deleted | bool | |
| is_linkable | bool | |
| visible_to | int | 1, 3, 5, 7 |
| owner_id | int | |
| add_time | ts | |
| update_time | ts | **cursor** |
| description | str | |
| category | str | |
| billing_frequency | str | `one-time`, `annually`, `semi-annually`, `quarterly`, `monthly`, `weekly` (Growth+) |
| billing_frequency_cycles | int | |
| prices | [{price: num, cost: num, product_id: int, currency: str, direct_cost: num, notes: str}] | |
| custom_fields | json | |

### pipelines (v2)

| Field | Type |
|---|---|
| id | int (PK) |
| name | str |
| order_nr | int |
| is_deleted | bool |
| is_deal_probability_enabled | bool |
| add_time | ts |
| update_time | ts |

### stages (v2)

| Field | Type |
|---|---|
| id | int (PK) |
| order_nr | int |
| name | str |
| is_deleted | bool |
| deal_probability | int |
| pipeline_id | int |
| is_deal_rot_enabled | bool |
| days_to_rotten | int |
| add_time | ts |
| update_time | ts |

### users (v1)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| name | str | |
| default_currency | str | |
| locale | str | |
| lang | int | |
| email | str | |
| phone | str | |
| activated | bool | |
| last_login | str (ts) | `YYYY-MM-DD HH:MM:SS` (UTC) |
| created | str (ts) | `YYYY-MM-DD HH:MM:SS` |
| modified | str (ts) | `YYYY-MM-DD HH:MM:SS` |
| has_created_company | bool | |
| access | [{app: str, admin: bool, permission_set_id: str}] | |
| active_flag | bool | |
| timezone_name | str | |
| timezone_offset | str | |
| role_id | int | |
| icon_url | str | |
| is_you | bool | |
| is_deleted | bool | |
| is_admin | int | Airbyte schema only (0/1) |
| signup_flow_variation | str | Airbyte schema only |

### notes (v1)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| user_id | int | |
| deal_id | int | |
| person_id | int | |
| org_id | int | |
| lead_id | str (uuid) | |
| project_id | int | |
| task_id | int | |
| content | str | HTML |
| add_time | str (ts) | `YYYY-MM-DD HH:MM:SS` (UTC) |
| update_time | str (ts) | **cursor**, `YYYY-MM-DD HH:MM:SS` |
| active_flag | bool | |
| pinned_to_deal_flag | bool | |
| pinned_to_person_flag | bool | |
| pinned_to_organization_flag | bool | |
| pinned_to_lead_flag | bool | |
| pinned_to_project_flag | bool | |
| pinned_to_task_flag | bool | |
| last_update_user_id | int | |
| organization | {name: str} | |
| person | {name: str} | |
| deal | {title: str} | |
| lead | json | |
| project | {title: str} | |
| task | {title: str} | |
| user | {email: str, name: str, icon_url: str, is_you: bool} | |

### leads / leads_archived (v1, identical shape)

| Field | Type | Notes |
|---|---|---|
| id | str (uuid) | PK |
| title | str | |
| owner_id | int | |
| creator_id | int | |
| label_ids | [str (uuid)] | References `lead_labels.id` |
| person_id | int | |
| organization_id | int | |
| source_name | str | e.g. `API`, `Manually created` |
| origin | str | |
| origin_id | str | |
| channel | int | |
| channel_id | str | |
| source_deal_id | int | |
| is_archived | bool | |
| archive_reason | str | |
| archive_time | ts | Airbyte schema |
| was_seen | bool | |
| value | {amount: num, currency: str} | |
| expected_close_date | date | |
| next_activity_id | int | |
| add_time | ts | `YYYY-MM-DDTHH:MM:SS(.fff)Z` |
| update_time | ts | **cursor** |
| visible_to | str | `"1"`, `"3"`, `"5"`, `"7"` (string in v1) |
| cc_email | str | |
| *(deal custom-field hashes)* | any | Root-level 40-hex keys, present only when set. Gather into `custom_fields` json. |

### lead_labels (v1)

| Field | Type |
|---|---|
| id | str (uuid, PK) |
| name | str |
| color | str (`green`, `blue`, `red`, `yellow`, `purple`, `gray`) |
| add_time | ts |
| update_time | ts |

### files (v1)

| Field | Type | Notes |
|---|---|---|
| id | int | PK |
| user_id | int | |
| deal_id | int | |
| person_id | int | |
| org_id | int | |
| product_id | int | |
| activity_id | int | |
| lead_id | str (uuid) | |
| project_id | int | |
| log_id | int | |
| add_time | str (ts) | `YYYY-MM-DD HH:MM:SS` |
| update_time | str (ts) | cursor (client-side), `YYYY-MM-DD HH:MM:SS` |
| file_name | str | |
| file_type | str | |
| file_size | int | |
| active_flag | bool | |
| inline_flag | bool | |
| remote_location | str | e.g. `s3`, `googledrive` |
| remote_id | str | |
| cid | str | |
| s3_bucket | str | |
| mail_message_id | str | |
| mail_template_id | str | |
| deal_name | str | |
| person_name | str | |
| people_name | str | |
| org_name | str | |
| product_name | str | |
| lead_name | str | |
| project_name | str | |
| url | str | Download URL (needs auth) |
| name | str | |
| description | str | |

### projects (v2, `/projects` and `/projects/archived` share the shape)

| Field | Type |
|---|---|
| id | int (PK) |
| title | str |
| description | str |
| status | str (`open`, `completed`, `canceled`, `deleted`) |
| board_id | int |
| phase_id | int |
| owner_id | int |
| start_date | date |
| end_date | date |
| deal_ids | [int] |
| person_ids | [int] |
| org_ids | [int] |
| label_ids | [int] |
| health_status | int |
| add_time | ts |
| update_time | ts |
| status_change_time | ts |
| archive_time | ts (null when not archived) |
| custom_fields | json |

### tasks (v2)

| Field | Type |
|---|---|
| id | int (PK) |
| title | str |
| creator_id | int |
| description | str |
| project_id | int |
| is_done | bool |
| is_milestone | bool |
| due_date | date |
| start_date | date |
| parent_task_id | int |
| assignee_ids | [int] |
| priority | int |
| add_time | ts |
| update_time | ts |
| marked_as_done_time | ts |

### activity_types (v1)

| Field | Type |
|---|---|
| id | int (PK) |
| name | str |
| icon_key | str (enum, e.g. `task`, `call`, `meeting`, …) |
| color | str (6-hex) |
| order_nr | int |
| key_string | str |
| active_flag | bool |
| is_custom_flag | bool |
| add_time | str (ts, `YYYY-MM-DD HH:MM:SS`) |
| update_time | str (ts) |

### currencies (v1)

| Field | Type |
|---|---|
| id | int (PK) |
| code | str (ISO 4217 for non-custom) |
| name | str |
| decimal_points | int |
| symbol | str |
| active_flag | bool |
| is_custom_flag | bool |

### deal_fields / person_fields / organization_fields / product_fields / activity_fields (v1, shared `GetField` shape)

| Field | Type | Notes |
|---|---|---|
| key | str | **PK**. Built-in name (e.g. `title`) or 40-hex hash for custom fields |
| id | int | Null for subfields, so not usable as PK |
| name | str | |
| order_nr | int | |
| field_type | str | `address`, `date`, `daterange`, `double`, `enum`, `monetary`, `org`, `people`, `phone`, `set`, `text`, `time`, `timerange`, `user`, `varchar`, `varchar_auto`, `visible_to`, `int`, `boolean`, `json`, `varchar_options`, `activity`, `deal`, `lead`, `pipeline`, `product`, `project`, `stage`, `status`, `picture`, `price_list`, `billing_frequency`, `projects_board`, `projects_phase` |
| add_time | str (ts) | `YYYY-MM-DD HH:MM:SS` |
| update_time | str (ts) | |
| last_updated_by_user_id | int | |
| created_by_user_id | int | |
| active_flag | bool | |
| edit_flag | bool | |
| index_visible_flag | bool | |
| details_visible_flag | bool | |
| add_visible_flag | bool | |
| important_flag | bool | |
| bulk_edit_allowed | bool | |
| searchable_flag | bool | |
| filtering_allowed | bool | |
| sortable_flag | bool | |
| mandatory_flag | bool | Occasionally returned as object by Pipedrive (Airbyte allows bool/object). Store as string if not bool. |
| options | [json] | e.g. `[{id, label, color?}]` for enum/set |
| options_deleted | [json] | Present only if any deleted option exists |
| is_subfield | bool | |
| subfields | [json] | Present for address/monetary etc. |

TBD: Airbyte's field schemas also list extra keys such as `json_column_flag`, `use_field`, `link`,
`group_id`, `display_field`, `autocomplete`, `parent_id` and `projects_detail_visible_flag`, which the official
spec does not document. The connector should tolerate unknown keys (drop them or put them in an overflow map).

### deal_products (v2)

| Field | Type | Notes |
|---|---|---|
| id | int | PK (deal-product attachment ID) |
| deal_id | int | |
| product_id | int | |
| product_variation_id | int | |
| name | str | |
| sum | num | |
| tax | num | |
| order_nr | int | |
| add_time | ts | |
| update_time | ts | |
| comments | str | |
| currency | str | |
| discount | num | |
| discount_type | str | `percentage`, `amount` |
| quantity | num | |
| item_price | num | |
| tax_method | str | `exclusive`, `inclusive`, `none` |
| is_enabled | bool | |
| billing_frequency | str | |
| billing_frequency_cycles | int | |
| billing_start_date | date | |

### deal_flow (v1, `items=dealChange`)

The raw response is `data: [{object: "dealChange", timestamp: str, data: {...}}]`. Emit the inner `data` object as
the row, as Airbyte does with `field_path: data/*/data`:

| Field | Type | Notes |
|---|---|---|
| id | int | PK (change-log entry ID) |
| item_id | int | The deal ID |
| user_id | int | User who made the change |
| field_key | str | Changed field key (built-in or custom hash) |
| old_value | str | Mixed types (string/number/bool) in API. Store as string. |
| new_value | str | Same as above |
| is_bulk_update_flag | bool | API returns bool or int. Coerce to bool. |
| log_time | str (ts) | **cursor**, `YYYY-MM-DD HH:MM:SS` (UTC) |
| change_source | str | e.g. `app`, `api` |
| change_source_user_agent | str | |
| origin | str | |
| origin_id | str | API returns int or string. Store as string. |
| additional_data | json | Array or object. Store as JSON string. |

Example (v2 list response):

```json
{
  "success": true,
  "data": [
    {
      "id": 1, "title": "Deal Title", "owner_id": 1, "person_id": 1, "org_id": 1,
      "stage_id": 1, "pipeline_id": 1, "value": 200, "currency": "USD",
      "add_time": "2021-01-01T00:00:00Z", "update_time": "2021-01-01T00:00:00Z",
      "status": "open", "is_archived": false, "is_deleted": false,
      "label_ids": [1, 2, 3], "custom_fields": {}
    }
  ],
  "additional_data": { "next_cursor": "eyJmaWVsZCI6ImlkIiwiZmllbGRWYWx1ZSI6Nywic29ydERpcmVjdGlvbiI6ImFzYyIsImlkIjo3fQ" }
}
```

---

## **Get Object Primary Keys**

Primary keys are static. No API returns them.

| Table | Primary key |
|---|---|
| deals, deals_archived, persons, organizations, activities, products, pipelines, stages, users, notes, files, projects, tasks, activity_types, currencies, deal_products, deal_flow | `id` |
| leads, leads_archived, lead_labels | `id` (UUID string) |
| deal_fields, person_fields, organization_fields, product_fields, activity_fields | `key` |

Note: a deal is in exactly one of `deals` / `deals_archived` at a time. When it is archived or unarchived it moves
from one table to the other. The same applies to leads (`leads` / `leads_archived`). See Known Quirks.

---

## **Object's ingestion type**

| Table | Type | Cursor | Rationale |
|---|---|---|---|
| deals | `cdc_with_deletes` | `update_time` | Server-side `updated_since` + `sort_by=update_time`. With `status=open,won,lost,deleted`, soft-deleted deals (deleted ≤30 days ago) come back with `is_deleted=true` / `status=deleted`, and delete time updates `update_time`. Treat `is_deleted=true` as a delete. Deletions older than 30 days are permanently purged and are only caught by a full refresh. |
| deals_archived | `cdc_with_deletes` | `update_time` | Same parameters as deals. |
| persons | `cdc` | `update_time` | `updated_since` + sort supported. No documented filter returns deleted persons, so deletes are not captured (`is_deleted` is kept as a column). |
| organizations | `cdc` | `update_time` | Same as persons. |
| activities | `cdc` | `update_time` | Same as persons. The schema has an `is_deleted` field, but no documented filter returns deleted activities (the v2 migration guide only says `is_deleted` replaced `is_active`). Decision: deletes are not captured. If a row with `is_deleted=true` does come back, it is upserted as a normal row and keeps the `is_deleted` column. A full refresh removes hard-deleted rows. |
| products | `cdc` | `update_time` | `updated_since` + `sort_by=update_time` (no `updated_until` documented). |
| pipelines | `snapshot` | — | Small. No `updated_since` filter. |
| stages | `snapshot` | — | Small. No `updated_since` filter. |
| users | `snapshot` | — | Small, not paginated. |
| notes | `cdc` | `update_time` | v1 supports `updated_since`. No delete feed. |
| leads | `cdc` | `update_time` | v1 supports `updated_since`. No delete feed. |
| leads_archived | `snapshot` | — | `updated_since` is **not** documented for `/v1/leads/archived`. |
| lead_labels | `snapshot` | — | Small, not paginated. |
| files | `snapshot` | — | No server-side time filter. Airbyte filters `update_time` client-side after a full scan, which costs the same API calls as a snapshot. Optional: run as `cdc` with client-side filtering on `update_time` and a 1h lookback. |
| projects | `snapshot` | — | No `updated_since` on projects endpoints. Use `status=open,completed,canceled,deleted` to include deleted projects. |
| tasks | `snapshot` | — | No `updated_since`. |
| activity_types, currencies | `snapshot` | — | Small reference tables. |
| deal_fields, person_fields, organization_fields, product_fields, activity_fields | `snapshot` | — | Metadata. |
| deal_products | `snapshot` | — | Child of deals. Requires deal IDs, no time filter. |
| deal_flow | `cdc` (upsert on `id`) | `log_time` | Immutable change-log rows; upserted because the uncapped parent walk can revisit a deal edited mid-walk. Incremental by re-reading only deals whose `update_time` ≥ last cursor (from the deals / deals_archived incremental read) and keeping entries with `log_time` > cursor (client-side). Airbyte uses the same parent-incremental pattern. |

---

## **Read API for Data Retrieval**

### Common: v2 list endpoints (cursor pagination)

- Method: `GET`, header `x-api-token`.
- `limit`: default 100, **max 500**. Use 500.
- `cursor`: opaque. Take it from `additional_data.next_cursor`. It is `null` (or absent) at the end of the data.
- Response: `{ "success": true, "data": [...], "additional_data": { "next_cursor": "..." } }`.
- Where supported, the connector sends `sort_by=update_time&sort_direction=asc` (default sort is `id asc`).
- The ID-list and filter params (`ids`, `filter_id`, `owner_id`, …) are not needed.

### Common: v1 list endpoints (offset pagination)

- `start` (default 0), `limit` (default 100, max 500; **max 100 for `/v1/files`**).
- Response pagination lives at `additional_data.pagination = {start, limit, more_items_in_collection, next_start}`.
  Continue with `start=next_start` while `more_items_in_collection` is true.
- Endpoints without pagination (`/v1/users`, `/v1/leadLabels`, `/v1/activityTypes`, `/v1/currencies`) return the
  whole list in one response.
- For offset pagination on a mutable result set, sort by an **immutable** key (`sort=id ASC`) so that an edit during
  the read cannot move a row onto an already-read page. Airbyte does this for leads, notes and files.

### Per-table read details

| Table | Request | Incremental params |
|---|---|---|
| deals | `GET /api/v2/deals?limit=500&sort_by=update_time&sort_direction=asc&status=open,won,lost,deleted` | `updated_since=<cursor RFC3339>`; optionally `updated_until=<run start>` to cap the window |
| deals_archived | `GET /api/v2/deals/archived?limit=500&sort_by=update_time&sort_direction=asc&status=open,won,lost,deleted` | same |
| persons | `GET /api/v2/persons?limit=500&sort_by=update_time&sort_direction=asc` | `updated_since`, `updated_until` |
| organizations | `GET /api/v2/organizations?limit=500&sort_by=update_time&sort_direction=asc` | `updated_since`, `updated_until` |
| activities | `GET /api/v2/activities?limit=500&sort_by=update_time&sort_direction=asc` | `updated_since`, `updated_until` |
| products | `GET /api/v2/products?limit=500&sort_by=update_time&sort_direction=asc` | `updated_since` only |
| pipelines | `GET /api/v2/pipelines?limit=500` | — |
| stages | `GET /api/v2/stages?limit=500` | — |
| users | `GET /v1/users` | — |
| notes | `GET /v1/notes?limit=500&sort=id ASC` | `updated_since=<RFC3339>` |
| leads | `GET /v1/leads?limit=500&sort=id ASC` | `updated_since=<ISO 8601>` |
| leads_archived | `GET /v1/leads/archived?limit=500&sort=id ASC` | — |
| lead_labels | `GET /v1/leadLabels` | — |
| files | `GET /v1/files?limit=100&sort=id ASC` | — (client-side optional) |
| projects | `GET /api/v2/projects?limit=500&status=open,completed,canceled,deleted` **then** `GET /api/v2/projects/archived?limit=500&status=open,completed,canceled,deleted`; union, dedupe on `id` | — |
| tasks | `GET /api/v2/tasks?limit=500` | — |
| activity_types | `GET /v1/activityTypes` | — |
| currencies | `GET /v1/currencies` | — |
| `*_fields` | `GET /v1/{deal,person,organization,product,activity}Fields?limit=500` | — |
| deal_products | `GET /api/v2/deals/products?deal_ids=1,2,...,100&limit=500` (cursor paginated per batch) | — |
| deal_flow | `GET /v1/deals/{id}/flow?items=dealChange&all_changes=1&limit=500` (offset paginated per deal) | Parent deals filtered by `update_time` ≥ cursor; rows filtered `log_time` > cursor |

Incremental algorithm (v2 core entities, plus v1 notes and leads):

1. `since = state.cursor or start_date`. Capture `until = now()` at run start and pass `updated_until` where
   supported. This bounds the window, so records updated mid-read go to the next run.
2. Page through with `sort_by=update_time asc`.
3. New cursor = max `update_time` seen (or `until`). `updated_since` is inclusive (`>=`), so boundary records are
   re-read and de-duplicated by PK upsert. A small lookback (Airbyte uses 1h for v1 notes/leads) protects against
   clock skew and late-committed writes.
4. v2 timestamps are RFC 3339 (`2025-01-01T10:20:00Z`). v1 notes return `YYYY-MM-DD HH:MM:SS` (UTC) but accept
   RFC 3339 for `updated_since`. v1 leads return `YYYY-MM-DDTHH:MM:SS(.fff)Z`.

Deleted records:

- `deals` / `deals_archived`: include `status=deleted` and you get soft-deleted deals (≤30 days) with
  `is_deleted=true`. Treat them as deletes.
- `projects`: `status=deleted` includes deleted projects. A snapshot read reflects them via `status`.
- persons, organizations, activities, products, notes, leads: no documented deleted-records feed. Hard deletes are
  only detected by a full refresh. Soft-deleted entities are purged by Pipedrive 30 days after the last activity.
- `pipelines`, `stages`: `is_deleted` flag on the row (snapshot).

Child-table reads:

- `deal_products`: collect all deal IDs from `deals` and `deals_archived`, dedupe, chunk into groups of
  **≤100**, and call once per chunk (`deal_ids` is required, max 100).
- `deal_flow`: one request per deal (cost 40 tokens each), offset-paginated. A 404 / 410 / 403 for a deal deleted
  or hidden between the parent read and the child read should be skipped, not treated as fatal (Airbyte does
  this).

Optional tables (projects, tasks): if the company lacks the Projects add-on, the endpoints answer 402 / 403 / 415,
or a JSON 404. Airbyte treats these as "skip table" (empty result) rather than failing. An HTML 404 means the route
is unknown and should fail.

### Rate limits

- **Daily token budget** per company: 30,000 base tokens × plan multiplier (Lite 1, Growth 2, Premium 5,
  Ultimate 7) × seats, plus purchased top-ups. Resets at midnight server time. Once exhausted, every call gets
  **429** until the reset.
- **Token cost per call** (from `x-token-cost` in the specs): v2 list deals / persons / orgs / activities /
  products / projects / tasks / deal_products = 10; v2 deals/archived = 20; v2 pipelines / stages = 5; v1 users /
  notes / leads / files / leadLabels / activityTypes / currencies / *Fields = 20; v1 leads/archived = 40; v1
  deals/{id}/flow = 40. `deal_flow` is by far the most expensive table (one 40-token call per deal).
- **Burst limit** (rolling 2 s window, per user/token): API token Lite 20, Growth 40, Premium 100, Ultimate 120
  (OAuth apps 4× that). The plan is not known from config, so default to ≤20 requests / 2 s (Airbyte's floor).
- Headers: `x-ratelimit-limit`, `x-ratelimit-remaining`, `x-ratelimit-reset` (seconds until window reset,
  **relative**, not epoch), `x-daily-requests-left`.
- On 429: wait `x-ratelimit-reset` seconds (or exponential backoff) and retry. Repeated 429 floods can trigger a
  Cloudflare **403 HTML** page, so the connector must throttle proactively.

---

## **Field Type Mapping**

| Pipedrive / JSON type | Spark type | Notes |
|---|---|---|
| integer (IDs, counts, `visible_to` in v2) | LongType | |
| number (`value`, `price`, `probability`, `sum`, …) | DoubleType | |
| boolean (`is_deleted`, `*_flag`, `done`, …) | BooleanType | v1 `is_bulk_update_flag` may be 0/1: coerce |
| string | StringType | |
| RFC 3339 timestamp (v2 `add_time`, `update_time`, …) | StringType (ISO) or TimestampType | Keep as string to avoid parse failures. Can be cast downstream. |
| v1 timestamp `YYYY-MM-DD HH:MM:SS` (UTC) | StringType | Same reasoning. Lexicographically sortable. |
| date `YYYY-MM-DD` | StringType / DateType | |
| UUID (lead IDs, lead label IDs, `source_lead_id`) | StringType | |
| arrays of scalars (`label_ids`, `deal_ids`, `assignee_ids`) | ArrayType(Long / String) | |
| nested objects (`address`, `location`, `value`, `emails[]`, `prices[]`) | StructType / ArrayType(StructType) | Fixed shapes listed above |
| `custom_fields`, `options`, `subfields`, `additional_data`, mixed-type `old_value`/`new_value` | StringType (JSON-serialized) or MapType(String, String) | Shape varies per company |

Special behaviours:

- `visible_to` is an **integer** in v2 (1 owner & followers, 3 entire company, 5 / 7 visibility groups) but a
  **string** in v1 leads.
- `origin` is set by Pipedrive at creation and is immutable.
- Custom field values: v2 nests composite types (monetary → `{value, currency}`, daterange →
  `{value, until}`, address → struct). Single-option values are numeric IDs, multi-option values are numeric
  arrays. Resolve them with `*_fields.options`.
- `update_time` can be null on v2 deals / persons / organizations / stages for records never updated. Fall back
  to `add_time` when computing the cursor.

---

## **Known Quirks**

- **Archived split**: `/api/v2/deals` excludes archived deals and `/api/v2/deals/archived` returns only archived
  ones. Archiving a deal makes it disappear from `deals` without any delete signal there. Downstream consumers
  should union the two tables (or the connector can treat both as one logical set).
  Leads behave the same way (`/v1/leads` vs `/v1/leads/archived`).
- **Deleted deals window**: `status=deleted` only returns deals deleted in the last 30 days.
- **OAuth host**: OAuth calls must target the company `api_domain`. API token calls can use
  `https://api.pipedrive.com`. Not relevant for the chosen API token method, except that `company_domain` is
  accepted as an optional override.
- **Field tables v1 vs v2**: v1 `/v1/dealFields` etc. are not deprecated and are used here. v2 equivalents exist
  with a different shape (`field_code`, no timestamps).
- **`annual_revenue`** is typed integer in the spec. Use LongType.
- **Persons `im` vs `ims`**: the spec response schema uses `im`, the migration guide uses `ims`. Accept either
  and map both to one column.
- **Example-only fields**: v2 deals example payloads include `creator_user_id`, `archive_time` and `local_*_date`,
  which are absent from the formal schema. They are included as nullable columns.
- **Plan-gated fields**: `arr` / `mrr` / `acv`, `billing_frequency*` (Growth+). Contact-sync-only person fields.
- **Projects / tasks** need the Projects add-on. Skip gracefully when the plan doesn't include it.
- **HTML error pages**: Pipedrive answers some errors (Cloudflare 403 block, unknown v2 route 404) with
  `text/html` instead of JSON. Detect via `Content-Type`.
- TBD: whether attaching or changing a deal product bumps the parent deal's `update_time`. This decides whether
  `deal_products` could be read incrementally via updated deals. Until verified live, it stays `snapshot`.
- **Deleted activities / persons / organizations / products**: these entities have an `is_deleted` field
  (soft-deleted, purged 30 days after last activity per the v2 migration guide). Unlike deals (`status=deleted`),
  their v2 list endpoints document no filter that returns deleted rows. Decision: these tables are `cdc`
  without delete capture. Any `is_deleted=true` row that does come back is upserted as a normal row, with
  `is_deleted` kept as a column. Hard-deleted rows are removed only by a full refresh. Live record-mode runs
  can upgrade a table to `cdc_with_deletes` if they show deleted rows coming back with a bumped `update_time`.

---

## **Sources and References**

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official OpenAPI v2 spec | https://developers.pipedrive.com/docs/api/v1/openapi-v2.yaml | 2026-11-18 | Highest | v2 endpoints, query params (`updated_since`, `updated_until`, `sort_by`, `status`, `limit` ≤500, `cursor`), response schemas for deals, deals/archived, persons, organizations, activities, products, pipelines, stages, projects, projects/archived, tasks, deals/products, dealFields (v2), `x-token-cost` values |
| Official OpenAPI v1 spec | https://developers.pipedrive.com/docs/api/v1/openapi.yaml | 2026-11-18 | Highest | v1 endpoints and schemas for users, notes, leads, leads/archived, files, activityTypes, currencies, *Fields, deals/{id}/flow; deprecation flags (v1 deals/archived deprecated; v1 core entity list endpoints absent) |
| Official docs: Deals reference | https://developers.pipedrive.com/docs/api/v1/Deals | 2026-11-18 | High | Deals v2 params, `status=deleted` 30-day window, deal flow params |
| Official docs: Authentication | https://pipedrive.readme.io/docs/core-api-concepts-authentication | 2026-11-18 | High | `x-api-token` header, company-domain base URL, one active token per user |
| Official docs: Rate limiting | https://pipedrive.readme.io/docs/core-api-concepts-rate-limiting | 2026-11-18 | High | Daily token budget formula, burst limits per plan, rate-limit headers, 429 / Cloudflare 403 behaviour |
| Official docs: Pagination | https://pipedrive.readme.io/docs/core-api-concepts-pagination | 2026-11-18 | High | v1 offset pagination (`additional_data.pagination.next_start`), v2 cursor pagination (`next_cursor`), limit default 100 / max 500 |
| Official docs: v2 migration guide | https://pipedrive.readme.io/docs/pipedrive-api-v2-migration-guide | 2026-11-18 | High | Deleted-entity handling, RFC 3339 timestamps, `custom_fields` nesting, v1→v2 field renames |
| Airbyte source-pipedrive manifest | https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-pipedrive/manifest.yaml | 2026-11-18 | Medium–High | Stream list, endpoint choice per table (v2 vs v1), PKs (`key` for field tables), cursor fields, `sort=id ASC` for offset streams, 1h lookback, deal_products batching (100 IDs), deal_flow record path `data/*/data` with `items=dealChange&all_changes=1`, projects union of `/projects` + `/projects/archived` with all statuses, error handling (skip 402/403/415 for projects/tasks, skip 404/410/403 for deal_flow children), burst budget 20 / 2 s, schemas for files, notes, leads, users, deal_flow |
| Airbyte connector docs | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-pipedrive | 2026-11-18 | Medium | Reference implementation (user-provided) |

Conflict resolution:

- **Field tables**: Airbyte and the v1 spec agree (`/v1/*Fields`, PK `key`). v2 field endpoints exist but have a
  different shape. Chose v1 (official, not deprecated, matches the reference implementation).
- **Persons `im`/`ims`**: the spec schema says `im`, the migration guide says `ims`. Accept both (see Known
  Quirks).
- **Files incrementality**: Airbyte runs it as client-side incremental, but the official spec offers no time
  filter. Documented as `snapshot`, with client-side cdc as an option.
- **`api_token` query parameter**: older community docs mention it, the current official auth page documents only
  the header. Chose the header (official).
