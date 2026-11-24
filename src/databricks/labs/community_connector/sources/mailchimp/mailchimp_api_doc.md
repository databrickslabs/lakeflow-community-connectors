# **Mailchimp Marketing API Documentation**

This document maps each connector table to its underlying **Mailchimp Marketing
API v3.0** endpoint(s), authentication, pagination, incremental-sync strategy,
schemas, and rate-limit behavior. It targets the batch ingestion of marketing
campaign-performance data (campaigns, reports, audiences/lists, and members)
for consolidation in Databricks.

- Official API base: `https://<dc>.api.mailchimp.com/3.0/`
- API version: `3.0` (current stable)

---

## **Authorization**

### Preferred method: API key (HTTP Basic auth)

Mailchimp Marketing API supports two equivalent credential-passing styles for a
**stored API key**. The connector uses one long-lived API key that the user
supplies; no interactive flow is run.

1. **HTTP Basic Authentication** (preferred): send the API key as the password
   with any non-empty username (Mailchimp ignores the username). The connector
   uses the literal username `anystring`.

   ```
   Authorization: Basic base64("anystring:<API_KEY>")
   ```

2. **Bearer token** (equivalent): send the same API key as a bearer token.

   ```
   Authorization: Bearer <API_KEY>
   ```

Either header authenticates identically. The connector standardizes on **HTTP
Basic** because it is the form documented in every official example.

### Deriving the base URL from the data center (`<dc>`) suffix

Every Mailchimp API key ends with a data-center suffix after the last hyphen,
e.g. `0123456789abcdef0123456789abcde-us21`. The suffix (`us21`) is the account
data center and **must** be used as the subdomain of the API root:

- API key: `...-us21`  →  base URL: `https://us21.api.mailchimp.com/3.0/`
- API key: `...-us6`   →  base URL: `https://us6.api.mailchimp.com/3.0/`

Derivation rule the connector applies at runtime:

```
dc       = api_key.rsplit("-", 1)[1]      # text after the final hyphen, e.g. "us21"
base_url = f"https://{dc}.api.mailchimp.com/3.0/"
```

If a key has no `-<dc>` suffix (rare, legacy), the data center cannot be
inferred and the request will fail — the user must supply the data center
explicitly. The `<dc>` can also be found in the account API-keys page URL
(`https://us21.mailchimp.com/account/api/`).

### OAuth 2 (not used by this connector)

Mailchimp also offers OAuth 2 for accessing *other users'* accounts (agency /
multi-tenant apps). Per the connector convention, the connector **stores**
credentials and exchanges them at runtime — it does **not** run user-facing
OAuth flows. For OAuth-based deployments the connector would store the
`access_token` and resolve `<dc>` via the OAuth metadata endpoint
(`https://login.mailchimp.com/oauth2/metadata`) rather than the key suffix.
For the batch scope here, a single stored **API key** is the documented path.

### Example authenticated request

```bash
# Basic auth (preferred)
curl --request GET \
  --url 'https://us21.api.mailchimp.com/3.0/ping' \
  --user 'anystring:0123456789abcdef0123456789abcde-us21'

# Bearer (equivalent)
curl --request GET \
  --url 'https://us21.api.mailchimp.com/3.0/ping' \
  --header 'Authorization: Bearer 0123456789abcdef0123456789abcde-us21'
```

`GET /ping` is the recommended connectivity/health check (returns
`{"health_status":"Everything's Chimpy!"}`).

---

## **Object List**

The object list for this connector is **static** — Mailchimp exposes a fixed
set of top-level resources, not a dynamic "list objects" endpoint. The
in-scope core tables are:

| Table       | Endpoint (list)                     | Layering / scope                                   |
|-------------|-------------------------------------|----------------------------------------------------|
| `campaigns` | `GET /campaigns`                    | Account-level (all campaigns).                     |
| `reports`   | `GET /reports`                      | Account-level; one report per **sent** campaign. `id` = campaign `id`. |
| `lists`     | `GET /lists`                        | Account-level (a.k.a. **audiences**).              |
| `members`   | `GET /lists/{list_id}/members`      | **Nested under a list.** Must be iterated per `list_id`. |

### Handling the nested `members` object

`members` are not addressable at the account root — they live under each list.
To ingest all members the connector must:

1. Enumerate every audience with `GET /lists` (paginate with `count`/`offset`).
2. For each returned `lists[].id`, page `GET /lists/{list_id}/members`.
3. Union the results; each member row already carries its own `list_id` field,
   so provenance is preserved in the output table.

There is no cross-list "all members" endpoint, so member fan-out is
`O(number_of_lists)` sequences of paginated calls.

---

## **Object Schema**

Schemas are **static** (Mailchimp does not offer a per-object schema-discovery
endpoint). The authoritative shape is the documented JSON schema of each
endpoint's response. Fields below are the load-bearing columns; the connector
should preserve the full JSON and may flatten nested structs. All list
responses are wrapped in an envelope containing the named array plus
`total_items` and `_links`.

### `campaigns` — `GET /campaigns` → `campaigns[]`

| Field                     | API type            | Notes |
|---------------------------|---------------------|-------|
| `id`                      | string              | **Primary key.** Unique campaign id. |
| `web_id`                  | integer             | ID used in the Mailchimp web app URL. |
| `type`                    | string (enum)       | `regular`, `plaintext`, `absplit`, `rss`, `variate`. |
| `create_time`            | string (ISO 8601)   | Creation timestamp. |
| `send_time`               | string (ISO 8601)   | Delivery timestamp (null for unsent). |
| `status`                  | string (enum)       | `save`, `paused`, `schedule`, `sending`, `sent`, `canceled`, `archived`. |
| `emails_sent`             | integer             | Emails delivered. |
| `archive_url`             | string              | Archive URL. |
| `content_type`            | string              | e.g. `template`, `html`. |
| `recipients`              | object (struct)     | `list_id`, `list_name`, `segment_text`, `recipient_count`, `segment_opts`. |
| `settings`                | object (struct)     | `subject_line`, `title`, `from_name`, `reply_to`, `folder_id`, `authenticate`, … |
| `tracking`                | object (struct)     | `opens`, `html_clicks`, `text_clicks`, `goal_tracking`, `ecomm360`, … (booleans). |
| `report_summary`          | object (struct)     | `opens`, `unique_opens`, `open_rate`, `clicks`, `subscriber_clicks`, `click_rate`, `ecommerce`. |
| `delivery_status`         | object (struct)     | `enabled`, `can_cancel`, `status`, `emails_sent`, `emails_canceled`. |

### `reports` — `GET /reports` → `reports[]`

| Field               | API type          | Notes |
|---------------------|-------------------|-------|
| `id`                | string            | **Primary key.** Same value as the campaign `id`. |
| `campaign_title`    | string            | Title from campaign settings. |
| `type`              | string            | Campaign type. |
| `list_id`           | string            | Audience the campaign was sent to. |
| `list_is_active`    | boolean           | Whether the list still exists/active. |
| `list_name`         | string            | Audience name. |
| `subject_line`      | string            | Subject line. |
| `preview_text`      | string            | Preview text. |
| `emails_sent`       | integer           | Emails delivered. |
| `abuse_reports`     | integer           | Abuse complaints. |
| `unsubscribed`      | integer           | Unsubscribes. |
| `send_time`         | string (ISO 8601) | When the campaign was sent. |
| `bounces`           | object (struct)   | `hard_bounces`, `soft_bounces`, `syntax_errors`. |
| `forwards`          | object (struct)   | `forwards_count`, `forwards_opens`. |
| `opens`             | object (struct)   | `opens_total`, `unique_opens`, `open_rate`, `last_open`. |
| `clicks`            | object (struct)   | `clicks_total`, `unique_clicks`, `unique_subscriber_clicks`, `click_rate`, `last_click`. |
| `facebook_likes`    | object (struct)   | `recipient_likes`, `unique_likes`, `facebook_likes`. |
| `list_stats`        | object (struct)   | `sub_rate`, `unsub_rate`, `open_rate`, `click_rate`. |
| `ecommerce`         | object (struct)   | `total_orders`, `total_spent`, `total_revenue`. |
| `delivery_status`   | object (struct)   | Delivery status block. |

### `lists` — `GET /lists` → `lists[]`

| Field                  | API type          | Notes |
|------------------------|-------------------|-------|
| `id`                   | string            | **Primary key.** Audience id. |
| `web_id`               | integer           | Web-app ID. |
| `name`                 | string            | Audience name. |
| `contact`              | object (struct)   | `company`, `address1`, `address2`, `city`, `state`, `zip`, `country`, `phone`. |
| `permission_reminder`  | string            | Permission reminder text. |
| `use_archive_bar`      | boolean           | Show archive bar. |
| `campaign_defaults`    | object (struct)   | `from_name`, `from_email`, `subject`, `language`. |
| `notify_on_subscribe`  | string            | Email notified on subscribe. |
| `notify_on_unsubscribe`| string            | Email notified on unsubscribe. |
| `date_created`         | string (ISO 8601) | Audience creation timestamp. |
| `list_rating`          | integer           | Star rating (0–5). |
| `email_type_option`    | boolean           | Whether members choose HTML/text. |
| `subscribe_url_short`  | string            | Short hosted signup URL. |
| `subscribe_url_long`   | string            | Long hosted signup URL. |
| `beamer_address`       | string            | Email-to-list address. |
| `visibility`           | string (enum)     | `pub` / `prv`. |
| `double_optin`         | boolean           | Double opt-in enabled. |
| `marketing_permissions`| boolean           | GDPR marketing permissions enabled. |
| `stats`                | object (struct)   | `member_count`, `unsubscribe_count`, `cleaned_count`, `member_count_since_send`, `unsubscribe_count_since_send`, `cleaned_count_since_send`, `campaign_count`, `campaign_last_sent`, `merge_field_count`, `avg_sub_rate`, `avg_unsub_rate`, `target_sub_rate`, `open_rate`, `click_rate`, `last_sub_date`, `last_unsub_date`. |

### `members` — `GET /lists/{list_id}/members` → `members[]`

| Field               | API type          | Notes |
|---------------------|-------------------|-------|
| `id`                | string            | **Primary key.** MD5 hash of the lowercased `email_address`. |
| `email_address`     | string            | Member email. |
| `unique_email_id`   | string            | Stable per-email id (persists across lists). |
| `contact_id`        | string            | Cross-audience contact id. |
| `full_name`         | string            | Full name. |
| `web_id`            | integer           | Web-app ID. |
| `email_type`        | string (enum)     | `html` / `text`. |
| `status`            | string (enum)     | `subscribed`, `unsubscribed`, `cleaned`, `pending`, `transactional`, `archived`. |
| `merge_fields`      | object (struct)   | Merge fields (e.g. `FNAME`, `LNAME`, `ADDRESS`); shape varies per list. |
| `interests`         | object (struct)   | Interest-id → boolean map. |
| `stats`             | object (struct)   | `avg_open_rate`, `avg_click_rate`, plus `ecommerce_data`. |
| `ip_signup`         | string            | Signup IP. |
| `timestamp_signup`  | string (ISO 8601) | Signup time. |
| `ip_opt`            | string            | Opt-in IP. |
| `timestamp_opt`     | string (ISO 8601) | Opt-in time. |
| `member_rating`     | integer           | Star rating (1–5). |
| `last_changed`      | string (ISO 8601) | **Cursor field.** Last modification timestamp. |
| `language`          | string            | Language code. |
| `vip`               | boolean           | VIP flag. |
| `email_client`      | string            | Detected email client. |
| `location`          | object (struct)   | `latitude`, `longitude`, `gmtoff`, `dstoff`, `country_code`, `timezone`. |
| `tags_count`        | integer           | Number of tags. |
| `tags`              | array<object>     | `[{id, name}]`. |
| `list_id`           | string            | Parent audience id (provenance for the unioned table). |

> Note on `email_id`: some downstream tooling (Airbyte/Fivetran) refers to a
> member `email_id`; in the current v3 payload the stable per-email identifier
> is `unique_email_id`, and the record key is `id` (MD5 of the lowercased
> email). The connector keys on `id`.

---

## **Get Object Primary Keys**

Primary keys are **static** (no API to fetch them). They are the documented
identity fields:

| Table       | Primary key | Derivation / uniqueness |
|-------------|-------------|-------------------------|
| `campaigns` | `id`        | Server-assigned unique campaign id. |
| `reports`   | `id`        | Equals the campaign `id` (one report per sent campaign). |
| `lists`     | `id`        | Server-assigned unique audience id. |
| `members`   | `id`        | MD5 hex digest of the **lowercased** `email_address`. Unique **within a list**; the connector's global key is the composite `(list_id, id)`. `unique_email_id` identifies the same email across lists. |

For `members`, because `id` is only unique per list, the connector treats
`(list_id, id)` as the effective primary key for the unioned output table.

---

## **Object's ingestion type**

There are four types: `cdc`, `cdc_with_deletes`, `snapshot`, `append`.

| Table       | Ingestion type | Cursor field | Filter param | Justification |
|-------------|----------------|--------------|--------------|---------------|
| `members`   | `cdc`          | `last_changed` | `since_last_changed` / `before_last_changed` | True last-modified cursor. Records are mutated in place (status changes, merge-field edits) and re-read on each window; upsert by `(list_id, id)`. Deletes: `unsubscribed`/`cleaned`/`archived` are captured as **soft deletes** via the `status` field, so `cdc` (not `cdc_with_deletes`) — Mailchimp exposes no dedicated hard-delete change feed. |
| `campaigns` | `cdc`          | `create_time` | `since_create_time` / `before_create_time` (also `since_send_time`) | Stable PK `id`, upsert semantics. Only a creation-time filter exists (no `updated_at`), so incremental windows reliably capture **new** campaigns; status/report changes on already-synced campaigns are refreshed only on a full re-read. Classified `cdc` because reads upsert by `id`. |
| `reports`   | `cdc`          | `send_time`  | `since_send_time` / `before_send_time` | One report per sent campaign, PK `id`. Cursor is the immutable `send_time`. Aggregate metrics (opens/clicks) keep accumulating after send, so a periodic full refresh is recommended to recapture late engagement on older campaigns; upsert by `id`. |
| `lists`     | `cdc`          | `date_created` | `since_date_created` / `before_date_created` | Stable PK `id`, upsert by `id`. Only a creation-time filter exists; `stats` mutate over time, so like campaigns/reports, mutations to existing audiences are recaptured on full refresh. Small cardinality (audiences are few) makes periodic full refresh cheap. |

Snapshot mode is a safe fallback for `campaigns`, `reports`, and `lists` given
their creation-only cursors and mutable aggregate fields; the table above uses
`cdc` to match the Airbyte/Fivetran incremental behavior while noting the
mutation caveat. `members` is the one object with genuine change-data capture
via `last_changed`.

---

## **Read API for Data Retrieval**

### Method

All reads are **GET** requests returning JSON. There are no POST search
endpoints — filtering is done with query parameters.

### Pagination (`count` + `offset`)

Mailchimp uses classic offset pagination on every list endpoint:

- `count` — page size. **Default `10`, maximum `1000`.**
- `offset` — number of records to skip. **Default `0`.**

The response envelope includes `total_items`, letting the connector compute the
number of pages. The loop advances `offset` by `count` until
`offset >= total_items` (or a page returns fewer than `count` rows).

```
offset = 0
count  = 1000
loop:
  GET /{resource}?count={count}&offset={offset}[&<cursor filter>]
  emit response["{resource}"]
  offset += count
  stop when offset >= response["total_items"]
```

> Caveat: large `offset` values degrade performance on high-cardinality
> `members` lists. To keep windows bounded and stable, combine a tight
> `since_last_changed`/`before_last_changed` window with paging rather than
> deep-offsetting an unfiltered list.

### Incremental filters (cursors)

| Table       | Incremental params |
|-------------|--------------------|
| `campaigns` | `since_create_time`, `before_create_time`, `since_send_time`, `before_send_time` (RFC 3339 / ISO 8601) |
| `reports`   | `since_send_time`, `before_send_time` |
| `lists`     | `since_date_created`, `before_date_created` |
| `members`   | `since_last_changed`, `before_last_changed`, `since_timestamp_opt`, `before_timestamp_opt` |

To guarantee bounded, terminating microbatches, the connector captures the
window upper bound (`before_*` = init timestamp) once per run so the read window
does not chase records created/changed during the sync.

### Partial responses (`fields` / `exclude_fields`)

Both endpoints accept mutually exclusive `fields` and `exclude_fields`
(comma-separated, dot-notation for nesting, e.g. `lists.id,lists.name`). Using
`fields` to request only needed columns reduces payload size and avoids the
120-second call timeout on wide objects. Mailchimp errors if a named field is
invalid.

### Deleted records

Mailchimp does not expose a hard-delete change feed for these objects:

- `members`: contacts that leave are reflected as `status` = `unsubscribed`,
  `cleaned`, or `archived` — treat as **soft deletes** read through the normal
  `since_last_changed` window. Archived members can additionally be pulled with
  `status=archived`. (A separate `unsubscribes` object, deferred below, records
  the unsubscribe event per campaign.)
- `campaigns` / `reports` / `lists`: no delete feed; a full refresh reconciles
  removals.

Because no reliable hard-delete stream exists, no table is classified
`cdc_with_deletes`.

### Example request + response — `campaigns`

```bash
curl --request GET \
  --url 'https://us21.api.mailchimp.com/3.0/campaigns?count=1000&offset=0&since_create_time=2026-01-01T00:00:00Z&sort_field=create_time&sort_dir=ASC&exclude_fields=campaigns._links' \
  --user 'anystring:<API_KEY>'
```

```json
{
  "campaigns": [
    {
      "id": "42694e9e57",
      "web_id": 1178420,
      "type": "regular",
      "create_time": "2026-03-04T18:14:29+00:00",
      "send_time": "2026-03-05T15:00:00+00:00",
      "status": "sent",
      "emails_sent": 15308,
      "recipients": { "list_id": "abc123def4", "recipient_count": 15308 },
      "settings": { "subject_line": "March promo", "title": "March Blast", "from_name": "Acme Retail" },
      "report_summary": { "opens": 5893, "unique_opens": 4102, "open_rate": 0.268, "clicks": 812, "click_rate": 0.053 }
    }
  ],
  "total_items": 1,
  "_links": []
}
```

### Example request + response — `reports`

```bash
curl --request GET \
  --url 'https://us21.api.mailchimp.com/3.0/reports?count=1000&offset=0&since_send_time=2026-01-01T00:00:00Z' \
  --user 'anystring:<API_KEY>'
```

```json
{
  "reports": [
    {
      "id": "42694e9e57",
      "campaign_title": "March Blast",
      "type": "regular",
      "list_id": "abc123def4",
      "emails_sent": 15308,
      "abuse_reports": 0,
      "unsubscribed": 12,
      "send_time": "2026-03-05T15:00:00+00:00",
      "bounces": { "hard_bounces": 4, "soft_bounces": 9, "syntax_errors": 0 },
      "opens": { "opens_total": 5893, "unique_opens": 4102, "open_rate": 0.268, "last_open": "2026-03-07T09:11:02+00:00" },
      "clicks": { "clicks_total": 1204, "unique_clicks": 812, "click_rate": 0.053, "last_click": "2026-03-07T08:40:55+00:00" }
    }
  ],
  "total_items": 1,
  "_links": []
}
```

### Example request + response — `lists`

```bash
curl --request GET \
  --url 'https://us21.api.mailchimp.com/3.0/lists?count=1000&offset=0' \
  --user 'anystring:<API_KEY>'
```

```json
{
  "lists": [
    {
      "id": "abc123def4",
      "web_id": 987654,
      "name": "Acme Retail Marketing Audience",
      "date_created": "2025-11-02T20:31:00+00:00",
      "list_rating": 3,
      "email_type_option": true,
      "visibility": "prv",
      "stats": { "member_count": 15412, "unsubscribe_count": 233, "cleaned_count": 87, "open_rate": 0.271, "click_rate": 0.049 }
    }
  ],
  "total_items": 1,
  "_links": []
}
```

### Example request + response — `members` (nested under a list)

```bash
curl --request GET \
  --url 'https://us21.api.mailchimp.com/3.0/lists/abc123def4/members?count=1000&offset=0&since_last_changed=2026-06-01T00:00:00Z' \
  --user 'anystring:<API_KEY>'
```

```json
{
  "members": [
    {
      "id": "852aeff14a625f14f65c05a44860d7de",
      "email_address": "cliente@example.com",
      "unique_email_id": "b1cd9f6e2a",
      "contact_id": "c0ffee1234",
      "full_name": "Ana Perez",
      "email_type": "html",
      "status": "subscribed",
      "merge_fields": { "FNAME": "Ana", "LNAME": "Perez" },
      "stats": { "avg_open_rate": 0.31, "avg_click_rate": 0.06 },
      "member_rating": 4,
      "last_changed": "2026-06-18T14:02:41+00:00",
      "vip": false,
      "list_id": "abc123def4"
    }
  ],
  "list_id": "abc123def4",
  "total_items": 1,
  "_links": []
}
```

### Rate limits (must not be exceeded)

- **Max 10 simultaneous connections per API key.** Exceeding this returns
  HTTP `429`. The connector must cap parallelism at ≤ 10 concurrent requests
  (Airbyte defaults to 2, range 2–10); a conservative default of 2–3 is safe.
- **120-second timeout per API call.** Wide objects should use `fields` /
  `exclude_fields` and `count=1000` paging to stay under the timeout.
- On `429`, back off (exponential) and retry; there is no documented daily
  request cap, only the concurrency limit.

---

## **Field Type Mapping**

| Mailchimp API type            | Standard type | Notes |
|-------------------------------|---------------|-------|
| string                        | string        | Ids, names, enums, URLs. |
| string (ISO 8601 / RFC 3339)  | datetime      | `create_time`, `send_time`, `date_created`, `last_changed`, `timestamp_*`. Format `YYYY-MM-DDThh:mm:ss+00:00` (UTC). |
| number / integer              | integer       | `web_id`, `emails_sent`, `member_count`, counts. |
| number (fractional)           | double        | Rates such as `open_rate`, `click_rate`, `avg_sub_rate` (0.0–1.0). |
| boolean                       | boolean       | `vip`, `email_type_option`, `double_optin`, tracking flags. |
| object                        | struct        | `settings`, `recipients`, `tracking`, `stats`, `opens`, `clicks`, `bounces`, `contact`, `campaign_defaults`, `location`. |
| array                         | array<...>    | `tags` (array<struct>), `_links` (array<struct>). |
| object with dynamic keys      | struct/map    | `merge_fields`, `interests` — key set varies per list; treat as a JSON/map column since shape is not fixed. |

Special behaviors and constraints:

- **Enumerations**: `campaigns.type`, `campaigns.status`, `members.status`,
  `members.email_type`, `lists.visibility` are constrained enum strings
  (values listed in the schema tables above).
- **Auto-generated ids**: `campaigns.id`, `lists.id` are server-assigned;
  `reports.id` mirrors the campaign id; `members.id` is a deterministic
  **MD5 of the lowercased email** (client-reproducible).
- **Relationships**: `reports.list_id` and `campaigns.recipients.list_id`
  → `lists.id`; `members.list_id` → `lists.id`. `reports.id` ↔ `campaigns.id`.
- **Rates** are decimal fractions (multiply by 100 for percentages).
- **Timestamps** are UTC ISO 8601 with an explicit `+00:00` offset; nullable
  where an event has not occurred (e.g. `send_time` for unsent campaigns).
- **Mutable aggregates**: `report_summary`, `stats`, `opens`, `clicks` change
  after the cursor timestamp; see the ingestion-type caveats.

---

## **Deferred Tables**

The following objects are supported by Airbyte/Fivetran and are clearly core to
a fuller Mailchimp analytics model, but are **out of scope** for this initial
batch (campaign-performance consolidation) and are documented here for a later
iteration:

| Deferred table       | Endpoint                                             | Why deferred / notes |
|----------------------|------------------------------------------------------|----------------------|
| `automations`        | `GET /automations`                                   | Classic automations (legacy); Customer Journeys are the modern equivalent and lack a symmetrical list API. Incremental via `create_time`. |
| `segments`           | `GET /lists/{list_id}/segments`                      | Nested under a list; incremental via `updated_at`. PK `id`. |
| `segment_members`    | `GET /lists/{list_id}/segments/{segment_id}/members` | Doubly nested; large volume. |
| `unsubscribes`       | `GET /reports/{campaign_id}/unsubscribed`            | Nested under a report; composite key `(campaign_id, email_id)`; captures unsubscribe events per campaign. |
| `email_activity`     | `GET /reports/{campaign_id}/email-activity`          | Nested under a report; very high volume; composite key `(email_id, action, timestamp)`. |
| `interests` / `interest_categories` | `GET /lists/{list_id}/interest-categories[...]` | Reference data, snapshot only. |
| `tags`               | (member-level `tags` array; `GET /lists/{list_id}/tag-search`) | Reference data. |

Adding any of these follows the same auth/pagination/rate-limit patterns
documented above; the nested ones require an extra fan-out level (per campaign
or per list/segment).

---

## Sources and References

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://mailchimp.com/developer/marketing/docs/fundamentals/ | 2026-07-09 | Highest | API-key/Bearer/Basic auth, `<dc>` suffix → base URL derivation, OAuth 2 existence, 10 simultaneous-connection limit, 120s timeout, 429 behavior. |
| Official Docs | https://mailchimp.com/developer/marketing/docs/methods-parameters/ | 2026-07-09 | Highest | Pagination `count` (default 10, max 1000) + `offset` (default 0), `total_items` envelope, named result arrays, `fields`/`exclude_fields` mutual exclusivity and dot notation. |
| Official Docs | https://mailchimp.com/developer/marketing/api/campaigns/list-campaigns/ | 2026-07-09 | Highest | `GET /campaigns`, query params (`since_create_time`, `since_send_time`, `status`, `sort_field`, `sort_dir`, `fields`), campaign schema fields. |
| Official Docs | https://mailchimp.com/developer/marketing/api/reports/list-campaign-reports/ | 2026-07-09 | Highest | `GET /reports`, `since_send_time`/`before_send_time`, report schema incl. nested `bounces`/`opens`/`clicks`/`list_stats`. |
| Official Docs | https://mailchimp.com/developer/marketing/api/lists/get-lists-info/ | 2026-07-09 | Highest | `GET /lists`, `since_date_created`/`before_date_created`, list/audience schema incl. `stats` struct. |
| Official Docs | https://mailchimp.com/developer/marketing/api/list-members/list-members-info/ | 2026-07-09 | Highest | `GET /lists/{list_id}/members` (nested), `since_last_changed`/`before_last_changed`/`status`, member schema, `id` = MD5 of lowercased email. |
| Airbyte | https://docs.airbyte.com/integrations/sources/mailchimp | 2026-07-09 | High | Stream list (campaigns, reports, lists, list_members, +automations/segments/unsubscribes/email_activity/tags/interests), incremental support, primary keys, 10-connection limit, concurrent-threads default 2 (range 2–10). |
| Airbyte (source) | https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/mailchimp.md | 2026-07-09 | High | Per-stream PKs (`id`; composite keys for email_activity/unsubscribes), full-refresh + incremental modes. |
| Fivetran | https://fivetran.com/docs/applications/mailchimp | 2026-07-09 | High | Core entity set (campaigns, lists, members, segments, unsubscribes, activity), member `source`/`unsubscribe_reason` columns, excluded large tables. |
| Fivetran (dbt) | https://github.com/fivetran/dbt_mailchimp | 2026-07-09 | Medium | Cross-reference on modeled entities and relationships. |
| Community | https://yizeng.me/2016/04/30/work-with-mailchimp-api-3-0s-pagination/ | 2026-07-09 | Medium | Confirmed offset/count pagination pattern and `total_items` usage (corroborates official docs). |

### Conflict resolution notes

- **`email_id` vs `unique_email_id`**: Airbyte/Fivetran vocabulary references a
  member `email_id`; the official v3 payload uses `unique_email_id` for the
  stable per-email id and `id` (MD5 of lowercased email) as the record key.
  Official docs prioritized — connector keys on `id` (composite `(list_id,id)`).
- **Incremental cursors**: Airbyte marks campaigns/reports/lists incremental but
  does not publish exact cursor fields. Cursors here are taken from the official
  filter parameters (`since_create_time`, `since_send_time`, `since_date_created`,
  `since_last_changed`), which is the authoritative source. The mutation caveat
  (creation-only cursors on campaigns/reports/lists) is called out explicitly.
