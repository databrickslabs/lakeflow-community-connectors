# Klaviyo API Documentation

## Table of Contents

1. [Authorization](#authorization)
2. [Cross-Cutting API Patterns](#cross-cutting-api-patterns)
3. [Object List & Table Scope](#object-list--table-scope)
4. [Table Specifications](#table-specifications)
   - [profiles](#profiles)
   - [events](#events)
   - [lists](#lists)
   - [campaigns](#campaigns)
   - [metrics](#metrics)
   - [flows](#flows)
   - [segments](#segments)
   - [templates](#templates)
5. [Field Type Mapping](#field-type-mapping)
6. [Deferred Tables](#deferred-tables)
7. [Research Log](#research-log)

---

## Authorization

**Method:** Private API Key (server-side only)

All requests to `https://a.klaviyo.com/api/` require two headers:

| Header | Value | Notes |
|--------|-------|-------|
| `Authorization` | `Klaviyo-API-Key <pk_…>` | Private key from Klaviyo account settings. Prefix must be included literally. |
| `revision` | `2024-10-15` (recommended pin) | ISO 8601 date string identifying the API version. Required on every request. Omitting it defaults to the latest revision, which can cause unexpected breaking changes. |

**Key formats:**
- Private keys (server-side, `pk_…`): used for all `/api/` endpoints documented here.
- Public keys (6-character company ID): used only for client-side `/client/` endpoints (not relevant to data ingestion).

**Scopes:** Klaviyo private keys can be scoped. The minimum read scopes needed are:
`profiles:read`, `events:read`, `lists:read`, `campaigns:read`, `metrics:read`, `flows:read`, `segments:read`, `templates:read`

**Example request:**

```http
GET https://a.klaviyo.com/api/profiles/
Authorization: Klaviyo-API-Key pk_abc123...
revision: 2024-10-15
accept: application/json
```

**Alternatives noted:** OAuth is available for Klaviyo tech partners. Not recommended for internal connectors — private key auth is simpler and sufficient.

---

## Cross-Cutting API Patterns

### Response Format: JSON:API

All responses follow the [JSON:API 1.0](https://jsonapi.org/) specification:

```json
{
  "data": [
    {
      "type": "profile",
      "id": "01GDDKASAP8TKDDA2GRZDSVP4H",
      "attributes": {
        "email": "user@example.com",
        "updated": "2024-01-15T12:00:00+00:00"
      },
      "relationships": {
        "lists": { "links": { "related": "https://a.klaviyo.com/api/profiles/01G.../lists/" } }
      },
      "links": { "self": "https://a.klaviyo.com/api/profiles/01G.../" }
    }
  ],
  "links": {
    "self": "https://a.klaviyo.com/api/profiles/",
    "next": "https://a.klaviyo.com/api/profiles/?page%5Bcursor%5D=bmV4dDo6aWQ6Ok43dW1iVw",
    "prev": null
  }
}
```

- `data`: array (list endpoints) or single object (single-resource endpoints)
- `data[].type`: resource type string (e.g., `"profile"`, `"event"`, `"campaign"`)
- `data[].id`: opaque string — the primary key
- `data[].attributes`: all scalar fields live here
- `data[].relationships`: links to related resources by type/id
- `links.next` / `links.prev`: pagination cursors (null when no more pages)
- `included`: present only when `include` query param is used; contains embedded related resources

### Pagination: Link-Cursor Model

Klaviyo uses **opaque cursor pagination**. Do not construct page cursors manually.

**How it works:**
1. Make first request (no `page[cursor]`).
2. If `links.next` is non-null, extract the `page[cursor]` value from the URL and pass it in the next request.
3. Stop when `links.next` is null.

```
GET /api/profiles/?page[size]=100
→ links.next = "https://a.klaviyo.com/api/profiles/?page%5Bcursor%5D=bmV4dDo6aWQ6Ok43dW1iVw"

GET /api/profiles/?page[size]=100&page[cursor]=bmV4dDo6aWQ6Ok43dW1iVw
→ links.next = null  (last page)
```

**Key point:** The cursor value is opaque and URL-encoded. Parse it from `links.next` by extracting the `page[cursor]` query parameter, then pass it verbatim in subsequent requests.

### Filter Syntax

Klaviyo uses a **functional DSL** on the `filter` query parameter. All filter expressions should be URI-encoded.

**Operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `equals` | Exact match | `equals(status,"Sent")` |
| `greater-than` | Strict greater-than for datetimes/numbers | `greater-than(updated,2024-01-01T00:00:00Z)` |
| `greater-or-equal` | Greater-than or equal | `greater-or-equal(datetime,2024-01-01T00:00:00Z)` |
| `less-than` | Strict less-than | `less-than(created,2024-06-01T00:00:00Z)` |
| `less-or-equal` | Less-than or equal | `less-or-equal(timestamp,1704067200)` |
| `any` | Field equals any item in array | `any(id,["A","B","C"])` |
| `contains` | Substring match | `contains(name,"welcome")` |
| `starts-with` | Prefix match | `starts-with(name,"Newsletter")` |
| `ends-with` | Suffix match | `ends-with(name,"2024")` |
| `has` | Resource existence check | `has(profile)` |

**Combining filters (AND):** Use comma-separation (implicit) or explicit `and()` wrapper:
```
filter=greater-than(updated,2024-01-01T00:00:00Z),equals(archived,false)
filter=and(greater-than(updated,2024-01-01T00:00:00Z),equals(archived,false))
```

**DateTime format:** ISO 8601 RFC 3339, e.g., `2024-01-15T00:00:00Z` or `2024-01-15T00:00:00+00:00`.

**Important:** Not every operator is supported on every field. Supported operators are documented per-table below.

### Sparse Fieldsets

Reduce payload size by requesting only specific fields:
```
GET /api/profiles/?fields[profile]=email,updated,created
```
Format: `fields[<resource_type>]=field1,field2,...`

Most connectors should **not** use this — fetch all fields so schema is complete.

### Compound Documents (`include`)

The `include` parameter embeds related resources in the `included` array, avoiding extra round-trips. Example: `include=metric,profile` on the events endpoint embeds metric and profile objects.

**Recommendation for connectors: avoid `include`** and instead sync related tables separately. Using `include` triggers stricter rate limits and makes schema/parsing more complex. Fetch `profiles` and `metrics` as standalone tables; join by ID downstream.

### API Revision Strategy

Klaviyo follows a **2-year revision lifecycle**: Stable (year 1) → Deprecated (year 2) → Retired. Each revision is pinned via the `revision` header.

- **Pin your revision** in connector config. Do not rely on the default (latest) revision — a new default can break response shapes.
- **Recommended pin:** `2024-10-15` — confirmed stable as of May 2026.
- **Update cadence:** Plan to update the revision pin every 12–18 months. Subscribe to the [Klaviyo developer changelog](https://developers.klaviyo.com/en/docs/changelog).

### Rate Limits

Klaviyo uses a **fixed-window algorithm** with two windows: burst (per-second) and steady (per-minute). Limits are per-account.

**Standard tiers (burst/steady):**

| Tier | Burst | Steady | Endpoints using it |
|------|-------|--------|-------------------|
| XL | 350/s | 3,500/m | `events` |
| L | 75/s | 750/m | `profiles`, `lists`, `segments`, `templates` |
| M | 10/s | 150/m | `campaigns`, `metrics` |
| S | 3/s | 60/m | `flows` |

**429 handling:**
- When rate-limited, the API returns HTTP 429 with a `Retry-After` header (integer seconds).
- Do **not** immediately retry — this will always receive another 429.
- Implement **exponential backoff with jitter**: start at `Retry-After` seconds, double on each retry, add random jitter (±20%).

**Rate limit response headers (on non-429 responses):**
- `RateLimit-Limit`: total allowance for the period
- `RateLimit-Remaining`: approximate remaining requests
- `RateLimit-Reset`: seconds until window resets

**Stricter limits apply when using:**
- `additional-fields[profile]=predictive_analytics`: drops profiles to 10/s burst, 150/m steady
- `include` parameter on any endpoint

---

## Object List & Table Scope

### Table Selection Rationale

The following 8 tables were selected based on:
1. Coverage by both Airbyte and Fivetran Klaviyo connectors (validated business value)
2. Core business objects in the Klaviyo data model
3. API homogeneity — all use the same JSON:API pattern, cursor pagination, and filter DSL

| Table | Ingestion Type | Cursor Field | Notes |
|-------|---------------|--------------|-------|
| `profiles` | cdc | `updated` | Core customer records |
| `events` | append | `datetime` | High-volume event log; append-only |
| `lists` | cdc | `updated` | Subscriber list definitions |
| `campaigns` | cdc | `updated_at` | Email/SMS campaign objects |
| `metrics` | snapshot | — | ~100s of records; full reload acceptable |
| `flows` | cdc | `updated` | Automation flow definitions |
| `segments` | cdc | `updated` | Dynamic audience segments |
| `templates` | cdc | `updated` | Email/SMS message templates |

All 8 tables share the same JSON:API response shape, link-cursor pagination, and filter DSL. No tables are deferred in this batch (see [Deferred Tables](#deferred-tables) for analytics/reporting endpoints that use a different paradigm).

---

## Table Specifications

---

### `profiles`

**Description:** Customer contact records — the central object in Klaviyo. Each profile represents one identifiable person, identified by email, phone, or external_id.

#### Endpoint

```
GET https://a.klaviyo.com/api/profiles/
GET https://a.klaviyo.com/api/profiles/{profile_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | No | string | — | — | See filter fields below |
| `sort` | No | string | — | — | `id`, `created`, `updated`, `email`; prefix `-` for desc |
| `fields[profile]` | No | string[] | all fields | — | Sparse fieldsets |
| `additional-fields[profile]` | No | string[] | — | — | `subscriptions`, `predictive_analytics` (triggers lower rate limit) |
| `include` | No | string[] | — | — | Supported: `lists`, `segments` (avoid in connectors) |
| `page[cursor]` | No | string | — | — | Opaque cursor from `links.next` |
| `page[size]` | No | integer | 20 | 100 | Records per page |

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `id` | `any`, `equals` |
| `email` | `any`, `equals` |
| `phone_number` | `any`, `equals` |
| `external_id` | `any`, `equals` |
| `_kx` | `equals` |
| `created` | `greater-than`, `less-than` |
| `updated` | `greater-than`, `less-than` |
| `subscriptions.email.marketing.suppression.reason` | `equals` |
| `subscriptions.email.marketing.suppression.timestamp` | `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |

**Incremental filter example:**
```
filter=greater-than(updated,2024-01-15T00:00:00Z)
sort=updated
```

#### Response Shape

```json
{
  "data": [
    {
      "type": "profile",
      "id": "01GDDKASAP8TKDDA2GRZDSVP4H",
      "attributes": {
        "email": "user@example.com",
        "phone_number": "+15005550006",
        "external_id": "ext_12345",
        "first_name": "Jane",
        "last_name": "Doe",
        "organization": "Acme Corp",
        "locale": "en-US",
        "title": "Marketing Manager",
        "image": "https://example.com/avatar.jpg",
        "created": "2023-06-15T10:00:00+00:00",
        "updated": "2024-01-15T12:00:00+00:00",
        "last_event_date": "2024-01-14T09:30:00+00:00",
        "location": {
          "address1": "123 Main St",
          "address2": "Suite 400",
          "city": "Boston",
          "country": "United States",
          "region": "Massachusetts",
          "zip": "02101",
          "timezone": "America/New_York",
          "latitude": "42.360082",
          "longitude": "-71.058880",
          "ip": "1.2.3.4"
        },
        "properties": {
          "custom_field_1": "value",
          "loyalty_tier": "Gold"
        }
      },
      "relationships": {
        "lists": { "links": { "related": "https://a.klaviyo.com/api/profiles/01G.../lists/" } },
        "segments": { "links": { "related": "https://a.klaviyo.com/api/profiles/01G.../segments/" } }
      },
      "links": { "self": "https://a.klaviyo.com/api/profiles/01G.../" }
    }
  ],
  "links": { "self": "...", "next": "...", "prev": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.email` | string (nullable) | |
| `attributes.phone_number` | string (nullable) | E.164 format |
| `attributes.external_id` | string (nullable) | Caller-supplied ID |
| `attributes.anonymous_id` | string (nullable) | Klaviyo-issued anonymous identifier; live-verified on revision `2024-10-15` and present in our schemas. Not listed in the public OpenAPI spec at time of writing. |
| `attributes.first_name` | string (nullable) | |
| `attributes.last_name` | string (nullable) | |
| `attributes.organization` | string (nullable) | |
| `attributes.locale` | string (nullable) | IETF BCP 47 |
| `attributes.title` | string (nullable) | |
| `attributes.image` | string (nullable) | URL |
| `attributes.created` | datetime (nullable) | ISO 8601 |
| `attributes.updated` | datetime (nullable) | ISO 8601; **cursor field** |
| `attributes.last_event_date` | datetime (nullable) | ISO 8601 |
| `attributes.location.address1` | string (nullable) | |
| `attributes.location.address2` | string (nullable) | |
| `attributes.location.city` | string (nullable) | |
| `attributes.location.country` | string (nullable) | |
| `attributes.location.region` | string (nullable) | State/province |
| `attributes.location.zip` | string (nullable) | |
| `attributes.location.timezone` | string (nullable) | IANA tz |
| `attributes.location.latitude` | string (nullable) | |
| `attributes.location.longitude` | string (nullable) | |
| `attributes.location.ip` | string (nullable) | |
| `attributes.properties` | object (nullable) | Custom key/value pairs |

#### Ingestion Strategy

- **Type:** `cdc` (incremental upsert)
- **Cursor field:** `updated`
- **Sort:** `sort=updated` (ascending) ensures pages are in cursor order
- **Filter:** `filter=greater-than(updated,<last_cursor>)`
- **Primary key:** `id`
- **Lookback:** Add a short lookback (e.g., 1–5 minutes) to catch late-arriving updates
- **Page size:** 100 (maximum)

#### Rate Limits

- Standard: 75/s burst, 750/m steady
- With `predictive_analytics`: 10/s burst, 150/m steady — avoid unless needed

---

### `events`

**Description:** Immutable event log — every action a profile takes (Placed Order, Opened Email, Clicked Link, etc.). This is the highest-volume table. Events are append-only and are never modified after creation.

#### Endpoint

```
GET https://a.klaviyo.com/api/events/
GET https://a.klaviyo.com/api/events/{event_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | No | string | — | — | See filter fields below |
| `sort` | No | string | `datetime` | — | `datetime`, `-datetime`, `timestamp`, `-timestamp` |
| `fields[event]` | No | string[] | all fields | — | Sparse fieldsets |
| `fields[metric]` | No | string[] | — | — | If using `include=metric` |
| `fields[profile]` | No | string[] | — | — | If using `include=profile` |
| `include` | No | string[] | — | — | `metric`, `profile` (avoid in connectors) |
| `page[cursor]` | No | string | — | — | From `links.next` |
| `page[size]` | No | integer | 200 | 1000 | Records per page |

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `metric_id` | `equals` |
| `profile_id` | `equals` |
| `profile` | `has` |
| `datetime` | `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |
| `timestamp` | `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |

**Incremental filter example:**
```
filter=greater-or-equal(datetime,2024-01-15T00:00:00Z)
sort=datetime
```

**Note:** `datetime` accepts ISO 8601 strings; `timestamp` accepts Unix epoch integers (seconds). Use `datetime` for readability.

#### Response Shape

```json
{
  "data": [
    {
      "type": "event",
      "id": "QEsC8c",
      "attributes": {
        "timestamp": 1705320000,
        "event_properties": {
          "OrderId": "1234",
          "ItemNames": ["T-Shirt", "Hoodie"],
          "Value": 79.99
        },
        "datetime": "2024-01-15T12:00:00+00:00",
        "uuid": "unique-event-uuid"
      },
      "relationships": {
        "profile": {
          "data": { "type": "profile", "id": "01GDDKASAP8TKDDA2GRZDSVP4H" }
        },
        "metric": {
          "data": { "type": "metric", "id": "XnYkB2" }
        },
        "attributions": { "links": { "related": "..." } }
      },
      "links": { "self": "https://a.klaviyo.com/api/events/QEsC8c/" }
    }
  ],
  "links": { "self": "...", "next": "...", "prev": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.timestamp` | integer (nullable) | Unix epoch seconds |
| `attributes.datetime` | datetime (nullable) | ISO 8601; **cursor field** |
| `attributes.uuid` | string (nullable) | Deduplication ID; can be used as cursor |
| `attributes.event_properties` | object (nullable) | Arbitrary event payload |
| `relationships.profile.data.id` | string | FK → profiles.id |
| `relationships.metric.data.id` | string | FK → metrics.id |

#### Ingestion Strategy

- **Type:** `append` (events are immutable; no updates)
- **Cursor field:** `datetime`
- **Sort:** `sort=datetime` (ascending)
- **Filter:** `filter=greater-or-equal(datetime,<last_cursor>)`
- **Primary key:** `id`
- **Lookback:** Apply a 1-hour lookback on the cursor to handle late-arriving events (Klaviyo can ingest historical events out of order)
- **Page size:** 1000 (maximum) — use this for efficiency

#### Rate Limits

- 350/s burst, 3,500/m steady (XL tier — most permissive)

---

### `lists`

**Description:** Static subscriber lists. Lists are membership groups that profiles can be subscribed to or unsubscribed from.

#### Endpoint

```
GET https://a.klaviyo.com/api/lists/
GET https://a.klaviyo.com/api/lists/{list_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | No | string | — | — | See filter fields below |
| `sort` | No | string | — | — | `id`, `created`, `name`, `updated`; prefix `-` for desc |
| `fields[list]` | No | string[] | all fields | — | Sparse fieldsets |
| `additional-fields[list]` | No | string[] | — | — | `profile_count` (triggers stricter rate limit) |
| `include` | No | string[] | — | — | `tags`, `flow-triggers` |
| `page[cursor]` | No | string | — | — | From `links.next` |
| `page[size]` | No | integer | 10 | 10 | Max 10 per page |

**Note:** Maximum page size is 10. There are typically few hundred lists at most — full pagination is fast.

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `id` | `any`, `equals` |
| `name` | `any`, `equals` |
| `created` | `greater-than` |
| `updated` | `greater-than` |

**Incremental filter example:**
```
filter=greater-than(updated,2024-01-15T00:00:00Z)
sort=updated
```

#### Response Shape

```json
{
  "data": [
    {
      "type": "list",
      "id": "Y6nRLr",
      "attributes": {
        "name": "Newsletter Subscribers",
        "created": "2022-03-01T10:00:00+00:00",
        "updated": "2024-01-10T08:00:00+00:00",
        "opt_in_process": "double_opt_in"
      },
      "relationships": {
        "tags": { "links": { "related": "https://a.klaviyo.com/api/lists/Y6nRLr/tags/" } },
        "profiles": { "links": { "related": "https://a.klaviyo.com/api/lists/Y6nRLr/profiles/" } }
      },
      "links": { "self": "https://a.klaviyo.com/api/lists/Y6nRLr/" }
    }
  ],
  "links": { "self": "...", "next": null, "prev": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.name` | string (nullable) | |
| `attributes.created` | datetime (nullable) | ISO 8601 |
| `attributes.updated` | datetime (nullable) | ISO 8601; **cursor field** |
| `attributes.opt_in_process` | enum (nullable) | `double_opt_in` or `single_opt_in` |

#### Ingestion Strategy

- **Type:** `cdc`
- **Cursor field:** `updated`
- **Sort:** `sort=updated`
- **Filter:** `filter=greater-than(updated,<last_cursor>)`
- **Primary key:** `id`
- **Note:** The `filter` operator for `updated` is `greater-than` only (no `greater-or-equal`). Add a 1-second lookback on the cursor to avoid missing records with equal timestamps.

#### Rate Limits

- 75/s burst, 750/m steady (L tier)
- Stricter (unspecified) when using `additional-fields[list]=profile_count`

---

### `campaigns`

**Description:** Email and SMS marketing campaigns. Includes send status, scheduling, audience configuration, and tracking settings.

#### Endpoint

```
GET https://a.klaviyo.com/api/campaigns/
GET https://a.klaviyo.com/api/campaigns/{campaign_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | **Yes** | string | — | — | **`messages.channel` filter is required** |
| `sort` | No | string | — | — | `created_at`, `id`, `name`, `scheduled_at`, `updated_at`; prefix `-` for desc |
| `fields[campaign]` | No | string[] | all fields | — | Sparse fieldsets |
| `fields[campaign-message]` | No | string[] | — | — | If using `include=campaign-messages` |
| `include` | No | string[] | — | — | `campaign-messages`, `tags` |
| `page[cursor]` | No | string | — | — | From `links.next` |
| `page[size]` | No | integer | 100 | 100 | Max 100 per page |

**IMPORTANT:** The `filter` parameter is **required** and must include a channel specifier. On revision `2024-10-15` the filter accepts only two channel values (verified live):
- `equals(messages.channel,'email')`
- `equals(messages.channel,'sms')`

Klaviyo's general docs also describe `mobile_push` as a campaign channel, but supplying `equals(messages.channel,'mobile_push')` on this revision returns `HTTP 400 'channel' must be one of: email, sms (got mobile_push)`. The connector's `CAMPAIGN_CHANNELS` tuple (see `klaviyo_schemas.py`) reflects the live-accepted set. A connector syncing all campaigns must make **one paginated request per accepted channel** and union the results — two requests on revision `2024-10-15`.

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `id` | `any` |
| `messages.channel` | `equals` (**required**) |
| `name` | `contains` |
| `status` | `any`, `equals` |
| `archived` | `equals` |
| `created_at` | `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |
| `scheduled_at` | `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |
| `updated_at` | `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |

**Incremental filter example (email channel):**
```
filter=equals(messages.channel,'email'),greater-or-equal(updated_at,2024-01-15T00:00:00Z)
sort=updated_at
```

#### Response Shape

```json
{
  "data": [
    {
      "type": "campaign",
      "id": "01GRRV5AS28VWZ7YTFXK3DMP3Q",
      "attributes": {
        "name": "Spring Sale 2024",
        "status": "Sent",
        "archived": false,
        "audiences": {
          "included": ["Y6nRLr"],
          "excluded": []
        },
        "send_options": {
          "use_smart_sending": true,
          "ignore_unsubscribes": false
        },
        "tracking_options": {
          "add_tracking_params": true,
          "custom_tracking_params": [
            { "type": "dynamic", "name": "utm_campaign", "value": "spring_sale" },
            { "type": "static", "value": "newsletter" }
          ],
          "is_tracking_clicks": true,
          "is_tracking_opens": true
        },
        "send_strategy": {
          "method": "static",
          "options_static": {
            "datetime": "2024-03-20T09:00:00+00:00",
            "is_local": false,
            "send_past_recipients_immediately": false
          },
          "options_throttled": null,
          "options_sto": null
        },
        "created_at": "2024-03-01T10:00:00+00:00",
        "scheduled_at": "2024-03-20T09:00:00+00:00",
        "updated_at": "2024-03-19T15:00:00+00:00",
        "send_time": "2024-03-20T09:00:00+00:00"
      },
      "relationships": {
        "campaign-messages": { "links": { "related": "..." } },
        "tags": { "links": { "related": "..." } }
      },
      "links": { "self": "https://a.klaviyo.com/api/campaigns/01G.../" }
    }
  ],
  "links": { "self": "...", "next": "...", "prev": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.name` | string | |
| `attributes.status` | enum | `Draft`, `Scheduled`, `Sending`, `Sent`, `Cancelled` |
| `attributes.archived` | boolean | |
| `attributes.audiences.included` | string[] | List/segment IDs |
| `attributes.audiences.excluded` | string[] | List/segment IDs |
| `attributes.send_options.use_smart_sending` | boolean (nullable) | |
| `attributes.send_options.ignore_unsubscribes` | boolean (nullable) | |
| `attributes.tracking_options.add_tracking_params` | boolean (nullable) | Verified live on revision `2024-10-15`. Older docs name this `is_add_utm`. |
| `attributes.tracking_options.custom_tracking_params` | array of objects (nullable) | One entry per tracking param. Each has `type` (`"static"` or `"dynamic"`), `value`, and `name` (only present on dynamic entries). Older docs name this `utm_params`. |
| `attributes.tracking_options.is_tracking_clicks` | boolean (nullable) | |
| `attributes.tracking_options.is_tracking_opens` | boolean (nullable) | |
| `attributes.send_strategy.method` | enum (nullable) | `static`, `throttled`, `smart_send_time` — discriminates which `options_*` sub-object below is populated. |
| `attributes.send_strategy.options_static.datetime` | datetime (nullable) | Populated when `method = static`. Other `options_*` fields are `null` in this case. |
| `attributes.send_strategy.options_static.is_local` | boolean (nullable) | |
| `attributes.send_strategy.options_static.send_past_recipients_immediately` | boolean (nullable) | |
| `attributes.send_strategy.options_throttled.datetime` | datetime (nullable) | Populated when `method = throttled`. |
| `attributes.send_strategy.options_throttled.throttle_percentage` | integer (nullable) | |
| `attributes.send_strategy.options_sto.date` | string (nullable) | Populated when `method = smart_send_time`. |
| `attributes.created_at` | datetime | ISO 8601 |
| `attributes.scheduled_at` | datetime (nullable) | ISO 8601 |
| `attributes.updated_at` | datetime | ISO 8601; **cursor field** |
| `attributes.send_time` | datetime (nullable) | Actual send time |

**Note on field naming:** `campaigns` uses `created_at` / `updated_at` (with `_at` suffix), unlike most other Klaviyo resources which use `created` / `updated`. This is a known inconsistency in the API.

#### Ingestion Strategy

- **Type:** `cdc`
- **Cursor field:** `updated_at`
- **Sort:** `sort=updated_at`
- **Filter:** `equals(messages.channel,'email'),greater-or-equal(updated_at,<last_cursor>)` — run once per channel type and union results
- **Primary key:** `id`
- **Quirk:** Must make separate paginated requests per channel — on revision `2024-10-15` the accepted values are `email` and `sms` only. Store channel as an additional column or handle in the connector.

#### Rate Limits

- 10/s burst, 150/m steady (M tier)

---

### `metrics`

**Description:** Metric definitions — categories of events (e.g., "Placed Order", "Opened Email"). Used as a lookup/reference table to resolve `metric_id` values in events. Small table (typically under a few hundred records).

#### Endpoint

```
GET https://a.klaviyo.com/api/metrics/
GET https://a.klaviyo.com/api/metrics/{metric_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | No | string | — | — | See filter fields below |
| `fields[metric]` | No | string[] | all fields | — | Sparse fieldsets |
| `include` | No | string[] | — | — | `flow-triggers` |
| `page[cursor]` | No | string | — | — | From `links.next` |
| `page[size]` | No | integer | — | 200 | Default unspecified; max 200 |

**Note:** No sort parameter documented. The API does not support sorting on metrics.

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `integration.name` | `equals` |
| `integration.category` | `equals` |

**Example:** `filter=equals(integration.name,"Klaviyo")`

No timestamp filter is available — metrics cannot be incrementally filtered by creation/update time.

#### Response Shape

```json
{
  "data": [
    {
      "type": "metric",
      "id": "XnYkB2",
      "attributes": {
        "name": "Placed Order",
        "created": "2020-01-15T10:00:00+00:00",
        "updated": "2023-06-01T08:00:00+00:00",
        "integration": {
          "id": "Shopify",
          "name": "Shopify",
          "category": "eCommerce"
        }
      },
      "links": { "self": "https://a.klaviyo.com/api/metrics/XnYkB2/" }
    }
  ],
  "links": { "self": "...", "next": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.name` | string (nullable) | Human-readable metric name |
| `attributes.created` | datetime (nullable) | ISO 8601 |
| `attributes.updated` | datetime (nullable) | ISO 8601 |
| `attributes.integration.id` | string (nullable) | Integration identifier |
| `attributes.integration.name` | string (nullable) | e.g., `"Shopify"`, `"API"`, `"Klaviyo"` |
| `attributes.integration.category` | string (nullable) | e.g., `"eCommerce"`, `"Internal"`, `"API"` |

#### Ingestion Strategy

- **Type:** `snapshot` (full reload on each sync)
- **Cursor field:** none — no timestamp filter available
- **Primary key:** `id`
- **Rationale:** Metrics are a small, stable lookup table (typically <200 records). No cursor filter is supported. Full reload is cheap and correct.

#### Rate Limits

- 10/s burst, 150/m steady (M tier)

---

### `flows`

**Description:** Marketing automation flows — multi-step automated sequences triggered by events, dates, or list membership. Includes flow status and trigger configuration.

#### Endpoint

```
GET https://a.klaviyo.com/api/flows/
GET https://a.klaviyo.com/api/flows/{flow_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | No | string | — | — | See filter fields below |
| `sort` | No | string | — | — | `created`, `id`, `name`, `status`, `trigger_type`, `updated`; prefix `-` for desc |
| `fields[flow]` | No | string[] | all fields | — | Sparse fieldsets |
| `fields[flow-action]` | No | string[] | — | — | If using `include=flow-actions` |
| `include` | No | string[] | — | — | `flow-actions`, `tags` |
| `page[cursor]` | No | string | — | — | From `links.next` |
| `page[size]` | No | integer | 50 | 50 | Max 50 per page |

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `id` | `any` |
| `name` | `contains`, `ends-with`, `equals`, `starts-with` |
| `status` | `equals` |
| `archived` | `equals` |
| `created` | `equals`, `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |
| `updated` | `equals`, `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |
| `trigger_type` | `equals` |

**Incremental filter example:**
```
filter=greater-or-equal(updated,2024-01-15T00:00:00Z)
sort=updated
```

#### Response Shape

```json
{
  "data": [
    {
      "type": "flow",
      "id": "RARMWY",
      "attributes": {
        "name": "Welcome Series",
        "status": "live",
        "archived": false,
        "created": "2021-11-01T09:00:00+00:00",
        "updated": "2024-01-10T14:00:00+00:00",
        "trigger_type": "Added to List"
      },
      "relationships": {
        "flow-actions": { "links": { "related": "..." } },
        "tags": { "links": { "related": "..." } }
      },
      "links": { "self": "https://a.klaviyo.com/api/flows/RARMWY/" }
    }
  ],
  "links": { "self": "...", "next": null, "prev": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.name` | string | |
| `attributes.status` | enum | `draft`, `manual`, `live` |
| `attributes.archived` | boolean | |
| `attributes.created` | datetime | ISO 8601 |
| `attributes.updated` | datetime | ISO 8601; **cursor field** |
| `attributes.trigger_type` | enum | `Added to List`, `Date Based`, `Low Inventory`, `Metric`, `Price Drop`, `Unconfigured` |

#### Ingestion Strategy

- **Type:** `cdc`
- **Cursor field:** `updated`
- **Sort:** `sort=updated`
- **Filter:** `filter=greater-or-equal(updated,<last_cursor>)`
- **Primary key:** `id`
- **Page size:** 50 (maximum) — flows are typically few hundred records; full pagination is fast

#### Rate Limits

- 3/s burst, 60/m steady (S tier — most restrictive of the 8 tables)
- Connectors should pace requests aggressively on this endpoint

---

### `segments`

**Description:** Dynamic audience segments — automatically computed lists of profiles matching defined conditions. Like lists, but condition-based rather than static membership.

#### Endpoint

```
GET https://a.klaviyo.com/api/segments/
GET https://a.klaviyo.com/api/segments/{segment_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | No | string | — | — | See filter fields below |
| `sort` | No | string | — | — | `created`, `id`, `name`, `updated`; prefix `-` for desc |
| `fields[segment]` | No | string[] | all fields | — | Sparse fieldsets |
| `include` | No | string[] | — | — | `flow-triggers`, `tags` |
| `page[cursor]` | No | string | — | — | From `links.next` |
| `page[size]` | No | integer | 10 | 10 | Max 10 per page |

**Note:** Maximum page size is 10. Segments are typically a small catalog; full pagination with this page size is still fast.

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `id` | `any`, `equals` |
| `name` | `any`, `equals` |
| `created` | `greater-than` |
| `updated` | `greater-than` |
| `is_active` | `any`, `equals` |
| `is_starred` | `equals` |

**Incremental filter example:**
```
filter=greater-than(updated,2024-01-15T00:00:00Z)
sort=updated
```

**Note:** Like `lists`, the `updated` operator is `greater-than` only (no `greater-or-equal`). Apply 1-second lookback.

#### Response Shape

```json
{
  "data": [
    {
      "type": "segment",
      "id": "Nrh5b7",
      "attributes": {
        "name": "High-Value Customers",
        "definition": {
          "condition_groups": [
            {
              "conditions": [
                {
                  "type": "metric",
                  "metric_id": "XnYkB2",
                  "operator": "greater-than",
                  "value": 500
                }
              ]
            }
          ]
        },
        "created": "2022-05-10T08:00:00+00:00",
        "updated": "2024-01-12T11:00:00+00:00",
        "is_active": true,
        "is_processing": false,
        "is_starred": false
      },
      "relationships": {
        "tags": { "links": { "related": "..." } },
        "profiles": { "links": { "related": "..." } }
      },
      "links": { "self": "https://a.klaviyo.com/api/segments/Nrh5b7/" }
    }
  ],
  "links": { "self": "...", "next": null, "prev": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.name` | string (nullable) | |
| `attributes.definition` | object (nullable) | Segment condition tree |
| `attributes.definition.condition_groups` | array | Nested condition logic |
| `attributes.created` | datetime (nullable) | ISO 8601 |
| `attributes.updated` | datetime (nullable) | ISO 8601; **cursor field** |
| `attributes.is_active` | boolean | Whether segment is computed |
| `attributes.is_processing` | boolean | Whether currently recomputing |
| `attributes.is_starred` | boolean | UI favorite flag |

#### Ingestion Strategy

- **Type:** `cdc`
- **Cursor field:** `updated`
- **Sort:** `sort=updated`
- **Filter:** `filter=greater-than(updated,<last_cursor>)`
- **Primary key:** `id`
- **Note:** Segment `definition` is a nested object — serialize as JSON string for storage if the target schema doesn't support nested types

#### Rate Limits

- 75/s burst, 750/m steady (L tier)

---

### `templates`

**Description:** Email and SMS message templates. Includes HTML, plain text, and AMP versions plus metadata about the editor type used to create the template.

#### Endpoint

```
GET https://a.klaviyo.com/api/templates/
GET https://a.klaviyo.com/api/templates/{template_id}   (single record)
```

#### Query Parameters

| Parameter | Required | Type | Default | Max | Notes |
|-----------|----------|------|---------|-----|-------|
| `filter` | No | string | — | — | See filter fields below |
| `sort` | No | string | — | — | `created`, `id`, `name`, `updated`; prefix `-` for desc |
| `fields[template]` | No | string[] | all fields | — | Sparse fieldsets |
| `additional-fields[template]` | No | string[] | — | — | `definition` for drag-and-drop templates (omitted by default) |
| `page[cursor]` | No | string | — | — | From `links.next` |
| `page[size]` | No | integer | 10 | 10 | Max 10 per page |

#### Filter Fields & Operators

| Field | Operators |
|-------|-----------|
| `id` | `any`, `equals` |
| `name` | `any`, `contains`, `equals` |
| `created` | `equals`, `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |
| `updated` | `equals`, `greater-or-equal`, `greater-than`, `less-or-equal`, `less-than` |

**Incremental filter example:**
```
filter=greater-or-equal(updated,2024-01-15T00:00:00Z)
sort=updated
```

#### Response Shape

```json
{
  "data": [
    {
      "type": "template",
      "id": "dqMFPA",
      "attributes": {
        "name": "Spring Sale Email",
        "editor_type": "CODE",
        "html": "<html>...</html>",
        "text": "Spring Sale — Save 30%",
        "amp": null,
        "created": "2023-02-01T10:00:00+00:00",
        "updated": "2024-01-08T16:00:00+00:00"
      },
      "links": { "self": "https://a.klaviyo.com/api/templates/dqMFPA/" }
    }
  ],
  "links": { "self": "...", "next": null, "prev": null }
}
```

#### Schema

| Field Path | Type | Notes |
|------------|------|-------|
| `id` | string | Primary key |
| `attributes.name` | string | |
| `attributes.editor_type` | enum | `CODE`, `USER_DRAGGABLE`, `SYSTEM_DRAGGABLE` |
| `attributes.html` | string (nullable) | Rendered HTML body |
| `attributes.text` | string (nullable) | Plain text version |
| `attributes.amp` | string (nullable) | AMP email version |
| `attributes.definition` | object (nullable) | Only present if `additional-fields[template]=definition` requested and `editor_type=SYSTEM_DRAGGABLE` |
| `attributes.created` | datetime | ISO 8601 |
| `attributes.updated` | datetime | ISO 8601; **cursor field** |

**Note:** `html` can be large. If storage cost is a concern, use `fields[template]=id,name,editor_type,created,updated` to skip it initially.

#### Ingestion Strategy

- **Type:** `cdc`
- **Cursor field:** `updated`
- **Sort:** `sort=updated`
- **Filter:** `filter=greater-or-equal(updated,<last_cursor>)`
- **Primary key:** `id`

#### Rate Limits

- 75/s burst, 750/m steady (L tier)

---

## Field Type Mapping

Mapping from Klaviyo API types to standard types for schema inference:

| Klaviyo API Type | Standard Type | Notes |
|------------------|--------------|-------|
| `string` | `string` | |
| `string` (ISO 8601 datetime) | `timestamp` | Fields named `created`, `updated`, `datetime`, `*_at` |
| `integer` | `integer` | e.g., `timestamp` field on events (Unix seconds) |
| `number` | `double` | Numeric values in event properties |
| `boolean` | `boolean` | e.g., `archived`, `is_active` |
| `enum (string)` | `string` | Status fields, channel, trigger_type |
| `object` | `string` (JSON) | Nested objects: `event_properties`, `properties`, `definition`, `location` |
| `array of strings` | `string` (JSON array) or `array<string>` | e.g., `audiences.included` |
| `null` | nullable variant of base type | Most fields are nullable |

**Special behaviors:**
- `event_properties`: Arbitrary JSON object. Schema varies per event type. Store as JSON string.
- `properties` (profiles): Arbitrary custom key/value pairs. Store as JSON string.
- `definition` (segments): Nested condition tree. Store as JSON string.
- Datetime fields: All use ISO 8601 RFC 3339 with UTC offset. Parse to timestamp type.
- `location.latitude` / `location.longitude`: Typed as string in the API despite being numeric — safe to cast to `double`.

---

## Deferred Tables

The following Klaviyo API resources were intentionally excluded from this batch. They are candidates for a future implementation batch.

| Table | Reason Deferred |
|-------|----------------|
| `campaign_values_reports` | Uses a different API paradigm — POST to `/api/campaign-values-reports/` with a reporting query body. Aggregated analytics, not raw records. Different pagination and schema model. |
| `flow_series_reports` | Same analytics API paradigm as campaign values reports. POST-based query endpoint. |
| `global_exclusions` (suppression list) | Available via `/api/profiles/` with suppression filters, or a dedicated suppression endpoint. Supported by Airbyte; lower priority for initial batch. |
| `coupons` / `coupon_codes` | Smaller use case; not in core Airbyte or Fivetran stream sets. |
| `catalogs` | Large sub-API (catalog items, variants, categories). Relevant for eCommerce but adds scope complexity. |
| `forms` / `form_versions` | Added by Fivetran recently. POST-based or different endpoint structure. Lower priority. |
| `tags` | Reference/lookup table; small and queryable as relationships on other resources. |
| `webhooks` | Configuration objects, not data records. Low ingestion value. |

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://developers.klaviyo.com/en/reference/api_overview | 2026-05-11 | High | Auth headers, revision header, pagination model, response format, rate limit tiers, filter DSL |
| Official Docs | https://developers.klaviyo.com/en/reference/get_profiles | 2026-05-11 | High | Profiles endpoint params, filter fields/operators, rate limits (75/s, 750/m), page size (max 100) |
| Official Docs | https://developers.klaviyo.com/en/reference/get_events | 2026-05-11 | High | Events endpoint params, filter fields, sort options, rate limits (350/s, 3500/m), page size (max 1000) |
| Official Docs | https://developers.klaviyo.com/en/reference/get_lists | 2026-05-11 | High | Lists endpoint, rate limits (75/s, 750/m), page size (max 10), filter operators |
| Official Docs | https://developers.klaviyo.com/en/reference/get_campaigns | 2026-05-11 | High | Campaigns endpoint, required channel filter, rate limits (10/s, 150/m), page size (max 100), all filter fields |
| Official Docs | https://developers.klaviyo.com/en/reference/get_flows | 2026-05-11 | High | Flows endpoint, rate limits (3/s, 60/m), page size (max 50), all filter fields |
| Official Docs | https://developers.klaviyo.com/en/reference/get_metrics | 2026-05-11 | High | Metrics endpoint, rate limits (10/s, 150/m), filter fields (integration only) |
| Official Docs | https://developers.klaviyo.com/en/reference/get_segments | 2026-05-11 | High | Segments endpoint, rate limits (75/s, 750/m), page size (max 10), filter fields |
| Official Docs | https://developers.klaviyo.com/en/reference/get_templates | 2026-05-11 | High | Templates endpoint, rate limits (75/s, 750/m), page size (max 10), filter fields |
| Official Docs | https://developers.klaviyo.com/en/docs/filtering | 2026-05-11 | High | Filter DSL operators, AND combination (comma-separated), datetime format, escaping rules |
| Official Docs | https://developers.klaviyo.com/en/docs/rate_limits_and_error_handling | 2026-05-11 | High | Rate limit tiers (XS/S/M/L/XL), Retry-After header, 429 semantics, exponential backoff |
| Official Docs | https://developers.klaviyo.com/en/docs/api_versioning_and_deprecation_policy | 2026-05-11 | High | 2-year lifecycle, Stable/Deprecated/Retired phases, revision pinning best practices |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_campaigns.json | 2026-05-11 | High | Complete campaigns response schema, all attribute field names and types |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_flows.json | 2026-05-11 | High | Complete flows response schema, trigger_type enum values |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_profiles.json | 2026-05-11 | High | Complete profiles response schema including location sub-object |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_events.json | 2026-05-11 | High | Events schema — uuid, timestamp, datetime, event_properties |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_metrics.json | 2026-05-11 | High | Metrics schema — integration object structure |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_segments.json | 2026-05-11 | High | Segments schema — definition/condition_groups structure, is_active/is_processing |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_lists.json | 2026-05-11 | High | Lists schema — opt_in_process enum |
| OpenAPI Spec | https://raw.githubusercontent.com/klaviyo/openapi/main/openapi/stable/apis/get_templates.json | 2026-05-11 | High | Templates schema — editor_type enum, definition object |
| Airbyte OSS | https://github.com/airbytehq/airbyte/blob/master/docs/integrations/sources/klaviyo.md | 2026-05-11 | Medium | Confirmed core streams (campaigns, events, flows, lists, metrics, profiles, templates); sync modes; known quirks (predictive analytics rate limit, events lookback window) |
| Fivetran | https://fivetran.com/docs/connectors/applications/klaviyo | 2026-05-11 | Medium | Confirmed same core tables; notes on campaign re-import strategy; FORM/FORM_VERSION as newer additions |
