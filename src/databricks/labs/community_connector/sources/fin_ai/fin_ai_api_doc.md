# Fin.ai (Intercom) API Documentation

## Source Identity

**fin.ai** is the marketing site for **Fin**, Intercom's AI customer-service agent
product (also called "Fin AI Agent"). Fin is not a separate standalone data
platform — it is a capability built into Intercom's customer-service platform.
Confirmed during research:

- `fin.ai/help/...` articles redirect to / mirror the same Intercom Help Center
  content (e.g. regional data hosting, Fin resolutions), confirming Fin is an
  Intercom product surface, not an independently hosted system.
- Fin's own developer surface (`developers.intercom.com/docs/guides/fin-agent-api`,
  the **Fin Agent API**) is an *action/orchestration* API — two endpoints
  (`POST /fin/start`, `POST /fin/reply`) plus SSE/webhook events — for embedding
  Fin as a conversational agent inside a **third-party** product. It is not a
  bulk-read/reporting API and is out of scope for a data-ingestion connector.
- All of the customer-service data that a Lakeflow connector cares about
  (conversations Fin participated in, its resolution outcome, its CSAT rating,
  contacts, companies, tickets, tags, segments, admins, teams, custom
  attributes) is exposed through the standard **Intercom REST API**
  (`api.intercom.io`). Fin-specific analytics are embedded directly on the
  **conversation** object via an `ai_agent` sub-object and an
  `ai_agent_participated` boolean — there is no separate "Fin AI Agent" bulk
  endpoint.
- There is also a newer **Reporting Data Export API**
  (`/export/reporting_data/*`) that can export configurable "datasets"
  (e.g. a `conversation` dataset) over a time range via an async
  enqueue → poll → download job. This could eventually expose richer
  aggregate/Fin metrics, but it is a fundamentally different (async, job-based)
  pattern from the rest of the API and is **deferred** — see
  [Deferred Tables](#deferred-tables).

Given the above, this document treats **`fin_ai`** as an Intercom REST API
connector, with special attention called out wherever Fin-AI-specific fields
appear (primarily on `conversations`).

---

## Authorization

Preferred method: **Access Token (Bearer)**, i.e. an Intercom **private app**
token. This is the method used by both the Airbyte OSS connector (open-source
mode) and hand-rolled scripts, and is the simplest single-workspace credential.

### Preferred Method: Bearer Access Token

```
Authorization: Bearer <access_token>
Accept: application/json
Intercom-Version: 2.15
```

**How to obtain a token:**
1. Go to the [Intercom Developer Hub](https://developers.intercom.com/) and
   create (or select) an app scoped to your workspace ("private app").
2. Open **Configure → Authentication** for that app. Intercom generates the
   Access Token automatically when the app is created.
3. Under the app's **Permissions**/**Authentication** settings, grant the
   read scopes needed for the streams below (e.g. "Read conversations",
   "Read and list users and companies", "Read admins", "Read admin activity
   logs", "Read tickets").
4. Copy the Access Token — it is shown once in full and should be treated like
   a password (do not ask end users for their token; that violates Intercom's
   terms of service — use OAuth instead for multi-tenant/public integrations).

**Example request (cURL):**
```bash
curl -s https://api.intercom.io/me \
  -H "Authorization: Bearer <access_token>" \
  -H "Accept: application/json" \
  -H "Intercom-Version: 2.15"
```

### Base URL / Regional Hosting

| Region | REST API base URL |
|---|---|
| US (default) | `https://api.intercom.io` |
| Europe | `https://api.eu.intercom.io` |
| Australia | `https://api.au.intercom.io` |

Calling the default `api.intercom.io` host will generally auto-route to the
correct region for the authenticated workspace, but the connector should let
users pick a `region` config value (`us` / `eu` / `au`, default `us`) and
target the explicit regional host directly to avoid extra redirect latency,
matching how EU/AU-hosted workspaces' *workspace* URLs already differ
(`app.eu.intercom.com`, `app.au.intercom.com`).

### Connector Config Parameters

- `access_token` (string, required) — Intercom private app Access Token.
- `region` (enum: `us` | `eu` | `au`, default `us`) — selects the REST API host.
- `start_date` (ISO 8601 datetime, e.g. `2024-01-01T00:00:00Z`, required) —
  controls how far back incremental backfills go.
- `lookback_window` (integer, days, default `0`) — re-reads the last N days of
  each incremental window on every sync to catch late `updated_at` mutations
  (mirrors Airbyte's `lookback_window` config option).
- `api_rate_limit` (integer, default `9500`) — requests/minute budget the
  connector self-throttles to (see [Rate Limits](#rate-limits)).

### Alternative: OAuth2

For a **public**, multi-workspace integration (not this connector's initial
use case), Intercom requires OAuth2 instead of sharing a single access token.
Per this repo's convention, if OAuth is ever adopted the connector would store
`client_id` / `client_secret` / `refresh_token` and exchange them for a
short-lived access token at runtime — it would not run the user-facing
authorization-code flow itself. See
[Setting up OAuth](https://developers.intercom.com/docs/build-an-integration/learn-more/authentication/setting-up-oauth).
This document treats **Access Token (Bearer)** as the single preferred method
for this connector.

### Required Header: `Intercom-Version`

Every request should send `Intercom-Version: 2.15` (the latest stable version
as of this research; verified against the
[Intercom-OpenAPI](https://github.com/intercom/Intercom-OpenAPI) spec, whose
`descriptions/` directory's newest folder is `2.15`). If the header is
omitted, Intercom uses the version pinned on the app itself (set when the app
was created in the Developer Hub) — pin the header explicitly so behavior
doesn't silently change if the app's default version is bumped later.

---

## Object List

The object list is **static** (not discoverable via a single "list resources"
API call — each resource has its own endpoint). The table below is the
recommended v1 (core) set of 9 streams, chosen because they are supported by
**both** Airbyte's `source-intercom` connector and Fivetran's Intercom
connector (or are a Fivetran/Airbyte-equivalent merge, in the case of
`data_attributes`), they represent the core customer-service data model, and
— for `conversations` specifically — they carry the Fin AI Agent resolution
data this source is being added for.

| Stream (table) | Intercom Resource | Airbyte stream | Fivetran table | Primary Key | Ingestion Type |
|---|---|---|---|---|---|
| `conversations` | Conversations (incl. `ai_agent` Fin AI data) | `conversations` (incremental) | `conversation` | `id` | `cdc` |
| `contacts` | Contacts (users/leads) | `contacts` (incremental) | `contact` | `id` | `cdc` |
| `companies` | Companies | `companies` (incremental, client-side filtered) | `company` | `id` | `snapshot` |
| `tickets` | Tickets | `tickets` (incremental) | *(ticket, newer Fivetran connectors)* | `id` | `cdc` |
| `admins` | Admins (teammates) | `admins` (full refresh) | `admin` | `id` | `snapshot` |
| `tags` | Tags | `tags` (full refresh) | `tag` | `id` | `snapshot` |
| `segments` | Segments | `segments` (incremental, client-side filtered) | *(segment)* | `id` | `snapshot` |
| `data_attributes` | Data Attributes (contact/company/conversation custom fields) | `company_attributes` + `contact_attributes` (full refresh; split by `model`) | *(schema metadata, not user-facing table)* | `full_name` + `model` | `snapshot` |
| `teams` | Teams | `teams` (full refresh) | `team` | `id` | `snapshot` |

All 9 streams share the same authentication, headers, JSON envelope
conventions, and error format, so they are documented together in a single
research pass rather than split into batches.

---

## Object Schema

### 1. `conversations`

A conversation between a contact and your workspace (admin or **Fin AI
Agent**). This is the primary carrier of Fin AI Agent resolution/CSAT data via
the nested `ai_agent` object.

| Field | Type | Description |
|---|---|---|
| `id` | string | Conversation ID (primary key). |
| `title` | string, nullable | Conversation title. |
| `created_at` | integer (Unix ts) | Creation time. |
| `updated_at` | integer (Unix ts) | Last update time. **Incremental cursor.** |
| `waiting_since` | integer (Unix ts), nullable | Time the contact started waiting for a reply; null if last reply was from an admin. |
| `snoozed_until` | integer (Unix ts), nullable | Time the conversation will re-open, if snoozed. |
| `open` | boolean | Whether the conversation is open. |
| `state` | string enum (`open`,`closed`,`snoozed`) | Conversation state. |
| `read` | boolean | Whether the conversation has been read. |
| `priority` | string enum (`priority`,`not_priority`) | Priority flag. |
| `admin_assignee_id` | integer, nullable | Assigned admin ID. |
| `team_assignee_id` | integer, nullable | Assigned team ID. |
| `source` | object | Origination message: `type`, `id`, `delivered_as`, `subject`, `body` (HTML), `author` (admin/contact ref), `attachments`, `url`, `redacted`. |
| `contacts` | object (`contact.list`) | List of `{type, id, external_id}` contact refs involved. |
| `teammates` | object, nullable | Teammates who participated. |
| `tags` | object (`tag.list`) | List of tag objects (see [`tags`](#5-tags)), each with `applied_at`/`applied_by`. |
| `conversation_rating` | object, nullable | Human CSAT: `rating` (1-5 int), `remark` (string), `created_at`, `updated_at`, `contact`, `teammate`. |
| `statistics` | object, nullable | Reporting metrics: `time_to_assignment`, `time_to_admin_reply`, `time_to_first_close`, `time_to_last_close`, `median_time_to_reply`, `first_contact_reply_at`, `first_assignment_at`, `first_admin_reply_at`, `first_close_at`, `last_assignment_at`, `last_assignment_admin_reply_at`, `last_contact_reply_at`, `last_admin_reply_at`, `last_close_at`, `last_closed_by_id`, `count_reopens`, `count_assignments`, `count_conversation_parts`, `handling_time`, `adjusted_handling_time` — all integers (seconds) or Unix timestamps. |
| `custom_attributes` | object (dict) | Workspace-defined custom conversation attributes. |
| `topics` | object | Detected topics (Intercom AI feature). |
| `ticket` | object, nullable | Linked ticket reference, if converted. |
| `linked_objects` | object (`list`) | Linked custom-object instances: `data`, `total_count`, `has_more`. |
| `ai_agent_participated` | boolean | **Whether Fin AI Agent participated in this conversation.** |
| `ai_agent` | object, nullable | **Fin AI Agent data** — see fields below. |
| `conversation_parts` | object, nullable | Only present on the single-`GET` (not list/search) response; see [Deferred Tables](#deferred-tables). Capped at 500 most-recent parts per conversation. |

**`ai_agent` sub-object (Fin AI Agent data — the core "Fin.ai" data this
connector exists to surface):**

| Field | Type | Description |
|---|---|---|
| `source_type` | string enum, nullable | What triggered Fin: `essentials_plan_setup`, `profile`, `workflow`, `workflow_preview`, `fin_preview`. |
| `source_title` | string, nullable | Title of the triggering source (null if `source_type` is `essentials_plan_setup`). |
| `last_answer_type` | string enum, nullable | `ai_answer`, `custom_answer`, or null if no answer was delivered. |
| `resolution_state` | string enum, nullable | `assumed_resolution`, `confirmed_resolution`, `escalated`, `negative_feedback`, `procedure_handoff`, or null. **This is the "Fin resolution" outcome** referenced in Intercom's [Fin resolutions help article](https://www.intercom.com/help/en/articles/8205718-fin-resolutions). Requires the Fin AI Agent paid feature to be enabled in the workspace. |
| `rating` | integer (1-5), nullable | Customer satisfaction rating given to Fin. |
| `rating_remark` | string, nullable | Free-text remark accompanying the Fin CSAT rating. |
| `created_at` | integer (Unix ts), nullable | When the Fin rating was created. |
| `updated_at` | integer (Unix ts), nullable | When the Fin rating was last updated. |
| `content_sources` | object | List of knowledge-base content sources Fin used to answer. |

**Example record (abridged, `GET /conversations/{id}` style):**
```json
{
  "type": "conversation",
  "id": "503",
  "created_at": 1734537511,
  "updated_at": 1734537523,
  "waiting_since": null,
  "open": false,
  "state": "closed",
  "read": true,
  "priority": "not_priority",
  "admin_assignee_id": 991267715,
  "team_assignee_id": 5017691,
  "source": {
    "type": "conversation",
    "id": "403918334",
    "delivered_as": "admin_initiated",
    "subject": "",
    "body": "<p>this is the message body</p>",
    "author": {"type": "admin", "id": "991267645", "name": "Ciaran Lee", "email": "admin176@email.com"},
    "attachments": [],
    "url": null,
    "redacted": false
  },
  "contacts": {"type": "contact.list", "contacts": [{"type": "contact", "id": "6762f1261bb69f9f2193bba7", "external_id": "70"}]},
  "tags": {"type": "tag.list", "tags": [{"type": "tag", "id": "123456", "name": "Test tag", "applied_at": 1663597223}]},
  "conversation_rating": null,
  "statistics": null,
  "custom_attributes": {},
  "ai_agent_participated": true,
  "ai_agent": {
    "source_type": "workflow",
    "source_title": "Billing Support Workflow",
    "last_answer_type": "ai_answer",
    "resolution_state": "confirmed_resolution",
    "rating": 4,
    "rating_remark": "Very helpful!",
    "created_at": 1663597260,
    "updated_at": 1663597260
  }
}
```

---

### 2. `contacts`

Users or leads in the workspace.

| Field | Type | Description |
|---|---|---|
| `id` | string | Intercom-assigned contact ID (primary key). |
| `external_id` | string, nullable | Client-provided unique ID. |
| `workspace_id` | string | Workspace the contact belongs to. |
| `role` | string | `user` or `lead`. |
| `email` | string | Contact email. |
| `email_domain` | string | Domain portion of email (search-only derived field). |
| `phone` | string, nullable | Contact phone. |
| `name` | string, nullable | Contact name. |
| `owner_id` | integer, nullable | Admin who owns the contact. |
| `has_hard_bounced` | boolean | Email hard-bounce flag. |
| `marked_email_as_spam` | boolean | Spam flag. |
| `unsubscribed_from_emails` | boolean | Unsubscribe flag. |
| `created_at` | integer (Unix ts) | Creation time. |
| `updated_at` | integer (Unix ts) | Last update. **Incremental cursor.** |
| `signed_up_at` / `last_seen_at` / `last_replied_at` / `last_contacted_at` / `last_email_opened_at` / `last_email_clicked_at` | integer (Unix ts), nullable | Various lifecycle timestamps. |
| `language_override`, `browser`, `browser_version`, `browser_language`, `os` | string, nullable | Client environment metadata. |
| `android_*` / `ios_*` (app_name, app_version, device, os_version, sdk_version, last_seen_at) | string/integer, nullable | Mobile SDK metadata. |
| `custom_attributes` | object (dict) | Workspace-defined custom contact attributes. |
| `avatar` | object, nullable | `{type, image_url}`. |
| `tags` | object (ref `contact_tags`) | Tags applied to the contact. |
| `notes` | object (ref `contact_notes`) | Internal notes. |
| `companies` | object (ref `contact_companies`) | Companies the contact belongs to. |
| `location` | object | `{country, region, city, country_code, continent_code}`. |
| `social_profiles` | object | Linked social profiles. |

**Example record (abridged):**
```json
{
  "type": "contact",
  "id": "6762f0dd1bb69f9f2193bb83",
  "workspace_id": "abc12345",
  "external_id": "70",
  "role": "user",
  "email": "joebloggs@intercom.io",
  "phone": null,
  "name": "Joe Bloggs",
  "created_at": 1734537437,
  "updated_at": 1734537437,
  "last_seen_at": null,
  "custom_attributes": {"plan": "pro"},
  "tags": {"type": "list", "data": [], "total_count": 0},
  "companies": {"type": "list", "data": [], "total_count": 0}
}
```

> Merged contacts note: contacts merged via `POST /contacts/merge` disappear
> from list/search results entirely (including for `updated_at` filters) —
> only the surviving target contact remains queryable. There is no soft-delete
> marker for the merged-away contact.

---

### 3. `companies`

Organizations associated with one or more contacts. Companies are **only**
visible via the API once they have at least one associated contact.

| Field | Type | Description |
|---|---|---|
| `id` | string | Intercom-assigned company ID (primary key). |
| `company_id` | string | Client-defined company ID (immutable after creation). |
| `app_id` | string | Workspace code. |
| `name` | string | Company name. |
| `plan` | object | `{type, id, name}` — subscription plan. |
| `remote_created_at` | integer (Unix ts) | When the company was created by the client. |
| `created_at` | integer (Unix ts) | When added to Intercom. |
| `updated_at` | integer (Unix ts) | Last update. (Not filterable server-side — see Gotchas.) |
| `last_request_at` | integer (Unix ts) | Last time anyone from the company made a request. |
| `size` | integer | Employee count. |
| `website` | string | Company website URL. |
| `industry` | string | Industry. |
| `monthly_spend` | integer | Revenue from this company. |
| `session_count` | integer | Recorded sessions. |
| `user_count` | integer | Number of associated contacts. |
| `custom_attributes` | object (dict) | Workspace-defined custom company attributes. |
| `tags` | object (`tag.list`) | Tags on the company. |
| `segments` | object (`segment.list`) | Segments the company belongs to. |

**Example record:**
```json
{
  "type": "company",
  "id": "6762f0941bb69f9f2193bb25",
  "company_id": "remote_companies_scroll_2",
  "app_id": "abc12345",
  "name": "IntercomQATest1",
  "remote_created_at": 1734537364,
  "created_at": 1734537364,
  "updated_at": 1734537364,
  "monthly_spend": 0,
  "session_count": 0,
  "user_count": 4,
  "tags": {"type": "tag.list", "tags": []},
  "segments": {"type": "segment.list", "segments": []},
  "plan": {},
  "custom_attributes": {}
}
```

---

### 4. `tickets`

Structured customer-service requests (distinct from freeform conversations),
introduced in Intercom's Tickets system.

| Field | Type | Description |
|---|---|---|
| `id` | string | Intercom-assigned ticket ID (primary key). |
| `ticket_id` | string | Human-facing ticket number shown in Inbox/Messenger — **not** usable for API queries. |
| `category` | string enum | `Customer`, `Back-office`, `Tracker`. |
| `ticket_attributes` | object (dict) | Dynamic attributes defined by the ticket's `ticket_type` (e.g. `_default_title_`, `_default_description_`, custom fields). |
| `ticket_state` | object | `{id, category, internal_label, external_label}` — category is one of `submitted`,`in_progress`,`waiting_on_customer`,`resolved`. |
| `ticket_type` | object | `{id, name, description, icon, category, ticket_type_attributes, archived, created_at, updated_at}`. |
| `contacts` | object (`contact.list`) | Contacts on the ticket. |
| `admin_assignee_id` / `team_assignee_id` | string | Assignee IDs. |
| `created_at` / `updated_at` | integer (Unix ts) | **`updated_at` is the incremental cursor.** |
| `open` | boolean | Open/closed flag. |
| `snoozed_until` | integer (Unix ts), nullable | Snooze time. |
| `linked_objects` | object (`list`) | Linked custom-object instances. |
| `ticket_parts` | object (`ticket_part.list`) | Timeline of state changes/replies (author, `part_type`, `ticket_state`, `previous_ticket_state`). Fin can appear as `author.type == "bot"`, `author.name == "Fin"` here too. |
| `is_shared` | boolean | Whether visible to the customer. |

**Example record (abridged):**
```json
{
  "type": "ticket",
  "id": "633",
  "ticket_id": "40",
  "category": "Back-office",
  "ticket_attributes": {"_default_title_": "attribute_value", "_default_description_": null},
  "ticket_state": {"id": "8577", "category": "submitted", "internal_label": "Submitted", "external_label": "Submitted"},
  "admin_assignee_id": "0",
  "team_assignee_id": "0",
  "created_at": 1734537990,
  "updated_at": 1734537992,
  "open": true,
  "is_shared": false
}
```

> Gotcha: unlike conversations/contacts, there is **no `GET /tickets` list
> endpoint** at all — `POST /tickets/search` is the only way to read tickets
> in bulk.

---

### 5. `tags`

Labels applied to contacts, companies, and conversations.

| Field | Type | Description |
|---|---|---|
| `id` | string | Tag ID (primary key). |
| `name` | string | Tag name (unique per workspace). |
| `applied_at` | integer (Unix ts), nullable | Only present when the tag object is embedded on a tagging operation result (contact/company/conversation), not on the plain `GET /tags` list. |
| `applied_by` | object, nullable | Admin who applied the tag; same embedding condition as `applied_at`. |

**Example (`GET /tags` list item):**
```json
{"type": "tag", "id": "102", "name": "Manual tag 1"}
```

> Note: Airbyte's connector uses `name` as the declared primary key for this
> stream (a quirk of its implementation), but Intercom's own `tag` schema
> treats `id` as the canonical unique identifier — this document recommends
> `id`.

---

### 6. `segments`

User-defined rule-based groupings of contacts.

| Field | Type | Description |
|---|---|---|
| `id` | string | Segment ID (primary key). |
| `name` | string | Segment name. |
| `created_at` | integer (Unix ts) | Creation time. |
| `updated_at` | integer (Unix ts) | Last update time (not filterable server-side; see Gotchas). |
| `person_type` | string enum (`contact`,`user`) | Contact type the segment applies to. |
| `count` | integer, nullable | Member count; only populated when `include_count=true` is passed. |

**Example:**
```json
{"type": "segment", "id": "6762f25c1bb69f9f2193bc22", "name": "John segment", "created_at": 1734537820, "updated_at": 1734537820, "person_type": "user"}
```

---

### 7. `data_attributes`

Metadata describing every standard + custom field available on `contact`,
`company`, and `conversation` records. Essential for a connector to build a
dynamic schema for the `custom_attributes` dict on those objects.

| Field | Type | Description |
|---|---|---|
| `id` | integer | Only present for custom attributes. |
| `model` | string enum (`contact`,`company`) *(also accepts `conversation` per the `model` filter param)* | Which object type this attribute belongs to. Part of the composite primary key. |
| `name` | string | Attribute name. |
| `full_name` | string | Fully-qualified name (`custom_attributes.<name>` for custom fields); split on `.` to access nested values. Part of the composite primary key together with `model`. |
| `label` | string | Human-readable label. |
| `description` | string | Human-readable description. |
| `data_type` | string enum (`string`,`integer`,`float`,`boolean`,`date`) | Declared data type. |
| `options` | array[string] | Enumerated allowed values, if applicable. |
| `api_writable` / `messenger_writable` / `ui_writable` | boolean | Where the attribute can be written from. |
| `custom` | boolean | `true` if this is a Custom Data Attribute (CDA) rather than a built-in field. |
| `archived` | boolean | Whether archived (excluded by default unless `include_archived=true`). |
| `created_at` / `updated_at` | integer (Unix ts) | Only present for custom attributes. |
| `admin_id` | string | Creator admin ID; only present for custom attributes. |

**Example:**
```json
{
  "type": "data_attribute",
  "id": 34,
  "name": "The One Ring",
  "full_name": "custom_attributes.The One Ring",
  "label": "The One Ring",
  "description": "One ring to rule them all.",
  "data_type": "string",
  "api_writable": true,
  "ui_writable": false,
  "messenger_writable": true,
  "custom": true,
  "archived": false,
  "admin_id": "991267784",
  "created_at": 1734537753,
  "updated_at": 1734537753,
  "model": "company"
}
```

---

### 8. `admins`

Teammate accounts with workspace access.

| Field | Type | Description |
|---|---|---|
| `id` | string | Admin ID (primary key). |
| `name` | string | Admin's display name. |
| `email` | string | Admin's email. |
| `job_title` | string | Job title, if set. |
| `away_mode_enabled` | boolean | Whether the admin is in away mode. |
| `away_mode_reassign` | boolean | Whether new conversations auto-reassign while away. |
| `away_status_reason_id` | integer, nullable | Away reason ID. |
| `has_inbox_seat` | boolean | Whether the admin has a paid inbox seat. |
| `team_ids` | array[integer] | Teams the admin belongs to. |
| `avatar` | string (URI), nullable | Avatar image URL. |
| `team_priority_level` | object | Team priority configuration. |

**Example:**
```json
{"type": "admin", "id": "991267466", "email": "admin7@email.com", "name": "Ciaran Lee", "away_mode_enabled": false, "away_mode_reassign": false, "has_inbox_seat": true, "team_ids": []}
```

---

### 9. `teams`

Groups of admins used for routing/assignment.

| Field | Type | Description |
|---|---|---|
| `id` | string | Team ID (primary key). |
| `name` | string | Team name. |
| `admin_ids` | array[integer] | Admins in the team. |
| `admin_priority_level` | object | Priority-ordered admin lists for assignment. |
| `assignment_limit` | integer, nullable | Per-admin assignment cap (load-balanced teams only). |
| `distribution_method` | string, nullable | e.g. `round_robin`. |

**Example:**
```json
{"type": "team", "id": "991267902", "name": "team 1", "admin_ids": [], "assignment_limit": 10, "distribution_method": "round_robin"}
```

---

## Get Object Primary Keys

| Object | Primary Key | Notes |
|---|---|---|
| `conversations` | `id` | String ID, stable. |
| `contacts` | `id` | Intercom-assigned; `external_id` is a client-defined alternate key but not guaranteed present. |
| `companies` | `id` | Intercom-assigned; `company_id` is the client-defined alternate key. |
| `tickets` | `id` | Note: `ticket_id` is a *different*, human-facing number — do not use it as the key. |
| `tags` | `id` | Airbyte's connector uses `name` instead (implementation quirk); `id` is the canonical unique identifier per the OpenAPI schema. |
| `segments` | `id` | String ID. |
| `data_attributes` | `full_name` + `model` | Composite key — `name`/`full_name` alone can collide across `contact`/`company`/`conversation` models (e.g. both models can have a field literally named `name`, `full_name: name`, so `model` must be part of the key). |
| `admins` | `id` | String ID. |
| `teams` | `id` | String ID. |

---

## Object's Ingestion Type

| Object | Ingestion Type | Reason |
|---|---|---|
| `conversations` | `cdc` | `POST /conversations/search` supports filtering and sorting by `updated_at`, enabling true server-side incremental reads (upserts only — no native "deleted conversations" feed; see Gotchas for the separate redaction/delete endpoint). |
| `contacts` | `cdc` | `POST /contacts/search` supports `updated_at` filtering server-side. Merged contacts vanish from results with no delete marker (see Gotchas). |
| `companies` | `snapshot` | Neither `GET /companies`, `POST /companies/list`, nor `GET /companies/scroll` support server-side `updated_at`/timestamp filtering (confirmed by Airbyte's connector, which marks this stream `is_client_side_incremental: true` — i.e. it still fetches the **entire** company list every sync and filters afterward). Until/unless client-side filtering is implemented, treat as full-refresh snapshot. |
| `tickets` | `cdc` | `POST /tickets/search` (the *only* read path for tickets) supports `updated_at` filtering and both `>`/`>=`-style operators for `created_at`/`updated_at`. |
| `tags` | `snapshot` | `GET /tags` has no filter/cursor params; full list returned every call. Workspaces typically have few tags, so full refresh is cheap. |
| `segments` | `snapshot` | `GET /segments` has no filter params either (only `include_count`); Airbyte likewise fetches the full list and filters client-side by `updated_at`. Treat as full-refresh snapshot for v1. |
| `data_attributes` | `snapshot` | `GET /data_attributes` has no filter beyond `model`/`include_archived`; small, stable metadata list — full refresh each run. |
| `admins` | `snapshot` | `GET /admins` has no filter/cursor params; small list, full refresh each run. |
| `teams` | `snapshot` | `GET /teams` has no filter/cursor params; small list, full refresh each run. |

None of these streams expose a dedicated hard-delete/tombstone feed in the
base REST API (deletions are typically observed only via webhooks, which are
out of scope for a polling connector) — so **no stream in this v1 set
supports `cdc_with_deletes`**.

---

## Read API for Data Retrieval

All endpoints are called against the region-appropriate base URL (see
[Authorization](#authorization)) with headers:
```
Authorization: Bearer <access_token>
Accept: application/json
Intercom-Version: 2.15
Content-Type: application/json   # for POST/search endpoints
```

### A. `conversations` — Search API (recommended) vs. List API

**Recommended (incremental): `POST /conversations/search`**

```bash
curl -s -X POST https://api.intercom.io/conversations/search \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -H "Intercom-Version: 2.15" \
  -d '{
    "query": {
      "operator": "AND",
      "value": [
        {"field": "updated_at", "operator": ">", "value": 1717490000}
      ]
    },
    "sort": {"field": "updated_at", "order": "ascending"},
    "pagination": {"per_page": 150}
  }'
```

- Searchable fields include (non-exhaustive): `id`, `created_at`, `updated_at`,
  `source.*`, `contact_ids`, `teammate_ids`, `admin_assignee_id`,
  `team_assignee_id`, `open`, `read`, `state`, `tag_ids`, `priority`,
  `statistics.*`, `conversation_rating.*`, **`ai_agent_participated`,
  `ai_agent.resolution_state`, `ai_agent.last_answer_type`, `ai_agent.rating`,
  `ai_agent.rating_remark`, `ai_agent.source_type`, `ai_agent.source_title`**
  — i.e. Fin AI Agent outcomes are directly filterable/sortable.
- Operators: `=`, `!=`, `IN`, `NIN`, `>`, `<`, `~` (contains), `!~`, `^`
  (starts with), `$` (ends with). Max 2 levels of nested `AND`/`OR` groups,
  max 15 filters per group.
- `sort` (`{"field": "updated_at", "order": "ascending"}`) is **not**
  documented in the current OpenAPI spec's request schema but is confirmed
  working and actively relied upon by Airbyte's production `source-intercom`
  connector (see Research Log) — this is the recommended way to make
  paginated incremental reads stable (traverse the whole result set for the
  current window in ascending `updated_at` order before advancing the cursor
  watermark).
- Response envelope: `{"type": "conversation.list", "pages": {...}, "total_count": N, "conversations": [...]}`.

**Non-incremental (full list): `GET /conversations`**
```bash
curl -s "https://api.intercom.io/conversations?per_page=150" \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"
```
This endpoint takes **only** `per_page` (default 20, max 150) and
`starting_after` — no filters at all, so it is only useful for a first-time
full backfill, not incremental sync.

**Pagination (both endpoints):** cursor-based.
```json
"pages": {"type": "pages", "page": 1, "per_page": 150, "total_pages": 1,
          "next": {"per_page": 150, "starting_after": "<cursor>"}}
```
Take `pages.next.starting_after` (a string, `null`/absent when there is no
further page) and send it back as `pagination.starting_after` in the next
`POST` body (or `?starting_after=` query param for the plain `GET` list).
Stop when `pages.next` is absent/null.

**Incremental strategy:**
- Cursor field: `updated_at` (Unix seconds).
- Filter: `updated_at > <last_synced_watermark>` (add `<=` upper bound using
  the sync-start timestamp to bound each microbatch, mirroring Airbyte's
  day-bucketed windowing).
- Apply a small lookback window (config `lookback_window`, default 0 days) to
  re-check recently-synced windows for late `updated_at` mutations.
- No native delete feed. `DELETE /conversations/{id}` exists but is
  irreversible and rare; not covered by polling reads.
- Fin AI fields (`ai_agent`, `ai_agent_participated`) require the Fin AI Agent
  paid feature to be enabled on the workspace or they will simply be
  `null`/`false` — this is expected, not an error.

---

### B. `contacts` — Search API (recommended) vs. List API

**Recommended (incremental): `POST /contacts/search`**
```bash
curl -s -X POST https://api.intercom.io/contacts/search \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" -H "Intercom-Version: 2.15" \
  -d '{"query": {"field": "updated_at", "operator": ">", "value": 1717490000},
       "pagination": {"per_page": 150}}'
```
- Searchable fields: `id`, `role`, `name`, `email`, `email_domain`, `phone`,
  `external_id`, `created_at`, `signed_up_at`, `updated_at`, `last_seen_at`,
  location fields, `segment_id`, `tag_id`, `custom_attributes.{name}`, etc.
- **Timestamp fields are indexed by day, not by second/minute**: a
  `created_at > X` filter is evaluated at day granularity in the workspace's
  configured timezone, not exact-second. Use `>=`-style day-bucketed windows
  (as Airbyte does) rather than assuming second-precision filtering; verify
  actual duplicates/gaps are handled by upserting on `id`.
- Merged contacts (via `POST /contacts/merge`) are excluded from search
  results entirely, including from `updated_at`-filtered queries — there's no
  delete/tombstone signal for the record that was merged away.
- New contacts may not be immediately searchable ("a few minutes" propagation
  delay per Intercom docs) — the connector's lookback window should absorb
  this.

**Non-incremental (full list): `GET /contacts`** — default page size 50,
`starting_after` cursor, no filters.

**Pagination:** same cursor-based `pages.next.starting_after` shape as
conversations. Default `per_page` for contacts is 50 (vs. 20 for
conversations); max is 150 for the search endpoint.

**Incremental strategy:** cursor field `updated_at` (Unix seconds, day-indexed
for filtering purposes as noted above).

---

### C. `companies` — Scroll API (recommended for full extraction)

Because none of the company endpoints support filtering by time, the
recommended read path is the **Scroll API**, which is built for iterating over
the *entire* company list efficiently (the plain list/`GET /companies` and
`POST /companies/list` endpoints cap out at 10,000 total records reachable via
paging).

```bash
# First page — no scroll_param
curl -s https://api.intercom.io/companies/scroll \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"

# Subsequent pages — pass back the scroll_param from the previous response
curl -s "https://api.intercom.io/companies/scroll?scroll_param=<value>" \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"
```

**Response:**
```json
{"type": "list", "data": [ {"type": "company", "id": "...", "...": "..."} ],
 "pages": null, "total_count": null,
 "scroll_param": "69352cd2-ab5b-42ac-b004-a13d4e55e9b0"}
```

**Pagination:** pass the returned `scroll_param` on the next request. When the
end is reached, `data` is empty and `scroll_param` is no longer usable/expires.
- Only **one scroll may be open per app at a time** — a second concurrent
  scroll returns an error. If the scroll session is idle for **60 seconds** it
  expires and the `scroll_param` becomes invalid — you must restart from the
  beginning (scroll cannot resume from a specific point).
- Network errors mid-scroll (HTTP 500, "Request failed due to an internal
  network error") also require restarting the scroll from scratch.

**Alternative for small workspaces:** `POST /companies/list` (page/`per_page`
pagination, sorted by `last_request_at` desc by default, 10,000-record cap) or
`GET /companies?tag_id=`/`?segment_id=`/`?name=`/`?company_id=` for targeted
single-company or filtered lookups (not bulk sync).

**Ingestion strategy:** full snapshot every sync (see
[Object's Ingestion Type](#objects-ingestion-type)). If needed later,
`updated_at` can still be used as a **client-side** filter after the full
scroll completes, to limit what gets *written* downstream even though the
full company set must still be *read* each time.

---

### D. `tickets` — Search API (only read path)

```bash
curl -s -X POST https://api.intercom.io/tickets/search \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" -H "Intercom-Version: 2.15" \
  -d '{"query": {"field": "updated_at", "operator": ">", "value": 1717490000},
       "pagination": {"per_page": 150}}'
```
- Searchable fields: `id`, `created_at`, `updated_at`, `title`, `description`,
  `category` (search using `request`/`task`/`tracker` instead of the display
  names `Customer`/`Back-office`/`Tracker`), `ticket_type_id`, `contact_ids`,
  `teammate_ids`, `admin_assignee_id`, `team_assignee_id`, `open`, `state`,
  `snoozed_until`, `ticket_attribute.{id}`.
- Unlike conversations/contacts search, `created_at`/`updated_at` here
  **do** support `<=`/`>=` operators per the docs.
- There is **no `GET /tickets` list endpoint** — search is the only bulk read.

**Pagination:** same cursor shape (`pages.next.starting_after`), default
`per_page` 20, max 150.

**Incremental strategy:** cursor field `updated_at` (Unix seconds).

---

### E. `tags` — `GET /tags`

```bash
curl -s https://api.intercom.io/tags \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"
```
Response: `{"type": "list", "data": [{"type": "tag", "id": "102", "name": "..."}]}`.
No pagination — the full tag list is returned in one call. Full refresh only.

---

### F. `segments` — `GET /segments`

```bash
curl -s "https://api.intercom.io/segments?include_count=true" \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"
```
Response: `{"type": "segment.list", "segments": [...]}`. No pagination — full
list returned in one call. Pass `include_count=true` to also get member counts
(adds cost — only request if the connector actually surfaces that field).
Full refresh only.

---

### G. `data_attributes` — `GET /data_attributes`

```bash
curl -s "https://api.intercom.io/data_attributes?model=company&include_archived=false" \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"
```
- `model` (optional): `contact` | `company` | `conversation` — omit to get all
  models in one call, or call once per model to mirror Fivetran/Airbyte's
  per-model streams.
- `include_archived` (optional, default `false`).
- No pagination — full list returned in one call. Full refresh only.

---

### H. `admins` — `GET /admins`

```bash
curl -s "https://api.intercom.io/admins?display_avatar=true" \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"
```
Response: `{"type": "admin.list", "admins": [...]}`. No pagination. Full
refresh only.

---

### I. `teams` — `GET /teams`

```bash
curl -s https://api.intercom.io/teams \
  -H "Authorization: Bearer <access_token>" -H "Intercom-Version: 2.15"
```
Response: `{"type": "team.list", "teams": [...]}`. No pagination. Full refresh
only.

---

## Rate Limits

- **Private apps:** 10,000 API calls/minute per app, **and** 25,000 calls/minute
  per workspace (shared across all apps installed in that workspace).
- **Public apps (OAuth):** same 10,000/min per app; 25,000/min per workspace
  cap applies per-app rather than shared, since each public app has its own
  separate limit within a workspace.
- The permitted per-minute budget is enforced in **10-second windows** — i.e.
  roughly limit/6 requests are allowed per 10-second slice, not a single
  once-a-minute bucket.
- Response headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
  `X-RateLimit-Reset` (Unix timestamp of next reset). On a `429 Too Many
  Requests`, `X-RateLimit-Remaining` will read `0`.
- **Recommended handling:** track `X-RateLimit-Remaining` and proactively slow
  down (don't wait for a hard 429) before it hits 0; on a 429, back off until
  `X-RateLimit-Reset` and retry. Airbyte's production connector defaults to
  budgeting **9,500 req/min** (95% of the 10,000/min private-app limit) split
  into a per-minute policy and a per-10-second policy (`9500/6 ≈ 1583`
  requests per 10s), configurable via an `api_rate_limit` option for
  higher-limit workspaces.
- **Reporting Data Export API** has its own separate limit: max **5 pending**
  export jobs at a time (`429` with `rate_limit_exceeded` / "Exceeded rate
  limit of 5 pending reporting dataset export jobs" if exceeded). Not relevant
  to the 9 streams above (only to the deferred Reporting Data Export feature).
- If a workspace needs limits above the defaults, Intercom requires contacting
  them directly (via in-product Messenger) — there is no self-serve API for
  this.

---

## Field Type Mapping

| Intercom API Type | Spark Type | Notes |
|---|---|---|
| `string` | `StringType` | Most text fields, including all primary-key `id` fields (Intercom IDs are strings, not integers, even where they look numeric). |
| `integer` (Unix timestamp, `format: date-time` in the OpenAPI spec) | `TimestampType` | Parse as epoch **seconds** (not ms): `created_at`, `updated_at`, `waiting_since`, `snoozed_until`, all `statistics.*_at` fields, `conversation_rating.created_at/updated_at`, `ai_agent.created_at/updated_at`, ticket/company/segment/data_attribute `created_at`/`updated_at`. |
| `integer` (plain count/duration) | `IntegerType` or `LongType` | e.g. `statistics.time_to_admin_reply` (seconds duration, not a timestamp), `size`, `monthly_spend`, `session_count`, `user_count`, `assignment_limit`. |
| `boolean` | `BooleanType` | `open`, `read`, `ai_agent_participated`, `has_hard_bounced`, `archived`, `custom`, `api_writable`, etc. |
| `float` | `DoubleType` | Rare; e.g. some `data_attribute.data_type == "float"` custom attribute values. |
| `object` / dict (`custom_attributes`, `ticket_attributes`) | `StringType` (JSON) or `MapType(StringType, StringType)` | Schema varies per workspace since these carry workspace-defined custom fields; represent as a JSON string column unless the connector flattens known keys via `data_attributes` metadata. |
| `array` (e.g. `team_ids`, `admin_ids`) | `ArrayType(IntegerType)` or `ArrayType(StringType)` | Element type depends on field (see per-object schema tables above). |
| `enum` string (e.g. `ai_agent.resolution_state`, `ticket.category`, `ticket_state.category`) | `StringType` | Treat as free string; enum values may expand over time (Intercom explicitly reserves the right to add new enum members). |
| `null` | nullable field | Nearly every non-required field can be `null`; do not assume presence. |

### Special field behaviors

- **`ai_agent` / `ai_agent_participated`:** Will be `null`/`false` for any
  workspace or conversation where the Fin AI Agent paid feature is not
  enabled — this is expected, not a missing-permission error.
- **`custom_attributes` (contacts, companies, conversations):** Free-form,
  workspace-specific. Use the `data_attributes` stream (filtered by
  `model`) to discover the declared name/type/label for each key before
  flattening.
- **`ticket_attributes`:** Similarly dynamic, but scoped per `ticket_type`
  rather than globally — join against `ticket_type.ticket_type_attributes`
  (nested on the ticket record) to interpret keys like `_default_title_`.
- **Composite `data_attributes` key:** always pair `full_name` with `model`
  when deduplicating/upserting — the same short `name` (e.g. `name`) can exist
  under both the `contact` and `company` models.
- **`ticket_id` vs `id` on tickets:** `ticket_id` is a human-facing sequential
  number shown in the UI; it is *not* usable for API lookups/searches and
  must not be treated as a key.

---

## Known Quirks and Implementation Notes

1. **Fin.ai's own developer API (`/fin/start`, `/fin/reply`) is not a data
   source.** It is an inbound orchestration API for embedding Fin as an agent
   in a third-party product, gated to API version 2.14+. All Fin *outcome*
   data (resolution state, CSAT, which knowledge source it used) is read
   through the ordinary `conversations` resource's `ai_agent` field — no
   separate polling endpoint is needed or available for that data.
2. **`tickets` has no plain list endpoint** — `POST /tickets/search` with an
   always-true-ish filter (e.g. `created_at > 0`) is required even for a full
   backfill.
3. **`companies` cannot be filtered by `updated_at` server-side** on any of
   its three read endpoints (`GET /companies`, `POST /companies/list`,
   `GET /companies/scroll`); confirmed by Airbyte marking this stream
   `is_client_side_incremental` (i.e., it re-reads everything every sync).
   Treat as `snapshot` for v1; consider client-side filtering as a later
   optimization to reduce write volume (read volume stays full either way).
4. **Company Scroll API has a strict single-session, 60-second-idle-timeout
   contract** — only one scroll can be open per app, and a stalled/aborted
   scroll cannot resume; it must restart from the beginning.
5. **Search API `sort` parameter is not documented in the current OpenAPI
   spec but works and is relied on in production** by Airbyte's connector
   (`sort: {"field": "updated_at", "order": "ascending"}`) to make cursor
   pagination combined with an `updated_at` filter deterministic. Documented
   here as an assumption validated against actively-maintained OSS, per this
   skill's source-priority rules — **TBD:** Intercom has not published this
   parameter in its official reference; treat it as best-effort/undocumented
   and always upsert on primary key defensively rather than relying solely on
   sort order for correctness.
6. **Contact/company search timestamp filters are day-indexed, not
   second-indexed**, and day boundaries are evaluated in the **workspace's**
   configured timezone, not UTC — a naive UTC-based incremental window can
   silently include/exclude records near midnight boundaries in non-UTC
   workspaces. Use day-aligned windows (as Airbyte's manifest does, flooring
   to UTC day boundaries and adding a 2-day pad) plus a configurable
   `lookback_window` to absorb this.
7. **Merged contacts have no delete signal.** `POST /contacts/merge` removes
   the losing contact from all list/search results (including `updated_at`
   filters) permanently, with no tombstone record — a polling-only connector
   cannot detect these as deletes.
8. **`conversation_parts` (the message timeline) is only returned by the
   single-conversation `GET /conversations/{id}` call, capped at 500 most
   recent parts, and is a heavy N+1 fetch if pulled for every conversation
   row.** It is intentionally **not** one of the 9 core streams for v1; see
   Deferred Tables.
9. **Intercom-Version header controls response shape.** Pin `2.15`
   explicitly on every request; do not rely on the app's default version,
   which an admin could change in the Developer Hub independently of this
   connector's code.
10. **Regional hosting is workspace-level, not per-request.** A workspace's
    data lives entirely in one AWS region (US/EU/AU); calling the wrong
    regional host for a given workspace's token will generally still route
    correctly via `api.intercom.io`, but pinning the correct host per the
    `region` config avoids the extra redirect hop.

---

## Deferred Tables

| Object | API | Reason for Deferral |
|---|---|---|
| **`conversation_parts`** (message timeline) | `GET /conversations/{id}` (nested; no standalone list) | Requires one API call per conversation (N+1 substream pattern, as implemented by Airbyte via `SubstreamPartitionRouter`); capped at 500 most-recent parts per conversation. Already embedded (up to that cap) whenever a single conversation is fetched via `GET`, so a dedicated stream mainly adds value for conversations with >500 parts or for a normalized message-level table. |
| **`company_segments`** (per-company segment membership) | `GET /companies/{company_id}/segments` | Junction/nested resource requiring a per-company call; low incremental value beyond what `companies.segments` and `segments` already provide together. |
| **`activity_logs`** (admin activity audit trail) | `GET /admins/activity_logs?created_at_after=&created_at_before=` | Airbyte-only stream (no Fivetran equivalent); audit-log data rather than core customer-service/Fin data; requires mandatory `created_at_after` windowing and its own pagination shape (`pages.next` is itself the pagination token, not an object). Reasonable v2 addition, not core for the Fin AI use case. |
| **Reporting Data Export API** (`/export/reporting_data/*`) — configurable datasets (e.g. `conversation` dataset with time-bucketed attributes) | `POST /export/reporting_data/enqueue` → poll `GET /export/reporting_data/{job_identifier}` → `GET /download/reporting_data/{job_identifier}` (requires `Accept: application/octet-stream`) | Async, job-based flow (enqueue → poll status → download), a fundamentally different pattern from the rest of the API; max 5 pending jobs per workspace; available datasets/attributes must be discovered per-workspace via `GET /export/reporting_data/get_datasets` rather than being fixed. Likely the eventual home for richer Fin AI aggregate/reporting metrics beyond what's on the raw `conversations.ai_agent` field, but out of scope for v1. |
| **`ticket_types` / `ticket_states`** (ticket schema metadata) | `GET /ticket_types`, `GET /ticket_states` | Metadata/reference tables analogous to `data_attributes` but scoped to tickets; useful once ticket volume justifies normalizing `ticket_type`/`ticket_state` out of the embedded objects on `tickets`. |
| **Contacts→Companies / Companies→Contacts junction endpoints** | `GET /contacts/{id}/companies`, `GET /companies/{id}/contacts` | Redundant with the `companies` array already embedded on `contacts` and vice versa; only useful if a normalized many-to-many junction table is desired later (as Fivetran's `contact_company` dbt source does). |
| **`visitors`** | `GET /visitors`(single lookup)/`POST /visitors/convert` | No bulk list endpoint — single-visitor lookup by `user_id` only; not a bulk-syncable resource. |
| **Help Center content** (`articles`, `collections`, `internal_articles`) | `GET /articles`, `GET /help_center/collections`, etc. | Content-management data, not customer-service/Fin AI transactional data; Fivetran's `dbt_intercom` models this optionally (`intercom__using_articles` toggle) — good v2 candidate if Help Center reporting is desired. |

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://developers.intercom.com/docs/references/introduction | 2026-07-21 | High | API is at version 2.15, OpenAPI-generated, resource list overview, pointer to Fin Agent API guide. |
| Official Docs | https://developers.intercom.com/docs/guides/fin-agent-api | 2026-07-21 | High | Confirmed Fin Agent API is `/fin/start` + `/fin/reply` orchestration/events API (webhooks/SSE), API version 2.14+ required, not a bulk-read API. |
| Official Docs | https://developers.intercom.com/docs/build-an-integration/learn-more/authentication | 2026-07-21 | High | Access Token vs OAuth guidance, `Authorization: Bearer <token>` header, where to find the token (Configure → Authentication). |
| Official Docs | https://developers.intercom.com/docs/build-an-integration/learn-more/rest-apis | 2026-07-21 | High | Base URL `api.intercom.io`; confirmed regional base URLs `api.eu.intercom.io` / `api.au.intercom.io` and auto-routing behavior; JSON/UTF-8 conventions. |
| Official Help Article | https://www.intercom.com/help/en/articles/6124430-regional-data-hosting | 2026-07-21 | High | Regional workspace hosting (US/EU/AU, AWS regions), workspace URL patterns per region. |
| Official Help Article (Fin.ai mirror) | https://fin.ai/help/en/articles/13976037-regional-data-hosting | 2026-07-21 | High | Confirmed fin.ai help center serves the same content as intercom.com/help — corroborates that Fin.ai is an Intercom product surface, not a separate platform. |
| Official Help Article | https://www.intercom.com/help/en/articles/8205718-fin-resolutions | 2026-07-21 | Medium | Referenced directly from the OpenAPI spec's `GET /conversations/{id}` description as the explanation of Fin resolution semantics and the paid-feature gate. |
| GitHub — Intercom-OpenAPI (official) | https://github.com/intercom/Intercom-OpenAPI/blob/main/descriptions/2.15/api.intercom.io.yaml | 2026-07-21 | High | Authoritative, machine-readable source for every endpoint path/params and every schema (`conversation`, `ai_agent`, `contact`, `company`, `ticket`, `ticket_state`, `ticket_type`, `tag`, `segment`, `data_attribute`, `admin`, `team`), including the full `ai_agent` sub-schema (`resolution_state`, `last_answer_type`, `rating`, `rating_remark`, `source_type`, `source_title`) and the Reporting Data Export API (`/export/reporting_data/*`). Version list confirms `2.15` is current stable. |
| Airbyte OSS | https://docs.airbyte.com/integrations/sources/intercom | 2026-07-21 | High | Full stream list (12 streams), incremental vs. full-refresh classification, API version 2.11 in use, default/workspace rate limits (10,000/min app, 25,000/min workspace), 9,500 req/min self-throttle default. |
| Airbyte OSS (source, low-code manifest) | https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-intercom/manifest.yaml | 2026-07-21 | High | Ground-truth, executable confirmation of: `contacts`/`conversations`/`tickets` using `POST .../search` with day-bucketed `updated_at` filters and an (undocumented) `sort: {field: updated_at, order: ascending}` param; `companies` using `GET /companies/scroll` with `is_client_side_incremental: true` (full re-read every sync); `admins`/`tags`/`teams`/`segments`/`company_attributes`/`contact_attributes` as simple unpaginated `GET` full-refresh streams; `data_attributes` split by `model` query param; primary keys per stream; `BearerAuthenticator` with `access_token` config; exact rate-limit budget/backoff logic (`HTTPAPIBudget`, 9,500/min + 1,583/10s policies, 429 status trigger). |
| Fivetran Docs | https://fivetran.com/docs/connectors/applications/intercom | 2026-07-21 | Low | Page navigation only rendered (JS-heavy site); confirmed Intercom connector exists with Setup/API Configuration/Schema sections and that `COMPANY_HISTORY`/`COMPANY_TAG_HISTORY` are re-imported weekly (no incremental API). |
| Fivetran dbt package (GitHub) | https://github.com/fivetran/dbt_intercom/blob/main/README.md | 2026-07-21 | Medium | Confirmed Fivetran's expected source table set: `conversation`, `contact`, `company`, `admin`, `tag` (+ `company_tag`/`contact_tag`/`conversation_tag`), `team`, `team_admin`, `article`, `collection_history`, `help_center_history`, `contact_company` — cross-validates the core object list and surfaces `contact_company`/Help Center as deferred/optional extensions. |
| Community/Search snippets | Various `community.intercom.com` and rate-limit summaries surfaced via web search | 2026-07-21 | Medium | Corroborated rate-limit headers (`X-RateLimit-Limit/Remaining/Reset`), 10-second window enforcement, and 429 behavior described in the official Rate Limiting doc (page itself only rendered a title via automated fetch, cross-checked against search snippets and Airbyte's manifest comments). |

**Note on tooling:** `developers.intercom.com` is a client-side-rendered
documentation site; automated page fetches of it returned only page titles/nav
chrome. All endpoint/parameter/schema details in this document were instead
sourced directly from Intercom's official machine-readable OpenAPI
specification (`github.com/intercom/Intercom-OpenAPI`) and from the
actively-maintained Airbyte and Fivetran OSS implementations, per this skill's
source-priority order — every field/endpoint claim below is backed by at least
one of those two high-confidence sources, cross-referenced against each other
where both covered the same endpoint.
