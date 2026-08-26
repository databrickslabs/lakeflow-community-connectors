# Lakeflow Fin.ai Community Connector

Ingest **Intercom** customer-service data — including **Fin AI Agent** outcomes — into Databricks Delta tables via Lakeflow Connect. No coding required.

**Fin** (marketed at [fin.ai](https://fin.ai)) is Intercom's AI customer-service agent. It is not a separate data platform: all the data a connector cares about — the conversations Fin handled, its resolution outcome, its CSAT rating, and which knowledge source it used, plus the surrounding contacts, companies, tickets, tags, segments, admins, teams, and custom-attribute metadata — is served by the standard **Intercom REST API** (`api.intercom.io`).

Fin-specific analytics are surfaced on the `conversations` table via a nested **`ai_agent`** field (resolution state, CSAT rating, answer type, and the content sources Fin used) and an **`ai_agent_participated`** boolean. There is no separate "Fin AI Agent" bulk endpoint — this connector reads Fin outcomes directly from the conversations it participated in.

---

## Prerequisites

- **Intercom workspace**: Access to the Intercom workspace whose data you want to ingest.
- **Private-app Access Token**: The connector authenticates with a Bearer token generated from an Intercom private app (see [Step 1](#step-1-get-your-intercom-access-token)). Creating a private app requires an admin-level account.
- **Read scopes**: The token must be granted read permissions for the objects you plan to sync — conversations, contacts, companies, tickets, admins, tags, segments, and custom attributes.
- **Data region**: Know whether your Intercom workspace is hosted in the **US** (default), **EU**, or **Australia** data center.
- **Fin AI Agent (optional, for Fin data)**: The `ai_agent` fields on `conversations` are only populated for workspaces with the Fin AI Agent paid feature enabled. Without it, those fields are simply `null`/`false` — this is expected, not an error.

---

## Step 1: Get Your Intercom Access Token

1. Go to the [Intercom Developer Hub](https://developers.intercom.com/) and create (or select) an app scoped to your workspace — a **private app**.
2. Open **Configure → Authentication** for that app. Intercom generates the **Access Token** automatically when the app is created.
3. Under the app's **Permissions** / **Authentication** settings, grant the read scopes needed for the tables you will sync (e.g. "Read conversations", "Read and list users and companies", "Read admins", "Read tickets").
4. Copy the Access Token. It is shown in full **only once** — treat it like a password and store it securely.

> **Do not reuse someone else's token.** Asking end users for their personal token violates Intercom's terms of service; for multi-workspace or public integrations, Intercom requires OAuth instead. This connector uses a single private-app token per connection.

**Choosing your region:**

| Region | Value to use | REST API host |
|--------|--------------|---------------|
| United States (default) | `us` | `https://api.intercom.io` |
| Europe | `eu` | `https://api.eu.intercom.io` |
| Australia | `au` | `https://api.au.intercom.io` |

A workspace's data lives entirely in one region. Calling the default US host generally auto-routes to the correct region, but selecting the matching `region` value targets the right host directly and avoids an extra redirect hop.

> **API version:** Every request is pinned to Intercom API version **`2.15`** via the `Intercom-Version` header. This keeps response shapes stable even if your app's default version is later changed in the Developer Hub — you do not need to configure anything for this.

---

## Step 2: Create a Connection in Databricks

1. In your Databricks workspace, go to **Add Data → Lakeflow Community Connectors**.
2. Search for **Fin.ai (Intercom)** and click **Configure**.
3. Fill in the connection form:

| Field | Type | Required | What to enter |
|-------|------|----------|---------------|
| **Access Token** (`access_token`) | string (secret) | Yes | Your Intercom private-app Access Token from Step 1. |
| **Region** (`region`) | string | No | `us` (default), `eu`, or `au` — matching your workspace's data center. |
| **Start Date** (`start_date`) | string | No | ISO-8601 date or timestamp for the lower bound of the historical backfill on incremental tables, e.g. `2024-01-01` or `2024-01-01T00:00:00Z`. Applies only to `conversations`, `contacts`, and `tickets`. If omitted, incremental tables start from the connector's first-run time. |
| **Lookback Window** (`lookback_window`) | string | No | Number of days to re-read at the end of each incremental window to catch late `updated_at` mutations, e.g. `2`. Defaults to `0` (no lookback). Applies to `conversations`, `contacts`, and `tickets`. |
| **API Rate Limit** (`api_rate_limit`) | string | No | Self-throttle budget in API calls per minute. Defaults to `9500` (95% of Intercom's 10,000/min per-app limit). |
| **External Options Allow List** (`externalOptionsAllowList`) | string | Yes | Copy and paste this value exactly: `window_days,per_page,include_archived,include_count,display_avatar` |

> **Why do I need External Options Allow List?**
> This connector supports optional per-table settings (partition window size, page size, and a few optional fields). The `externalOptionsAllowList` connection option tells Databricks which option names are allowed to pass through from your pipeline to the connector. Paste `window_days,per_page,include_archived,include_count,display_avatar` into the field — you do not need to understand each option unless you want to customize behavior later (see [Per-Table Options](#per-table-options)).

4. Click **Test connection** to verify, then **Save**.

The connection can also be created using the standard Unity Catalog API.

---

## Step 3: Configure Your Pipeline

Once the connection is saved, Databricks guides you through creating an ingestion pipeline where you choose which tables to ingest. The pipeline configuration (stored in `ingest.py` in your workspace) looks like this — **you only need to edit the table list and any optional settings**:

```json
{
  "connection_name": "my_fin_ai_connection",
  "objects": [
    { "table": { "source_table": "conversations" } },
    { "table": { "source_table": "contacts" } },
    { "table": { "source_table": "tickets" } },
    { "table": { "source_table": "companies" } },
    { "table": { "source_table": "admins" } },
    { "table": { "source_table": "tags" } },
    { "table": { "source_table": "segments" } },
    { "table": { "source_table": "data_attributes" } },
    { "table": { "source_table": "teams" } }
  ]
}
```

> **Tip — Start small:** Add a couple of lightweight tables (e.g. `admins`, `tags`) on the first run to verify credentials and data shape. Add the high-volume incremental tables (`conversations`, `contacts`) once you have confirmed the connection is working.

---

## Step 4: Run and Schedule the Pipeline

Run the pipeline from the Databricks UI or schedule it as a recurring job.

- **First run**: incremental tables (`conversations`, `contacts`, `tickets`) backfill from your `start_date` (or from the connector's first-run time if `start_date` was left blank). Snapshot tables read their full contents.
- **Subsequent runs**: incremental tables pick up from where they left off using stored watermarks, fetching only records whose `updated_at` has advanced. Snapshot tables re-read in full each run.

---

## Supported Tables

The connector exposes the following **9 tables**. Use the exact lowercase names from the **Table** column as `source_table` in your pipeline.

| Table | API source | How it syncs | Primary key | Cursor field |
|-------|-----------|--------------|-------------|--------------|
| `conversations` | `POST /conversations/search` | CDC (incremental) | `id` | `updated_at` |
| `contacts` | `POST /contacts/search` | CDC (incremental) | `id` | `updated_at` |
| `tickets` | `POST /tickets/search` | CDC (incremental) | `id` | `updated_at` |
| `companies` | `GET /companies/scroll` | Snapshot (full refresh) | `id` | — |
| `admins` | `GET /admins` | Snapshot (full refresh) | `id` | — |
| `tags` | `GET /tags` | Snapshot (full refresh) | `id` | — |
| `segments` | `GET /segments` | Snapshot (full refresh) | `id` | — |
| `data_attributes` | `GET /data_attributes` | Snapshot (full refresh) | `full_name` + `model` | — |
| `teams` | `GET /teams` | Snapshot (full refresh) | `id` | — |

### What each table contains

- **`conversations`** — Conversations between a contact and your workspace (a human admin or **Fin AI Agent**). This is the primary carrier of Fin AI Agent data via the nested `ai_agent` field and `ai_agent_participated` boolean. Also includes `conversation_rating` (human CSAT), `statistics` (handling times, reply counts), tags, and linked ticket references.
- **`contacts`** — People in the workspace. This spans **both `user` and `lead`** records (distinguished by the `role` field), including profile, lifecycle timestamps, client environment metadata, and custom attributes.
- **`tickets`** — Structured customer-service requests (distinct from freeform conversations), with ticket state, ticket type, assignees, and a timeline of parts.
- **`companies`** — Organizations associated with one or more contacts, including plan, size, spend, and segment membership. A company is only visible via the API once it has at least one associated contact.
- **`admins`** — Teammate accounts with workspace access.
- **`tags`** — Labels applied to contacts, companies, and conversations.
- **`segments`** — Rule-based groupings of contacts.
- **`data_attributes`** — Metadata describing every standard and custom field available on `contact`, `company`, and `conversation` records. Use this to interpret the workspace-defined keys inside `custom_attributes`.
- **`teams`** — Groups of admins used for routing and assignment.

### How incremental (CDC) tables work

`conversations`, `contacts`, and `tickets` are read incrementally via Intercom's **Search API** using `updated_at` (Unix epoch seconds) as the cursor:

- Each sync queries records whose `updated_at` falls within a bounded `(since, until]` window, sorted ascending, and pages through results using Intercom's cursor pagination.
- The upper bound is frozen at the connector's start time for a given run, so once a table catches up to that snapshot the sync terminates cleanly; the next scheduled run picks up anything modified since.
- For large backfills, each table's `updated_at` range is split into independent time windows (see the `window_days` option) that Databricks can process in parallel.
- **Watermark / late-data handling:** Intercom evaluates `created_at`/`updated_at` search filters at **day granularity in the workspace's configured timezone**, and newly created records can take a few minutes to become searchable. If you see records arriving late or near day boundaries, set a **`lookback_window`** (e.g. `2`) so the connector re-reads the last N days of each window on every sync and re-captures records modified after the prior run. The stored watermark itself is not rewound — the lookback only widens what gets re-queried.

### How snapshot (full-refresh) tables work

`companies`, `admins`, `tags`, `segments`, `data_attributes`, and `teams` have no server-side time filter, so they are re-read in full on every run. `companies` is drained via Intercom's Scroll API; the rest are single-call list endpoints.

> **No delete capture.** None of these tables expose a hard-delete or tombstone feed in the base REST API, so all tables use `cdc`/`snapshot` ingestion **without** delete synchronization. Deletions are only observable via Intercom webhooks, which are out of scope for a polling connector. In particular, contacts merged via Intercom's merge feature simply vanish from search results with no delete signal (see [Limitations](#limitations-and-gotchas)).

---

## Per-Table Options

These settings are **optional** — all tables work without them. To use one, add it inside the `table_configuration` block for the relevant table. Every option name below must also appear in the connection's `externalOptionsAllowList` (see [Step 2](#step-2-create-a-connection-in-databricks)).

| Option | Applies to | Default | Description |
|--------|-----------|---------|-------------|
| `window_days` | `conversations`, `contacts`, `tickets` | `7` | Width (in days) of each incremental partition window. Reduce for very high-volume workspaces; increase to make fewer, larger requests. |
| `per_page` | `conversations`, `contacts`, `tickets` | `150` | Page size for the Search API. Maximum is `150`. |
| `include_archived` | `data_attributes` | `false` | Set to `true` to include archived custom attributes in the output. |
| `include_count` | `segments` | `false` | Set to `true` to populate each segment's `count` (member count) field. Adds request cost — only enable if you need it. |
| `display_avatar` | `admins` | `false` | Set to `true` to include each admin's avatar URL. |

**Example with options:**

```json
{
  "connection_name": "my_fin_ai_connection",
  "objects": [
    {
      "table": {
        "source_table": "conversations",
        "table_configuration": { "window_days": "7", "per_page": "150" }
      }
    },
    {
      "table": {
        "source_table": "data_attributes",
        "table_configuration": { "include_archived": "true" }
      }
    },
    {
      "table": {
        "source_table": "segments",
        "table_configuration": { "include_count": "true" }
      }
    },
    {
      "table": {
        "source_table": "admins",
        "table_configuration": { "display_avatar": "true" }
      }
    }
  ]
}
```

---

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of the [supported tables](#supported-tables)) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering) |

Source-specific options are documented above under [Per-Table Options](#per-table-options).

---

## Schema Notes

### Fields that need special attention

- **`conversations.ai_agent`** (struct) — The core Fin AI Agent data. Fields include `resolution_state` (e.g. `assumed_resolution`, `confirmed_resolution`, `escalated`, `negative_feedback`, `procedure_handoff`), `rating` and `rating_remark` (Fin CSAT), `last_answer_type` (`ai_answer` / `custom_answer`), `source_type` / `source_title` (what triggered Fin), and `content_sources` (the knowledge sources Fin used). **`ai_agent` is `null` and `ai_agent_participated` is `false`** for conversations Fin did not handle, or for any workspace without the Fin AI Agent paid feature.
- **`conversations.conversation_rating`** (struct) — Human CSAT (separate from the Fin rating on `ai_agent`): `rating`, `remark`, and the contact/teammate involved.
- **`conversations.statistics`** (struct) — Reporting metrics such as `time_to_admin_reply`, `handling_time`, `count_reopens`, and various `*_at` milestone timestamps.
- **`custom_attributes`** (on `conversations`, `contacts`, `companies`) and **`ticket_attributes`** (on `tickets`) — Workspace-defined dynamic fields, modelled as `MapType(StringType, StringType)` because their keys are not known ahead of time. Use the **`data_attributes`** table to discover the declared name, type, and label for each key.
- **`data_attributes` composite key** — This table is keyed on `full_name` + `model` together. The same short name (e.g. `name`) can exist under both the `contact` and `company` models, so `model` must be part of the key when deduplicating or upserting.
- **`tickets.ticket_id`** — This is a human-facing sequential number shown in the Intercom UI; it is **not** usable for API lookups. Always use `id` as the ticket key.
- **Timestamps are Unix epoch seconds.** Every Intercom timestamp (`created_at`, `updated_at`, `waiting_since`, all `statistics.*` and `ai_agent.*_at` fields, etc.) is returned **verbatim as `LongType`** (an integer count of seconds since 1970). The connector does not convert them — cast to a timestamp downstream as needed (for example, multiply by 1000 for milliseconds). Keeping the cursor a plain integer is also what makes the `updated_at` watermark sort and compare correctly.

### Data type mapping

| Intercom API type | Spark / Delta type | Notes |
|-------------------|--------------------|-------|
| string | `StringType` | Most text fields, including all primary-key `id` values (Intercom IDs are strings even when they look numeric). |
| integer (Unix timestamp) | `LongType` | Returned verbatim as epoch **seconds** — not converted. Cast downstream. |
| integer (count / duration) | `LongType` | Counts and durations, e.g. `size`, `monthly_spend`, `statistics.handling_time`. |
| float | `DoubleType` | Rare; some float-typed custom-attribute values. |
| boolean | `BooleanType` | `open`, `read`, `ai_agent_participated`, `archived`, `custom`, etc. |
| object / free-form dict (`custom_attributes`, `ticket_attributes`, `topics`, `teammates`) | `MapType(StringType, StringType)` | Keys vary per workspace. |
| nested object | `StructType` | e.g. `ai_agent`, `source`, `conversation_rating`, `statistics`, `ticket_state`, `ticket_type`. |
| array | `ArrayType(...)` | e.g. `team_ids` / `admin_ids` (`ArrayType(LongType)`), `data_attributes.options` (`ArrayType(StringType)`). |
| null | nullable field | Nearly every non-key field can be `null`; do not assume presence. |

---

## Limitations and Gotchas

- **Fin AI data requires the paid feature.** `ai_agent` and `ai_agent_participated` are populated only when the Fin AI Agent feature is enabled on the workspace. Otherwise they are `null`/`false` — expected, not an error.
- **`companies` has no server-side time filter.** None of Intercom's company endpoints support filtering by `updated_at`, so `companies` is a full snapshot on every run: the entire company list is re-read each sync. Companies also only appear once they have at least one associated contact.
- **`contacts` spans users and leads.** Both `user` and `lead` roles are returned in one stream (see the `role` field). **Merged contacts have no delete signal** — when two contacts are merged in Intercom, the losing record disappears from search results permanently (including from `updated_at`-filtered queries) with no tombstone, so a polling connector cannot detect these as deletes.
- **`tickets` has no plain list endpoint.** Tickets can only be read via `POST /tickets/search`; there is no `GET /tickets`. And `ticket_id` is a display number, not the API key — use `id`.
- **Search timestamp filters are day-indexed.** `contacts`/`companies` search filters evaluate timestamps at day granularity in the workspace's configured timezone, and new records take a few minutes to become searchable. Use `lookback_window` to absorb this.
- **API version is pinned.** All requests send `Intercom-Version: 2.15`. If Intercom changes response shapes in a future version, the connector code (not your pipeline config) would need to be updated.
- **`companies` Scroll API is single-session.** Only one company scroll can be open per app at a time, and a stalled scroll cannot resume — it restarts from the beginning. Avoid running two company syncs against the same token concurrently.

---

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`) with your table list.
2. (Optional) Add per-table options inside `table_configuration` — see [Per-Table Options](#per-table-options).
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start small**: Begin by syncing a couple of lightweight tables (`admins`, `tags`) to verify credentials, then add `conversations` and `contacts`.
- **Use incremental sync**: `conversations`, `contacts`, and `tickets` sync incrementally by `updated_at`, reducing API calls and improving performance.
- **Tune the window**: Adjust `window_days` and `per_page` for very high-volume workspaces.
- **Set a lookback window**: Set `lookback_window` (e.g. `2`) if you observe late-arriving or near-midnight records on incremental tables.
- **Mind the rate limits**: Intercom allows **10,000 API calls/minute per app** and **25,000/minute per workspace** (shared across all apps in the workspace), enforced in 10-second sub-windows. The connector self-throttles to `api_rate_limit` (default `9500`) and, on a `429 Too Many Requests`, backs off and retries honoring the `Retry-After` and `X-RateLimit-Reset` headers. It also retries transient `500`/`502`/`503`/`504` responses with exponential backoff.
- **Set appropriate schedules**: Balance data freshness against API usage, especially if multiple integrations share your workspace's rate-limit budget.

#### Troubleshooting

**Authentication failures (`401` / `403`)**
- Verify the `access_token` is correct and has not been revoked, and that the private app has the read scopes for the tables you are syncing.
- Confirm the `region` matches your workspace's data center — a US token will not work against the EU/AU host and vice versa.

**Rate limiting (`429`)**
- The connector already retries and backs off, but persistent throttling means you should widen the schedule interval or reduce pipeline concurrency. If you legitimately need a higher limit, Intercom raises it only on request (via in-product Messenger).

**Fin (`ai_agent`) fields are empty**
- Confirm the Fin AI Agent paid feature is enabled on the workspace. Conversations Fin did not participate in will always have `ai_agent = null` and `ai_agent_participated = false`.

**Missing or late records on incremental tables**
- Intercom's search timestamps are day-indexed and new records take a few minutes to be searchable. Increase `lookback_window` to re-check recently updated records.

**A merged contact is still present / a deletion is not reflected**
- The base REST API has no delete feed. Merged/deleted records simply stop appearing in future snapshots; there is no per-record delete event to capture.

---

## References

| Resource | Link |
|----------|------|
| Intercom REST API introduction | https://developers.intercom.com/docs/references/introduction |
| Intercom authentication (Access Token) | https://developers.intercom.com/docs/build-an-integration/learn-more/authentication |
| Regional data hosting (US / EU / AU) | https://www.intercom.com/help/en/articles/6124430-regional-data-hosting |
| Fin resolutions (resolution state semantics) | https://www.intercom.com/help/en/articles/8205718-fin-resolutions |
| Intercom API rate limits | https://developers.intercom.com/docs/references/rest-api/errors/rate-limiting/ |
| Connector implementation | `src/databricks/labs/community_connector/sources/fin_ai/fin_ai.py` |
| Connector schemas | `src/databricks/labs/community_connector/sources/fin_ai/fin_ai_schemas.py` |
