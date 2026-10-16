# Lakeflow Egnyte Community Connector

This documentation describes how to configure and use the **Egnyte** Lakeflow community connector to ingest content-collaboration metadata and activity from the [Egnyte](https://www.egnyte.com/) Public API (`https://{domain}.egnyte.com/pubapi/...`) into Databricks Unity Catalog Delta tables.

The connector performs a **read-only** extract of nine tables: the file/folder tree, users and groups, share links, the file-system event feed, and the three job-based audit reports (logins, file activity, permission changes). It never writes back to Egnyte.

Egnyte is a **per-tenant** product: every API call goes to a customer-specific hostname, and there is no way to discover that hostname from a token. The tenant `domain` is therefore a required connection parameter, and every emitted row carries an `egnyte_domain` column so several tenants can land in one table.

## Prerequisites

- **An Egnyte tenant** reachable at `https://{domain}.egnyte.com` (or a custom-branded hostname such as `files.acme.com`). `{domain}` is the subdomain portion — for `https://acmecorp.egnyte.com` the domain is `acmecorp`.
- **A registered Egnyte API key (OAuth application)**. Egnyte issues an API key (`client_id`) and secret (`client_secret`) per integration; registration is a one-time admin action per tenant, requested through the [Egnyte developer portal](https://developers.egnyte.com/member/register).
- **An OAuth refresh token** obtained once, out of band, via the authorization-code flow (see [Obtaining the Required Parameters](#obtaining-the-required-parameters)). The connector never runs the interactive step itself — it only refreshes.
- **Scopes** granted to the token, space-delimited at authorization time. To read every table in this connector, request:
  ```
  Egnyte.filesystem Egnyte.link Egnyte.user Egnyte.group Egnyte.audit
  ```
  | Scope | Tables it unlocks |
  |---|---|
  | `Egnyte.filesystem` | `files`, `folders`, `events` |
  | `Egnyte.link` | `links` |
  | `Egnyte.user` | `users` |
  | `Egnyte.group` | `groups` |
  | `Egnyte.audit` | `audit_logins`, `audit_files`, `audit_permissions` |

  (`Egnyte.permission` is listed by Egnyte for the Permissions API, which this connector does not read — see [Limitations](#limitations).)
- **An admin (or "can run reports" power user) account for the audit tables.** The three `audit_*` tables are admin-gated: a standard-user token receives `403` on `/pubapi/v1/audit/...` even though the same token works fine for files, users, groups, links, and events. If you only need the non-audit tables, a standard user is sufficient.
- **Network access** from the pipeline compute to `https://{domain}.egnyte.com`.
- **Rate-limit headroom.** Egnyte's default quota is **2 API calls/second/token and 1,000 API calls/day/token**. A full recursive tree walk of a large tenant can exhaust the daily quota on its own — see [Rate limits and quotas](#rate-limits-and-quotas) before enabling `files`/`folders` against a big tenant.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options. These correspond to the options read in `EgnyteLakeflowConnect.__init__`.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `domain` | string | **yes** | Tenant identifier. A bare label gets `.egnyte.com` appended; a value containing a dot is used verbatim (custom-branded hostnames); a full `https://...` URL is accepted as-is. There is no discovery path — this must be supplied. | `acmecorp`, `files.acme.com`, `https://acmecorp.egnyte.com` |
| `client_id` | string | conditional | Egnyte API key. Required unless `access_token` is supplied. | `abcd1234efgh5678` |
| `client_secret` | string (secret) | conditional | Egnyte API secret. Required unless `access_token` is supplied. | `s3cr3t...` |
| `refresh_token` | string (secret) | conditional | Long-lived refresh token from the one-time authorization-code exchange. Required unless `access_token` is supplied. | `efgh5678...` |
| `access_token` | string (secret) | conditional | A pre-issued OAuth bearer token. When present, the refresh exchange is skipped entirely. Accepted under the alias `token` as well. | `abcd1234...` |
| `timeout` | int | no | Per-request HTTP timeout in seconds. Default `30`. | `60` |
| `min_request_interval` | float | no | Minimum seconds between successive calls on one HTTP client, used to stay under the 2 QPS ceiling. Default `0.5`. Set to `0` to disable pacing (not recommended). | `0.5` |

You must supply **either** `access_token` **or** the trio `client_id` + `client_secret` + `refresh_token`. If neither is complete, the connector raises a `ValueError` at construction. A missing or empty `domain` also raises a `ValueError`.

### Authentication

Egnyte uses OAuth 2.0 against the tenant host, outside `/pubapi`:

```
POST https://{domain}.egnyte.com/puboauth/token
Content-Type: application/x-www-form-urlencoded

client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token&refresh_token={refresh_token}
```

The connector implements the **refresh-token flow**: it exchanges the stored refresh token for a fresh access token **once per run, on the driver**, and the resolved token travels to executors with the pickled reader. Access tokens live 30 days (`expires_in: 2592000`), so one exchange per run keeps the connector far below the token endpoint's **100 requests/hour** cap. Every `/pubapi` request then carries `Authorization: Bearer {access_token}`.

If a `access_token` (or `token`) option is present — for example injected by a Unity Catalog connection — the exchange is skipped and that token is used directly.

The token endpoint answers **`409`** (not `429`) when its hourly cap is breached; the connector retries that status with `Retry-After`-aware backoff.

### Obtaining the Required Parameters

**1. Find your `domain`.** Sign in to Egnyte. The subdomain in `https://{domain}.egnyte.com` is the value to supply. If your tenant uses a custom-branded hostname, supply the full hostname instead.

**2. Register an API key (one-time, per tenant).** Request an API key through the [Egnyte developer portal](https://developers.egnyte.com/member/register), specifying the redirect URI you will use for the authorization step and the scopes listed under [Prerequisites](#prerequisites). Egnyte issues a `client_id` (API key) and `client_secret`.

**3. Obtain a `refresh_token` (one-time, out of band).** Run the authorization-code flow once as the user whose permissions the connector should inherit — use an **admin or "can run reports" power user** if you intend to ingest the audit tables.

Open in a browser:

```
GET https://{domain}.egnyte.com/puboauth/token
    ?client_id={client_id}
    &redirect_uri={your_callback_url}
    &scope=Egnyte.filesystem%20Egnyte.link%20Egnyte.user%20Egnyte.group%20Egnyte.audit
    &state={random_state}
    &response_type=code
```

After approval Egnyte redirects to `{your_callback_url}?code={code}&state={state}`. Exchange the code:

```
POST https://{domain}.egnyte.com/puboauth/token
Content-Type: application/x-www-form-urlencoded

client_id={client_id}&client_secret={client_secret}&code={code}&grant_type=authorization_code&redirect_uri={your_callback_url}
```

The response contains both an `access_token` and a `refresh_token`. Store the **`refresh_token`** securely and supply it to the connection; the connector mints access tokens from it.

**4. Verify connectivity (optional).**

```bash
curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "https://$DOMAIN.egnyte.com/pubapi/v1/userinfo"
```

A `200` with `{"id": ..., "username": ...}` confirms the domain, token, and network path. To confirm the audit tables will work, also check that the account is an admin or a power user with "can run reports" — a bare `403` on `/pubapi/v1/audit/logins` is a permissions failure, not a throttle.

### Create a Unity Catalog Connection

Create a Unity Catalog COMMUNITY connection for this connector and provide `domain` plus either `access_token` or `client_id` / `client_secret` / `refresh_token`, along with any optional `timeout` / `min_request_interval` overrides.

The connection can be created through the Lakeflow Community Connector UI ("Add data" flow) or via the standard Unity Catalog API / the `community-connector` CLI.

**`externalOptionsAllowList` is required for this connector.** Every per-table option below is passed through the connection, so any option you intend to use must appear in the allow-list. The full, definitive set of table-level options supported by this connector is:

```
root_paths, fs_page_size, max_depth, include_perms, include_locks, page_size, filter, include_members, window_days, start_timestamp, link_path, link_type, accessibility, events_api_version, events_page_size, start_event_id, event_type, folder, suppress, start_date, initial_backfill_days, audit_page_size, audit_poll_interval_seconds, audit_poll_max_attempts, audit_events, audit_folders, audit_users, audit_transaction_types, audit_file, audit_assigners, audit_assignee_users, audit_assignee_groups, max_records_per_batch, max_partitions_per_batch
```

## Supported Objects

Egnyte exposes no machine-readable object catalog, so the connector serves a **static list of 9 tables**:

| Table | Source endpoint | Description |
|---|---|---|
| `files` | `GET /pubapi/v1/fs/{path}` | One row per file, emitted from the `files[]` array of each folder listing during a recursive tree walk. |
| `folders` | `GET /pubapi/v1/fs/{path}` | One row per folder visited by the same walk. |
| `users` | `GET /pubapi/v2/users` | SCIM-influenced user directory. |
| `groups` | `GET /pubapi/v2/groups` | Group directory. Membership is opt-in (see `include_members`). |
| `links` | `GET /pubapi/v2/links` | Share links, as full Link objects (v2 avoids v1's id-list + per-id fan-out). |
| `events` | `GET /pubapi/v1/events` (or `v2`) | The always-on file-system activity feed. |
| `audit_logins` | `POST /pubapi/v1/audit/logins` | Login / failed-attempt audit report rows. **Admin-gated.** |
| `audit_files` | `POST /pubapi/v1/audit/files` | File-activity audit report rows. **Admin-gated.** |
| `audit_permissions` | `POST /pubapi/v1/audit/permissions` | Permission-change audit report rows. **Admin-gated.** |

Table names must be used with exactly this casing.

### Object summary, primary keys, and ingestion mode

| Table | Ingestion Type | Primary Key | Incremental Cursor | Partitioned stream |
|---|---|---|---|---|
| `files` | `snapshot` | `group_id`, `path` (composite) | n/a | no (batch-partitioned by subtree) |
| `folders` | `snapshot` | `folder_id` | n/a | no (batch-partitioned by subtree) |
| `users` | `snapshot` | `id` | n/a | no |
| `groups` | `snapshot` | `id` | n/a | no |
| `links` | `append` | `id` | `creation_date` (ISO-8601) | **yes** — time windows |
| `events` | `append` | `id` | `id` (monotonic int) | **yes** (single unit of work) |
| `audit_logins` | `append` | `user_id`, `time`, `event` | `time` | **yes** — date windows |
| `audit_files` | `append` | `user_id`, `time`, `transaction` | `time` | **yes** — date windows |
| `audit_permissions` | `append` | `assignee_id`, `time`, `change` | `time` | **yes** — date windows |

Notes on the keys:

- **`files` uses a composite `(group_id, path)`.** `entry_id` identifies *one version* and changes on every upload, so it is unsuitable as a key. `group_id` is the stable identity of a file across versions, but a file can be renamed or moved without it changing, and two entries can share a `group_id` after a copy — hence the composite.
- **Audit report rows carry no server-assigned row id.** The composite natural keys above are the recommended dedup keys and remain **UNVERIFIED** against live data. Because of this, `time` is deliberately kept as a `StringType` on the three audit tables: it is a *key column*, and a key has to survive byte-for-byte rather than be re-rendered by a timestamp parser. Cast it downstream.
- **No delete synchronization.** No table is `cdc_with_deletes`. Snapshot tables (`files`, `folders`, `users`, `groups`) reflect deletions naturally on the next full snapshot. Append-only tables never remove rows: link deletion in particular is not surfaced by the links endpoint at all (see [Limitations](#limitations)).

### Schema highlights

- **`egnyte_domain`** is stamped onto every row of every table by the connector (non-null), so multiple tenants can share one destination table.
- **`files.parent_path` / `folders.parent_path`** are connector-derived: the folder each entry was listed under.
- **`files.uploaded` and `folders.lastModified` are epoch *milliseconds* kept as `LongType`**, not timestamps. Divide by 1000 before casting. (`files.last_modified`, by contrast, is an ISO-8601 string from the fs API and *is* a `TimestampType`.)
- **`groups.members`** is `NULL` unless the `include_members` table option is enabled — the list endpoint returns only `id` and `displayName`.
- **`users` fields `createdDate`, `lastModificationDate`, `lastActiveDate`, `isServiceAccount`, `language`, `idpUserId`, `userPrincipalName`** are documented by Egnyte but absent from the SDK attribute list. They are declared nullable so they populate if present and stay `NULL` otherwise (**UNVERIFIED**).
- **`events.data`** is a struct whose populated keys vary by `type`/`action` (copy and move carry both source and target; create/delete only target). Absent keys are `NULL`, not empty structs.
- **`events.actor` and `audit_*.user_id`** are user ids that join to `users.id`.
- **`audit_*.username` is rendered as `"Jane Smith (jsmith@company.com)"`**, not a bare username. Parse downstream if you need the address; prefer joining on `user_id`.
- Nested objects (`users.name`, `events.data`, `groups.members`) are preserved as structs / arrays of structs, never flattened.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (one of the nine above) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Applies to the four `snapshot` tables only; the five `append` tables (`links`, `events`, `audit_*`) do not support this option. |
| `primary_keys` | No | List of columns overriding the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. Defaults to the table's cursor field. |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

Every option below must also be listed in the connection's `externalOptionsAllowList`.

#### `files` and `folders` (the fs tree walk)

| Option | Default | Description |
|---|---|---|
| `root_paths` | `/Shared` | Comma-separated list of roots to walk, e.g. `/Shared,/Private/jsmith`. Paths are normalized to a leading slash with no trailing slash. |
| `fs_page_size` | `100` | Items per folder-listing page (`count`). |
| `max_depth` | `20` | Recursion depth guard, measured from each root. Subtrees deeper than this are not descended into. |
| `include_perms` | `false` | Sends `perms=true`, adding the per-folder permissions block. Costs nothing extra in call count. |
| `include_locks` | `false` | Sends `include_locks=true`, adding file lock information. |

#### `users` and `groups`

| Option | Default | Description |
|---|---|---|
| `page_size` | `100` | Records per SCIM page. **Hard-capped at 100 by the API**; larger values are clamped. |
| `filter` | — | A verbatim SCIM filter string, e.g. `email eq "jsmith@acme.com"`. Users support exact `eq` on `email` / `externalId` / `userName`; groups additionally support `co` / `sw` on `displayName`. There is **no** modified-since filter. |
| `include_members` | `false` | **`groups` only.** Fans out one `GET /pubapi/v2/groups/{id}` per group to populate `members`. This is an N+1 against a 1,000-call/day quota — enable deliberately. |

#### `links`

| Option | Default | Description |
|---|---|---|
| `window_days` | `7` | Width in days of each `created_after`/`created_before` partition window. |
| `start_timestamp` | — | ISO-8601 lower bound for the very first read (e.g. `2026-01-01T00:00:00Z`). Without it, the first run is a single open-ended partition covering all history. |
| `page_size` | `100` | Records per page. API maximum is `500`; larger values are clamped. |
| `link_path` | — | Server-side `path` filter. |
| `link_type` | — | Server-side `type` filter: `file`, `folder`, or `upload`. |
| `accessibility` | — | Server-side `accessibility` filter: `anyone`, `password`, `domain`, or `recipients`. |
| `max_partitions_per_batch` | `1` | Windows consumed per microbatch on the single-driver path. |
| `max_records_per_batch` | `5000` | Soft record cap per microbatch on the single-driver path — the connector stops taking *further* windows once reached, never truncating a window mid-way. |

#### `events`

| Option | Default | Description |
|---|---|---|
| `events_api_version` | `v1` | `v1` or `v2`. `v2` adds `permission_change` events to the `type` enum and to default coverage. Anything else falls back to `v1`. |
| `events_page_size` | `100` | Events per call (`count`). |
| `start_event_id` | — | Bootstrap cursor for the first run. Without it the connector reads `/events/cursor` and starts from `oldest_event_id`. |
| `event_type` | — | Server-side `type` filter, e.g. `file_system`, `note`, `permission_change` (v2). |
| `folder` | — | Server-side folder filter. |
| `suppress` | — | Server-side `suppress` filter: `app` or `user`. |
| `max_records_per_batch` | `5000` | Soft cap per microbatch. Pages always land whole — the connector stops issuing *further* calls once the cap is reached. |

#### `audit_logins`, `audit_files`, `audit_permissions`

| Option | Default | Applies to | Description |
|---|---|---|---|
| `window_days` | `7` | all three | Days per report job. Each window is one independent create → poll → fetch lifecycle. |
| `initial_backfill_days` | `7` | all three | First-run lookback, in days, when no `start_date` is given. |
| `start_date` | — | all three | Explicit `YYYY-MM-DD` first-run start. Overrides `initial_backfill_days`. |
| `audit_page_size` | `100` | all three | Report rows fetched per page. |
| `audit_poll_interval_seconds` | `120` | all three | Seconds between job-status polls. Egnyte's official guidance is no more than once every 2 minutes — lowering this risks throttling. |
| `audit_poll_max_attempts` | `30` | all three | Polls before giving up on a job (raises an error). At the defaults, that is a 1-hour ceiling per window. |
| `audit_events` | `successful_login,failed_attempts` | `audit_logins` | Comma-separated login event types to include. |
| `audit_folders` | *(none)* for `audit_files`; `/Shared` for `audit_permissions` | `audit_files`, `audit_permissions` | Comma-separated folder paths to scope the report to. |
| `audit_users` | — | `audit_files` | Comma-separated users to scope the report to. |
| `audit_transaction_types` | — | `audit_files` | Comma-separated transaction types to include. |
| `audit_file` | — | `audit_files` | A single file path to scope the report to. |
| `audit_assigners` | *(empty = no restriction)* | `audit_permissions` | Comma-separated assigners. |
| `audit_assignee_users` | *(empty = no restriction)* | `audit_permissions` | Comma-separated assignee users. |
| `audit_assignee_groups` | *(empty = no restriction)* | `audit_permissions` | Comma-separated assignee groups. |
| `max_partitions_per_batch` | `1` | all three | Date windows consumed per microbatch on the single-driver path. |
| `max_records_per_batch` | `5000` | all three | Soft record cap per microbatch on the single-driver path. |

The `permissions` report requires all four of `folders`, `assigners`, `assignee_users`, and `assignee_groups` in its request body, so the connector always sends them; empty lists mean "no restriction".

## Incremental sync behavior

**Snapshot tables** (`files`, `folders`, `users`, `groups`) are re-read in full on every run. This is forced by the API, not a design choice: the File System API has no domain-wide "changed since" endpoint, and the SCIM-style `filter` on users/groups only supports exact match on name-like fields. Change detection is a full pull plus a downstream diff.

**`links`** uses `creation_date` as its cursor and offsets of the form `{"cursor": "<ISO-8601>"}`. `created_after` / `created_before` are genuine server-side filters. Windows are treated as half-open (`since <= creation_date < until`) client-side, because Egnyte's own bound inclusivity is undocumented — without that, adjacent windows would double-count boundary rows in an append-only table. On the first run (no stored offset and no `start_timestamp`) the reader issues **one open-ended partition** rather than decades of empty weekly windows; set `start_timestamp` if you want a bounded, parallelized first run.

**`events`** uses the monotonic event `id` as its cursor, with offsets of the form `{"cursor": <int>}`. The bootstrap value comes from `start_event_id`, or from `GET /events/cursor`'s `oldest_event_id` (stepped one back, because `id` means "everything strictly after this"). `204 No Content` means no new events and is not an error. If the stored cursor has aged out of the retention window the list call answers **`404`**; per Egnyte's official guidance the connector re-reads `/events/cursor`, resumes from `oldest_event_id`, logs a warning, and **accepts a gap in coverage**. Poll at least every 30 minutes on a busy tenant to avoid this.

**`audit_logins` / `audit_files` / `audit_permissions`** use whole-calendar-day offsets of the form `{"date": "YYYY-MM-DD"}`. Report windows tile the range with no overlap and no gap (the API's `date_end` is inclusive, so the resume offset is the following day), which matters because audit rows are append-only with no server id to dedup on. The first run covers `initial_backfill_days` (default 7) back from today, or from `start_date` if set.

**Termination under `Trigger.AvailableNow`.** The connector records an init-time snapshot in `__init__` and never returns an offset past it, so the stream drains to that snapshot and stops; newer data is picked up by the next trigger's fresh instance. The audit tables cap one step tighter — at the **start of the current UTC day** — because report windows are whole calendar days and re-requesting a partial day would duplicate append-only rows. Practically: **audit data for "today" never arrives until tomorrow.**

## Partitioning behavior

The connector implements `SupportsPartitionedStream`. `is_partitioned()` returns `True` only for the tables backed by genuine **server-side range queries**:

| Table | `is_partitioned` | Partition unit |
|---|---|---|
| `links` | yes | `[since, until)` time windows of `window_days` days, from `created_after`/`created_before` |
| `audit_logins`, `audit_files`, `audit_permissions` | yes | Inclusive `date_start`..`date_end` calendar-day windows of `window_days` days; each window is its own report job |
| `files`, `folders` | no (streaming) | **Batch reads only**: one non-recursive partition per configured root, plus one recursive partition per immediate child folder of each root |
| `users`, `groups`, `events` | no | A single unit of work |

The audit tables benefit the most from partitioning: each window is a create → poll → fetch round trip that spends most of its wall time *waiting* on Egnyte's report generator, so running windows on separate executors converts dead polling time into throughput.

`files` / `folders` opt out of the *streaming* partitioned path (there is no range filter to split on) but still parallelize **batch** reads by subtree. Computing those partitions costs one listing call per root on the driver; the root itself gets a non-recursive partition — its own record plus its direct files — so no subtree is walked twice. The walk keeps a `visited` set keyed on the canonical path, guarding against cycles and malformed listings that point back at an ancestor.

`users`, `groups`, and `events` have no natural split: SCIM `filter` cannot express a range, and the events API only answers "give me what follows this id", so id ranges are not knowable up front.

Every pagination loop also carries an absolute ceiling of 1,000 pages per read, because Egnyte's pagination metadata is inconsistent across resources and a server that never returns a short page must not spin the loop forever.

## Rate limits and quotas

Egnyte enforces two independent tiers, and the connector honors both.

**Standard `/pubapi` calls — per access token, not per API key:**

- **2 API calls/second/token**
- **1,000 API calls/day/token**

The connector paces requests with `min_request_interval` (default `0.5` s, i.e. 2 QPS) so a recursive tree walk or a wide partition fan-out cannot trip the per-second ceiling on its own. It also reads the `X-Accesstoken-Qps-Current` / `X-Accesstoken-Qps-Allotted` headroom headers and sleeps ~1 s proactively when current is at or above the allotment.

**Throttle detection handles both documented styles**, because Egnyte documents them differently on different pages:

- **`429`** with `Retry-After` (modern style), and
- **`403`** with `X-Mashery-Error-Code: ERR_403_DEVELOPER_OVER_QPS` (per-second) or `ERR_403_DEVELOPER_OVER_RATE` (daily), plus `Retry-After` (legacy style).

A **bare `403` with no Mashery header is a permissions failure and is never retried** — retrying would burn daily quota against a request that can never succeed. This is the expected failure mode for audit tables read with a non-admin token.

Retriable statuses (`429`, `500`, `502`, `503`, `504`, and throttle-flavoured `403`s) are retried up to 5 attempts with exponential backoff starting at 1 second, honoring `Retry-After` when present.

**OAuth token endpoint (`/puboauth/token`): 100 requests/hour**, answering `409` (not `429`) when breached. The connector resolves the token once on the driver and ships it to executors with the pickled reader, so a run costs at most one call against this bucket.

**Note on the daily quota.** 1,000 calls/day is the binding constraint for most tenants. A `files`/`folders` walk costs roughly one call per folder (plus extra pages for folders with more than `fs_page_size` entries); `include_members` on `groups` costs one call per group; each audit window costs one create, up to `audit_poll_max_attempts` polls, plus one call per report page. If your anticipated volume exceeds the defaults, contact `api-support@egnyte.com` for a higher-quota arrangement.

## Data Type Mapping

| Egnyte representation | Example fields | Connector Spark type | Notes |
|---|---|---|---|
| Opaque identifier string | `folder_id`, `entry_id`, `group_id`, `parent_id`, `links.id`, `groups.id` | `StringType` | Not necessarily UUID v4 — never parse or narrow them. |
| Large integer id | `users.id`, `events.id`, `events.actor`, `audit_*.user_id` / `assignee_id` / `assigner_id` | `LongType` | Observed values such as `9967960066` overflow `IntegerType`. |
| Epoch **milliseconds** | `files.uploaded`, `folders.lastModified` | `LongType` | Deliberately **not** `TimestampType`: the framework's coercion interprets the number as seconds, which would land in the year 56000. Divide by 1000 downstream. |
| ISO-8601 string | `files.last_modified`, `links.creation_date`, `events.timestamp`, `users.createdDate` | `TimestampType` | Both `...Z` and `...+00:00` spellings, with or without fractional seconds, are parsed. |
| ISO-8601 string used as a key | `audit_logins.time`, `audit_files.time`, `audit_permissions.time` | `StringType` | Kept as the raw string because it is part of the composite primary key and the report-row format is UNVERIFIED. Cast downstream. |
| Byte size / counts | `files.size`, `files.num_versions` | `LongType` | |
| Boolean | `is_folder`, `locked`, `active`, `notify`, `allow_links` | `BooleanType` | |
| Enum string | `permission`, `accessibility`, `protection`, `authType`, `userType`, `action` | `StringType` | New enum values may appear additively; validate downstream rather than in the schema. |
| Nested object | `users.name` (`givenName`/`familyName`/`formatted`), `events.data` | `StructType` | Preserved, not flattened. Absent keys are `NULL`. |
| Array of objects | `groups.members` | `ArrayType(StructType)` | `NULL` unless `include_members` is set. |
| Array of strings | `links.recipients` | `ArrayType(StringType)` | |
| Connector-derived | `egnyte_domain`, `files.parent_path`, `folders.parent_path` | `StringType` | `egnyte_domain` is non-null on every row. |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Egnyte connector source in your workspace.

### Step 2: Configure Your Pipeline

Point the `pipeline_spec` at the Unity Catalog connection and list the tables to ingest. A minimal spec:

```json
{
  "pipeline_spec": {
    "connection_name": "egnyte_connection",
    "objects": [
      { "table": { "source_table": "users" } },
      { "table": { "source_table": "groups" } },
      { "table": { "source_table": "events" } }
    ]
  }
}
```

A fuller example exercising the source-specific options (each of which must be present in the connection's `externalOptionsAllowList`):

```json
{
  "pipeline_spec": {
    "connection_name": "egnyte_connection",
    "objects": [
      {
        "table": {
          "source_table": "folders",
          "table_configuration": {
            "root_paths": "/Shared,/Shared/Legal",
            "max_depth": "8",
            "fs_page_size": "100"
          }
        }
      },
      {
        "table": {
          "source_table": "files",
          "table_configuration": {
            "root_paths": "/Shared/Legal",
            "include_locks": "true"
          }
        }
      },
      {
        "table": {
          "source_table": "groups",
          "table_configuration": { "include_members": "true" }
        }
      },
      {
        "table": {
          "source_table": "links",
          "table_configuration": {
            "start_timestamp": "2026-01-01T00:00:00Z",
            "window_days": "7",
            "page_size": "500"
          }
        }
      },
      {
        "table": {
          "source_table": "events",
          "table_configuration": {
            "events_api_version": "v2",
            "events_page_size": "100"
          }
        }
      },
      {
        "table": {
          "source_table": "audit_logins",
          "table_configuration": {
            "start_date": "2026-07-01",
            "window_days": "7",
            "audit_events": "successful_login,failed_attempts"
          }
        }
      },
      {
        "table": {
          "source_table": "audit_permissions",
          "table_configuration": {
            "initial_backfill_days": "30",
            "audit_folders": "/Shared"
          }
        }
      }
    ]
  }
}
```

The equivalent in Python:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set(
    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"
)
register(spark, "egnyte")

pipeline_spec = {
    "connection_name": "egnyte_connection",
    "objects": [
        {"table": {"source_table": "users"}},
        {"table": {"source_table": "groups"}},
        {
            "table": {
                "source_table": "links",
                "table_configuration": {"start_timestamp": "2026-01-01T00:00:00Z"},
            }
        },
        {"table": {"source_table": "events"}},
        {
            "table": {
                "source_table": "audit_logins",
                "table_configuration": {"initial_backfill_days": "30"},
            }
        },
    ],
}

ingest(spark, pipeline_spec)
```

### Step 3: Run and Schedule the Pipeline

The first run backfills according to each table's rules: snapshot tables pull everything under `root_paths`, `links` covers all history (or from `start_timestamp`), `events` starts at the oldest retained event, and the audit tables cover `initial_backfill_days` (or from `start_date`). Subsequent runs resume from the stored offsets.

#### Best Practices

- **Start small.** Enable `users`, `groups`, and `events` first — they are cheap and validate auth and shape. Add `files`/`folders` with a narrow `root_paths` before pointing them at the whole tenant.
- **Budget the daily quota before enabling `files`/`folders`.** At 1,000 calls/day/token, roughly one call per folder, a tenant with more than a few hundred folders will exhaust the quota in a single walk. Narrow `root_paths`, lower `max_depth`, run the walk on a slower schedule than the append tables, or arrange a higher quota with Egnyte.
- **Schedule `events` at least every 30 minutes.** Its retention window is 30 days by count *or* time; a cursor that ages out costs you a permanent gap.
- **Do not lower `audit_poll_interval_seconds` below 120.** That is Egnyte's own guidance and each poll spends daily quota.
- **Split schedules by cost profile.** Audit and file-tree tables are expensive and slow; `users`, `groups`, `links`, and `events` are cheap. Running them in separate pipelines keeps a slow tree walk from delaying fresh activity data.
- **Use one token per pipeline where possible.** The 2 QPS / 1,000-per-day quota is enforced *per access token*, so separate tokens do not share a bucket.
- **Prefer `include_members=false` unless you need membership.** It turns the `groups` read into an N+1 fan-out.

#### Troubleshooting

**`403` on the `audit_*` tables while everything else works.**
The token's user is not an admin and lacks the "can run reports" power-user permission. This is a bare `403` with no `X-Mashery-Error-Code`, so the connector does not retry it. Re-run the one-time authorization-code flow as an admin and store the resulting refresh token.

**`401 Unauthorized` on every call.**
The access token expired (30-day lifetime), the user's password changed, or the token was revoked — all of which invalidate it immediately. If using the refresh flow, verify `client_id` / `client_secret` / `refresh_token`; revoking an access token also revokes its paired refresh token, which requires re-running the authorization-code flow.

**`409` from the token endpoint.**
The `/puboauth/token` bucket (100 requests/hour) is exhausted. The connector retries with backoff. If it recurs, check that nothing else in your environment is minting tokens with the same credentials on every call.

**`403` with `X-Mashery-Error-Code: ERR_403_DEVELOPER_OVER_RATE`, or repeated retry warnings.**
The 1,000-call/day quota is spent. Narrow `root_paths`, disable `include_members`, widen the schedule, or contact `api-support@egnyte.com` for a higher quota. `ERR_403_DEVELOPER_OVER_QPS` (per-second) instead suggests raising `min_request_interval`.

**Warning: "Egnyte events cursor aged out; resuming from oldest retained event id …".**
The stored event cursor fell outside the retention window. The connector recovers automatically but the skipped events are unrecoverable from this endpoint. Increase polling frequency; use the audit reports to backfill the gap for the activity types they cover.

**"audit report job … did not complete after N polls".**
A report window is taking longer than `audit_poll_max_attempts × audit_poll_interval_seconds` (1 hour at the defaults). Reduce `window_days` so each job covers less data, or raise `audit_poll_max_attempts`.

**`domain` errors at startup (`ValueError`).**
`domain` is empty. Supply the subdomain (`acmecorp`), a full custom hostname (`files.acme.com`), or a full URL.

**Empty `groups.members`.**
Expected. The list endpoint does not return members; set `include_members=true` and add it to `externalOptionsAllowList`.

**Empty `files` / `folders`.**
`root_paths` points somewhere the token's user cannot see, or `max_depth` is cutting off the subtree. Confirm with `GET /pubapi/v1/fs/Shared?list_content=true`.

**Timestamps land in the far future.**
You cast `files.uploaded` or `folders.lastModified` directly. They are epoch **milliseconds** as `LongType`; divide by 1000 first.

## Limitations

- **Read-only.** The connector never writes to Egnyte.
- **`folder_permissions` is not implemented.** Egnyte has **no domain-wide "list all permission grants" endpoint** — `/pubapi/v1/perms` and `/pubapi/v2/perms` are per-folder (or per-user) lookups only. A full current-state permission matrix would require one call per folder discovered by the tree walk, an N-call fan-out against the same 2 QPS / 1,000-per-day budget. Use `audit_permissions` to track permission *changes* over time; it cannot reconstruct the full grant matrix in one pass.
- **No incremental read for `files` / `folders`.** No domain-wide "changed since" endpoint exists on the File System API, so every run re-walks the tree. (The Search API v2 `modified_after` filter is a possible future pseudo-incremental path; it does not surface deletes and is unvalidated.)
- **No incremental read for `users` / `groups`.** SCIM `filter` supports only exact match on name-like fields.
- **`events` retention is 30 days**, and either 300,000 or 500,000 events depending on which Egnyte doc you read. Both sources agree on the 30-day ceiling. A cursor that ages out is recovered by resuming from `oldest_event_id`, which means **a permanent gap** in the ingested data.
- **Audit windows are whole calendar days, and today is always excluded.** Offsets are dates and `date_end` is inclusive, so windows tile cleanly; the upper bound is capped at the start of the current UTC day. Audit rows for the current day arrive on the following day's run.
- **Audit Reporting v2 streaming (`/pubapi/v2/audit/stream`) is deliberately not used.** It serves only the trailing 7 days and carries its own much tighter 10/minute, 100/hour limit, so it cannot backfill. It remains a possible future low-latency add-on, not a replacement for v1.
- **Link deletion is not surfaced.** `/pubapi/v2/links` filters on creation only; a deleted link simply stops appearing. `links` is a create-feed, not a CDC stream, and the Events API does not emit link events either.
- **No delete detection anywhere.** No table is `cdc_with_deletes`; the snapshot tables reflect deletions only via a fresh full snapshot.
- **Audit report primary keys are UNVERIFIED.** Report rows carry no server-assigned id; the composite natural keys documented above are the recommended dedup keys pending live confirmation.
- **`links` first run is not parallelized** unless `start_timestamp` is set — an unbounded first run is served as a single open-ended partition.
- **Several `users` fields are UNVERIFIED** (`createdDate`, `lastModificationDate`, `lastActiveDate`, `isServiceAccount`, `language`, `idpUserId`, `userPrincipalName`) — documented by Egnyte but absent from the SDK attribute list. They stay `NULL` if the API does not return them.
- **Deferred resource families.** Search, Comments/Notes, Bookmarks, Trash, Folder Options, Metadata/Custom Properties, Webhooks, Upload Requests, User Insights, Sign, Workflow, Controlled Document Management, eTMF, Document Portal, Project Folders, MSP, Agent, AI, and Navigate are all present in Egnyte's catalog but out of scope for this connector. Egnyte's separate "Secure & Govern" product (`developers.egnyteprotect.com`) is a different API family and is not covered.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/egnyte/egnyte.py`
- HTTP client, auth, and throttling: `src/databricks/labs/community_connector/sources/egnyte/egnyte_client.py`
- Schemas and tuning constants: `src/databricks/labs/community_connector/sources/egnyte/egnyte_schemas.py`
- Audit job lifecycle: `src/databricks/labs/community_connector/sources/egnyte/egnyte_audit.py`
- Source API research notes: `src/databricks/labs/community_connector/sources/egnyte/egnyte_api_doc.md`
- [Egnyte Developer Portal — API catalog](https://developers.egnyte.com/integration/cfs/api-docs/)
- [Egnyte Authentication (OAuth 2.0)](https://developers.egnyte.com/docs/read/Public_API_Authentication)
- [Egnyte File System API](https://developers.egnyte.com/docs/read/File_System_Management_API_Documentation)
- [Egnyte User Management API](https://developers.egnyte.com/docs/read/User_Management_API_Documentation)
- [Egnyte Group Management API](https://developers.egnyte.com/docs/read/Group_Management_API_Documentation)
- [Egnyte Link Management API](https://developers.egnyte.com/docs/read/Link_API_Documentation)
- [Egnyte Events API](https://developers.egnyte.com/docs/read/Events_API_Documentation)
- [Egnyte Audit Reporting API](https://developers.egnyte.com/docs/read/Audit_Reporting_API_Documentation)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

## Connector Information

- **Source**: Egnyte Public API (`https://{domain}.egnyte.com/pubapi/...`), per-tenant; resource families version independently (`v1` / `v2`)
- **Supported Objects**: 9 tables (`files`, `folders`, `users`, `groups`, `links`, `events`, `audit_logins`, `audit_files`, `audit_permissions`)
- **Authentication**: OAuth 2.0 refresh-token flow (`client_id` + `client_secret` + `refresh_token`), or a pre-issued `access_token`
- **Supported Ingestion Types**: `snapshot` (4 tables), `append` (5 tables)
- **Partitioned reads**: `links` and the three `audit_*` tables stream as time windows; `files` / `folders` partition by subtree for batch reads
- **Delete handling**: none — no delete feed on any endpoint
- **Admin requirement**: the three `audit_*` tables require an admin or "can run reports" power user
