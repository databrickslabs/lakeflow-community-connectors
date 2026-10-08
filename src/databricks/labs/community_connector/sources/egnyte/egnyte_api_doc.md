# **Egnyte API Documentation**

Egnyte is a per-tenant (per-domain) content collaboration / cloud file-sharing
platform. Every API call is made against a customer-specific hostname, and
almost the entire surface lives under `/pubapi/` (with numbered sub-versions
per resource, e.g. `v1`, `v2`). There is **no Airbyte or Fivetran connector
for Egnyte** as of this research (verified 2026-08-05 via web search of both
vendors' connector catalogs/docs) — table selection below is therefore based
on a direct survey of Egnyte's own Public API catalog and general "which
objects are valuable for analytics" judgment, not on cross-vendor consensus.

## **Base URL / Domain**

- Pattern: `https://{domain}.egnyte.com/pubapi/{version}/{resource}`
  - `{domain}` is the tenant subdomain (e.g. `acmecorp` → `acmecorp.egnyte.com`).
    If the value supplied already contains a `.`, it is used verbatim (some
    customers have custom-branded domains); otherwise `.egnyte.com` is
    appended. (Source: Python SDK `Session.__init__`, `base.py`.)
  - `{version}` varies **per resource, not globally** — there is no single
    "API version" for the whole product. As of this research:
    - `v1`: File System (`fs`, `fs-content`, `fs-content-chunked`), Links
      (also has a `v2`), Notes/Comments, Events (also has a `v2`),
      Permissions (`perms`, also has a `v2`), Audit Reporting v1
      (`audit/{type}`, `audit/jobs/{id}`), token revoke
      (`tokens/revoke`), user info (`userinfo`).
    - `v2`: Users (`users`), Groups (`groups`), Search (`search`),
      Permissions (`perms`, newer delta-based replacement for v1), Links
      (newer full-object replacement for v1), Events (adds
      `permission_change` events), Audit Reporting v2 — **streaming**
      (`audit/stream`).
  - OAuth token issuance/refresh/revocation happens **outside** `/pubapi`,
    under `/puboauth/token` on the same tenant host (e.g.
    `https://acmecorp.egnyte.com/puboauth/token`).
  - A separate helper service, "Enhanced Auth Service" (used only for the
    interactive, one-time authorization-code step when the tenant domain is
    not yet known), lives on Egnyte's own multi-tenant infra, not the
    customer domain: `https://partner-integrations.egnyte.com/services/`
    (EU) / `https://us-partner-integrations.egnyte.com/services/` (US).
    A connector that already knows the customer's domain and holds a
    long-lived refresh token never needs this service.
  - Egnyte also operates a **separate product/API family, "Secure & Govern"**
    documented at `developers.egnyteprotect.com`, which is out of scope here
    — it is a different product from core Egnyte Connect/content
    collaboration and was not investigated further. UNVERIFIED whether it
    overlaps with anything in `/pubapi`.

## **Authorization**

Egnyte uses OAuth 2.0. Two identifiers matter:
- **API key** (`client_id`) — identifies the integration/application.
- **API token** (`access_token`) — the bearer token for a specific user,
  obtained via one of the OAuth flows below.

**Preferred method for this connector (matches project convention — see
CLAUDE.md OAuth note): Refresh Token flow.**
The connector is configured with `domain`, `client_id`, `client_secret`, and
a previously-obtained `refresh_token`, and exchanges the refresh token for a
fresh `access_token` at the start of each run. It does **not** run the
user-facing authorization step itself; that happens once, out-of-band, when
the integration is first set up (typically via the Authorization Code flow
below), and the resulting `refresh_token` is stored in the connector config.

Token exchange request (used for both the one-time authorization-code
exchange and subsequent refreshes):

```
POST https://{domain}.egnyte.com/puboauth/token
Content-Type: application/x-www-form-urlencoded

client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token&refresh_token={refresh_token}
```

Response:
```json
{
  "access_token": "abcd1234...",
  "refresh_token": "efgh5678...",
  "token_type": "bearer",
  "expires_in": 2592000
}
```
- `access_token` expires after 30 days (`expires_in: 2592000` seconds), or
  immediately if the user's password changes or the token is explicitly
  revoked.
- Using the token: `Authorization: Bearer {access_token}` on every
  `/pubapi/...` request.

**One-time setup (out of band, not run by the connector at runtime) —
Authorization Code flow:**
```
GET https://{domain}.egnyte.com/puboauth/token
    ?client_id={client_id}
    &redirect_uri={callback_url}
    &scope={space-delimited scopes}
    &state={state}
    &response_type=code
```
On approval Egnyte redirects to `{callback_url}?code={code}&state={state}`.
Exchange the code:
```
POST https://{domain}.egnyte.com/puboauth/token
client_id={client_id}&client_secret={client_secret}&code={code}
&grant_type=authorization_code&redirect_uri={callback_url}
```
Response shape is identical to the refresh-token response above (includes
both `access_token` and `refresh_token`).

**Alternative flows** (documented for completeness, not used by this
connector — noted here per Known Quirks rather than as competing primary
methods):
- **Implicit Grant** — browser-only, `response_type=token`, returns the
  token in the URL fragment, no refresh token. Not usable for a server-side
  connector.
- **Resource Owner Password Credentials** — `grant_type=password` with
  `username`/`password` in the body instead of a code. Documented as for
  "internal applications" (Egnyte customers building their own integration)
  only; third-party/public apps cannot use it. Still exchanges for the same
  `access_token`/`refresh_token` pair. This is what the old `python-egnyte`
  SDK's `get_access_token()` helper uses by default.

**Token revocation** (not used in normal operation, documented for
completeness): `POST /pubapi/v1/tokens/revoke` with `token` and
`client_secret` in the body. Revoking an access token also revokes its
paired refresh token.

**Scopes** — space-delimited in the `scope` parameter (Authorization
Code/Implicit) or body field (Resource Owner Password). Scopes needed for
the tables in this doc:

| Scope | Covers |
|---|---|
| `Egnyte.filesystem` | File System, Search, Events, Comments/Notes, Folder Options, User Insights, Trash, Workflows |
| `Egnyte.permission` | Permissions (v1 and v2) |
| `Egnyte.link` | Links (v1 and v2) |
| `Egnyte.user` | User Management |
| `Egnyte.group` | Group Management |
| `Egnyte.audit` | Audit Reporting v1 and v2 |

A connector reading every table in this doc should request:
`Egnyte.filesystem Egnyte.permission Egnyte.link Egnyte.user Egnyte.group Egnyte.audit`.

**Getting current-user info** (useful for connectivity checks / discovering
who the token belongs to):
```
GET https://{domain}.egnyte.com/pubapi/v1/userinfo
Authorization: Bearer {access_token}
```
```json
{ "id": 121, "first_name": "Jane", "last_name": "Smith", "username": "jsmith" }
```

**Admin/role requirement (quirk):** Audit Reporting (both v1 and v2)
requires the token's user to be an admin or a power user with the
"can run reports" permission — a token for a standard user will get `403`
even though the same token works fine for File System / Users / Groups.

## **Object List**

The object list is **static** (fixed set of documented resource families;
Egnyte does not expose a machine-readable "list of available
objects/endpoints" API). The full catalog, discovered via the developer
portal index at `https://developers.egnyte.com/integration/cfs/api-docs/`,
includes far more than what's documented in depth below (Bookmarks,
Comments, Trash, Workflow, Sign, AI, Metadata/custom-properties, Webhooks,
Upload Requests, User Insights, MSP, Controlled Document Management,
eTMF, Document Portal, Project Folders, Navigate, Agent, Remote MCP Server,
etc.) — see **Deferred Tables** at the end.

Tables documented in this pass (chosen as the core, highest analytics-value
objects with the most homogeneous read patterns):

1. **Files & Folders** — `pubapi/v1/fs`
2. **Users** — `pubapi/v2/users`
3. **Groups** — `pubapi/v2/groups`
4. **Links** — `pubapi/v1/links` (+ `v2` variant)
5. **Events** (file-system activity feed) — `pubapi/v1/events` / `v2/events`
6. **Audit Reports: Logins, Files, Permissions** — `pubapi/v1/audit/{type}`
   (job-based; `pubapi/v2/audit/stream` documented as a near-real-time
   alternative)
7. **Folder Permissions** — `pubapi/v1/perms` / `pubapi/v2/perms` (documented
   as a per-folder lookup, not a bulk-enumerable table — see quirks)

---

## **1. Files & Folders**

### Object Schema

**Endpoint:** `GET /pubapi/v1/fs/{full/path/to/file/or/folder}`
(equivalently by opaque ID: `GET /pubapi/v1/fs/ids/{file|folder}/{ID}`)

Each path segment must be URL-encoded individually; the `/` separators
between segments must **not** be encoded (e.g.
`Shared/example?path/$file.txt` → `Shared/example%3Fpath/%24file.txt`).

Query parameters (folder listing):

| Param | Type | Notes |
|---|---|---|
| `list_content` | bool | Include child folders/files in response (folders only) |
| `count` | int | Max items returned (pagination) |
| `offset` | int | 0-based starting index (pagination) |
| `sort_by` | string | `name`, `last_modified`, `uploaded_by`, `custom_metadata` |
| `sort_direction` | string | `ascending` / `descending` |
| `perms` | bool | Include a `permissions` block in the response |
| `include_locks` | bool | Include file lock info |
| `list_custom_metadata` | bool | Include custom-metadata values |

**Folder response shape:**
```json
{
  "name": "Documents",
  "path": "/Shared/Documents",
  "folder_id": "9d270e56-b493-476c-a1a0-example",
  "parent_id": "6e2f...",
  "is_folder": true,
  "permission": "Owner",
  "folder_description": "",
  "public_links": "files_folders",
  "allow_links": true,
  "allow_upload_links": true,
  "restrict_move_delete": false,
  "count": 2,
  "offset": 0,
  "total_count": 2,
  "folders": [
    {"name": "2025", "path": "/Shared/Documents/2025", "folder_id": "...", "is_folder": true, "lastModified": 1716902472000}
  ],
  "files": [
    {
      "name": "Contract.docx",
      "path": "/Shared/Documents/Contract.docx",
      "checksum": "b6f4...sha512",
      "size": 48213,
      "entry_id": "89b1e9d9-8a04-4277-807c-d68107796c76",
      "group_id": "48f2dade-cd0a-472e-ab81-ab4b78135328",
      "parent_id": "9d270e56-...",
      "is_folder": false,
      "locked": false,
      "last_modified": "2025-05-28T11:41:12Z",
      "uploaded": 1716896472000,
      "uploaded_by": "jsmith",
      "num_versions": 3,
      "permission": "Editor"
    }
  ]
}
```

**File response shape** (`GET` on a file path directly returns the object
without the `folders`/`files` wrapper, plus a `versions` array):
```json
{
  "name": "Contract.docx",
  "path": "/Shared/Documents/Contract.docx",
  "checksum": "b6f4...sha512",
  "size": 48213,
  "is_folder": false,
  "entry_id": "89b1e9d9-8a04-4277-807c-d68107796c76",
  "group_id": "48f2dade-cd0a-472e-ab81-ab4b78135328",
  "num_versions": 3,
  "uploaded_by": "jsmith",
  "last_modified": "2025-05-28T11:41:12Z",
  "versions": [
    {"entry_id": "89b1...", "checksum": "...", "last_modified": "2025-05-28T11:41:12Z", "uploaded_by": "jsmith", "size": 48213}
  ]
}
```

### Primary Keys
- **File**: `group_id` (stable identity of the file across versions) is the
  natural business key; `entry_id` identifies one specific version and
  changes on every new upload. UNVERIFIED which one Egnyte itself would
  recommend as canonical PK for a "files" table — recommend `group_id` +
  `path` as a composite, since `path` can also change on rename/move without
  changing `group_id`.
- **Folder**: `folder_id`.

### Ingestion Type
`snapshot`. There is no domain-wide "list all files modified since X"
endpoint on the File System API itself — you must recursively walk
`fs` starting from a root (e.g. `/Shared`, `/Private`) using `list_content`,
`offset`/`count` pagination at each folder level. This is inherently a full
tree walk each run.

**Incremental alternative (documented, not yet validated live):** the
**Search API v2** (`POST /pubapi/v2/search`) supports `modified_after` /
`modified_before` filters and can be queried with `type=FILE` and a
non-empty-but-broad `query` or `custom_metadata` filter, giving a
pseudo-incremental "changed since" read without a full tree walk. It does
**not** surface deletes. Combined with the **Events** table (action=`delete`)
for deletion detection, Files could become `cdc_with_deletes` — flagged as a
follow-up enhancement, not implemented in this pass. `search_by_metadata`
availability, exact `modified_after` semantics, and result completeness for
large tenants are UNVERIFIED and should be confirmed during live validation.

### Read API for Data Retrieval — Recursive Listing
```
GET https://{domain}.egnyte.com/pubapi/v1/fs/Shared?list_content=true&count=100&offset=0
Authorization: Bearer {access_token}
```
Recurse into each entry in `folders[]`. Use `count`/`offset` when
`total_count` exceeds the page returned. Rate-limited like all other
`/pubapi` calls (see Rate Limiting section) — a large tenant tree walk can
consume the full daily quota; consider spreading it or using a
higher-quota arrangement (contact `api-support@egnyte.com`).

---

## **2. Users**

### Object Schema
**Endpoint:** `GET /pubapi/v2/users` (list) / `GET /pubapi/v2/users/{id}` (single)
SCIM-influenced (Egnyte docs state it "adheres to the SCIM standard").

Query parameters (list):

| Param | Type | Notes |
|---|---|---|
| `startIndex` | int | 1-based index of first record (default 1) |
| `count` | int | Page size, max 100 |
| `filter` | string | `{field} eq "{value}"` on `email`, `externalId`, or `userName` only — **no free-text or wildcard search, no modified-since filter** |

**List response:**
```json
{
  "totalResults": 532,
  "itemsPerPage": 100,
  "startIndex": 1,
  "resources": [
    {
      "id": 9967960066,
      "userName": "jsmith",
      "email": "jsmith@acme.com",
      "externalId": "S-1-5-21-...",
      "name": {"givenName": "Jane", "familyName": "Smith", "formatted": "Jane Smith"},
      "active": true,
      "locked": false,
      "authType": "sso",
      "userType": "power",
      "role": "Default"
    }
  ]
}
```
Fields reported by the official docs but **not** present in either version
of the Python SDK's `_lazy_attributes` list (SDK is several years older than
the current docs) — treat as **UNVERIFIED** until confirmed live:
`createdDate`, `lastModificationDate`, `lastActiveDate`, `isServiceAccount`,
`language`, `idpUserId`, `userPrincipalName`, `groups` (nested array).

### Primary Key
`id` (integer — confirmed indirectly: Group `members[].value` references
this same integer user id, e.g. `9967960066`).

### Ingestion Type
`snapshot`. The `filter` query parameter only supports exact-match `eq` on
`email`/`externalId`/`userName` — there is no `lastModificationDate`-style
filter usable server-side for incremental pulls, even though the field may
exist on the object (UNVERIFIED). Client-side diffing against a full list
pull is the safe approach, matching the pattern used for other SCIM-style
APIs without incremental filters.

### Read API for Data Retrieval
```
GET https://{domain}.egnyte.com/pubapi/v2/users?startIndex=1&count=100
Authorization: Bearer {access_token}
```
Page by incrementing `startIndex` by `count` until
`startIndex + itemsPerPage - 1 >= totalResults`.

---

## **3. Groups**

### Object Schema
**Endpoint:** `GET /pubapi/v2/groups` (list) / `GET /pubapi/v2/groups/{id}` (single)

Query parameters (list):

| Param | Type | Notes |
|---|---|---|
| `startIndex` | int | 1-based index (default 1) |
| `count` | int | Page size, 1–100 |
| `filter` | string | `displayName eq/co/sw "{value}"` — prefix (`*x`) or contains (`*x*`) wildcard support via `sw`/`co` operators |

**List response:**
```json
{
  "schemas": ["urn:scim:schemas:core:1.0"],
  "totalResults": 3,
  "itemsPerPage": 3,
  "startIndex": 1,
  "resources": [
    {"id": "8b9dd5aa-40ab-4a43-9b76-f55a412c8a6a", "displayName": "IT"}
  ]
}
```
**Single-group response** (includes membership — not returned by the list
endpoint):
```json
{
  "schemas": ["urn:scim:schemas:core:1.0"],
  "id": "e3ba9d90-ebc7-483e-abaa-a84e92480c86",
  "displayName": "Finance",
  "members": [
    {"username": "jsmith", "value": 9967960066, "display": "Jane Smith"}
  ]
}
```

### Primary Key
`id` (GUID string).

### Ingestion Type
`snapshot`. Same limitation as Users: no modified-since filter. Membership
(`members`) is only returned on the single-group `GET`, so a full sync
needs a list call followed by one `GET` per group id if membership is
required as part of the analytics model — this is an N+1 fan-out and should
be flagged to the implementer as a rate-limit/runtime cost consideration.

### Read API for Data Retrieval
```
GET https://{domain}.egnyte.com/pubapi/v2/groups?startIndex=1&count=100
```
then per-group, if membership needed:
```
GET https://{domain}.egnyte.com/pubapi/v2/groups/{id}
```

---

## **4. Links**

### Object Schema
Two versions coexist:

**v1 — list only returns IDs:**
```
GET /pubapi/v1/links?path=...&username=...&created_before=YYYY-MM-DD&created_after=YYYY-MM-DD&type=File|Folder&accessibility=Anyone|Password|Domain|Recipients&offset=0&count=100
```
```json
{ "ids": ["c9c8e6b0-...", "a1b2..."], "offset": 0, "count": 2, "total_count": 5 }
```
Each id must then be fetched individually: `GET /pubapi/v1/links/{id}`.

**v2 — list returns full objects directly** (preferred for a connector —
avoids the v1 N+1 fan-out):
```
GET /pubapi/v2/links?path=...&created_after=...&type=file|folder|upload&accessibility=anyone|password|domain|recipients&offset=0&count=100
```
Response includes full `Link` objects per row:
```json
{
  "id": "c9c8e6b0-1234-4abc-9def-000000000001",
  "url": "https://acme.egnyte.com/dl/abc123",
  "path": "/Shared/Documents/Contract.docx",
  "type": "file",
  "accessibility": "recipients",
  "protection": "none",
  "recipients": ["reviewer@partner.com"],
  "notify": true,
  "link_to_current": false,
  "creation_date": "2025-05-20T09:00:00Z",
  "created_by": "jsmith",
  "resource_id": "48f2dade-cd0a-472e-ab81-ab4b78135328"
}
```
Note: v2 `created_after`/`created_before` require the `+` in an ISO-8601
offset to be URI-encoded as `%2B` — a real gotcha for naive query-string
builders.

### Primary Key
`id` (string/GUID).

### Ingestion Type
`append`. `created_after` is a genuine server-side incremental filter — new
links can be pulled by date. Link **deletion** is not surfaced by this
endpoint (a deleted link simply stops appearing); full delete tracking would
require periodic full re-lists and diffing, or cross-referencing the Events
API (UNVERIFIED whether Events surfaces link deletion as its own event
type — the documented Events `type` enum only covers `file_system`, `note`,
and, in v2, `permission_change`; links do not appear in it).

### Read API for Data Retrieval
```
GET https://{domain}.egnyte.com/pubapi/v2/links?created_after=2026-08-01T00%2B00%3A00&offset=0&count=100
```
Page via `offset`/`count` (max 500/request per docs); use `total_count` (v1)
/ equivalent v2 field (UNVERIFIED exact pagination-metadata field name for
v2 list — confirm live) to know when to stop.

---

## **5. Events** (file-system activity feed)

This is Egnyte's lightweight, always-on activity stream — distinct from the
heavier, job-based Audit Reporting API. It is the best incremental source
for "what changed" without waiting on an async report job.

### Object Schema
**Cursor endpoint:** `GET /pubapi/v1/events/cursor` (or `v2/events/cursor`)
```json
{ "latest_event_id": 16342, "oldest_event_id": 1, "timestamp": "2026-08-05T00:00:00Z" }
```

**List endpoint:** `GET /pubapi/v1/events?id={cursor}&count=50&folder=...&suppress=app|user&type=file_system|note`
(v2 adds `permission_change` to the `type` enum and to default coverage)

```json
{
  "latest_id": 16342,
  "oldest_id": 16300,
  "count": 1,
  "events": [
    {
      "id": 16342,
      "timestamp": "2025-05-28T11:41:12.000Z",
      "action_source": "WebUI",
      "actor": 1,
      "type": "file_system",
      "action": "copy",
      "data": {
        "target_path": "/Shared/Documents/My Contract.docx",
        "target_id": "89b1e9d9-8a04-4277-807c-d68107796c76",
        "target_group_id": "48f2dade-cd0a-472e-ab81-ab4b78135328",
        "source_path": "/Shared/Contracts/My Contract.docx",
        "source_id": "85756cb4-7d82-439c-9ea4-be80eaebaecf",
        "source_group_id": "19f157de-267a-45e9-903a-a34fdf3a3e4e",
        "is_folder": false
      }
    }
  ]
}
```
`action` values include at least `create`, `delete`, `restore`, `move`,
`copy`, `rename`, and (v2, `permission_change` type) `permission_change`.
`actor` is a user `id` (join to Users table).

### Primary Key
`id` (integer, monotonically increasing).

### Ingestion Type
`append`. This is a pure event log — read forward from a stored cursor
(`id`), never re-read or update past rows.

**Retention quirk / source conflict:** the community "Integrations
Cookbook" (egnyte.github.io, older doc) states the stream retains the latest
**300,000** events for up to **30 days**. The current official docs
(`developers.egnyte.com/integration/cfs/api-docs/events-api`) state
**500,000** events for up to 30 days. Both sources agree on the 30-day
ceiling; the exact count differs — likely the limit was raised at some
point and the cookbook page is stale. **Practical implication either way:**
if a connector polls too infrequently, the cursor can fall outside the
retained window, causing the next `list` call to `404`. On `404`, call
`/cursor` again and resume from its `oldest_event_id` (accepting a gap) per
official guidance. Recommended minimum poll interval: 5 minutes (per both
sources).

### Read API for Data Retrieval
```
GET https://{domain}.egnyte.com/pubapi/v1/events/cursor    # once, to bootstrap
GET https://{domain}.egnyte.com/pubapi/v1/events?id={last_saved_id}&count=100
```
Store the last-seen `id` (or `latest_id` from the response) as the
connector's cursor state. `204 No Content` means no new events since the
supplied `id` — not an error.

---

## **6. Audit Reports** (Logins, Files, Permissions)

Egnyte has **two generations** of this API that a connector implementer
must choose between (or combine):

### 6a. Audit Reporting API v1 — job-based, historical, any date range

**Step 1 — create a report job:**
```
POST https://{domain}.egnyte.com/pubapi/v1/audit/logins
Content-Type: application/json
Authorization: Bearer {access_token}

{
  "format": "json",
  "date_start": "2026-07-01",
  "date_end": "2026-07-31",
  "events": ["successful_login", "failed_attempts"]
}
```
Analogous bodies exist for:
- `POST /pubapi/v1/audit/files` — `folders`, `file`, `users`,
  `transaction_type` filters
- `POST /pubapi/v1/audit/permissions` — `folders`, `assigners`,
  `assignee_users`, `assignee_groups` (all required per the SDK signature)
- Also present per the current endpoint catalog (not detailed in this pass,
  see Deferred Tables): `users`, `groups`, `workgroup-settings`,
  `workflows`, `workflow-templates`, `quality-docs`, `etmf`,
  `snapshot-restore`, `upload-requests`.

Response: `202 Accepted` with a job reference:
```json
{ "id": "job-8ad3f1c2" }
```

**Step 2 — poll job status:**
```
GET https://{domain}.egnyte.com/pubapi/v1/audit/jobs/{id}
```
- `200 OK` + `{"status": "running"}` → not ready yet. Poll no more often
  than once every 2 minutes (official guidance).
- `303 See Other` + a `Location` header pointing at the completed report →
  ready.

**Step 3 — fetch the completed report:**
```
GET https://{domain}.egnyte.com/pubapi/v1/audit/{type}/{id}?offset=0&count=100
```
(`{type}` is `logins`/`files`/`permissions`/etc., matching Step 1;
`offset`/`count` paginate a `json`-format report.)

Sample **login** report page:
```json
{
  "total_count": 100, "offset": 10, "count": 2,
  "events": [
    {"username": "John Smith (jsmith@company.com)", "user_id": 121,
     "event": "Failed Attempt", "ip_address": "198.51.100.0",
     "access": "Web", "time": "2026-07-26T18:35:00Z"}
  ]
}
```
Sample **file** report page:
```json
{
  "total_count": 100, "offset": 10, "count": 1,
  "events": [
    {"username": "John Smith (jsmith@company.com)", "user_id": 121,
     "file": "/Shared/Documents/example.txt",
     "target_path": "/Shared/Documents/subfolder",
     "transaction": "Move File", "access": "Mobile",
     "time": "2026-02-15T07:58:17Z"}
  ]
}
```
Sample **permissions** report page:
```json
{
  "total_count": 100, "offset": 10, "count": 1,
  "events": [
    {"folder": "/Shared/Marketing",
     "assignee": "Sarah Doerr (sdoerr@company.com)", "assignee_id": 107,
     "assigner": "John Doe (jdoe@company.com)", "assigner_id": 101,
     "change": "+Editor", "time": "2026-05-26T18:35:00Z"}
  ]
}
```

### Primary Key
UNVERIFIED — none of the three sampled report row shapes include an
explicit row-level `id` field. Treat each row as an immutable append-only
log line; recommend a composite natural key of
`(user_id, time, event|transaction|change)` for dedup purposes until a real
key is confirmed against live data.

### Ingestion Type
`append`. Each `date_start`/`date_end` window produces an immutable set of
historical rows. A connector should track the last successfully-audited
`date_end` and request the next contiguous window on each run (with some
overlap/lookback to be safe, since "when did this event actually get
indexed" vs. "when did it happen" could differ slightly — UNVERIFIED).

### 6b. Audit Reporting API v2 — streaming, near-real-time, 7-day window only

Documented as the more modern alternative for near-real-time needs. **Does
not replace v1** for historical backfill — v2 only serves the trailing 7
days; anything older still requires v1.
```
GET/POST https://{domain}.egnyte.com/pubapi/v2/audit/stream
  ?startDate=2026-08-01&auditType=FILE_AUDIT&auditType=LOGIN_AUDIT
```
or, to continue from a prior page: `?nextCursor={cursor}`.

Response:
```json
{
  "nextCursor": "AAN_lwABAX1zZe9AAAAAAAAAAAAAAAAAAAAAAA",
  "events": [
    {"date": 1638936585716, "sourcePath": "/Shared/Departments/Marketing/Branding/Logo.jpg",
     "targetPath": "N/A", "user": "Jack Smith ( jsmith@company.com )", "userId": "101",
     "action": "Preview", "access": "Web UI", "ipAddress": "173.226.89.189",
     "actionInfo": "", "auditSource": "FILE_AUDIT"},
    {"date": 1638940605824, "actor": "Jennifer Watkins ( jwatkins@company.com )",
     "subject": "Paul Chen ( pchen@company.com )", "action": "Disable",
     "actionInfo": "", "source": "Web UI", "auditSource": "USER_AUDIT"}
  ],
  "moreEvents": true
}
```
- Up to 5,000 events per page; each request internally spans up to a
  30-minute slice of history.
- `moreEvents: false` → drained; save `nextCursor` anyway to resume later.
- **Cursors expire after 7 days** — a `400` on resume means restart with a
  fresh `startDate` (accepting a gap, or falling back to v1 to backfill it).
- Requires admin / "can run reports" role, same as v1.
- **Rate limited independently and much more tightly than the rest of the
  API**: 10 requests/minute, 100 requests/hour (vs. the standard 2 QPS /
  1000-per-day for the rest of `/pubapi`). Exceeding it returns `429` with
  `Retry-After`.
- `auditType` enum observed: `FILE_AUDIT`, `LOGIN_AUDIT`, `PERMISSION_AUDIT`,
  `USER_AUDIT`, `GROUP_AUDIT`, `WG_SETTINGS_AUDIT`, `WORKFLOW_AUDIT`, and at
  least 7 more (UNVERIFIED full enum — confirm live or via a docs page
  fetch during validation).

**Recommendation for implementation:** use v1 (job-based) as the primary,
reliable read path for Logins/Files/Permissions since it supports arbitrary
historical ranges and standard rate limits; treat v2 streaming as a future
optimization for lower-latency incremental audit ingestion, not a
replacement, given its narrow 7-day/aggressive-rate-limit constraints.

---

## **7. Folder Permissions** (documented, not a bulk-enumerable table)

### Object Schema
Two versions:

**v1 (deprecated but still live):**
```
GET /pubapi/v1/perms/folder/{path}?users=jsmith|ajones&groups=Marketing
```
```json
{
  "users": [{"subject": "jsmith", "permission": "Full"}, {"subject": "ajones", "permission": "Viewer"}],
  "groups": [{"subject": "All Power Users", "permission": "Editor"}]
}
```
**v2 (current):**
```
GET /pubapi/v2/perms/{path}
```
```json
{
  "userPerms": {"jsmith": "Full", "ajones": "Viewer"},
  "groupPerms": {"All Administrators": "Owner", "Marketing Team": "Editor"},
  "inheritsPermissions": true
}
```
**Effective permission for one user on one folder:**
```
GET /pubapi/v1/perms/user/{username}?folder=/Shared/Documents
```
```json
{ "permission": "Full" }
```
Permission-level enum: `None`, `Nav`, `Viewer Only`, `Viewer`, `Editor`,
`Full`, `Owner`.

### Primary Key
Composite: `(folder_path, subject_type, subject_name)` — there is no
surrogate id.

### Ingestion Type
`snapshot`, and only practically feasible **on demand / per folder** — there
is **no domain-wide "list all permission grants" endpoint**. A connector
wanting a full permissions table would have to call this once per folder
discovered by the Files & Folders walk, which is an expensive N-calls
fan-out gated by the same 2 QPS / 1000-per-day token limits as everything
else. **Recommendation:** treat this as a **deferred / opt-in** table
(document it, but don't include it in a default sync plan) unless the
target tenant's folder count is known to be small. The **Permission
Audit Report** (section 6) is a much cheaper way to see permission
*changes* over time, even though it can't give you the full current-state
grant matrix in one call.

---

## **Field Type Mapping**

| Egnyte type / representation | Standard type | Notes |
|---|---|---|
| `string` (path, name, username, displayName, etc.) | `string` | UTF-8; paths use `/`-delimited segments |
| integer epoch milliseconds (`uploaded`, `lastModified`, audit `date`) | `timestamp` | Divide by 1000 for seconds; some endpoints instead use... |
| ISO-8601 string (`last_modified`, `time`, `creation_date`, `timestamp`) | `timestamp` | Mixed formats seen: `YYYY-MM-DDTHH:MM:SSZ` and `YYYY-MM-DDTHH:MM:SS.sssZ` — parse permissively |
| `boolean` (`is_folder`, `active`, `locked`, `notify`, ...) | `boolean` | — |
| `integer` (`size`, `id` for users/events, `total_count`, ...) | `long`/`integer` | User `id` observed as a large integer (e.g. `9967960066`), not a GUID |
| GUID string (`folder_id`, `entry_id`, `group_id`, group `id`, link `id`) | `string` | Treat as opaque strings, don't assume UUID v4 format universally |
| enum string (`permission`, `accessibility`, `authType`, `userType`, `action`, `auditSource`) | `string` | Validate against documented enum values; new values may appear (additive) |
| nested object (`name: {givenName, familyName}`, `data: {...}` on events) | `struct` | Shape varies by `type`/`action` — see Events section for the `data` sub-schemas |
| array of objects (`resources`, `folders`, `files`, `events`, `members`) | `array<struct>` | Response envelope wrapper, not the record itself |

## **Rate Limiting**

Two independent tiers apply, and a connector must respect **both**:

1. **Standard `/pubapi` calls** (default, per access token):
   - **2 API calls/second/token**
   - **1,000 API calls/day/token**
   - Enforced **per access token**, not per API key/client_id — multiple
     users under the same integration don't share one quota.
   - On breach: **`403`** with header `X-Mashery-Error-Code` set to
     `ERR_403_DEVELOPER_OVER_QPS` (per-second) or `ERR_403_DEVELOPER_OVER_RATE`
     (daily). `Retry-After` header gives seconds until reset.
   - **Quirk / source conflict:** newer official docs (e.g. Permissions API
     and File System API error tables) describe throttling as returning
     **`429`** with `Retry-After` instead of `403` + `X-Mashery-Error-Code`.
     Both behaviors are documented by Egnyte itself on different pages.
     **A robust connector should treat both `403` (checking for the
     `X-Mashery-Error-Code` header) and `429` as throttle signals**, and
     always honor `Retry-After` when present.
   - Proactive monitoring (avoid ever hitting the hard error): parse
     `X-Accesstoken-Qps-Current` / `X-Accesstoken-Qps-Allotted` and
     `X-Accesstoken-Quota-Current` on every response; back off briefly (e.g.
     sleep ~1s) if current is at/near allotted.
2. **OAuth token endpoint** (`/puboauth/token`): **100 requests/hour**. On
   breach: **`409`**, with `Retry-After`. In practice this should almost
   never be hit if the connector caches and reuses tokens/refresh tokens
   rather than requesting a new one per call.
3. **Audit Reporting v2 streaming** (`/pubapi/v2/audit/stream`) has its own,
   much tighter, independent limit: **10 requests/minute, 100 requests/hour**,
   `429` + `Retry-After` on breach.
4. (Not relevant to tables in this doc, noted for completeness) **AI APIs**:
   100 calls/day, 10 calls/minute, 2 calls/second.

If anticipated call volume will exceed defaults, Egnyte says to contact
`api-support@egnyte.com` for a higher-quota arrangement.

## **Error Response Shape**

Two shapes have been observed across different documentation pages —
**UNVERIFIED which is authoritative for which specific endpoint**; a
connector's error handling should check for both:

**Shape A** (official best-practices page):
```json
{ "Errors": [ { "description": "Link does not exist.", "code": "404" } ] }
```

**Shape B** (implied by the Python SDK's `extract_errors`, which walks a
lowercase `errors` key and an optional nested `inputErrors`):
```json
{ "errors": { "inputErrors": [ { "code": "...", "message": "..." } ] } }
```

Standard HTTP status codes used across the APIs in this doc: `400` (bad
request/invalid params), `401` (missing/expired token), `403`
(insufficient permissions, or legacy-style throttle — see Rate Limiting),
`404` (not found — also returned by `Events` when a cursor has aged out),
`409` (conflict, e.g. duplicate folder; also OAuth-token-endpoint
throttling), `413` (payload/file too large), `429` (rate limited, modern
style).

## **Known Quirks Summary** (implementer checklist)

- **Domain is a required, per-tenant config value** — there is no way to
  discover it from a token; it must be supplied (subdomain or full custom
  hostname).
- **No single API version** — every resource family has its own `v1`/`v2`,
  and several (`links`, `perms`, `events`) have *both* live simultaneously
  with different response shapes. Pick a version per-resource, don't assume
  consistency.
- **Path encoding**: encode each path segment separately; never encode the
  `/` separators.
- **Users/Groups (SCIM-style) have no incremental filter** — only exact-
  match `eq` (users) or `eq`/`co`/`sw` (groups) on name-like fields. Full
  snapshot + client-side diff is required for change detection.
- **Group membership is not included in the list response** — requires a
  per-group `GET` fan-out.
- **Files & Folders has no domain-wide "list changed files" endpoint** —
  requires a full recursive tree walk per run, or the Search API v2
  `modified_after` filter as an unverified alternative.
- **Folder Permissions has no domain-wide list endpoint** — only
  per-folder/per-user lookups; treat as opt-in/deferred, not a default
  sync target.
- **Audit Reporting is job-based (v1) and admin-gated** — budget for the
  create→poll→fetch lifecycle (poll no more than once every 2 minutes) and
  ensure the connecting user has admin/report-running rights.
- **Audit Reporting v2 streaming only covers the trailing 7 days** and has
  an independently tight rate limit — not a drop-in replacement for v1.
- **Events retention is either 300K/30 days or 500K/30 days depending on
  which Egnyte doc you read** — code defensively (handle `404` on a
  too-old cursor by resuming from `/cursor`'s `oldest_event_id`).
- **Throttling response shape is inconsistent across docs** (`403`+header
  vs. `429`+`Retry-After`) — handle both.
- **Error envelope key casing is inconsistent** (`Errors` vs `errors`) —
  check both, case-insensitively if convenient.
- **No canonical row-level primary key documented for Audit Report rows**
  (v1 report rows or v2 stream events) — use a composite natural key until
  live-validated otherwise.

## **Deferred Tables**

The following resource families exist in Egnyte's Public API catalog
(confirmed via the developer-portal index at
`developers.egnyte.com/integration/cfs/api-docs/`) but were **not**
researched in depth in this pass, either because they are secondary in
analytics value, have highly divergent/niche API shapes, or are gated
behind product add-ons not universally enabled:

- **Search API v2** (`pubapi/v2/search`) — not a distinct "table" so much
  as an alternate read-path for Files; noted above as a candidate
  incremental strategy for the Files & Folders table, not documented as its
  own object.
- **Comments/Notes** (`pubapi/v1/notes`) — small, low analytics value; basic
  shape captured incidentally via the SDK (`file_id`, `message`, `username`,
  `creation_time`) but not fully documented.
- **Bookmarks API**, **Trash API**, **Folder Options API** — low
  analytics value, simple CRUD-style resources tied to Files/Folders.
- **Metadata / Custom Properties API** (`pubapi/v1/properties/namespace/...`)
  — schema-defining API (namespaces/keys), not itself a data table; values
  surface as `custom_metadata`/`custom_properties` fields on Files and
  Search results, which are covered incidentally above.
- **Webhooks API** — push-based, not a pollable "table"; would change the
  connector's architecture (listener) rather than fitting the read-API
  pattern used elsewhere in this doc.
- **Upload Requests API**, **User Insights API**, **Sign API**,
  **Workflow API** (+ Workflow Templates), **Controlled Document
  Management API**, **eTMF API**, **Document Portal API**, **Project
  Folder / Project Custom Metadata APIs**, **MSP API**, **Agent API**,
  **AI API**, **Navigate API** — all product-specific/add-on features with
  their own auth/scoping quirks (e.g. `Egnyte.sign`, `Egnyte.ai` scopes) and
  meaningfully different data shapes from the core file-collaboration
  tables; each would need its own dedicated research pass if a future
  connector iteration wants to cover them.
- **Third-Party Integrations**, **UI Integration Framework**, **Mobile
  Development**, **Remote MCP Server** — developer-tooling/UI-embedding
  concerns, not data-ingestion APIs; explicitly out of scope for a
  connector.

---

## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/authentication | 2026-08-05 | High | OAuth flows, scopes list, token lifecycle, revoke endpoint, userinfo |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/best-practices | 2026-08-05 | High | Error envelope shape, rate-limit headers, path encoding, impersonation headers |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/file-system-management | 2026-08-05 | High | fs endpoints, file/folder schema, chunked upload, error codes |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/user-management-api | 2026-08-05 | High | Users v2 endpoints, SCIM filter syntax, list/single schema |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/group-management | 2026-08-05 | High | Groups v2 endpoints, list/single/create/update/delete, sample JSON |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/permissions-api | 2026-08-05 | High | Perms v1 and v2 endpoints, sample JSON, permission enum, errors |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/links-api | 2026-08-05 | High | Links v1 (id-list) vs v2 (full-object) shapes, query params |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/events-api | 2026-08-05 | High | Events v1/v2 endpoints, cursor mechanism, event/data schema, retention (500K/30d variant) |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/audit-reporting-api/v1 | 2026-08-05 | High | Job creation endpoints per report type, polling contract (200 running / 303 done), sample JSON per report type |
| Official Docs (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/audit-reporting-api/v2 | 2026-08-05 | High | Streaming endpoint, cursor/`nextCursor`, 7-day window, rate limits, sample JSON |
| Official Docs (via WebSearch snippet; direct WebFetch returned only page `<title>` — ReadMe.io-rendered) | https://developers.egnyte.com/docs/read/getting_started | 2026-08-05 | Medium | Corroborated base URL pattern, `/pubapi` structure |
| GitHub Pages (Egnyte-maintained Integrations Cookbook, static HTML) | https://egnyte.github.io/integrations-cookbook/throttling.html | 2026-08-05 | High (raw HTML fetched directly via curl, not summarized) | 2 QPS/1000-per-day per token, `X-Mashery-Error-Code`, 403 + Retry-After, 409 on oauth token endpoint |
| GitHub Pages (Egnyte-maintained Integrations Cookbook, static HTML) | https://egnyte.github.io/integrations-cookbook/auth.html | 2026-08-05 | High (raw HTML) | OAuth flow choices by app type, Enhanced Auth Service purpose |
| GitHub Pages (Egnyte-maintained Integrations Cookbook, static HTML) | https://egnyte.github.io/integrations-cookbook/api-examples.html | 2026-08-05 | High (raw HTML) | Full space-delimited scope list used in practice: `Egnyte.filesystem Egnyte.user Egnyte.group Egnyte.link Egnyte.permission Egnyte.bookmark Egnyte.launchwebsession` |
| GitHub Pages (Egnyte-maintained Integrations Cookbook, static HTML) | https://egnyte.github.io/integrations-cookbook/events-app.html | 2026-08-05 | High (raw HTML) | Events API purpose, 300K-events/30-day retention (older figure), cursor-reset guidance, poll interval guidance |
| GitHub Pages (Egnyte-maintained Integrations Cookbook, static HTML) | https://egnyte.github.io/integrations-cookbook/user-provisioning.html | 2026-08-05 | High (raw HTML) | SCIM 1.1 compatibility note for Users/Groups |
| Official SDK source (raw, read directly, current) | https://raw.githubusercontent.com/egnyte/python-egnyte/master/egnyte/resources.py | 2026-08-05 | High | Confirmed exact endpoint templates for fs, links, users, groups, perms, notes; confirmed Search v2 endpoint and params |
| Official SDK source (raw, read directly, current) | https://raw.githubusercontent.com/egnyte/python-egnyte/master/egnyte/events.py | 2026-08-05 | High | Confirmed Events endpoint templates, `Event`/`Events` field semantics, `data` sub-schema per action type |
| Official SDK source (via Sphinx-rendered docs, HTML stripped and read directly) | https://pythonhosted.org/egnyte/_modules/egnyte/audits.py (audits.html) | 2026-08-05 | High | Confirmed job-based audit lifecycle: POST creates job → `pubapi/v1/audit/jobs/{id}` polled (200 running / 303 done) → `pubapi/v1/audit/{type}/{id}` fetched |
| Official SDK source (via Sphinx-rendered docs, HTML stripped and read directly) | https://pythonhosted.org/egnyte/_modules/egnyte/base.py | 2026-08-05 | High | Domain resolution logic (`+ ".egnyte.com"`), Bearer header usage, password-grant token acquisition helper, `ResultList` (offset/total_count) pagination pattern |
| Official SDK source (via Sphinx-rendered docs, HTML stripped and read directly) | https://pythonhosted.org/egnyte/_modules/egnyte/exc.py | 2026-08-05 | High | HTTP-status-to-exception mapping (400/401/403/404/409/413/303), lowercase `errors`/`inputErrors` error-extraction shape |
| WebSearch (no direct fetchable page found) | Query: "Egnyte Airbyte connector source-egnyte" | 2026-08-05 | High (absence confirmed) | No Airbyte connector exists for Egnyte |
| WebSearch (no direct fetchable page found) | Query: "Egnyte Fivetran connector" | 2026-08-05 | High (absence confirmed) | No standard Fivetran connector exists for Egnyte |
| WebSearch (aggregated, page not independently fetched) | Query: "Egnyte pubapi/v2/audit streaming events nextCursor eventTypes" | 2026-08-05 | Medium (corroborated by official docs fetch above) | Preliminary confirmation of v2 streaming shape before it was directly fetched from official docs |
| Developer portal index (server-rendered variant) | https://developers.egnyte.com/integration/cfs/api-docs/overview (list rendering) | 2026-08-05 | High | Full current catalog of `/cfs/api-docs/*` resource pages, used to build the Deferred Tables list |

**Note on source reliability for this doc:** `developers.egnyte.com` serves
at least two different front-ends for what is nominally the same content —
a ReadMe.io-style JS-rendered set of pages under `/docs/read/...` and
`/api-docs/read/...` (which reliably defeats `WebFetch`, returning only the
page `<title>`, consistent with prior findings for other ReadMe.io-hosted
vendors), and a second, apparently server-rendered set under
`/integration/cfs/api-docs/...` that `WebFetch` retrieved successfully and
in detail. The `/integration/cfs/api-docs/...` pages were treated as the
primary official source for this doc precisely because they were verifiably
fetched (specific GUIDs, exact field names, and internally-consistent
detail across many separately-fetched pages), and were cross-referenced
against the (older, but directly-read-as-raw-text, not
summarized-by-a-tool) Python SDK source and the static Integrations
Cookbook GitHub Pages site wherever both existed for the same resource.
Where the two disagreed (event retention counts, throttle status code),
both figures are recorded and flagged above rather than silently resolved.
