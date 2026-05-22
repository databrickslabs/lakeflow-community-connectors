# **actiTIME API Documentation**

> **Scope decision (Phase 1)** — The orchestrator did not specify a table list. Neither Airbyte nor Fivetran publish an actiTIME connector, so reference connectors were unavailable. The table set below was derived from the official actiTIME REST API v1 documentation (https://www.actitime.com/api-documentation), prioritizing the resources that represent core time-tracking business data and reference dimensions. All endpoints share the same auth model, base URL, pagination conventions, error format, and rate limits — so they are documented in a single batch (no deferred tables).
>
> **Tables in scope (16)**: `customers`, `projects`, `tasks`, `timetrack`, `leavetime`, `users`, `departments`, `userGroups`, `userRates`, `typesOfWork`, `leaveTypes`, `workflowStatuses`, `timeZoneGroups`, `settings`, `holidays`, `approvalStatus`.

---

## **Authorization**

- **Chosen method**: HTTP Basic Authentication.
- **Base URL**: `https://<your_actitime_url>/api/v1`
  - Online: e.g. `https://online.actitime.com/<tenant>/api/v1`
  - Self-hosted: `https://<host>/api/v1` (requires actiTIME Self-Hosted 2019.2+)
- **Auth placement**:
  - HTTP header: `Authorization: Basic <base64(username:password)>`
  - Credentials are the actiTIME username (or email) and password of a service account.
- **Required headers**:
  - `Accept: application/json; charset=UTF-8`
  - `Content-Type: application/json; charset=UTF-8` (for write requests; not required for GET)
- **Connector storage**: connector stores `base_url`, `username`, `password`. There is no OAuth flow; no token exchange is required at runtime.
- **Permissions**: the service account needs read permission on the resources being ingested. To read PII fields from `/users` (`username`, `email`, `hired`, `releaseDate`) the account must hold the **"Manage Accounts & Permissions"** permission. Without it, those fields are omitted from the response.

### Example authenticated requests

```bash
# Using -u (recommended)
curl -X GET \
  -H "Accept: application/json; charset=UTF-8" \
  -u "username:password" \
  "https://<actitime_url>/api/v1/customers"

# Using explicit Authorization header
curl -X GET \
  -H "Accept: application/json; charset=UTF-8" \
  -H "Authorization: Basic $(printf 'username:password' | base64)" \
  "https://<actitime_url>/api/v1/customers"
```

### Auth failure behavior

- After **3 failed authentication attempts within 10 seconds**, the source IP is banned for **1 minute**. The connector must back off and retry with exponential delay on `401` responses.
- A `401` while inside a long ingestion run usually indicates an account password reset; the run should fail fast.

---

## **Object List**

The object list is **static**. actiTIME does not provide a discovery endpoint that enumerates all resources. The connector hardcodes the supported resources based on the published API v1 reference.

### Core business objects

| Object | Description | Primary endpoint | Ingestion type |
|---|---|---|---|
| `customers` | Customer/client entities | `GET /customers` | `cdc` |
| `projects` | Projects belonging to customers | `GET /projects` | `cdc` |
| `tasks` | Tasks within projects | `GET /tasks` | `cdc` |
| `timetrack` | Time-track entries per user per date | `GET /timetrack` | `append` |
| `leavetime` | Leave-time entries per user per date | `GET /leavetime` | `append` |

### User & organization objects

| Object | Description | Primary endpoint | Ingestion type |
|---|---|---|---|
| `users` | User accounts | `GET /users` | `cdc` |
| `departments` | Organizational departments | `GET /departments` | `snapshot` |
| `userGroups` | User groups (permission groupings) | `GET /userGroups` | `snapshot` |
| `userRates` | Per-user billing/cost rates | `GET /userRates/{userId}` | `snapshot` |

### Reference / configuration objects

| Object | Description | Primary endpoint | Ingestion type |
|---|---|---|---|
| `typesOfWork` | Work-type categories | `GET /typesOfWork` | `snapshot` |
| `leaveTypes` | Leave-type definitions | `GET /leaveTypes` | `snapshot` |
| `workflowStatuses` | Task workflow status definitions | `GET /workflowStatuses` | `snapshot` |
| `timeZoneGroups` | Time-zone groups | `GET /timeZoneGroups` | `snapshot` |
| `settings` | Single-record system settings | `GET /settings` | `snapshot` |
| `holidays` | Holiday calendar entries | `GET /holidays` | `snapshot` |

### Approval objects

| Object | Description | Primary endpoint | Ingestion type |
|---|---|---|---|
| `approvalStatus` | Per-user-week time approval status | `GET /approvalStatus` | `append` |

### Object hierarchy

- `Customers` → `Projects` → `Tasks`
- `Users` belong to `Departments` and one or more `UserGroups`
- `TimeTrack` entries link `Users` → `Tasks` by `date`
- `LeaveTime` entries link `Users` → `LeaveTypes` by `date`
- `UserRates` is a sub-resource of `Users` (per-user history of rates)

---

## **Object Schema**

### General notes

- Responses are JSON; field names are **camelCase**.
- `id` fields are integers.
- Timestamps in fields named `created` are **Unix epoch milliseconds** (long).
- Date fields use ISO-8601 calendar dates (`YYYY-MM-DD`).
- Boolean fields use JSON `true`/`false`.
- Null values may appear explicitly as `null` or be omitted from the payload — connector treats both as null.
- Some endpoints accept `includeReferenced=<resource[,resource]>` to embed related objects.

### `customers`

**Endpoint**: `GET /api/v1/customers`

**Query parameters**:

| Parameter | Type | Notes |
|---|---|---|
| `offset` | integer | Skip first N records. |
| `limit` | integer | Max records to return. |
| `sort` | string | e.g. `+name`, `-name`, `+created`. URL-encode `+` as `%2B`. |
| `words` | string | Text search in name/description. |
| `ids` | string | Comma-separated customer IDs. |
| `archived` | boolean / `all` | Filter by archived state; `all` returns both. |
| `includeReferenced` | string | E.g. `projects,typesOfWork`. |

**Response example**:
```json
[
  {
    "id": 1,
    "name": "Customer A",
    "archived": false,
    "description": "Description of Customer A",
    "created": 1625501337000,
    "url": "https://<actitime_url>/tasks/tasklist.do?customerId=1",
    "allowedActions": {
      "canModify": true,
      "canDelete": true
    }
  }
]
```

**Schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | Customer name. |
| `description` | string \| null | Customer description. |
| `archived` | boolean | Whether archived. |
| `created` | long (epoch ms) | Creation timestamp. |
| `url` | string | UI deep-link. |
| `allowed_actions_can_modify` | boolean | Flattened from `allowedActions.canModify`. |
| `allowed_actions_can_delete` | boolean | Flattened from `allowedActions.canDelete`. |

### `projects`

**Endpoint**: `GET /api/v1/projects`

**Query parameters** (same shape as `/customers` plus):

| Parameter | Type | Notes |
|---|---|---|
| `customerIds` | string | Comma-separated customer IDs. |

**Response example**:
```json
[
  {
    "id": 10,
    "name": "Project Alpha",
    "description": "Project description",
    "customerId": 1,
    "archived": false,
    "created": 1625501337000,
    "workflowEnabled": true,
    "defaultTypeOfWorkId": 5,
    "url": "https://<actitime_url>/tasks/tasklist.do?projectId=10",
    "allowedActions": {"canModify": true, "canDelete": true}
  }
]
```

**Schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | Project name. |
| `description` | string \| null | |
| `customer_id` | integer | Parent customer (FK). |
| `archived` | boolean | |
| `created` | long (epoch ms) | |
| `workflow_enabled` | boolean | |
| `default_type_of_work_id` | integer \| null | |
| `url` | string | UI deep-link. |
| `allowed_actions_can_modify` | boolean | |
| `allowed_actions_can_delete` | boolean | |

### `tasks`

**Endpoint**: `GET /api/v1/tasks`

**Query parameters**:

| Parameter | Type | Notes |
|---|---|---|
| `offset`, `limit`, `sort`, `words`, `ids`, `archived`, `includeReferenced` | — | As above. |
| `projectIds` | string | Comma-separated project IDs. |
| `customerIds` | string | Comma-separated customer IDs. |
| `typeOfWorkIds` | string | Comma-separated typeOfWork IDs. |
| `workflowStatusIds` | string | Comma-separated workflow status IDs. |

**Response example**:
```json
[
  {
    "id": 100,
    "name": "Task 1",
    "description": "Task description",
    "projectId": 10,
    "customerId": 1,
    "archived": false,
    "created": 1625501337000,
    "typeOfWorkId": 5,
    "workflowStatusId": 2,
    "deadline": "2025-01-31",
    "estimatedTime": 28800,
    "url": "https://<actitime_url>/tasks/taskview.do?taskId=100",
    "allowedActions": {"canModify": true, "canDelete": true, "canComplete": true}
  }
]
```

**Schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | |
| `description` | string \| null | |
| `project_id` | integer | FK to projects. |
| `customer_id` | integer | FK to customers (denormalized). |
| `archived` | boolean | |
| `created` | long (epoch ms) | |
| `type_of_work_id` | integer \| null | FK to typesOfWork. |
| `workflow_status_id` | integer \| null | FK to workflowStatuses. |
| `deadline` | date \| null | YYYY-MM-DD. |
| `estimated_time` | integer \| null | Seconds. |
| `url` | string | |
| `allowed_actions_can_modify` | boolean | |
| `allowed_actions_can_delete` | boolean | |
| `allowed_actions_can_complete` | boolean | |

### `timetrack`

**Endpoint**: `GET /api/v1/timetrack`

> Response is nested by `(userId, date)` with a `records` array. The connector flattens to one row per record.

**Query parameters**:

| Parameter | Type | Required | Notes |
|---|---|---|---|
| `dateFrom` | date | yes | Inclusive start date (`YYYY-MM-DD`). |
| `dateTo` | date | yes | Inclusive end date. |
| `userIds` | string | no | Comma-separated user IDs. |
| `taskIds` | string | no | Comma-separated task IDs. |
| `projectIds` | string | no | Comma-separated project IDs. |
| `customerIds` | string | no | Comma-separated customer IDs. |
| `approved` | boolean | no | If set, returns only approved (`true`) or only unapproved (`false`) entries. |
| `stopAfter` | integer | no | Stop after N records. **This endpoint uses `stopAfter` rather than `limit`** (confirmed in official docs). |
| `includeReferenced` | string | no | E.g. `users,tasks,typesOfWork,approvalStatus`. |

**Response example**:
```json
[
  {
    "userId": 1,
    "date": "2025-01-08",
    "records": [
      {
        "id": 5001,
        "taskId": 100,
        "time": 14400,
        "comment": "Worked on feature X",
        "approved": false,
        "locked": false,
        "typeOfWorkId": 5
      }
    ]
  }
]
```

**Flattened schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key for the time-track record. |
| `user_id` | integer | FK to users (from envelope). |
| `date` | date | YYYY-MM-DD (from envelope). |
| `task_id` | integer | FK to tasks. |
| `time` | integer | Logged time in **seconds**. |
| `comment` | string \| null | |
| `approved` | boolean | |
| `locked` | boolean | True once the week has been approved/locked. |
| `type_of_work_id` | integer \| null | FK to typesOfWork. |

### `leavetime`

**Endpoint**: `GET /api/v1/leavetime`

> Same nested envelope as `/timetrack`; flatten records similarly.

**Query parameters**:

| Parameter | Type | Required | Notes |
|---|---|---|---|
| `dateFrom`, `dateTo` | date | yes | YYYY-MM-DD. |
| `userIds` | string | no | |
| `leaveTypeIds` | string | no | |
| `stopAfter` | integer | no | |
| `includeReferenced` | string | no | E.g. `users,leaveTypes`. |

**Response example**:
```json
[
  {
    "userId": 1,
    "date": "2025-01-08",
    "records": [
      {"id": 2001, "leaveTypeId": 1, "time": 28800, "approved": true}
    ]
  }
]
```

**Flattened schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `user_id` | integer | FK. |
| `date` | date | |
| `leave_type_id` | integer | FK to leaveTypes. |
| `time` | integer | Seconds. |
| `approved` | boolean | |

### `users`

**Endpoint**: `GET /api/v1/users`

**Query parameters**:

| Parameter | Type | Notes |
|---|---|---|
| `offset`, `limit`, `sort`, `ids`, `includeReferenced` | — | |
| `departmentIds` | string | Comma-separated department IDs. |
| `active` | boolean | Filter by active flag. |

**Response example**:
```json
[
  {
    "id": 1,
    "firstName": "John",
    "middleName": null,
    "lastName": "Doe",
    "fullName": "John Doe",
    "username": "johndoe",
    "email": "john.doe@example.com",
    "departmentId": 1,
    "active": true,
    "created": 1625501337000,
    "timeZoneGroupId": 1,
    "hired": "2024-01-15",
    "releaseDate": null,
    "userGroups": [1, 2],
    "userRoles": ["ROLE_ADMIN", "ROLE_MANAGER"],
    "allowedActions": {"canSubmitTimetrack": true}
  }
]
```

**Schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `first_name` | string | |
| `middle_name` | string \| null | |
| `last_name` | string | |
| `full_name` | string | Convenience field returned by the API. |
| `username` | string \| null | Only returned to accounts with "Manage Accounts & Permissions". |
| `email` | string \| null | Same restriction as `username`. |
| `department_id` | integer \| null | FK. |
| `active` | boolean | |
| `created` | long (epoch ms) | |
| `time_zone_group_id` | integer \| null | FK. |
| `hired` | date \| null | Restricted; YYYY-MM-DD. |
| `release_date` | date \| null | Restricted. |
| `user_groups` | array<integer> | List of userGroup IDs. |
| `user_roles` | array<string> | Role identifiers (e.g. `ROLE_ADMIN`). |
| `allowed_actions_can_submit_timetrack` | boolean | From `allowedActions.canSubmitTimetrack`. |

### `departments`

**Endpoint**: `GET /api/v1/departments`

**Response example**:
```json
[
  {"id": 1, "name": "Engineering", "description": "Engineering department",
   "parentId": null, "managerId": 5}
]
```

**Schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | |
| `description` | string \| null | |
| `parent_id` | integer \| null | Self-FK for hierarchy. |
| `manager_id` | integer \| null | FK to users. |

### `userGroups`

**Endpoint**: `GET /api/v1/userGroups`

**Response example**:
```json
[ {"id": 1, "name": "Developers", "description": "Developer team"} ]
```

**Schema**:

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | |
| `description` | string \| null | |

### `userRates`

**Endpoint**: `GET /api/v1/userRates/{userId}` — must be called per user.

> The connector iterates the user list (loaded from `/users`) and calls this endpoint per user. The synthetic composite key `(user_id, date_from)` is constructed by the connector since `userId` is in the path, not the response body.

**Response example**:
```json
[
  {
    "dateFrom": "2025-01-01",
    "regularRate": 75.00,
    "overtimeRate": 112.50,
    "leaveRates": {"1": 75.00, "2": 0.00}
  }
]
```

**Schema**:

| Column | Type | Description |
|---|---|---|
| `user_id` | integer | From path parameter (connector-injected). |
| `date_from` | date | Rate effective date. |
| `regular_rate` | decimal | |
| `overtime_rate` | decimal | |
| `leave_rates` | map<string, decimal> | Map of `leaveTypeId` (string) → rate. |

### `typesOfWork`

**Endpoint**: `GET /api/v1/typesOfWork`

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | |
| `description` | string \| null | |
| `archived` | boolean | |
| `billable` | boolean | |

### `leaveTypes`

**Endpoint**: `GET /api/v1/leaveTypes`

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | |
| `description` | string \| null | |
| `archived` | boolean | |
| `paid_leave` | boolean | |
| `auto_accrual` | boolean | |

### `workflowStatuses`

**Endpoint**: `GET /api/v1/workflowStatuses` (also documented as the "info" endpoint group).

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | |
| `type` | string | One of `open`, `in_progress`, `completed`. |
| `order` | integer | Display order. |

### `timeZoneGroups`

**Endpoint**: `GET /api/v1/timeZoneGroups`

| Column | Type | Description |
|---|---|---|
| `id` | integer | Primary key. |
| `name` | string | |
| `time_zone` | string | IANA TZ id, e.g. `America/New_York`. |

### `settings`

**Endpoint**: `GET /api/v1/settings` — returns a **single JSON object** (not an array).

> The connector synthesizes a constant primary key (e.g. `id = 1`) so this resource can be stored as a 1-row table.

| Column | Type | Description |
|---|---|---|
| `id` | integer | Synthetic constant (= 1). |
| `workday_duration` | integer | Seconds. |
| `week_start_day` | integer | 0=Sunday, 1=Monday, etc. |
| `date_format` | string | Pattern, e.g. `MM/dd/yyyy`. |
| `time_format` | string | Pattern, e.g. `HH:mm`. |
| `currency_code` | string | |
| `decimal_separator` | string | |
| `thousands_separator` | string | |

### `holidays`

**Endpoint**: `GET /api/v1/holidays`

| Query param | Type | Notes |
|---|---|---|
| `dateFrom`, `dateTo` | date | Optional range filter. |

| Column | Type | Description |
|---|---|---|
| `date` | date | Part of composite PK. |
| `name` | string | |
| `time_zone_group_id` | integer \| null | Part of composite PK (null = applies to all groups). |

### `approvalStatus`

**Endpoint**: `GET /api/v1/approvalStatus`

| Query param | Type | Notes |
|---|---|---|
| `dateFrom`, `dateTo` | date | Required range filter (week-start dates). |
| `userIds` | string | Optional comma-separated list. |

**Response example**:
```json
[
  {
    "userId": 1,
    "weekStartDate": "2025-01-06",
    "status": "approved",
    "submittedAt": 1736380800000,
    "approvedAt": 1736467200000,
    "approverId": 5
  }
]
```

| Column | Type | Description |
|---|---|---|
| `user_id` | integer | Part of composite PK. |
| `week_start_date` | date | Part of composite PK. |
| `status` | string | E.g. `submitted`, `approved`, `rejected`, `pending`. |
| `submitted_at` | long (epoch ms) \| null | |
| `approved_at` | long (epoch ms) \| null | |
| `approver_id` | integer \| null | FK to users. |

---

## **Get Object Primary Keys**

There is no metadata endpoint that exposes primary keys. Primary keys are **statically defined** by the connector based on the API resource model.

| Object | Primary Key(s) | Notes |
|---|---|---|
| `customers` | `id` | |
| `projects` | `id` | |
| `tasks` | `id` | |
| `timetrack` | `id` | After flattening the nested `records` array. |
| `leavetime` | `id` | After flattening. |
| `users` | `id` | |
| `departments` | `id` | |
| `userGroups` | `id` | |
| `userRates` | (`user_id`, `date_from`) | Composite; `user_id` injected from path. |
| `typesOfWork` | `id` | |
| `leaveTypes` | `id` | |
| `workflowStatuses` | `id` | |
| `timeZoneGroups` | `id` | |
| `settings` | `id` (synthetic = 1) | Singleton resource. |
| `holidays` | (`date`, `time_zone_group_id`) | Composite; `time_zone_group_id` may be null. |
| `approvalStatus` | (`user_id`, `week_start_date`) | Composite. |

---

## **Object's Ingestion Type**

Framework types in use: `cdc`, `cdc_with_deletes`, `snapshot`, `append`.

| Object | Ingestion type | Cursor field | Rationale |
|---|---|---|---|
| `customers` | `cdc` | `created` (proxy)* | Created/updated/archived but no `modified` field exposed — see Known Quirks. |
| `projects` | `cdc` | `created` (proxy)* | Same as above. |
| `tasks` | `cdc` | `created` (proxy)* | Same as above. |
| `timetrack` | `append` | `date` | Records are written per-date; use `dateFrom`/`dateTo` window. |
| `leavetime` | `append` | `date` | Same windowed access. |
| `users` | `cdc` | `created` (proxy)* | New users are uncommon; full-list refresh remains cheap. |
| `departments` | `snapshot` | — | Small reference table. |
| `userGroups` | `snapshot` | — | Small reference table. |
| `typesOfWork` | `snapshot` | — | |
| `leaveTypes` | `snapshot` | — | |
| `workflowStatuses` | `snapshot` | — | |
| `timeZoneGroups` | `snapshot` | — | |
| `settings` | `snapshot` | — | Single record. |
| `holidays` | `snapshot` | — | Calendar table. |
| `userRates` | `snapshot` | — | Per-user history; refetched in full each run. |
| `approvalStatus` | `append` | `week_start_date` | Approval rows are added per week per user. |

\* **`cdc` cursor caveat**: The actiTIME API does **not** expose a `modifiedAt`/`lastUpdated` field on `customers`/`projects`/`tasks`/`users`, and there is no server-side filter for "modified since". The recommended implementation is:

1. List the full collection on every run (`GET /customers`, `GET /projects`, etc. — these collections are bounded and small).
2. Use `id` as the primary key for upserts so re-syncs are idempotent.
3. Optionally fall back to row-hash comparison in the bronze layer to skip unchanged rows.

If implementing strict CDC behavior is required, treat these tables effectively as **snapshot-with-upsert** (full list + upsert on `id`).

### Incremental strategy for time-windowed objects

- **Cursor field**: `date` (for `timetrack`, `leavetime`); `week_start_date` (for `approvalStatus`).
- **Filter parameters**: `dateFrom`, `dateTo` (`YYYY-MM-DD`).
- **Lookback window**: recommend **7-day lookback** to capture late-arriving time entries and approval-state changes within the open editable window.
- **Deletes**: actiTIME allows deletion of `timetrack` and `leavetime` entries, but the API does **not** provide a deleted-records feed. Use `append` (history is treated as immutable in the warehouse) and document this limitation. If deletes must be tracked, periodically re-sync the full open window (today − N days) and reconcile by `id`.

---

## **Read API for Data Retrieval**

### General API pattern

- **HTTP method**: `GET` for all read operations.
- **Base URL**: `https://<actitime_url>/api/v1`
- **Response media type**: `application/json; charset=UTF-8`
- **Top-level shape**: most endpoints return a JSON array; `/settings` returns a JSON object.

### Pagination

actiTIME exposes **two** pagination styles depending on the endpoint:

| Style | Endpoints | Parameters |
|---|---|---|
| Offset/limit | `customers`, `projects`, `tasks`, `users` (and most list resources) | `offset` (default 0), `limit` (max records per page) |
| `stopAfter` cap | `timetrack`, `leavetime` (and similar time-windowed resources) | `stopAfter` — server returns up to that many records, after which the client must move the date window forward and re-query |

**Implementation guidance**:
- For offset/limit endpoints, loop with `offset += page_size` until the returned array length is `< limit`.
- For `stopAfter` endpoints, request a generous `stopAfter` (e.g. 1000) and slice the date window by chunks (e.g. 7-day chunks) to bound page size deterministically.
- Default `limit` if not specified varies by endpoint; pass an explicit `limit` (e.g. 1000) to avoid relying on defaults.

Example paginated request:

```bash
curl -X GET \
  -u "username:password" \
  -H "Accept: application/json; charset=UTF-8" \
  "https://<actitime_url>/api/v1/tasks?offset=0&limit=1000&sort=%2Bcreated"
```

Time-window paginated request:

```bash
curl -X GET \
  -u "username:password" \
  -H "Accept: application/json; charset=UTF-8" \
  "https://<actitime_url>/api/v1/timetrack?dateFrom=2025-01-01&dateTo=2025-01-07&stopAfter=1000&includeReferenced=approvalStatus"
```

### Sorting

The `sort` parameter accepts a field name prefixed with `+` (asc) or `-` (desc). The `+` must be URL-encoded as `%2B`.

| Value | Meaning |
|---|---|
| `%2Bname` | Ascending by name |
| `-name` | Descending by name |
| `%2Bcreated` | Ascending by creation date |
| `-created` | Descending by creation date |

### Filtering

| Parameter | Applies to | Description |
|---|---|---|
| `words` | `customers`, `projects`, `tasks` | Free-text search in name/description. |
| `ids` | Most list endpoints | Comma-separated IDs. |
| `archived` | `customers`, `projects`, `tasks`, `typesOfWork`, `leaveTypes` | `true`, `false`, or `all`. |
| `customerIds` | `projects`, `tasks`, `timetrack` | |
| `projectIds` | `tasks`, `timetrack` | |
| `userIds` | `users`, `timetrack`, `leavetime`, `approvalStatus` | |
| `departmentIds` | `users` | |
| `dateFrom`, `dateTo` | `timetrack`, `leavetime`, `holidays`, `approvalStatus` | Inclusive date range. |
| `approved` | `timetrack` | Filter by approval state. |
| `active` | `users` | Filter by active flag. |

### `includeReferenced`

`includeReferenced=<csv>` embeds related objects in the response (reduces follow-up calls). Examples:

```bash
# Tasks with their customers, projects, and types-of-work
curl -u "username:password" \
  "https://<actitime_url>/api/v1/tasks?includeReferenced=customers,projects,typesOfWork"

# Time-track with the linked users and tasks
curl -u "username:password" \
  "https://<actitime_url>/api/v1/timetrack?dateFrom=2025-01-01&dateTo=2025-01-07&includeReferenced=users,tasks"
```

The connector should **not** rely on `includeReferenced` for primary ingestion (each related object has its own dedicated stream); use it only for ad-hoc joining or when the API does not expose a separate endpoint.

### Pulling deleted records

actiTIME does **not** expose a delete feed for any resource:
- `customers`, `projects`, `tasks`: deletes are not retrievable. The recommended pattern is full-list refresh + upsert on `id`. Deleted rows simply stop appearing.
- `timetrack`, `leavetime`: deletes within the editable window are not surfaced. Periodically re-sync the last N days to reconcile.

Mark connector ingestion type as `cdc` (full-list upsert) or `append`, not `cdc_with_deletes`.

### Rate limits

| Limit | Value | Notes |
|---|---|---|
| Per-second | **100 requests** per user account | |
| Per-minute | **1000 requests** per user account | |
| Auth-failure ban | 3 failed auths in 10 s | IP banned for 1 minute. |

Headers in responses:

- `X-Ratelimit-Remaining` — requests left in current window.
- `X-Ratelimit-Reset` — seconds until window reset.
- `Retry-After` — seconds to wait (returned on `429`).

The connector should respect `Retry-After` and implement exponential backoff with jitter.

### Error responses

| Code | Meaning |
|---|---|
| 200 | OK. |
| 204 | OK, no content (typically on DELETE). |
| 400 | Bad request (cannot parse args, invalid auth scheme). |
| 401 | Unauthorized (invalid credentials, or account banned). |
| 403 | Forbidden (insufficient permission, or business-rule conflict). |
| 404 | Resource not found. |
| 429 | Rate limit exceeded — honor `Retry-After`. |
| 500 | Server error. |

Error body:

```json
{
  "key": "api.error.customer_exists",
  "message": "Customer with specified name already exists"
}
```

### Comparison: alternate ingestion paths

| Need | Option A (chosen) | Option B (alternate) |
|---|---|---|
| Discover new tasks per project | `GET /tasks?projectIds=X` per project | `GET /tasks` (all) + filter client-side. The connector uses the latter for fewer round trips. |
| Time-track with task context | Pull `/timetrack` then join to `/tasks` client-side | `GET /timetrack?includeReferenced=tasks`. Use only for one-off inspection — full join in bronze is cheaper. |

---

## **Field Type Mapping**

### General mapping

| actiTIME JSON type | Example fields | Logical type | Notes |
|---|---|---|---|
| integer | `id`, `customerId`, `projectId` | `long` (64-bit) | API uses 32-bit IDs but pin to 64-bit for safety. |
| string | `name`, `description`, `username` | `string` | UTF-8. |
| boolean | `archived`, `active`, `approved`, `locked` | `boolean` | |
| long (epoch ms) | `created`, `submittedAt`, `approvedAt` | `timestamp` | Multiply-by-1ms; convert to UTC `TIMESTAMP`. |
| string (ISO date) | `date`, `dateFrom`, `deadline`, `hired`, `releaseDate` | `date` | `YYYY-MM-DD`. |
| array<integer> | `userGroups` | `array<long>` | |
| array<string> | `userRoles` | `array<string>` | |
| object | `allowedActions` | `struct` (flattened) | Recommended: flatten to scalar columns. |
| object (map) | `leaveRates` | `map<string, decimal>` | Keys are leaveTypeId as string. |
| null / absent | optional fields | nullable target type | Treat missing key same as `null`. |

### Specific categories

| Category | Fields | Target |
|---|---|---|
| Identifiers | all `*Id` fields | `long` |
| Names | `name`, `firstName`, `lastName`, `username`, `fullName` | `string` |
| Free text | `description`, `comment` | `string` |
| Created timestamps | `created` | `timestamp` (from epoch ms) |
| Calendar dates | `date`, `dateFrom`, `dateTo`, `deadline`, `hired`, `releaseDate`, `weekStartDate` | `date` |
| Durations (seconds) | `time`, `estimatedTime`, `workdayDuration` | `long` (keep as seconds; derive hours downstream) |
| Rates | `regularRate`, `overtimeRate`, `leaveRates` values | `decimal(18,4)` |
| Booleans | `archived`, `active`, `approved`, `locked`, `billable`, `paidLeave`, `autoAccrual` | `boolean` |
| URLs | `url` | `string` |

### Special behaviors

1. **Epoch-ms timestamps** must be converted to `timestamp` (UTC). Do not write the raw long unless explicitly requested.
2. **Seconds durations** are kept as `long` (seconds); a `time_hours` derivation is a downstream concern.
3. **`allowedActions`** is consistently flattened to `allowed_actions_<sub_field>` scalar booleans for analytic-friendly storage.
4. **Arrays** (`userGroups`, `userRoles`) are stored as Spark `ARRAY` types; do not explode by default.
5. **Map-valued fields** (`leaveRates`) are stored as `MAP<STRING, DECIMAL>`.

---

## **Known Quirks & Edge Cases**

1. **No modified-since cursor on dimension tables** — `customers`, `projects`, `tasks`, `users` only expose `created`. Updates and archiving are not stamped. Treat as full-list refresh + upsert on `id`.
2. **No delete feed anywhere** — the API does not surface deletes for any resource. The connector cannot natively implement `cdc_with_deletes`.
3. **Two pagination styles** — `offset/limit` on most list resources but `stopAfter` on `timetrack` and `leavetime`. Document this in the connector code clearly.
4. **PII fields gated by permission** — `users.username`, `users.email`, `users.hired`, `users.releaseDate` are returned only to accounts with "Manage Accounts & Permissions". Other accounts see those fields as omitted/null. Document the required permission for the service account.
5. **Nested time-track envelope** — `/timetrack` and `/leavetime` return `[{userId, date, records: [...]}]`. The connector flattens this to one record per `records` element.
6. **`/settings` is a singleton** — returns a JSON object, not an array. The connector synthesizes a constant primary key (`id = 1`) to make it tabular.
7. **`/userRates` is per-user only** — there is no list-all endpoint. The connector fans out one request per user from the users list.
8. **`+` in `sort` must be URL-encoded** as `%2B`. Some HTTP libraries handle this automatically; verify in tests.
9. **Self-hosted version requirement** — the v1 REST API ships with actiTIME Online (paid plans) and Self-Hosted **2019.2+**. Older self-hosted installs do not support it.
10. **Auth ban after 3 failures in 10 s** — invalidates the source IP for 1 minute. Treat `401` after a successful first call as a transient state; do not hammer.
11. **Rate-limit per account**, not per token — multiple connectors sharing the same actiTIME user will share the 100 rps / 1000 rpm budget.
12. **REST Hooks exist** — actiTIME supports webhook subscriptions via `POST /api/v1/hooks` (`GET` to list, `DELETE /api/v1/hooks/{id}` to remove). Out of scope for a polling connector but useful for future low-latency variants. Documented here for completeness only.
13. **Archived filter default** — `archived` defaults to `false`. To capture historical entities the connector should request `archived=all`.
14. **Cascade deletes** — deleting a customer or project may cascade to projects/tasks depending on system settings; this is server-side behavior, not visible in the response stream.
15. **Workflow statuses dual endpoint** — official docs refer to this resource both as "Workflow Statuses" and (in the navigation) as "Info Resource". The path is `/workflowStatuses`.

---

## **Sources and References**

### Official documentation (highest confidence)

| URL | Used for |
|---|---|
| https://www.actitime.com/api-documentation | API overview, version, supported HTTP methods, REST Hooks pointer. |
| https://www.actitime.com/api-documentation/api-usage | Auth scheme, rate limits, error codes, pagination concepts. |
| https://www.actitime.com/api-documentation/customers-resource | `customers` resource (verified by web search snippet). |
| https://www.actitime.com/api-documentation/projects-resource | `projects` resource. |
| https://www.actitime.com/api-documentation/tasks-resource | `tasks` resource. |
| https://www.actitime.com/api-documentation/users-resource | `users` resource (incl. permission-gated fields). |
| https://www.actitime.com/api-documentation/time-track-resource | `timetrack` parameters incl. `stopAfter`, `approved`. |
| https://www.actitime.com/api-documentation/leave-time-resource | `leavetime` parameters. |
| https://www.actitime.com/api-documentation/departments-resource | `departments` resource. |
| https://www.actitime.com/api-documentation/types-of-work-resource | `typesOfWork` resource. |
| https://www.actitime.com/api-documentation/info-resource | `workflowStatuses` resource. |
| https://www.actitime.com/api-documentation/rest-hooks | REST Hooks (informational only). |
| https://demo.actitime.com/api/v1/swagger | Swagger UI for the v1 API (redirects to trial page when not authenticated). |

### Third-party OpenAPI

| URL | Used for | Confidence |
|---|---|---|
| https://www.versori.com/open-api-spec-library/actitime-api | Partial OpenAPI 3.0 spec confirming Basic Auth and `/api/v1/customers` shape; spec is only partial (`/customers` paths) so used for cross-reference only. | Medium |
| https://storage.googleapis.com/versori-assets/public-specs/20240223/Actitime-API.yml | Raw spec file (downloaded). | Medium |

### Reference connectors

| Source | Outcome |
|---|---|
| Airbyte connector catalog (https://docs.airbyte.com/integrations/sources/, https://github.com/airbytehq/airbyte) | **No actiTIME connector** exists in Airbyte. |
| Fivetran connector directory (https://www.fivetran.com/connectors, https://fivetran.com/docs/connectors) | **No actiTIME connector** exists in Fivetran. |
| Singer (singer.io) / Meltano hub | No actiTIME tap found. |

### Prior in-repo research

| Path | Used for |
|---|---|
| `sources/actitime/actitime_api_doc.md` (from commit `da00b7b`) | Prior internal documentation; cross-referenced and refined against current official sources (corrected pagination param for time-windowed endpoints, added permission gating on `users` PII, added `allowedActions.canSubmitTimetrack`, added `fullName`/`hired`/`releaseDate`, clarified `stopAfter` vs `limit`). |

### Conflict resolution

- The prior in-repo doc claimed `limit`/`offset` for `/timetrack` and `/leavetime`. The **official docs** show those endpoints use `stopAfter`. **Official wins** — this doc lists `stopAfter` as the pagination knob for the two time-windowed endpoints.
- The prior doc did not list `fullName`, `hired`, `releaseDate`, or `allowedActions.canSubmitTimetrack` on `users`. The official users-resource page lists them — added here, with the permission caveat.
- The prior doc treated `customers`/`projects`/`tasks`/`users` as straightforward `cdc`. Since the API exposes no modified-since field, this doc keeps `cdc` as the framework type but documents the implementation reality (full-list refresh + upsert) under the cursor caveat.

---

## **Research Log**

| Source type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official docs (index) | https://www.actitime.com/api-documentation | 2026-05-11 | Highest | API v1 only, supported methods (GET/PATCH/POST/DELETE), Swagger at `/api/v1/swagger`. |
| Official docs (usage) | https://www.actitime.com/api-documentation/api-usage | 2026-05-11 | Highest | Auth scheme, error codes, rate-limit behavior. |
| Official docs (time-track) | https://www.actitime.com/api-documentation/time-track-resource | 2026-05-11 | Highest | `stopAfter` pagination, `approved` filter, example with `includeReferenced=approvalStatus`. |
| Official docs (leave-time) | https://www.actitime.com/api-documentation/leave-time-resource | 2026-05-11 | Highest | `stopAfter`, `userIds`, `dateFrom/dateTo`, `includeReferenced=leaveTypes`. |
| Official docs (users) | https://www.actitime.com/api-documentation/users-resource | 2026-05-11 | Highest | Permission-gated PII fields (`username`, `email`, `hired`, `releaseDate`), `fullName`, `allowedActions.canSubmitTimetrack`. |
| Official docs (REST Hooks) | https://www.actitime.com/api-documentation/rest-hooks | 2026-05-11 | Highest | Webhook resource endpoints (informational only). |
| Web search | actiTIME API rate limits | 2026-05-11 | High | Confirmed 100 req/s, 1000 req/min, `429` + `Retry-After`. |
| Web search | actiTIME stopAfter vs offset/limit | 2026-05-11 | High | Confirmed dual pagination styles. |
| OpenAPI mirror | https://storage.googleapis.com/versori-assets/public-specs/20240223/Actitime-API.yml | 2026-05-11 | Medium | Basic auth, `/api/v1/customers` shape (partial spec — only customers paths). |
| Airbyte catalog | https://docs.airbyte.com/integrations/sources, https://github.com/airbytehq/airbyte | 2026-05-11 | High | No actiTIME source connector exists. |
| Fivetran catalog | https://www.fivetran.com/connectors, https://fivetran.com/docs/connectors | 2026-05-11 | High | No actiTIME connector exists. |
| In-repo prior research | git commit `da00b7b` → `sources/actitime/actitime_api_doc.md` | 2026-05-11 | High | Prior schema/structure used as starting point; corrections applied above. |

---

## **Deferred Tables**

None. All 16 candidate tables share the same auth model, base URL, and response conventions, and were documented in this single batch.
