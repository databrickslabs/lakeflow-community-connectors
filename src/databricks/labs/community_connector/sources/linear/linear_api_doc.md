# Linear API Documentation

## Authorization

Linear supports two authentication methods. **Personal API Keys** are recommended for connector use.

### Preferred: Personal API Key

Personal API keys are long-lived, user-scoped credentials that do not expire automatically. They inherit the permissions of the user who created them.

**Header format:**
```
Authorization: <API_KEY>
```
Note: No `Bearer` prefix — the raw key is passed directly.

**How to generate:**
1. Log in to Linear and navigate to **Settings** (click your workspace name in the sidebar).
2. Select **Security & access**.
3. Scroll to **Personal API keys** and click **Create key**.
4. Copy the key immediately — Linear shows it only once. Keys begin with `lin_api_`.

**Example request:**
```bash
curl -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: lin_api_XXXXXXXXXXXXX" \
  --data '{"query": "{ viewer { id name email } }"}'
```

### Alternative: OAuth 2.0

For applications acting on behalf of users, OAuth 2.0 is preferred. The connector stores `client_id`, `client_secret`, and `refresh_token` and exchanges the refresh token for a short-lived access token at runtime.

**Header format:**
```
Authorization: Bearer <ACCESS_TOKEN>
```

**OAuth token exchange:**
```bash
curl -X POST https://api.linear.app/oauth/token \
  -d "grant_type=refresh_token" \
  -d "client_id=<CLIENT_ID>" \
  -d "client_secret=<CLIENT_SECRET>" \
  -d "refresh_token=<REFRESH_TOKEN>"
```

**Available scopes:** `read`, `write`, `issues:create`, `admin`, `initiative:read`, `initiative:write`, `customer:read`, `customer:write`

For a read-only connector, the `read` scope is sufficient.

---

## Object List

The Linear API is a single-endpoint GraphQL API. The object list is **static** — all queryable resources are defined in the GraphQL schema. The relevant objects for this connector are:

| Object | Description |
|--------|-------------|
| `issues` | Work items (bugs, features, tasks) assigned to teams |
| `projects` | High-level groupings of issues with goals and timelines |

Additional related objects referenced in schemas:
- `teams` — Organizational units that own issues and projects
- `users` — Workspace members
- `workflowStates` — Workflow statuses (e.g., Todo, In Progress, Done)
- `issueLabels` — Labels applied to issues
- `projectMilestones` — Milestones within a project

All data is accessed via POST requests to: `https://api.linear.app/graphql`

---

## Object Schema

### Issues

The `Issue` object represents a single work item in a Linear team.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String` (UUID) | **Primary key.** Unique identifier for the issue |
| `identifier` | `String` | Human-readable ID (e.g., `ENG-123`) combining team key and number |
| `title` | `String` | Issue title |
| `description` | `String` (Markdown) | Detailed description in Markdown format |
| `priority` | `Int` | Priority level: `0`=None, `1`=Urgent, `2`=High, `3`=Normal, `4`=Low |
| `priorityLabel` | `String` | Human-readable priority label (e.g., `"High"`) |
| `estimate` | `Float` | Story point estimate |
| `dueDate` | `TimelessDate` (YYYY-MM-DD) | Due date (date only, no time component) |
| `url` | `String` | Full URL to the issue in the Linear app |
| `trashed` | `Boolean` | Whether the issue has been trashed (soft deleted) |
| `createdAt` | `DateTime` (ISO 8601) | When the issue was created |
| `updatedAt` | `DateTime` (ISO 8601) | When the issue was last meaningfully updated |
| `archivedAt` | `DateTime` \| `null` | When the issue was archived; `null` if not archived |
| `startedAt` | `DateTime` \| `null` | When work on the issue began |
| `completedAt` | `DateTime` \| `null` | When the issue was completed |
| `canceledAt` | `DateTime` \| `null` | When the issue was canceled |
| `autoArchivedAt` | `DateTime` \| `null` | When the issue was auto-archived |
| `autoClosedAt` | `DateTime` \| `null` | When the issue was auto-closed |
| `state` | `WorkflowState` | Current workflow state object: `{ id, name, type, color }` |
| `assignee` | `User` \| `null` | Assigned user: `{ id, name, email }` |
| `team` | `Team` | Owning team: `{ id, key, name }` |
| `labels` | `IssueLabel[]` (connection) | Applied labels: `[{ id, name, color }]` |
| `project` | `Project` \| `null` | Associated project: `{ id, name, slugId }` |
| `parent` | `Issue` \| `null` | Parent issue for sub-issues: `{ id, identifier, title }` |
| `creator` | `User` \| `null` | User who created the issue: `{ id, name, email }` |
| `cycle` | `Cycle` \| `null` | Associated sprint/cycle: `{ id, name, number }` |

**Example query to retrieve issue schema fields:**
```graphql
query Issue($id: String!) {
  issue(id: $id) {
    id
    identifier
    title
    description
    priority
    priorityLabel
    estimate
    dueDate
    url
    trashed
    createdAt
    updatedAt
    archivedAt
    completedAt
    canceledAt
    state {
      id
      name
      type
      color
    }
    assignee {
      id
      name
      email
    }
    team {
      id
      key
      name
    }
    labels {
      nodes {
        id
        name
        color
      }
    }
    project {
      id
      name
      slugId
    }
    parent {
      id
      identifier
      title
    }
    creator {
      id
      name
      email
    }
  }
}
```

---

### Projects

The `Project` object represents a high-level initiative grouping related issues.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String` (UUID) | **Primary key.** Unique identifier for the project |
| `name` | `String` | Project name |
| `description` | `String` (Markdown) | Description in Markdown format |
| `slugId` | `String` | URL-safe unique slug (e.g., `eng-q1-roadmap`) |
| `state` | `String` | Project lifecycle state: `planned`, `started`, `paused`, `completed`, `cancelled` |
| `progress` | `Float` | Completion percentage (0.0–1.0) based on issue states |
| `scope` | `Float` | Total estimate points in the project |
| `priority` | `Int` | Priority: `0`=No priority, `1`=Urgent, `2`=High, `3`=Medium, `4`=Low |
| `priorityLabel` | `String` | Human-readable priority label |
| `url` | `String` | Full URL to the project |
| `startDate` | `TimelessDate` (YYYY-MM-DD) | Planned start date |
| `targetDate` | `TimelessDate` (YYYY-MM-DD) | Target completion date |
| `startedAt` | `DateTime` \| `null` | When the project actually started |
| `completedAt` | `DateTime` \| `null` | When the project was completed |
| `canceledAt` | `DateTime` \| `null` | When the project was canceled |
| `autoArchivedAt` | `DateTime` \| `null` | When auto-archived |
| `issueCountHistory` | `[Int]` | Weekly history of issue count |
| `scopeHistory` | `[Float]` | Weekly history of scope (estimate points) |
| `createdAt` | `DateTime` (ISO 8601) | When the project was created |
| `updatedAt` | `DateTime` (ISO 8601) | When the project was last meaningfully updated |
| `archivedAt` | `DateTime` \| `null` | When archived; `null` if not archived |
| `lead` | `User` \| `null` | Project lead: `{ id, name, email }` |
| `creator` | `User` | Project creator: `{ id, name, email }` |
| `members` | `User[]` (connection) | Project members: `[{ id, name, email }]` |
| `teams` | `Team[]` (connection) | Associated teams: `[{ id, key, name }]` |
| `projectMilestones` | `ProjectMilestone[]` (connection) | Milestones: `[{ id, name, targetDate, status, progress }]` |
| `issues` | `Issue[]` (connection) | Issues in the project |

**Example query to retrieve project schema fields:**
```graphql
query Project($id: String!) {
  project(id: $id) {
    id
    name
    description
    slugId
    state
    progress
    scope
    priority
    priorityLabel
    url
    startDate
    targetDate
    startedAt
    completedAt
    canceledAt
    createdAt
    updatedAt
    archivedAt
    lead {
      id
      name
      email
    }
    creator {
      id
      name
      email
    }
    members {
      nodes {
        id
        name
        email
      }
    }
    teams {
      nodes {
        id
        key
        name
      }
    }
    projectMilestones {
      nodes {
        id
        name
        targetDate
        status
        progress
      }
    }
  }
}
```

---

## Get Object Primary Keys

Primary keys are **static** and do not require an API call to determine.

| Object | Primary Key | Type | Notes |
|--------|-------------|------|-------|
| `issues` | `id` | `String` (UUID v4) | Also has `identifier` (e.g., `ENG-123`) usable for single-issue lookups |
| `projects` | `id` | `String` (UUID v4) | Also has `slugId` (URL slug) |

Both `id` fields are stable, globally unique UUIDs assigned at creation and never reused.

---

## Object Ingestion Type

| Object | Ingestion Type | Cursor Field | Notes |
|--------|---------------|--------------|-------|
| `issues` | `cdc` | `updatedAt` | Incremental via `updatedAt` filter. Archived issues accessible via `includeArchived: true`; trashed (soft-deleted) issues have `trashed: true`. No dedicated delete event stream — trashed records are retrievable for ~30 days. |
| `projects` | `cdc` | `updatedAt` | Incremental via `updatedAt` filter. Archived projects accessible via `includeArchived: true`. |

**Ingestion type rationale:**
- Both objects expose `updatedAt` and accept a filter on `updatedAt { gte: <cursor> }`, enabling incremental reads with upserts. This qualifies as `cdc`.
- True hard-deletes are not surfaced in the list API. Trashed issues can be detected via `trashed: true` (soft delete). After the 30-day grace period, permanently deleted records disappear from the API. The Airbyte connector treats this as `cdc` (no deletes), which is the pragmatic choice.

---

## Read API for Data Retrieval

### Endpoint

```
POST https://api.linear.app/graphql
Content-Type: application/json
```

All queries are POST requests with a JSON body: `{"query": "...", "variables": {...}}`.

### Pagination

Linear uses **Relay-style cursor pagination**:
- `first: Int` — number of records to fetch (default: 50, max: 250)
- `after: String` — opaque cursor for forward pagination (from `pageInfo.endCursor`)
- `last: Int` — number of records for backward pagination
- `before: String` — cursor for backward pagination

**Response always includes:**
```graphql
pageInfo {
  hasNextPage
  hasPreviousPage
  startCursor
  endCursor
}
```

Continue fetching while `pageInfo.hasNextPage == true`, passing `pageInfo.endCursor` as `after` in the next request.

### Rate Limits

| Authentication | Request Limit | Per | Period |
|----------------|--------------|-----|--------|
| Personal API Key | 2,500 requests | User | 1 hour |
| OAuth App | 5,000 requests | User / App User | 1 hour |
| Unauthenticated | 600 requests | IP Address | 1 hour |

| Authentication | Complexity Limit | Per | Period |
|----------------|-----------------|-----|--------|
| Personal API Key | 3,000,000 points | User | 1 hour |
| OAuth App | 2,000,000 points | User / App User | 1 hour |

**Single query hard cap:** 10,000 complexity points per query.

Rate limit status is returned in response headers:
- `X-RateLimit-Requests-Remaining` — remaining requests in the current window
- `X-RateLimit-Complexity-Remaining` — remaining complexity points
- `X-RateLimit-Requests-Reset` — reset time (UTC epoch ms)
- `X-Complexity` — complexity of the current query

**Recommended page size:** `first: 50` to keep individual query complexity low. Reduce to 25 if complexity errors occur on queries with many nested fields.

### Reading Issues (Full Refresh)

```graphql
query Issues($first: Int, $after: String, $includeArchived: Boolean) {
  issues(
    first: $first
    after: $after
    includeArchived: $includeArchived
    orderBy: updatedAt
  ) {
    nodes {
      id
      identifier
      title
      description
      priority
      priorityLabel
      estimate
      dueDate
      url
      trashed
      createdAt
      updatedAt
      archivedAt
      completedAt
      canceledAt
      state {
        id
        name
        type
        color
      }
      assignee {
        id
        name
        email
      }
      team {
        id
        key
        name
      }
      labels {
        nodes {
          id
          name
          color
        }
      }
      project {
        id
        name
        slugId
      }
      creator {
        id
        name
        email
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

**Variables:**
```json
{
  "first": 50,
  "after": null,
  "includeArchived": true
}
```

**Curl example:**
```bash
curl -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: lin_api_XXXXX" \
  --data '{
    "query": "query Issues($first: Int, $after: String, $includeArchived: Boolean) { issues(first: $first, after: $after, includeArchived: $includeArchived, orderBy: updatedAt) { nodes { id identifier title priority updatedAt createdAt archivedAt trashed state { id name type } assignee { id name } team { id key name } } pageInfo { hasNextPage endCursor } } }",
    "variables": {"first": 50, "after": null, "includeArchived": true}
  }'
```

### Reading Issues (Incremental)

Use the `updatedAt` filter with a cursor value from the previous sync. Pass `includeArchived: true` to capture issues that were archived after the cursor.

```graphql
query IssuesIncremental(
  $first: Int
  $after: String
  $updatedAfter: DateTime!
) {
  issues(
    first: $first
    after: $after
    includeArchived: true
    orderBy: updatedAt
    filter: {
      updatedAt: { gte: $updatedAfter }
    }
  ) {
    nodes {
      id
      identifier
      title
      description
      priority
      priorityLabel
      estimate
      dueDate
      url
      trashed
      createdAt
      updatedAt
      archivedAt
      completedAt
      canceledAt
      state {
        id
        name
        type
        color
      }
      assignee {
        id
        name
        email
      }
      team {
        id
        key
        name
      }
      labels {
        nodes {
          id
          name
          color
        }
      }
      project {
        id
        name
        slugId
      }
      creator {
        id
        name
        email
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

**Variables:**
```json
{
  "first": 50,
  "after": null,
  "updatedAfter": "2024-01-01T00:00:00.000Z"
}
```

**Incremental strategy:**
1. Track `max(updatedAt)` from all records returned in the previous sync as `cursor`.
2. On subsequent syncs, filter: `updatedAt { gte: cursor }`.
3. Apply a **lookback window** of 5–10 minutes (subtract from cursor before querying) to handle clock skew and late writes.
4. Always use `orderBy: updatedAt` with `includeArchived: true`.

### Reading Projects (Full Refresh)

```graphql
query Projects($first: Int, $after: String, $includeArchived: Boolean) {
  projects(
    first: $first
    after: $after
    includeArchived: $includeArchived
    orderBy: updatedAt
  ) {
    nodes {
      id
      name
      description
      slugId
      state
      progress
      scope
      priority
      priorityLabel
      url
      startDate
      targetDate
      startedAt
      completedAt
      canceledAt
      createdAt
      updatedAt
      archivedAt
      lead {
        id
        name
        email
      }
      creator {
        id
        name
        email
      }
      members {
        nodes {
          id
          name
          email
        }
      }
      teams {
        nodes {
          id
          key
          name
        }
      }
      projectMilestones {
        nodes {
          id
          name
          targetDate
          status
          progress
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

**Variables:**
```json
{
  "first": 50,
  "after": null,
  "includeArchived": true
}
```

### Reading Projects (Incremental)

```graphql
query ProjectsIncremental(
  $first: Int
  $after: String
  $updatedAfter: DateTime!
) {
  projects(
    first: $first
    after: $after
    includeArchived: true
    orderBy: updatedAt
    filter: {
      updatedAt: { gte: $updatedAfter }
    }
  ) {
    nodes {
      id
      name
      description
      slugId
      state
      progress
      scope
      priority
      priorityLabel
      url
      startDate
      targetDate
      startedAt
      completedAt
      canceledAt
      createdAt
      updatedAt
      archivedAt
      lead {
        id
        name
        email
      }
      creator {
        id
        name
        email
      }
      members {
        nodes {
          id
          name
          email
        }
      }
      teams {
        nodes {
          id
          key
          name
        }
      }
      projectMilestones {
        nodes {
          id
          name
          targetDate
          status
          progress
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

**Variables:**
```json
{
  "first": 50,
  "after": null,
  "updatedAfter": "2024-01-01T00:00:00.000Z"
}
```

### Deleted / Trashed Records

Linear does not expose a dedicated delete event stream. Deletion behavior:

- **Trashed issues:** `trashed: true` is set on soft-deleted issues. The issue remains accessible via the API (with `includeArchived: true`) for approximately 30 days before permanent deletion.
- **Permanently deleted issues:** Disappear from the API after the 30-day grace period. They cannot be retrieved.
- **Archived issues/projects:** Soft-archived records have a non-null `archivedAt` timestamp and remain accessible with `includeArchived: true`.

**Practical approach:**
- Include `trashed` and `archivedAt` fields in all queries.
- Use `includeArchived: true` on all queries to capture soft-archived records.
- Records that disappear entirely (after permanent deletion) will not be detectable via the list API.

---

## Field Type Mapping

| Linear Type | Python / Spark Type | Notes |
|-------------|---------------------|-------|
| `String` | `str` | UUIDs, identifiers, text fields |
| `Int` | `int` | Priority values, counts |
| `Float` | `float` | Progress (0.0–1.0), estimate points, scope |
| `Boolean` | `bool` | `trashed`, `includeArchived` flags |
| `DateTime` | `datetime` (ISO 8601 UTC) | Format: `2024-01-15T12:34:56.789Z`. Always UTC. Map to `TimestampType` in Spark. |
| `TimelessDate` | `str` (YYYY-MM-DD) | Date without time zone; used for `dueDate`, `startDate`, `targetDate`. Map to `DateType` in Spark. |
| `JSON` / `JSONObject` | `str` (serialized JSON) | Internal fields like `bodyData`, `descriptionData` — treat as opaque strings |
| Nested object (`state`, `assignee`, etc.) | `StructType` or flatten | Expand to named columns or serialize to JSON string |
| Connection (`labels.nodes`, `members.nodes`) | `ArrayType(StructType)` | Repeated nested objects; expand to array of structs |
| Enum (`state` on Project) | `str` | `planned`, `started`, `paused`, `completed`, `cancelled` |
| Priority enum (`priority`) | `int` | 0=None/No Priority, 1=Urgent, 2=High, 3=Normal/Medium, 4=Low |
| `WorkflowState.type` | `str` | `backlog`, `unstarted`, `started`, `completed`, `canceled` |
| `ProjectMilestone.status` | `str` | `done`, `next`, `overdue`, `unstarted` |

**Key behaviors:**
- `DateTime` fields are always UTC. Use ISO 8601 strings with `Z` suffix for filter values.
- `priority = 0` means no priority assigned (not the highest urgency).
- `archivedAt`, `completedAt`, `canceledAt`, `startedAt` are nullable — will be `null` for non-matching states.
- `description` and `descriptionData` are both present; prefer `description` (Markdown string) for human-readable text.
- `identifier` (e.g., `ENG-123`) is human-readable but the stable key for `issue()` lookups is `id` (UUID).

---

## Sources and References

### Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://linear.app/developers/graphql | 2026-05-31 | High | Endpoint, auth methods, example queries for issues/teams |
| Official Docs | https://linear.app/developers/pagination | 2026-05-31 | High | Relay cursor pagination, default/max page sizes, `orderBy: updatedAt` |
| Official Docs | https://linear.app/developers/rate-limiting | 2026-05-31 | High | Exact rate limits (2,500/5,000 req/hr), complexity limits (3M/2M), max query complexity (10,000) |
| Airbyte OSS Connector | https://docs.airbyte.com/integrations/sources/linear | 2026-05-31 | High | Supported streams (issues, projects incremental), `updatedAt` cursor strategy, `start_date` config, OAuth scope (`read`), confirmed rate limits |
| Community Reference (linctl) | https://github.com/dorkitude/linctl/blob/master/master_api_ref.md | 2026-05-31 | Medium | Issue/project GraphQL query patterns, `IssueFilter`, `ProjectFilter` filter types, `IssueOrderBy` vs `PaginationOrderBy` disambiguation |
| Community Reference (linctl) | https://github.com/charlietran/linctl/blob/6f91456acc87fe45a4af25793f72b255111ae117/master_api_ref.md | 2026-05-31 | Medium | Detailed field lists for issues and projects, filter examples with priority encoding |
| Linear SDK (Official GitHub) | https://github.com/linear/linear/blob/master/packages/sdk/src/_generated_documents.graphql | 2026-05-31 | High | Official generated GraphQL documents confirming project fields (`slugId`, `scopeHistory`, `issueCountHistory`, `priority`, `priorityLabel`) |
| Linear Knowledge Base (One.ai) | https://www.withone.ai/knowledge/linear/* | 2026-05-31 | Medium | `includeArchived` param confirmation, trashed issues behavior, `ProjectMilestone.status` enum values |
| eliteai.tools | https://eliteai.tools/agent-skills/linear-install-auth | 2026-05-31 | Medium | OAuth scopes list, API key format (`lin_api_` prefix), error codes |

### Existing Connector Implementations

- **Airbyte Linear connector** (v0.2.2, 2026-05-18): [https://docs.airbyte.com/integrations/sources/linear](https://docs.airbyte.com/integrations/sources/linear) — High confidence reference for supported streams and incremental strategy.

### Known Quirks

1. **Auth header format differs between key types:** Personal API keys use `Authorization: <key>` (no `Bearer`); OAuth tokens use `Authorization: Bearer <token>`. Mixing these up returns a 401.

2. **`IssueOrderBy` vs `PaginationOrderBy`:** The `issues` query accepts `IssueOrderBy`, not the global `PaginationOrderBy` enum. Some community references use `PaginationOrderBy` for issues — this works at runtime (the enum values overlap) but `IssueOrderBy` is the typed correct enum per the official schema.

3. **`updatedAt` filter for incremental:** The `filter` argument on `issues` and `projects` accepts `updatedAt: { gte: "..." }` for incremental reads. Apply a short lookback window (~5 minutes) to avoid missing records due to clock skew.

4. **Deleted issues not in list API:** Permanently deleted issues (after 30-day trash grace period) are not retrievable. Connectors cannot detect hard deletes from the list API alone.

5. **`members` on Projects is a connection:** `project.members` returns a paginated `UserConnection`. For projects with many members, this nested connection may require separate pagination. For typical projects, `first: 100` on the nested connection is sufficient.

6. **Complexity budget:** Deeply nested queries (e.g., issues + labels + project + milestones in one request) can exceed the 10,000 per-query complexity cap. If complexity errors occur, reduce `first` or remove nested sub-selections and fetch them separately.

7. **`start_date` default:** Airbyte defaults the start date to 2 years before first sync when unset. The same default is recommended for this connector.
