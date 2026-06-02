# Linear API Documentation

## Authorization

Linear supports two authentication methods. **Personal API Keys** are preferred for server-side connectors (data pipelines); **OAuth 2.0** is recommended when acting on behalf of end-users.

### Preferred Method: Personal API Key

Pass the raw API key in the `Authorization` header — no `Bearer` prefix:

```http
POST https://api.linear.app/graphql
Content-Type: application/json
Authorization: <YOUR_API_KEY>

{ "query": "{ viewer { id name email } }" }
```

**How to create a Personal API Key:**
1. Log in to Linear → click workspace name in the sidebar.
2. Navigate to **Settings → Security & access**.
3. Under **Personal API keys**, click **Create key**, give it a label, and click **Create**.
4. Copy immediately — Linear shows the key only once.

The API key inherits the permissions of the user who created it; the connector can only read data that the key owner can see in the Linear application.

### Alternative: OAuth 2.0 (Authorization Code Flow)

For multi-tenant or user-delegated access, use OAuth 2.0. The connector stores `client_id`, `client_secret`, and `refresh_token`; it exchanges the refresh token for a short-lived access token at runtime.

**Authorization endpoint:**
```
GET https://linear.app/oauth/authorize
  ?client_id=<CLIENT_ID>
  &redirect_uri=<REDIRECT_URI>
  &response_type=code
  &scope=read
  &state=<RANDOM_STATE>
```

**Token endpoint (exchange code for tokens):**
```http
POST https://api.linear.app/oauth/token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code
&code=<AUTH_CODE>
&client_id=<CLIENT_ID>
&client_secret=<CLIENT_SECRET>
&redirect_uri=<REDIRECT_URI>
```

**Token refresh:**
```http
POST https://api.linear.app/oauth/token
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token
&refresh_token=<REFRESH_TOKEN>
&client_id=<CLIENT_ID>
&client_secret=<CLIENT_SECRET>
```

Access tokens expire in ~24 hours. Refresh tokens are issued by default for OAuth apps created after October 1, 2025.

**Required scopes for read-only connector:** `read` (default, always present)  
For customer-related data: also include `customer:read`

**Bearer authentication for OAuth tokens:**
```http
Authorization: Bearer <ACCESS_TOKEN>
```

**Known Quirks:**
- Personal API keys do NOT use `Bearer` prefix; OAuth access tokens DO.
- API key rate limits: 2,500 requests/hour (some sources cite 5,000 — see Rate Limits section).
- OAuth rate limits: 5,000 requests/hour per user/app-user.

---

## Object List

Linear's object graph is accessible through introspection. The objects below are accessible via the single GraphQL endpoint. The list of objects is **static** (defined by the schema), though new fields can be added over time.

**Full object list (all Airbyte-supported streams):**

| Object | Incremental? | Description |
|--------|-------------|-------------|
| `issues` | Yes (`updatedAt`) | Issues and tasks in every team, including archived |
| `projects` | Yes (`updatedAt`) | Projects across all teams |
| `teams` | Yes (`updatedAt`) | Teams in the workspace |
| `users` | Yes (`updatedAt`) | Users in the workspace |
| `comments` | Yes (`updatedAt`) | Comments posted on issues |
| `cycles` | Yes (`updatedAt`) | Sprint cycles per team |
| `issue_labels` | Yes (`updatedAt`) | Labels applicable to issues |
| `workflow_states` | Yes (`updatedAt`) | Workflow states (statuses) per team |
| `attachments` | Yes (`updatedAt`) | File/link attachments on issues |
| `project_milestones` | Yes (`updatedAt`) | Milestones inside projects |
| `customer_needs` | Yes (`updatedAt`) | Customer needs linked to issues |
| `customers` | Yes (`updatedAt`) | Customer records (if using Linear's customer feature) |
| `issue_relations` | No (full refresh) | Relationships between issues (blocks, duplicates) |
| `project_statuses` | No (full refresh) | Status definitions for projects |
| `customer_statuses` | No (full refresh) | Status definitions for customers |
| `customer_tiers` | No (full refresh) | Tier definitions for customers |

**Scope for this connector:** `issues` and `projects` (plus supporting entities `teams`, `users`, `workflow_states`, `issue_labels` as needed for foreign key resolution).

**Retrieving the object list via introspection:**
```graphql
{
  __schema {
    queryType {
      fields {
        name
        description
      }
    }
  }
}
```

---

## Object Schema

All schemas are discoverable via GraphQL introspection. Below are the full field-level schemas for the in-scope objects, sourced from the official Linear SDK's generated GraphQL fragments (highest confidence).

### Issue Schema

Query: `issues` (workspace-level) or `team.issues` (team-scoped)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | UUID | No | Unique identifier (primary key) |
| `identifier` | String | No | Human-readable ID, e.g. `ENG-123` |
| `title` | String | No | Issue title |
| `description` | String | Yes | Body in Markdown format |
| `number` | Int | No | Issue number (unique within team) |
| `url` | String | No | Full Linear URL to the issue |
| `priority` | Int | No | 0=No priority, 1=Urgent, 2=High, 3=Medium, 4=Low |
| `priorityLabel` | String | No | Human label for priority (e.g., "High") |
| `estimate` | Float | Yes | Story point estimate |
| `dueDate` | Date | Yes | Due date (YYYY-MM-DD) |
| `boardOrder` | Float | No | Order in board column |
| `sortOrder` | Float | No | Global sort order |
| `prioritySortOrder` | Float | No | Sort order by priority |
| `subIssueSortOrder` | Float | Yes | Sort order in sub-issue list |
| `branchName` | String | Yes | Suggested Git branch name |
| `labelIds` | [UUID] | No | Array of label IDs |
| `previousIdentifiers` | [String] | No | Past identifiers if moved between teams |
| `integrationSourceType` | String | Yes | Integration type that created this issue |
| `customerTicketCount` | Int | No | Number of support ticket attachments |
| `reactionData` | JSON | No | Emoji reactions summary |
| `trashed` | Boolean | No | Whether in trash |
| `inheritsSharedAccess` | Boolean | No | Whether shared access is inherited from parent |
| `slaType` | String | Yes | SLA type: "calendarDays" or "businessDays" |
| `createdAt` | DateTime | No | ISO 8601 creation timestamp |
| `updatedAt` | DateTime | No | ISO 8601 last meaningful update timestamp |
| `archivedAt` | DateTime | Yes | When archived (null if not archived) |
| `completedAt` | DateTime | Yes | When moved to completed state |
| `canceledAt` | DateTime | Yes | When moved to canceled state |
| `startedAt` | DateTime | Yes | When moved to started state |
| `autoArchivedAt` | DateTime | Yes | When auto-archived by pruning |
| `autoClosedAt` | DateTime | Yes | When auto-closed by pruning |
| `addedToCycleAt` | DateTime | Yes | When added to a cycle |
| `addedToProjectAt` | DateTime | Yes | When added to a project |
| `addedToTeamAt` | DateTime | Yes | When added to a team |
| `startedTriageAt` | DateTime | Yes | When entered triage |
| `triagedAt` | DateTime | Yes | When left triage |
| `snoozedUntilAt` | DateTime | Yes | Snoozed until this time in triage |
| `slaStartedAt` | DateTime | Yes | When SLA began |
| `slaBreachesAt` | DateTime | Yes | When SLA will breach |
| `slaHighRiskAt` | DateTime | Yes | When SLA enters high-risk state |
| `slaMediumRiskAt` | DateTime | Yes | When SLA enters medium-risk state |
| `team.id` | UUID | No | FK: Team ID |
| `state.id` | UUID | No | FK: WorkflowState ID |
| `assignee.id` | UUID | Yes | FK: User ID of assignee |
| `creator.id` | UUID | Yes | FK: User ID of creator |
| `parent.id` | UUID | Yes | FK: Parent issue ID (for sub-issues) |
| `project.id` | UUID | Yes | FK: Project ID |
| `projectMilestone.id` | UUID | Yes | FK: ProjectMilestone ID |
| `cycle.id` | UUID | Yes | FK: Cycle ID |
| `delegate.id` | UUID | Yes | FK: Agent user delegated to this issue |
| `snoozedBy.id` | UUID | Yes | FK: User who snoozed the issue |
| `sourceComment.id` | UUID | Yes | FK: Comment from which issue was created |
| `recurringIssueTemplate.id` | UUID | Yes | FK: Recurring template |
| `botActor` | Object | Yes | Bot that created the issue (if any) |
| `externalUserCreator.id` | UUID | Yes | FK: External user who created |
| `asksExternalUserRequester.id` | UUID | Yes | FK: External user who requested creation |
| `asksRequester.id` | UUID | Yes | FK: Internal user who requested creation |
| `lastAppliedTemplate.id` | UUID | Yes | FK: Last template applied |
| `favorite.id` | UUID | Yes | FK: User's favorite record for this issue |
| `reactions` | [Reaction] | No | Reaction objects |
| `sharedAccess` | Object | Yes | Shared access metadata |
| `syncedWith` | [Object] | No | External service sync info |

### Project Schema

Query: `projects` (workspace-level)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `id` | UUID | No | Unique identifier (primary key) |
| `name` | String | No | Project name |
| `description` | String | Yes | Short text description |
| `content` | String | Yes | Full content in Markdown |
| `slugId` | String | No | URL-friendly slug |
| `url` | String | No | Full Linear URL |
| `color` | String | Yes | Hex color code |
| `icon` | String | Yes | Icon identifier |
| `priority` | Int | No | 0=No priority, 1=Urgent, 2=High, 3=Medium, 4=Low |
| `priorityLabel` | String | No | Human label for priority |
| `prioritySortOrder` | Float | No | Sort order by priority |
| `sortOrder` | Float | No | Global sort order |
| `progress` | Float | No | Overall progress 0.0–1.0 (completed + 0.25×in-progress / total estimate) |
| `scope` | Float | No | Total estimate points |
| `health` | String | Yes | Health status of the project |
| `state` | String | No | **Deprecated** — type of state (use `status` instead) |
| `targetDate` | Date | Yes | Estimated completion date (YYYY-MM-DD) |
| `targetDateResolution` | String | Yes | Resolution of targetDate ("day", "week", "month", "quarter") |
| `startDate` | Date | Yes | Estimated start date (YYYY-MM-DD) |
| `startDateResolution` | String | Yes | Resolution of startDate |
| `labelIds` | [UUID] | No | Array of label IDs |
| `trashed` | Boolean | No | Whether in trash |
| `slackIssueComments` | Boolean | No | Slack notifications for new comments |
| `slackNewIssue` | Boolean | No | Slack notifications for new issues |
| `slackIssueStatuses` | Boolean | No | Slack notifications for status updates |
| `updateReminderFrequency` | String | Yes | Update reminder frequency |
| `updateReminderFrequencyInWeeks` | Int | Yes | N-weekly reminder frequency |
| `updateRemindersDay` | String | Yes | Day of week for reminders |
| `updateRemindersHour` | Int | Yes | Hour for reminders |
| `frequencyResolution` | String | Yes | Resolution of reminder frequency |
| `completedScopeHistory` | [Float] | No | Weekly history of completed estimate points |
| `completedIssueCountHistory` | [Int] | No | Weekly history of completed issue counts |
| `inProgressScopeHistory` | [Float] | No | Weekly history of in-progress estimate points |
| `scopeHistory` | [Float] | No | Weekly history of total estimate points |
| `issueCountHistory` | [Int] | No | Weekly history of total issue counts |
| `createdAt` | DateTime | No | ISO 8601 creation timestamp |
| `updatedAt` | DateTime | No | ISO 8601 last meaningful update timestamp |
| `archivedAt` | DateTime | Yes | When archived |
| `autoArchivedAt` | DateTime | Yes | When auto-archived |
| `canceledAt` | DateTime | Yes | When moved to canceled state |
| `completedAt` | DateTime | Yes | When moved to completed state |
| `startedAt` | DateTime | Yes | When moved to started state |
| `healthUpdatedAt` | DateTime | Yes | When project health was last updated |
| `projectUpdateRemindersPausedUntilAt` | DateTime | Yes | Until when reminders are paused |
| `status.id` | UUID | Yes | FK: ProjectStatus ID |
| `lead.id` | UUID | Yes | FK: User ID of project lead |
| `creator.id` | UUID | Yes | FK: User ID of creator |
| `convertedFromIssue.id` | UUID | Yes | FK: Issue that this project was created from |
| `lastUpdate.id` | UUID | Yes | FK: Last project update |
| `lastAppliedTemplate.id` | UUID | Yes | FK: Last template applied |
| `favorite.id` | UUID | Yes | FK: User's favorite record |
| `integrationsSettings.id` | UUID | Yes | FK: Integration settings |
| `documentContent` | Object | Yes | Rich document content |
| `syncedWith` | [Object] | No | External service sync info |

---

## Get Object Primary Keys

Both objects have a single UUID primary key field.

| Object | Primary Key | Type | Notes |
|--------|------------|------|-------|
| `issues` | `id` | UUID | Also has `identifier` (e.g. `ENG-123`) as a human-readable alternate key; `id` is the stable UUID |
| `projects` | `id` | UUID | Also has `slugId` as URL-friendly alternate |

Both `id` fields are server-generated, immutable, and globally unique within a Linear workspace.

---

## Object Ingestion Type

| Object | Ingestion Type | Cursor Field | Notes |
|--------|---------------|-------------|-------|
| `issues` | `cdc` | `updatedAt` | Linear does not expose soft-delete markers; deleted issues disappear from results silently. Archived issues are excluded by default but available via `includeArchived: true`. |
| `projects` | `cdc` | `updatedAt` | Same delete behavior as issues. Archived projects excluded by default; include with `includeArchived: true`. |

**Delete handling:** Linear does not provide a deleted-records feed or soft-delete flag for either `issues` or `projects`. Deleted records simply vanish from the API. This means true `cdc_with_deletes` is not achievable without a full snapshot comparison. For the connector, use `cdc` (upsert-only incremental) and optionally perform periodic full-snapshot reconciliation to detect hard deletes.

**Archived records:** Archived issues and projects are hidden from paginated results by default. To include them in a full sync, pass `includeArchived: true` in the filter/query.

---

## Read API for Data Retrieval

### Endpoint

```
POST https://api.linear.app/graphql
Content-Type: application/json
Authorization: <API_KEY>   # or: Bearer <OAUTH_TOKEN>
```

All reads use HTTP POST with a JSON body containing `query` and optional `variables`.

### Pagination

Linear implements [Relay-style cursor-based pagination](https://linear.app/developers/pagination) on all list queries.

- **Arguments:** `first` (page size, default 50, max 250), `after` (cursor string), `orderBy` (enum)
- **Response:** `nodes` array + `pageInfo { hasNextPage endCursor }`
- **Default order:** `createdAt` ascending
- **Incremental order:** use `orderBy: updatedAt`

Pagination loop:
1. Query with `first: 50, orderBy: updatedAt`
2. If `pageInfo.hasNextPage == true`, re-query with `after: pageInfo.endCursor`
3. Repeat until `hasNextPage == false`

### Incremental Sync

Filter by `updatedAt` using a date comparator, and order by `updatedAt` to ensure stable cursor advancement:

```graphql
query IssuesIncremental(
  $after: String
  $cursor: DateTime
) {
  issues(
    first: 50
    after: $after
    orderBy: updatedAt
    filter: { updatedAt: { gte: $cursor } }
    includeArchived: true
  ) {
    nodes {
      id
      identifier
      title
      description
      number
      url
      priority
      priorityLabel
      estimate
      dueDate
      labelIds
      trashed
      branchName
      createdAt
      updatedAt
      archivedAt
      completedAt
      canceledAt
      startedAt
      team { id }
      state { id }
      assignee { id }
      creator { id }
      parent { id }
      project { id }
      projectMilestone { id }
      cycle { id }
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
  "cursor": "2024-01-01T00:00:00.000Z",
  "after": null
}
```

### Full Issues Query (first load or full refresh)

```graphql
query Issues($after: String) {
  issues(
    first: 50
    after: $after
    orderBy: updatedAt
    includeArchived: true
  ) {
    nodes {
      id
      identifier
      title
      description
      number
      url
      priority
      priorityLabel
      estimate
      dueDate
      labelIds
      trashed
      boardOrder
      sortOrder
      prioritySortOrder
      branchName
      reactionData
      customerTicketCount
      previousIdentifiers
      integrationSourceType
      createdAt
      updatedAt
      archivedAt
      completedAt
      canceledAt
      startedAt
      autoArchivedAt
      autoClosedAt
      addedToCycleAt
      addedToProjectAt
      addedToTeamAt
      startedTriageAt
      triagedAt
      snoozedUntilAt
      slaStartedAt
      slaBreachesAt
      slaHighRiskAt
      slaMediumRiskAt
      slaType
      team { id }
      state { id }
      assignee { id }
      creator { id }
      parent { id }
      project { id }
      projectMilestone { id }
      cycle { id }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

### Projects Query (incremental)

```graphql
query ProjectsIncremental(
  $after: String
  $cursor: DateTime
) {
  projects(
    first: 50
    after: $after
    orderBy: updatedAt
    filter: { updatedAt: { gte: $cursor } }
    includeArchived: true
  ) {
    nodes {
      id
      name
      description
      content
      slugId
      url
      color
      icon
      priority
      priorityLabel
      progress
      scope
      health
      state
      targetDate
      targetDateResolution
      startDate
      startDateResolution
      labelIds
      trashed
      slackIssueComments
      slackNewIssue
      slackIssueStatuses
      completedScopeHistory
      completedIssueCountHistory
      inProgressScopeHistory
      scopeHistory
      issueCountHistory
      createdAt
      updatedAt
      archivedAt
      autoArchivedAt
      canceledAt
      completedAt
      startedAt
      healthUpdatedAt
      projectUpdateRemindersPausedUntilAt
      status { id }
      lead { id }
      creator { id }
      convertedFromIssue { id }
      lastUpdate { id }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

### Get Single Issue by ID

```graphql
query Issue($id: String!) {
  issue(id: $id) {
    id
    identifier
    title
    description
    priority
    state { id name type color }
    assignee { id name email }
    team { id key name }
    labels { nodes { id name color } }
    comments { nodes { id body createdAt user { id name } } }
    attachments { nodes { id title url createdAt } }
    createdAt
    updatedAt
    dueDate
    estimate
  }
}
```

**Variables:** `{ "id": "BLA-123" }` (accepts either UUID or human identifier like `ENG-123`)

### Filtering

Issues and projects support a rich filter syntax. Useful filters for connector:

```graphql
# Filter by updatedAt for incremental ingestion
filter: { updatedAt: { gte: "2024-01-01T00:00:00Z" } }

# Filter by team
filter: { team: { id: { eq: "TEAM_UUID" } } }

# Filter by priority
filter: { priority: { lte: 2, neq: 0 } }

# Combine filters (AND by default)
filter: {
  updatedAt: { gte: "2024-01-01T00:00:00Z" }
  team: { id: { eq: "TEAM_UUID" } }
}
```

### Rate Limits

Linear uses a **leaky bucket** algorithm with two independent budgets per hour:

| Budget | API Key | OAuth App |
|--------|---------|-----------|
| Request count | 2,500 req/hr (Airbyte docs) / 5,000 req/hr (official ref) | 5,000 req/hr |
| Query complexity | 3,000,000 pts/hr | 2,000,000 pts/hr |
| Max per-request complexity | 10,000 pts | 10,000 pts |
| Burst protection | 30 req/min per endpoint | 30 req/min per endpoint |

**Complexity calculation:**
- 0.1 pts per scalar field
- 1 pt per object
- Connection complexity = children's complexity × `first` argument value

**Key response headers to monitor:**
```
X-RateLimit-Requests-Limit
X-RateLimit-Requests-Remaining
X-RateLimit-Requests-Reset
X-RateLimit-Complexity-Limit
X-RateLimit-Complexity-Remaining
X-RateLimit-Complexity-Reset
```

**Rate limit error:** HTTP 400 with `errors[].extensions.type == "RATELIMITED"`. Implement exponential back-off on this error.

**Best practices for connectors:**
- Use `first: 25` (not 50 or 250) for complex nested queries to stay under 10,000 complexity.
- Use `first: 50` for flat (scalar-heavy) queries.
- Order by `updatedAt` and filter by `updatedAt.gte` to minimize data fetched per incremental sync.
- Pass `includeArchived: true` once during initial full load; subsequent incremental runs may omit it depending on business need.

### Deleted Records

Linear provides no soft-delete flag or deleted-records endpoint. Options:
1. **Ignore deletes:** Only upsert records. Simplest approach.
2. **Periodic full reconciliation:** Run a full `issues` query periodically; compare IDs to detect disappearances.
3. **Webhooks (out of scope for batch connector):** Register webhook events for `Issue.remove` and `Project.remove` events to capture deletes in near-real-time.

For this connector, **option 1 (ignore deletes)** is the recommended starting point.

---

## Field Type Mapping

| GraphQL Type | Python/Spark Type | Notes |
|-------------|------------------|-------|
| `ID` / `UUID` | `StringType` | UUID v4, e.g. `9cfb482a-81e3-4154-b5b9-2c805e70a02d` |
| `String` | `StringType` | Plain text or Markdown content |
| `Int` | `IntegerType` | 32-bit signed integer |
| `Float` | `DoubleType` | Used for sort orders, progress (0.0–1.0), estimate points |
| `Boolean` | `BooleanType` | |
| `DateTime` | `TimestampType` | ISO 8601 with milliseconds, e.g. `2024-01-15T10:30:00.000Z` |
| `Date` | `DateType` | Calendar date, e.g. `2024-03-31` (no time component) |
| `JSON` | `StringType` (serialized JSON) | Used for `reactionData`; store as JSON string |
| `[UUID]` (`labelIds`, etc.) | `ArrayType(StringType)` | Array of UUID strings |
| `[Float]` (history arrays) | `ArrayType(DoubleType)` | Weekly history arrays |
| `[Int]` (history arrays) | `ArrayType(IntegerType)` | Weekly history arrays |
| Enum (e.g. `priority`) | `IntegerType` | Stored as integer per Linear spec |
| Enum string (e.g. `state`, `health`) | `StringType` | e.g. "backlog", "started", "completed" |
| Nested object reference (`team.id`) | `StringType` | Flatten to `team_id` as UUID string |

**Special field behaviors:**
- `priority`: integer enum (0=None, 1=Urgent, 2=High, 3=Medium, 4=Low). `0` means no priority set — treat as nullable.
- `progress`: float 0.0 to 1.0 representing percentage complete.
- `updatedAt`: always set; used as the incremental cursor. Never null for existing records.
- `archivedAt`, `completedAt`, etc.: null means not yet in that state.
- `identifier` (Issue): composite `{TEAM_KEY}-{number}` string, e.g. `ENG-42`. Stable unless issue is moved between teams.
- `labelIds`: array of UUIDs — denormalized copy of labels for quick filtering without joining `labels` connection.
- `state` (Project): **deprecated** string field — use `status.id` (FK to `ProjectStatus`) instead.
- `reactionData`: JSON blob — emoji grouped by type, e.g. `{"👍": 3, "🚀": 1}`.
- `completedScopeHistory` / `scopeHistory` / etc.: ordered arrays, one entry per week since project inception.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://linear.app/developers/graphql | 2026-06-02 | High | Endpoint, auth methods (API key vs OAuth), basic queries |
| Official Docs | https://linear.app/developers/pagination | 2026-06-02 | High | Relay-style pagination, `first`/`after`, `pageInfo`, `orderBy: updatedAt` |
| Official Docs | https://linear.app/developers/rate-limiting | 2026-06-02 | High | Leaky bucket algorithm, complexity budget, rate-limit headers |
| Official Docs | https://linear.app/developers/filtering | 2026-06-02 | High | Filter syntax, comparators (`gte`, `lte`, `eq`), logical operators, `includeArchived` |
| Official Docs | https://linear.app/developers/oauth-2-0-authentication | 2026-06-02 | High | OAuth endpoints, scopes, refresh token behavior (Oct 2025 cutoff) |
| Linear SDK (official) | https://github.com/linear/linear/blob/master/packages/sdk/src/_generated_documents.graphql | 2026-06-02 | High | Complete `Issue` and `Project` GraphQL fragments with all scalar fields and FK references |
| Airbyte Official Docs | https://docs.airbyte.com/integrations/sources/linear | 2026-06-02 | High | Stream list, incremental vs full-refresh classification, `updatedAt` cursor, rate limit numbers |
| Linear Official Airbyte Source | https://github.com/linear/linear-airbyte-source | 2026-06-02 | High | Confirmed stream coverage: Issue, Project, and supporting entities |
| Community API Reference | https://github.com/dorkitude/linctl/blob/master/master_api_ref.md | 2026-06-02 | Medium | Rate limit numbers (5,000 req/hr for API key), pagination pattern, project query example |
| Airbyte PR | https://github.com/airbytehq/airbyte/pull/76429 | 2026-06-02 | Medium | Incremental sync via `filter.updatedAt.gte` + `orderBy: updatedAt`, 12 incremental streams |
| DX Data Cloud Schema | https://docs.getdx.com/schema/linear_projects/ | 2026-06-02 | Medium | Confirmed Project field names and types in a production data product |
| Community Skill | https://github.com/OpenHands/extensions/blob/main/skills/linear/SKILL.md | 2026-06-02 | Low | Confirmed curl-based authentication and query patterns |

**Conflict resolution:**
- Request rate limit: Airbyte docs say 2,500 req/hr for API keys; community reference and official rate-limit page mention 5,000 req/hr. Airbyte's recent (2026) docs are treated as authoritative for safety — the connector targets 2,500 req/hr to stay conservative. Note this with `TBD:` if precision matters.
- `state` field on Project: official SDK marks it `[DEPRECATED]`. Use `status.id` (FK to `ProjectStatus` type) for the current state. Connector should map both for backward compatibility.
