# **GitHub API Documentation**

## **Authorization**

- **Chosen method**: Personal Access Token (PAT) for the GitHub REST API v3.
- **Base URL**: `https://api.github.com`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <personal_access_token>`
  - Recommended scopes for read-only analytics on repositories and issues:
    - Public repositories only: `public_repo`
    - Public + private repositories: `repo`
- **Other supported methods (not used by this connector)**:
  - OAuth apps and GitHub Apps are also supported by GitHub, but the connector will **not** perform interactive OAuth flows. Tokens must be provisioned out-of-band and stored in configuration.

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/<owner>/<repo>/issues?per_page=2"
```

Notes:
- `Accept: application/vnd.github+json` is recommended by GitHub for the REST API.
- Rate limiting for authenticated requests is typically **5,000 requests per hour per user/app**; unauthenticated requests are limited to **60/hour** (see official docs).


## **Object List**

For connector purposes, we treat specific GitHub REST resources as **objects/tables**.  
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Endpoint | Ingestion Type (planned) |
|------------|-------------|------------------|--------------------------|
| `issues` | Issues (and PRs presented as issues) for a given repository | `GET /repos/{owner}/{repo}/issues` | `cdc` (upserts based on `updated_at`) |
| `assignees` | Users who can be assigned to issues in a repository | `GET /repos/{owner}/{repo}/assignees` | `snapshot` (TBD cursor) |
| `branches` | Branches within a repository | `GET /repos/{owner}/{repo}/branches` | `snapshot` |
| `collaborators` | Users with explicit access to a repository | `GET /repos/{owner}/{repo}/collaborators` | `snapshot` |
| `organizations` | Organization metadata for the authenticated context | `GET /user/orgs` + `GET /orgs/{org}` | `snapshot` |
| `teams` | Teams visible to the authenticated user | `GET /user/teams` + `GET /orgs/{org}/teams/{team_slug}` | `snapshot` |
| `users` | Authenticated user metadata | `GET /user` | `snapshot` |
| `comments` | Issue comments for a repository | `GET /repos/{owner}/{repo}/issues/comments` | `cdc` (TBD details) |
| `commits` | Commits for a repository | `GET /repos/{owner}/{repo}/commits` | `append` (TBD details) |
| `repositories` | Metadata about repositories for a given user or organization | `GET /users/{username}/repos` | `snapshot` |
| `pull_requests` | Pull requests for a given repository | `GET /repos/{owner}/{repo}/pulls` | `cdc` (planned; TBD details) |
| `reviews` | Pull request reviews for a repository | `GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews` | `append` (TBD details) |

**Connector scope for initial implementation**:
- Step 1 focuses on the `issues` object and documents it in detail.
- Other objects are listed for future extension and are marked `TBD` where details are not yet specified.

High-level notes on additional objects (for future Step 1 expansions):
- **Assignees**: For analytics, the connector will typically derive assignee details from the `issues` and `pull_requests` tables; the dedicated `assignees` endpoint is mainly for validation and discovery of valid assignees per repo.
- **Branches**: Treated as a relatively small, slowly changing snapshot list per repository; no incremental cursor is planned initially.
- **Collaborators / Users / Organizations / Teams**: These objects describe the people and org structure; initial treatment is snapshot-style metadata keyed by stable IDs or logins, refreshed periodically.
- **Comments**: Candidates for `cdc` ingestion keyed by comment `id` with `updated_at` cursor, using issue-level comment endpoints for more targeted reads if needed.
- **Commits**: Modeled as an append-only stream per repository, where new commits are appended and existing commits are immutable.
- **Pull requests / Reviews**: PRs will mirror the `issues` incremental pattern using `updated_at` as a cursor; `reviews` act as an append-only timeline attached to each PR.


## **Object Schema**

### General notes

- GitHub provides a static JSON schema for resources via its REST API documentation and OpenAPI spec.
- For the connector, we define **tabular schemas** per object, derived from the JSON representation.
- Nested JSON objects (e.g., `user`, `labels`) are modeled as **nested structures/arrays** rather than being fully flattened.

### `issues` object (primary table)

**Source endpoint**:  
`GET /repos/{owner}/{repo}/issues`

**Key behavior**:
- Returns both issues and pull requests (PRs appear as issues with an additional `pull_request` object).
- Supports filtering and incremental reads via `state`, `since`, and pagination parameters.

**High-level schema (connector view)**:

Top-level fields (all from the GitHub REST API unless marked as “connector-derived”):

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer (64-bit) | Unique identifier for the issue. Globally unique across GitHub. |
| `node_id` | string | GraphQL node ID for the issue. |
| `number` | integer | Issue number within the repository (unique per repo). |
| `repository_owner` | string (connector-derived) | Owner part of the `{owner}` path parameter for the repository whose issues are being read. |
| `repository_name` | string (connector-derived) | Repo name part of the `{repo}` path parameter for the repository whose issues are being read. |
| `title` | string | Issue title. |
| `body` | string or null | Markdown-formatted issue body. |
| `state` | string (enum) | Issue state: typically `open` or `closed`. |
| `locked` | boolean | Whether the issue is locked. |
| `comments` | integer | Number of comments on the issue. |
| `created_at` | string (ISO 8601 datetime) | Issue creation time. |
| `updated_at` | string (ISO 8601 datetime) | Last time the issue was updated. Used as incremental cursor. |
| `closed_at` | string (ISO 8601 datetime) or null | Time when the issue was closed (if closed). |
| `author_association` | string | How the author is associated with the repo (e.g., `OWNER`, `CONTRIBUTOR`). |
| `url` | string | API URL for the issue resource. |
| `html_url` | string | Web URL for the issue in the GitHub UI. |
| `labels_url` | string | Template URL for labels for this issue. |
| `comments_url` | string | API URL for comments for this issue. |
| `events_url` | string | API URL for events of this issue. |
| `timeline_url` | string | API URL for the timeline of this issue. |
| `state_reason` | string or null | Optional reason for the current state (e.g., `completed`, `not_planned`). |
| `user` | struct | User who created the issue (see nested schema). |
| `assignee` | struct or null | Single assignee, if any (see nested schema). |
| `assignees` | array\<struct\> | All assignees for the issue. |
| `labels` | array\<struct\> | Labels applied to the issue. |
| `milestone` | struct or null | Milestone associated with the issue, if any. |
| `pull_request` | struct or null | Present when the issue is a pull request. |
| `reactions` | struct or null | Aggregated reaction counts if requested with appropriate preview/accept header. |

**Nested `user` struct** (creator/owner of the issue):

| Field | Type | Description |
|-------|------|-------------|
| `login` | string | Username of the user. |
| `id` | integer (64-bit) | Unique user ID. |
| `node_id` | string | GraphQL node ID for the user. |
| `type` | string | Type of user (typically `User`, `Bot`, or `Organization`). |
| `site_admin` | boolean | Whether the user is a GitHub site admin. |

**Nested `label` struct** (elements of `labels` array):

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer (64-bit) | Unique label ID. |
| `node_id` | string | GraphQL node ID for the label. |
| `name` | string | Label name. |
| `color` | string | Hex color code for the label (without `#`). |
| `description` | string or null | Label description. |
| `default` | boolean | Whether this is a default label. |

**Nested `assignee` struct** (similar shape to `user`):

| Field | Type | Description |
|-------|------|-------------|
| `login` | string | Username of the assignee. |
| `id` | integer (64-bit) | Unique assignee ID. |
| `node_id` | string | GraphQL node ID. |
| `type` | string | Type of user. |
| `site_admin` | boolean | Whether the assignee is a site admin. |

**Nested `milestone` struct** (connector schema):

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer (64-bit) | Unique milestone ID. |
| `number` | integer | Milestone number. |
| `title` | string | Milestone title. |
| `description` | string or null | Milestone description. |
| `state` | string | Milestone state (`open` or `closed`). |
| `created_at` | string (ISO 8601 datetime) | Milestone creation time. |
| `updated_at` | string (ISO 8601 datetime) | Last update time. |
| `due_on` | string (ISO 8601 datetime) or null | Due date, if set. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/<owner>/<repo>/issues?state=all&per_page=2"
```

**Example response (truncated)**:

```json
[
  {
    "id": 1,
    "node_id": "MDU6SXNzdWUx",
    "number": 1347,
    "title": "Found a bug",
    "user": {
      "login": "octocat",
      "id": 1,
      "node_id": "MDQ6VXNlcjE=",
      "type": "User",
      "site_admin": false
    },
    "state": "open",
    "locked": false,
    "comments": 0,
    "created_at": "2011-04-22T13:33:48Z",
    "updated_at": "2011-04-22T13:33:48Z",
    "closed_at": null,
    "body": "I'm having a problem with this.",
    "labels": [],
    "assignee": null,
    "assignees": [],
    "milestone": null,
    "url": "https://api.github.com/repos/octocat/Hello-World/issues/1347",
    "html_url": "https://github.com/octocat/Hello-World/issues/1347",
    "comments_url": "https://api.github.com/repos/octocat/Hello-World/issues/1347/comments",
    "events_url": "https://api.github.com/repos/octocat/Hello-World/issues/1347/events",
    "timeline_url": "https://api.github.com/repos/octocat/Hello-World/issues/1347/timeline"
  }
]
```

> The columns listed above define the **complete connector schema** for the `issues` table.  
> If additional GitHub issue fields are needed in the future, they must be added as new columns here so the documentation continues to reflect the full table schema.


### `repositories` object (snapshot metadata)

**Source endpoints** (listing repositories):
- `GET /users/{username}/repos` — list repositories for a given user.
- `GET /orgs/{org}/repos` — list repositories for a given organization.

**High-level schema (connector view)** — full connector schema for repositories:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer (64-bit) | Unique repository ID. |
| `node_id` | string | GraphQL node ID for the repository. |
| `repository_owner` | string (connector-derived) | Owner login derived from the `owner.login` field in the repository object. |
| `repository_name` | string (connector-derived) | Repository `name` field from the repository object. |
| `name` | string | Short repository name. |
| `full_name` | string | `<owner>/<repo>` full name. |
| `owner` | struct | Owner object (same shape as nested `user` schema, including URLs and metadata). |
| `private` | boolean | Whether the repository is private. |
| `html_url` | string | Web URL for the repository. |
| `description` | string or null | Repository description. |
| `fork` | boolean | Whether this repository is a fork. |
| `url` | string | API URL for the repository. |
| `archive_url` | string | URL template for archive downloads. |
| `assignees_url` | string | URL template for assignees. |
| `blobs_url` | string | URL template for git blobs. |
| `branches_url` | string | URL template for branches. |
| `collaborators_url` | string | URL template for collaborators. |
| `comments_url` | string | URL template for comments. |
| `commits_url` | string | URL template for commits. |
| `compare_url` | string | URL template for commit comparisons. |
| `contents_url` | string | URL template for repository contents. |
| `contributors_url` | string | URL for contributors. |
| `deployments_url` | string | URL for deployments. |
| `downloads_url` | string | URL for downloads. |
| `events_url` | string | URL for repository events. |
| `forks_url` | string | URL for forks. |
| `git_commits_url` | string | URL template for git commits. |
| `git_refs_url` | string | URL template for git refs. |
| `git_tags_url` | string | URL template for git tags. |
| `git_url` | string | Git protocol URL for the repository. |
| `issue_comment_url` | string | URL template for issue comments. |
| `issue_events_url` | string | URL template for issue events. |
| `issues_url` | string | URL template for issues. |
| `keys_url` | string | URL template for deploy keys. |
| `labels_url` | string | URL template for labels. |
| `languages_url` | string | URL for languages used in the repository. |
| `merges_url` | string | URL for merges. |
| `milestones_url` | string | URL template for milestones. |
| `notifications_url` | string | URL template for notifications. |
| `pulls_url` | string | URL template for pull requests. |
| `releases_url` | string | URL template for releases. |
| `ssh_url` | string | SSH URL for cloning. |
| `stargazers_url` | string | URL for stargazers. |
| `statuses_url` | string | URL template for commit statuses. |
| `subscribers_url` | string | URL for subscribers. |
| `subscription_url` | string | URL for the current user’s subscription. |
| `tags_url` | string | URL for tags. |
| `teams_url` | string | URL for teams with access. |
| `trees_url` | string | URL template for git trees. |
| `clone_url` | string | Clone URL (HTTPS). |
| `mirror_url` | string or null | Mirror URL, if this is a mirrored repository. |
| `hooks_url` | string | URL for webhooks. |
| `svn_url` | string | Subversion-compatible URL. |
| `homepage` | string or null | Repository homepage URL. |
| `language` | string or null | Primary language, if detected. |
| `forks_count` | integer | Number of forks. |
| `stargazers_count` | integer | Number of stargazers. |
| `watchers_count` | integer | Number of watchers. |
| `size` | integer | Repository size in kilobytes. |
| `default_branch` | string | Name of the default branch. |
| `open_issues_count` | integer | Number of open issues. |
| `is_template` | boolean | Whether this repository is marked as a template. |
| `topics` | array\<string\> | List of repository topics. |
| `has_issues` | boolean | Whether issues are enabled. |
| `has_projects` | boolean | Whether projects are enabled. |
| `has_wiki` | boolean | Whether the wiki is enabled. |
| `has_pages` | boolean | Whether GitHub Pages is enabled. |
| `has_downloads` | boolean | Whether downloads are enabled. |
| `archived` | boolean | Whether the repository is archived. |
| `disabled` | boolean | Whether the repository is disabled. |
| `visibility` | string | Visibility level, e.g., `public` or `private`. |
| `pushed_at` | string (ISO 8601 datetime) | Last push time. |
| `created_at` | string (ISO 8601 datetime) | Repository creation time. |
| `updated_at` | string (ISO 8601 datetime) | Last update time. |
| `permissions` | struct | Permissions for the authenticated user (fields like `admin`, `push`, `pull`). |
| `allow_rebase_merge` | boolean | Whether rebase merges are allowed. |
| `template_repository` | struct or null | Template repository information, if this repo was created from a template. |
| `temp_clone_token` | string or null | Temporary clone token, if present. |
| `allow_squash_merge` | boolean | Whether squash merges are allowed. |
| `allow_merge_commit` | boolean | Whether merge commits are allowed. |
| `subscribers_count` | integer | Number of subscribers. |
| `network_count` | integer | Number of forks in the network. |
| `license` | struct or null | License information (fields like `key`, `name`, `spdx_id`, `url`, `node_id`). |

> The columns listed above define the **full connector schema** for the `repositories` table.  
> If the connector is extended to expose additional GitHub repository fields, they must be added here so this documentation remains the single source of truth.


### `pull_requests` object

**Source endpoint**:  
`GET /repos/{owner}/{repo}/pulls`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer (64-bit) | Unique pull request ID. |
| `node_id` | string | GraphQL node ID for the pull request. |
| `number` | integer | Pull request number within the repository. |
| `repository_owner` | string (connector-derived) | Owner part of `{owner}` path parameter. |
| `repository_name` | string (connector-derived) | Repo name part of `{repo}` path parameter. |
| `state` | string (enum) | PR state, e.g., `open`, `closed`. |
| `title` | string | PR title. |
| `body` | string or null | PR description/body. |
| `draft` | boolean | Whether the PR is a draft. |
| `created_at` | string (ISO 8601 datetime) | Creation timestamp. |
| `updated_at` | string (ISO 8601 datetime) | Last update timestamp (candidate cursor). |
| `closed_at` | string (ISO 8601 datetime) or null | Time when PR was closed. |
| `merged_at` | string (ISO 8601 datetime) or null | Time when PR was merged, if applicable. |
| `merge_commit_sha` | string or null | SHA of the merge commit, if merged. |
| `user` | struct | Author of the PR (nested `user` schema). |
| `base` | struct | Base branch info (e.g., `ref`, `sha`, `repo`). |
| `head` | struct | Head branch info (e.g., `ref`, `sha`, `repo`). |
| `html_url` | string | Web URL for the PR. |
| `url` | string | API URL for the PR. |

> For ingestion, `updated_at` will mirror the `issues` incremental pattern, with `id` as a stable primary key.


### `comments` object (issue comments)

**Source endpoint**:  
`GET /repos/{owner}/{repo}/issues/comments`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer (64-bit) | Unique comment ID. |
| `node_id` | string | GraphQL node ID for the comment. |
| `repository_owner` | string (connector-derived) | Owner part of `{owner}` path parameter. |
| `repository_name` | string (connector-derived) | Repo name part of `{repo}` path parameter. |
| `issue_url` | string | API URL of the associated issue. |
| `html_url` | string | Web URL of the comment. |
| `body` | string | Comment body (Markdown). |
| `user` | struct | Comment author (nested `user` schema). |
| `created_at` | string (ISO 8601 datetime) | Creation timestamp. |
| `updated_at` | string (ISO 8601 datetime) | Last update timestamp (candidate cursor). |
| `author_association` | string | Relationship of the author to the repo (e.g., `OWNER`, `CONTRIBUTOR`). |

> Comments are mostly append-only but can be edited; treating them as `cdc` with `updated_at` as cursor allows upserts for edits.


### `commits` object

**Source endpoint**:  
`GET /repos/{owner}/{repo}/commits`

**High-level schema (connector view)** (flattened representation of the commit):

| Column Name | Type | Description |
|------------|------|-------------|
| `sha` | string | Commit SHA (immutable identifier). |
| `node_id` | string | GraphQL node ID for the commit. |
| `repository_owner` | string (connector-derived) | Owner part of `{owner}` path parameter. |
| `repository_name` | string (connector-derived) | Repo name part of `{repo}` path parameter. |
| `commit_message` | string | Commit message (`commit.message`). |
| `commit_author_name` | string | Author name from `commit.author.name`. |
| `commit_author_email` | string | Author email from `commit.author.email`. |
| `commit_author_date` | string (ISO 8601 datetime) | `commit.author.date`. |
| `commit_committer_name` | string | Committer name from `commit.committer.name`. |
| `commit_committer_email` | string | Committer email from `commit.committer.email`. |
| `commit_committer_date` | string (ISO 8601 datetime) | `commit.committer.date`. |
| `html_url` | string | Web URL for the commit. |
| `url` | string | API URL for the commit. |
| `author` | struct or null | GitHub user associated as author, if any. |
| `committer` | struct or null | GitHub user associated as committer, if any. |

> Commits are treated as an **append-only** stream keyed by `sha`; historical commits are immutable.


### `users`, `organizations`, and `teams` objects

These objects share many fields with the nested `user` struct, with additional metadata:

- **Users** (`GET /user`) — high-level schema (connector view):
  - `id` (64-bit integer): User ID.  
  - `login` (string): Username.  
  - `node_id` (string): GraphQL node ID.  
  - `type` (string): `User` or `Bot`.  
  - `site_admin` (boolean).  
  - `name`, `company`, `blog`, `location`, `email` (string or null).  
  - `created_at`, `updated_at` (ISO 8601 datetime).

- **Organizations** (`GET /user/orgs` + `GET /orgs/{org}`) — high-level schema (connector view):
  - `id` (64-bit integer).  
  - `login` (string): Organization login.  
  - `node_id` (string).  
  - `name` (string or null).  
  - `description` (string or null).  
  - `blog`, `location` (string or null).  
  - `created_at`, `updated_at` (ISO 8601 datetime).

- **Teams** (`GET /user/teams` + `GET /orgs/{org}/teams/{team_slug}`) — high-level schema (connector view):
  - `id` (64-bit integer).  
  - `node_id` (string).  
  - `organization_login` (string, connector-derived) — `{org}` path parameter.  
  - `name` (string).  
  - `slug` (string).  
  - `description` (string or null).  
  - `privacy` (string).  
  - `permission` (string).  

In all three cases, the connector will treat the schemas as **snapshot metadata tables** keyed by `id`.


### `assignees` and `collaborators` objects

Both `assignees` and `collaborators` are user-like resources associated with a given repository:

- **Assignees** (`GET /repos/{owner}/{repo}/assignees`):
  - `repository_owner`, `repository_name` (connector-derived).  
  - `login`, `id`, `node_id`, `type`, `site_admin` (same as `user` struct).  

- **Collaborators** (`GET /repos/{owner}/{repo}/collaborators`):
  - Same core user fields as above, plus a `permissions` struct with booleans like `admin`, `push`, `pull`.  

For analytics, these tables are primarily used to understand who *can* be assigned or who *has* access, and are modeled as snapshot tables keyed by a composite of `(repository_owner, repository_name, login)` or the underlying numeric `id` plus repo identifiers.


### `reviews` object (pull request reviews)

**Source endpoint**:  
`GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews`

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer (64-bit) | Unique review ID. |
| `node_id` | string | GraphQL node ID for the review. |
| `repository_owner` | string (connector-derived) | Owner part of `{owner}` path parameter. |
| `repository_name` | string (connector-derived) | Repo name part of `{repo}` path parameter. |
| `pull_number` | integer | Pull request number (`{pull_number}`). |
| `state` | string | Review state, e.g., `APPROVED`, `CHANGES_REQUESTED`, `COMMENTED`. |
| `body` | string or null | Review body text. |
| `user` | struct | Reviewer (nested `user` schema). |
| `commit_id` | string | Commit SHA that the review pertains to. |
| `submitted_at` | string (ISO 8601 datetime) or null | Time when the review was submitted. |
| `html_url` | string | Web URL for the review. |

> Reviews form an **append-only timeline** attached to each pull request; updates are rare and can be modeled as upserts by `id` if needed.


## **Get Object Primary Key**

There is no dedicated metadata endpoint to get the primary key for the `issues` object.  
Instead, the primary key is defined **statically** based on the resource schema.

- **Primary key for `issues`**: `id`  
  - Type: 64-bit integer  
  - Property: Unique across all issues on GitHub (not just within a repository).

The connector will:
- Read the `id` field from each issue record returned by `GET /repos/{owner}/{repo}/issues`.
- Use it as the immutable primary key for upserts when ingestion type is `cdc`.

Example showing primary key in response:

```json
{
  "id": 1,
  "number": 1347,
  "title": "Found a bug",
  "state": "open",
  "created_at": "2011-04-22T13:33:48Z",
  "updated_at": "2011-04-22T13:33:48Z"
}
```

Primary keys for additional objects (defined statically based on GitHub’s resource representations):

- **`repositories`**: `id` (64-bit integer; unique per repository).  
- **`pull_requests`**: `id` (64-bit integer; stable across the lifetime of the PR).  
- **`comments`**: `id` (64-bit integer; unique per comment).  
- **`commits`**: `sha` (string; immutable commit identifier).  
- **`users`**: `id` (64-bit integer; unique per user).  
- **`organizations`**: `id` (64-bit integer; unique per org).  
- **`teams`**: `id` (64-bit integer; unique per team).  
- **`assignees` / `collaborators`**: composite of `repository_owner`, `repository_name`, and `id` (or `login`) to reflect per-repo relationships.  
- **`reviews`**: `id` (64-bit integer; unique per review).

Where composites are mentioned, the connector may choose to materialize them as separate columns but treat the combination as the logical primary key in downstream systems.


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Planned ingestion types for GitHub objects:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `issues` | `cdc` | Issues have a stable primary key `id` and an `updated_at` field that can be used as a cursor for incremental syncs. Issues are not truly deleted in most workflows; updates and closures can be modeled as upserts. |
| `repositories` | `snapshot` | Repository metadata is relatively small and can be re-snapshotted. No dedicated incremental cursor is required for initial implementation. |
| `pull_requests` | `cdc` (TBD) | PRs expose similar fields to issues, including `updated_at`. Detailed ingestion strategy will mirror `issues` but is out of scope for this initial doc. |
| `assignees` | `snapshot` (TBD cursor) | List of assignable users per repository changes infrequently; full refresh per repo is acceptable initially. |
| `branches` | `snapshot` | Branch lists are relatively small and stable; full snapshot per repo is simpler than incremental tracking of renames/deletes. |
| `collaborators` | `snapshot` | Collaborator membership and permissions are best treated as point-in-time metadata refreshed periodically. |
| `organizations` | `snapshot` | Org metadata changes rarely; full refresh is sufficient. |
| `teams` | `snapshot` | Team definitions and metadata change infrequently; snapshot fits. |
| `users` | `snapshot` | User profiles change slowly; snapshot with occasional refresh is adequate for analytics. |
| `comments` | `cdc` (TBD details) | Comments are mostly append-only but can be edited; using `updated_at` as a cursor enables upserts while capturing new comments and edits. |
| `commits` | `append` (TBD details) | Commits are immutable; new commits can be modeled as append-only events identified by `sha`. |
| `reviews` | `append` (TBD details) | Reviews form an append-only review timeline per PR; they are typically not updated after submission. |

For `issues`:
- **Primary key**: `id`
- **Cursor field**: `updated_at`
- **Sort order**: Ascending by `updated_at`, then `id` as tie-breaker (implementation detail).
- **Deletes**: GitHub issues are typically closed but not hard-deleted; the connector treats changes in `state` and `closed_at` as updates (upserts), not deletes.


## **Read API for Data Retrieval**

### Primary read endpoint for `issues`

- **HTTP method**: `GET`
- **Endpoint**: `/repos/{owner}/{repo}/issues`
- **Base URL**: `https://api.github.com`

**Path parameters**:
- `owner` (string, required): Repository owner (user or organization).
- `repo` (string, required): Repository name.

**Key query parameters** (relevant for ingestion):

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `state` | string | no | `open` | Filter by state: `open`, `closed`, or `all`. For full history, use `all`. |
| `since` | string (ISO 8601 datetime) | no | none | Return issues updated at or after this time. Used for incremental reads. |
| `per_page` | integer | no | 30 | Number of results per page (max 100). |
| `page` | integer | no | 1 | Page number of the results to fetch. |
| `sort` | string | no | `created` | The field to sort by (e.g., `created`, `updated`, `comments`). |
| `direction` | string | no | `desc` | Sort direction: `asc` or `desc`. |
| `labels` | string | no | none | Comma-separated list of label names. If provided, only issues with all these labels are returned. |

**Pagination strategy**:
- GitHub uses traditional page-based pagination with `per_page` and `page` parameters.
- Pagination links are provided via the `Link` response header (RFC 5988 style) with `rel="next"`, `rel="prev"`, `rel="first"`, and `rel="last"`.
- The connector should:
  - Request with `per_page=100` (maximum) for efficiency.
  - Follow the `Link` header `rel="next"` URL until it is no longer present.

Example incremental read using `since`:

```bash
SINCE_TS="2024-01-01T00:00:00Z"
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/<owner>/<repo>/issues?state=all&since=${SINCE_TS}&per_page=100"
```

**Incremental strategy**:
- On the first run, the connector can:
  - Either perform a full historical backfill (no `since`), or
  - Use a configurable `start_date` as the initial `since` cursor.
- On subsequent runs:
  - Use the maximum `updated_at` value (plus a small lookback window, e.g., a few minutes) from the previous sync as the new `since` parameter.
  - Sort by `updated` (or rely on GitHub’s default) and reprocess overlapping records to handle late-arriving updates.

**Handling deletes / closed issues**:
- GitHub does not commonly hard-delete issues; instead, issues are closed.
- The connector should:
  - Treat changes in `state` (e.g., from `open` to `closed`) and `closed_at` as **updates**, not deletes.
  - If a downstream system needs to interpret “closed” as a type of soft delete, this can be derived from the `state` and `closed_at` fields.

**Alternative APIs**:
- For more complex analytics, related endpoints (e.g., comments, events, timeline) can be used:
  - `GET /repos/{owner}/{repo}/issues/{issue_number}/comments`
  - `GET /repos/{owner}/{repo}/issues/{issue_number}/events`
  - `GET /repos/{owner}/{repo}/issues/{issue_number}/timeline`
- Those are out of scope for the initial connector’s primary `issues` table.


### Read endpoints for snapshot-style metadata objects

For objects treated as snapshots (`repositories`, `assignees`, `branches`, `collaborators`, `organizations`, `teams`, `users`), the connector typically performs a **full scan** per logical key (e.g., per owner or organization) on each sync.

- **Repositories**:  
  - `GET /users/{username}/repos` — list repositories for a user.  
  - `GET /orgs/{org}/repos` — list repositories for an organization.  
- **Assignees**:  
  - `GET /repos/{owner}/{repo}/assignees` — list of assignable users for a repository (paginated with `per_page` and `page`).  
- **Branches**:  
  - `GET /repos/{owner}/{repo}/branches` — supports pagination via `per_page` and `page`.  
- **Collaborators**:  
  - `GET /repos/{owner}/{repo}/collaborators` — paginated list of collaborators and permissions; may require elevated scopes.  
- **Organizations**:  
  - `GET /orgs/{org}` — single org by login.  
- **Teams**:  
  - `GET /orgs/{org}/teams` — paginated list of teams; uses `per_page`/`page`.  
- **Users**:  
  - `GET /users/{username}` — single user by login.

Snapshot tables do not use cursors; instead, the connector replaces the target table contents (or relies on downstream merge semantics).


### Read endpoints for activity-style objects

For objects that represent activity streams (`comments`, `commits`, `pull_requests`, `reviews`), the connector aims to read incrementally where practical:

- **Comments** (`cdc`):  
  - Endpoint: `GET /repos/{owner}/{repo}/issues/comments`  
  - Key query params: `since`, `per_page`, `page`.  
  - Incremental strategy: use `since` with the max `updated_at` from previous run (plus lookback), following `Link` pagination.

- **Commits** (`append`):  
  - Endpoint: `GET /repos/{owner}/{repo}/commits`  
  - Key query params: `sha` (branch), `since`, `until`, `per_page`, `page`.  
  - Incremental strategy (per branch): track the latest seen commit date or last `sha` per branch; treat earlier commits as immutable.

- **Pull requests** (`cdc`):  
  - Endpoint: `GET /repos/{owner}/{repo}/pulls`  
  - Key query params: `state`, `sort`, `direction`, `per_page`, `page`.  
  - Incremental strategy (TBD detail): mirror `issues` using `updated_at` as cursor, with lookback window and pagination via `Link` headers.

- **Reviews** (`append`):  
  - Endpoint: `GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews`  
  - Strategy: for each PR in scope, pull all reviews and treat them as append-only records keyed by review `id`.


## **Field Type Mapping**

### General mapping (GitHub JSON → connector logical types)

| GitHub JSON Type | Example Fields | Connector Logical Type | Notes |
|------------------|----------------|------------------------|-------|
| integer (32/64-bit) | `id`, `number`, `comments` | `long` / integer | For Spark-based connectors, prefer 64-bit integer (`LongType`). |
| string | `title`, `body`, `state`, `author_association` | string | UTF-8 text. Long markdown bodies should be supported. |
| boolean | `locked`, `site_admin`, `default` | boolean | Standard true/false. |
| string (ISO 8601 datetime) | `created_at`, `updated_at`, `closed_at`, `due_on` | timestamp with timezone | Stored as UTC timestamps; parsing must respect timezone `Z`. |
| object | `user`, `assignee`, `milestone`, `pull_request` | struct | Represented as nested records instead of flattened columns. |
| array | `labels`, `assignees` | array\<struct\> | Arrays of nested objects. |
| nullable fields | `body`, `closed_at`, `milestone`, `assignee` | corresponding type + null | When fields are absent or null, the connector should surface `null`, not `{}`. |

### Special behaviors and constraints

- `id` and other numeric identifiers should be stored as **64-bit integers** to avoid overflow (issues, comments, repositories, users, orgs, teams, reviews, etc.).
- `state`, `author_association`, review states, and PR states are effectively enums, but are represented as strings; connector may choose to preserve as strings for flexibility.
- Timestamp fields use ISO 8601 in UTC (e.g., `"2011-04-22T13:33:48Z"`); parsing must be robust to this format.
- Nested structs (`user`, `labels`, `owner`, `permissions`, `base`, `head`, etc.) should not be flattened in the connector implementation; instead, they should be represented as nested types.
- Commit SHAs are opaque strings and should not be truncated or cast to numeric types.


## **Write API**

The initial connector implementation is primarily **read-only**. However, for completeness, the GitHub REST API supports write operations relevant to the `issues` object.

### Create an issue

- **HTTP method**: `POST`
- **Endpoint**: `/repos/{owner}/{repo}/issues`

**Required scope**:
- For public repositories: `public_repo`
- For private repositories: `repo`

**Request body (JSON)**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `title` | string | yes | Title of the issue. |
| `body` | string | no | Body content of the issue. |
| `assignees` | array\<string\> | no | Usernames to assign to the issue. |
| `labels` | array\<string\> | no | Label names to apply. |
| `milestone` | integer | no | Milestone number to associate. |

Example request:

```bash
curl -X POST \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Found a bug",
    "body": "I am having a problem with this.",
    "assignees": ["octocat"],
    "labels": ["bug"]
  }' \
  "https://api.github.com/repos/<owner>/<repo>/issues"
```

### Update an issue

- **HTTP method**: `PATCH`
- **Endpoint**: `/repos/{owner}/{repo}/issues/{issue_number}`

**Commonly updated fields**:
- `title`
- `body`
- `state` (`open` or `closed`)
- `assignees`
- `labels`

Example request to close an issue:

```bash
curl -X PATCH \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  -H "Content-Type: application/json" \
  -d '{"state": "closed"}' \
  "https://api.github.com/repos/<owner>/<repo>/issues/1347"
```

**Validation / read-after-write**:
- The connector (or user) can validate writes by:
  - Reading back the updated issue via `GET /repos/{owner}/{repo}/issues/{issue_number}`, or
  - Letting the next incremental read (using `since` and `updated_at`) pick up the change.


## **Known Quirks & Edge Cases**

- **Issues vs pull requests**:
  - The `issues` endpoint returns both issues and pull requests; pull requests are distinguished by a non-null `pull_request` object.
  - Depending on downstream modeling, PRs may be filtered out or kept in the same table with an indicator.
- **Missing or partial fields**:
  - Some fields (e.g., `reactions`) may require specific preview headers or may be absent for older data.
  - The connector should treat missing nested objects as `null`, not empty dicts, to align with downstream schema expectations.
- **Authentication methods**:
  - Although GitHub supports OAuth apps and GitHub Apps, the connector will **only** support PAT-based auth to keep configuration simple and avoid managing OAuth flows.
- **Commits are immutable**:
  - Once pushed, commits identified by `sha` are effectively immutable; force-pushes only change which commits are reachable from a branch, not the commits themselves.
  - Incremental logic should treat commit data as append-only and avoid attempting to model updates or deletes.
- **Branch renames and deletions**:
  - Branch lists can change due to renames or deletions; the `branches` snapshot should be treated as authoritative per sync rather than attempting incremental branch-level CDC.
- **Permissions and collaborators**:
  - Collaborator lists and permissions can differ by repository and may require higher scopes; failures due to insufficient permissions should be handled gracefully and surfaced as configuration errors.
- **Org and team visibility**:
  - Organization and team endpoints may return restricted subsets of data depending on the authenticated user’s membership and role.
- **Review lifecycle**:
  - Reviews may be dismissed, but the underlying review record is typically retained; treating reviews as append-only is usually sufficient for analytics.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://docs.github.com/en/rest/issues/issues | 2025-11-14 | High | `GET /repos/{owner}/{repo}/issues` endpoint behavior, parameters, example response, and issue fields. |
| Official Docs | https://docs.github.com/en/rest/using-the-rest-api/authenticating-with-github-api | 2025-11-14 | High | Personal access token authentication method, header format, and rate limit behavior. |
| Official Docs | https://docs.github.com/en/rest/overview/resources-in-the-rest-api#rate-limiting | 2025-11-14 | High | Authenticated vs unauthenticated rate limits. |
| Official Docs | https://docs.github.com/en/rest/repos/repos#get-a-repository | 2025-11-14 | High | Repository object and justification for treating `repositories` as snapshot. |
| Official Docs | https://docs.github.com/en/rest/issues/issues#create-an-issue | 2025-11-14 | High | Write API for creating issues, required fields, and scopes. |
| Official Docs | https://docs.github.com/en/rest/issues/issues#update-an-issue | 2025-11-14 | High | Write API for updating issues and common mutable fields. |
| Official Docs | https://docs.github.com/en/rest/branches/branches | 2025-11-14 | High | `GET /repos/{owner}/{repo}/branches` behavior and branch fields. |
| Official Docs | https://docs.github.com/en/rest/collaborators/collaborators | 2025-11-14 | High | Collaborators endpoints, required scopes, and permissions structure. |
| Official Docs | https://docs.github.com/en/rest/orgs/orgs | 2025-11-14 | High | Organization object fields and access patterns. |
| Official Docs | https://docs.github.com/en/rest/teams/teams | 2025-11-14 | High | Team object fields and listing behavior. |
| Official Docs | https://docs.github.com/en/rest/users/users | 2025-11-14 | High | User object fields and retrieval by username. |
| Official Docs | https://docs.github.com/en/rest/issues/comments | 2025-11-14 | High | Issue comments endpoints, use of `since`, and comment fields. |
| Official Docs | https://docs.github.com/en/rest/commits/commits | 2025-11-14 | High | Commit listing endpoints, parameters (`sha`, `since`, `until`), and commit structure. |
| Official Docs | https://docs.github.com/en/rest/pulls/pulls | 2025-11-14 | High | Pull request listing behavior, fields, and state/sort parameters. |
| Official Docs | https://docs.github.com/en/rest/pulls/reviews | 2025-11-14 | High | Pull request review endpoints, review states, and fields. |
| OSS Connector Docs | https://docs.airbyte.com/integrations/sources/github | 2025-11-14 | High | Airbyte GitHub source connector behavior, use of `updated_at` as cursor, and supported streams. |
| OSS Connector Code | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-github | 2025-11-14 | High | Reference implementation details for incremental sync, pagination handling via `Link` headers, and object list. |


## **Sources and References**

- **Official GitHub REST API documentation** (highest confidence)
  - `https://docs.github.com/en/rest`
  - `https://docs.github.com/en/rest/issues/issues`
  - `https://docs.github.com/en/rest/using-the-rest-api/authenticating-with-github-api`
  - `https://docs.github.com/en/rest/overview/resources-in-the-rest-api#rate-limiting`
  - `https://docs.github.com/en/rest/issues/issues#create-an-issue`
  - `https://docs.github.com/en/rest/issues/issues#update-an-issue`
  - `https://docs.github.com/en/rest/branches/branches`
  - `https://docs.github.com/en/rest/collaborators/collaborators`
  - `https://docs.github.com/en/rest/orgs/orgs`
  - `https://docs.github.com/en/rest/teams/teams`
  - `https://docs.github.com/en/rest/users/users`
  - `https://docs.github.com/en/rest/issues/comments`
  - `https://docs.github.com/en/rest/commits/commits`
  - `https://docs.github.com/en/rest/pulls/pulls`
  - `https://docs.github.com/en/rest/pulls/reviews`
- **Airbyte GitHub source connector documentation and implementation** (high confidence)
  - `https://docs.airbyte.com/integrations/sources/github`
  - `https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-github`

When conflicts arise, **official GitHub documentation** is treated as the source of truth, with the Airbyte connector used to validate practical details like pagination and cursor fields.


