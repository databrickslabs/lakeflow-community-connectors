# **Azure DevOps Git API Documentation**

## **Authorization**

- **Chosen method**: Personal Access Token (PAT) for the Azure DevOps REST API v7.1.
- **Base URL**: `https://dev.azure.com/{organization}`
- **Auth placement**:
  - HTTP header: `Authorization: Basic <base64_encoded_pat>`
  - The PAT must be Base64-encoded in the format `:{pat}` (empty username, colon, then the PAT).
  - Alternatively, HTTP Basic authentication with empty username and PAT as password.
- **Required scopes for read-only access**:
  - `Code (read)` - Grants read access to source code, commits, and Git repositories (for Git objects: repositories, commits, pullrequests, pullrequest_threads, pullrequest_workitems, pullrequest_commits, pullrequest_reviewers, refs, pushes).
  - `Graph (read)` - Grants read access to user identities and profiles via the Graph Users API (for users object).
  - `Project and Team (read)` - Grants read access to project metadata (for projects object).
  - `Work Items (read)` - Grants read access to work items, revisions, and work item types (for workitems, workitem_revisions, workitem_types objects).
- **Other supported methods (not used by this connector)**:
  - OAuth 2.0 is also supported by Azure DevOps, but the connector will **not** perform interactive OAuth flows. Tokens must be provisioned out-of-band and stored in configuration.

**API Version Note**:
- This connector uses API version **7.1** (stable) instead of 7.2 to avoid preview version requirements.
- As of the implementation date, version 7.2 requires the `-preview` flag in the api-version parameter.
- Version 7.1 provides the same repository data without preview restrictions.

Example authenticated request:

```bash
# Using Basic Auth with PAT
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/{organization}/{project}/_apis/git/repositories?api-version=7.1"

# Or using Authorization header with Base64-encoded PAT
curl -H "Authorization: Basic <BASE64_ENCODED_PAT>" \
  -H "Accept: application/json" \
  "https://dev.azure.com/{organization}/{project}/_apis/git/repositories?api-version=7.1"
```

Notes:
- The connector stores `organization`, `project`, and `personal_access_token` in configuration.
- Rate limiting for Azure DevOps Services follows global service limits and throttling rules documented at https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits
- Azure DevOps applies throttling based on the number of requests and the TSTUs (throughput units). Typical limits are around 200 requests per user per minute for most APIs, but this can vary by resource.


## **Object List**

For connector purposes, we treat specific Azure DevOps Git REST resources as **objects/tables**.  
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Endpoint | Ingestion Type |
|------------|-------------|------------------|----------------|
| `projects` | Projects within an organization | `GET /{organization}/_apis/projects` | `snapshot` |
| `repositories` | Git repositories within a project | `GET /{organization}/{project}/_apis/git/repositories` | `snapshot` |
| `commits` | Git commits across repositories | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/commits` | `append` |
| `pullrequests` | Pull requests across repositories | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pullrequests` | `cdc` |
| `pullrequest_threads` | Comments and discussions on pull requests | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/threads` | `append` |
| `pullrequest_workitems` | Work items linked to pull requests | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/workitems` | `snapshot` |
| `pullrequest_commits` | Commits included in pull requests | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/commits` | `snapshot` |
| `pullrequest_reviewers` | Detailed reviewer information for pull requests | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/reviewers` | `cdc` |
| `refs` | Git references (branches and tags) | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/refs` | `snapshot` |
| `pushes` | Git push events to repositories | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pushes` | `append` |
| `users` | User identities and profiles in the organization | `GET https://vssps.dev.azure.com/{organization}/_apis/graph/users` | `snapshot` |
| `workitems` | Work items (tasks, bugs, user stories, etc.) | `GET /{organization}/{project}/_apis/wit/workitems` | `cdc` |
| `workitem_revisions` | Historical changes to work items | `GET /{organization}/{project}/_apis/wit/reporting/workitemrevisions` | `append` |
| `workitem_types` | Work item type definitions and fields | `GET /{organization}/{project}/_apis/wit/workitemtypes` | `snapshot` |

**Connector scope**:
- Supports 14 objects from Azure DevOps REST API v7.1: 1 project object + 9 Git objects (including 4 PR-related) + 1 identity object + 3 work item tracking objects.
- **Connection parameters**:
  - `organization` (required) - Azure DevOps organization name
  - `project` (optional) - If provided, scopes Git and Work Item operations to specific project; if omitted, tables can fetch from all projects
- The `projects` and `users` objects require only `organization` connection parameter.
- Git objects support **project auto-discovery**:
  - **If `project` specified at connection level**: Use that project for all Git operations
  - **If `project` specified in table_options**: Use that specific project (overrides connection-level setting)
  - **If neither specified**: Auto-fetch from ALL projects in the organization
- Git objects except `repositories` also support `repository_id` as an **optional** table option:
  - **If provided**: Fetches data from specific repository only
  - **If omitted**: Auto-fetches data from ALL repositories (in the specified or discovered projects)
- PR-related objects (`pullrequest_threads`, `pullrequest_workitems`, `pullrequest_commits`, `pullrequest_reviewers`) support **pull request auto-discovery**:
  - **If both `repository_id` and `pullrequest_id` provided**: Fetch data for specific PR only (most efficient)
  - **If only `repository_id` provided**: Auto-fetch data for ALL pull requests in that repository
  - **If neither provided**: Auto-fetch data for ALL pull requests across ALL repositories (and projects if project not specified)
- Work Item objects (`workitems`, `workitem_revisions`, `workitem_types`) **require project specification**:
  - Work items are scoped to a specific project (cannot query across all projects in a single API call)
  - **If `project` specified at connection level**: Use that project
  - **If `project` specified in table_options**: Use that specific project (overrides connection-level setting)
  - **If neither specified**: Connector will iterate through all projects (fetched via `projects` table) and aggregate results

High-level notes:
- **Projects**: Project metadata including name, description, state, visibility, and capabilities.
- **Repositories**: Repository metadata including configuration, URLs, and project information.
- **Commits**: Individual Git commits with author, committer, message, tree ID, and parent commits.
- **Pull Requests**: Pull request metadata including source/target branches, status, reviewers, and completion details.
- **Pull Request Threads**: Comments and discussion threads on pull requests, including replies and thread status.
- **Pull Request Work Items**: Work items (stories, bugs, tasks) linked to pull requests for traceability.
- **Pull Request Commits**: Specific commits included in a pull request, showing the changes being reviewed.
- **Pull Request Reviewers**: Detailed reviewer information including vote status, review time, and reviewer identity.
- **Refs**: Git references (branches and tags) with names, commit IDs, and creators.
- **Pushes**: Push events with pusher information, commit ranges, and ref updates.
- **Users**: User identity information including display names, email addresses, account status, and profile details.
- **Work Items**: Individual work items including tasks, bugs, user stories, features, and epics with all fields and relationships.
- **Work Item Revisions**: Complete history of changes to work items, including field updates, state transitions, and assignments.
- **Work Item Types**: Work item type definitions including fields, states, workflow rules, and categories.


## **Object Schema**

### General notes

- Azure DevOps provides a well-defined JSON schema for resources via its REST API documentation.
- For the connector, we define **tabular schemas** per object, derived from the JSON representation.
- Nested JSON objects (e.g., `project`, `_links`) are modeled as **nested structures** rather than being fully flattened.

### `projects` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/_apis/projects?api-version=7.1`

**Key behavior**:
- Returns all projects within an organization that the authenticated user has access to.
- No built-in incremental cursor; treated as snapshot data.
- Enables project discovery for multi-project organizations.

**High-level schema (connector view)**:

Top-level fields (all from the Azure DevOps REST API):

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Unique identifier for the project (GUID format). |
| `name` | string | Project name. |
| `description` | string | Project description (may be empty). |
| `url` | string | API URL for the project resource. |
| `state` | string | Project state (e.g., `wellFormed`, `createPending`, `deleting`, `deleted`). |
| `revision` | long | Project revision number (increments with updates). |
| `visibility` | string | Project visibility (`private` or `public`). |
| `lastUpdateTime` | string (ISO 8601) | Timestamp of last project update. |
| `organization` | string | Organization name (connector-derived). |

**Nested structures**:

None for basic project listing. The full project details API returns additional nested structures like capabilities, but the list endpoint provides the essential metadata shown above.

**Primary key**:  
`id` (the project GUID)

**Notes on data types**:
- All timestamps are ISO 8601 UTC strings.
- The `organization` field is added by the connector to provide context.
- Missing or null values for optional fields (like `description`) are represented as `null` in the schema.

### `repositories` object (primary table)

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories?api-version=7.1`

**Key behavior**:
- Returns all Git repositories within a specified project.
- Supports retrieval of individual repositories by ID or name.
- No built-in incremental cursor; treated as snapshot data.

**High-level schema (connector view)**:

Top-level fields (all from the Azure DevOps REST API):

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string (UUID) | Unique identifier for the repository (GUID format). |
| `name` | string | Repository name. |
| `url` | string | API URL for the repository resource. |
| `project` | struct | Project metadata containing the repository (see nested schema). |
| `defaultBranch` | string or null | Full reference name of the default branch (e.g., `refs/heads/main`). |
| `size` | integer (64-bit) or null | Repository size in bytes. May be null for empty repositories. |
| `remoteUrl` | string | HTTPS clone URL for the repository. |
| `sshUrl` | string | SSH clone URL for the repository. |
| `webUrl` | string | Web URL for browsing the repository in Azure DevOps web interface. |
| `isDisabled` | boolean or null | Whether the repository is disabled. |
| `isInMaintenance` | boolean or null | Whether the repository is in maintenance mode. |
| `isFork` | boolean or null | Whether the repository is a fork. May be null if not applicable. |
| `parentRepository` | struct or null | Reference to the parent repository if this is a fork (see nested schema). |
| `_links` | struct | HAL-style links to related resources (see nested schema). |
| `organization` | string (connector-derived) | The `{organization}` path parameter used to retrieve this repository. |
| `project_name` | string (connector-derived) | The `{project}` path parameter used to retrieve this repository. |

**Nested `project` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique project identifier. |
| `name` | string | Project name. |
| `url` | string | API URL for the project resource. |
| `state` | string | Project state (e.g., `wellFormed`, `createPending`, `deleting`). |
| `revision` | integer (64-bit) | Project revision number. |
| `visibility` | string or null | Project visibility (`private` or `public`). |
| `lastUpdateTime` | string (ISO 8601 datetime) or null | Last time the project was updated. |

**Nested `parentRepository` struct** (present only if `isFork` is true):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique identifier of the parent repository. |
| `name` | string | Parent repository name. |
| `url` | string | API URL for the parent repository. |
| `project` | struct | Project containing the parent repository (same schema as top-level `project`). |

**Nested `_links` struct** (HAL-style hypermedia links):

| Field | Type | Description |
|-------|------|-------------|
| `self` | struct | Link to this repository resource. Contains `href` (string). |
| `project` | struct | Link to the containing project. Contains `href` (string). |
| `web` | struct | Link to the web UI for this repository. Contains `href` (string). |
| `ssh` | struct | SSH clone URL. Contains `href` (string). |
| `commits` | struct | Link to commits API. Contains `href` (string). |
| `refs` | struct | Link to refs (branches/tags) API. Contains `href` (string). |
| `pullRequests` | struct | Link to pull requests API. Contains `href` (string). |
| `items` | struct | Link to items (files/folders) API. Contains `href` (string). |
| `pushes` | struct | Link to pushes API. Contains `href` (string). |

**Example request** (list all repositories in a project):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories?api-version=7.1"
```

**Example response (truncated)**:

```json
{
  "count": 3,
  "value": [
    {
      "id": "5febef5a-833d-4e14-b9c0-14cb638f91e6",
      "name": "AnotherRepository",
      "url": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6",
      "project": {
        "id": "6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c",
        "name": "Fabrikam-Fiber-Git",
        "url": "https://dev.azure.com/fabrikam/_apis/projects/6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c",
        "state": "wellFormed",
        "revision": 293012730,
        "visibility": "private",
        "lastUpdateTime": "2023-01-15T10:30:00Z"
      },
      "defaultBranch": "refs/heads/master",
      "remoteUrl": "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_git/AnotherRepository",
      "sshUrl": "git@ssh.dev.azure.com:v3/fabrikam/Fabrikam-Fiber-Git/AnotherRepository",
      "webUrl": "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_git/AnotherRepository",
      "size": 1234567,
      "isDisabled": false,
      "isInMaintenance": false,
      "_links": {
        "self": {
          "href": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6"
        },
        "project": {
          "href": "https://dev.azure.com/fabrikam/_apis/projects/6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c"
        },
        "web": {
          "href": "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_git/AnotherRepository"
        },
        "commits": {
          "href": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6/commits"
        },
        "refs": {
          "href": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6/refs"
        },
        "pullRequests": {
          "href": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6/pullRequests"
        },
        "items": {
          "href": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6/items"
        },
        "pushes": {
          "href": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6/pushes"
        }
      }
    },
    {
      "id": "278d5cd2-584d-4b63-824a-2ba458937249",
      "name": "Fabrikam-Fiber-Git",
      "url": "https://dev.azure.com/fabrikam/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249",
      "project": {
        "id": "6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c",
        "name": "Fabrikam-Fiber-Git",
        "url": "https://dev.azure.com/fabrikam/_apis/projects/6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c",
        "state": "wellFormed"
      },
      "defaultBranch": "refs/heads/master",
      "remoteUrl": "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_git/Fabrikam-Fiber-Git",
      "sshUrl": "git@ssh.dev.azure.com:v3/fabrikam/Fabrikam-Fiber-Git/Fabrikam-Fiber-Git",
      "webUrl": "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_git/Fabrikam-Fiber-Git"
    }
  ]
}
```

**Example request** (get a single repository by ID):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6?api-version=7.1"
```

> The columns listed above define the **complete connector schema** for the `repositories` table.  
> If additional Azure DevOps repository fields are needed in the future, they must be added as new columns here so the documentation continues to reflect the full table schema.


### `commits` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/commits?api-version=7.1`

**Key behavior**:
- Returns commits from a specific repository.
- Supports pagination using `top` and `skip` parameters.
- Supports filtering by date range, author, and commit IDs.
- Append-only ingestion using commit timestamps or skip-based pagination.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `commitId` | string (SHA-1 hash) | Unique SHA-1 hash identifying the commit (40 hex characters). |
| `author` | struct | Commit author information (see nested schema). |
| `committer` | struct | Committer information (see nested schema). |
| `comment` | string | Commit message. |
| `commentTruncated` | boolean | Whether the commit message was truncated in the response. |
| `changeCounts` | struct or null | Statistics on changes (Add, Edit, Delete counts). |
| `url` | string | API URL for this commit resource. |
| `remoteUrl` | string | Web URL for viewing the commit. |
| `treeId` | string (SHA-1 hash) or null | Git tree ID for this commit. |
| `parents` | array of strings | Array of parent commit IDs (SHA-1 hashes). |
| `push` | struct or null | Information about the push that created this commit. |
| `workItems` | array of structs or null | Work items linked to this commit. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID this commit belongs to. |

**Nested `author` and `committer` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Display name of the author/committer. |
| `email` | string | Email address. |
| `date` | string (ISO 8601 datetime) | Timestamp when the commit was authored/committed. |

**Nested `changeCounts` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `Add` | integer | Number of files added. |
| `Edit` | integer | Number of files modified. |
| `Delete` | integer | Number of files deleted. |

**Example request** (list commits with pagination):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/commits?api-version=7.1&$top=100"
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name to fetch commits from.
  - If provided: Fetches commits from the specified repository only.
  - If omitted: Auto-fetches commits from ALL repositories in the project.


### `pullrequests` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullrequests?api-version=7.1`

**Key behavior**:
- Returns pull requests from a specific repository.
- Supports filtering by status (Active, Completed, Abandoned, All).
- Supports filtering by creator, reviewer, source/target branch.
- CDC ingestion using `lastMergeCommit.commitId` or `closedDate` as cursor.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `pullRequestId` | integer | Unique identifier for the pull request. |
| `codeReviewId` | integer | Associated code review ID. |
| `status` | string | Pull request status: `active`, `completed`, `abandoned`. |
| `createdBy` | struct | User who created the pull request (see nested identity schema). |
| `creationDate` | string (ISO 8601 datetime) | When the pull request was created. |
| `closedDate` | string (ISO 8601 datetime) or null | When the pull request was closed (completed or abandoned). |
| `title` | string | Pull request title. |
| `description` | string or null | Pull request description. |
| `sourceRefName` | string | Source branch reference (e.g., `refs/heads/feature-branch`). |
| `targetRefName` | string | Target branch reference (e.g., `refs/heads/main`). |
| `mergeStatus` | string | Merge status: `succeeded`, `failed`, `conflicts`, `queued`, etc. |
| `mergeId` | string (SHA-1 hash) or null | Merge commit ID if completed. |
| `lastMergeSourceCommit` | struct or null | Source commit at time of last merge. |
| `lastMergeTargetCommit` | struct or null | Target commit at time of last merge. |
| `lastMergeCommit` | struct or null | The merge commit itself. |
| `reviewers` | array of structs | List of reviewers with their vote status. |
| `url` | string | API URL for this pull request. |
| `supportsIterations` | boolean | Whether this PR supports iterations. |
| `artifactId` | string or null | Artifact ID for the pull request. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID. |

**Nested identity struct** (for `createdBy`, reviewers, etc.):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | User ID. |
| `displayName` | string | Display name. |
| `uniqueName` | string | Unique name (usually email). |
| `url` | string or null | API URL for the identity. |
| `imageUrl` | string or null | Profile image URL. |

**Nested commit reference struct**:

| Field | Type | Description |
|-------|------|-------------|
| `commitId` | string (SHA-1 hash) | Commit ID. |
| `url` | string | API URL for the commit. |

**Example request** (list active pull requests):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullrequests?api-version=7.1&searchCriteria.status=active"
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name.
  - If provided: Fetches pull requests from the specified repository only.
  - If omitted: Auto-fetches pull requests from ALL repositories in the project.
- `status_filter` (string, optional): Filter by status - `active`, `completed`, `abandoned`, or `all`. Default: `active`.


### `pullrequest_threads` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/threads?api-version=7.1`

**Key behavior**:
- Returns all comment threads for a specific pull request.
- Includes inline code comments, file comments, and general discussion.
- Each thread can have multiple comments (replies).
- Thread status tracks resolution state.
- Append-only for new comments; supports CDC for status updates.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer | Unique identifier for the thread. |
| `publishedDate` | string (ISO 8601 datetime) | When the thread was created. |
| `lastUpdatedDate` | string (ISO 8601 datetime) | When the thread was last updated. |
| `comments` | array of structs | All comments in this thread (see nested schema). |
| `status` | string | Thread status: `active`, `fixed`, `wontFix`, `closed`, `byDesign`, `pending`. |
| `threadContext` | struct or null | Context information for code comments (file path, line position). |
| `properties` | struct or null | Additional properties for the thread. |
| `identities` | struct or null | Identity references used in the thread. |
| `isDeleted` | boolean | Whether the thread has been deleted. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID. |
| `pullrequest_id` | integer (connector-derived) | Pull request ID. |

**Nested `comments` array item struct**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Comment ID. |
| `parentCommentId` | integer or null | Parent comment ID for replies. |
| `author` | struct | User who wrote the comment (see identity struct). |
| `content` | string | Comment text (supports markdown). |
| `publishedDate` | string (ISO 8601 datetime) | When comment was published. |
| `lastUpdatedDate` | string (ISO 8601 datetime) | When comment was last edited. |
| `lastContentUpdatedDate` | string (ISO 8601 datetime) or null | When content was last changed. |
| `commentType` | string | Comment type: `text`, `codeChange`, `system`. |
| `usersLiked` | array of structs or null | Users who liked this comment. |

**Nested `threadContext` struct** (for code comments):

| Field | Type | Description |
|-------|------|-------------|
| `filePath` | string | Path to the file being commented on. |
| `rightFileStart` | struct or null | Start position in the new version (line, offset). |
| `rightFileEnd` | struct or null | End position in the new version. |
| `leftFileStart` | struct or null | Start position in the old version. |
| `leftFileEnd` | struct or null | End position in the old version. |

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/threads?api-version=7.1"
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name.
  - If provided: Fetches from specified repository only.
  - If omitted: Auto-fetches from ALL repositories.
- `pullrequest_id` (integer, **optional**): Pull request ID.
  - If provided: Fetches threads for specific PR only.
  - If omitted: Auto-fetches threads for ALL pull requests (after fetching PRs).


### `pullrequest_workitems` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/workitems?api-version=7.1`

**Key behavior**:
- Returns work items linked to a specific pull request.
- Provides lightweight work item references (ID, URL, title).
- Links are created when PRs reference work items in commit messages or manually.
- Snapshot ingestion - links can be added or removed.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | string | Work item ID. |
| `url` | string | API URL for the work item. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID. |
| `pullrequest_id` | integer (connector-derived) | Pull request ID. |

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/workitems?api-version=7.1"
```

**Example response**:

```json
[
  {
    "id": "299",
    "url": "https://dev.azure.com/fabrikam/_apis/wit/workItems/299"
  },
  {
    "id": "300",
    "url": "https://dev.azure.com/fabrikam/_apis/wit/workItems/300"
  }
]
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name.
  - If provided: Fetches from specified repository only.
  - If omitted: Auto-fetches from ALL repositories.
- `pullrequest_id` (integer, **optional**): Pull request ID.
  - If provided: Fetches work items for specific PR only.
  - If omitted: Auto-fetches work items for ALL pull requests.


### `pullrequest_commits` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/commits?api-version=7.1`

**Key behavior**:
- Returns commits included in a specific pull request.
- Shows the exact changes being reviewed in the PR.
- Commits are listed in chronological order.
- Snapshot ingestion - commit list can grow as PR is updated.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `commitId` | string (SHA-1 hash) | Commit SHA-1 hash. |
| `author` | struct | Commit author information (see nested schema). |
| `committer` | struct | Committer information. |
| `comment` | string | Commit message. |
| `commentTruncated` | boolean | Whether commit message was truncated. |
| `url` | string | API URL for this commit. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID. |
| `pullrequest_id` | integer (connector-derived) | Pull request ID. |

**Nested `author` and `committer` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Display name. |
| `email` | string | Email address. |
| `date` | string (ISO 8601 datetime) | Timestamp. |

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/commits?api-version=7.1"
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name.
  - If provided: Fetches from specified repository only.
  - If omitted: Auto-fetches from ALL repositories.
- `pullrequest_id` (integer, **optional**): Pull request ID.
  - If provided: Fetches commits for specific PR only.
  - If omitted: Auto-fetches commits for ALL pull requests.


### `pullrequest_reviewers` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/reviewers?api-version=7.1`

**Key behavior**:
- Returns detailed reviewer information for a specific pull request.
- Includes vote status, review time, and required reviewer flags.
- Reviewers can be added/removed, and votes can change.
- CDC ingestion - tracks reviewer additions and vote changes.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `reviewerUrl` | string or null | API URL for the reviewer resource. |
| `vote` | integer | Reviewer's vote: 10 (approved), 5 (approved with suggestions), 0 (no vote), -5 (waiting for author), -10 (rejected). |
| `hasDeclined` | boolean or null | Whether the reviewer has declined to review. |
| `isFlagged` | boolean or null | Whether the reviewer has flagged the PR. |
| `displayName` | string | Reviewer's display name. |
| `id` | string (UUID) | Reviewer's user ID. |
| `uniqueName` | string | Unique name (usually email). |
| `url` | string or null | API URL for the user. |
| `imageUrl` | string or null | Profile image URL. |
| `isRequired` | boolean or null | Whether this reviewer is required. |
| `votedFor` | array of structs or null | For group reviewers, individual users who voted. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID. |
| `pullrequest_id` | integer (connector-derived) | Pull request ID. |

**Vote values**:
- `10`: Approved
- `5`: Approved with suggestions
- `0`: No vote (or reviewer added but hasn't reviewed)
- `-5`: Waiting for author (changes requested)
- `-10`: Rejected

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/reviewers?api-version=7.1"
```

**Example response**:

```json
[
  {
    "reviewerUrl": "https://dev.azure.com/fabrikam/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/reviewers/a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890",
    "vote": 10,
    "hasDeclined": false,
    "isFlagged": false,
    "displayName": "John Doe",
    "id": "a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890",
    "uniqueName": "john.doe@fabrikam.com",
    "url": "https://vssps.dev.azure.com/fabrikam/_apis/Identities/a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890",
    "imageUrl": "https://dev.azure.com/fabrikam/_api/_common/identityImage?id=a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890",
    "isRequired": true
  }
]
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name.
  - If provided: Fetches from specified repository only.
  - If omitted: Auto-fetches from ALL repositories.
- `pullrequest_id` (integer, **optional**): Pull request ID.
  - If provided: Fetches reviewers for specific PR only.
  - If omitted: Auto-fetches reviewers for ALL pull requests.


### `refs` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/refs?api-version=7.1`

**Key behavior**:
- Returns Git references (branches and tags) from a repository.
- Supports filtering by ref name prefix (e.g., `heads/` for branches, `tags/` for tags).
- No pagination typically needed (refs list is usually small).
- Snapshot ingestion (refs can be created, updated, or deleted).

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `name` | string | Full reference name (e.g., `refs/heads/main`, `refs/tags/v1.0`). |
| `objectId` | string (SHA-1 hash) | SHA-1 of the commit this ref points to. |
| `creator` | struct or null | User who created the ref (see identity struct). |
| `url` | string | API URL for this ref. |
| `peeledObjectId` | string (SHA-1 hash) or null | For tags pointing to tag objects, the commit SHA-1. |
| `statuses` | array of structs or null | Status checks associated with this ref. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID. |

**Example request** (list all refs):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/refs?api-version=7.1"
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name.
  - If provided: Fetches refs from the specified repository only.
  - If omitted: Auto-fetches refs from ALL repositories in the project.
- `filter` (string, optional): Ref name prefix filter (e.g., `heads/` for branches, `tags/` for tags).


### `pushes` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repositoryId}/pushes?api-version=7.1`

**Key behavior**:
- Returns push events to a repository.
- Supports pagination using `top` and `skip` parameters.
- Supports filtering by date range, pusher, and ref name.
- Append-only ingestion using push ID or date.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `pushId` | integer | Unique identifier for the push. |
| `date` | string (ISO 8601 datetime) | Timestamp when the push occurred. |
| `pushedBy` | struct | User who performed the push (see identity struct). |
| `url` | string | API URL for this push. |
| `refUpdates` | array of structs | List of ref updates in this push. |
| `commits` | array of structs | Commits included in the push. |
| `repository` | struct or null | Repository information. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |
| `repository_id` | string (connector-derived) | Repository ID. |

**Nested `refUpdates` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Reference name (e.g., `refs/heads/main`). |
| `oldObjectId` | string (SHA-1 hash) | Previous commit ID. |
| `newObjectId` | string (SHA-1 hash) | New commit ID after push. |

**Example request** (list recent pushes):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pushes?api-version=7.1&$top=100"
```

**Table options**:
- `repository_id` (string, **optional**): Repository ID or name.
  - If provided: Fetches pushes from the specified repository only.
  - If omitted: Auto-fetches pushes from ALL repositories in the project.


### `users` object

**Source endpoint**:  
`GET https://vssps.dev.azure.com/{organization}/_apis/graph/users?api-version=7.1-preview.1`

**Important Notes**:
- The Graph API uses a different base URL: `https://vssps.dev.azure.com` instead of `https://dev.azure.com`
- This API is part of the Visual Studio Services Platform and handles user/identity management across Azure DevOps
- While marked as "preview", this API is stable and widely used in production

**Key behavior**:
- Returns all user identities in the Azure DevOps organization.
- Does NOT require a project parameter - operates at organization level.
- Supports pagination using continuation tokens.
- Snapshot ingestion - no incremental cursor available.
- Includes both active and inactive users.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `descriptor` | string | Unique immutable identifier for the user (base64-encoded). Primary key. |
| `displayName` | string | User's display name. |
| `mailAddress` | string or null | User's email address. |
| `principalName` | string | User's principal name (typically email or UPN). |
| `origin` | string | Origin of the user identity (e.g., `aad` for Azure Active Directory, `msa` for Microsoft Account). |
| `originId` | string | Identifier in the origin system (e.g., Azure AD object ID). |
| `subjectKind` | string | Type of subject, typically `user`. |
| `domain` | string or null | Domain for the user account. |
| `directoryAlias` | string or null | Directory alias for the user. |
| `url` | string | API URL for this user resource. |
| `_links` | struct | HAL-style hypermedia links to related resources. |
| `organization` | string (connector-derived) | Organization name. |

**Nested `_links` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `self` | struct with href | Link to this user resource. |
| `memberships` | struct with href | Link to user's group memberships. |
| `membershipState` | struct with href | Link to membership state information. |
| `storageKey` | struct with href | Link to storage key for the user. |
| `avatar` | struct with href | Link to user's avatar image. |

**Example request** (list all users):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://vssps.dev.azure.com/fabrikam/_apis/graph/users?api-version=7.1-preview.1"
```

**Example response** (single user):

```json
{
  "subjectKind": "user",
  "domain": "fabrikam.com",
  "principalName": "john.doe@fabrikam.com",
  "mailAddress": "john.doe@fabrikam.com",
  "origin": "aad",
  "originId": "a1b2c3d4-e5f6-7890-a1b2-c3d4e5f67890",
  "displayName": "John Doe",
  "url": "https://vssps.dev.azure.com/fabrikam/_apis/Graph/Users/aad.YTFiMmMzZDQtZTVmNi03ODkwLWExYjItYzNkNGU1ZjY3ODkw",
  "descriptor": "aad.YTFiMmMzZDQtZTVmNi03ODkwLWExYjItYzNkNGU1ZjY3ODkw",
  "_links": {
    "self": {
      "href": "https://vssps.dev.azure.com/fabrikam/_apis/Graph/Users/aad.YTFiMmMzZDQtZTVmNi03ODkwLWExYjItYzNkNGU1ZjY3ODkw"
    },
    "memberships": {
      "href": "https://vssps.dev.azure.com/fabrikam/_apis/Graph/Memberships/aad.YTFiMmMzZDQtZTVmNi03ODkwLWExYjItYzNkNGU1ZjY3ODkw"
    },
    "avatar": {
      "href": "https://dev.azure.com/fabrikam/_apis/GraphProfile/MemberAvatars/aad.YTFiMmMzZDQtZTVmNi03ODkwLWExYjItYzNkNGU1ZjY3ODkw"
    }
  }
}
```

**Table options**:
- None. The `users` object operates at organization level and does not require table-specific options.

**Authentication requirements**:
- Requires PAT with `Graph (read)` scope.
- This is a different API surface (Graph API) than the Git objects, which use the `Code (read)` scope.


### `workitems` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/wit/workitems?ids={ids}&api-version=7.1`

**Key behavior**:
- Returns work items (tasks, bugs, user stories, features, epics, etc.) with all fields.
- **Optional IDs**: If `ids` table option is provided, fetches specific work items. If omitted, uses WIQL to auto-discover and fetch all work items in the project(s).
- Supports batch retrieval of up to 200 work items per request.
- CDC ingestion - work items can be created, updated, or soft-deleted.
- Fields are dynamic based on work item type and process template.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer | Unique work item ID across the organization. |
| `rev` | integer | Revision number (increments with each update). |
| `fields` | struct | Dynamic field collection (see nested schema). |
| `relations` | array of structs or null | Links to other work items, commits, PRs, etc. |
| `_links` | struct | HAL-style hypermedia links. |
| `url` | string | API URL for this work item. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |

**Nested `fields` struct** (common fields - actual fields vary by work item type):

| Field | Type | Description |
|-------|------|-------------|
| `System.Id` | integer | Work item ID (duplicate of top-level `id`). |
| `System.Title` | string | Work item title. |
| `System.WorkItemType` | string | Type: `Task`, `Bug`, `User Story`, `Feature`, `Epic`, etc. |
| `System.State` | string | Current state: `New`, `Active`, `Resolved`, `Closed`, etc. |
| `System.AssignedTo` | string or null | Assigned user display name. |
| `System.CreatedDate` | string (ISO 8601 datetime) | When work item was created. |
| `System.CreatedBy` | string | User who created the work item. |
| `System.ChangedDate` | string (ISO 8601 datetime) | Last update timestamp. |
| `System.ChangedBy` | string | User who last modified the work item. |
| `System.Reason` | string | Reason for current state. |
| `System.Description` | string or null | HTML-formatted description. |
| `System.Tags` | string or null | Semicolon-separated tags. |
| `System.AreaPath` | string | Area path for categorization. |
| `System.IterationPath` | string | Iteration/sprint path. |
| `System.Priority` | integer or null | Priority level. |
| `Microsoft.VSTS.Common.Severity` | string or null | Severity (for bugs). |
| `Microsoft.VSTS.Scheduling.StoryPoints` | number or null | Story points estimate. |

**Nested `relations` array item struct**:

| Field | Type | Description |
|-------|------|-------------|
| `rel` | string | Relation type: `System.LinkTypes.Hierarchy-Forward`, `ArtifactLink`, etc. |
| `url` | string | URL to the related item. |
| `attributes` | struct or null | Relation attributes (name, comment, etc.). |

**Example request** (batch get work items):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/workitems?ids=1,2,3,4,5&api-version=7.1&\$expand=relations"
```

**Example response** (single work item):

```json
{
  "id": 299,
  "rev": 10,
  "fields": {
    "System.Id": 299,
    "System.Title": "Fix login bug",
    "System.WorkItemType": "Bug",
    "System.State": "Active",
    "System.AssignedTo": "John Doe <john.doe@fabrikam.com>",
    "System.CreatedDate": "2023-01-15T10:00:00Z",
    "System.CreatedBy": "Jane Smith <jane.smith@fabrikam.com>",
    "System.ChangedDate": "2023-01-20T14:30:00Z",
    "System.ChangedBy": "John Doe <john.doe@fabrikam.com>",
    "System.Reason": "Accepted",
    "System.AreaPath": "Fabrikam-Fiber\\Web",
    "System.IterationPath": "Fabrikam-Fiber\\Sprint 1",
    "Microsoft.VSTS.Common.Priority": 1,
    "Microsoft.VSTS.Common.Severity": "2 - High"
  },
  "relations": [
    {
      "rel": "System.LinkTypes.Hierarchy-Reverse",
      "url": "https://dev.azure.com/fabrikam/_apis/wit/workItems/298",
      "attributes": {
        "isLocked": false,
        "name": "Parent"
      }
    }
  ],
  "url": "https://dev.azure.com/fabrikam/_apis/wit/workItems/299"
}
```

**Table options**:
- `ids` (comma-separated integers, **optional**): Specific work item IDs to fetch.
  - If provided: Fetches only specified work items.
  - If omitted: Uses workitem_revisions table to discover all work items incrementally.
- `expand` (string, optional): Expand options - `relations`, `fields`, `links`, or `all`. Default: `relations`.

**Authentication requirements**:
- Requires PAT with `Work Items (read)` scope.


### `workitem_revisions` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/wit/reporting/workitemrevisions?api-version=7.1`

**Key behavior**:
- Returns historical revisions of all work items in a project.
- Each revision represents a snapshot of the work item at a specific point in time.
- Supports incremental ingestion using continuation tokens and watermarks.
- Ideal for discovering all work items and tracking changes over time.
- Includes deleted work items (with `System.IsDeleted` field).
- Append-only ingestion - revisions are immutable.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer | Work item ID. |
| `rev` | integer | Revision number. |
| `workItemId` | integer | Work item ID (duplicate of `id`). |
| `revisedDate` | string (ISO 8601 datetime) or null | When this revision was created. |
| `fields` | struct | Field values at this revision (same structure as workitems). |
| `relations` | array of structs or null | Relations at this revision. |
| `url` | string | API URL for this work item. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |

**Example request** (incremental read):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/reporting/workitemrevisions?api-version=7.1&continuationToken=ABC123&includeDeleted=true"
```

**Example response**:

```json
{
  "values": [
    {
      "id": 299,
      "rev": 1,
      "workItemId": 299,
      "revisedDate": "2023-01-15T10:00:00Z",
      "fields": {
        "System.Id": 299,
        "System.Title": "Fix login bug",
        "System.WorkItemType": "Bug",
        "System.State": "New",
        "System.CreatedDate": "2023-01-15T10:00:00Z"
      }
    },
    {
      "id": 299,
      "rev": 2,
      "workItemId": 299,
      "revisedDate": "2023-01-16T11:00:00Z",
      "fields": {
        "System.Id": 299,
        "System.Title": "Fix login bug",
        "System.WorkItemType": "Bug",
        "System.State": "Active",
        "System.AssignedTo": "John Doe <john.doe@fabrikam.com>"
      }
    }
  ],
  "continuationToken": "XYZ789"
}
```

**Table options**:
- `includeDeleted` (boolean, optional): Include deleted work items. Default: `true`.
- `continuationToken` (string, optional): Token for pagination/incremental reads.
- `$expand` (string, optional): Expand options - `none`, `relations`, `all`. Default: `none`.

**Authentication requirements**:
- Requires PAT with `Work Items (read)` scope.


### `workitem_types` object

**Source endpoint**:  
`GET https://dev.azure.com/{organization}/{project}/_apis/wit/workitemtypes?api-version=7.1`

**Key behavior**:
- Returns work item type definitions for a project.
- Includes field definitions, states, transitions, and rules.
- Snapshot ingestion - type definitions change infrequently.
- Essential metadata for understanding work item schemas.

**High-level schema (connector view)**:

| Column Name | Type | Description |
|------------|------|-------------|
| `name` | string | Work item type name: `Task`, `Bug`, `User Story`, etc. |
| `referenceName` | string | Unique reference name (e.g., `Microsoft.VSTS.WorkItemTypes.Task`). |
| `description` | string or null | Type description. |
| `color` | string | Hex color code for UI display. |
| `icon` | struct | Icon information (id, url). |
| `isDisabled` | boolean | Whether this type is disabled. |
| `xmlForm` | string or null | XML form definition (legacy). |
| `fields` | array of structs | Field definitions for this type. |
| `fieldInstances` | array of structs or null | Field instance definitions. |
| `transitions` | struct or null | State transitions map. |
| `states` | array of structs | Valid states for this type. |
| `url` | string | API URL for this work item type. |
| `organization` | string (connector-derived) | Organization name. |
| `project_name` | string (connector-derived) | Project name. |

**Nested `fields` array item struct**:

| Field | Type | Description |
|-------|------|-------------|
| `referenceName` | string | Field reference name (e.g., `System.Title`). |
| `name` | string | Field display name. |
| `type` | string | Field type: `string`, `integer`, `dateTime`, `html`, etc. |
| `readOnly` | boolean | Whether field is read-only. |
| `required` | boolean | Whether field is required. |
| `defaultValue` | string or null | Default value. |
| `helpText` | string or null | Help text for the field. |

**Nested `states` array item struct**:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | State name: `New`, `Active`, `Resolved`, `Closed`, etc. |
| `color` | string | Hex color code. |
| `category` | string | State category: `Proposed`, `InProgress`, `Resolved`, `Completed`, `Removed`. |

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/workitemtypes?api-version=7.1"
```

**Example response** (single type):

```json
{
  "name": "Bug",
  "referenceName": "Microsoft.VSTS.WorkItemTypes.Bug",
  "description": "Describes a problem or issue",
  "color": "CC293D",
  "icon": {
    "id": "icon_bug",
    "url": "https://dev.azure.com/fabrikam/_apis/wit/workItemIcons/icon_bug"
  },
  "isDisabled": false,
  "fields": [
    {
      "referenceName": "System.Title",
      "name": "Title",
      "type": "string",
      "readOnly": false,
      "required": true
    },
    {
      "referenceName": "System.State",
      "name": "State",
      "type": "string",
      "readOnly": false,
      "required": true,
      "defaultValue": "New"
    }
  ],
  "states": [
    {
      "name": "New",
      "color": "b2b2b2",
      "category": "Proposed"
    },
    {
      "name": "Active",
      "color": "007acc",
      "category": "InProgress"
    },
    {
      "name": "Resolved",
      "color": "ff9d00",
      "category": "Resolved"
    },
    {
      "name": "Closed",
      "color": "339933",
      "category": "Completed"
    }
  ],
  "url": "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/workItemTypes/Bug"
}
```

**Table options**:
- None. Fetches all work item types in the project.

**Authentication requirements**:
- Requires PAT with `Work Items (read)` scope.


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get primary keys for Azure DevOps Git objects.  
Instead, primary keys are defined **statically** based on resource schemas.

| Object | Primary Key Field(s) | Type | Notes |
|--------|---------------------|------|-------|
| `projects` | `id` | string (UUID) | Unique across all projects in an organization. |
| `repositories` | `id` | string (UUID) | Unique across all repositories in an organization. |
| `commits` | `commitId`, `repository_id` | string (SHA-1), string (UUID) | Composite key: commits are unique within a repository but the same commit can exist across multiple repos (forks). |
| `pullrequests` | `pullRequestId`, `repository_id` | integer, string (UUID) | Composite key: pull request IDs are unique within a repository. |
| `pullrequest_threads` | `id`, `pullrequest_id`, `repository_id` | integer, integer, string (UUID) | Composite key: thread IDs are unique within a pull request. |
| `pullrequest_workitems` | `id`, `pullrequest_id`, `repository_id` | string, integer, string (UUID) | Composite key: work item links are unique per PR. |
| `pullrequest_commits` | `commitId`, `pullrequest_id`, `repository_id` | string (SHA-1), integer, string (UUID) | Composite key: commits are scoped to the PR context. |
| `pullrequest_reviewers` | `id`, `pullrequest_id`, `repository_id` | string (UUID), integer, string (UUID) | Composite key: reviewers are unique per PR. |
| `refs` | `name`, `repository_id` | string, string (UUID) | Composite key: ref names (e.g., `refs/heads/main`) are unique within a repository. |
| `pushes` | `pushId`, `repository_id` | integer, string (UUID) | Composite key: push IDs are unique within a repository. |
| `users` | `descriptor` | string (base64-encoded) | Unique immutable identifier for users across the organization. |
| `workitems` | `id` | integer | Unique across all work items in the organization. |
| `workitem_revisions` | `id`, `rev` | integer, integer | Composite key: work item ID + revision number. |
| `workitem_types` | `referenceName`, `project_name` | string, string | Composite key: type reference name is unique within a project. |

**Implementation notes**:
- For `repositories`: Use `id` field directly from the API response.
- For objects scoped to a repository (`commits`, `pullrequests`, `refs`, `pushes`): 
  - The connector must add `repository_id` as a derived field based on the table options.
  - Use composite keys combining the resource's unique identifier with `repository_id`.
  
Example showing primary keys in responses:

**Repositories**:
```json
{
  "id": "5febef5a-833d-4e14-b9c0-14cb638f91e6",
  "name": "AnotherRepository"
}
```
Primary key: `id` = `"5febef5a-833d-4e14-b9c0-14cb638f91e6"`

**Commits**:
```json
{
  "commitId": "be67f8871a4d2c75f13a51c1d3c30ac0d74d4ef4",
  "author": { "name": "John Doe", "date": "2023-01-15T10:00:00Z" }
}
```
Primary key: (`commitId`, `repository_id`) = (`"be67f8871a4d2c75f13a51c1d3c30ac0d74d4ef4"`, `"5febef5a-..."`)

**Pull Requests**:
```json
{
  "pullRequestId": 123,
  "title": "Add feature X"
}
```
Primary key: (`pullRequestId`, `repository_id`) = (`123`, `"5febef5a-..."`)

**Refs**:
```json
{
  "name": "refs/heads/main",
  "objectId": "be67f8871a4d2c75f13a51c1d3c30ac0d74d4ef4"
}
```
Primary key: (`name`, `repository_id`) = (`"refs/heads/main"`, `"5febef5a-..."`)

**Pushes**:
```json
{
  "pushId": 456,
  "date": "2023-01-15T10:30:00Z"
}
```
Primary key: (`pushId`, `repository_id`) = (`456`, `"5febef5a-..."`)


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Planned ingestion type for Azure DevOps Git objects:

| Object | Ingestion Type | Cursor Field | Rationale |
|--------|----------------|--------------|-----------|
| `projects` | `snapshot` | None | Project metadata changes infrequently. No built-in incremental cursor. Full snapshot approach recommended. |
| `repositories` | `snapshot` | None | Repository metadata changes infrequently. No built-in incremental cursor. Full snapshot approach recommended. |
| `commits` | `append` | `author.date` or skip-based | Commits are immutable once created. Use author date for time-based incremental reads or skip-based pagination. |
| `pullrequests` | `cdc` | `closedDate` or `lastMergeCommit.commitId` | Pull requests can be updated (status changes, new reviews, completion). Track by closed date or merge commit for incremental updates. |
| `pullrequest_threads` | `append` | `publishedDate` or `lastUpdatedDate` | New comments are append-only. Thread status can change (use CDC for status tracking). Comments are immutable once posted (edits tracked via lastUpdatedDate). |
| `pullrequest_workitems` | `snapshot` | None | Work item links can be added or removed. Snapshot approach captures all current links. No built-in cursor. |
| `pullrequest_commits` | `snapshot` | None | Commit list can grow as PR is updated (force pushes, new commits). Snapshot approach captures current state. |
| `pullrequest_reviewers` | `cdc` | None | Reviewers can be added/removed, votes can change. CDC tracks all changes. No time-based cursor available. |
| `refs` | `snapshot` | None | Refs (branches/tags) can be created, updated (point to different commits), or deleted. Snapshot approach captures all changes. |
| `pushes` | `append` | `pushId` or `date` | Push events are immutable historical records. Use push ID or date for incremental reads. |
| `users` | `snapshot` | None | User identities can be added or updated. No built-in incremental cursor. Full snapshot captures user additions, profile changes, and status updates. |
| `workitems` | `cdc` | `System.ChangedDate` or `rev` | Work items can be created, updated, or deleted. Use revision number or changed date for incremental tracking. |
| `workitem_revisions` | `append` | `continuationToken` or `revisedDate` | Revisions are immutable historical snapshots. Use continuation tokens for pagination and incremental reads. |
| `workitem_types` | `snapshot` | None | Type definitions change infrequently (usually only with process template changes). Full snapshot approach. |

**Detailed ingestion strategies**:

**For `repositories` (snapshot)**:
- **Primary key**: `id` (string UUID)
- **Cursor field**: Not applicable
- **Strategy**: Full snapshot on each sync; compare with previous to detect changes
- **Deletes**: Detect by absence from current snapshot

**For `commits` (append)**:
- **Primary key**: (`commitId`, `repository_id`)
- **Cursor field**: `author.date` (timestamp)
- **Strategy**: Query commits where `author.date` > last sync timestamp
- **Query parameters**: `searchCriteria.fromDate`, `searchCriteria.toDate`
- **Deletes**: Not applicable (commits are immutable)

**For `pullrequests` (cdc)**:
- **Primary key**: (`pullRequestId`, `repository_id`)
- **Cursor field**: `closedDate` for completed/abandoned PRs
- **Strategy**: 
  - Fetch all active PRs on each sync
  - For completed/abandoned PRs, use `closedDate` > last sync timestamp
  - Track status changes and updates
- **Query parameters**: `searchCriteria.status`, `searchCriteria.targetRefName`
- **Updates**: PR status, reviewers, merge status can change

**For `refs` (snapshot)**:
- **Primary key**: (`name`, `repository_id`)
- **Cursor field**: Not applicable
- **Strategy**: Full snapshot of all refs; compare `objectId` to detect ref updates
- **Deletes**: Detect by absence from current snapshot (deleted branches/tags)

**For `pushes` (append)**:
- **Primary key**: (`pushId`, `repository_id`)
- **Cursor field**: `pushId` (integer, auto-incrementing) or `date`
- **Strategy**: Query pushes where `pushId` > last sync max pushId
- **Query parameters**: `searchCriteria.fromDate`, `searchCriteria.toDate`
- **Deletes**: Not applicable (pushes are immutable historical events)

**For `users` (snapshot)**:
- **Primary key**: `descriptor` (string, base64-encoded immutable identifier)
- **Cursor field**: Not applicable
- **Strategy**: Full snapshot on each sync; compare with previous to detect new users or profile changes
- **Pagination**: Uses continuation tokens via `continuationToken` response header
- **Updates**: Display name, email, and other profile fields can change
- **Deletes**: Users can be removed from organization (detect by absence from snapshot)

**For `pullrequest_threads` (append)**:
- **Primary key**: (`id`, `pullrequest_id`, `repository_id`)
- **Cursor field**: `publishedDate` for new threads, `lastUpdatedDate` for updates
- **Strategy**: 
  - Fetch new threads where `publishedDate` > last sync timestamp
  - For existing threads, check `lastUpdatedDate` to detect new comments or status changes
  - Comments within threads are immutable (edits tracked via `lastUpdatedDate`)
- **Deletes**: Threads marked with `isDeleted` flag rather than being removed

**For `pullrequest_workitems` (snapshot)**:
- **Primary key**: (`id`, `pullrequest_id`, `repository_id`)
- **Cursor field**: Not applicable
- **Strategy**: Full snapshot for each PR; compare with previous to detect added/removed work items
- **Deletes**: Detect by absence from current snapshot (work item unlinked)

**For `pullrequest_commits` (snapshot)**:
- **Primary key**: (`commitId`, `pullrequest_id`, `repository_id`)
- **Cursor field**: Not applicable
- **Strategy**: Full snapshot for each PR; captures current commit list
- **Updates**: Commit list can change if PR is force-pushed or rebased
- **Deletes**: Commits can be removed if PR is rebased or amended

**For `pullrequest_reviewers` (cdc)**:
- **Primary key**: (`id`, `pullrequest_id`, `repository_id`)
- **Cursor field**: Not applicable (no timestamp available)
- **Strategy**: 
  - Fetch all reviewers for active PRs on each sync
  - Track vote changes by comparing `vote` field
  - Detect reviewer additions/removals
  - For completed PRs, fetch once and archive
- **Updates**: Vote status, isRequired flag can change

**For `workitems` (cdc)**:
- **Primary key**: `id` (integer, unique across organization)
- **Cursor field**: `System.ChangedDate` or `rev` (revision number)
- **Strategy**: 
  - First sync: Fetch all work items via workitem_revisions endpoint (gets all IDs)
  - Subsequent syncs: Use workitem_revisions with continuation token to get only changed items
  - Batch fetch work items (up to 200 per request) using discovered IDs
  - Track field changes, state transitions, and assignments
  - Detect deleted work items via `System.IsDeleted` field in revisions
- **Query parameters**: `$expand=relations` to include links
- **Updates**: Any field can change; full work item state is replaced on update

**For `workitem_revisions` (append)**:
- **Primary key**: (`id`, `rev`)
- **Cursor field**: `continuationToken` (for pagination) or `revisedDate` (for time-based incremental)
- **Strategy**: 
  - First sync: Fetch all revisions using continuation tokens until exhausted
  - Subsequent syncs: Use continuation token from last sync to get only new revisions
  - Revisions are immutable - once created, they never change
  - Includes all work items (active and deleted)
  - Store continuation token as watermark for next sync
- **Query parameters**: `includeDeleted=true`, `$expand=none` (relations not needed for discovery)
- **Deletes**: Deleted work items have revisions with `System.IsDeleted=true`

**For `workitem_types` (snapshot)**:
- **Primary key**: (`referenceName`, `project_name`)
- **Cursor field**: Not applicable
- **Strategy**: 
  - Full snapshot on each sync (types rarely change)
  - Compare with previous snapshot to detect type additions, field changes, or state workflow updates
  - Type changes usually only occur when process templates are modified
  - Small dataset (typically 10-20 types per project)
- **Updates**: Field definitions, states, transitions can change with process template updates
- **Deletes**: Types can be disabled (`isDisabled=true`) but not typically removed


## **Read API for Data Retrieval**

### `projects` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/_apis/projects`
- **Base URL**: `https://dev.azure.com`

**Path parameters**:
- `organization` (string, required): Azure DevOps organization name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `stateFilter` | string | no | `wellFormed` | Filter by project state. Options: `all`, `createPending`, `deleted`, `deleting`, `new`, `unchanged`, `wellFormed`. |
| `$top` | integer | no | none | Maximum number of projects to return. |
| `$skip` | integer | no | 0 | Number of projects to skip. |
| `continuationToken` | string | no | none | Token for fetching next page of results. |

**Pagination**: 
- Supports `$top` and `$skip` for basic pagination.
- Returns `x-ms-continuationtoken` header if more results available.
- For large organizations, pagination may be needed.

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/_apis/projects?api-version=7.1"
```

**Example response**:

```json
{
  "count": 2,
  "value": [
    {
      "id": "6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c",
      "name": "Fabrikam-Fiber",
      "description": "Team Foundation Server project",
      "url": "https://dev.azure.com/fabrikam/_apis/projects/6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c",
      "state": "wellFormed",
      "revision": 16,
      "visibility": "private",
      "lastUpdateTime": "2014-10-27T16:51:27.46Z"
    },
    {
      "id": "3b3ae425-0079-421f-9101-bcf15d6df041",
      "name": "Fabrikam-Fiber-Git",
      "description": "Git repository for Fabrikam Fiber",
      "url": "https://dev.azure.com/fabrikam/_apis/projects/3b3ae425-0079-421f-9101-bcf15d6df041",
      "state": "wellFormed",
      "revision": 293015,
      "visibility": "public",
      "lastUpdateTime": "2014-10-27T16:55:00.077Z"
    }
  ]
}
```

**Snapshot strategy**:
- Fetch all accessible projects in each sync.
- Compare with previous snapshot to detect additions, updates, and deletions.
- Project list typically changes infrequently, so full refresh is efficient.

### `repositories` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories`
- **Base URL**: `https://dev.azure.com`

**Path parameters**:
- `organization` (string, required): Azure DevOps organization name.
- `project` (string, required): Project ID or project name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `includeLinks` | boolean | no | false | Include `_links` in response. Recommended: `true`. |
| `includeAllUrls` | boolean | no | false | Include all remote URLs. Recommended: `true`. |
| `includeHidden` | boolean | no | false | Include hidden repositories. |

**Pagination**: Not required (returns all repositories in single response).

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories?api-version=7.1&includeLinks=true&includeAllUrls=true"
```

**Snapshot strategy**:
- Fetch all repositories in each sync.
- Compare with previous snapshot to detect additions, updates, and deletions.
- Alternatively, use full-refresh strategy.


### `commits` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/commits`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `$top` | integer | no | 100 | Maximum number of commits to return (max 10,000). |
| `$skip` | integer | no | 0 | Number of commits to skip for pagination. |
| `searchCriteria.fromDate` | string (ISO 8601) | no | none | Include commits authored after this date. |
| `searchCriteria.toDate` | string (ISO 8601) | no | none | Include commits authored before this date. |
| `searchCriteria.author` | string | no | none | Filter by author name or email. |
| `searchCriteria.itemPath` | string | no | none | Filter by file path. |
| `searchCriteria.fromCommitId` | string (SHA-1) | no | none | Include commits after this commit. |
| `searchCriteria.toCommitId` | string (SHA-1) | no | none | Include commits up to this commit. |

**Pagination**: Use `$top` and `$skip` for pagination (required for large repositories).

**Example request** (incremental read):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/commits?api-version=7.1&\$top=1000&searchCriteria.fromDate=2023-01-01T00:00:00Z"
```

**Append strategy**:
- First sync: Fetch all commits using pagination.
- Subsequent syncs: Use `searchCriteria.fromDate` with last sync's max `author.date`.
- Store `$skip` offset for resume capability if interrupted.


### `pullrequests` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullrequests`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `searchCriteria.status` | string | no | `active` | Filter by status: `active`, `completed`, `abandoned`, or `all`. |
| `searchCriteria.creatorId` | string (UUID) | no | none | Filter by creator user ID. |
| `searchCriteria.reviewerId` | string (UUID) | no | none | Filter by reviewer user ID. |
| `searchCriteria.sourceRefName` | string | no | none | Filter by source branch name. |
| `searchCriteria.targetRefName` | string | no | none | Filter by target branch name. |
| `$top` | integer | no | none | Maximum number of PRs to return. |
| `$skip` | integer | no | 0 | Number of PRs to skip for pagination. |

**Pagination**: Optional (use `$top` and `$skip` if needed).

**Example request** (fetch all PRs):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullrequests?api-version=7.1&searchCriteria.status=all"
```

**CDC strategy**:
- Fetch all active PRs on each sync (status changes tracked).
- For completed/abandoned PRs, use time-based filtering if available.
- Track `lastMergeCommit.commitId` or `closedDate` as cursor.
- Detect status changes by comparing with previous state.


### `pullrequest_threads` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/threads`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.
- `pullRequestId` (integer, required): Pull request ID.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `$skip` | integer | no | 0 | Number of threads to skip for pagination. |
| `$top` | integer | no | none | Maximum number of threads to return. |

**Pagination**: Optional (use `$top` and `$skip` if needed for PRs with many threads).

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/threads?api-version=7.1"
```

**Append/CDC strategy**:
- Fetch threads for all active PRs on each sync.
- For new threads: Use `publishedDate` > last sync timestamp.
- For updates: Check `lastUpdatedDate` to detect new comments or status changes.
- Track thread status changes (`active` → `fixed`, etc.).


### `pullrequest_workitems` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/workitems`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.
- `pullRequestId` (integer, required): Pull request ID.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |

**Pagination**: Not required (returns all linked work items).

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/workitems?api-version=7.1"
```

**Snapshot strategy**:
- Fetch all work items for each PR on each sync.
- Compare with previous state to detect added/removed links.
- Work item links are lightweight (just ID and URL).


### `pullrequest_commits` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/commits`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.
- `pullRequestId` (integer, required): Pull request ID.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `$top` | integer | no | none | Maximum number of commits to return. |
| `$skip` | integer | no | 0 | Number of commits to skip for pagination. |

**Pagination**: Optional (use `$top` and `$skip` if needed for PRs with many commits).

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/commits?api-version=7.1"
```

**Snapshot strategy**:
- Fetch all commits for each PR on each sync.
- Commit list can change if PR is updated (force push, rebase).
- Captures the exact changes being reviewed in the PR.


### `pullrequest_reviewers` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/pullRequests/{pullRequestId}/reviewers`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.
- `pullRequestId` (integer, required): Pull request ID.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |

**Pagination**: Not required (returns all reviewers).

**Example request**:

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pullRequests/123/reviewers?api-version=7.1"
```

**CDC strategy**:
- Fetch all reviewers for each PR on each sync.
- Track vote changes (10, 5, 0, -5, -10).
- Detect reviewer additions/removals.
- For active PRs, votes can change frequently.
- For completed PRs, reviewer state is final.


### `refs` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/refs`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `filter` | string | no | none | Filter by ref name prefix (e.g., `heads/` for branches, `tags/` for tags). |
| `includeLinks` | boolean | no | false | Include `_links` in response. |
| `includeStatuses` | boolean | no | false | Include status checks for refs. |
| `peelTags` | boolean | no | false | For annotated tags, return the commit they point to. |

**Pagination**: Not typically required (ref count is usually small).

**Example request** (list all branches):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/refs?api-version=7.1&filter=heads/"
```

**Snapshot strategy**:
- Fetch all refs on each sync.
- Compare `objectId` (commit SHA) to detect ref updates.
- Detect deletions by absence from current snapshot.


### `pushes` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories/{repositoryId}/pushes`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.
- `repositoryId` (string, required): Repository ID or name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `$top` | integer | no | 100 | Maximum number of pushes to return. |
| `$skip` | integer | no | 0 | Number of pushes to skip for pagination. |
| `searchCriteria.fromDate` | string (ISO 8601) | no | none | Include pushes after this date. |
| `searchCriteria.toDate` | string (ISO 8601) | no | none | Include pushes before this date. |
| `searchCriteria.pusherId` | string (UUID) | no | none | Filter by pusher user ID. |
| `searchCriteria.refName` | string | no | none | Filter by ref name. |
| `searchCriteria.includeRefUpdates` | boolean | no | false | Include ref updates in response. |

**Pagination**: Use `$top` and `$skip` for pagination.

**Example request** (incremental read):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/278d5cd2-584d-4b63-824a-2ba458937249/pushes?api-version=7.1&\$top=1000&searchCriteria.fromDate=2023-01-01T00:00:00Z"
```

**Append strategy**:
- First sync: Fetch all pushes using pagination.
- Subsequent syncs: Use `searchCriteria.fromDate` with last sync's max `date`.
- Push IDs are auto-incrementing; can also use max pushId as cursor.


### `users` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `https://vssps.dev.azure.com/{organization}/_apis/graph/users`
- **Base URL**: Note the different base URL for Graph API (`vssps.dev.azure.com` instead of `dev.azure.com`)

**Path parameters**:
- `organization` (string, required): Organization name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1-preview.1`. |
| `continuationToken` | string | no | none | Token for paginated responses. Obtained from previous response. |

**Response headers**:
- `X-MS-ContinuationToken`: Continuation token for next page (if more results exist).

**Pagination**: Uses continuation tokens. Check response header `X-MS-ContinuationToken` for next page token.

**Example request** (list all users):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://vssps.dev.azure.com/fabrikam/_apis/graph/users?api-version=7.1-preview.1"
```

**Example request** (with continuation token):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://vssps.dev.azure.com/fabrikam/_apis/graph/users?api-version=7.1-preview.1&continuationToken=ABC123..."
```

**Snapshot strategy**:
- First sync: Fetch all users following continuation tokens until exhausted.
- Subsequent syncs: Full fetch again; compare descriptors to detect new/removed users.
- No incremental cursor available - snapshot approach required.

**Important notes**:
- Does not require `project` parameter - operates at organization level.
- Returns all users across the entire organization.
- User `descriptor` is immutable and should be used as the primary key.
- User profile fields (displayName, mailAddress) can change over time.


### `workitems` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/wit/workitems`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `ids` | comma-separated integers | yes | none | Work item IDs to retrieve (max 200 per request). |
| `fields` | comma-separated strings | no | all | Specific fields to return (e.g., `System.Id,System.Title`). |
| `asOf` | string (ISO 8601 datetime) | no | none | Return work items as of a specific date/time. |
| `$expand` | string | no | none | Expand options: `relations`, `fields`, `links`, `all`, `none`. |

**Pagination**: Use batch requests with up to 200 IDs per request. For large datasets, split IDs into multiple batches.

**Example request** (batch get work items):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/workitems?ids=1,2,3,4,5&api-version=7.1&\$expand=relations"
```

**Example request** (specific fields only):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/workitems?ids=299,300,301&fields=System.Id,System.Title,System.State,System.AssignedTo&api-version=7.1"
```

**CDC strategy**:
- Use `workitem_revisions` endpoint to discover changed work items.
- Batch fetch work items by ID (up to 200 per request).
- Track `System.ChangedDate` to identify updates.
- Detect deletions via revisions with `System.IsDeleted=true`.
- For active work items, periodically refresh to capture latest state.


### `workitem_revisions` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/wit/reporting/workitemrevisions`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |
| `continuationToken` | string | no | none | Token for pagination/incremental reads. |
| `includeDeleted` | boolean | no | true | Include deleted work items. |
| `$expand` | string | no | none | Expand options: `none`, `relations`, `all`. |
| `fields` | comma-separated strings | no | all | Specific fields to return. |

**Response format**:
- Returns `values` array with revisions.
- Returns `continuationToken` for next page if more results exist.
- Revisions are ordered chronologically.

**Pagination**: Uses continuation tokens. Store token to resume from last position.

**Example request** (initial read):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/reporting/workitemrevisions?api-version=7.1&includeDeleted=true"
```

**Example request** (incremental read with continuation token):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/reporting/workitemrevisions?api-version=7.1&continuationToken=ABC123XYZ&includeDeleted=true"
```

**Append strategy**:
- First sync: Fetch all revisions following continuation tokens until exhausted.
- Subsequent syncs: Use stored continuation token to fetch only new revisions.
- Store continuation token as watermark for incremental reads.
- Revisions include work item ID and revision number - use to identify changes.
- Extract unique work item IDs from revisions to fetch current state via workitems endpoint.


### `workitem_types` read endpoint

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/wit/workitemtypes`

**Path parameters**:
- `organization` (string, required): Organization name.
- `project` (string, required): Project name or ID.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1`. |

**Pagination**: Not required (returns all work item types in single response).

**Example request** (list all types):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/workitemtypes?api-version=7.1"
```

**Example request** (get specific type):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber/_apis/wit/workitemtypes/Bug?api-version=7.1"
```

**Snapshot strategy**:
- Fetch all work item types on each sync.
- Compare with previous snapshot to detect changes in type definitions.
- Types rarely change (only with process template updates).
- Small dataset - full refresh is efficient.


### General rate limiting considerations

**Rate limits**:
- Azure DevOps Services applies rate limiting based on throughput units (TSTUs) and request volume.
- Typical limits: ~200 requests per user per minute, though this varies by resource type.
- The connector should:
  - Respect `Retry-After` headers on `429 Too Many Requests` responses.
  - Implement exponential backoff for retries.
  - Use bulk endpoints where available (e.g., list all repositories vs. individual fetches).
  - Batch requests efficiently during pagination.
- Official documentation: https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits

**Best practices**:
- Use pagination parameters (`$top`, `$skip`) to control request size and avoid timeouts.
- Leverage date-based filtering (`searchCriteria.fromDate`) for incremental syncs.
- For tables with optional `repository_id`:
  - **Omit `repository_id`** for comprehensive data collection across all repositories (auto-fetch mode)
  - **Provide `repository_id`** for targeted, efficient queries on specific repositories
  - In auto-fetch mode, the connector fetches all repositories first, then iterates
- For PR-related tables (`pullrequest_threads`, `pullrequest_workitems`, `pullrequest_commits`, `pullrequest_reviewers`):
  - These require both `repository_id` and `pullrequest_id` in the API path
  - If both are omitted, the connector first fetches all repositories, then all PRs, then the PR-related data
  - For efficiency, provide at least `repository_id` to limit scope
  - Best performance: provide both `repository_id` and `pullrequest_id` for targeted queries
- Cache repository and PR metadata to avoid redundant API calls.


## **Field Type Mapping**

### General mapping (Azure DevOps JSON → connector logical types)

| Azure DevOps JSON Type | Example Fields | Connector Logical Type | Notes |
|------------------------|----------------|------------------------|-------|
| string (UUID) | `id`, `project.id`, `createdBy.id` | string | GUID/UUID format (e.g., `5febef5a-833d-4e14-b9c0-14cb638f91e6`). Store as string. |
| string (SHA-1 hash) | `commitId`, `objectId`, `treeId`, `newObjectId`, `oldObjectId` | string | 40-character hexadecimal Git SHA-1 hash. Store as string. |
| string | `name`, `defaultBranch`, `remoteUrl`, `title`, `description`, `comment` | string | UTF-8 text. |
| integer (32-bit) | `pullRequestId`, `pushId`, `codeReviewId` | integer | Standard 32-bit signed integer. |
| integer (64-bit) | `size`, `project.revision` | long / integer | Large numeric values. Use 64-bit integer (`LongType` in Spark). |
| boolean | `isDisabled`, `isInMaintenance`, `isFork`, `commentTruncated`, `supportsIterations` | boolean | Standard true/false. |
| string (ISO 8601 datetime) | `date`, `creationDate`, `closedDate`, `author.date`, `committer.date` | timestamp with timezone | UTC timestamps (e.g., `"2023-01-15T10:30:00Z"`). |
| object/struct | `project`, `author`, `committer`, `createdBy`, `pushedBy`, `changeCounts` | struct | Nested records; preserve structure rather than flattening. |
| array | `parents`, `commits`, `refUpdates`, `reviewers`, `workItems`, `statuses` | array | Lists of values or nested objects. |
| nullable fields | Most fields can be null | corresponding type + null | Surface `null` when fields are absent, not empty objects `{}`. |

### Object-specific field types

**Repositories**:
- `id` (string UUID): Repository identifier.
- `size` (long or null): Repository size in bytes; can be null for empty repos.
- `defaultBranch` (string or null): Full Git ref name (e.g., `refs/heads/main`).

**Commits**:
- `commitId` (string SHA-1): Unique 40-character commit hash.
- `parents` (array of strings): Array of parent commit SHA-1 hashes.
- `author.date`, `committer.date` (ISO 8601 string): Timestamps with timezone.
- `treeId` (string SHA-1 or null): Git tree object ID.
- `changeCounts` (struct with `Add`, `Edit`, `Delete` as integers).

**Pull Requests**:
- `pullRequestId` (integer): Numeric PR identifier within repository.
- `status` (string enum): `active`, `completed`, `abandoned`.
- `mergeStatus` (string enum): `succeeded`, `failed`, `conflicts`, `queued`, `notSet`, etc.
- `creationDate`, `closedDate` (ISO 8601 string): Timestamps; `closedDate` is null for active PRs.
- `reviewers` (array of structs): Each reviewer has `id`, `displayName`, `vote` (integer).

**Refs**:
- `name` (string): Full Git reference name (e.g., `refs/heads/main`, `refs/tags/v1.0`).
- `objectId` (string SHA-1): Commit SHA this ref points to.
- `peeledObjectId` (string SHA-1 or null): For annotated tags, the underlying commit.

**Pushes**:
- `pushId` (integer): Numeric push identifier (auto-incrementing within repository).
- `date` (ISO 8601 string): When the push occurred.
- `refUpdates` (array of structs): Each has `name`, `oldObjectId`, `newObjectId`.

**Pull Request Threads**:
- `id` (integer): Thread identifier (unique within pull request).
- `status` (string enum): `active`, `fixed`, `wontFix`, `closed`, `byDesign`, `pending`.
- `comments` (array of structs): All comments in thread; each comment has `id`, `author`, `content`, `publishedDate`.
- `threadContext` (struct or null): For code comments, includes file path and line positions.
- `publishedDate`, `lastUpdatedDate` (ISO 8601 string): Creation and last update timestamps.

**Pull Request Work Items**:
- `id` (string): Work item ID (numeric but returned as string).
- `url` (string): API URL for the work item resource.

**Pull Request Commits**:
- `commitId` (string SHA-1): Commit hash included in the PR.
- `author`, `committer` (struct): Same structure as main commits object.
- `comment` (string): Commit message.

**Pull Request Reviewers**:
- `vote` (integer): Vote status (10=approved, 5=approved with suggestions, 0=no vote, -5=waiting for author, -10=rejected).
- `isRequired` (boolean or null): Whether reviewer is mandatory for PR approval.
- `hasDeclined`, `isFlagged` (boolean or null): Reviewer status flags.
- `votedFor` (array of structs or null): For group reviewers, individual user votes.

**Work Items**:
- `id` (integer): Work item ID (unique across organization).
- `rev` (integer): Revision number (auto-incrementing with each update).
- `fields` (struct): Dynamic field collection - actual fields vary by work item type.
- `System.Title` (string): Work item title.
- `System.WorkItemType` (string): Type name: `Task`, `Bug`, `User Story`, `Feature`, `Epic`, etc.
- `System.State` (string): Current state: `New`, `Active`, `Resolved`, `Closed`, etc.
- `System.AssignedTo` (string or null): Assigned user (format: `Display Name <email>`).
- `System.CreatedDate`, `System.ChangedDate` (ISO 8601 string): Creation and last update timestamps.
- `System.AreaPath`, `System.IterationPath` (string): Hierarchical categorization paths.
- `System.Tags` (string or null): Semicolon-separated tags.
- `Microsoft.VSTS.Scheduling.StoryPoints` (number or null): Estimation in story points.
- `relations` (array of structs): Links to other work items, commits, PRs, external artifacts.

**Work Item Revisions**:
- Same structure as work items, but represents a historical snapshot.
- `revisedDate` (ISO 8601 string or null): When this revision was created.
- `System.IsDeleted` (boolean): Whether work item is deleted (present in revisions of deleted items).

**Work Item Types**:
- `name` (string): Type name (e.g., `Bug`, `Task`).
- `referenceName` (string): Unique reference name (e.g., `Microsoft.VSTS.WorkItemTypes.Bug`).
- `color` (string): Hex color code for UI display (e.g., `CC293D`).
- `fields` (array of structs): Field definitions with type, constraints, and default values.
- `states` (array of structs): Valid states with names, colors, and categories.
- `isDisabled` (boolean): Whether type is disabled.

### Special behaviors and constraints

- **UUIDs**: Store as strings in canonical hyphenated format. Do not parse or convert to numeric types.
- **SHA-1 hashes**: Store as 40-character lowercase hexadecimal strings. These uniquely identify Git commits, trees, and blobs.
- **Timestamps**: Use ISO 8601 format in UTC (e.g., `"2023-01-15T10:30:00Z"`). Ensure timezone handling is correct.
- **Enums as strings**: Fields like `status`, `mergeStatus`, `state` are string enums. Store as strings for flexibility and forward compatibility.
- **Nested structs**: Preserve as nested types rather than flattening. This maintains API structure and simplifies schema evolution.
- **Arrays**: Use array types for multi-valued fields like `parents`, `reviewers`, `commits`.
- **Null handling**: When fields are absent or explicitly null in API responses, surface as `null` rather than empty structures `{}` or empty arrays `[]` unless the field is genuinely an empty collection.
- **Git reference names**: Full reference paths like `refs/heads/main` (branches) or `refs/tags/v1.0` (tags), not short names.
- **Clone URLs**: `remoteUrl` (HTTPS) and `sshUrl` (SSH) are complete Git clone URLs.
- **Identity objects**: User objects (`createdBy`, `author`, `pushedBy`) contain `id` (UUID), `displayName`, `uniqueName` (email), and optional `url`/`imageUrl`.
- **Commit relationships**: `parents` array shows commit ancestry; empty array means initial commit, multiple parents indicate merge commit.


## **Sources and References**

- **Official Azure DevOps REST API documentation v7.1** (highest confidence)
  - Main API reference: https://learn.microsoft.com/en-us/rest/api/azure/devops/?view=azure-devops-rest-7.1
  - Projects API: https://learn.microsoft.com/en-us/rest/api/azure/devops/core/projects?view=azure-devops-rest-7.1
  - Git Repositories API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories?view=azure-devops-rest-7.1
  - Git Commits API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/commits?view=azure-devops-rest-7.1
  - Git Pull Requests API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-requests?view=azure-devops-rest-7.1
  - Git Pull Request Threads API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-threads?view=azure-devops-rest-7.1
  - Git Pull Request Work Items API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-work-items?view=azure-devops-rest-7.1
  - Git Pull Request Commits API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-commits?view=azure-devops-rest-7.1
  - Git Pull Request Reviewers API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-reviewers?view=azure-devops-rest-7.1
  - Git Refs API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/refs?view=azure-devops-rest-7.1
  - Git Pushes API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pushes?view=azure-devops-rest-7.1
  - Graph Users API: https://learn.microsoft.com/en-us/rest/api/azure/devops/graph/users?view=azure-devops-rest-7.1
  - Work Items API: https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/work-items?view=azure-devops-rest-7.1
  - Work Item Revisions API: https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/reporting-work-item-revisions?view=azure-devops-rest-7.1
  - Work Item Types API: https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/work-item-types?view=azure-devops-rest-7.1
  - Authentication: https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/authentication-guidance
  - Rate Limits: https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits
  - API versioning: https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rest-api-versioning

When conflicts arise, **official Azure DevOps documentation** is treated as the source of truth.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/?view=azure-devops-rest-7.1 | 2025-01-06 | High | Base URL structure, API version parameter, authentication methods. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories?view=azure-devops-rest-7.1 | 2025-01-06 | High | Repositories API: List and Get endpoints, query parameters, response schemas, no pagination required. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/commits?view=azure-devops-rest-7.1 | 2025-01-06 | High | Commits API: Get Commits endpoint, pagination with $top/$skip, searchCriteria parameters for date/author filtering, commit schema including author/committer/parents. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-requests?view=azure-devops-rest-7.1 | 2025-01-06 | High | Pull Requests API: Get Pull Requests endpoint, searchCriteria for status/creator/reviewer filtering, PR schema including reviewers/status/merge details. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-threads?view=azure-devops-rest-7.1 | 2025-01-09 | High | Pull Request Threads API: Get Threads endpoint, thread schema including comments/status/threadContext, pagination support. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-work-items?view=azure-devops-rest-7.1 | 2025-01-09 | High | Pull Request Work Items API: Get Work Items endpoint, returns lightweight work item references (id, url). |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-commits?view=azure-devops-rest-7.1 | 2025-01-09 | High | Pull Request Commits API: Get Commits endpoint, returns commits included in PR with pagination support. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-request-reviewers?view=azure-devops-rest-7.1 | 2025-01-09 | High | Pull Request Reviewers API: Get Reviewers endpoint, reviewer schema with vote status/isRequired/flags. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/refs?view=azure-devops-rest-7.1 | 2025-01-06 | High | Refs API: List endpoint, filter parameter for branches/tags, ref schema including name/objectId/creator. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pushes?view=azure-devops-rest-7.1 | 2025-01-06 | High | Pushes API: Get Pushes endpoint, pagination, searchCriteria for date/pusher filtering, push schema including refUpdates and commits. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/graph/users?view=azure-devops-rest-7.1 | 2025-01-08 | High | Graph Users API: List users endpoint, continuation token pagination, user schema including descriptor/displayName/mailAddress/principalName/origin. Uses different base URL (vssps.dev.azure.com). |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/work-items?view=azure-devops-rest-7.1 | 2025-01-09 | High | Work Items API: Get Work Items endpoint, batch retrieval (up to 200 IDs), dynamic fields based on work item type, expand options for relations. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/reporting-work-item-revisions?view=azure-devops-rest-7.1 | 2025-01-09 | High | Work Item Revisions API: Read Reporting Revisions endpoint, continuation token pagination, includes deleted items, immutable historical snapshots. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/work-item-types?view=azure-devops-rest-7.1 | 2025-01-09 | High | Work Item Types API: List and Get endpoints, type definitions with fields/states/transitions, process template metadata. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/authentication-guidance | 2025-01-06 | High | Personal Access Token authentication, header format, Base64 encoding. Required scopes: Code (read) for Git objects, Graph (read) for users, Work Items (read) for work items. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits | 2025-01-06 | High | Rate limiting behavior, typical limits (~200 requests/user/minute), throttling strategy, Retry-After headers. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rest-api-versioning | 2025-01-06 | High | API versioning scheme. Connector uses `api-version=7.1` (stable) to avoid preview version requirements. |

