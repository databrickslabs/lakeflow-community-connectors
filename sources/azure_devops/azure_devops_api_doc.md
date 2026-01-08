# **Azure DevOps Git API Documentation**

## **Authorization**

- **Chosen method**: Personal Access Token (PAT) for the Azure DevOps REST API v7.1.
- **Base URL**: `https://dev.azure.com/{organization}`
- **Auth placement**:
  - HTTP header: `Authorization: Basic <base64_encoded_pat>`
  - The PAT must be Base64-encoded in the format `:{pat}` (empty username, colon, then the PAT).
  - Alternatively, HTTP Basic authentication with empty username and PAT as password.
- **Required scopes for read-only access to Git repositories**:
  - `Code (read)` - Grants read access to source code, commits, and Git repositories.
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
| `repositories` | Git repositories within a project | `GET /{organization}/{project}/_apis/git/repositories` | `snapshot` |
| `commits` | Git commits across repositories | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/commits` | `append` |
| `pullrequests` | Pull requests across repositories | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pullrequests` | `cdc` |
| `refs` | Git references (branches and tags) | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/refs` | `snapshot` |
| `pushes` | Git push events to repositories | `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}/pushes` | `append` |

**Connector scope**:
- Supports 5 Git objects from Azure DevOps REST API v7.1.
- All objects require `organization` and `project` connection parameters.
- Objects except `repositories` support `repository_id` as an **optional** table option:
  - **If provided**: Fetches data from specific repository only
  - **If omitted**: Auto-fetches data from ALL repositories in the project

High-level notes:
- **Repositories**: Repository metadata including configuration, URLs, and project information.
- **Commits**: Individual Git commits with author, committer, message, tree ID, and parent commits.
- **Pull Requests**: Pull request metadata including source/target branches, status, reviewers, and completion details.
- **Refs**: Git references (branches and tags) with names, commit IDs, and creators.
- **Pushes**: Push events with pusher information, commit ranges, and ref updates.


## **Object Schema**

### General notes

- Azure DevOps provides a well-defined JSON schema for resources via its REST API documentation.
- For the connector, we define **tabular schemas** per object, derived from the JSON representation.
- Nested JSON objects (e.g., `project`, `_links`) are modeled as **nested structures** rather than being fully flattened.

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


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get primary keys for Azure DevOps Git objects.  
Instead, primary keys are defined **statically** based on resource schemas.

| Object | Primary Key Field(s) | Type | Notes |
|--------|---------------------|------|-------|
| `repositories` | `id` | string (UUID) | Unique across all repositories in an organization. |
| `commits` | `commitId`, `repository_id` | string (SHA-1), string (UUID) | Composite key: commits are unique within a repository but the same commit can exist across multiple repos (forks). |
| `pullrequests` | `pullRequestId`, `repository_id` | integer, string (UUID) | Composite key: pull request IDs are unique within a repository. |
| `refs` | `name`, `repository_id` | string, string (UUID) | Composite key: ref names (e.g., `refs/heads/main`) are unique within a repository. |
| `pushes` | `pushId`, `repository_id` | integer, string (UUID) | Composite key: push IDs are unique within a repository. |

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
| `repositories` | `snapshot` | None | Repository metadata changes infrequently. No built-in incremental cursor. Full snapshot approach recommended. |
| `commits` | `append` | `author.date` or skip-based | Commits are immutable once created. Use author date for time-based incremental reads or skip-based pagination. |
| `pullrequests` | `cdc` | `closedDate` or `lastMergeCommit.commitId` | Pull requests can be updated (status changes, new reviews, completion). Track by closed date or merge commit for incremental updates. |
| `refs` | `snapshot` | None | Refs (branches/tags) can be created, updated (point to different commits), or deleted. Snapshot approach captures all changes. |
| `pushes` | `append` | `pushId` or `date` | Push events are immutable historical records. Use push ID or date for incremental reads. |

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


## **Read API for Data Retrieval**

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
- Cache repository metadata to avoid redundant repository list calls.


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
  - Git Repositories API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories?view=azure-devops-rest-7.1
  - Git Commits API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/commits?view=azure-devops-rest-7.1
  - Git Pull Requests API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-requests?view=azure-devops-rest-7.1
  - Git Refs API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/refs?view=azure-devops-rest-7.1
  - Git Pushes API: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pushes?view=azure-devops-rest-7.1
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
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/refs?view=azure-devops-rest-7.1 | 2025-01-06 | High | Refs API: List endpoint, filter parameter for branches/tags, ref schema including name/objectId/creator. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pushes?view=azure-devops-rest-7.1 | 2025-01-06 | High | Pushes API: Get Pushes endpoint, pagination, searchCriteria for date/pusher filtering, push schema including refUpdates and commits. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/authentication-guidance | 2025-01-06 | High | Personal Access Token authentication, header format, Base64 encoding. Required scope: Code (read). |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits | 2025-01-06 | High | Rate limiting behavior, typical limits (~200 requests/user/minute), throttling strategy, Retry-After headers. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rest-api-versioning | 2025-01-06 | High | API versioning scheme. Connector uses `api-version=7.1` (stable) to avoid preview version requirements. |

