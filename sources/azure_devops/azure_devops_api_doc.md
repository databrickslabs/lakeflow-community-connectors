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

**Connector scope for initial implementation**:
- Step 1 focuses on the `repositories` object only, as requested.
- This provides metadata about all Git repositories in an Azure DevOps project.

High-level notes:
- **Repositories**: Contains comprehensive metadata about Git repositories including name, ID, URLs, default branch, project information, and links to related resources (commits, refs, pull requests, items, pushes).
- Additional objects (commits, pull requests, pushes, refs, items) can be added in future iterations but are out of scope for this initial documentation.


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


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get the primary key for the `repositories` object.  
Instead, the primary key is defined **statically** based on the resource schema.

- **Primary key for `repositories`**: `id`  
  - Type: string (UUID/GUID format)  
  - Property: Unique across all repositories in an Azure DevOps organization.

The connector will:
- Read the `id` field from each repository record returned by `GET /{organization}/{project}/_apis/git/repositories`.
- Use it as the immutable primary key for identifying unique repositories.

Example showing primary key in response:

```json
{
  "id": "5febef5a-833d-4e14-b9c0-14cb638f91e6",
  "name": "AnotherRepository",
  "url": "https://dev.azure.com/fabrikam/_apis/git/repositories/5febef5a-833d-4e14-b9c0-14cb638f91e6",
  "project": {
    "id": "6ce954b1-ce1f-45d1-b94d-e6bf2464ba2c",
    "name": "Fabrikam-Fiber-Git"
  },
  "defaultBranch": "refs/heads/master"
}
```

The `id` field (`"5febef5a-833d-4e14-b9c0-14cb638f91e6"`) is the primary key.


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Planned ingestion type for Azure DevOps Git objects:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `repositories` | `snapshot` | Repository metadata changes infrequently (creation, name changes, project moves, deletion). There is no built-in incremental cursor or `updated_at` timestamp in the API response. The recommended approach is to perform a full snapshot of all repositories in a project on each sync and compare against the previous snapshot to detect additions, modifications, or removals. |

For `repositories`:
- **Primary key**: `id` (string UUID)
- **Cursor field**: Not applicable (snapshot ingestion)
- **Sort order**: Not applicable (API returns repositories in arbitrary order; connector can optionally sort by `id` or `name` for consistency)
- **Deletes**: Deleted repositories will no longer appear in the list response. The connector should detect missing repositories by comparing the current snapshot with the previous snapshot and mark them as deleted in the target system, or use a full-refresh strategy.


## **Read API for Data Retrieval**

### Primary read endpoint for `repositories`

- **HTTP method**: `GET`
- **Endpoint**: `/{organization}/{project}/_apis/git/repositories`
- **Base URL**: `https://dev.azure.com`

**Path parameters**:
- `organization` (string, required): Azure DevOps organization name.
- `project` (string, required): Project ID or project name.

**Query parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `api-version` | string | yes | none | API version. Use `7.1` for this connector. |
| `includeLinks` | boolean | no | false | Whether to include `_links` in the response. Recommended to set to `true` for complete metadata. |
| `includeAllUrls` | boolean | no | false | Whether to include all remote URLs. Recommended to set to `true`. |
| `includeHidden` | boolean | no | false | Whether to include hidden repositories (typically system or deleted repos). |

**No pagination required**:
- The list repositories endpoint returns all repositories in a single response (no pagination).
- For organizations with a very large number of repositories (hundreds or thousands), the API still returns all repositories in the `value` array.
- There is no `continuationToken`, `top`, or `skip` parameter for this endpoint.

**Example request** (list all repositories in a project):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories?api-version=7.1&includeLinks=true&includeAllUrls=true"
```

**Snapshot strategy**:
- On each sync run, the connector should:
  - Call `GET /{organization}/{project}/_apis/git/repositories?api-version=7.1` to retrieve the current list of all repositories.
  - Store the full list as a snapshot.
  - Compare with the previous snapshot (if any) to detect:
    - **New repositories**: Repositories with `id` values not present in the previous snapshot.
    - **Updated repositories**: Repositories with the same `id` but different field values (e.g., `name`, `defaultBranch`, `size`).
    - **Deleted repositories**: Repositories present in the previous snapshot but missing from the current response.
  - Alternatively, use a full-refresh strategy where the target table is fully replaced on each sync.

**Handling deletes**:
- Azure DevOps does not provide a dedicated API to list deleted repositories or a "soft delete" flag in the repository object.
- The connector must detect deletions by:
  - Tracking which `id` values were present in the previous sync.
  - Identifying `id` values that are missing from the current sync.
  - Marking those repositories as deleted in the target system, or removing them if using a full-refresh strategy.

**Alternative API for single repository**:
- To retrieve a single repository by ID or name:
  - `GET /{organization}/{project}/_apis/git/repositories/{repositoryId}?api-version=7.1`
  - Where `{repositoryId}` can be either the repository GUID or the repository name.
- This is useful for validation or fetching details for a specific repository but is not recommended for bulk ingestion (use the list endpoint instead).

**Example request** (get a single repository):

```bash
curl -u :{PERSONAL_ACCESS_TOKEN} \
  -H "Accept: application/json" \
  "https://dev.azure.com/fabrikam/Fabrikam-Fiber-Git/_apis/git/repositories/AnotherRepository?api-version=7.1"
```

**Rate limits**:
- Azure DevOps Services applies rate limiting based on throughput units and request volume.
- Typical limits: ~200 requests per user per minute for most APIs, though this can vary.
- The connector should:
  - Respect `Retry-After` headers if a `429 Too Many Requests` response is received.
  - Implement exponential backoff for retries.
  - Avoid making excessive calls (e.g., fetching repositories individually in a loop when a bulk list endpoint is available).
- Official documentation: https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits


## **Field Type Mapping**

### General mapping (Azure DevOps JSON → connector logical types)

| Azure DevOps JSON Type | Example Fields | Connector Logical Type | Notes |
|------------------------|----------------|------------------------|-------|
| string (UUID) | `id`, `project.id` | string | GUID/UUID format (e.g., `5febef5a-833d-4e14-b9c0-14cb638f91e6`). Should be stored as string, not parsed. |
| string | `name`, `defaultBranch`, `remoteUrl`, `sshUrl`, `webUrl`, `url` | string | UTF-8 text. |
| integer (64-bit) | `size`, `project.revision` | long / integer | Repository size in bytes. For Spark-based connectors, prefer 64-bit integer (`LongType`). |
| boolean | `isDisabled`, `isInMaintenance`, `isFork` | boolean | Standard true/false. |
| string (ISO 8601 datetime) | `project.lastUpdateTime` | timestamp with timezone | Stored as UTC timestamps; parsing must respect timezone `Z`. |
| object | `project`, `parentRepository`, `_links` | struct | Represented as nested records instead of flattened columns. |
| nullable fields | `defaultBranch`, `size`, `parentRepository`, `project.lastUpdateTime`, `project.visibility` | corresponding type + null | When fields are absent or null, the connector should surface `null`, not `{}`. |

### Special behaviors and constraints

- `id` fields (repository ID, project ID) are UUIDs/GUIDs and should be stored as **strings** in their canonical hyphenated format (e.g., `5febef5a-833d-4e14-b9c0-14cb638f91e6`). Do not parse or convert to numeric types.
- `state` (project state) is effectively an enum but represented as a string; connector may choose to preserve as string for flexibility. Known values include `wellFormed`, `createPending`, `deleting`, `new`, `deleted`.
- `visibility` (project visibility) is also an enum string, typically `private` or `public`.
- Timestamp fields use ISO 8601 in UTC (e.g., `"2023-01-15T10:30:00Z"`); parsing must be robust to this format.
- Nested structs (`project`, `parentRepository`, `_links`) should not be flattened in the connector implementation; instead, they should be represented as nested types.
- The `_links` object contains HAL-style hypermedia links; each link is a struct with an `href` field (string URL).
- `defaultBranch` uses Git ref naming format (e.g., `refs/heads/main`, `refs/heads/master`). It is a full reference path, not just a branch name.
- `remoteUrl` and `sshUrl` are clone URLs that can be used with Git clients.
- `size` is in bytes and can be null for empty or newly created repositories.


## **Sources and References**

- **Official Azure DevOps REST API documentation** (highest confidence)
  - Main API reference: https://learn.microsoft.com/en-us/rest/api/azure/devops/?view=azure-devops-rest-7.2
  - Git Repositories - List: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories/list?view=azure-devops-rest-7.2
  - Git Repositories - Get Repository: https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories/get-repository?view=azure-devops-rest-7.2
  - Authentication: https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/authentication-guidance
  - Rate Limits: https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits
  - API versioning: https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rest-api-versioning

When conflicts arise, **official Azure DevOps documentation** is treated as the source of truth.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/?view=azure-devops-rest-7.1 | 2025-01-03 | High | Base URL structure, API version parameter, authentication methods. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories/list?view=azure-devops-rest-7.1 | 2025-01-03 | High | List repositories endpoint, query parameters, response schema, no pagination. |
| Official Docs | https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories/get-repository?view=azure-devops-rest-7.1 | 2025-01-03 | High | Get single repository endpoint, path parameters, response schema. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/authentication-guidance | 2025-01-03 | High | Personal Access Token authentication, header format, Base64 encoding. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits | 2025-01-03 | High | Rate limiting behavior, typical limits (~200 requests/user/minute), throttling strategy. |
| Official Docs | https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rest-api-versioning | 2025-01-03 | High | API versioning scheme. Connector uses `api-version=7.1` (stable) instead of 7.2 to avoid preview version requirements. |
| Web Search | Aggregated from multiple Azure DevOps REST API pages | 2025-01-03 | High | Repository object fields, project nested structure, `_links` HAL format, example requests/responses. |

