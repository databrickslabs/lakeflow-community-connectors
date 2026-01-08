# Lakeflow Azure DevOps Community Connector

This documentation describes how to configure and use the **Azure DevOps** Lakeflow community connector to ingest data from the Azure DevOps Services REST API into Databricks.


## Prerequisites

- **Azure DevOps account**: You need an Azure DevOps user or service account with access to the organization and projects you want to read.
- **Personal Access Token (PAT)**:
  - Must be created in Azure DevOps and supplied to the connector as the `personal_access_token` option.
  - Minimum scopes:
    - `Code (read)` - Grants read access to source code, commits, and Git repositories.
- **Network access**: The environment running the connector must be able to reach `https://dev.azure.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection options exposed by the connector.

| Name                     | Type   | Required | Description                                                                                 | Example                            |
|--------------------------|--------|----------|---------------------------------------------------------------------------------------------|------------------------------------|
| `organization`           | string | yes      | Azure DevOps organization name. This is the organization segment in the Azure DevOps URL.  | `my-organization`                  |
| `project`                | string | yes      | Project name or ID. Specifies which Azure DevOps project to read Git data from.           | `my-project`                       |
| `personal_access_token`  | string | yes      | Personal Access Token (PAT) used for authentication with Azure DevOps Services REST API.   | `7tq...qDnpdg1Nitj8JQQJ99BLAC...` |
| `externalOptionsAllowList` | string | yes    | Comma-separated list of allowed table-specific options. Must be set to: `repository_id,status_filter,filter` | `repository_id,status_filter,filter` |

**Important**: The `externalOptionsAllowList` parameter is **required** because some tables (`commits`, `pullrequests`, `refs`, `pushes`) require table-specific configuration options.

### Obtaining the Required Parameters

- **Azure DevOps Organization and Project**:
  1. Sign in to Azure DevOps at `https://dev.azure.com`.
  2. Your organization name appears in the URL: `https://dev.azure.com/{organization}`.
  3. Navigate to the project you want to ingest data from. The project name is visible in the URL and navigation.
  
- **Personal Access Token (PAT)**:
  1. Sign in to your Azure DevOps organization.
  2. Click on your profile icon in the top-right corner and select **Personal access tokens**.
  3. Click **+ New Token**.
  4. Configure the token:
     - **Name**: Give it a descriptive name (e.g., "Lakeflow Connector").
     - **Organization**: Select the organization you want to access.
     - **Expiration**: Set an appropriate expiration date (or use a custom date).
     - **Scopes**: Select **Custom defined** and check **Code (read)** under the Code section.
  5. Click **Create** and copy the generated token immediately. Store it securely as you won't be able to see it again.
  6. Use this token as the `personal_access_token` connection option.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the required connection parameters:
   - `organization`: Your Azure DevOps organization name
   - `project`: Your project name or ID
   - `personal_access_token`: Your PAT for authentication
   - `externalOptionsAllowList`: Set to `repository_id,status_filter,filter` to enable table-specific options

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Azure DevOps connector exposes a **static list** of 5 Git-related tables:

- `repositories` - Git repository metadata
- `commits` - Git commit history
- `pullrequests` - Pull request data
- `refs` - Git references (branches and tags)
- `pushes` - Git push events

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key for each table:

| Table          | Description                                           | Ingestion Type | Primary Key                       | Incremental Cursor (if any) |
|----------------|-------------------------------------------------------|----------------|-----------------------------------|-----------------------------|
| `repositories` | Git repository metadata within an Azure DevOps project | `snapshot`     | `id`                              | n/a                         |
| `commits`      | Git commits across all repositories in the project     | `append`       | `commitId`, `repository_id`       | n/a                         |
| `pullrequests` | Pull requests with merge status and reviewers          | `cdc`          | `pullRequestId`, `repository_id`  | `closedDate`                |
| `refs`         | Git references (branches and tags) per repository      | `snapshot`     | `name`, `repository_id`           | n/a                         |
| `pushes`       | Git push events to repositories                        | `append`       | `pushId`, `repository_id`         | n/a                         |

### Required and optional table options

Each table has different configuration requirements:

| Table          | Required Options | Optional Options | Notes |
|----------------|------------------|------------------|-------|
| `repositories` | None             | None             | Uses connection-level `organization` and `project` |
| `commits`      | None             | `repository_id`  | If `repository_id` omitted: auto-fetches from ALL repos; if provided: fetches from specific repo only |
| `pullrequests` | None             | `repository_id`, `status_filter` | If `repository_id` omitted: auto-fetches from ALL repos; `status_filter`: `active`, `completed`, `abandoned`, or `all` (default: `all`) |
| `refs`         | None             | `repository_id`, `filter` | If `repository_id` omitted: auto-fetches from ALL repos; `filter` for branches (`heads/`) or tags (`tags/`) |
| `pushes`       | None             | `repository_id`  | If `repository_id` omitted: auto-fetches from ALL repos; if provided: fetches from specific repo only |

**Important Notes**:
- **Auto-fetch mode** (repository_id omitted): Connector automatically fetches data from ALL repositories in the project. The `repository_id` field is populated as a foreign key for each record.
- **Targeted mode** (repository_id provided): Connector fetches data only from the specified repository. More efficient for large projects with many repositories.
- To obtain `repository_id` values, first query the `repositories` table and use the `id` field.

### Schema highlights

All table schemas preserve the nested JSON structure from the Azure DevOps API rather than flattening it. Key fields for each table:

#### `repositories` table (16 fields)
- **Identity**: `id` (UUID string), `name`
- **URLs**: `url`, `remoteUrl`, `sshUrl`, `webUrl`
- **Metadata**: `defaultBranch`, `size`, `isDisabled`, `isInMaintenance`, `isFork`
- **Nested structures**: `project`, `parentRepository`, `_links`
- **Connector-derived**: `organization`, `project_name`

#### `commits` table (13 fields)
- **Identity**: `commitId` (SHA-1 hash string)
- **Authorship**: `author` (struct with name, email, date), `committer` (struct)
- **Content**: `comment`, `commentTruncated`, `changeCounts` (struct with Add/Edit/Delete counts)
- **Git metadata**: `treeId`, `parents` (array of parent commit SHAs)
- **URLs**: `url`, `remoteUrl`
- **Connector-derived**: `organization`, `project_name`, `repository_id`

#### `pullrequests` table (21 fields)
- **Identity**: `pullRequestId` (long integer), `codeReviewId`
- **Status**: `status` (active/completed/abandoned), `mergeStatus`
- **Creator**: `createdBy` (struct with user identity)
- **Timestamps**: `creationDate`, `closedDate`
- **Details**: `title`, `description`
- **Branches**: `sourceRefName`, `targetRefName`
- **Merge info**: `mergeId`, `lastMergeSourceCommit`, `lastMergeTargetCommit`, `lastMergeCommit` (all structs)
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`, `repository_id`

#### `refs` table (8 fields)
- **Identity**: `name` (full ref path, e.g., `refs/heads/main`)
- **Target**: `objectId` (commit SHA-1), `peeledObjectId` (for annotated tags)
- **Creator**: `creator` (struct with user identity)
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`, `repository_id`

#### `pushes` table (8 fields)
- **Identity**: `pushId` (long integer)
- **Timestamp**: `date` (ISO 8601 string)
- **Pusher**: `pushedBy` (struct with user identity)
- **Changes**: `refUpdates` (array of structs with name, oldObjectId, newObjectId)
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`, `repository_id`

**Common patterns across all tables**:
- All connector-derived fields (`organization`, `project_name`, and `repository_id` where applicable) are non-nullable strings.
- Nested user identities include `id`, `displayName`, `uniqueName`, `url`, and `imageUrl`.
- All timestamps are stored as ISO 8601 UTC strings; cast to timestamp type in downstream processing if needed.
- Missing or inapplicable nested structures are represented as `null`, not empty objects.

## Data Type Mapping

Azure DevOps JSON fields are mapped to Spark types as follows:

| Azure DevOps JSON Type     | Example Fields                                       | Connector Spark Type             | Notes |
|----------------------------|------------------------------------------------------|----------------------------------|-------|
| string (UUID)              | `id` (repository, project), `commitId`, `mergeId`, `treeId`, `objectId` | string (`StringType`) | UUIDs and SHA-1 hashes are stored as strings. |
| string                     | `name`, `url`, `remoteUrl`, `title`, `comment`, `status`, `sourceRefName` | string (`StringType`) | All text fields including URLs, descriptions, and references. |
| integer (small)            | `Add`, `Edit`, `Delete` (in changeCounts)            | 32-bit integer (`IntegerType`)   | Small counters for file changes. |
| integer (64-bit)           | `size`, `revision`, `pullRequestId`, `codeReviewId`, `pushId` | 64-bit integer (`LongType`) | Large IDs and sizes to prevent overflow. |
| boolean                    | `isDisabled`, `isInMaintenance`, `isFork`, `commentTruncated`, `supportsIterations` | boolean (`BooleanType`) | Standard `true`/`false` values; may be `null` if not applicable. |
| ISO 8601 datetime (string) | `lastUpdateTime`, `date`, `creationDate`, `closedDate` | string in schema              | Stored as UTC strings; cast to timestamp downstream. |
| array of strings           | `parents` (commit SHAs), `validRemoteUrls`           | array of strings (`ArrayType(StringType)`) | Collections of string values. |
| array of objects           | `refUpdates` (in pushes)                             | array of structs (`ArrayType(StructType)`) | Collections of structured objects. |
| object (identity)          | `author`, `committer`, `createdBy`, `pushedBy`, `creator` | struct (`StructType`)      | User/identity objects with id, displayName, email, etc. |
| object (nested data)       | `project`, `parentRepository`, `_links`, `changeCounts`, `lastMergeCommit` | struct (`StructType`) | Nested objects preserved as structs. |
| nullable fields            | `defaultBranch`, `size`, `parentRepository`, `closedDate`, `peeledObjectId` | same as base type + `null` | Missing fields are `null`, not empty objects or strings. |

The connector is designed to:

- **Preserve nested JSON structures** from the Azure DevOps API as Spark structs instead of flattening them.
- **Treat absent nested fields as `null`** rather than empty objects to conform to Lakeflow's expectations.
- **Use `LongType` for IDs and large integers** to prevent overflow (e.g., pullRequestId, pushId, size).
- **Store dates as strings** in ISO 8601 format; cast to timestamp type in downstream transformations if needed.
- **Use arrays for collections** to maintain the structure of multi-valued fields like commit parents or ref updates.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Azure DevOps connector source in your workspace. This will typically place the connector code (e.g., `azure_devops.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py` or a similar entrypoint), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Azure DevOps connector.
- One or more **tables** to ingest.

#### Example 1: Ingest repositories (no table options required)

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "repositories"
        }
      }
    ]
  }
}
```

#### Example 2a: Ingest commits from ALL repositories (auto-fetch)

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "commits"
        }
      }
    ]
  }
}
```

**What happens**: The connector automatically:
1. Fetches all repositories in the project
2. For each repository, fetches commits
3. Combines all commits with `repository_id` populated

#### Example 2b: Ingest commits from a specific repository (targeted)

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "commits",
          "repository_id": "e39fba9d-6cf8-4cfb-a4b9-1714d52d160b"
        }
      }
    ]
  }
}
```

**What happens**: The connector fetches commits only from the specified repository.

#### Example 3: Ingest pull requests with status filter

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "pullrequests",
          "repository_id": "e39fba9d-6cf8-4cfb-a4b9-1714d52d160b",
          "status_filter": "completed"
        }
      }
    ]
  }
}
```

#### Example 4: Ingest only branches (not tags)

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "refs",
          "repository_id": "e39fba9d-6cf8-4cfb-a4b9-1714d52d160b",
          "filter": "heads/"
        }
      }
    ]
  }
}
```

#### Example 5: Ingest multiple objects from the same repository

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "repositories"
        }
      },
      {
        "table": {
          "source_table": "commits",
          "repository_id": "e39fba9d-6cf8-4cfb-a4b9-1714d52d160b"
        }
      },
      {
        "table": {
          "source_table": "pullrequests",
          "repository_id": "e39fba9d-6cf8-4cfb-a4b9-1714d52d160b",
          "status_filter": "all"
        }
      },
      {
        "table": {
          "source_table": "refs",
          "repository_id": "e39fba9d-6cf8-4cfb-a4b9-1714d52d160b"
        }
      },
      {
        "table": {
          "source_table": "pushes",
          "repository_id": "e39fba9d-6cf8-4cfb-a4b9-1714d52d160b"
        }
      }
    ]
  }
}
```

**Configuration notes**:
- `connection_name` must point to the Unity Catalog connection with your `organization`, `project`, `personal_access_token`, and `externalOptionsAllowList` configured.
- For `commits`, `pullrequests`, `refs`, and `pushes` tables, `repository_id` is **optional**:
  - **Omit `repository_id`** to auto-fetch from ALL repositories (convenient, comprehensive)
  - **Provide `repository_id`** to fetch from a specific repository (efficient, targeted)
  - Obtain `repository_id` UUID from the `repositories` table's `id` field
- The `status_filter` option for `pullrequests` accepts: `active`, `completed`, `abandoned`, or `all` (default).
- The `filter` option for `refs` can be `heads/` (branches only), `tags/` (tags only), or omitted (all refs).

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

**Understanding ingestion types**:
- **Snapshot** (`repositories`, `refs`): Full refresh on each sync. All records are fetched and replaced.
- **Append** (`commits`, `pushes`): New records are added incrementally. Historical data is never modified or deleted.
- **CDC** (`pullrequests`): Change Data Capture using the `closedDate` cursor field. Captures new and updated pull requests efficiently.

**API characteristics**:
- `repositories`: Single API call retrieves all repositories in the project
- `commits`, `pushes`: Paginated APIs with 1,000 records per page
- `pullrequests`, `refs`: Single API call per repository (typically small result sets)

#### Best Practices

- **Start small**: 
  - Begin with the `repositories` table to discover available repository IDs.
  - For initial testing, use **targeted mode** (provide `repository_id`) with a single repository.
  - Once validated, switch to **auto-fetch mode** (omit `repository_id`) for comprehensive data collection.
  - Consider using a test project before syncing production data.
  
- **Schedule appropriately based on ingestion type**:
  - **Snapshot tables** (`repositories`, `refs`): Schedule based on change frequency. Daily or weekly syncs are typically sufficient for repository metadata and branch/tag updates.
  - **Append tables** (`commits`, `pushes`): Can be run more frequently (hourly or daily) to capture new commits and pushes without re-fetching historical data.
  - **CDC tables** (`pullrequests`): Run frequently (hourly or multiple times daily) to capture PR status changes efficiently using the cursor field.
  
- **Choose the right mode for multiple repositories**:
  - **Auto-fetch mode** (omit `repository_id`): Best for syncing ALL repositories in a project
    - Single configuration per table
    - Automatically discovers new repositories
    - Makes N+1 API calls (1 for repos + 1 per repo for child data)
  - **Targeted mode** (provide `repository_id`): Best for selective repository syncing
    - Create separate pipeline objects for specific repositories
    - More control over sync schedules per repository
    - Consider staggering sync schedules to distribute API load
    - More efficient when you only need specific repositories
  
- **Monitor API usage**: 
  - Azure DevOps enforces rate limiting based on requests per user and Throughput Units (TSTUs).
  - Typical limits are around 200 requests per user per minute, though this can vary by resource type.
  - Each table per repository typically requires 1-N API calls depending on data volume and pagination.
  - For more details, see the [Azure DevOps Rate Limits documentation](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits).
  
- **Use service accounts**:
  - Create a dedicated service account with a PAT for production pipelines rather than using personal accounts.
  - This provides better auditability and avoids disruption if team members leave.
  - Ensure the service account has `Code (read)` permissions on all target repositories.

#### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`401 Unauthorized`)**:
  - Verify that the `personal_access_token` is correct and not expired or revoked.
  - Check that the token has the `Code (read)` scope enabled.
  - Ensure the account associated with the PAT has access to the specified organization and project.

- **`404 Not Found` errors**:
  - **For connection-level errors**: Verify that the `organization` and `project` names are spelled correctly. Check that they exist and are accessible.
  - **For table-level errors**: Verify that the `repository_id` is correct and that the repository exists in the specified project.
  - Organization and project names are case-sensitive in some contexts.

- **`400 Bad Request` with "preview" version errors**:
  - The connector uses API version `7.1` (stable) to avoid preview version requirements.
  - If you encounter version-related errors, verify that your Azure DevOps instance supports API version 7.1.

- **Empty results when using auto-fetch mode**:
  - If tables like `commits`, `pullrequests`, `refs`, or `pushes` return no data in auto-fetch mode (no `repository_id` provided):
    - Check that repositories exist in the project by querying the `repositories` table first.
    - Some repositories may be empty or have no data for specific tables (e.g., no commits, no PRs).
    - Use targeted mode with a known `repository_id` to verify data exists.

- **`externalOptionsAllowList` configuration errors**:
  - If you receive errors about disallowed table options, ensure the connection parameter `externalOptionsAllowList` is set to: `repository_id,status_filter,filter`
  - This parameter must be configured at connection creation time and cannot be changed for existing connections.

- **Permission denied errors**:
  - Ensure the PAT has the appropriate scopes (`Code (read)` at minimum).
  - Check that the user or service account has read permissions on the project and its repositories.
  - Organization-level policies may restrict API access; consult your Azure DevOps administrator.

- **Rate limiting (`429 Too Many Requests` or throttling errors)**:
  - Reduce the frequency of pipeline runs, especially for append tables with large commit histories.
  - If syncing multiple repositories, stagger the schedules to distribute API usage over time.
  - The `commits` and `pushes` tables use pagination and may require multiple API calls for repositories with extensive history.
  - Review the [Azure DevOps Rate Limits documentation](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits) for guidance.

- **No data returned for `commits` or `pushes`**:
  - These tables use pagination. If a repository is new or empty, they may return zero records, which is expected.
  - Verify the `repository_id` is correct and that the repository has commit/push history.

- **`pullrequests` missing expected records**:
  - The `status_filter` option controls which pull requests are returned.
  - Default is `all`, but you can filter by `active`, `completed`, or `abandoned`.
  - For CDC ingestion, the `closedDate` cursor field is used, which only applies to closed PRs.

- **Empty or null nested fields**:
  - Nested structures like `parentRepository`, `author`, `creator`, `pushedBy` may be `null` if not applicable.
  - Empty repositories may have `null` values for `size`, `defaultBranch`, or other optional fields.
  - This is expected behavior and aligns with the Azure DevOps API response structure.

## References

### Connector Files
- Connector implementation: `sources/azure_devops/azure_devops.py`
- Connector API documentation and schemas: `sources/azure_devops/azure_devops_api_doc.md`
- Test suite: `sources/azure_devops/test/test_azure_devops_lakeflow_connect.py`

### Official Azure DevOps REST API Documentation (v7.1)
- [Azure DevOps REST API Overview](https://learn.microsoft.com/en-us/rest/api/azure/devops/?view=azure-devops-rest-7.1)
- [Git Repositories API](https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories?view=azure-devops-rest-7.1)
- [Git Commits API](https://learn.microsoft.com/en-us/rest/api/azure/devops/git/commits?view=azure-devops-rest-7.1)
- [Git Pull Requests API](https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pull-requests?view=azure-devops-rest-7.1)
- [Git Refs API](https://learn.microsoft.com/en-us/rest/api/azure/devops/git/refs?view=azure-devops-rest-7.1)
- [Git Pushes API](https://learn.microsoft.com/en-us/rest/api/azure/devops/git/pushes?view=azure-devops-rest-7.1)

### Azure DevOps Guides
- [Authentication with Personal Access Tokens](https://learn.microsoft.com/en-us/azure/devops/organizations/accounts/use-personal-access-tokens-to-authenticate)
- [Rate Limits and Throttling](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits)
- [Azure DevOps Services REST API Versioning](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rest-api-versioning)

