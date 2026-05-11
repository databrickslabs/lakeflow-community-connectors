# Lakeflow Azure DevOps Community Connector

This documentation describes how to configure and use the **Azure DevOps** Lakeflow community connector to ingest data from the Azure DevOps Services REST API into Databricks.


## Prerequisites

- **Azure DevOps account**: You need an Azure DevOps user or service account with access to the organization and projects you want to read.
- **Personal Access Token (PAT)**:
  - Must be created in Azure DevOps and supplied to the connector as the `personal_access_token` option.
  - Minimum scopes:
    - `Code (read)` - Grants read access to source code, commits, Git repositories, and pull request details.
    - `Graph (read)` - Required if ingesting the `users` table from the Graph Users API.
    - `Project and Team (read)` - Required for the `projects` table.
    - `Work Items (read)` - Required if ingesting work item tables (`workitems`, `workitem_revisions`, `workitem_types`).
- **Network access**: The environment running the connector must be able to reach `https://dev.azure.com` and `https://vssps.dev.azure.com` (for the `users` table).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection options exposed by the connector.

| Name                     | Type   | Required | Description                                                                                 | Example                            |
|--------------------------|--------|----------|---------------------------------------------------------------------------------------------|------------------------------------|
| `organization`           | string | yes      | Azure DevOps organization name. This is the organization segment in the Azure DevOps URL.  | `my-organization`                  |
| `project`                | string | no       | Project name or ID. If provided, scopes Git and Work Item operations to this project. If omitted, tables can fetch from all projects. | `my-project`                       |
| `personal_access_token`  | string | yes      | Personal Access Token (PAT) used for authentication with Azure DevOps Services REST API.   | `7tq...qDnpdg1Nitj8JQQJ99BLAC...` |
| `externalOptionsAllowList` | string | yes    | Comma-separated list of allowed table-specific options. Must be set to: `project,repository_id,pullrequest_id,status_filter,filter,ids` | `project,repository_id,pullrequest_id,status_filter,filter,ids` |

**Important**: The `externalOptionsAllowList` parameter is **required** because tables support table-specific configuration options (`project`, `repository_id`, `pullrequest_id`, `status_filter`, `filter`, `ids`).

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
     - **Scopes**: Select **Custom defined** and check:
       - **Code (read)** under the Code section - Required for Git operations and pull request details
       - **Graph (read)** under the Graph section - Required if you plan to ingest the `users` table
       - **Project and Team (read)** under the Project and Team section - Required for the `projects` table
       - **Work Items (read)** under the Work Items section - Required if you plan to ingest work item tables
  5. Click **Create** and copy the generated token immediately. Store it securely as you won't be able to see it again.
  6. Use this token as the `personal_access_token` connection option.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Provide the required connection parameters:
   - `organization`: Your Azure DevOps organization name
   - `project`: Your project name or ID (optional)
   - `personal_access_token`: Your PAT for authentication
   - `externalOptionsAllowList`: Set to `project,repository_id,pullrequest_id,status_filter,filter,ids` to enable table-specific options

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Azure DevOps connector exposes a **static list** of 14 tables:

**Organization tables:**
- `projects` - Projects within an organization

**Git-related tables:**
- `repositories` - Git repository metadata
- `commits` - Git commit history
- `pullrequests` - Pull request data
- `refs` - Git references (branches and tags)
- `pushes` - Git push events

**Pull request detail tables:**
- `pullrequest_threads` - Comments and discussion threads on pull requests
- `pullrequest_workitems` - Work items linked to pull requests
- `pullrequest_commits` - Commits included in pull requests
- `pullrequest_reviewers` - Detailed reviewer information for pull requests

**Identity tables:**
- `users` - User profile and identity information from Azure DevOps

**Work item tracking tables:**
- `workitems` - Work items (tasks, bugs, user stories, features, epics)
- `workitem_revisions` - Historical changes to work items
- `workitem_types` - Work item type definitions and fields

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key for each table:

| Table                   | Description                                           | Ingestion Type | Primary Key                                      | Incremental Cursor (if any) |
|-------------------------|-------------------------------------------------------|----------------|--------------------------------------------------|-----------------------------|
| `projects`              | Projects within an Azure DevOps organization           | `snapshot`     | `id`                                             | n/a                         |
| `repositories`          | Git repository metadata within an Azure DevOps project | `snapshot`     | `id`                                             | n/a                         |
| `commits`               | Git commits across all repositories                    | `append`       | `commitId`, `repository_id`                      | n/a                         |
| `pullrequests`          | Pull requests with merge status and reviewers          | `cdc`          | `pullRequestId`, `repository_id`                 | `closedDate`                |
| `pullrequest_threads`   | Comment threads and discussions on pull requests       | `append`       | `id`, `pullrequest_id`, `repository_id`          | `publishedDate`             |
| `pullrequest_workitems` | Work items linked to pull requests                     | `snapshot`     | `id`, `pullrequest_id`, `repository_id`          | n/a                         |
| `pullrequest_commits`   | Commits included in pull requests                      | `snapshot`     | `commitId`, `pullrequest_id`, `repository_id`    | n/a                         |
| `pullrequest_reviewers` | Detailed reviewer information for pull requests        | `snapshot`     | `id`, `pullrequest_id`, `repository_id`          | n/a                         |
| `refs`                  | Git references (branches and tags) per repository      | `snapshot`     | `name`, `repository_id`                          | n/a                         |
| `pushes`                | Git push events to repositories                        | `append`       | `pushId`, `repository_id`                        | n/a                         |
| `users`                 | User profiles and identities in the organization       | `snapshot`     | `descriptor`                                     | n/a                         |
| `workitems`             | Work items (tasks, bugs, user stories, etc.)           | `cdc`          | `id`                                             | `rev`                       |
| `workitem_revisions`    | Historical changes to work items                       | `append`       | `id`, `rev`                                      | n/a                         |
| `workitem_types`        | Work item type definitions and fields                  | `snapshot`     | `referenceName`, `project_name`                  | n/a                         |

### Required and optional table options

Each table has different configuration requirements:

| Table                   | Required Options | Optional Options | Notes |
|-------------------------|------------------|------------------|-------|
| `projects`              | None             | None             | Uses connection-level `organization`; fetches all projects in the organization |
| `repositories`          | None             | `project`        | If `project` omitted: uses connection-level project OR fetches from ALL projects |
| `commits`               | None             | `project`, `repository_id`  | Project and repository auto-discovery supported |
| `pullrequests`          | None             | `project`, `repository_id`, `status_filter` | Project and repository auto-discovery supported; `status_filter`: `active`, `completed`, `abandoned`, or `all` (default: `all`) |
| `pullrequest_threads`   | None             | `project`, `repository_id`, `pullrequest_id` | Supports auto-discovery: fetches from all PRs if `pullrequest_id` omitted |
| `pullrequest_workitems` | None             | `project`, `repository_id`, `pullrequest_id` | Supports auto-discovery: fetches from all PRs if `pullrequest_id` omitted |
| `pullrequest_commits`   | None             | `project`, `repository_id`, `pullrequest_id` | Supports auto-discovery: fetches from all PRs if `pullrequest_id` omitted |
| `pullrequest_reviewers` | None             | `project`, `repository_id`, `pullrequest_id` | Supports auto-discovery: fetches from all PRs if `pullrequest_id` omitted |
| `refs`                  | None             | `project`, `repository_id`, `filter` | Project and repository auto-discovery supported; `filter` for branches (`heads/`) or tags (`tags/`) |
| `pushes`                | None             | `project`, `repository_id`  | Project and repository auto-discovery supported |
| `users`                 | None             | None             | Uses connection-level `organization`; fetches all users in the organization |
| `workitems`             | None             | `project`, `ids` | Optional `ids` for specific work items; omit to fetch all work items via WIQL |
| `workitem_revisions`    | None             | `project`        | Fetches all revisions incrementally; project auto-discovery supported |
| `workitem_types`        | None             | `project`        | Fetches all type definitions; project auto-discovery supported |

**Important Notes**:

**Project Discovery**:
- **Connection-level project**: If `project` is specified at connection level, all Git and Work Item tables default to that project
- **Table-level project**: Use `project` in table_options to override connection-level or fetch from a specific project
- **Auto-discovery**: If no project specified anywhere, tables automatically fetch from ALL projects in the organization
- To discover available projects, query the `projects` table first

**Repository Discovery**:
- **Auto-fetch mode** (repository_id omitted): Connector automatically fetches data from ALL repositories. The `repository_id` field is populated as a foreign key for each record.
- **Targeted mode** (repository_id provided): Connector fetches data only from the specified repository. More efficient for large projects with many repositories.
- To obtain `repository_id` values, query the `repositories` table and use the `id` field.

**Pull Request Discovery**:
- **Auto-fetch mode** (pullrequest_id omitted): Connector automatically fetches data from ALL pull requests in the specified or auto-discovered repositories.
- **Targeted mode** (pullrequest_id provided): Connector fetches data only for the specified pull request. Most efficient for specific PR analysis.
- To obtain `pullrequest_id` values, query the `pullrequests` table and use the `pullRequestId` field.

### Schema highlights

All table schemas preserve the nested JSON structure from the Azure DevOps API rather than flattening it. Key fields for each table:

#### `projects` table (9 fields)
- **Identity**: `id` (UUID string), `name`
- **Details**: `description`, `state`, `visibility`
- **Metadata**: `revision`, `lastUpdateTime`, `url`
- **Connector-derived**: `organization`

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

#### `pullrequests` table (20 fields)
- **Identity**: `pullRequestId` (long integer), `codeReviewId`
- **Status**: `status` (active/completed/abandoned), `mergeStatus`
- **Creator**: `createdBy` (struct with user identity)
- **Timestamps**: `creationDate`, `closedDate`
- **Details**: `title`, `description`
- **Branches**: `sourceRefName`, `targetRefName`
- **Merge info**: `mergeId`, `lastMergeSourceCommit`, `lastMergeTargetCommit`, `lastMergeCommit` (all structs)
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`, `repository_id`

#### `pullrequest_threads` table (11 fields)
- **Identity**: `id` (long integer)
- **Timestamps**: `publishedDate`, `lastUpdatedDate`
- **Content**: `comments` (array of comment structs with author, content, dates)
- **Status**: `status` (active/fixed/wontFix/closed/byDesign/pending)
- **Context**: `threadContext` (struct with file path and line positions for code comments)
- **Flags**: `isDeleted`
- **Connector-derived**: `organization`, `project_name`, `repository_id`, `pullrequest_id`

#### `pullrequest_workitems` table (6 fields)
- **Identity**: `id` (string, work item ID)
- **Reference**: `url` (API URL to work item)
- **Connector-derived**: `organization`, `project_name`, `repository_id`, `pullrequest_id`

#### `pullrequest_commits` table (10 fields)
- **Identity**: `commitId` (SHA-1 hash string)
- **Authorship**: `author`, `committer` (structs with name, email, date)
- **Content**: `comment`, `commentTruncated`
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`, `repository_id`, `pullrequest_id`

#### `pullrequest_reviewers` table (14 fields)
- **Identity**: `id` (UUID string), `displayName`, `uniqueName`
- **Vote**: `vote` (integer: 10=approved, 5=approved with suggestions, 0=no vote, -5=waiting for author, -10=rejected)
- **Status**: `hasDeclined`, `isFlagged`, `isRequired`
- **URLs**: `reviewerUrl`, `url`, `imageUrl`
- **Connector-derived**: `organization`, `project_name`, `repository_id`, `pullrequest_id`

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

#### `users` table (12 fields)
- **Identity**: `descriptor` (unique string identifier), `principalName` (email or UPN)
- **Display info**: `displayName`, `mailAddress`
- **Origin**: `origin` (source of identity: aad, vsts, etc.), `originId` (external ID)
- **Type**: `subjectKind` (user, group, etc.), `domain`
- **Metadata**: `directoryAlias`, `url`
- **Links**: `_links` (struct with avatar and other profile links)
- **Connector-derived**: `organization`

#### `workitems` table (7 fields)
- **Identity**: `id` (long integer), `rev` (revision number)
- **Content**: `fields` (JSON string with dynamic fields based on work item type)
- **Relationships**: `relations` (array of structs with related items)
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`

#### `workitem_revisions` table (8 fields)
- **Identity**: `id` (long integer), `rev` (revision number), `workItemId`
- **Timestamp**: `revisedDate`
- **Content**: `fields` (JSON string with field values at this revision)
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`

#### `workitem_types` table (10 fields)
- **Identity**: `name`, `referenceName`
- **Appearance**: `color`, `icon` (struct with id and url)
- **Metadata**: `description`, `isDisabled`
- **Schema**: `fields` (array of field definition structs), `states` (array of state structs)
- **URLs**: `url`
- **Connector-derived**: `organization`, `project_name`

**Common patterns across all tables**:
- All connector-derived fields (`organization`, `project_name`, `repository_id`, `pullrequest_id` where applicable) are non-nullable strings or integers.
- Nested user identities include `id`, `displayName`, `uniqueName`, `url`, and `imageUrl`.
- All timestamps are stored as ISO 8601 UTC strings; cast to timestamp type in downstream processing if needed.
- Missing or inapplicable nested structures are represented as `null`, not empty objects.

## Data Type Mapping

Azure DevOps JSON fields are mapped to Spark types as follows:

| Azure DevOps JSON Type     | Example Fields                                       | Connector Spark Type             | Notes |
|----------------------------|------------------------------------------------------|----------------------------------|-------|
| string (UUID)              | `id` (repository, project), `descriptor`, `originId` | string (`StringType`) | UUIDs are stored as strings. |
| string (SHA-1)             | `commitId`, `mergeId`, `treeId`, `objectId` | string (`StringType`) | SHA-1 hashes stored as 40-character hex strings. |
| string                     | `name`, `url`, `remoteUrl`, `title`, `comment`, `status`, `sourceRefName` | string (`StringType`) | All text fields including URLs, descriptions, and references. |
| integer (64-bit)           | `id`, `rev`, `size`, `revision`, `pullRequestId`, `codeReviewId`, `pushId`, `vote` | 64-bit integer (`LongType`) | All IDs and large integers to prevent overflow. |
| boolean                    | `isDisabled`, `isInMaintenance`, `isFork`, `commentTruncated`, `supportsIterations`, `isDeleted` | boolean (`BooleanType`) | Standard `true`/`false` values; may be `null` if not applicable. |
| ISO 8601 datetime (string) | `lastUpdateTime`, `date`, `creationDate`, `closedDate`, `publishedDate`, `revisedDate` | string in schema              | Stored as UTC strings; cast to timestamp downstream. |
| array of strings           | `parents` (commit SHAs)                              | array of strings (`ArrayType(StringType)`) | Collections of string values. |
| array of objects           | `refUpdates`, `comments`, `fields`, `states`, `relations` | array of structs (`ArrayType(StructType)`) | Collections of structured objects. |
| object (identity)          | `author`, `committer`, `createdBy`, `pushedBy`, `creator` | struct (`StructType`)      | User/identity objects with id, displayName, email, etc. |
| object (nested data)       | `project`, `parentRepository`, `_links`, `changeCounts`, `threadContext`, `icon` | struct (`StructType`) | Nested objects preserved as structs. |
| JSON string                | `fields` (in workitems, workitem_revisions)          | string (`StringType`)            | Dynamic work item fields stored as JSON string for flexibility. |
| nullable fields            | `defaultBranch`, `size`, `parentRepository`, `closedDate`, `peeledObjectId` | same as base type + `null` | Missing fields are `null`, not empty objects or strings. |

The connector is designed to:

- **Preserve nested JSON structures** from the Azure DevOps API as Spark structs instead of flattening them.
- **Treat absent nested fields as `null`** rather than empty objects to conform to Lakeflow's expectations.
- **Use `LongType` for all IDs and integers** to prevent overflow (e.g., pullRequestId, pushId, size, vote, id, rev).
- **Store dates as strings** in ISO 8601 format; cast to timestamp type in downstream transformations if needed.
- **Use arrays for collections** to maintain the structure of multi-valued fields like commit parents or ref updates.
- **Store dynamic work item fields as JSON strings** to handle varying schemas across work item types and process templates.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Azure DevOps connector source in your workspace. This will typically place the connector code (e.g., `azure_devops.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py` or a similar entrypoint), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Azure DevOps connector.
- One or more **tables** to ingest.

#### Example 1: Ingest projects table

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "projects"
        }
      }
    ]
  }
}
```

#### Example 2: Ingest repositories from a specific project

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "repositories",
          "table_options": {
            "project": "MyProject"
          }
        }
      }
    ]
  }
}
```

#### Example 3: Ingest commits from all repositories (auto-discovery)

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

#### Example 4: Ingest commits from a specific repository

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "commits",
          "table_options": {
            "project": "MyProject",
            "repository_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
          }
        }
      }
    ]
  }
}
```

#### Example 5: Ingest pull request threads from a specific PR

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "pullrequest_threads",
          "table_options": {
            "project": "MyProject",
            "repository_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "pullrequest_id": "123"
          }
        }
      }
    ]
  }
}
```

#### Example 6: Ingest all pull request threads (auto-discovery)

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "pullrequest_threads"
        }
      }
    ]
  }
}
```

#### Example 7: Ingest users table

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "users"
        }
      }
    ]
  }
}
```

#### Example 8: Ingest specific work items

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "workitems",
          "table_options": {
            "project": "MyProject",
            "ids": "1,2,3,4,5"
          }
        }
      }
    ]
  }
}
```

#### Example 9: Ingest all work item revisions (for discovery)

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "workitem_revisions"
        }
      }
    ]
  }
}
```

#### Example 10: Ingest work item types

```json
{
  "pipeline_spec": {
    "connection_name": "azure_devops_connection",
    "object": [
      {
        "table": {
          "source_table": "workitem_types",
          "table_options": {
            "project": "MyProject"
          }
        }
      }
    ]
  }
}
```

### Step 3: Run Your Pipeline

Execute your Lakeflow pipeline using the standard Lakeflow workflow. The connector will authenticate with Azure DevOps using the provided PAT and fetch data according to your configuration.

## Troubleshooting

### Authentication Errors (401 Unauthorized)

**Problem**: API calls return 401 Unauthorized errors.

**Possible causes**:
1. **Invalid PAT**: The Personal Access Token may have expired or been revoked.
2. **Insufficient scopes**: The PAT doesn't have the required permissions.
   - Git tables require `Code (read)`
   - Users table requires `Graph (read)`
   - Projects table requires `Project and Team (read)`
   - Work item tables require `Work Items (read)`
3. **Incorrect organization/project**: The organization or project name in the connection parameters may be wrong.

**Solution**:
- Verify your PAT is still valid in Azure DevOps (User Settings > Personal Access Tokens).
- Regenerate the PAT with all required scopes if necessary.
- Double-check the organization and project names match your Azure DevOps setup.

### Empty Results / No Data

**Problem**: The connector runs successfully but returns no data.

**Possible causes**:
1. **Empty project/repository**: The specified project or repository may not contain any data.
2. **Incorrect repository_id**: The provided repository_id may not exist or may be typed incorrectly.
3. **Permission issues**: Your PAT may not have access to the specified project or repository.
4. **Status filter**: For pullrequests, the default status filter may exclude the PRs you're looking for.

**Solution**:
- Query the `projects` table first to verify project names and IDs.
- Query the `repositories` table to verify repository IDs before querying repository-specific tables.
- Try omitting the `repository_id` to use auto-discovery mode and see if data appears.
- For pullrequests, try setting `status_filter` to `all` to include completed and abandoned PRs.
- For work items, use `workitem_revisions` to discover available work item IDs first.

### Schema Validation Errors

**Problem**: Schema validation fails during pipeline execution.

**Possible causes**:
1. **API version mismatch**: Azure DevOps may have changed the API response structure.
2. **Null fields**: Some expected fields may be null in the API response.

**Solution**:
- The connector uses API version 7.1 for stability. Verify you're using the latest connector code.
- All schemas are designed to handle null values gracefully.
- If schema errors persist, check the Azure DevOps API documentation for changes.

### Rate Limiting

**Problem**: API calls are being throttled or rate-limited.

**Symptoms**: Slow ingestion, intermittent failures, or 429 Too Many Requests errors.

**Solution**:
- Azure DevOps Services has rate limits (~200 requests per user per minute).
- For large projects with many repositories or pull requests, consider:
  - Using targeted queries with specific `repository_id` or `pullrequest_id` instead of auto-discovery
  - Splitting large ingestion jobs across multiple pipelines
  - Adding delays between pipeline runs
- The connector respects `Retry-After` headers and implements exponential backoff automatically.

### Project or Repository Not Found (404)

**Problem**: API returns 404 errors for specific projects or repositories.

**Possible causes**:
1. **Incorrect names/IDs**: Typo in project or repository name/ID.
2. **No project specified**: Some Git tables require a project context.
3. **Permissions**: Your PAT may not have access to the specified resources.

**Solution**:
- Verify project and repository names/IDs are correct (they are case-sensitive).
- For repository-specific tables, ensure you've specified a valid project (either at connection level or in table_options).
- Check your PAT permissions in Azure DevOps to ensure you have access to the resources.
- Use the `projects` and `repositories` tables to discover available resources.

### Work Item Queries Failing

**Problem**: Work item tables return errors or no data.

**Possible causes**:
1. **Missing PAT scope**: The PAT doesn't have `Work Items (read)` permission.
2. **Missing IDs**: The `workitems` table requires explicit work item IDs.
3. **Project required**: Work items are project-scoped.

**Solution**:
- Regenerate your PAT with `Work Items (read)` scope included.
- For the `workitems` table, always provide the `ids` parameter with comma-separated work item IDs.
- Use the `workitem_revisions` table first to discover all available work item IDs.
- Ensure a project is specified (connection-level or table-level) for work item queries.

## References

- [Azure DevOps Services REST API Reference (v7.1)](https://learn.microsoft.com/en-us/rest/api/azure/devops/?view=azure-devops-rest-7.1)
- [Azure DevOps Git API](https://learn.microsoft.com/en-us/rest/api/azure/devops/git/?view=azure-devops-rest-7.1)
- [Azure DevOps Graph API](https://learn.microsoft.com/en-us/rest/api/azure/devops/graph/?view=azure-devops-rest-7.1)
- [Azure DevOps Work Item Tracking API](https://learn.microsoft.com/en-us/rest/api/azure/devops/wit/?view=azure-devops-rest-7.1)
- [Azure DevOps Authentication](https://learn.microsoft.com/en-us/azure/devops/integrate/get-started/authentication/authentication-guidance)
- [Azure DevOps Rate Limits](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits)
- [Lakeflow Community Connectors Documentation](https://docs.databricks.com/en/lakehouse-connect/)

##  Connector Information

- **Source**: Azure DevOps Services (https://dev.azure.com)
- **Supported Objects**: 14 tables (projects, Git repositories and history, pull request details, users, work items)
- **API Version**: 7.1 (stable)
- **Authentication**: Personal Access Token (PAT)
- **Supported Ingestion Types**: snapshot, append, cdc
- **Auto-discovery**: Projects, repositories, and pull requests

