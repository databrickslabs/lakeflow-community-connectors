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
| `project`                | string | yes      | Project name or ID. Specifies which Azure DevOps project to read repositories from.       | `my-project`                       |
| `personal_access_token`  | string | yes      | Personal Access Token (PAT) used for authentication with Azure DevOps Services REST API.   | `7tq...qDnpdg1Nitj8JQQJ99BLAC...` |

This connector does **not** require table-specific options, so `externalOptionsAllowList` does not need to be included as a connection parameter.

> **Note**: All required configuration is provided at the connection level. Individual tables do not require additional options.

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
3. Provide the required connection parameters: `organization`, `project`, and `personal_access_token`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Azure DevOps connector currently exposes a **static list** of tables:

- `repositories`

Additional objects (such as commits, pull requests, pushes, refs, items) may be added in future iterations.

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key for each table:

| Table          | Description                                           | Ingestion Type | Primary Key           | Incremental Cursor (if any) |
|----------------|-------------------------------------------------------|----------------|-----------------------|-----------------------------|
| `repositories` | Git repository metadata within an Azure DevOps project | `snapshot`     | `id` (string UUID)    | n/a                         |

### Required and optional table options

The `repositories` table does **not** require any table-specific options. All necessary configuration (`organization` and `project`) is provided at the connection level.

### Schema highlights

The `repositories` table schema aligns with the Azure DevOps Git Repositories REST API and includes:

- **Identity fields**:
  - `id` (string, UUID format): Unique identifier for the repository.
  - `name` (string): Repository name.
  
- **URLs and access**:
  - `url` (string): API URL for the repository resource.
  - `remoteUrl` (string): HTTPS clone URL.
  - `sshUrl` (string): SSH clone URL.
  - `webUrl` (string): Web URL for browsing the repository in Azure DevOps.
  
- **Repository metadata**:
  - `defaultBranch` (string or null): Full reference name of the default branch (e.g., `refs/heads/main`).
  - `size` (long integer or null): Repository size in bytes (may be null for empty repositories).
  - `isDisabled` (boolean or null): Whether the repository is disabled.
  - `isInMaintenance` (boolean or null): Whether the repository is in maintenance mode.
  - `isFork` (boolean or null): Whether the repository is a fork.
  
- **Nested structures**:
  - `project` (struct): Contains project metadata (`id`, `name`, `url`, `state`, `revision`, `visibility`, `lastUpdateTime`).
  - `parentRepository` (struct or null): Reference to the parent repository if this is a fork (includes `id`, `name`, `url`, and nested `project`).
  - `_links` (struct): HAL-style hypermedia links to related resources such as `self`, `project`, `web`, `ssh`, `commits`, `refs`, `pullRequests`, `items`, and `pushes`.
  
- **Connector-derived fields**:
  - `organization` (string): The organization name used to retrieve this repository (from connection config).
  - `project_name` (string): The project name used to retrieve this repository (from connection config).

Full schemas are defined by the connector and preserve the nested JSON structure from the Azure DevOps API rather than flattening it.

## Data Type Mapping

Azure DevOps JSON fields are mapped to Spark types as follows:

| Azure DevOps JSON Type     | Example Fields                                       | Connector Spark Type             | Notes |
|----------------------------|------------------------------------------------------|----------------------------------|-------|
| string (UUID)              | `id` (repository, project)                           | string (`StringType`)            | UUIDs are stored as strings in standard GUID format. |
| string                     | `name`, `url`, `remoteUrl`, `sshUrl`, `webUrl`, `defaultBranch`, `state`, `visibility` | string (`StringType`) | All text fields including URLs and references. |
| integer (64-bit)           | `size`, `revision`                                   | 64-bit integer (`LongType`)      | Repository size in bytes and project revision numbers. |
| boolean                    | `isDisabled`, `isInMaintenance`, `isFork`            | boolean (`BooleanType`)          | Standard `true`/`false` values; may be `null` if not applicable. |
| ISO 8601 datetime (string) | `lastUpdateTime`                                     | string in schema                 | Stored as UTC strings; downstream processing can cast to timestamp. |
| object                     | `project`, `parentRepository`, `_links`              | struct (`StructType`)            | Nested objects are preserved as structs instead of being flattened. |
| nullable fields            | `defaultBranch`, `size`, `parentRepository`, `isDisabled`, `isInMaintenance`, `isFork` | same as base type + `null` | Missing or inapplicable fields are surfaced as `null`, not empty objects. |

The connector is designed to:

- Preserve nested JSON structures from the Azure DevOps API as Spark structs.
- Treat absent or inapplicable nested fields as `None`/`null` to conform to Lakeflow's expectations.
- Use `LongType` for large numeric values (such as repository size) to avoid overflow.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Azure DevOps connector source in your workspace. This will typically place the connector code (e.g., `azure_devops.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g., `ingestion_pipeline.py` or a similar entrypoint), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Azure DevOps connector.
- One or more **tables** to ingest.

Example `pipeline_spec` snippet for the `repositories` table:

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

- `connection_name` must point to the Unity Catalog connection configured with your Azure DevOps `organization`, `project`, and `personal_access_token`.
- For the `repositories` table:
  - `source_table` must be set to `"repositories"`.
  - No additional table options are required.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow). 

- The `repositories` table uses **snapshot ingestion**, meaning it performs a full refresh on each sync.
- All Git repositories in the configured project will be retrieved in a single API call.

#### Best Practices

- **Start small**: 
  - Begin with a single project to validate configuration and data shape.
  - Consider starting with a test project before syncing production repositories.
  
- **Schedule appropriately**: 
  - Since this is a snapshot ingestion, schedule syncs based on how frequently your repository metadata changes.
  - Daily or less frequent syncs are typically sufficient for repository metadata.
  
- **Monitor API usage**: 
  - Azure DevOps enforces rate limiting based on requests per user and Throughput Units (TSTUs).
  - Typical limits are around 200 requests per user per minute, though this can vary by resource.
  - For more details, see the [Azure DevOps Rate Limits documentation](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits).
  
- **Use service accounts**:
  - Consider creating a dedicated service account with a PAT for production pipelines rather than using personal accounts.
  - This provides better auditability and avoids disruption if team members leave.

#### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`401 Unauthorized`)**:
  - Verify that the `personal_access_token` is correct and not expired or revoked.
  - Check that the token has the `Code (read)` scope enabled.
  - Ensure the account associated with the PAT has access to the specified organization and project.

- **`404 Not Found` errors**:
  - Verify that the `organization` and `project` names are spelled correctly.
  - Check that the organization and project exist and are accessible to the account.
  - Organization and project names are case-sensitive in some contexts.

- **`400 Bad Request` with "preview" version errors**:
  - The connector uses API version `7.1` (stable) to avoid preview version requirements.
  - If you encounter version-related errors, verify that your Azure DevOps instance supports API version 7.1.

- **Permission denied errors**:
  - Ensure the PAT has the appropriate scopes (`Code (read)` at minimum).
  - Check that the user or service account has read permissions on the project and its repositories.
  - Organization-level policies may restrict API access; consult your Azure DevOps administrator.

- **Rate limiting (`429 Too Many Requests` or throttling errors)**:
  - Reduce the frequency of pipeline runs.
  - If syncing multiple projects, stagger the schedules to distribute API usage.
  - Review the [Azure DevOps Rate Limits documentation](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits) for guidance.

- **Empty or null nested fields**:
  - Some fields like `parentRepository` will be `null` if not applicable (e.g., non-forked repositories).
  - Empty repositories may have `null` values for `size` or `defaultBranch`.
  - This is expected behavior and aligns with the Azure DevOps API response.

## References

- Connector implementation: `sources/azure_devops/azure_devops.py`
- Connector API documentation and schemas: `sources/azure_devops/azure_devops_api_doc.md`
- Official Azure DevOps REST API documentation:
  - [Azure DevOps REST API Reference (v7.2)](https://learn.microsoft.com/en-us/rest/api/azure/devops/?view=azure-devops-rest-7.2)
  - [Git Repositories API](https://learn.microsoft.com/en-us/rest/api/azure/devops/git/repositories?view=azure-devops-rest-7.1)
  - [Authentication with Personal Access Tokens](https://learn.microsoft.com/en-us/azure/devops/organizations/accounts/use-personal-access-tokens-to-authenticate)
  - [Rate Limits](https://learn.microsoft.com/en-us/azure/devops/integrate/concepts/rate-limits)

