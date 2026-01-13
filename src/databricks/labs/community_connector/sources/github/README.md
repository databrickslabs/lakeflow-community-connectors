# Lakeflow GitHub Community Connector

This documentation describes how to configure and use the **GitHub** Lakeflow community connector to ingest data from the GitHub REST API v3 into Databricks.


## Prerequisites

- **GitHub account**: You need a GitHub user or service account with access to the repositories and organizations you want to read.
- **Personal Access Token (PAT)**:
  - Must be created in GitHub and supplied to the connector as the `token` option.
  - Minimum scopes:
    - For public repositories only: `public_repo`
    - For public + private repositories: `repo`
- **Network access**: The environment running the connector must be able to reach `https://api.github.com`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection options exposed by the connector.

| Name      | Type   | Required | Description                                                                                 | Example                            |
|-----------|--------|----------|---------------------------------------------------------------------------------------------|------------------------------------|
| `token`   | string | yes      | GitHub Personal Access Token used for authentication.                                       | `ghp_xxx...`                       |
| `base_url`| string | no       | Base URL for the GitHub API. Override for GitHub Enterprise Server if needed; otherwise defaults to `https://api.github.com`. | `https://github.mycompany.com/api/v3` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector requires table-specific options, so this parameter must be set. | `owner,repo,state,start_date,per_page,max_pages_per_batch,lookback_seconds,org,pull_number` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`owner,repo,state,start_date,per_page,max_pages_per_batch,lookback_seconds,org,pull_number`

> **Note**: Table-specific options such as `owner`, `repo`, or `org` are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **GitHub Personal Access Token (PAT)**:
  1. Sign in to GitHub.
  2. Navigate to **Settings → Developer settings → Personal access tokens**.
  3. Create a **fine-grained** or **classic** token (depending on your org policy).
  4. Grant at least the scopes required for the repositories you will read:
     - `public_repo` for public repos only, or
     - `repo` for both public and private repos.
  5. Copy the generated token and store it securely. Use this as the `token` connection option.
- **GitHub Enterprise Server** (optional):
  - If you are using GitHub Enterprise, set `base_url` to your instance API base, e.g. `https://github.mycompany.com/api/v3`.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `owner,repo,state,start_date,per_page,max_pages_per_batch,lookback_seconds,org,pull_number` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The GitHub connector exposes a **static list** of tables:

- `issues`
- `repositories`
- `pull_requests`
- `comments`
- `commits`
- `assignees`
- `branches`
- `collaborators`
- `organizations`
- `teams`
- `users`
- `reviews`

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key for each table:

| Table           | Description                                           | Ingestion Type | Primary Key                                           | Incremental Cursor (if any) |
|-----------------|-------------------------------------------------------|----------------|-------------------------------------------------------|------------------------------|
| `issues`        | Issues (and PRs exposed as issues) per repository     | `cdc`          | `id` (64-bit integer)                                 | `updated_at`                 |
| `repositories`  | Repository metadata for users/orgs                   | `snapshot`     | `id` (64-bit integer)                                 | n/a                          |
| `pull_requests` | Pull requests per repository                         | `cdc`          | `id` (64-bit integer)                                 | `updated_at`                 |
| `comments`      | Issue comments per repository                        | `cdc`          | `id` (64-bit integer)                                 | `updated_at`                 |
| `commits`       | Commits per repository                               | `append`       | `sha` (commit SHA)                                    | n/a (append-only)           |
| `assignees`     | Assignable users for a repository                    | `snapshot`     | `["repository_owner", "repository_name", "id"]`       | n/a                          |
| `branches`      | Branch list per repository                           | `snapshot`     | `["repository_owner", "repository_name", "name"]`     | n/a                          |
| `collaborators` | Collaborators and permissions per repository         | `snapshot`     | `["repository_owner", "repository_name", "id"]`       | n/a                          |
| `organizations` | Organizations visible to the authenticated account   | `snapshot`     | `id` (64-bit integer)                                 | n/a                          |
| `teams`         | Teams visible to the authenticated account/org       | `snapshot`     | `id` (64-bit integer)                                 | n/a                          |
| `users`         | Authenticated user profile / user metadata           | `snapshot`     | `id` (64-bit integer)                                 | n/a                          |
| `reviews`       | Pull request reviews per repository                  | `append`       | `id` (64-bit integer)                                 | n/a (append-only)           |

### Required and optional table options

Table-specific options are passed via the pipeline spec under `table` in `objects`. The most important options per table are:

- **Common options for repository-scoped tables** (`issues`, `pull_requests`, `comments`, `commits`, `assignees`, `branches`, `collaborators`, `reviews`):
  - `owner` (string, required): Repository owner (user or organization).
  - `repo` (string, required): Repository name.
  - `state` (string, optional, where applicable): e.g. `"open"`, `"closed"`, or `"all"` for issues/PRs. Defaults to `"all"` in the implementation.
  - `per_page` (integer, optional): Page size for GitHub pagination. Defaults to `100` (max).
  - `max_pages_per_batch` (integer, optional): Safety limit on pages per `read_table` call. Defaults to `50`.
  - `start_date` (ISO 8601 string, optional; for `cdc` tables): Initial cursor used when there is no stored offset yet.
  - `lookback_seconds` (integer, optional; for `cdc` tables): Lookback window when computing the next cursor to handle late updates. Defaults to `300`.
- **`repositories`**:
  - `owner` (string, optional): User/organization login.
  - `org` (string, optional): Organization login.
  - Exactly **one** of `owner` or `org` should be provided; the implementation enforces that they are not both set at the same time.
- **`reviews`**:
  - `owner` / `repo` as above.
  - `pull_number` (integer, optional): If provided, restricts the read to a specific pull request; if omitted, the connector will iterate through PRs and combine reviews into a single logical table.

For metadata tables (`users`, `organizations`, `teams`), no additional table options are required in the initial implementation.

### Schema highlights

Full schemas are defined by the connector and align with the GitHub API documentation:

- `issues`:
  - Uses `id` and `number` plus connector-derived `repository_owner` / `repository_name`.
  - Includes nested structures for `user`, `assignee`, `assignees`, `labels`, `milestone`, `pull_request`, and `reactions`.
  - `updated_at` is the incremental cursor.
- `repositories`:
  - Exposes common repository metadata (`name`, `full_name`, visibility flags, counts).
  - Adds connector-derived `repository_owner` and `repository_name`.
  - Includes nested `owner`, `permissions`, `license`, and `template_repository` structs.
- `pull_requests`, `comments`, `commits`, `users`, `organizations`, `teams`, `assignees`, `collaborators`, `branches`, `reviews`:
  - Follow the high-level schemas described in `github_api_doc.md` and preserve nested GitHub JSON objects as Spark `StructType` / arrays where applicable.

You usually do not need to customize the schema; it is static and driven by the connector implementation.

## Data Type Mapping

GitHub JSON fields are mapped to logical types as follows (and then to Spark types in the implementation):

| GitHub JSON Type            | Example Fields                                | Connector Logical Type                  | Notes |
|-----------------------------|-----------------------------------------------|-----------------------------------------|-------|
| integer (32/64-bit)         | `id`, `number`, `comments`, counts           | 64-bit integer (`LongType`)             | All numeric IDs are stored as `LongType` to avoid overflow. |
| string                      | `title`, `body`, `state`, `author_association` | string (`StringType`)                 | Supports long markdown bodies. |
| boolean                     | `locked`, `site_admin`, `default`            | boolean (`BooleanType`)                | Standard `true`/`false` values. |
| ISO 8601 datetime (string)  | `created_at`, `updated_at`, `closed_at`      | string in schema; parsed as timestamps downstream | Stored as UTC strings; downstream processing can cast to timestamp. |
| object                      | `user`, `assignee`, `milestone`, `pull_request`, `license`, `permissions` | struct (`StructType`) | Nested objects are preserved instead of flattened. |
| array                       | `labels`, `assignees`, `topics`              | array of primitive or struct types      | Arrays are preserved as nested collections. |
| nullable fields             | `body`, `closed_at`, `milestone`, `assignee` | same as base type + `null`             | Missing nested objects are surfaced as `null`, not `{}`. |

The connector is designed to:

- Prefer `LongType` for all identifier fields (issues, comments, repos, users, orgs, teams, etc.).
- Preserve nested JSON structures instead of flattening them into separate tables.
- Treat absent nested fields as `None`/`null` to conform to Lakeflow’s expectations.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the GitHub connector source in your workspace. This will typically place the connector code (for example, `github.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g. `ingestion_pipeline.py` or a similar entrypoint), you will configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this GitHub connector.
- One or more **tables** to ingest, each with optional `table_options`.

Example `pipeline_spec` snippet for a single repository:

```json
{
  "pipeline_spec": {
    "connection_name": "github_connection",
    "object": [
      {
        "table": {
          "source_table": "issues",
          "owner": "my-org-or-user",
          "repo": "my-repo",
          "state": "all",
          "start_date": "2024-01-01T00:00:00Z"
        }
      },
      {
        "table": {
          "source_table": "repositories",
          "owner": "my-org-or-user"
        }
      },
      {
        "table": {
          "source_table": "commits",
          "owner": "my-org-or-user",
          "repo": "my-repo",
          "start_date": "2024-01-01T00:00:00Z"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your GitHub `token` (and optional `base_url`).
- For each `table`:
  - `source_table` must be one of the supported table names listed above.
  - Table options such as `owner`, `repo`, `state`, `start_date`, `per_page`, and `max_pages_per_batch` are passed directly to the connector and used to control how data is read.

You can ingest additional tables (e.g. `pull_requests`, `comments`, `assignees`, `branches`, `collaborators`, `reviews`) by adding more `table` entries with the appropriate options.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow). For incremental tables:

- On the **first run**, either:
  - Omit `start_date` to backfill all data (may be heavy for long-lived repos), or
  - Set `start_date` to a recent cutoff to limit history.
- On **subsequent runs**, the connector uses the stored `cursor` (based on `updated_at` for `cdc` tables) plus `lookback_seconds` to pick up late updates safely.

#### Best Practices

- **Start small**:
  - Begin with a single repository and one or two tables (e.g. `issues`, `repositories`) to validate configuration and data shape.
- **Use incremental sync where possible**:
  - For `issues`, `pull_requests`, and `comments`, rely on the `cdc` pattern with `updated_at` as the cursor to minimize API calls.
- **Tune page size and batch limits**:
  - Use `per_page=100` (the default) for efficiency, and adjust `max_pages_per_batch` if you need to limit runtime or API usage.
- **Respect rate limits**:
  - GitHub enforces rate limits per token; consider staggering syncs or splitting repositories across tokens if you encounter rate limiting.

#### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`401` / `403`)**:
  - Verify that the `token` is correct, not expired or revoked.
  - Ensure the token has the required scopes (`public_repo` or `repo`) for the repositories being read.
- **`404 Not Found` for repositories**:
  - Check that `owner` and `repo` are spelled correctly and that the token has access to that repository.
- **Permission-related errors for collaborators or org/teams**:
  - Reading collaborators, organizations, or teams may require elevated scopes or membership.
  - If these tables fail while others succeed, double-check token scopes and org policies.
- **Rate limiting (`403` with rate limit headers)**:
  - Reduce concurrency, widen schedule intervals, or use multiple tokens if allowed by your governance.
  - Inspect the GitHub `X-RateLimit-*` headers in logs (if surfaced) to understand usage.
- **Schema mismatches downstream**:
  - The connector uses nested structs extensively; ensure downstream tables are defined to accept nested types, or explicitly cast/flatten as needed in your transformations.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/github/github.py`
- Connector API documentation and schemas: `src/databricks/labs/community_connector/sources/github/github_api_doc.md`
- Official GitHub REST API documentation:
  - `https://docs.github.com/en/rest`
  - `https://docs.github.com/en/rest/issues/issues`
  - `https://docs.github.com/en/rest/commits/commits`
  - `https://docs.github.com/en/rest/pulls/pulls`
  - `https://docs.github.com/en/rest/issues/comments`
  - `https://docs.github.com/en/rest/branches/branches`
  - `https://docs.github.com/en/rest/collaborators/collaborators`
  - `https://docs.github.com/en/rest/orgs/orgs`
  - `https://docs.github.com/en/rest/teams/teams`
  - `https://docs.github.com/en/rest/users/users`


