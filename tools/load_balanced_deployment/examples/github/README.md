# GitHub Connector - Load Balanced Deployment Examples

This directory contains preset CSV files and configuration for deploying the GitHub connector with load-balanced multi-pipeline architecture.

## Available Preset CSV Files

### 1. `preset_all_tables.csv`
Single pipeline with all GitHub tables.
- **Use case:** Simple deployment, all tables in one pipeline
- **Tables:** 12 tables total

### 2. `preset_by_ingestion_type.csv`
Pipelines grouped by ingestion type (snapshot vs append).
- **Use case:** Separate snapshot and append workloads
- **Pipelines:** 2 (snapshot, append)

### 3. `preset_by_category_and_ingestion.csv`
Pipelines grouped by logical category AND ingestion type.
- **Use case:** Maximum parallelism and logical separation
- **Pipelines:** 6 groups
  - `core_snapshot`: issues, pull_requests, repositories
  - `activity_snapshot`: commits, reviews, comments
  - `metadata_snapshot`: users, organizations, teams, assignees, branches, collaborators

## Category Mapping

The GitHub connector doesn't have built-in `TABLES_*` attributes, so we use a category prefix map to classify tables:

**Category Definitions:**
- **core**: Primary GitHub objects (issues, pull_requests, repositories)
- **activity**: User activity data (commits, reviews, comments)
- **metadata**: Reference data (users, organizations, teams, assignees, branches, collaborators)

See `category_prefix_map.json` for the full mapping.

## Automatic Discovery with Category Prefix Map

To automatically discover and classify GitHub tables with custom categories:

```bash
python tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name github \
  --config-json '{"token": "ghp_your_token_here"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat tools/load_balanced_deployment/examples/github/category_prefix_map.json)" \
  --output-csv github_discovered.csv
```

**Alternative: Inline JSON**
```bash
python tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name github \
  --config-json '{"token": "ghp_xxx"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json '{"pull":"core","issue":"core","repositories":"core","commit":"activity","review":"activity","comment":"activity","user":"metadata","organization":"metadata","team":"metadata","assignee":"metadata","collaborator":"metadata","branch":"metadata"}' \
  --output-csv github_discovered.csv
```

## Deployment Instructions

### Step 1: Choose or Generate CSV
```bash
# Option A: Use preset CSV
cp tools/load_balanced_deployment/examples/github/preset_by_category_and_ingestion.csv my_github_deployment.csv

# Option B: Generate from live connector
python tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name github \
  --config-json '{"token": "ghp_xxx"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat tools/load_balanced_deployment/examples/github/category_prefix_map.json)" \
  --output-csv my_github_deployment.csv
```

### Step 2: Customize (Optional)
Edit the CSV to adjust table groupings, add filters, or modify schedules.

### Step 3: Deploy Pipelines
```bash
python tools/load_balanced_deployment/deploy_load_balanced_pipelines.py \
  --csv-file my_github_deployment.csv \
  --connector-name github \
  --connection-name my_github_connection \
  --catalog main \
  --target_schema github_bronze \
  --base-pipeline-name "GitHub Load Balanced"
```

## Table Schema

| Column | Description | Example |
|--------|-------------|---------|
| pipeline_name | Unique name for the pipeline | `core_snapshot` |
| table_name | Source table name | `issues` |
| destination_table | Target table name (optional) | `issues` |
| scd_type | Ingestion mode | `SCD_TYPE_1` or `APPEND_ONLY` |
| primary_keys | Comma-separated PKs | `id` |
| table_configuration | JSON options (optional) | `{"repo": "databricks/spark"}` |
| schedule | Cron expression | `0 */6 * * *` (every 6 hours) |
| weight | Pipeline priority | `10` |

## Ingestion Types by Table

**Snapshot (SCD_TYPE_1):**
- issues
- pull_requests
- repositories
- users
- organizations
- teams
- assignees
- branches
- collaborators

**Append (APPEND_ONLY):**
- commits
- reviews
- comments

## Recommended Schedules

- **Snapshot tables:** Every 6 hours (`0 */6 * * *`)
- **Append tables:** Every 1 hour (`0 * * * *`)
- **Metadata tables:** Every 12 hours (`0 */12 * * *`)

## Notes

- GitHub API has rate limits (5000 requests/hour for authenticated users)
- Consider using table_configuration to filter by repository: `{"repo": "owner/repo"}`
- For organization-wide ingestion, use: `{"owner": "org_name"}`
- The connector requires a GitHub Personal Access Token with appropriate scopes
