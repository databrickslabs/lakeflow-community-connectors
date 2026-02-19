# Azure DevOps Connector - CLI Commands

This document shows how to use the Community Connector CLI to set up and run Azure DevOps pipelines.

## Prerequisites

1. Install the Community Connector CLI:
```bash
cd tools/community_connector
pip install -e .
```

2. Configure Databricks authentication (one of):
   - Personal Access Token (PAT)
   - Service Principal
   - See: https://docs.databricks.com/dev-tools/auth/config-profiles

## Step 1: Create a Unity Catalog Connection

Create a connection with your Azure DevOps credentials:

```bash
community-connector create_connection azure_devops my_azure_devops_conn \
  -o '{
    "organization": "my-org",
    "personal_access_token": "your-pat-token",
    "externalOptionsAllowList": "project,repository_id,pullrequest_id,status_filter,filter,ids"
  }'
```

**With optional project parameter:**
```bash
community-connector create_connection azure_devops my_azure_devops_conn \
  -o '{
    "organization": "my-org",
    "project": "MyProject",
    "personal_access_token": "your-pat-token",
    "externalOptionsAllowList": "project,repository_id,pullrequest_id,status_filter,filter,ids"
  }'
```

## Step 2: Create a Pipeline

### Option A: Minimal Setup (Auto-discovery)

Ingest all core tables with auto-discovery:

```bash
community-connector create_pipeline azure_devops my_azure_devops_pipeline \
  -n my_azure_devops_conn \
  --catalog main \
  --target azure_devops_raw
```

### Option B: With Pipeline Spec File (Full Control)

```bash
community-connector create_pipeline azure_devops my_azure_devops_pipeline \
  -ps examples/pipeline_spec_full.yaml \
  --catalog main \
  --target azure_devops_raw
```

### Option C: With Inline JSON Spec

```bash
community-connector create_pipeline azure_devops my_azure_devops_pipeline \
  -ps '{
    "connection_name": "my_azure_devops_conn",
    "objects": [
      {"table": {"source_table": "projects"}},
      {"table": {"source_table": "repositories"}},
      {"table": {"source_table": "commits"}},
      {"table": {"source_table": "pullrequests"}},
      {"table": {"source_table": "users"}}
    ]
  }' \
  --catalog main \
  --target azure_devops_raw
```

## Step 3: Run the Pipeline

### Initial Run
```bash
community-connector run_pipeline my_azure_devops_pipeline
```

### Full Refresh
```bash
community-connector run_pipeline my_azure_devops_pipeline --full-refresh
```

### Check Pipeline Status
```bash
community-connector show_pipeline my_azure_devops_pipeline
```

## Example Use Cases

### Use Case 1: Organization-Wide Discovery

Ingest all data from all projects:

```bash
# 1. Create connection (no project specified)
community-connector create_connection azure_devops org_wide_conn \
  -o '{
    "organization": "my-org",
    "personal_access_token": "your-pat",
    "externalOptionsAllowList": "project,repository_id,pullrequest_id,status_filter,filter,ids"
  }'

# 2. Create pipeline with auto-discovery spec
community-connector create_pipeline azure_devops org_wide_pipeline \
  -ps examples/pipeline_spec_auto_discovery.yaml \
  --catalog main \
  --target azure_devops_org_wide

# 3. Run
community-connector run_pipeline org_wide_pipeline
```

### Use Case 2: Single Project Analysis

Focus on one specific project:

```bash
# 1. Create connection with project
community-connector create_connection azure_devops project_conn \
  -o '{
    "organization": "my-org",
    "project": "MyProject",
    "personal_access_token": "your-pat",
    "externalOptionsAllowList": "project,repository_id,pullrequest_id,status_filter,filter,ids"
  }'

# 2. Create pipeline
community-connector create_pipeline azure_devops project_pipeline \
  -n project_conn \
  --catalog main \
  --target myproject_raw

# 3. Run
community-connector run_pipeline project_pipeline
```

### Use Case 3: Specific Repository Deep Dive

Analyze a specific repository in detail:

```bash
# Create inline spec for targeted ingestion
community-connector create_pipeline azure_devops repo_analysis \
  -ps '{
    "connection_name": "my_azure_devops_conn",
    "objects": [
      {
        "table": {
          "source_table": "commits",
          "table_configuration": {
            "project": "MyProject",
            "repository_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
          }
        }
      },
      {
        "table": {
          "source_table": "pullrequests",
          "table_configuration": {
            "project": "MyProject",
            "repository_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "status_filter": "all"
          }
        }
      },
      {
        "table": {
          "source_table": "pullrequest_threads",
          "table_configuration": {
            "project": "MyProject",
            "repository_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
          }
        }
      }
    ]
  }' \
  --catalog main \
  --target repo_analysis

community-connector run_pipeline repo_analysis
```

### Use Case 4: Work Item Tracking Only

Ingest work items and their history:

```bash
community-connector create_pipeline azure_devops workitems_pipeline \
  -ps '{
    "connection_name": "my_azure_devops_conn",
    "objects": [
      {
        "table": {
          "source_table": "workitem_revisions",
          "destination_table": "workitem_revisions",
          "table_configuration": {
            "project": "MyProject"
          }
        }
      },
      {
        "table": {
          "source_table": "workitem_types",
          "destination_table": "workitem_types",
          "table_configuration": {
            "project": "MyProject"
          }
        }
      }
    ]
  }' \
  --catalog main \
  --target workitems_raw

community-connector run_pipeline workitems_pipeline
```

### Use Case 5: Pull Request Analytics

Deep dive into PR activities:

```bash
community-connector create_pipeline azure_devops pr_analytics \
  -ps '{
    "connection_name": "my_azure_devops_conn",
    "objects": [
      {"table": {"source_table": "pullrequests"}},
      {"table": {"source_table": "pullrequest_threads"}},
      {"table": {"source_table": "pullrequest_workitems"}},
      {"table": {"source_table": "pullrequest_commits"}},
      {"table": {"source_table": "pullrequest_reviewers"}}
    ]
  }' \
  --catalog main \
  --target pr_analytics

community-connector run_pipeline pr_analytics
```

## Update Connection

If you need to update your PAT or other connection settings:

```bash
community-connector update_connection azure_devops my_azure_devops_conn \
  -o '{
    "organization": "my-org",
    "project": "MyProject",
    "personal_access_token": "new-pat-token",
    "externalOptionsAllowList": "project,repository_id,pullrequest_id,status_filter,filter,ids"
  }'
```

## Debug Mode

Enable debug output for troubleshooting:

```bash
community-connector --debug create_pipeline azure_devops my_pipeline -n my_conn
community-connector --debug run_pipeline my_pipeline
```

## Notes

- **PAT Scopes Required**: Ensure your Personal Access Token includes:
  - `Code (read)` - For Git and PR tables
  - `Graph (read)` - For users table
  - `Project and Team (read)` - For projects table
  - `Work Items (read)` - For work item tables

- **Auto-discovery**: Omit `project`, `repository_id`, or `pullrequest_id` to fetch from all available resources

- **Targeted queries**: Specify IDs for more efficient ingestion of specific resources

- **Pipeline specs**: Use YAML files for complex configurations, inline JSON for quick tests

- **Catalog/Schema**: Adjust `--catalog` and `--target` parameters to match your Unity Catalog structure

