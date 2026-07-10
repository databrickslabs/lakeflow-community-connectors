# Deployment Notebook

Interactive Databricks notebook for deploying load-balanced multi-pipeline configurations.

## Notebook: `deploy_load_balanced_pipelines.py`

**All-in-one workflow** for deploying load-balanced pipelines with minimal configuration.

**What it does:**
1. Discovers tables (auto-discovery or preset CSV)
2. Generates Python ingest files for each pipeline group
3. Generates DAB YAML configuration
4. Uploads ingest files to workspace
5. Deploys pipelines and scheduled jobs using Databricks SDK
6. Verifies deployment

## Configuration (8 Widgets)

**Basic Settings:**
- **Connector Name:** Source connector (osipi, zendesk, github, etc.)
- **Connection Name:** UC connection with credentials
- **Use Preset CSV:** true = preset tables, false = auto-discover

**Destination:**
- **Catalog:** Target catalog (must exist)
- **Schema:** Target schema (created if missing)

**Pipeline Configuration:**
- **Group By:** How to split tables (category_and_ingestion_type recommended)
- **Create Scheduled Jobs:** Auto-create scheduled jobs
- **Pause Jobs Initially:** Start jobs in PAUSED state (recommended)

**Auto-configured:**
- Repo path: Auto-detected from current user
- Schedules: Hardcoded defaults (daily/15min/5min for snapshot/append/cdc)
- Compute: Serverless only (no cluster configuration)

## Prerequisites

1. **UC Connection created** with connector credentials
2. **Connector source synced** to `/Workspace/Users/{user}/lakeflow-community-connectors/`
3. **Serverless enabled** in your workspace

## Usage

### Step 1: Upload Notebook to Workspace

Sync the repo to your workspace:
```bash
# Ensure repo is at: /Workspace/Users/your.email@company.com/lakeflow-community-connectors/
```

### Step 2: Open and Configure

1. Open `tools/load_balanced_deployment/notebooks/deploy_load_balanced_pipelines.py`
2. Set the 8 configuration widgets
3. Run all cells

### Step 3: Verify Deployment

Navigate to **Workflows > Delta Live Tables** to see deployed pipelines.

## Configuration Tips

### Using Preset CSVs (Recommended)

Set `Use Preset CSV = true` and the notebook will use:
```
tools/load_balanced_deployment/examples/{connector}/preset_by_category_and_ingestion.csv
```

This skips auto-discovery and uses pre-defined table configuration.

### Grouping Strategies

| Mode | Description | Result |
|------|-------------|--------|
| `none` | All tables in one pipeline | 1 pipeline |
| `ingestion_type` | Group by snapshot/append/cdc | 2-3 pipelines |
| `category` | Group by logical category | N pipelines |
| `category_and_ingestion_type` | **Recommended** | N×M pipelines |

### Schedule Defaults (Hardcoded)

The notebook uses these schedule defaults:
- **Snapshot tables:** Daily at midnight (`0 0 * * *`)
- **Append tables:** Every 15 minutes (`*/15 * * * *`)
- **CDC tables:** Every 5 minutes (`*/5 * * * *`)

To customize: Edit the generated DAB YAML or change schedules in Databricks UI after deployment.

### Serverless Configuration

All pipelines use:
- `serverless: true` - Auto-scaling compute
- `channel: PREVIEW` - Required for community connectors

**No cluster configuration is needed or supported.**

## Troubleshooting

### Error: "Connection not found"

Ensure UC Connection exists:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
connections = list(w.connections.list())
print([c.name for c in connections])
```

### Error: "Connector source not found"

Verify repo is synced to workspace:
```
/Workspace/Users/{user}/lakeflow-community-connectors/sources/{connector}/{connector}.py
```

### Error: "Permission denied"

Check you have permissions to:
- Create pipelines in the workspace
- Create tables in the destination catalog
- Read from the UC connection

**Note:** The notebook uses timestamped work directories (`/tmp/.../load_balanced_deployment_{connector}_{timestamp}`) to avoid caching issues. Old directories are automatically cleaned up on each run.

**Serverless vs Classic Clusters:**
- ✅ **Serverless** (recommended): Works correctly with /tmp directories
- ⚠️ **Classic clusters**: May encounter /tmp permission issues

### Pipelines Not Starting

If jobs are in PAUSED state, enable them:
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import PauseStatus, JobSettings, CronSchedule

w = WorkspaceClient()
job = w.jobs.get(job_id)

w.jobs.update(
    job_id=job_id,
    new_settings=JobSettings(
        name=job.settings.name,
        tasks=job.settings.tasks,
        schedule=CronSchedule(
            quartz_cron_expression=job.settings.schedule.quartz_cron_expression,
            timezone_id=job.settings.schedule.timezone_id,
            pause_status=PauseStatus.UNPAUSED
        )
    )
)
```

## Next Steps After Deployment

1. **Monitor pipelines:**
   - Navigate to **Workflows > Delta Live Tables**
   - Check each pipeline group independently

2. **Enable scheduled jobs** (if created in PAUSED state)

3. **Trigger manual runs:**
   ```python
   w.pipelines.start_update(pipeline_id="<id>", full_refresh=False)
   ```

4. **Customize:**
   - Edit generated DAB YAML for schedule changes
   - Modify ingest files in workspace if needed
   - Adjust configuration in Databricks UI

## Notebook vs CLI

**Notebook (this approach):**
- ✅ Interactive, step-by-step
- ✅ Visual feedback
- ✅ Easy configuration via widgets
- ✅ Great for first-time setup

**CLI (Python scripts):**
- ✅ Automation-friendly (CI/CD)
- ✅ No workspace required
- ✅ Version-controlled configuration
- ✅ Faster for repeated deployments

See main [README](../README.md) for CLI usage.
