# Load-Balanced Multi-Pipeline Deployment for Community Connectors

**Hackathon Value-Add**: Automatic load balancing across multiple DLT pipelines for any Lakeflow Community Connector.

## Problem Statement

Single-pipeline deployments for connectors with many tables face challenges:
- **Long execution times** when one pipeline ingests 30+ tables sequentially
- **Inflexible scheduling** - all tables run on same schedule even if some need more frequent updates
- **Poor resource utilization** - can't parallelize across table groups
- **Difficult monitoring** - hard to identify which table group is causing failures

## Solution: Automatic Load Balancing

This toolkit automatically:
1. **Discovers** all connector tables and classifies them by size/type
2. **Groups** tables into optimal pipeline groups (by category, ingestion type, or size)
3. **Generates** multiple Python ingestion files (`ingest_*.py`) - one per group
4. **Creates** Databricks Asset Bundle (DAB) YAML for multi-pipeline deployment

### Key Benefits

✅ **Parallel execution** - Multiple pipelines run simultaneously
✅ **Independent scheduling** - Time-series tables every 15min, metadata daily
✅ **Better observability** - Clear visibility into which group is slow/failing
✅ **Cost optimization** - Right-size compute per group
✅ **Isolated failures** - One group's failure doesn't block others
✅ **Generic** - Works for **any** community connector (OSIPI, HubSpot, GitHub, etc.)

## Architecture

### Uses UC Connection (NOT secrets!)

**Important**: This approach uses `GENERIC_LAKEFLOW_CONNECT` UC Connection type with platform credential injection. **No `dbutils.secrets` calls!**

```python
# Generated ingest files use UC Connection
pipeline_spec = {
    "connection_name": "osipi_connection_lakeflow",  # Platform injects credentials
    "objects": [...tables for this group...]
}
ingest(spark, pipeline_spec)
```

### Multi-Pipeline Structure

```
Single Connector → Multiple Pipelines (Load Balanced)

osipi_metadata_pipeline        (3 tables, runs daily)
├─ pi_dataservers
├─ pi_points
└─ pi_point_attributes

osipi_timeseries_pipeline      (2 tables, runs every 15min)
├─ pi_timeseries
└─ pi_streamset_recorded

osipi_asset_framework_pipeline (5 tables, runs weekly)
├─ pi_assetservers
├─ pi_af_hierarchy
└─ ...
```

## Quick Start

### Two Approaches: CLI vs Notebooks

**CLI Approach** (this page): Command-line tools for automation and CI/CD
**Notebook Approach**: Interactive Databricks notebook - see [`notebooks/README.md`](notebooks/README.md)

### Prerequisites

1. UC Connection created with GENERIC_LAKEFLOW_CONNECT type:
   ```bash
   community-connector create_connection osipi osipi_connection -o '{"pi_base_url": "https://....", "access_token": "..."}'
   ```

2. Connector source code deployed to workspace

### Using Preset CSVs (Recommended)

Skip discovery and use preset CSV examples:

```bash
# Use preset CSV for OSIPI
python3 tools/load_balanced_deployment/generate_ingest_files.py \
  --csv tools/load_balanced_deployment/examples/osipi/preset_by_category_and_ingestion.csv \
  --output-dir /tmp/osipi_ingest_files \
  --source-name osipi \
  --connection-name osipi_connection_lakeflow \
  --catalog osipi \
  --schema bronze \
  --common-table-config-json '{}'
```

See [`examples/osipi/README.md`](examples/osipi/README.md) for available presets.

### Step 1: Discover and Classify Tables (Optional)

Auto-discover tables from connector and classify into groups:

```bash
python3 tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name osipi \
  --output-csv /tmp/osipi_tables.csv \
  --connection-name osipi_connection_lakeflow \
  --dest-catalog osipi \
  --dest-schema bronze \
  --group-by category_and_ingestion_type \
  --schedule-snapshot "0 0 * * *" \
  --schedule-append "*/15 * * * *" \
  --schedule-cdc "*/5 * * * *"
```

**Output CSV** (`/tmp/osipi_tables.csv`):
```csv
source_table,pipeline_group,schedule,category,ingestion_type
pi_dataservers,metadata_snapshot,0 0 * * *,metadata,snapshot
pi_points,metadata_snapshot,0 0 * * *,metadata,snapshot
pi_timeseries,timeseries_append,*/15 * * * *,timeseries,append
pi_streamset_recorded,timeseries_append,*/15 * * * *,timeseries,append
```

### Step 2: Generate Ingest Files

Generate Python files for each pipeline group:

```bash
python3 tools/load_balanced_deployment/generate_ingest_files.py \
  --csv /tmp/osipi_tables.csv \
  --output-dir /tmp/osipi_ingest_files \
  --source-name osipi \
  --connection-name osipi_connection_lakeflow \
  --catalog osipi \
  --schema bronze \
  --common-table-config-json '{}'
```

**Output Files**:
- `/tmp/osipi_ingest_files/ingest_metadata_snapshot.py`
- `/tmp/osipi_ingest_files/ingest_timeseries_append.py`
- `/tmp/osipi_ingest_files/ingest_asset_framework_snapshot.py`

### Step 3: Generate DAB YAML

Generate Databricks Asset Bundle configuration:

```bash
python3 tools/load_balanced_deployment/generate_dab_yaml.py \
  --connector-name osipi \
  --input-csv /tmp/osipi_tables.csv \
  --output-yaml /tmp/osipi_bundle/databricks.yml \
  --ingest-files-path /Workspace/Users/user@databricks.com/osipi_ingest \
  --connection-name osipi_connection_lakeflow \
  --catalog osipi \
  --schema bronze \
  --emit-jobs \
  --num-workers 2
```

**Output**: `databricks.yml` with multiple pipelines and optional scheduled jobs

**See Example**: Check `examples/osipi/dabs/` for a complete reference DAB bundle

### Step 4: Deploy

```bash
# Upload ingest files to workspace
databricks workspace import-dir /tmp/osipi_ingest_files /Workspace/Users/user@databricks.com/osipi_ingest

# Deploy bundle
cd /tmp/osipi_bundle
databricks bundle deploy

# Run pipelines (can run in parallel!)
databricks bundle run osipi_metadata_snapshot
databricks bundle run osipi_timeseries_append
```

## Grouping Strategies

### By Category and Ingestion Type (Recommended)
```bash
--group-by category_and_ingestion_type
```
Groups: `metadata_snapshot`, `timeseries_append`, `asset_framework_snapshot`

**Use when**: Tables have natural categories with different ingestion patterns

### By Ingestion Type Only
```bash
--group-by ingestion_type
```
Groups: `snapshot`, `append`, `cdc`

**Use when**: Optimizing for ingestion pattern (snapshot tables together, streaming tables together)

### By Category Only
```bash
--group-by category
```
Groups: `metadata`, `timeseries`, `asset_framework`

**Use when**: Tables in same category should always run together regardless of ingestion type

### No Grouping (Single Pipeline)
```bash
--group-by none
```
Group: `all`

**Use when**: Connector has few tables or testing before splitting

## Advanced Configuration

### Custom Schedules

Set different schedules per ingestion type:

```bash
--schedule-snapshot "0 0 * * *"      # Daily at midnight
--schedule-append "*/15 * * * *"     # Every 15 minutes
--schedule-cdc "*/5 * * * *"         # Every 5 minutes
--schedule-unknown "0 0 * * 0"       # Weekly on Sunday
```

### Custom Category Mapping

For connectors without `TABLES_*` class attributes, provide prefix-to-category mapping:

```bash
--category-prefix-map-json '{"pi_streamset":"timeseries","pi_af":"asset_framework","pi_event":"event_frames"}'
```

### Cluster Sizing

Configure workers per pipeline:

```bash
--num-workers 4  # Allocate 4 workers per pipeline
```

## Connector Compatibility

This toolkit works with **any** community connector that:
- ✅ Uses `GENERIC_LAKEFLOW_CONNECT` UC Connection type
- ✅ Implements `LakeflowConnect` interface (`list_tables()`, `read_table_metadata()`)
- ✅ Uses `pipeline_spec` with `connection_name`

### Tested Connectors

- **OSIPI** - 39 tables grouped into 4 pipelines
- **HubSpot** - (add your results)
- **GitHub** - (add your results)

### Adding New Connectors

No code changes needed! Just:
1. Ensure connector implements `LakeflowConnect` interface
2. Create UC Connection with `GENERIC_LAKEFLOW_CONNECT` type
3. Run the 3-step workflow above

## Comparison: Single vs Load-Balanced

### Before (Single Pipeline)

```yaml
resources:
  pipelines:
    osipi_pipeline:
      libraries:
        - file:
            path: /Workspace/.../ingest.py  # All 39 tables
```

**Issues**:
- 2+ hour execution time
- One table failure blocks all others
- Can't schedule different table groups differently
- Hard to identify bottlenecks

### After (Load-Balanced)

```yaml
resources:
  pipelines:
    osipi_metadata_snapshot:       # 10 tables, 15 min
    osipi_timeseries_append:       # 12 tables, 45 min
    osipi_asset_framework:         # 15 tables, 20 min
    osipi_event_frames:            # 2 tables, 5 min
```

**Benefits**:
- Runs in parallel: max 45 min (not 2+ hours)
- Isolated failures
- Independent scheduling (timeseries every 15min, metadata daily)
- Clear observability per group

## File Storage Best Practices

### Workspace Files vs UC Volumes

When deploying generated ingest files, you have two options:

#### Option 1: Workspace Files (Recommended for Code)

Best for Python code files like `ingest_*.py`:

```bash
# CLI approach
databricks workspace import-dir /tmp/osipi_ingest_files /Workspace/Users/user@company.com/osipi_ingest

# Notebook/SDK approach
for file in ingest_files:
    with open(file, 'r') as f:
        workspace_client.workspace.import_(
            path=f"/Workspace/Users/{username}/osipi_ingest/{file.name}",
            format=ImportFormat.SOURCE,
            content=base64.b64encode(f.read().encode()).decode()
        )
```

**DAB YAML references**:
```yaml
libraries:
  - file:
      path: /Workspace/Users/user@company.com/osipi_ingest/ingest_metadata_snapshot.py
```

#### Option 2: UC Volumes (Recommended for Governed Storage)

Best for data files and artifacts requiring governance:

```python
# Create volume first (one-time setup)
spark.sql("CREATE VOLUME IF NOT EXISTS main.default.deployment_artifacts")

# Upload files using dbutils.fs with UC Volumes path
for file in ingest_files:
    dbutils.fs.cp(
        f"file://{file}",
        f"/Volumes/main/default/deployment_artifacts/{file.name}"
    )
```

**DAB YAML references**:
```yaml
libraries:
  - file:
      path: /Volumes/main/default/deployment_artifacts/ingest_metadata_snapshot.py
```

### What NOT to Use

**DEPRECATED - Do NOT use**:
- ❌ DBFS root paths: `/dbfs/...`
- ❌ DBFS mount paths: `/mnt/...`
- ❌ `dbutils.fs.mount()` commands

These are deprecated in Unity Catalog environments and bypass governance controls.

### Key Points

1. **dbutils.fs utilities still work** - but use them with UC Volumes paths (`/Volumes/...`), not DBFS
2. **Workspace files are appropriate for code** - DLT pipelines can reference `/Workspace/...` paths
3. **UC Volumes provide governance** - use for data files, artifacts, and shared resources
4. **DBFS is deprecated** - avoid all DBFS root and mount patterns

## Troubleshooting

### Error: "Connection type HTTP is not supported"

**Solution**: Use `GENERIC_LAKEFLOW_CONNECT` type, not `HTTP`:
```bash
community-connector create_connection <source> <conn_name> -o '{...}'
```

### Error: "No LakeflowConnect class found"

**Solution**: Ensure connector source file is deployed to workspace and registered:
```python
from libs.source_loader import get_register_function
get_register_function("osipi")(spark)
```

### Pipelines not finding tables

**Solution**: Check `--connection-name` matches the actual UC Connection name:
```bash
databricks connections list | grep <connector>
```

## Future Enhancements

- [ ] Auto-balance tables across N pipelines based on estimated size
- [ ] Dynamic cluster sizing per group based on table count
- [ ] Integration with MLflow for pipeline performance tracking
- [ ] Cost estimation per pipeline group

## Credits

**Hackathon Submission**: Load-balanced multi-pipeline deployment for Lakeflow Community Connectors
**Author**: Pravin Varma
**Date**: 2026-01-06
