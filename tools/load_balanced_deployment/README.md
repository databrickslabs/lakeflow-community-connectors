# Load-Balanced Multi-Pipeline Deployment

Automatically deploy multiple DLT pipelines for any Lakeflow Community Connector with load balancing across table groups.

## Problem

Single-pipeline deployments for connectors with many tables face challenges:
- **Long execution times** when one pipeline ingests 30+ tables sequentially
- **Inflexible scheduling** - all tables run on same schedule even if some need more frequent updates
- **Poor resource utilization** - can't parallelize across table groups
- **Difficult monitoring** - hard to identify which table group is causing failures

## Solution

This toolkit automatically:
1. Discovers all connector tables and classifies them by type/category
2. Groups tables into optimal pipeline groups
3. Generates Python ingestion files (`ingest_*.py`) - one per group
4. Creates Databricks Asset Bundle (DAB) YAML for multi-pipeline deployment

**Benefits:**
- ✅ Parallel execution across multiple pipelines
- ✅ Independent scheduling (time-series every 15min, metadata daily)
- ✅ Better observability per group
- ✅ Isolated failures
- ✅ Generic - works for **any** community connector (OSIPI, HubSpot, GitHub, etc.)

## Architecture

### Multi-Pipeline Structure

```
Single Connector → Multiple Pipelines (Load Balanced)

osipi_metadata_snapshot      (3 tables, runs daily)
├─ pi_dataservers
├─ pi_points
└─ pi_point_attributes

osipi_timeseries_append      (2 tables, runs every 15min)
├─ pi_timeseries
└─ pi_streamset_recorded

osipi_asset_framework_snapshot (5 tables, runs weekly)
├─ pi_assetservers
├─ pi_af_hierarchy
└─ ...
```

### Pipeline Configuration

**All community connectors require:**
- `serverless: true` - Auto-scaling serverless compute (no cluster config needed)
- `channel: PREVIEW` - Required for PySpark Data Source API support

The toolkit automatically configures these settings. No cluster configuration is needed or supported.

## Quick Start

### Notebook Approach (Recommended)

Use the interactive Databricks notebook for guided deployment:

**Notebook:** `notebooks/deploy_load_balanced_pipelines.py`

**Features:**
- 8 essential configuration widgets
- Auto-detects repo path from current user
- Hardcoded sensible schedule defaults
- Step-by-step execution with verification

**Prerequisites:**
- UC Connection created with connector credentials
- Connector source synced to `/Workspace/Users/{user}/lakeflow-community-connectors/`

See [`notebooks/README.md`](notebooks/README.md) for detailed notebook usage.

### CLI Approach

For automation and CI/CD workflows:

#### Step 1: Discover Tables (Optional)

Use preset CSVs (recommended) or auto-discover:

```bash
# Option 1: Use preset CSV (recommended)
CSV_PATH=tools/load_balanced_deployment/examples/osipi/preset_by_category_and_ingestion.csv

# Option 2: Auto-discover tables
python3 tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name osipi \
  --connector-python-file sources/osipi/osipi.py \
  --output-csv /tmp/osipi_tables.csv \
  --connection-name osipi_connection_lakeflow \
  --dest-catalog osipi \
  --dest-schema bronze \
  --group-by category_and_ingestion_type \
  --schedule-snapshot "0 0 * * *" \
  --schedule-append "*/15 * * * *" \
  --schedule-cdc "*/5 * * * *" \
  --init-options-json '{"access_token": "..."}'
```

#### Step 2: Generate Ingest Files

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

**Output:** Multiple `ingest_*.py` files, one per pipeline group

#### Step 3: Generate DAB YAML

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
  --pause-jobs
```

**Output:** `databricks.yml` with multiple pipelines and optional scheduled jobs

**Example DAB bundle:** See `examples/osipi/dabs/` for complete reference

#### Step 4: Deploy

Upload ingest files to workspace and deploy pipelines using the notebook or Databricks SDK.

## Table Grouping

Tables are grouped into pipelines based on the `--group-by` parameter:

### Grouping Modes

| Mode | Description | Example Output |
|------|-------------|---------------|
| `none` | All tables in one pipeline | 1 pipeline |
| `ingestion_type` | Group by snapshot/append/cdc | 2-3 pipelines |
| `category` | Group by logical category | N pipelines (core, metadata, activity) |
| `category_and_ingestion_type` | **Recommended** - Group by category AND type | N×M pipelines (core_snapshot, core_append, ...) |

### Table Categorization

**Option 1: Built-in (OSIPI)**

Connectors with `TABLES_*` class attributes are automatically categorized:

```python
class LakeflowConnect:
    TABLES_TIME_SERIES = ["pi_timeseries", "pi_interpolated", ...]
    TABLES_ASSET_FRAMEWORK = ["pi_assetservers", "pi_af_hierarchy", ...]
```

**Option 2: Prefix Map (GitHub, HubSpot)**

For connectors without `TABLES_*` attributes, use a prefix map:

```json
{
  "pull": "core",
  "issue": "core",
  "commit": "activity",
  "user": "metadata"
}
```

Pass via `--category-prefix-map-json` or save to `examples/{connector}/category_prefix_map.json`

**Best Practices:**
- Use semantic category names (not `group1`, `misc`)
- Keep 3-8 pipelines per connector for optimal balance
- Test categorization before deploying

## Examples

Pre-configured examples for common connectors:

- **OSIPI:** `examples/osipi/` - 39 tables grouped by category + ingestion type
- **GitHub:** `examples/github/` - Uses category prefix map
- **HubSpot:** `examples/hubspot/` - Uses category prefix map

Each example includes:
- Preset CSV files (all tables, by ingestion type, by category)
- README with connector-specific guidance
- Category prefix map (if needed)
- Example DAB configuration

## Schedule Defaults

The toolkit uses sensible defaults for scheduled jobs:

- **Snapshot tables:** `0 0 * * *` (daily at midnight)
- **Append tables:** `*/15 * * * *` (every 15 minutes)
- **CDC tables:** `*/5 * * * *` (every 5 minutes)

Customize schedules by editing the generated DAB YAML or in the Databricks UI.

## Files Overview

**Core Python Scripts:**
- `discover_and_classify_tables.py` - Auto-discover and classify tables
- `generate_ingest_files.py` - Generate ingest_*.py files per group
- `generate_dab_yaml.py` - Generate DAB YAML configuration
- `utils.py` - Shared utilities

**Notebooks:**
- `notebooks/deploy_load_balanced_pipelines.py` - Interactive deployment notebook
- `notebooks/README.md` - Notebook usage guide

**Examples:**
- `examples/{connector}/preset_*.csv` - Pre-defined table lists
- `examples/{connector}/category_prefix_map.json` - Table categorization (if needed)
- `examples/{connector}/dabs/` - Example DAB configuration

## Troubleshooting

### Tables Not Categorized

**Symptom:** Tables appear in "unknown" category

**Solution:**
1. For connectors with `TABLES_*` attributes: verify attribute names
2. For prefix-based: check category prefix map JSON is valid
3. Add missing prefixes or update connector code with `TABLES_*` attributes

### Wrong Schedule Applied

**Symptom:** Pipeline has unexpected schedule

**Solution:**
Schedules are determined by:
1. Table's ingestion type (snapshot/append/cdc)
2. Default schedule for that type
3. Customize in generated DAB YAML if needed

### Deployment Fails

**Symptom:** Pipeline creation fails

**Solution:**
1. Verify UC Connection exists and has correct credentials
2. Check catalog/schema permissions
3. Ensure connector source is synced to workspace
4. Verify serverless is enabled in workspace

## Related Documentation

- [Notebooks README](notebooks/README.md) - Interactive deployment guide
- [OSIPI Example](examples/osipi/README.md) - Complete OSIPI setup
- [GitHub Example](examples/github/README.md) - Prefix map example
- [HubSpot Example](examples/hubspot/README.md) - CRM connector example
