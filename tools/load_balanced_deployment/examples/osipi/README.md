# OSIPI Preset CSV Examples

This directory contains preset CSV files for OSIPI connector load-balanced deployments.

## Available Presets

### 1. `preset_by_category_and_ingestion.csv`
**Recommended for most use cases**

Groups tables by both category and ingestion type for optimal load balancing:
- `metadata_snapshot` - 3 tables, daily schedule
- `timeseries_append` - 4 tables, 15-minute schedule
- `asset_framework_snapshot` - 5 tables, weekly schedule
- `event_frames_snapshot` - 2 tables, daily schedule

**Benefits**:
- Independent scheduling per group
- Natural separation by data type and update pattern
- Easy to monitor and troubleshoot

### 2. `preset_by_ingestion_type.csv`
Groups tables only by ingestion type:
- `snapshot` - 10 tables, daily schedule
- `append` - 4 tables, 15-minute schedule

**Use when**:
- You want all snapshot tables together regardless of category
- Simpler grouping structure preferred

### 3. `preset_all_tables.csv`
Single pipeline group (`all`) containing all tables.

**Use when**:
- Testing before implementing load balancing
- Connector has few tables (< 10)
- All tables have similar characteristics

## Usage

### Option 1: Use Preset Directly

Skip the discovery step and use a preset CSV:

```bash
# Generate ingest files from preset
python3 tools/load_balanced_deployment/generate_ingest_files.py \
  --csv tools/load_balanced_deployment/examples/osipi/preset_by_category_and_ingestion.csv \
  --output-dir /tmp/osipi_ingest_files \
  --source-name osipi \
  --connection-name osipi_connection_lakeflow \
  --catalog osipi \
  --schema bronze \
  --common-table-config-json '{}'

# Generate DAB YAML
python3 tools/load_balanced_deployment/generate_dab_yaml.py \
  --connector-name osipi \
  --input-csv tools/load_balanced_deployment/examples/osipi/preset_by_category_and_ingestion.csv \
  --output-yaml /tmp/osipi_bundle/databricks.yml \
  --ingest-files-path /Workspace/Users/user@databricks.com/osipi_ingest \
  --connection-name osipi_connection_lakeflow \
  --catalog osipi \
  --schema bronze \
  --emit-jobs \
  --num-workers 2
```

### Option 2: Customize Preset

1. Copy a preset to your working directory:
   ```bash
   cp tools/load_balanced_deployment/examples/osipi/preset_by_category_and_ingestion.csv /tmp/my_osipi_custom.csv
   ```

2. Edit the CSV:
   - Change schedules
   - Adjust pipeline groups
   - Modify table options
   - Change catalog/schema

3. Use your custom CSV in the workflow

## CSV Format

```csv
connection_name,source_table,destination_catalog,destination_schema,destination_table,pipeline_group,schedule,table_options_json,weight,ingestion_type,primary_keys,cursor_field,category
```

**Required columns**:
- `source_table` - Table name from connector
- `pipeline_group` - Which pipeline this table belongs to
- `connection_name` - UC Connection name

**Optional columns**:
- `schedule` - Cron expression for scheduled jobs (empty = no job)
- `destination_catalog` - Target catalog (default from --catalog)
- `destination_schema` - Target schema (default from --schema)
- `destination_table` - Target table name (default = source_table)
- `table_options_json` - JSON string of table-specific options
- `weight` - Relative size for balancing (not currently used)
- `ingestion_type` - snapshot, append, or cdc
- `primary_keys` - JSON array of primary key columns
- `cursor_field` - Column used for incremental ingestion
- `category` - Logical grouping (metadata, timeseries, etc.)

## Creating Presets for Other Connectors

When developing a new connector, provide preset CSVs:

1. Create directory: `tools/load_balanced_deployment/examples/{connector}/`

2. Generate CSV using discovery tool:
   ```bash
   python3 tools/load_balanced_deployment/discover_and_classify_tables.py \
     --connector-name {connector} \
     --output-csv examples/{connector}/preset_by_category_and_ingestion.csv \
     --connection-name {connector}_connection \
     --dest-catalog main \
     --dest-schema bronze \
     --group-by category_and_ingestion_type \
     --schedule-snapshot "0 0 * * *" \
     --schedule-append "*/15 * * * *" \
     --schedule-cdc "*/5 * * * *"
   ```

3. Create additional presets (by_ingestion_type, all_tables)

4. Document in connector's README

## Schedule Syntax

Schedules use standard 5-field cron syntax:
```
minute hour day_of_month month day_of_week
```

Examples:
- `0 0 * * *` - Daily at midnight
- `*/15 * * * *` - Every 15 minutes
- `*/5 * * * *` - Every 5 minutes
- `0 0 * * 0` - Weekly on Sunday at midnight
- `0 2 1 * *` - Monthly on 1st day at 2am

Leave empty for no scheduled job (manual triggers only).
