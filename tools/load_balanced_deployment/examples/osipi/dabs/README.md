# Example Databricks Asset Bundle (DAB) for Load-Balanced Deployment

This directory contains **example DAB YAML files** for reference purposes only.

## ⚠️ Important

**DO NOT check `databricks.yml` into your project root!**

These files are examples showing what the `generate_dab_yaml.py` tool produces. They help you understand:
- DAB structure for multi-pipeline deployments
- How to configure pipelines with UC Connections
- Variable usage for different environments (dev/prod)

## How to Use

### Option 1: Generate DAB YAML Automatically

Use the `generate_dab_yaml.py` script to create your own bundle:

```bash
python tools/load_balanced_deployment/scripts/generate_dab_yaml.py \
  --connector osipi \
  --connection-name osipi_connection_lakeflow \
  --ingest-files-dir /tmp/osipi_ingest \
  --output-dir ./my_osipi_bundle
```

This creates:
```
my_osipi_bundle/
├── databricks.yml          # Bundle configuration
└── ingest_files/           # Python notebooks
    ├── ingest_metadata_snapshot.py
    ├── ingest_timeseries_append.py
    └── ...
```

### Option 2: Manually Create Bundle

1. Copy `databricks.yml` to your desired location (NOT in source control)
2. Customize the configuration:
   - Update workspace host URLs
   - Adjust catalog/schema names
   - Modify pipeline names and paths
3. Upload ingest files to workspace
4. Deploy with `databricks bundle deploy`

## Example: Deploy to Dev Environment

```bash
cd my_osipi_bundle
databricks bundle deploy --target dev
databricks bundle run osipi_metadata_snapshot
databricks bundle run osipi_timeseries_append
```

## Example: Deploy to Production

```bash
databricks bundle deploy --target prod
databricks bundle run osipi_metadata_snapshot --target prod
```

## Key Configuration Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `osipi` | Unity Catalog name |
| `schema` | `bronze` | Target schema |
| `connection_name` | `osipi_connection_lakeflow` | UC Connection |

Override in `targets`:

```yaml
targets:
  prod:
    mode: production
    variables:
      catalog: osipi_production
      schema: bronze
      connection_name: osipi_prod_connection
```

## Pipeline Naming Convention

Generated pipelines follow this pattern:

- `{connector}_{category}` - e.g., `osipi_metadata_snapshot`
- `{connector}_{ingestion_type}` - e.g., `osipi_append`
- `{connector}_all` - All tables in one pipeline

Customize pipeline names in `databricks.yml`:

```yaml
resources:
  pipelines:
    my_custom_name:
      name: "My Custom Pipeline Name"
      # ... rest of config
```

## Serverless vs Classic Compute

This example uses **serverless pipelines**:

```yaml
photon: true
serverless: true
channel: preview
```

For **classic compute**, use:

```yaml
photon: true
clusters:
  - label: default
    num_workers: 2
    node_type_id: i3.xlarge
    autoscale:
      min_workers: 1
      max_workers: 5
```

## Troubleshooting

### Error: "Connection not found"

Ensure your UC Connection exists:

```sql
SHOW CONNECTIONS;
```

Create if missing:

```sql
CREATE CONNECTION osipi_connection_lakeflow
TYPE GENERIC_LAKEFLOW_CONNECT
OPTIONS (
  'pi_base_url' 'https://your-osipi-server.com/piwebapi'
);
```

### Error: "Path not found"

Upload ingest files first:

```bash
databricks workspace import-dir \
  ./ingest_files \
  /Workspace/Users/your.email@company.com/osipi_ingest
```

### Error: "Catalog not found"

Create catalog and schema:

```sql
CREATE CATALOG IF NOT EXISTS osipi;
CREATE SCHEMA IF NOT EXISTS osipi.bronze;
```

## Next Steps

1. Generate your own DAB bundle using `generate_dab_yaml.py`
2. Test in dev environment
3. Update configuration for production
4. Set up CI/CD with GitHub Actions or Azure DevOps

See main [Load-Balanced Deployment README](../../README.md) for full workflow.
