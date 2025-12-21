# OSIPI example: auto-generate ingestion pipelines + load-balance by tag count

This example shows how to use the **generic** generator in `tools/ingestion_dab_generator/` to produce
Lakeflow Connect **ingestion_definition** pipelines for the OSIPI connector, and how to use a metric
(tag count per plant) to **auto-assign pipeline groups** for load balancing.

## What this produces

- A CSV where each row represents a *logical ingestion unit*:
  - source table: `pi_timeseries`
  - destination table: `pi_timeseries_<plant>`
  - table options: `nameFilter="<Plant>_*"` to scope tags per plant
  - `weight=<tag_count>` so larger plants “weigh more” in balancing
- A single YAML file with:
  - multiple ingestion pipelines (`resources.pipelines.*`)
  - optional scheduled jobs (`resources.jobs.*`)

## 1) Discover partitions (implemented OSIPI example)

This script queries PI Web API points for each plant prefix and counts tags:

```bash
python tools/ingestion_dab_generator/examples/osipi/discover_osipi_partitions.py \
  --pi-base-url https://osipi-webserver-1444828305810485.aws.databricksapps.com \
  --access-token "<PASTE_BEARER_TOKEN_WITHOUT_BEARER_PREFIX>" \
  --plants Sydney,Melbourne,Brisbane,Perth,Adelaide,Darwin,Hobart \
  --output-csv /tmp/osipi_timeseries_partitions.csv \
  --dest-catalog main \
  --dest-schema bronze \
  --lookback-minutes 60 \
  --maxCount 1000
```

Notes:

## 2) Generate DAB YAML (auto load-balance)

Generate a YAML file that creates **N ingestion pipelines** and spreads plants across them by `weight`:

```bash
python tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py \
  --input-csv /tmp/osipi_timeseries_partitions.csv \
  --output-yaml /tmp/osipi_ingestion_pipelines.yml \
  --connector-name osipi \
  --connection-name "<YOUR_DATABRICKS_CONNECTION_NAME>" \
  --dest-catalog main \
  --dest-schema bronze \
  --num-pipelines 3 \
  --weight-column weight \
  --emit-jobs
```

## 3) Deploy with Databricks Asset Bundles

Create a small DAB folder (one-time) like:

```
osipi_ingestion_dab/
  databricks.yml
  resources/
    osipi_ingestion_pipelines.yml
```

and set `databricks.yml` to include the generated YAML under `resources/`.

Then deploy:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Why this is reusable tooling

- The generator is **connector-agnostic**: it only cares about `source_table` and `table_options_json`.
- The load balancing is **metric-driven**: the same bin packing works with row-counts (SFDC/SQL Server),
  file sizes (Autoloader), or tag counts (OSIPI).
- OSIPI-specific logic is limited to the **discovery script**, which is an adapter that emits the same CSV schema.

