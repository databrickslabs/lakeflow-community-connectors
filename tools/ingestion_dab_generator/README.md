# Lakeflow Connect Ingestion DAB Generator

Generate **Databricks Asset Bundle (DAB)** YAML for **Lakeflow Connect ingestion pipelines** from a metadata CSV.

This tool is intended for enterprise-scale ingestion where large numbers of tables/objects/partitions must be onboarded and kept in sync with minimal manual YAML editing.

## Why pipeline automation and load balancing matter

At enterprise scale:

- The inventory of ingestible entities (tables/objects/partitions) is large and changes frequently.
- Manually authoring and maintaining pipeline configuration is slow, error-prone, and difficult to keep consistent across environments.
- A single monolithic pipeline can become a bottleneck and an operational risk.

This tool provides:

- **Automation**: regenerate pipelines from metadata whenever the inventory changes.
- **Work distribution**: split the workload into multiple pipelines to improve throughput and reduce blast radius.
- **Load balancing (optional)**: automatically assign work into pipeline groups based on a measurable proxy weight.

## What it generates

A single YAML file containing:

- `resources.pipelines.*` entries using `ingestion_definition`
- optional `resources.jobs.*` entries (one per pipeline group) when scheduling is enabled

## Architecture

```
Metadata CSV
  │
  ▼
YAML generator (this tool)
  │
  ▼
DAB resources YAML (pipelines + optional jobs)
  │
  ▼
Deployed ingestion pipelines
  │
  ▼
Connector tables -> destination Delta/UC tables
```

Ingestion pipelines execute reads using `format("lakeflow_connect")` and apply the appropriate ingestion strategy (snapshot/cdc/append) based on connector-provided metadata.

## Supported pipeline specification format

The generated YAML follows the repository’s ingestion pipeline spec model (see `libs/spec_parser.py`):

- `ingestion_definition.connection_name` (required)
- `ingestion_definition.objects[]` (required)
  - `table.source_table` (required)
  - optional destination mapping:
    - `table.destination_catalog`
    - `table.destination_schema`
    - `table.destination_table`
  - optional `table.table_configuration` (string→string), used to pass per-table options

## Input CSV schema

Required:
- `source_table`

Common fields:
- `connection_name` (or pass `--connection-name`)
- `destination_catalog` (or pass `--dest-catalog`)
- `destination_schema` (or pass `--dest-schema`)
- `destination_table` (defaults to `source_table`)
- `pipeline_group` (if absent you can auto-assign with `--num-pipelines`)
- `schedule` (optional; emits jobs when `--emit-jobs`)
- `table_options_json` (optional; JSON object injected into `table_configuration`)
- `weight` (optional; numeric; used for auto-balancing)


### Prefix + priority grouping (hand-editable)

The generator supports an additional, hand-editable grouping pattern commonly used for operational control:

- If `pipeline_group` is empty and the CSV includes `prefix` (and optional `priority`), the tool derives:
  - `pipeline_group = <prefix>_<priority>` (or `<prefix>` if `priority` is empty)

This makes it easy for users to edit grouping and cadence directly in the CSV.

Tiny example:
- `tools/ingestion_dab_generator/examples/generic/example_prefix_priority.csv`
- `tools/ingestion_dab_generator/examples/generic/example_explicit_pipeline_group.csv`

## Load balancing model

When the future ingestion volume of each unit is unknown (common for incremental ingestion), load balancing uses a **proxy metric**:

- relational sources: row count or table size
- file-based sources: file size / file count
- partitioned logical units: entity counts per partition (e.g., tag count per plant)

If `pipeline_group` is empty for all rows and `--num-pipelines N` is provided, the tool assigns rows to pipeline groups using First-Fit Decreasing (FFD) bin packing on the `weight` column.

## Usage

### Generate YAML

```bash
python tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py \
  --input-csv /path/to/metadata.csv \
  --output-yaml /path/to/resources/ingestion_pipelines.yml \
  --connector-name <connector_name> \
  --connection-name <uc_connection_name> \
  --dest-catalog main \
  --dest-schema bronze \
  --num-pipelines 3 \
  --weight-column weight \
  --emit-jobs
```

### Deploy (template bundle)

A minimal bundle scaffold is provided at:

- `tools/ingestion_dab_generator/dab_template/`

Copy the generated YAML into `dab_template/resources/`, then deploy:

```bash
cd tools/ingestion_dab_generator/dab_template

databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Examples

- OSIPI: `tools/ingestion_dab_generator/examples/osipi/`
- GitHub: `tools/ingestion_dab_generator/examples/github/`
- Zendesk: `tools/ingestion_dab_generator/examples/zendesk/`

## Extending to additional connectors

The generator is connector-agnostic. To support additional sources, produce the same CSV schema from the source system’s metadata.

A common pattern is to add a small helper script under:

- `tools/ingestion_dab_generator/examples/<connector>/discover_*.py`

These helpers should emit the standard CSV with appropriate `source_table` and `table_options_json` values for that connector.

## Notes

- JSON-in-CSV must be quoted correctly (wrap JSON in quotes and double internal quotes).
- Jobs are created PAUSED by default. Pass `--unpause-jobs` to create them UNPAUSED.
