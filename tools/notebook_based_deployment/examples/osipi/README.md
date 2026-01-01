# OSIPI notebook-based deployment example

This folder provides a metadata CSV listing the OSIPI connector tables and a suggested pipeline grouping.

- `osipi_tables_metadata.csv`: input to the generators.

Example:

```bash
python tools/notebook_based_deployment/generate_dlt_notebooks_generic.py \
  --connector-name osipi \
  --input-csv tools/notebook_based_deployment/examples/osipi/osipi_tables_metadata.csv \
  --output-dir dlt_notebooks/osipi \
  --connector-path /Workspace/Users/<you>@company.com/connectors \
  --generate-all-groups

python tools/notebook_based_deployment/generate_dab_yaml_notebooks.py \
  --connector-name osipi \
  --input-csv tools/notebook_based_deployment/examples/osipi/osipi_tables_metadata.csv \
  --output-yaml tools/notebook_based_deployment/dab_template/resources/osipi_pipelines.yml \
  --notebook-base-path /Workspace/Users/<you>@company.com/osipi_dlt_pipelines \
  --dest-catalog osipi \
  --dest-schema bronze \
  --emit-jobs
```


## Table discovery / classification (optional)

You can generate a CSV automatically from connector metadata:

```bash
python tools/notebook_based_deployment/discover_and_classify_tables.py \
  --connector-name osipi \
  --output-csv /tmp/osipi_classified.csv \
  --dest-catalog osipi \
  --dest-schema bronzeosipi \
  --group-by category_and_ingestion_type
```


Example CSVs in this folder:
- `osipi_by_category_and_ingestion_type.csv`
- `osipi_auto_balance_no_groups.csv`
- `osipi_single_pipeline_minimal.csv`
