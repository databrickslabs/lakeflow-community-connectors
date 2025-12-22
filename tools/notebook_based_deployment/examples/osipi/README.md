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
  --output-yaml tools/ingestion_dab_generator/dab_template/resources/osipi_pipelines.yml \
  --notebook-base-path /Workspace/Users/<you>@company.com/osipi_dlt_pipelines \
  --dest-catalog osipi \
  --dest-schema bronze \
  --emit-jobs
```
