## OSIPI sample output script

This README describes how to use `print_osipi_samples.py` to show what the OSIPI connector can retrieve.

### What it does

- Instantiates `LakeflowConnect` directly (no Spark required)
- Optionally mints a Databricks OIDC Bearer token for a Databricks Apps-protected endpoint
- Iterates tables and prints:
  - `table_options` used
  - `next_offset`
  - up to **N sample records** (default 5)
- With `--verbose`, prints step-by-step telemetry (token minting, probe, per-table timing)

### Example command

```bash
python3 sources/osipi/test/print_osipi_samples.py \
  --pi-base-url https://<pi-web-api-host> \
  --mint-databricks-app-token \
  --probe \
  --verbose \
  --max-records 5
```

### Parameters

- `--pi-base-url`: base URL of PI Web API
- `--access-token`: provide a Bearer token directly (skips minting)
- `--mint-databricks-app-token`: mint a token via Databricks workspace OIDC using secrets in scope `sp-osipi`
- `--max-records`: records per table to print (default 5)
- `--tables`: comma-separated subset of tables to print (default: all tables)
- `--probe`: runs a lightweight `GET /piwebapi/dataservers` check before reading tables
- `--verbose`: prints telemetry logs

### Token minting requirements

`--mint-databricks-app-token` requires:
- `~/.databrickscfg` profile `DEFAULT` (or `--databricks-profile`) to access your workspace
- secret scope `sp-osipi` with keys:
  - `sp-client-id`
  - `sp-client-secret`

