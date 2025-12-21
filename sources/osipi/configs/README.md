# OSIPI dev configs

These files are used by the repo test harness (`pytest`) to run the OSI PI connector locally.

- `dev_config.json`: **connection-level** options (host + credentials)
- `dev_table_config.json`: **per-table** options (e.g., filters, lookback windows)

## Important

- Do **not** commit real credentials.
- `dev_config.json` is intended for local validation and should contain placeholders only. Use an appropriate secrets manager or Unity Catalog connection configuration for operational deployments.


