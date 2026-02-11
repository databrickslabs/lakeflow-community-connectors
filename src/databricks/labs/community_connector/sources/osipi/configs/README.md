# OSIPI Configuration Templates

This directory contains example configuration templates for the OSI PI connector.

- `dev_config.example.json`: Template for connection-level options (host + credentials)

## For Testing

Test configurations are located in `tests/unit/sources/osipi/configs/`:
- `dev_config.json`: Connection-level options (gitignored, create locally)
- `dev_table_config.json`: Per-table options (e.g., filters, lookback windows)

## Important

- Do **not** commit real credentials.
- Use an appropriate secrets manager or Unity Catalog connection configuration for operational deployments.
