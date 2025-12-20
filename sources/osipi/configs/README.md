# OSIPI dev configs (local testing only)

These files are used by the repo test harness (`pytest`) to run the OSI PI connector locally.

- `dev_config.json`: **connection-level** options (host + credentials)
- `dev_table_config.json`: **per-table** options (e.g., filters, lookback windows)

## Important

- Do **not** commit real credentials.
- For external submissions, follow the hackathon guidance and **remove** `dev_config.json` after testing.


