# CLAUDE.md

Lakeflow Community Connectors — data ingestion from source systems into Databricks via Spark Python Data Source API and Spark Declarative Pipeline (SDP).

## Key Constraint

When developing a connector, only modify files under `src/databricks/labs/community_connector/sources/{source}/`. Do **not** change library, pipeline, or interface code unless explicitly asked.

## Reference Files

- **Base interface**: `src/databricks/labs/community_connector/interface/lakeflow_connect.py`
- **Reference connector**: `src/databricks/labs/community_connector/sources/example/example.py`
- **Reference test**: `tests/unit/sources/example/test_example_lakeflow_connect.py`
- **Test harness**: `tests/unit/sources/test_suite.py` (`LakeflowConnectTester`)

## Testing

- Tests connect to real source systems — never mock data.
- Credentials: `tests/unit/sources/{source}/configs/dev_config.json`
- Write-back testing: `tests/unit/sources/lakeflow_connect_test_utils.py`

## Workflow

To create a connector end-to-end, follow `.claude/commands/create-connector.md`.
