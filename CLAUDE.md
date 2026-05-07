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

- Tests run against an in-process simulator by default (no creds, no network). Live runs against a real source happen on demand for cassette refresh / regression triage.
- Live-mode credentials are supplied per run via `CONNECTOR_TEST_CONFIG_JSON` (inline JSON env var) or `CONNECTOR_TEST_CONFIG_PATH` (path to a JSON file at any local path the developer picks). There is no canonical on-disk credentials path in the repo.
- Stand-in (simulate / replay) credentials are declared on the test class via `replay_config = {...}`; the simulator never validates them.
- Write-back testing: `tests/unit/sources/lakeflow_connect_test_utils.py`

## Workflow

To create a connector end-to-end, follow `.claude/commands/create-connector.md`.
