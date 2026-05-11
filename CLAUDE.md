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

The recommended developer flow is two-phase and decouples development from authentication:

1. `/develop-connector <source_name>` — Phase 1: research, implementation, simulator spec, simulate-mode tests, docs, and a PR labeled `needs-live-testing`. No credentials required.
2. `/validate-connector <source_name>` — Phase 2: collect credentials, run record-mode tests against the live source, optionally deploy.

For batch development across many connectors, use `/batch-develop-connectors`. Before requesting human review, run `/self-review-connector for {source}` for a scored audit (the repo's CI requires the resulting `connector-self-reviewed` label to merge).

The legacy `/create-connector` command still exists as a one-shot interactive flow for end users; it is **not recommended for developers** because it blocks on credential collection between research and implementation.
