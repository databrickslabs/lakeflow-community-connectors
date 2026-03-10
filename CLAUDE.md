# CLAUDE.md

<<<<<<< HEAD
## Project Overview
=======
Lakeflow Community Connectors — data ingestion from source systems into Databricks via Spark Python Data Source API and Spark Declarative Pipeline (SDP).

## Key Constraint
>>>>>>> origin

When developing a connector, only modify files under `src/databricks/labs/community_connector/sources/{source}/`. Do **not** change library, pipeline, or interface code unless explicitly asked.

## Reference Files

- **Base interface**: `src/databricks/labs/community_connector/interface/lakeflow_connect.py`
- **Reference connector**: `src/databricks/labs/community_connector/sources/example/example.py`
- **Reference test**: `tests/unit/sources/example/test_example_lakeflow_connect.py`
- **Test harness**: `tests/unit/sources/test_suite.py` (`LakeflowConnectTester`)

## Testing

<<<<<<< HEAD
All connectors implement the `LakeflowConnect` class in `src/databricks/labs/community_connector/interface/lakeflow_connect.py`:

## Development Workflow

Refer `.claude/agents/create-connector.md`

## Implementation Guidelines

- When developing a new connector, only modify `src/databricks/labs/community_connector/sources/{source_name}` — do **not** change the library, pipeline, or interface code.
- Shared code (libs, pipeline, interface) should only be updated when explicitly instructed to add new features or improvements to the framework itself.

## Testing Conventions

- Tests use `tests/unit/sources/test_suite.py` via `LakeflowConnectTester`
- Load credentials from `tests/unit/sources/{source_name}/configs/dev_config.json`
- Never mock data - tests connect to real source systems
- Optional write-back testing via `LakeflowConnectTestUtils` in `tests/unit/sources/lakeflow_connect_test_utils.py`

## Key Files to Reference

- `src/databricks/labs/community_connector/interface/lakeflow_connect.py` - Base interface definition
- `src/databricks/labs/community_connector/sources/zendesk/zendesk.py` - Reference implementation
- `src/databricks/labs/community_connector/sources/example/example.py` - Reference implementation
- `tests/unit/sources/test_suite.py` - Test harness
- `tests/unit/sources/example/test_example_lakeflow_connect.py` - Reference test implementation
=======
- Tests connect to real source systems — never mock data.
- Credentials: `tests/unit/sources/{source}/configs/dev_config.json`
- Write-back testing: `tests/unit/sources/lakeflow_connect_test_utils.py`
>>>>>>> origin

## Workflow

To create a connector end-to-end, follow `.claude/commands/create-connector.md`.
