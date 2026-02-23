# Create-Connector Agent Memory

## Project Conventions
- Source connectors live under: src/databricks/labs/community_connector/sources/{source_name}/
- Each connector requires: {source_name}.py, README.md, connector_spec.yaml
- Tests live under: tests/unit/sources/{source_name}/
- Dev config (credentials) lives at: tests/unit/sources/{source_name}/configs/dev_config.json
- Auth test lives at: tests/unit/sources/{source_name}/auth_test.py
- Connector tests: tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py

## Phase Execution Notes
- connector-auth-guide requires interactive step: user runs `python ./tools/scripts/authenticate.py -s {source_name}`
- All 6 phases must complete in order; Phase 2 is skipped on subsequent batches
- Subagents are defined in .claude/agents/ directory

## Known Working Connectors (for reference patterns)
- zendesk: good reference for REST API with pagination
- stripe: good reference for cursor-based incremental
- github: good reference for GraphQL (similar to monday.com)
