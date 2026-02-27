# CLAUDE.md

## Project Overview

Lakeflow Community Connectors enable data ingestion from various source systems into Databricks. Built on the Spark Python Data Source API and Spark Declarative Pipeline (SDP).

## Project Structure

```
src/databricks/labs/community_connector/
  interface/             # LakeflowConnect base interface
  sources/               # Source connectors (github/, zendesk/, stripe/, etc.)
    {source}/            # Each connector has: {source}.py, README.md
  libs/                  # Shared utilities (spec_parser.py, utils.py, source_loader.py)
  pipeline/              # SDP orchestration (ingestion_pipeline.py)
  sparkpds/              # PySpark Data Source generic implementation and registry API. 
tools/
  community_connector/   # CLI tool to set up and run community connectors in Databricks workspace.
  scripts/               # Build tools (merge_python_source.py)
tests/
  unit/
    libs/                # Unit tests for shared libs
    pipeline/            # Unit tests for pipeline
    sources/             # Connector tests and test utilities
      {source}/          # Per-connector test files
      test_suite.py      # Shared test harness
      test_utils.py      # Test utilities
      lakeflow_connect_test_utils.py  # Write-back test utilities
prompts/                 # Templates and guide for AI-assisted development
.claude/skills/          # Claude skill files (development workflow steps)
.claude/agents/          # Claude subagent that handles different phases of connector development
```

## Core Interface

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


