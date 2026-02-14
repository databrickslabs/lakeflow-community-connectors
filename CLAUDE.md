# CLAUDE.md

This file provides guidance for Claude Code when working with this repository.

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
prompts/                 # Templates for AI-assisted development
```

## Core Interface

All connectors implement the `LakeflowConnect` class in `src/databricks/labs/community_connector/interface/lakeflow_connect.py`:

```python
class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """Initialize with connection parameters (auth tokens, configs, etc.)"""

    def list_tables(self) -> list[str]:
        """Return names of all tables supported by this connector."""

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the Spark schema for a table."""

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return metadata: primary_keys, cursor_field, ingestion_type (snapshot|cdc|cdc_with_deletes|append)."""

    def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """Yield records as JSON dicts and return the next offset for incremental reads."""

    def read_table_deletes(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """Optional: Yield deleted records for delete synchronization. Only required if ingestion_type is 'cdc_with_deletes'."""
```

## Build & Test Commands

```bash
# Run tests for a specific connector
pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v

# Run all unit tests
pytest tests/unit/ -v

# Generate deployable file (temporary workaround)
python tools/scripts/merge_python_source.py {source_name}
```

## Development Workflow

1. **Understand the source** — Gather API specs, auth mechanisms, and schemas using the provided template
2. **Implement the connector** — Implement the `LakeflowConnect` interface methods
3. **Test & iterate** — Run the standard test suites against a real source system
   - *(Optional)* Implement write-back testing for end-to-end validation (write → read → verify cycle)
4. **Generate documentation** — Create user-facing docs using the documentation template
   - *(Temporary)* Run `tools/scripts/merge_python_source.py` to generate the deployable file

## Implementation Guidelines

- When developing a new connector, only modify `src/databricks/labs/community_connector/sources/{source_name}/{source_name}.py` — do **not** change the library, pipeline, or interface code.
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
- `prompts/README.md` - Detailed development guide
- `prompts/templates/source_api_doc_template.md` - API documentation template
- `prompts/templates/community_connector_doc_template.md` - User documentation template

