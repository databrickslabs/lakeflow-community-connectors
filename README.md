# Lakeflow Community Connectors

Lakeflow community connectors are built on top of the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) and [Spark Declarative Pipeline (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). These connectors enable users to ingest data from various source systems.

Each connector is packaged as Python source code that defines a configurable SDP, which consists of 4 parts:
1. Source connector implementation, following a predefined API
2. Pipeline spec, defined as a Pydantic class
3. Configurable ingestion pipeline definition
4. Shared utilities and libraries that package the source implementation together with the pipeline

Developers only need to implement or modify the source connector logic, while connector users configure ingestion behavior by updating the pipeline spec.

## Project Structure

- `sources/` — Source connectors (e.g., `github/`, `zendesk/`, `stripe/`). The `interface/` subfolder defines the `LakeflowConnect` base interface.
- `libs/` — Shared utilities for data type parsing, spec parsing, and module loading
- `pipeline/` — Core ingestion logic: PySpark Data Source implementation and SDP orchestration
- `scripts/` — Build tools for merging connectors into deployable files
- `tests/` — Generic test suites for validating connector implementations
- `prompts/` — Templates for AI-assisted connector development

## Developing New Connectors

Follow the instructions in [`prompts/vibe_coding_instruction.md`](prompts/vibe_coding_instruction.md) to create new connectors. The development workflow:

1. **Understand the source** — Gather API specs, auth mechanisms, and schemas using the provided template
2. **Implement the connector** — Implement the `LakeflowConnect` interface methods
3. **Test & iterate** — Run the standard test suites against a real source system
   - *(Optional)* Implement write-back testing for end-to-end validation (write → read → verify cycle)
4. **Generate documentation** — Create user-facing docs using the documentation template
   - *(Temporary)* Run `scripts/merge_python_source.py` to generate the deployable file

### API to Implement
Connectors are built on the [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html), with an abstraction layer (`LakeflowConnect`) that simplifies development. 
Developers can also choose to directly implement Python Data Source API (not recommended) as long as the implementation meets the API contracts of the community connectors.

**Please see more details under** [`sources/interface/README.md`](sources/interface/README.md).

```python
class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """Initialize with connection parameters (auth tokens, configs, etc.)"""

    def list_tables(self) -> list[str]:
        """Return names of all tables supported by this connector."""

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the Spark schema for a table."""

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """Return metadata: primary_keys, cursor_field, ingestion_type (snapshot|cdc|append)."""

    def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """Yield records as JSON dicts and return the next offset for incremental reads."""
```

### Tests

Each connector must include tests that run the **generic test suite** against a live source environment. These tests validate API usage, data parsing, and successful data retrieval.

- **Generic test suite** — Connects to a real source using provided credentials to verify end-to-end functionality
- **Write-back testing** *(recommended)* — Use the provided test harness to write data, read it back, and verify incremental reads work correctly
- **Unit tests** — Recommended for complex library code or connector-specific logic

## Using Community Connectors

Each connector runs as a configurable SDP. Define a **pipeline spec** to specify which tables to ingest and where to store them. See more details in this [example](pipeline-spec/example_ingest.py).

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "github"  # or "zendesk", "stripe", etc.
pipeline_spec = {
    "connection_name": "my_github_connection",
    "objects": [
        {"table": {"source_table": "pulls"}},
        {"table": {"source_table": "issues", "destination_table": "github_issues"}},
    ],
}

# Register the source and run ingestion
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Pipeline Spec Reference

- `connection_name` *(required)* — Unity Catalog connection name
- `objects` *(required)* — List of tables to ingest, each containing:
  - `table` — Table configuration object:
    - `source_table` *(required)* — Table name in the source system
    - `destination_catalog` — Target catalog (defaults to pipeline's default)
    - `destination_schema` — Target schema (defaults to pipeline's default)
    - `destination_table` — Target table name (defaults to `source_table`)
    - `table_configuration` — Additional options:
      - `scd_type` — `SCD_TYPE_1` (default), `SCD_TYPE_2`, or `APPEND_ONLY`
      - `primary_keys` — List of columns to override connector's default keys
      - Other source-specific options (see each connector's README)