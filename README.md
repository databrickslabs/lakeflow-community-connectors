# Lakeflow Community Connectors

**Note**: Lakeflow community connectors provide access to additional data sources beyond Databricks managed connectors.  
They are maintained by community contributors and are not subject to official Databricks SLAs, certifications, or guaranteed compatibility.

Lakeflow community connectors are built on top of the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) and [Spark Declarative Pipeline (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). These connectors enable users to ingest data from various source systems.

Each connector is packaged as Python source code that defines a configurable SDP, which consists of 4 parts:
1. Source connector implementation, following a predefined API
2. Pipeline spec, defined as a Pydantic class
3. Configurable ingestion pipeline definition
4. Shared utilities and libraries that package the source implementation together with the pipeline

Developers only need to implement or modify the source connector logic, while connector users configure ingestion behavior by updating the pipeline spec.

## Project Structure

Core modules live under `src/databricks/labs/community_connector/`:
- `interface/` — The `LakeflowConnect` base interface definition
- `sources/` — Source connectors (e.g., `github/`, `zendesk/`, `stripe/`)
- `sparkpds/` — PySpark Data Source implementation (`lakeflow_datasource.py`, `registry.py`)
- `libs/` — Shared utilities for data type parsing, spec parsing, and module loading
- `pipeline/` — Core ingestion logic: PySpark Data Source implementation and SDP orchestration

Other directories:
- `tools/` — Tools to build and deploy community connectors
- `tests/` — Generic test suites for validating connector implementations
- `prompts/` — Templates and guide for AI-assisted connector development
- `.claude/skills/` — Claude skill files for each development workflow step
- `.claude/agents/` - Claude subagents that handle different phases of connector development

## Developing New Connectors

Follow the instructions in [`prompts/README.md`](prompts/README.md) to create new connectors. The development workflow:

1. **Understand the source** — Research API specs, auth mechanisms, and schemas using the provided template
2. **Auth setup** — Generate the connection spec, configure credentials, and verify connectivity
3. **Implement the connector** — Implement the `LakeflowConnect` interface methods
4. **Test & iterate** — Run the standard test suites against a real source system
   - *(Optional)* Implement write-back testing for end-to-end validation (write → read → verify cycle)
5. **Generate documentation** — Create user-facing docs and connector spec
   - Create the public-facing README using the documentation template
   - Generate the connector spec YAML file (connection parameters and allowlist options)
6. **Build & deploy** — Build the connector as a Python package and generate the deployable file
   - *(Temporary)* Run `tools/scripts/merge_python_source.py` to generate the single-file deployment artifact

### AI-Assisted Development with Claude
Each step of the development workflow is packaged as a **skill** under `.claude/skills/`, which can be triggered individually. See [`prompts/README.md`](prompts/README.md) for the full step-by-step guide.

For a fully autonomous experience, the entire workflow is also wrapped as a **create-connector agent** (`.claude/agents/create-connector.md`) that orchestrates all phases end-to-end — from API research through auth setup, implementation, testing, documentation, and packaging — without manual intervention at each stage. Developers can invoke this agent directly and let it build a complete connector autonomously.

### API to Implement
Connectors are built on the [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html), with an abstraction layer (`LakeflowConnect`) that simplifies development. 
Developers can also choose to directly implement Python Data Source API (not recommended) as long as the implementation meets the API contracts of the community connectors.

**Please see more details under** [`src/databricks/labs/community_connector/interface/README.md`](src/databricks/labs/community_connector/interface/README.md).

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

### Tests

Each connector must include tests that run the **generic test suite** against a live source environment. These tests validate API usage, data parsing, and successful data retrieval.

- **Generic test suite** — Connects to a real source using provided credentials to verify end-to-end functionality
- **Write-back testing** *(recommended)* — Use the provided test harness to write data, read it back, and verify incremental reads and deletes(only for tables with ingestion type `cdc_with_deletes`) work correctly.
- **Unit tests** — Recommended for complex library code or connector-specific logic

## Using and Testing Community Connectors

Each connector runs as a configurable SDP. Define a **pipeline spec** to specify which tables to ingest and where to store them. See more details in this [example](pipeline-spec/example_ingest.py). You don't need to manually create files below, as both UI and CLI tool will automatically generate these files when setting the connector.

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

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

There are two ways to set up and run the community connectors. By default, the source code from the main repository (databrickslabs/lakeflow-community-connectors) is used to run the community connector. However, both methods described below allow you to override this by using your own Git repository, which should be cloned from the main repository.

### Databricks UI
On Databricks main page, click **“+New”** -> **“Add or upload data”**, and then select the source under **“Community connectors”**.
If you are using a custom connector from your own Git repository, select **"+ Add Community Connector"**.

### CLI tool
The **"community-connector"** CLI tool provides functionality equivalent to the UI. While access to a Databricks workspace is still required, this tool is particularly useful for validating and testing connectors during the development phase.

See more details at [tools/community_connector](tools/community_connector/README.md)
 

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