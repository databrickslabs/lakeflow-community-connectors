# Lakeflow Community Connectors

**Note**: Lakeflow community connectors provide access to additional data sources beyond Databricks managed connectors. They are maintained by community contributors and are not subject to official Databricks SLAs, certifications, or guaranteed compatibility.

Lakeflow community connectors are built on top of the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) and [Spark Declarative Pipeline (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). These connectors enable users to ingest data from various source systems.

Each connector is packaged as Python source code with 4 parts:
1. Source connector implementation, following a predefined API
2. Shared and configurable SDP pipeline 
3. Shared libraries that integrate source connectors as Spark PDS and the pipeline
4. User-configured pipeline spec 

Developers **only need to implement the [source connector interface](src/databricks/labs/community_connector/interface/README.md)**, while connector users configure ingestion behavior by **configuring the pipeline spec**.

## Connector Interface and Testing

### Interface to Implement
There are two ways to implement a connector:

- **`LakeflowConnect` abstraction (recommended)** — Implement the [`LakeflowConnect`](src/databricks/labs/community_connector/interface/lakeflow_connect.py) abstract class. This is a lightweight interface where you define methods for listing tables, returning schemas and metadata, and reading records. The shared libraries handle all Spark PDS integration, streaming, offset management, and pipeline orchestration automatically. This is the recommended approach, especially for REST API-based sources.

- **Direct Python Data Source API** — Implement the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) directly. This gives full control over data partitioning, schema handling, and read logic, but requires manually implementing the Spark API contracts expected by the shared pipeline and libraries.

See [`src/databricks/labs/community_connector/interface/README.md`](src/databricks/labs/community_connector/interface/README.md) for full details on both approaches.

### Tests to Build

Each connector must include tests that run the **generic test suite** against a live source environment. These tests validate API usage, data parsing, and successful data retrieval.

- **Generic test suite** — Connects to a real source using provided credentials to verify end-to-end functionality
- **Write-back testing** *(recommended)* — Use the provided test harness to write data, read it back, and verify incremental reads and deletes (only for tables with ingestion type `cdc_with_deletes`) work correctly.
- **Unit tests** — Recommended for complex library code or connector-specific logic

## Develop a New Connector
Build and deploy connectors using the `LakeflowConnect` abstraction with AI-assisted workflows — either as a single guided session or step-by-step with full control. These workflows do not apply to direct Python Data Source API implementations.

### Prerequisites
- Access to [Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Cursor](https://www.cursor.com/)
- Clone the repo:
```bash
git clone https://github.com/databrickslabs/lakeflow-community-connectors.git
```

The development workflow is identical in both Claude Code and Cursor. All skills, agents, and commands are defined under `.claude/` and are automatically discovered by both tools.

### Develop via Agent

The simplest way to build a connector. A single command kicks off an autonomous agent that guides you through the entire workflow — from API research to deployment. Run the command below and it will orchestrate all steps end-to-end:
```
/create-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]
```
Example: `/create-connector github`

Note: the agent will pause once to ask you for credentials/tokens to authenticate with the source system.

### Develop via Skills

For more control over the development process, you can run each step individually as a skill command. This lets you iterate on specific steps more easily and customize the workflow to your needs. Replace `{source}` with your connector name.

**Step 1** — Research and document the source system's READ API endpoints.
```
/research-source-api for {source}
```

**Step 2** — Collect credentials and validate connectivity.
```
/authenticate-source for {source}
```

**Step 3** — Implement connector source code.
```
/implement-connector for {source}
```

**Step 4** — Run tests and fix any failures.
```
/test-and-fix-connector for {source}
```

**Step 5a** — Generate user-facing documentation.
```
/create-connector-document for {source}
```

**Step 5b** — Finalize the connector spec.
```
/generate-connector-spec for {source}
```

**Step 6** — Build and package the connector.
```
/build-connector-package for {source}
```

**Write-Back Testing (Optional)** — For end-to-end validation, run these between Step 4 and Step 5. Skip if the source is read-only, only production access is available, or write operations are expensive/risky.

Research write APIs (separate from Step 1's read-only research):
```
/research-write-api-of-source for {source}
```

Implement write-back test utilities:
```
/write-back-testing for {source}
```

**Validate Incremental Sync** - Manual validation process to verify CDC implementation by checking offset structure, validating offset matches max cursor, and testing incremental filtering.
```
/validate-incremental-sync for {source}
```

## Project Structure

```
lakeflow-community-connectors/
|
|___ src/databricks/labs/community_connector/   # Core modules
|       |___ interface/          # The interface each source connector needs to implement 
|       |___ sources/            # Source connectors
|       |       |___ github/     
|       |       |___ zendesk/
|       |       |___ stripe/
|       |       |___ ...         # Each connector: python code, docs, spec and etc. 
|       |___ sparkpds/           # PySpark Data Source implementation and registry
|       |___ libs/               # Shared utilities (spec parsing, data types, module loading)
|       |___ pipeline/           # SDP ingestion orchestration
|
|___ tests/                      # Test suites
|       |___ unit/
|               |___ sources/    # Per-connector tests + generic test harness
|               |___ libs/       # Shared library tests
|               |___ pipeline/   # Pipeline tests
|
|___ tools/                      # Build and deployment tooling
|       |___ community_connector/  # CLI tool for workspace setup and deployment
|       |___ scripts/              # Build scripts (e.g., merge_python_source.py)
|
|___ prompts/                    # Templates and guide for AI-assisted development
|
|___ .claude/                    # AI-assisted development (auto-discovered by Claude Code and Cursor)
        |___ skills/             # Skill files for each workflow step
        |___ agents/             # Subagents for different development phases
        |___ commands/           # Slash commands (e.g., /create-connector)
```

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