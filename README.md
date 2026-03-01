# Lakeflow Community Connectors

Lakeflow community connectors are built on top of the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) and [Spark Declarative Pipeline (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). These connectors enable users to ingest data from various source systems.

> **Note**: Lakeflow community connectors provide access to additional data sources beyond Databricks managed connectors. They are maintained by community contributors and are not subject to official Databricks SLAs, certifications, or guaranteed compatibility.

## Interface and Testing

Two implementation approaches (see [interface README](src/databricks/labs/community_connector/interface/README.md)):

- **[`LakeflowConnect`](src/databricks/labs/community_connector/interface/lakeflow_connect.py) (recommended)** — Define methods for listing tables, schemas, and reading records. Shared libraries handle Spark PDS, streaming, and offset management automatically.

  > **Note**: All built-in AI-assisted development workflows (commands, skills, agents) apply exclusively to this approach.
- **Direct [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)** — Full control over partitioning, schemas, and read logic; requires manual Spark API contract implementation.

Tests run against live source environments (no mocks):
- **Generic test suite** — End-to-end validation using real credentials
- **Unit tests** — For complex connector-specific logic
- **Write-back testing** *(recommended)* — Write data, read it back, verify incremental reads and deletes

## Develop a New Connector

Build connectors with AI-assisted workflows using [Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Cursor](https://www.cursor.com/). All commands and skills are defined under `.claude/` and auto-discovered by both tools.

```bash
git clone https://github.com/databrickslabs/lakeflow-community-connectors.git
```

### One-Command Agent

A single command orchestrates the entire workflow — API research through deployment:

```
/create-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]
```

The agent pauses once to collect credentials for source authentication.

### Step-by-Step Skills

For more control, run each step individually. Replace `{source}` with your connector name.

| Step | Command |
|------|---------|
| 1. Research source READ APIs | `/research-source-api for {source}` |
| 2. Collect credentials | `/authenticate-source for {source}` |
| 3. Implement connector | `/implement-connector for {source}` |
| 4. Run tests and fix failures | `/test-and-fix-connector for {source}` |
| 5a. Generate documentation | `/create-connector-document for {source}` |
| 5b. Finalize connector spec | `/generate-connector-spec for {source}` |
| 6. Build and deploy | `/deploy-connector for {source}` |

**Optional: Write-Back Testing** — Run between steps 4 and 5. Skip for read-only sources or when writes are expensive/risky.

| Step | Command |
|------|---------|
| Research write APIs | `/research-write-api-of-source for {source}` |
| Implement write-back tests | `/write-back-testing for {source}` |
| Validate incremental sync | `/validate-incremental-sync for {source}` |

## Deploy and Run

Each connector runs as a configurable SDP pipeline. Define a **pipeline spec** to configure tables and destinations.

- **Databricks UI** — Click **"+New"** > **"Add or upload data"** > select the source under **"Community connectors"**. For custom connectors from your own repo, select **"+ Add Community Connector"**.
- **CLI tool** — Run `/deploy-connector` in Cursor or Claude Code for guided deployment, or use the CLI directly. See [tools/community_connector](tools/community_connector/README.md).

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
|___ templates/                  # Templates and guide for AI-assisted development
|
|___ .claude/                    # AI-assisted development (auto-discovered by Claude Code and Cursor)
        |___ skills/             # Skill files for each workflow step
        |___ agents/             # Subagents for different development phases
        |___ commands/           # Slash commands (e.g., /create-connector)
```
