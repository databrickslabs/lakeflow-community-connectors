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

### Recommended developer flow

Two commands, split at the credential boundary. Phase 1 is fully automatable; Phase 2 needs a developer with credentials.

```
# Phase 1: research, implement, simulator spec, simulate-mode tests, PR (no credentials)
/develop-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]

# Phase 2: authenticate, run record-mode tests, optionally deploy
/validate-connector <source_name>
```

To develop multiple connectors back-to-back without supervision:

```
/batch-develop-connectors source1 source2 source3 ...
```

Each connector lands as a separate PR; failures don't abort the rest of the batch.

### Self-review checkpoint (optional)

Before requesting human review, run the self-review skill for a scored audit of the connector — implementation, test coverage, simulator validation, security smells, doc consistency:

```
/self-review-connector for {source}
```

The skill writes a `SELF_REVIEW.md` next to the connector's tests, posts it as a sticky comment on the open PR, and adds the `connector-self-reviewed` label. The repo's CI requires that label to merge. The skill does **not** modify connector code.

### One-shot flow (experimental)

> **Not recommended for developers.** `/create-connector` blocks on credential collection between research and implementation, which prevents batch automation and forces the developer to be present throughout. It exists as a single-command option for end users who want to build, authenticate, validate, and deploy a connector in one continuous interactive session. For most development work, use `/develop-connector` + `/validate-connector` instead.

```
/create-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]
```

### Individual skills

The skills below are the building blocks the commands above orchestrate. Invoke them individually when you want manual control over a single step. Replace `{source}` with your connector name.

| Skill | Description |
|-------|-------------|
| `/research-source-api for {source}` | Research the source system's READ APIs and produce the API doc |
| `/authenticate-source for {source}` | Collect credentials via a browser form and write `dev_config.json` |
| `/implement-connector for {source}` | Implement the connector against the `LakeflowConnect` interface |
| `/test-and-fix-connector for {source}` | Run the connector's pytest suite and fix failures (simulate or record mode) |
| `/create-connector-document for {source}` | Generate the public-facing connector documentation |
| `/generate-connector-spec for {source}` | Finalize the connector spec YAML |
| `/deploy-connector for {source}` | Build and deploy the connector as a pipeline |
| `/research-write-api-of-source for {source}` | Research the source's write APIs (for write-back testing) |
| `/write-back-testing for {source}` | Implement write-back tests against a real source |

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
        |___ commands/           # Slash commands (e.g., /develop-connector, /validate-connector)
```
