# Community Connector CLI

A command-line tool for setting up and running Lakeflow community connectors in Databricks workspaces. This CLI provides functionality equivalent to the UI flow of community connectors on Databricks "Add Data" page.

## Prerequisites
You must have access to a Databricks workspace to set up and run Lakeflow community connectors:
- Production/Enterprise Workspace: Recommended for production migrations
- Development Workspace: Suitable for testing and development
- Free Databricks Workspace: Available at [databricks.com/try-databricks](databricks.com/try-databricks) - Perfect for evaluation and learning

### CLI Configuration & Authentication
The CLI is built using the Databricks Python SDK so it shares the same authentication mechanism as Databricks CLI tool. So it is recommended to install Databricks CLI on your machine. Refer to the installation instructions provided for Linux, MacOS, and Windows, available [here](https://docs.databricks.com/aws/en/dev-tools/cli/install#install-or-update-the-databricks-cli).

**Authentication Options**:
- Personal Access Token (PAT): Recommended for individual users - Generate from User Settings → Developer → Access Tokens
- Service Principal: Recommended for automated/production deployments - Requires admin privileges to create

**Configuration Profile**:
It is recommended to use a configuration profile with the CLI tool, just as you would with the Databricks CLI.
- [How to create configuration profile](https://docs.databricks.com/aws/en/dev-tools/auth/config-profiles)
- [How to use configuration profile](https://docs.databricks.com/aws/en/dev-tools/cli/profiles)


## Installation

```bash
cd tools/community_connector
pip install -e .
```

This installs the `community-connector` command globally.


## Commands

### `create_pipeline`

Create a community connector pipeline with a Git repo and DLT pipeline.

```bash
# Basic usage with connection name (uses default template)
community-connector create_pipeline github my_github_pipeline -n my_github_conn

# With catalog and schema
community-connector create_pipeline stripe my_stripe_pipeline -n stripe_conn \
  --catalog main --target raw_data

# With pipeline spec file
community-connector create_pipeline github my_pipeline -ps pipeline_spec.yaml

# With inline JSON spec
community-connector create_pipeline github my_pipeline \
  -ps '{"connection_name": "my_conn", "objects": [{"table": {"source_table": "users"}}]}'
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--connection-name` | `-n` | UC connection name (required if no `--pipeline-spec`) |
| `--pipeline-spec` | `-ps` | Pipeline spec as JSON string or path to .yaml/.json file |
| `--catalog` | `-c` | Unity Catalog name for the pipeline |
| `--target` | `-t` | Target schema for the pipeline |
| `--repo-url` | `-r` | Git repository URL (overrides default) |
| `--config` | `-f` | Path to custom config file |

### `run_pipeline`

Run an existing pipeline by name.

```bash
# Basic run
community-connector run_pipeline my_github_pipeline

# Full refresh
community-connector run_pipeline my_github_pipeline --full-refresh
```

### `show_pipeline`

Show status of a pipeline.

```bash
community-connector show_pipeline my_github_pipeline
```

### `create_connection`

Create a Unity Catalog connection for Lakeflow connectors.

The CLI validates connection options against the connector spec (`connector_spec.yaml`) and automatically configures the `externalOptionsAllowList` based on:
1. Source-specific options from the connector spec
2. Common options from the CLI's default config

```bash
# Basic usage - options are validated and externalOptionsAllowList is auto-configured
community-connector create_connection github my_github_conn \
  -o '{"token": "ghp_xxxx"}'

# With a local connector spec file
community-connector create_connection github my_github_conn \
  -o '{"token": "ghp_xxxx"}' --spec ./my_connector_spec.yaml

# With a custom GitHub repo (fetches spec from sources/{source}/connector_spec.yaml)
community-connector create_connection github my_github_conn \
  -o '{"token": "ghp_xxxx"}' --spec https://github.com/myorg/my-connectors
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--options` | `-o` | Connection options as JSON string (required) |
| `--spec` | `-s` | Optional: local path to connector_spec.yaml, or a GitHub repo URL |

**Validation:**
- Required connection parameters (from spec) must be provided
- Unknown parameters generate a warning
- `externalOptionsAllowList` is automatically set if not provided

### `update_connection`

Update an existing Unity Catalog connection.

```bash
# Basic usage - same validation and auto-configuration as create
community-connector update_connection github my_github_conn \
  -o '{"token": "ghp_new_token"}'

# With custom spec
community-connector update_connection github my_github_conn \
  -o '{"token": "ghp_xxxx"}' --spec ./my_connector_spec.yaml
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--options` | `-o` | Connection options as JSON string (required) |
| `--spec` | `-s` | Optional: local path to connector_spec.yaml, or a GitHub repo URL |

## Pipeline Spec Format

The pipeline spec defines which tables to ingest and how:

```yaml
# pipeline_spec.yaml
connection_name: my_github_conn
objects:
  # Minimal config
  - table:
      source_table: users

  # Full config
  - table:
      source_table: repos
      destination_catalog: main
      destination_schema: github_raw
      destination_table: repositories
      table_configuration:
        scd_type: SCD_TYPE_2
        primary_keys:
          - id
        owner: my-org  # Source-specific option
```

### Spec Reference

| Field | Required | Description |
|-------|----------|-------------|
| `connection_name` | Yes | Unity Catalog connection name |
| `objects` | Yes | List of tables to ingest |
| `objects[].table.source_table` | Yes | Table name in the source system |
| `objects[].table.destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `objects[].table.destination_schema` | No | Target schema (defaults to pipeline's default) |
| `objects[].table.destination_table` | No | Target table name (defaults to source_table) |
| `objects[].table.table_configuration.scd_type` | No | `SCD_TYPE_1`, `SCD_TYPE_2`, or `APPEND_ONLY` |
| `objects[].table.table_configuration.primary_keys` | No | List of primary key columns |

## Configuration

The CLI uses a layered configuration system:

**Precedence (highest to lowest):**
1. CLI arguments
2. Custom config file (`--config`)
3. Bundled `default_config.yaml`

### Default Configuration

The bundled defaults include:
- Workspace path: `/Users/{CURRENT_USER}/.lakeflow_community_connectors/{PIPELINE_NAME}`
- Git repo: `https://github.com/databrickslabs/lakeflow-community-connectors.git`
- Sparse checkout patterns for connector source files
- Serverless mode enabled
- Development mode enabled
- Connection external options allowlist: Common table config options (e.g., `tableName`, `tableNameList`, `isDeleteFlow`)

### Custom Config File

Override defaults with a custom YAML file:

```yaml
# my_config.yaml
workspace_path: "/Shared/connectors/{PIPELINE_NAME}"

repo:
  url: https://github.com/myorg/my-connectors.git
  branch: develop

pipeline:
  serverless: false
  development: false

connection:
  # Additional options to include in externalOptionsAllowList for all connectors
  external_options_allowlist: "tableName,tableNameList,isDeleteFlow,customOption"
```

Use with:
```bash
community-connector create_pipeline github my_pipeline -n conn --config my_config.yaml
```

## Debug Mode

Enable debug output with the `--debug` flag:

```bash
community-connector --debug create_pipeline github my_pipeline -n my_conn
```

## Development

### Running Tests

```bash
cd tools/community_connector
pip install -e ".[dev]"
pytest tests/ -v
```

### Project Structure

The main source code is organized under `src/databricks/labs/community_connector/`:
- `libs/` — Shared utilities (spec_parser.py, utils.py, source_loader.py)
- `pipeline/` — Core ingestion logic (PySpark Data Source, SDP orchestration)
- `sources/` — Source connectors (github/, zendesk/, stripe/, etc.)
  - `interface/` — LakeflowConnect base interface
  - `{source}/` — Each connector has: {source}.py, README.md, test/, configs/

```
tools/community_connector/
├── src/databricks/labs/community_connector/
│   ├── cli.py                    # Main CLI entry point
│   ├── config.py                 # Configuration management
│   ├── repo_client.py            # Databricks Repos API client
│   ├── pipeline_client.py        # Databricks Pipelines API client
│   ├── pipeline_spec_validator.py # Pipeline spec validation
│   ├── default_config.yaml       # Bundled default configuration
│   └── templates/
│       ├── ingest_template.py      # Template for ingest.py (with examples)
│       └── ingest_template_base.py # Base template (for --pipeline-spec)
├── tests/
│   ├── test_cli.py
│   ├── test_clients.py
│   └── test_pipeline_spec_validator.py
├── pyproject.toml
└── README.md
```

## License

See the repository root for license information.
