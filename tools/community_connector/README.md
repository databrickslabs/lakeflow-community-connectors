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

**Selecting a Profile**:
When `~/.databrickscfg` contains multiple profiles — especially several pointing at the same workspace host — the Databricks SDK cannot auto-pick one. Use the `DATABRICKS_CONFIG_PROFILE` environment variable to choose:

```bash
export DATABRICKS_CONFIG_PROFILE=my-profile
community-connector create_pipeline github my_pipeline -n my_conn
```

Or set it inline for a single command:

```bash
DATABRICKS_CONFIG_PROFILE=my-profile community-connector show_pipeline my_pipeline
```

When the variable is not set, the CLI falls back to the `[DEFAULT]` profile in `~/.databrickscfg`, so that case is unambiguous regardless of how many other profiles target the same host.


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

### `update_pipeline`

Update the `ingest.py` configuration for an existing pipeline. This command modifies only the pipeline spec in the workspace file; other pipeline settings remain unchanged.

```bash
# Update with a YAML spec file
community-connector update_pipeline my_github_pipeline -ps pipeline_spec.yaml

# Update with a JSON spec file
community-connector update_pipeline my_github_pipeline -ps pipeline_spec.json

# Update with inline JSON spec
community-connector update_pipeline my_github_pipeline \
  -ps '{"connection_name": "my_conn", "objects": [{"table": {"source_table": "users"}}]}'
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--pipeline-spec` | `-ps` | Pipeline spec as JSON string or path to .yaml/.json file (required) |

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

Create a Unity Catalog connection (type `COMMUNITY`) for Lakeflow connectors.

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

# With a custom GitHub repo (fetches spec from src/databricks/labs/community_connector/sources/{source}/connector_spec.yaml)
community-connector create_connection github my_github_conn \
  -o '{"token": "ghp_xxxx"}' --spec https://github.com/myorg/my-connectors
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--options` | `-o` | Connection options as JSON string (required) |
| `--auth-type` | | Authentication mode: `static`, `m2m`, `u2m`, or `u2m_per_user`. Optional — when omitted it is taken from the connector spec's `oauth.flow` (or `static` if the spec has no `oauth` block). Pass it only to override the spec. |
| `--redirect-port` | | Loopback port for the `u2m` OAuth redirect. Defaults to an OS-assigned free port. |
| `--spec` | `-s` | Optional: local path to connector_spec.yaml, or a GitHub repo URL |

**Validation:**
- Required connection parameters (from spec) must be provided
- Unknown parameters generate a warning
- `externalOptionsAllowList` is automatically set if not provided

#### Authentication modes

The `COMMUNITY` connection supports static credentials plus three OAuth 2.0 modes. The mode is selected by `--auth-type`, or derived from the connector spec's `connection.oauth.flow` when `--auth-type` is omitted. OAuth modes set the connection's `community_oauth_flow` automatically.

| Mode | What it is | Required options* |
|------|------------|-------------------|
| `static` | Arbitrary key/value credentials (token, API key, username/secret, …). No OAuth flow. | per connector spec |
| `m2m` | Client-credentials OAuth (no human). | `client_id`, `client_secret`, `token_endpoint` |
| `u2m` | One human authorizes once at connection creation. The CLI opens a browser, runs the authorization-code (+PKCE) flow against a `127.0.0.1` loopback redirect, and registers the captured grant. | `client_id`, `client_secret`, `authorization_endpoint`, `token_endpoint` |
| `u2m_per_user` | Each end user authorizes separately; UC resolves the right token per query at runtime. The connection stores only app config. | `client_id`, `client_secret`, `authorization_endpoint`, `token_endpoint` |

\* For OAuth modes the spec's `oauth` block supplies the endpoints and scope, so in practice the user passes only `client_id` + `client_secret`. The OAuth-issued token is injected into the connector at query time — it is never entered by hand. Spec keys `authorization_url` / `token_url` / `scopes` are accepted as aliases for the RFC 6749 names.

```bash
# OAuth connector whose spec declares oauth.flow — no --auth-type needed.
# A u2m flow opens a browser for the user to sign in and grant consent.
community-connector create_connection gmail my_gmail_conn \
  -o '{"client_id":"<id>.apps.googleusercontent.com","client_secret":"<secret>"}'

# Override the auth type explicitly (e.g. m2m), supplying the token endpoint:
community-connector create_connection github my_github_conn \
  --auth-type m2m \
  -o '{"client_id":"...","client_secret":"...","token_endpoint":"https://idp/token"}'
```

### `update_connection`

Update an existing Unity Catalog connection. The auth mode is fixed at creation time; for a `u2m` connection, re-running `update_connection` re-runs the browser authorization flow to refresh the OAuth grant.

```bash
# Basic usage - same validation and auto-configuration as create
community-connector update_connection github my_github_conn \
  -o '{"token": "ghp_new_token"}'

# With custom spec
community-connector update_connection github my_github_conn \
  -o '{"token": "ghp_xxxx"}' --spec ./my_connector_spec.yaml

# Refresh a u2m OAuth grant (re-opens the browser to re-consent)
community-connector update_connection gmail my_gmail_conn \
  -o '{"client_id":"...","client_secret":"..."}'
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--options` | `-o` | Connection options as JSON string (required) |
| `--auth-type` | | Authentication mode. Optional — defaults to the spec's `oauth.flow` (or `static`). Must match the connection's existing mode. |
| `--redirect-port` | | Loopback port for the `u2m` OAuth redirect. Defaults to an OS-assigned free port. |
| `--spec` | `-s` | Optional: local path to connector_spec.yaml, or a GitHub repo URL |

### `upload`

Build and upload connector wheels to a UC Volume. By default `upload` ships
**two** wheels per call: the root framework wheel
(`lakeflow_community_connectors-*.whl`) and the connector wheel
(`lakeflow_community_connectors_<source>-*.whl`). The connector declares the
framework as a runtime dep, so shipping both keeps clusters from trying to
fetch the framework from PyPI (where it is not published). The volume and any
missing subdirectories are created automatically.

```bash
# Default: build + upload framework wheel and connector wheel
community-connector upload example \
  --volume-path /Volumes/main/default/community_connector/packages

# Iterating on the connector: framework already on the volume, skip it
community-connector upload example \
  --volume-path /Volumes/main/default/community_connector/packages \
  --skip-framework

# Bring your own wheels
community-connector upload example \
  --volume-path /Volumes/main/default/community_connector/packages \
  --wheel ./dist/lakeflow_community_connectors_example-0.1.0-py3-none-any.whl \
  --framework-wheel ./dist/lakeflow_community_connectors-0.1.0-py3-none-any.whl

# Non-conventional source layout + retain a local copy of what we built
community-connector upload my_source \
  --volume-path /Volumes/main/default/community_connector/packages \
  --source-dir ./my_connector \
  --keep-wheel ./dist
```

The build step shells out to `python -m build`, which runs setuptools inside a
PEP 517 isolated environment — no per-connector venv setup needed. Install the
`build` frontend once into the venv that owns the CLI:

```bash
pip install build
```

After upload, reference both wheel paths in your pipeline's
`environment.dependencies` (e.g. via `community-connector create_pipeline
--package <fw_path> --package <connector_path>`) so the cluster installs them
in order.

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--volume-path` | `-v` | UC Volume directory to upload into (required). Auto-creates the volume and any subdirectories. |
| `--wheel` | | Pre-built connector wheel; skip building it. |
| `--framework-wheel` | | Pre-built framework (root) wheel; skip building it. |
| `--skip-framework` | | Do not upload the framework wheel (use when it is already on the volume). |
| `--source-dir` | | Override the connector source directory (default: locate `sources/<source_name>/`). |
| `--keep-wheel` | | After upload, copy any wheels built in this run into this local directory. User-supplied wheels are not copied. |

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
| `objects[].table.table_configuration.sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `objects[].table.table_configuration.cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering) |

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

The main source code is organized under `src/databricks/labs/community_connector_cli/`:
- `libs/` — Shared utilities (spec_parser.py, utils.py, source_loader.py)
- `pipeline/` — Core ingestion logic (PySpark Data Source, SDP orchestration)
- `sources/` — Source connectors (github/, zendesk/, stripe/, etc.)
  - `interface/` — LakeflowConnect base interface
  - `{source}/` — Each connector has: {source}.py, README.md, test/, configs/

```
tools/community_connector/
├── src/databricks/labs/community_connector_cli/
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
