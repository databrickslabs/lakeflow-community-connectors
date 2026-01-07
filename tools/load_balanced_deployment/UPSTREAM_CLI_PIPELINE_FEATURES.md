# Upstream CLI Pipeline-Related Functionalities

This document summarizes the pipeline-related features from the upstream `community-connector` CLI tool that was previously located at `tools/community_connector/`.

## Overview

The upstream CLI was a Click-based command-line tool for setting up and managing **single-pipeline** community connector deployments. It has since been removed from the repository (commit 2ec5af8), but provided valuable patterns for pipeline creation and management.

## CLI Commands

### 1. `create_pipeline` Command

**Purpose**: Create a complete community connector deployment with repo + pipeline + ingest file.

**Signature**:
```bash
community-connector create_pipeline SOURCE_NAME PIPELINE_NAME \
  --connection-name CONNECTION_NAME \
  --catalog CATALOG \
  --target SCHEMA \
  --repo-url REPO_URL \
  --config CONFIG_FILE \
  --pipeline-spec SPEC_FILE_OR_JSON
```

**What it does**:
1. **Creates a Git repo in workspace** using sparse checkout (only clones needed directories)
2. **Generates `ingest.py` file** in workspace with connection details
3. **Creates DLT pipeline** that references the ingest file and repo
4. **Cleans up excluded root files** (README, LICENSE, etc.) from cone mode checkout

**Key Features**:
- **Sparse checkout**: Only clones `libs/`, `pipeline/`, and `sources/{source_name}/`
- **Placeholder resolution**: Replaces `{CURRENT_USER}`, `{WORKSPACE_PATH}`, `{PIPELINE_NAME}` in paths
- **Two template modes**:
  - Full template with `{SOURCE_NAME}` and `{CONNECTION_NAME}` placeholders
  - Base template with `{PIPELINE_SPEC}` for advanced configurations
- **Configuration precedence**: CLI args → `--config` file → `default_config.yaml` → code defaults
- **Pipeline spec validation**: Validates the pipeline spec before creation with clear error messages

**Example**:
```bash
community-connector create_pipeline github my_github_pipeline \
  -n my_github_connection \
  -c main \
  -t bronze

community-connector create_pipeline osipi osipi_prod \
  --pipeline-spec osipi_config.yaml \
  --catalog osipi \
  --target bronze
```

---

### 2. `run_pipeline` Command

**Purpose**: Start a pipeline update (incremental or full refresh).

**Signature**:
```bash
community-connector run_pipeline PIPELINE_NAME [--full-refresh]
```

**What it does**:
1. **Finds pipeline by name** using the Databricks API
2. **Starts an update** (calls `pipelines.start_update()`)
3. **Returns update ID** and pipeline URL

**Example**:
```bash
community-connector run_pipeline my_github_pipeline
community-connector run_pipeline my_github_pipeline --full-refresh
```

---

### 3. `show_pipeline` Command

**Purpose**: Show the current status of a pipeline.

**Signature**:
```bash
community-connector show_pipeline PIPELINE_NAME
```

**What it does**:
1. **Finds pipeline by name**
2. **Displays**:
   - Pipeline name, ID, state
   - Latest update info (update ID, state, creation time)
   - Pipeline URL

**Example**:
```bash
community-connector show_pipeline my_github_pipeline
```

**Output**:
```
Pipeline Status
========================================
  Name:   my_github_pipeline
  ID:     abc123...
  State:  RUNNING

Latest Update:
  Update ID:   def456...
  State:       RUNNING
  Started:     2025-01-07T10:30:00Z

View pipeline: https://....databricks.com/pipelines/abc123
```

---

### 4. `create_connection` Command

**Purpose**: Create a Unity Catalog connection for a community connector.

**Signature**:
```bash
community-connector create_connection SOURCE_NAME CONNECTION_NAME \
  -o '{"host": "...", "externalOptionsAllowList": "..."}'
```

**What it does**:
1. **Creates UC connection** with type `GENERIC_LAKEFLOW_CONNECT`
2. **Automatically adds `sourceName`** to options
3. **Warns if `externalOptionsAllowList` is missing**

**Example**:
```bash
community-connector create_connection github my_github_conn \
  -o '{"host": "api.github.com", "externalOptionsAllowList": "owner,repo,per_page"}'
```

---

### 5. `update_connection` Command

**Purpose**: Update an existing UC connection's options.

**Signature**:
```bash
community-connector update_connection SOURCE_NAME CONNECTION_NAME \
  -o '{"host": "...", "externalOptionsAllowList": "..."}'
```

**What it does**:
1. **Updates connection options** (cannot change connection_type)
2. **Preserves connection ID and name**

---

## Pipeline Configuration Architecture

### Default Configuration File

**Location**: `tools/community_connector/src/databricks/labs/community_connector/default_config.yaml`

**Key Pipeline Settings**:
```yaml
pipeline:
  root_path: "{WORKSPACE_PATH}"
  libraries:
    - glob:
        include: "{WORKSPACE_PATH}/ingest.py"
  channel: PREVIEW          # REQUIRED for community connectors
  continuous: false
  development: true
  serverless: true          # REQUIRED for community connectors
  catalog: main
  schema: default
```

**Important**: The upstream CLI **always uses `serverless: true` and `channel: PREVIEW`** - this is the correct configuration for community connectors.

---

### PipelineClient Class

**Location**: `tools/community_connector/src/databricks/labs/community_connector/pipeline_client.py`

**Capabilities**:
1. **`create(config, repo_path, source_name)`**: Create a new DLT pipeline
2. **`get(pipeline_id)`**: Get pipeline information
3. **`start(pipeline_id, full_refresh)`**: Start a pipeline update
4. **`list(filter, max_results)`**: List pipelines in workspace

**Key Method**: `_build_create_payload()`
- Converts `PipelineConfig` to Databricks API payload
- Handles library path resolution (notebook, file, glob)
- Only includes optional fields if they have values

---

### Configuration Management

**File**: `tools/community_connector/src/databricks/labs/community_connector/config.py`

**Key Function**: `build_config(source_name, pipeline_name, repo_url, catalog, target, config_file)`

**Configuration Precedence** (highest to lowest):
1. CLI arguments (e.g., `--catalog`, `--target`)
2. Custom config file (`--config`)
3. Default config YAML (`default_config.yaml`)
4. Code defaults

**Configuration Merging**:
- Uses `deep_merge()` function to intelligently merge nested dictionaries
- Override values take precedence, but nested keys are preserved

---

## Placeholder Resolution System

**Function**: `_replace_placeholder_in_value(value, placeholder, replacement)`

**Supported Placeholders**:
- `{CURRENT_USER}` - Replaced with `workspace_client.current_user.me().user_name`
- `{WORKSPACE_PATH}` - Replaced with resolved workspace path
- `{PIPELINE_NAME}` - Replaced with the pipeline name
- `{SOURCE_NAME}` - Replaced with the connector source name

**Resolution Order**:
1. Resolve `{CURRENT_USER}` in `workspace_path`
2. Resolve `{WORKSPACE_PATH}` in `repo.path`
3. Resolve `{WORKSPACE_PATH}` in `pipeline.root_path`
4. Resolve `{WORKSPACE_PATH}` in `pipeline.libraries`

**Example**:
```yaml
workspace_path: "/Users/{CURRENT_USER}/.lakeflow_connectors/{PIPELINE_NAME}"
# Becomes: "/Users/john.doe@company.com/.lakeflow_connectors/github_pipeline"

pipeline:
  root_path: "{WORKSPACE_PATH}"
  # Becomes: "/Users/john.doe@company.com/.lakeflow_connectors/github_pipeline"
```

---

## Pipeline Spec Validation

**File**: `tools/community_connector/src/databricks/labs/community_connector/pipeline_spec_validator.py`

**Function**: `validate_pipeline_spec(spec)`

**Validation Rules**:
1. **Required field**: `connection_name` must be present
2. **Warns** for unknown fields (not errors)
3. **Returns warnings list** (non-fatal issues)

**Error Format**:
```python
raise PipelineSpecValidationError("connection_name is required in pipeline spec")
```

---

## Comparison: Upstream CLI vs Load-Balanced Toolkit

| Feature | Upstream CLI | Load-Balanced Toolkit |
|---------|-------------|----------------------|
| **Scope** | Single pipeline per deployment | Multiple pipelines (load-balanced groups) |
| **Repo Management** | Creates Git repo with sparse checkout | Expects repo already exists |
| **Ingest File** | Generates `ingest.py` from template | Generates `ingest_{group}.py` Python files |
| **Configuration** | YAML-based with precedence system | CSV-based with preset configurations |
| **Pipeline Creation** | Interactive CLI commands | Batch generation from CSV |
| **Connection Management** | CLI commands to create/update | Assumes connection already exists |
| **Validation** | Pipeline spec validation | CSV structure validation |
| **Run Management** | `run_pipeline` and `show_pipeline` commands | Manual via Databricks UI/API |
| **Channel** | `PREVIEW` (default) | Now `PREVIEW` (was incorrectly `CURRENT`) |
| **Serverless** | `true` (default) | Now `true` (was incorrectly using clusters) |

---

## What the Load-Balanced Toolkit Should Adopt

### ✅ Already Adopted

1. **Configuration merging with `deep_merge()`** - Adopted in `utils.py`
2. **Placeholder resolution** - Adopted in `utils.py` as `replace_placeholder_in_value()`
3. **CSV validation** - Adopted in `utils.py` as `validate_csv_structure()`
4. **Serverless + PREVIEW configuration** - Fixed in `generate_dab_yaml.py` (commit bedc3e2)

### 🔄 Could Be Adopted

1. **Pipeline running/monitoring commands**: Add `run_pipelines.py` script to start all pipelines in a group
2. **Pipeline status checking**: Add `show_pipeline_status.py` to check status of load-balanced pipelines
3. **Connection management**: Add CLI commands to create/update UC connections
4. **Ingest file templates**: Use template system for generating ingest files (currently hardcoded)
5. **Configuration file support**: Allow users to provide YAML config files instead of CLI args

### ❌ Not Applicable

1. **Repo creation with sparse checkout**: Load-balanced toolkit assumes repo already exists
2. **Single-pipeline focus**: Load-balanced toolkit is specifically for multi-pipeline deployments
3. **Interactive CLI**: Load-balanced toolkit is batch-oriented (CSV → multiple pipelines)

---

## Recommendations for Load-Balanced Toolkit

### 1. Add Pipeline Management Scripts

Create helper scripts for common operations:

```bash
# Run all pipelines in a group
python run_pipeline_group.py --group time_series --full-refresh

# Check status of all pipelines
python show_pipeline_status.py --connector osipi

# Restart failed pipelines
python restart_failed_pipelines.py --connector osipi
```

### 2. Support Configuration Files

Allow users to override CSV values with a config file:

```bash
python generate_dab_yaml.py \
  --input-csv tables.csv \
  --config overrides.yaml \
  --output-yaml databricks.yml
```

### 3. Add Template Support for Ingest Files

Use Jinja2 or similar for ingest file generation:

```python
# Instead of hardcoded string manipulation
template = load_template("ingest_group_template.py.j2")
content = template.render(
    source_name=source_name,
    connection_name=connection_name,
    tables=group_tables
)
```

### 4. Validate Before Generation

Add a `--validate` flag to check everything before generating files:

```bash
python generate_dab_yaml.py \
  --input-csv tables.csv \
  --validate-only  # Check but don't generate
```

---

## Key Takeaways

1. **Upstream CLI uses serverless + PREVIEW** - This is the CORRECT configuration for community connectors
2. **Upstream CLI has robust error handling** - Pipeline spec validation, clear error messages
3. **Upstream CLI uses configuration precedence** - CLI > config file > defaults
4. **Upstream CLI manages full lifecycle** - Repo creation, pipeline creation, running, monitoring
5. **Load-balanced toolkit should adopt management capabilities** - Add scripts to run, monitor, and manage pipeline groups

---

## References

- Upstream CLI source: commit `a81d32e` (before deletion in `2ec5af8`)
- Default config: `tools/community_connector/src/databricks/labs/community_connector/default_config.yaml`
- Pipeline client: `tools/community_connector/src/databricks/labs/community_connector/pipeline_client.py`
- CLI commands: `tools/community_connector/src/databricks/labs/community_connector/cli.py`
- Best practices adoption doc: `UPSTREAM_CLI_ADOPTION.md`
- Pipeline configuration notes: `PIPELINE_CONFIGURATION_NOTES.md`
