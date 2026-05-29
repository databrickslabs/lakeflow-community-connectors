---
name: build_connector_package
description: Create a pyproject.toml for a source connector and build it as an independent Python package.
disable-model-invocation: true
---

# Build Source Project

## Goal
Create a `pyproject.toml` for **{{source_name}}** connector that can be built and distributed as an independent Python package, then build it using the standard Python build process.

## Prerequisites
- The source connector implementation must already exist under `src/databricks/labs/community_connector/sources/{{source_name}}/`
- Python 3.10+ installed on your system

## Creating pyproject.toml

Create a `pyproject.toml` file in the source directory with the following structure:

```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "lakeflow-community-connectors-{{source_name}}"
version = "0.1.0"
description = "{{source_name}} connector for Lakeflow Community Connectors"
requires-python = ">=3.10"
dependencies = [
    "pyspark>=3.5.0",
    "pydantic>=2.0.0",
    "lakeflow-community-connectors>=0.1.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
]

[tool.setuptools]
packages = ["databricks.labs.community_connector.sources.{{source_name}}"]

[tool.setuptools.package-dir]
"databricks.labs.community_connector.sources.{{source_name}}" = "."

[tool.setuptools.package-data]
"*" = ["*.json", "*.md", "*.yaml"]
```

### Important Notes

1. **Dependencies**: Each source depends on:
   - `lakeflow-community-connectors>=0.1.0` - provides the `interface` and `libs` modules
   - `pyspark>=3.5.0` - for Spark DataFrame types
   - `pydantic>=2.0.0` - for data validation

2. **Package Naming**: Use `lakeflow-community-connectors-{{source_name}}` format (with hyphens, not underscores)

## Build + upload via the CLI (recommended)

`community-connector upload` wraps the build step and pushes the wheel to a UC
Volume in one call. The destination volume and any subdirectories are created
if they don't exist.

```bash
community-connector upload {{source_name}} \
  --volume-path /Volumes/<catalog>/<schema>/community_connector/packages
```

To skip the build step and upload a pre-built wheel:

```bash
community-connector upload {{source_name}} \
  --volume-path /Volumes/<catalog>/<schema>/community_connector/packages \
  --wheel dist/{{source_name}}/lakeflow_community_connectors_{{source_name}}-0.1.0-py3-none-any.whl
```

The CLI sanity-checks the built wheel against
`databricks/labs/community_connector/sources/{{source_name}}/` before upload, so
a misconfigured `pyproject.toml` is caught before it reaches the volume.

## Build-only commands (no upload)

Use these only when you need a local wheel artifact without uploading.
`python -m build` runs setuptools inside a PEP 517 isolated environment, so a
per-connector `.venv` is not needed — install `build` once into whichever venv
runs the command.

```bash
# One-time, in the venv that owns the CLI / your dev workflow
pip install build

# Build the wheel for {{source_name}}
python -m build --wheel \
  src/databricks/labs/community_connector/sources/{{source_name}} \
  --outdir dist/{{source_name}}

# Result:
# dist/{{source_name}}/lakeflow_community_connectors_{{source_name}}-0.1.0-py3-none-any.whl
```

## Verification

After building, list the wheel contents:

```bash
unzip -l dist/{{source_name}}/*.whl
```

The wheel must contain files under:
`databricks/labs/community_connector/sources/{{source_name}}/`

## References

- Refer to existing source pyproject.toml files under `src/databricks/labs/community_connector/sources/` for examples
- The main project pyproject.toml excludes sources using: `exclude = ["databricks.labs.community_connector.sources*"]`