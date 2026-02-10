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

## Build Commands

Run the following commands to set up a virtual environment and build the source package:

```bash
# Navigate to the source directory
cd src/databricks/labs/community_connector/sources/{{source_name}}

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install build dependencies
pip install build

# Build the package
python -m build

# The built packages will be in the dist/ directory:
# - lakeflow_community_connectors_{{source_name}}-0.1.0.tar.gz (source distribution)
# - lakeflow_community_connectors_{{source_name}}-0.1.0-py3-none-any.whl (wheel)
```

## Clean Up

After building, clean up build artifacts:

```bash
# Remove build artifacts (optional, before committing)
rm -rf dist/ build/ *.egg-info .venv
```

## Verification

After building, verify the package contents:

```bash
# List contents of the wheel
unzip -l dist/*.whl
```

The wheel should contain files under the path:
`databricks/labs/community_connector/sources/{{source_name}}/`

## References

- Refer to existing source pyproject.toml files under `src/databricks/labs/community_connector/sources/` for examples
- The main project pyproject.toml excludes sources using: `exclude = ["databricks.labs.community_connector.sources*"]`
