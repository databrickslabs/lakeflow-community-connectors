---
name: build_connector_package
description: Create a pyproject.toml for a source connector and bump its version and change log.
disable-model-invocation: true
---

# Build Source Project

## Goal
Create a `pyproject.toml` for **{{source_name}}** connector that can be built and distributed as an independent Python package, and bump its version and change log when it changes.

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

3. **Version**: a new connector starts at `0.1.0`, with a `version-change-log.md` next to `pyproject.toml` holding one `## 0.1.0 — <date> - INITIAL` entry (format in `CONTRIBUTING.md`, Change log). For an existing connector, never reset or hand-pick the version: bump it as described below.

## Choose the version

Skip this section for a new connector. Otherwise, compare the change with `master` and bump `version` in `src/databricks/labs/community_connector/sources/{{source_name}}/pyproject.toml` once, by the largest change. The full rules and examples are in `CONTRIBUTING.md` (Versioning).

1. For every table the change touches, decide what an existing pipeline needs after upgrading:
   - **full refresh**: primary key, cursor, ingestion type, column type, nested struct shape, value meaning or offset format changed, or past data must be repaired
   - **user action**: a table, column, connection parameter, table option or auth method was renamed or removed, an optional parameter or option became required, or a runtime requirement went past what supported runtimes provide
   - **nothing**
2. Pick the bump:
   - **MAJOR** (`x.y.z` → `x+1.0.0`) if any table needs a full refresh or user action
   - **MINOR** (`x.y.z` → `x.y+1.0`) if nothing is needed and the change adds capability (table, nullable column, optional parameter, auth method, new dependency)
   - **PATCH** (`x.y.z` → `x.y.z+1`) for fixes, performance, logging and refactors
   - **No bump** if only tests, `README.md` or `{{source_name}}_api_doc.md` changed. Stop here.
3. If `master` already has a higher version than your branch's base, bump from `master`'s version.
4. Add an entry at the top of `src/databricks/labs/community_connector/sources/{{source_name}}/version-change-log.md` with the new version, today's date and the bump type, filling in Changes, Compatibility, Upgrade instructions, Rollback and References (format in `CONTRIBUTING.md`, Change log). For MAJOR, Upgrade instructions must say what users must do, per table.
5. Record the bump in the commit message and PR description too: `{{source_name}} 0.1.0 → 0.2.0 (MINOR): add the cohorts table`.

If the change also touches framework code (outside `sources/`), bump the root `pyproject.toml`, add an entry to the root `version-change-log.md`, by the framework rules in `CONTRIBUTING.md` (Versioning → Framework: MAJOR if any connector must change, MINOR for new capability, PATCH for fixes; `source_simulator/` changes don't bump). The new framework version must satisfy every connector's `lakeflow-community-connectors` range: check each connector's `pyproject.toml`, and if a range excludes the new version, update those connectors in the same change (raise the cap and bump each one) or don't release the framework change.

## References

- Refer to existing source pyproject.toml files under `src/databricks/labs/community_connector/sources/` for examples
- The main project pyproject.toml excludes sources using: `exclude = ["databricks.labs.community_connector.sources*"]`