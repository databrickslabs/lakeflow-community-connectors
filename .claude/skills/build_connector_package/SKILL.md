---
name: build_connector_package
description: Create a pyproject.toml for a source connector, bump its version, and build its prebuilt wheel into wheelhouse/.
disable-model-invocation: true
---

# Build Source Project

## Goal
Create a `pyproject.toml` for **{{source_name}}** connector that can be built and distributed as an independent Python package, bump its version when it changes, and build its prebuilt wheel into `wheelhouse/{{source_name}}/`.

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
   - **No bump** if only tests, `README.md` or `{{source_name}}_api_doc.md` changed. Stop here: don't rebuild.
3. If `master` already has a higher version than your branch's base, bump from `master`'s version.
4. Add an entry at the top of `src/databricks/labs/community_connector/sources/{{source_name}}/version-change-log.md` with the new version, today's date and the bump type, filling in Changes, Compatibility, Upgrade instructions, Rollback and References (format in `CONTRIBUTING.md`, Change log). For MAJOR, Upgrade instructions must say what users must do, per table.
5. Record the bump in the commit message and PR description too: `{{source_name}} 0.1.0 → 0.2.0 (MINOR): add the cohorts table`.

If the change also touches framework code (outside `sources/`), bump the root `pyproject.toml`, add an entry to the root `version-change-log.md`, by the framework rules in `CONTRIBUTING.md` (Versioning → Framework: MAJOR if any connector must change, MINOR for new capability, PATCH for fixes; `source_simulator/` changes don't bump) and rebuild with `--framework`. The new framework version must satisfy every connector's `lakeflow-community-connectors` range: if the script reports a mismatch, update those connectors in the same change (raise the cap and bump each one) or don't release the framework change.

## Build the prebuilt wheel (required)

The managed ingestion wizard installs `wheelhouse/_framework/` and `wheelhouse/{{source_name}}/`, so rebuild in the same change as the code and the version bump:

```bash
# One-time, in the venv that runs the command
pip install build

python tools/scripts/build_wheelhouse.py {{source_name}}
# wheelhouse/{{source_name}}/lakeflow_community_connectors_{{source_name}}-<version>-py3-none-any.whl

# Only if framework code changed:
python tools/scripts/build_wheelhouse.py --framework
```

Then verify against `master`. Fix anything it reports and re-run until it passes:

```bash
python tools/scripts/build_wheelhouse.py {{source_name}} --check --base origin/master
```

It fails if the directory doesn't hold exactly one wheel matching the `pyproject.toml` version, if the wheel is stale, or if the wheel's contents changed without a version bump. Commit `wheelhouse/{{source_name}}/` (and `wheelhouse/_framework/` if rebuilt) together with the source change.

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
  --wheel wheelhouse/{{source_name}}/lakeflow_community_connectors_{{source_name}}-<version>-py3-none-any.whl
```

The CLI sanity-checks the built wheel against
`databricks/labs/community_connector/sources/{{source_name}}/` before upload, so
a misconfigured `pyproject.toml` is caught before it reaches the volume.

## Build-only commands (no upload)

Use these only when you need a local wheel artifact outside `wheelhouse/`, for example to test it by hand.
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
# dist/{{source_name}}/lakeflow_community_connectors_{{source_name}}-<version>-py3-none-any.whl
```

## Verification

After building, list the wheel contents:

```bash
unzip -l wheelhouse/{{source_name}}/*.whl
```

The wheel must contain files under:
`databricks/labs/community_connector/sources/{{source_name}}/`

## References

- Refer to existing source pyproject.toml files under `src/databricks/labs/community_connector/sources/` for examples
- The main project pyproject.toml excludes sources using: `exclude = ["databricks.labs.community_connector.sources*"]`