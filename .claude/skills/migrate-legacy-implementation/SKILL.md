---
name: migrate-legacy-implementation
description: Migrate a legacy source connector from the old sources/ directory to the new package structure under src/databricks/labs/community_connector/sources/.
---

# Migrate Legacy Source Implementation

## Description

This skill migrates an old connector implementation from the legacy `sources/` directory to the new package structure under `src/databricks/labs/community_connector/sources/`. It handles one specified source at a time, updating imports, conforming to the LakeflowConnect interface, and regenerating specs and build files.

## Instructions

Handle one specified source at a time.
Refer other source (e.g. github) as an example.
Use python3.10 to replace all python command.
When running any python test or build use virtual environment, run these:
python3.10 -m venv venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
then run other python commands or pytest.

### Steps

1. Run shell command to `git mv` the files for the specified source under `sources/` to the new directory. Commit the change.
   - Files under `sources/{source_name}/tests/` need to move to `tests/unit/sources/{source_name}`
   - `sources/{source_name}/configs` needs to move to `tests/unit/sources/{source_name}` as well

2. Update `{source_name}.py`:
   - Import the `LakeflowConnect` interface from `src/databricks/labs/community_connector/interface/lakeflow_connect.py` and change the existing `LakeflowConnect` class to implement the interface with name `{SourceName}LakeflowConnect`.

3. Update import paths in all moved files and expose `{SourceName}LakeflowConnect` in `__init__.py` under `{source_name}` to make it available in the package.
   - libs are moved from root directory to src/databricks/labs/community_connector/libs, so you can use the same library functions.
   - Also update the test imports and code if necessary.
   - Commit the changes so far.

4. If `{source_name}.py` is too large (>1000 lines), consider refactoring it into multiple files. One example to follow is `github`.

5. Run the tests — they will fail but should not be syntax or import errors.
   - Commit the changes so far.

6. Update the `README.md` under `{source_name}` directory to make sure all references and content match the latest changes.

7. Use the `generate-connector-spec` skill to regenerate the spec for this connector.

8. Use the `build-source-project` skill to build the `pyproject.toml` and related files.
   - Commit the changes so far.

10. If there are any .py files that should not be imported as part of {source_name}.py, exclude them in the tools/scripts/merge_exclude_config.json

11. You should change any other directories other than src/databricks/labs/community_connector/sources/{source_name} directory. Delete other directories like libs, pipeline under root.
    - Commit all the changes.

12. run tools/scripts/merge_python_source.py to create _generated source code.
    - Commit the the changes.

