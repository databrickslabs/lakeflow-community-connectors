We happily welcome contributions to Lakeflow-Community-Connectors. We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## CI on pull requests from forks

CI runs on a hardened runner group whose only egress to PyPI is a JFrog proxy that authenticates via the GitHub OIDC token. The OIDC token is only issued for events that run in base-repo context (`push`, internal-branch `pull_request`, `pull_request_target`), so fork PRs cannot run CI without a maintainer-mediated approval step.

To keep that approval step from becoming an arbitrary-code-execution channel, fork-PR CI is gated by an explicit `safe-to-test` label rather than by GitHub's "Require approval for outside collaborators" click. The label model makes the approval auditable, and the label is auto-stripped on every new push so each commit needs a fresh sign-off.

### Flow for fork contributors

1. Open the PR as normal. None of the CI workflows will run on first open.
2. A maintainer reviews the diff and, if they're satisfied, applies the `safe-to-test` label. CI then runs against the exact commit they labeled.
3. If you push new commits, the label is automatically removed and a maintainer must re-apply it. This is intentional — every commit on a fork PR needs a fresh sign-off.

### Flow for maintainers

Before applying `safe-to-test`, scan the diff for surfaces that execute in the privileged CI context. The most common pwn-request vectors:

- `.github/**` — workflow or action changes
- `pyproject.toml`, `requirements/**` — install-time hooks, new/changed deps
- `conftest.py` files — pytest auto-loads them at collection time
- `setup.py` / `[tool.setuptools]` entry points — install-time code
- New top-level `__init__.py` that imports as part of test discovery

If anything looks off, do not label — ask the contributor to clean it up first. Once you apply `safe-to-test`, the labeled commit is executed on the protected runner with OIDC in scope; treat the click like merging unreviewed code into a privileged context, because that's what it is.

Internal contributors pushing branches inside `databrickslabs/lakeflow-community-connectors` are not affected — their PRs run CI automatically on every push.

## Prebuilt connector wheels (`wheelhouse/`)

The Databricks managed ingestion wizard installs connectors from prebuilt wheels committed under `wheelhouse/`. When a pipeline is created, it sparse-checks out `wheelhouse/_framework/` and `wheelhouse/<source_name>/` and installs both wheels as pipeline dependencies, so they must stay in sync with the source.

```
wheelhouse/
├── _framework/      # lakeflow_community_connectors-<version>-py3-none-any.whl
└── <source_name>/   # lakeflow_community_connectors_<source_name>-<version>-py3-none-any.whl
```

`<version>` is the `version` in that package's `pyproject.toml`: the repo root for `_framework`, `src/databricks/labs/community_connector/sources/<source_name>/pyproject.toml` for a connector. Each directory holds **exactly one** wheel with its standard filename. Don't rename it: pip rejects wheels whose filename doesn't match their metadata. The directory is `wheelhouse/`, not `wheels/`, because `wheels/` is ignored by `.gitignore`.

### Updating the wheels

In the same PR as the change:

1. Bump `version` in the affected `pyproject.toml` (see [Versioning](#versioning)).
2. Rebuild:

   ```bash
   pip install build
   python tools/scripts/build_wheelhouse.py <source_name>   # a connector
   python tools/scripts/build_wheelhouse.py --framework     # the framework
   ```

   The script builds with `python -m build`, replaces the old wheel, and keeps the committed file when nothing that affects pipelines changed.
3. Check the result against `master`:

   ```bash
   python tools/scripts/build_wheelhouse.py <source_name> --check --base origin/master
   ```

   It fails if a wheel is missing or stale, or if its contents changed without a version bump.

What needs a rebuild:

- **A connector:** any change to files packaged from `sources/<source_name>/`: `*.py`, `connector_spec.yaml`, `pyproject.toml`.
- **The framework:** any change under `src/databricks/labs/community_connector/` outside `sources/` (`interface/`, `libs/`, `pipeline/`, `sparkpds/`), or to the root `pyproject.toml`. Every connector wheel depends on it, and it isn't published to PyPI.
- **No rebuild:** tests, CI, repo docs, docs-only edits to a connector's `README.md` or `<source_name>_api_doc.md`, and the source simulator (`source_simulator/`, test-only). The next code change picks up the docs.

Reviewers: wheels are binary and don't show up in the diff. Run the `--check --base origin/master` command above on the PR branch to confirm they match the source.

## Versioning

Connectors and the framework follow [semantic versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`), starting at `0.1.0`. Each package has one version, in its `pyproject.toml`, and is bumped by the author in the PR that changes it. Bump once per PR, by the largest change in it, and never reuse a version: if `master` moved on, rebase and bump again on top of it. A revert is a new version too, classified by its own effect.

### Connectors

Decide by what an **existing pipeline** needs when it picks up the new wheel:

**MAJOR**: at least one table needs a full refresh, or the user must change their connection or table configuration. Whether the change was planned or not:

- Moving to a new version of the source API where fields or endpoints change
- Changing a table's ingestion type (for example `snapshot` → `cdc`, or `cdc` → `cdc_with_deletes`)
- Changing a primary key or cursor field
- Renaming or removing a table, column, connection parameter, table option or auth method
- Changing a column's type, or making a nullable column non-nullable
- Changing what existing values mean (units, time zones, what one row represents)
- Changing a default in a way that changes which data is returned
- Changing the offset format so an old checkpoint can't be read
- Fixing a data-correctness bug where existing tables must be rebuilt to repair past data
- Raising `requires-python`, or depending on a package version newer than supported runtimes provide

**MINOR**: new capability, and existing pipelines keep working unchanged:

- A new table, a new nullable column, or a new optional parameter, table option or auth method whose default keeps the old behavior
- Deprecating something that keeps working (removal is a later MAJOR)
- Adding a third-party dependency (call it out in the PR: workspaces with a restricted package mirror may need it)
- Raising the framework minimum to use a new framework feature

**PATCH**: fixes and internal changes with no visible effect on existing tables:

- Bug fixes (pagination, retries, rate limits, auth refresh, error handling) and data-correctness fixes that don't need a rebuild of past data
- Performance, logging and error-message improvements, refactors
- Removing an unused dependency, widening a dependency range, or adding an upper cap such as `lakeflow-community-connectors>=0.1.0,<1.0`

**No bump**: anything that doesn't need a rebuild (see the list above).

Say in the PR description which bump you made and why. For a MAJOR bump, also say what users must do (run a full refresh of table `x`, rename option `y`, re-create the connection).

### Framework

Decide by what **connectors** need:

- **MAJOR**: at least one connector must change to keep working. For example, removing or renaming an interface method or mixin, changing a signature, adding a required key to `read_table_metadata()`, removing an `ingestion_type` value or a `libs/` helper, or changing the pipeline entry points.
- **MINOR**: connectors keep working unchanged, and there's new capability. For example, an optional method with a default implementation, a new mixin, a new `libs/` helper, or a new `ingestion_type` value.
- **PATCH**: fixes and internal changes.

Connectors declare the framework as `lakeflow-community-connectors>=<min>,<<next major>`, for example `>=0.1.0,<1.0`. A connector bumps only when it changes itself, not because the framework did. A framework MAJOR needs every connector checked and its cap raised.
