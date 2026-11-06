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

CI builds the wheels, so a PR changes only the source: bump `version` in the affected `pyproject.toml` and add an entry to its `version-change-log.md` (see [Versioning](#versioning) and [Change log](#change-log)).

The **Check Wheelhouse** workflow runs `build_wheelhouse.py all --dry-run --base <master>` on every PR. It builds each wheel without writing `wheelhouse/` and fails if:
- a wheel's contents changed but its version is one `master` already publishes
- a change log has no entry for the `pyproject.toml` version

It's advisory, not a required check, so a red result doesn't block merging. Leave the version unchanged only when a change can't affect pipelines, such as comments or formatting in a connector's code: after merge, Build Wheelhouse keeps the published wheel for that version, and the change ships with the connector's next bump.

After the PR merges, the **Build Wheelhouse** workflow rebuilds every changed wheel from `master` and opens or updates one PR from `bot/update-wheelhouse`. The managed ingestion wizard serves the new wheels once that PR merges. Until then it keeps serving the previous version, so two PRs that merge before the bot PR ship together under the newest version.

Maintainers: GitHub doesn't start workflows for a PR opened by `GITHUB_TOKEN`, so the required checks don't run on the bot PR by themselves. Close and reopen it to run them, then review and merge it.

You can still build wheels locally, for example to test one by hand: `python tools/scripts/build_wheelhouse.py <source_name>` writes `wheelhouse/<source_name>/`. Don't commit it: the bot PR does. The script keeps a committed wheel when nothing that affects pipelines changed, so rebuilding unchanged code doesn't churn binaries.

What changes a wheel, and so needs a version bump:

- **A connector:** any change to files packaged from `sources/<source_name>/`: `*.py`, `connector_spec.yaml`, `pyproject.toml`.
- **The framework:** any change under `src/databricks/labs/community_connector/` outside `sources/` (`interface/`, `libs/`, `pipeline/`, `sparkpds/`), or to the root `pyproject.toml`. Every connector wheel depends on it, and it isn't published to PyPI.
- **No wheel change:** tests, CI, repo docs, docs-only edits to a connector's `README.md` or `<source_name>_api_doc.md`, and the source simulator (`source_simulator/`, test-only). The next code change picks up the docs.

Reviewers of a bot PR: wheels are binary and don't show up in the diff. To confirm they match the source, check out the bot branch and run `python tools/scripts/build_wheelhouse.py all --check`.

## Versioning

Connectors and the framework follow [semantic versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`), starting at `0.1.0`. Each package has one version, in its `pyproject.toml`, and is bumped by the author in the PR that changes it. Bump once per PR, by the largest change in it, and never reuse a version: if `master` moved on, rebase and bump again on top of it. A revert is a new version too, classified by its own effect.

### Connectors

Decide by what an **existing pipeline** needs when it picks up the new wheel:

**MAJOR**: at least one table needs a full refresh, or the user must change their connection or table configuration. Whether the change was planned or not:

- Moving to a new version of the source API where fields or endpoints change
- Changing a table's ingestion type (for example `snapshot` → `cdc`, or `cdc` → `cdc_with_deletes`)
- Changing a primary key or cursor field
- Renaming or removing a table, column, connection parameter, table option or auth method
- Changing a column's type, changing a nested struct's shape incompatibly, or making a nullable column non-nullable
- Making an optional connection parameter or table option required
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

**No bump**: anything that doesn't change a wheel (see the list above).

Record every bump in the connector's change log (see [Change log](#change-log)), and say in the PR description which bump you made and why.

### Framework

The framework is the `lakeflow-community-connectors` package, built from the root `pyproject.toml` into `wheelhouse/_framework/`. Every pipeline installs it next to its connector wheel, so a framework change reaches every connector at once. Decide the bump by what **connectors** need, not by pipelines or tables.

What counts as the framework:

- `interface/`: `LakeflowConnect` and the `Supports*` mixins
- `libs/`: helpers that connectors import
- `pipeline/` and `sparkpds/`: the ingestion pipeline entry point and the Spark data source registration
- the root `pyproject.toml`: `requires-python` and the framework's dependencies
- `source_simulator/` ships in the wheel but is test-only, so changing it doesn't need a bump

**MAJOR**: at least one connector must change to keep working.

- Removing or renaming an interface method, a mixin, or a `libs/` helper that connectors import
- Changing a method signature or return contract, for example adding a required argument or a required key in `read_table_metadata()`
- Removing an `ingestion_type` value, or changing what an existing one means
- Changing how offsets are passed to or stored for connectors
- Changing the pipeline entry points or the data source registration that connectors rely on
- Raising `requires-python`, or adding or tightening a dependency beyond what supported runtimes or existing connectors allow

**MINOR**: connectors keep working unchanged, and there's new capability.

- Adding an optional interface method with a default implementation (like `read_table_deletes()`)
- Adding a new mixin, `libs/` helper, or `ingestion_type` value
- Adding an optional argument or metadata key that has a default
- Deprecating something that keeps working (removal is a later MAJOR)

**PATCH**: fixes and internal changes connectors can't observe.

- Bug fixes in `libs/`, `pipeline/` or `sparkpds/`
- Performance, logging and error-message improvements, refactors
- Removing an unused dependency, or widening a dependency range

**No bump**: tests, CI, docs, and `source_simulator/` specs and corpus.

#### Compatibility with connectors

- Every connector declares the framework range it works with: `lakeflow-community-connectors>=<min>,<<next major>`, for example `>=0.1.0,<1.0`.
- A connector raises `<min>` when it starts using a new framework feature, and bumps its own version (MINOR).
- A connector bumps only when it changes itself, not because the framework did.
- The wizard installs the framework and connector wheels from the same `master` revision, so every framework version on `master` must satisfy the range of every connector on `master`.

#### Releasing a framework change

1. Classify the change with the rules above, bump `version` in the root `pyproject.toml`, and add an entry to the root `version-change-log.md`.
2. Check the `lakeflow-community-connectors` range in every connector's `pyproject.toml` against the new version:
   - after a MINOR or PATCH, a connector whose range excludes the new version pins below it: fix its range in the same PR.
   - after a MAJOR, verify each connector against the new framework, change its code if needed, raise its cap to the next major (for example `<2.0`), and bump it by its own rules, all in the same PR. If the affected connectors can't all be updated, defer the framework release.
3. Confirm Check Wheelhouse passes on the PR. Build Wheelhouse publishes the framework and connector wheels together after merge.

Examples:

- **PATCH**: fix a retry helper in `libs/utils.py`. Framework `0.1.0` → `0.1.1`; no connector changes.
- **MINOR**: add an optional mixin with a default. Framework `0.1.1` → `0.2.0`; connectors that adopt it raise their minimum to `>=0.2.0` and bump MINOR.
- **MAJOR**: make `read_table_metadata()` require a new key. Framework `0.2.0` → `1.0.0`; in the same PR, every connector adds the key, moves its range to `>=1.0.0,<2.0`, and bumps (PATCH, or MAJOR if its tables are affected).

### Change log

Every package keeps a change log next to its `pyproject.toml`: `sources/<source_name>/version-change-log.md` for a connector, the root `version-change-log.md` for the framework. Every version bump adds one entry at the top, so the file lists versions from newest to oldest:

```markdown
## 1.0.0 — 2026-11-04 - MAJOR

### Changes
- Page `terms` with `skip`/`top` instead of `nextLink`, which this endpoint doesn't return.

### Compatibility
- Breaking change affecting the `terms` table: tables ingested with 0.1.0 may be missing records.
- Other tables retain their existing behavior.
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- Run a full refresh of `terms` after upgrading.

### Rollback
- Downgrading to 0.1.0 brings back the truncation; prefer a forward fix.

### References
- PR: <link>
```

- The heading is `## <version> — <YYYY-MM-DD> - <MAJOR|MINOR|PATCH>`, with the version from `pyproject.toml` and the merge date. A package's first version uses `INITIAL`.
- **Changes**: what changed, in terms users see.
- **Compatibility**: which tables are affected and how, which keep their behavior, and the framework range the version requires. A new dependency goes here too.
- **Upgrade instructions**: what users must do after upgrading: a full refresh of named tables, a renamed option, a re-created connection. Write "No action required." for MINOR and PATCH.
- **Rollback**: whether downgrading to the previous version is safe, and what it brings back.
- **References**: the PR, and any issue.

Check Wheelhouse flags a PR if the change log is missing, a heading doesn't match the format, the newest entry isn't for the `pyproject.toml` version, or versions don't decrease down the file. Docs-only changes don't bump, so they don't get an entry.
