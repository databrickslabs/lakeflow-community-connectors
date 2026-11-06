# Contribute the CM360 Connector

This is a Phase 1 contribution: simulated tests are available, but fresh
authenticated source validation and an end-to-end Databricks ingestion check
remain outstanding. Do not represent this as production-validated.

## Before Publishing

Confirm that you have permission to redistribute the implementation and any
branding. Preserve upstream LICENSE, NOTICE and attribution. Publish only the
sanitized contribution branch, never private development history, credentials,
raw source exports, cassettes or internal deployment records.

Review the [upstream contribution guide](https://github.com/databrickslabs/lakeflow-community-connectors/blob/master/CONTRIBUTING.md)
and current source-development instructions. Keep the contribution on the
existing Lakeflow interface; no shared-framework changes are needed.

## Open an Upstream Pull Request

A standalone distribution repository is not a GitHub fork. An ordinary
cross-repository PR needs a fork of `databrickslabs/lakeflow-community-connectors`.
Use the following commands from the clean contribution checkout, replacing
`<github-user>` with your GitHub account. Check `gh auth status` first.

```bash
gh repo fork databrickslabs/lakeflow-community-connectors --clone=false --remote=false
git remote add fork https://github.com/<github-user>/lakeflow-community-connectors.git
git push -u fork codex/cm360-community-release
gh pr create --repo databrickslabs/lakeflow-community-connectors \
  --base master --head <github-user>:codex/cm360-community-release \
  --title "feat(google_cm360): add Campaign Manager 360 reporting connector" \
  --body-file src/databricks/labs/community_connector/sources/google_cm360/PR_DESCRIPTION.md \
  --draft
```

If the fork or remote already exists, reuse it after verifying its URL rather
than recreating it. A maintainer may require the base to be refreshed before
review; re-run affected checks after updating the branch.

Request `needs-live-testing` while real-source evidence is missing. Run the
checked-in `/self-review-connector for google_cm360` workflow after the final
push, or perform its equivalent audit using the current rubric. The significant
connector-change gate requires `connector-self-reviewed`; a push invalidates it.
A maintainer must review the fork diff and apply `safe-to-test` for privileged
CI. Do not apply that authorization yourself or work around the protected CI.

## Local Checks

Use Python 3.10 to match upstream CI. Install the repository's current development
dependencies, then run:

```bash
CONNECTOR_TEST_MODE=simulate PYTHONPATH=src python -m pytest tests/unit/sources/google_cm360/ -q
python tools/scripts/merge_python_source.py google_cm360
git diff --exit-code -- src/databricks/labs/community_connector/sources/google_cm360/_generated_google_cm360_python_source.py
git diff --check
uv build --wheel
uv build --wheel src/databricks/labs/community_connector/sources/google_cm360
```

Also run the exact Pylint flags from upstream `.github/workflows/pylint.yml`.
Scan selected files and unpacked wheels for credentials and customer identifiers.
Neither successful builds nor simulated tests establish real-source compatibility.

## Authenticated Validation

Use an isolated, approved source profile and Databricks development environment.
The connector creates/runs/deletes temporary CM360 report definitions and reads
advertising data. It does not write campaigns, ads or placements.

Pass credentials per run with `CONNECTOR_TEST_CONFIG_PATH` or
`CONNECTOR_TEST_CONFIG_JSON`. Supply per-table bounds through the harness's
local, ignored `tests/unit/sources/google_cm360/configs/dev_table_config.json`. Include
every advertised table; period reach needs an explicit completed cutoff,
metadata anchor, country and account timezone. Start with short windows.

```bash
CONNECTOR_TEST_MODE=live CONNECTOR_TEST_CONFIG_PATH=/secure/path/cm360.json \
  PYTHONPATH=src python -m pytest tests/unit/sources/google_cm360/ -q
```

`record` is a supported alias in the CM360 test class. Retain validation and
coverage reports tied to the final source/generated-artifact SHAs. Compare
source-native control reports for identical dates/filters; verify locales,
cost units, reach markers, permissions and cleanup. Do not commit raw cassettes.
Complete a bounded E2E pipeline check and resolve maintainer findings before
requesting acceptance. Merge is a maintainer decision.

Community contribution is distinct from registering a Custom tile in one
workspace. See the [official documentation](https://docs.databricks.com/aws/en/ingestion/custom-connectors#contribute-your-connector-to-the-community).
