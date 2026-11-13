# `azure_devops` connector version history

Newest releases first. Versions match `version` in `sources/azure_devops/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Azure DevOps.
- **Connection:** personal access token, or a service principal (OAuth M2M).
- **Tables (14):** `commits`, `projects`, `pullrequest_commits`, `pullrequest_reviewers`, `pullrequest_threads`, `pullrequest_workitems`, `pullrequests`, `pushes`, `refs`, `repositories`, `users`, `workitem_revisions`, `workitem_types` and `workitems`.
- **Ingestion types:** `append` (4), `cdc` (2), `snapshot` (8).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
