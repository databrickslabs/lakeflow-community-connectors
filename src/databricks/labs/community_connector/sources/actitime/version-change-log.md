# `actitime` connector version history

Newest releases first. Versions match `version` in `sources/actitime/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** actiTIME.
- **Connection:** username and password (HTTP basic auth) against your actiTIME URL.
- **Tables (16):** `approvalStatus`, `customers`, `departments`, `holidays`, `leaveTypes`, `leavetime`, `projects`, `settings`, `tasks`, `timeZoneGroups`, `timetrack`, `typesOfWork`, `userGroups`, `userRates`, `users` and `workflowStatuses`.
- **Ingestion types:** `append` (3), `cdc` (3), `snapshot` (10).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
