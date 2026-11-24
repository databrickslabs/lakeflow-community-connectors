# `mixpanel` connector version history

Newest releases first. Versions match `version` in `sources/mixpanel/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Mixpanel.
- **Connection:** service account, or project API secret.
- **Tables (4):** `cohort_members`, `cohorts`, `engage` and `events`.
- **Ingestion types:** `cdc` (2), `snapshot` (2).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
