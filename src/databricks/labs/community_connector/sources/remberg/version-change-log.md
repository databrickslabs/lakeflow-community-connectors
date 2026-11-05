# `remberg` connector version history

Newest releases first. Versions match `version` in `sources/remberg/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** remberg.
- **Connection:** API key.
- **Tables (9):** `assets`, `contacts`, `forms`, `organizations`, `parts`, `tickets`, `users`, `work_orders` and `work_requests`.
- **Ingestion types:** `cdc` (6), `snapshot` (3).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
