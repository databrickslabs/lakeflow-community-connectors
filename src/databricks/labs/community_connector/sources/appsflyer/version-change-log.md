# `appsflyer` connector version history

Newest releases first. Versions match `version` in `sources/appsflyer/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** AppsFlyer.
- **Connection:** API token.
- **Tables (7):** `apps`, `in_app_events_report`, `installs_report`, `organic_in_app_events_report`, `organic_installs_report`, `organic_uninstall_events_report` and `uninstall_events_report`.
- **Ingestion types:** `cdc` (6), `snapshot` (1).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
