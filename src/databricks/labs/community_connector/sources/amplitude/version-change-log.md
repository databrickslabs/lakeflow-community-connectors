# `amplitude` connector version history

Newest releases first. Versions match `version` in `sources/amplitude/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Amplitude.
- **Connection:** project API key and secret key.
- **Tables (10):** `active_users_counts`, `annotations`, `average_session_length`, `cohorts`, `events`, `events_list`, `session_length_distribution`, `sessions_per_user`, `taxonomy_events` and `taxonomy_user_properties`.
- **Ingestion types:** `append` (1), `cdc` (3), `snapshot` (6).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0,<1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
