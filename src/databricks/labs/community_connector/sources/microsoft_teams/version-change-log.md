# `microsoft_teams` connector version history

Newest releases first. Versions match `version` in `sources/microsoft_teams/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Microsoft Teams.
- **Connection:** Microsoft Entra app (tenant ID, client ID and client secret).
- **Tables (5):** `channels`, `members`, `message_replies`, `messages` and `teams`.
- **Ingestion types:** `cdc` (2), `snapshot` (3).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
