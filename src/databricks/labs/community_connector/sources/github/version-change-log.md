# `github` connector version history

Newest releases first. Versions match `version` in `sources/github/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** GitHub.
- **Connection:** personal access token.
- **Tables (12):** `assignees`, `branches`, `collaborators`, `comments`, `commits`, `issues`, `organizations`, `pull_requests`, `repositories`, `reviews`, `teams` and `users`.
- **Ingestion types:** `append` (2), `cdc` (3), `snapshot` (7).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
