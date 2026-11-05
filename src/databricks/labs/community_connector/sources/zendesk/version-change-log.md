# `zendesk` connector version history

Newest releases first. Versions match `version` in `sources/zendesk/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Zendesk.
- **Connection:** email address and API token.
- **Tables (8):** `articles`, `brands`, `groups`, `organizations`, `ticket_comments`, `tickets`, `topics` and `users`.
- **Ingestion types:** `cdc` (4), `snapshot` (4).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
