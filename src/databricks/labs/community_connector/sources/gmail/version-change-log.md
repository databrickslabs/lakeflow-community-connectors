# `gmail` connector version history

Newest releases first. Versions match `version` in `sources/gmail/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Gmail.
- **Connection:** Google OAuth (U2M).
- **Tables (10):** `delegates`, `drafts`, `filters`, `forwarding_addresses`, `labels`, `messages`, `profile`, `send_as`, `settings` and `threads`.
- **Ingestion types:** `cdc_with_deletes` (2), `snapshot` (8).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
