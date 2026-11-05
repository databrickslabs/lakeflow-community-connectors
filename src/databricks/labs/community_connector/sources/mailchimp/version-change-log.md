# `mailchimp` connector version history

Newest releases first. Versions match `version` in `sources/mailchimp/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Mailchimp.
- **Connection:** API key.
- **Tables (4):** `campaigns`, `lists`, `members` and `reports`.
- **Ingestion types:** `cdc` (4).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
