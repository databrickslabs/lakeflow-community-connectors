# `google_sheets_docs` connector version history

Newest releases first. Versions match `version` in `sources/google_sheets_docs/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Google Sheets and Docs.
- **Connection:** Google OAuth (client ID and secret with a refresh token).
- **Tables (3):** `documents`, `sheet_values` and `spreadsheets`.
- **Ingestion types:** `snapshot` (3).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
