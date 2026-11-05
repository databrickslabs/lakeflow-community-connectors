# `qualtrics` connector version history

Newest releases first. Versions match `version` in `sources/qualtrics/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Qualtrics.
- **Connection:** API token.
- **Tables (9):** `directories`, `directory_contacts`, `distributions`, `mailing_list_contacts`, `mailing_lists`, `survey_definitions`, `survey_responses`, `surveys` and `users`.
- **Ingestion types:** `append` (1), `cdc` (3), `snapshot` (5).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
