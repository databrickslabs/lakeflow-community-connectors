# `surveymonkey` connector version history

Newest releases first. Versions match `version` in `sources/surveymonkey/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** SurveyMonkey.
- **Connection:** access token.
- **Tables (18):** `benchmark_bundles`, `collectors`, `contact_lists`, `contacts`, `group_members`, `groups`, `survey_categories`, `survey_folders`, `survey_languages`, `survey_pages`, `survey_questions`, `survey_responses`, `survey_rollups`, `survey_templates`, `surveys`, `users`, `webhooks` and `workgroups`.
- **Ingestion types:** `cdc` (3), `snapshot` (15).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
