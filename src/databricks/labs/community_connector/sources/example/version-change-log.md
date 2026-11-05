# `example` connector version history

Newest releases first. Versions match `version` in `sources/example/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Example.
- **Connection:** username and password.
- **Tables:** served by the example source's `/tables` endpoint (reference connector for contributors).
- **Ingestion types used:** `append`, `cdc`, `cdc_with_deletes`, `snapshot`.

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
