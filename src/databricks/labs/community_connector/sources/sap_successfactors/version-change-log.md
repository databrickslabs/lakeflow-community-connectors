# `sap_successfactors` connector version history

Newest releases first. Versions match `version` in `sources/sap_successfactors/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** SAP SuccessFactors.
- **Connection:** username and password (HTTP basic auth).
- **Tables:** 250 entities from the bundled definitions by default; with `metadata_mode: dynamic`, the entity sets in the tenant's OData `$metadata`.
- **Ingestion types used:** `cdc`, `snapshot`.

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
