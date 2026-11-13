# `odata` connector version history

Newest releases first. Versions match `version` in `sources/odata/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** OData.
- **Connection:** bearer token, HTTP basic auth, API key, or OAuth 2.0 (handled by the connector or by a Unity Catalog connection).
- **Tables:** the service's entity sets and contained collections, discovered from its `$metadata`.
- **Ingestion types used:** `cdc`, `snapshot`.

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0,<1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
