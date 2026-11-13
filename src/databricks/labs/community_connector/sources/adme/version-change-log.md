# `adme` connector version history

Newest releases first. Versions match `version` in `sources/adme/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Azure Data Manager for Energy (ADME OSDU).
- **Connection:** Microsoft Entra ID: a service principal (default), managed identity, federated identity, or a static token.
- **Tables (3):** `Reservoir`, `Rock_and_Fluid` and `Wellbore`.
- **Ingestion types:** `cdc` (3).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0,<1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
