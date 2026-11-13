# `zoho_crm` connector version history

Newest releases first. Versions match `version` in `sources/zoho_crm/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Zoho CRM.
- **Connection:** Zoho OAuth (client ID and secret with a refresh token).
- **Tables:** the CRM modules discovered from the org, plus `Users`, `Roles`, `Profiles`, `Campaigns_Leads`, `Campaigns_Contacts` and `Contacts_X_Deals`.
- **Ingestion types used:** `cdc`, `snapshot`.

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
