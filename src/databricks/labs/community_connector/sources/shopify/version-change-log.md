# `shopify` connector version history

Newest releases first. Versions match `version` in `sources/shopify/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Shopify.
- **Connection:** Admin API access token.
- **Tables (7):** `customers`, `fulfillments`, `inventory_levels`, `locations`, `orders`, `products` and `refunds`.
- **Ingestion types:** `append` (1), `cdc` (5), `snapshot` (1).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
