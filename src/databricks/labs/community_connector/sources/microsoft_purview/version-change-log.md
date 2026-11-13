# `microsoft_purview` connector version history

Newest releases first. Versions match `version` in `sources/microsoft_purview/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 1.0.0 — 2026-11-04 - MAJOR

### Changes
- Page `data_products` and `terms` with `skip`/`top` instead of `nextLink`. These Purview endpoints don't return a `nextLink`, so 0.1.0 stopped after the first page and ingested at most one page (100 records by default) of each table.
- `business_domains` still follows `nextLink`.

### Compatibility
- Breaking change affecting the `data_products` and `terms` tables: tables ingested with 0.1.0 may be missing records.
- Other tables retain their existing behavior. No connection or configuration changes.
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- Run a full refresh of `data_products` and `terms` after upgrading. Both are `cdc` on `lastModifiedAt`, so records 0.1.0 skipped that have older timestamps are never picked up incrementally.

### Rollback
- Downgrading to 0.1.0 brings back the truncation; prefer a forward fix.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/305

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Microsoft Purview.
- **Connection:** API token, or a service principal (OAuth M2M).
- **Tables (3):** `business_domains`, `data_products` and `terms`.
- **Ingestion types:** `cdc` (2), `snapshot` (1).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
