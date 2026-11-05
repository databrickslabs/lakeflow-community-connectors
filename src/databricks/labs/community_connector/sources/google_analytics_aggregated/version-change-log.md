# `google_analytics_aggregated` connector version history

Newest releases first. Versions match `version` in `sources/google_analytics_aggregated/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** Google Analytics Aggregated Data.
- **Connection:** Google service account credentials (JSON).
- **Tables:** 15 prebuilt reports (`traffic_by_country`, `user_acquisition`, `events_summary`, `page_performance`, `device_breakdown`, `landing_page_performance`, `ecommerce_purchases`, `purchase_journey`, `checkout_journey`, `promotions_performance`, `user_retention`, `demographic_details`, `audience_performance`, `tech_details` and `advertising_channels`), plus custom reports defined with table options.
- **Ingestion types used:** `cdc`.

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
