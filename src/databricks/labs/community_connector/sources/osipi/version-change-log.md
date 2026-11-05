# `osipi` connector version history

Newest releases first. Versions match `version` in `sources/osipi/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** OSI PI.
- **Connection:** bearer token, OIDC client credentials, or HTTP basic auth.
- **Tables (39):** `pi_af_hierarchy`, `pi_af_table_rows`, `pi_af_tables`, `pi_analyses`, `pi_analysis_templates`, `pi_assetdatabases`, `pi_assetservers`, `pi_attribute_templates`, `pi_calculated`, `pi_categories`, `pi_current_value`, `pi_dataservers`, `pi_element_attributes`, `pi_element_template_attributes`, `pi_element_templates`, `pi_end`, `pi_event_frames`, `pi_eventframe_acknowledgements`, `pi_eventframe_annotations`, `pi_eventframe_attributes`, `pi_eventframe_referenced_elements`, `pi_eventframe_template_attributes`, `pi_eventframe_templates`, `pi_interpolated`, `pi_links`, `pi_plot`, `pi_point_attributes`, `pi_point_type_catalog`, `pi_points`, `pi_recorded_at_time`, `pi_streamset_end`, `pi_streamset_interpolated`, `pi_streamset_plot`, `pi_streamset_recorded`, `pi_streamset_summary`, `pi_summary`, `pi_timeseries`, `pi_units_of_measure` and `pi_value_at_time`.
- **Ingestion types:** `append` (10), `snapshot` (29).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
