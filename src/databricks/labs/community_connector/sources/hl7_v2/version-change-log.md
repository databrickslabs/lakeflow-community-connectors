# `hl7_v2` connector version history

Newest releases first. Versions match `version` in `sources/hl7_v2/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** HL7 v2.
- **Connection:** Google Cloud Healthcare API, or HL7 v2 files in a Unity Catalog volume.
- **Tables (23):** `al1`, `dg1`, `evn`, `ft1`, `gt1`, `iam`, `in1`, `mrg`, `msh`, `nk1`, `nte`, `obr`, `obx`, `orc`, `pd1`, `pid`, `pr1`, `pv1`, `pv2`, `rxa`, `sch`, `spm` and `txa`.
- **Ingestion types:** `append` (23).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
