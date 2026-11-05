# `fhir` connector version history

Newest releases first. Versions match `version` in `sources/fhir/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** FHIR R4.
- **Connection:** SMART backend services (JWT assertion), a client secret, or none.
- **Tables:** one table per FHIR resource type; 14 resource types by default (`Patient`, `Observation`, `Condition`, `Encounter`, `Procedure`, `MedicationRequest`, `DiagnosticReport`, `AllergyIntolerance`, `Immunization`, `Coverage`, `CarePlan`, `Goal`, `Device` and `DocumentReference`), configurable with `resource_types`.
- **Ingestion types used:** `cdc`.

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
