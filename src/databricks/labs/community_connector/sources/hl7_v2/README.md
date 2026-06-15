# Lakeflow HL7 v2 Community Connector

The Lakeflow HL7 v2 Connector parses HL7 v2 pipe-delimited messages and loads each segment type into its own Delta table, enabling structured analytics over clinical data. The connector supports incremental append-only ingestion, automatically tracking new messages by their `createTime` (GCP Healthcare API) or file modification time (Unity Catalog Volume).

## Supported Source Modes

| Mode | `source_type` value | Description |
|---|---|---|
| **GCP Healthcare API** | `gcp` (default) | Fetches HL7 v2 messages from a Google Cloud Healthcare API HL7v2 store via REST |
| **Unity Catalog Volume** | `volume` | Reads HL7 v2 files directly from a UC Volume path FUSE-mounted on the executor |

## Prerequisites

**GCP Mode** — A GCP project with the Cloud Healthcare API enabled, an HL7v2 store containing messages, and a service account key with `healthcare.hl7V2Messages.list` and `healthcare.hl7V2Messages.get` permissions.

**Volume Mode** — A Unity Catalog Volume containing HL7 v2 message files (one HL7 wire payload per file, or HL7 batch files with multiple `MSH|...` segments). Files are read by Spark executors (which have Unity Catalog Volume FUSE access), so no other credentials are required — the executor identity simply needs `READ VOLUME` on the configured Volume. This is the same pattern used by the `dicomweb` connector for writing DICOM files; see its README for prior art.

## Connection Parameters

The `source_type` parameter determines which credentials are required. Defaults to `gcp` if omitted.

### GCP Mode

| Parameter | Required | Description | Example |
|---|---|---|---|
| `source_type` | No | Set to `gcp` or omit | `gcp` |
| `project_id` | Yes | GCP project ID | `my-gcp-project` |
| `location` | Yes | GCP region | `us-central1` |
| `dataset_id` | Yes | Healthcare API dataset ID | `My_Clinical_Dataset` |
| `hl7v2_store_id` | Yes | HL7v2 store name | `ehr_messages` |
| `service_account_json` | Yes | Full JSON content of a GCP service account key file | `{"type": "service_account", ...}` |

### Volume Mode

| Parameter | Required | Description | Example |
|---|---|---|---|
| `source_type` | Yes | Must be `volume` | `volume` |
| `volume_path` | Yes | Absolute UC Volume path containing HL7 files | `/Volumes/catalog/schema/hl7_inbound/` |
| `file_glob` | No | Shell-style glob applied to filenames (default `*`) | `*.hl7` |
| `recursive` | No | `true`/`false`; descend into subdirectories of `volume_path` (default `false`) | `true` |

> **No SDK, no PAT, no HTTP.** Files are read by Spark executors (which have Unity Catalog Volume FUSE access) using plain Python `os.scandir` + `open()`. The only access control is UC: the executor identity needs `READ VOLUME` on the configured Volume. There is no `databricks_host`, no `databricks_token`, and no SQL warehouse involved. The same FUSE pattern is used by the `dicomweb` connector for writing DICOM files (with `WRITE FILES` on the Volume) — this connector is the read-side equivalent.

### File Layout

Each file under `volume_path` is treated as one HL7 v2 payload. Both shapes are supported transparently:

- **One message per file** — e.g. an MLLP-decoded ADT, ORU, or DFT message landed as `ADT_A04_2024-01-15T12_30_00.hl7`. This is the most common deployment.
- **HL7 batch file** — one file containing multiple concatenated messages separated by `MSH|` segment boundaries (per HL7 batch protocol). The connector's `_split_messages` helper detects the boundary and yields each message individually, so cursor advancement is per-batch-file, not per-message.

File contents must be UTF-8 (or ASCII; the connector decodes with `errors="replace"`, so isolated non-UTF-8 bytes don't fail the read). Files that are empty or whose first segment isn't `MSH|` are skipped silently.

### Incremental Cursor

The cursor for volume mode is each file's **last-modified time** (`mtime`), formatted as RFC 3339 (`yyyy-MM-ddTHH:mm:ssZ`). Each micro-batch advances a sliding window `(since, since + window_seconds]` over file mtimes. The end-of-stream guard caps the right side of the window at the connector's init timestamp, so files written after the pipeline started won't be read until the next trigger fires.

> **Important:** Don't overwrite files in `volume_path` in place — the connector treats `mtime` as monotonically advancing. If a file's mtime moves backward (e.g. a re-upload with a preserved older timestamp), the connector will skip it. Either write new files with new names or let new uploads carry the natural current `mtime`.

## Table-Level Options

Set per table via `table_configuration`. The UC connection must include `externalOptionsAllowList` = `segment_type,window_seconds,start_timestamp,max_records_per_batch` for these to take effect.

| Option | Default | Description |
|---|---|---|
| `segment_type` | *(table name)* | Override the HL7 segment type to extract. Only needed for custom Z-segments (e.g., `ZPD`). |
| `window_seconds` | `86400` | Sliding time window size in seconds. Use `3600` for high-volume sources, `86400` for low-volume. |
| `start_timestamp` | *(auto-discovered)* | RFC 3339 timestamp to start reading from (e.g., `2024-01-01T00:00:00Z`). |
| `max_records_per_batch` | *(unbounded)* | Hard upper bound on rows produced by a single micro-batch. When the window contains more rows than this, the iterator stops early and the cursor advances to the `create_time` of the last yielded message; the next batch resumes from there. Use to bound memory on busy stores. |

### Custom Z-Segments

Standard segments (PID, OBX, MSH, etc.) are auto-matched from `source_table` — no extra config needed. For custom Z-segments, set `segment_type`:

```json
{
    "table": {
        "source_table": "custom_patient_demographics",
        "table_configuration": {
            "segment_type": "ZPD"
        }
    }
}
```

Z-segments produce a generic schema with columns `field_1` through `field_25` plus standard metadata columns. The primary key is `message_id` (no `set_id`).

#### Why a positional `field_N` schema?

Z-segments are site-specific and the HL7 standard leaves their structure undefined, so a generic connector can't know field names or types. We expose them positionally (`field_N` = the Nth HL7 field) because it matches how the HL7 ecosystem reads Z-segments (HAPI `getField(3)`, Mirth `seg['ZPV.3']`) and is easy to query in SQL. Each row also keeps `raw_segment` (the full original line), so anything beyond 25 fields or below the field level (components/repetitions) is still recoverable. Typed, named columns would require per-site structure declarations (e.g. Google Healthcare `ParserConfig`) and are intentionally out of scope. The width is the `_GENERIC_FIELD_COUNT` constant in `hl7_v2_schemas.py` (25, sized like rich standard segments such as `OBX`/`PR1`).

## Supported Objects

One table per HL7 v2 segment type. Schemas follow the HL7 v2.9 specification (superset of all prior versions). Not all messages contain all segment types — the tables you see data in depend on your message types (ADT, ORU, DFT, SIU, etc.).

### Patient Administration

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `msh` | MSH | Message Header | `message_id` |
| `evn` | EVN | Event Type | `message_id` |
| `pid` | PID | Patient Identification | `message_id` |
| `pd1` | PD1 | Patient Additional Demographic | `message_id` |
| `pv1` | PV1 | Patient Visit | `message_id` |
| `pv2` | PV2 | Patient Visit Additional | `message_id` |
| `nk1` | NK1 | Next of Kin / Associated Parties | `message_id`, `set_id` |
| `mrg` | MRG | Merge Patient Information | `message_id` |

### Clinical

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `al1` | AL1 | Patient Allergy Information | `message_id`, `set_id` |
| `iam` | IAM | Patient Adverse Reaction Information | `message_id`, `set_id` |
| `dg1` | DG1 | Diagnosis | `message_id`, `set_id` |
| `pr1` | PR1 | Procedures | `message_id`, `set_id` |

### Orders and Results

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `orc` | ORC | Common Order | `message_id`, `set_id` |
| `obr` | OBR | Observation Request | `message_id`, `set_id` |
| `obx` | OBX | Observation Result | `message_id`, `set_id` |
| `nte` | NTE | Notes and Comments | `message_id`, `set_id` |
| `spm` | SPM | Specimen | `message_id`, `set_id` |

### Financial and Insurance

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `in1` | IN1 | Insurance | `message_id`, `set_id` |
| `gt1` | GT1 | Guarantor | `message_id`, `set_id` |
| `ft1` | FT1 | Financial Transaction | `message_id`, `set_id` |

### Pharmacy, Scheduling, and Documents

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `rxa` | RXA | Pharmacy/Treatment Administration | `message_id`, `set_id` |
| `sch` | SCH | Scheduling Activity Information | `message_id` |
| `txa` | TXA | Transcription Document Header | `message_id` |

### Common Metadata Columns

Every table includes these columns for traceability and cross-table joins:

| Column | Description |
|---|---|
| `message_id` | Unique message control ID from MSH-10 — primary join key across all tables |
| `message_timestamp` | Message date/time from MSH-7 (e.g., `20240115120000`) |
| `hl7_version` | HL7 version from MSH-12 (e.g., `2.5.1`) |
| `source_file` | Source identifier — GCP API resource name (`projects/.../messages/<id>`) or, in volume mode, the absolute file path on the UC Volume |
| `send_time` | Original HL7 message send time. In GCP mode this is the API-reported `sendTime`; in volume mode it falls back to the file's mtime, since the wire-level send time is already preserved in `message_timestamp` (MSH-7) |
| `create_time` | When the message became available to the connector. GCP mode: API `createTime`; volume mode: file mtime. Used as the incremental cursor in both modes. |
| `raw_segment` | Original unparsed HL7 segment text |

### Incremental Ingestion

All tables use **append-only** ingestion. HL7 v2 is a messaging protocol — every message is an immutable event. The connector tracks progress via `createTime` using a sliding time window, advancing the cursor after each batch. Messages already processed are never re-fetched.

## Data Type Mapping

### TL;DR — the two-rule policy

The connector is **lossless by design** — every component of every composite (CWE, XCN, XPN, CX, XAD, XON, XTN, EI, EIP, …) is always preserved. Only the packaging differs based on HL7 spec cardinality:

1. **Single-occurrence composite (`0..1` / `1..1`)** → modeled as one nested `STRUCT<...>` column named `<prefix>`, whose struct fields hold the sub-components. Access them with `<prefix>.<subfield>`.
   E.g. `pv1.visit_number` (CX 0..1) is `STRUCT<id, check_digit, assigning_authority, …>`, accessed as `pv1.visit_number.id`, `pv1.visit_number.check_digit`. `dg1.diagnosis_type` (CWE 0..1) is `STRUCT<code, text, coding_system, …>`, accessed as `dg1.diagnosis_type.code`. When the whole composite is absent the struct column is `NULL`.
2. **Repeating composite (`0..*` / `1..*`)** → wrapped into a single `ARRAY<STRUCT<...>>` column. The struct fields hold the same sub-components; access them with `[i].<subfield>`.
   E.g. `pv1.attending_doctor` (XCN 0..*) is `ARRAY<STRUCT<id, family_name, given_name, …>>`, accessed as `pv1.attending_doctor[0].id`, `pv1.attending_doctor[0].family_name`. Every `~`-separated repetition is preserved — nothing is silently dropped.

This mirrors the convention used by the `fhir` connector (CodeableConcept, HumanName, …): each HL7 composite is isomorphic to a nested struct, so the single-occurrence and repeating forms of the same type share the same field names (just wrapped in `ARRAY<>` for the repeating case). The single exception is **DR (date/time range)**, kept as two flat `_start` / `_end` string columns.

The single intentional exception is **`OBX-5` (observation value)**, which is spec-typed `Varies` — its true type is determined at runtime by `OBX-2` (`value_type`). It stays a plain STRING and the consumer must interpret it based on `value_type`.

The table and sections below give the full per-type details and the complete enumeration of array columns.

---

Most leaf HL7 v2 fields are stored as `STRING`. Composite types are modeled losslessly as nested `STRUCT` (single-occurrence) or `ARRAY<STRUCT>` (repeating) columns; the struct field names are identical between the two forms. Downstream SQL can cast as needed.

| HL7 Data Type | Examples | Databricks Type | Struct fields |
|---|---|---|---|
| ST, TX, FT, IS, ID | Plain text, coded values | STRING | Stored as-is |
| NM, SI | Numeric strings | STRING | Cast with `CAST(col AS INT)` as needed |
| TS, DT, TM | Timestamps, dates, times | STRING | HL7 DTM format; datetime fields like `date_of_birth` are parsed to ISO-8601 |
| CE, CWE, CNE | Coded elements | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | 9 active components: `code`, `text`, `coding_system`, `alt_code`, `alt_text`, `alt_coding_system`, `coding_system_version`, `alt_coding_system_version`, `original_text` |
| XPN | Person name | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | 14 components: `family_name`, `given_name`, `middle_name`, `suffix`, `prefix`, `degree`, `name_type_code`, `name_representation_code`, `name_context`, `name_assembly_order`, `name_effective_date`, `name_expiration_date`, `professional_suffix`, `called_by` |
| XAD | Address | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | `street`, `other_designation`, `city`, `state`, `zip`, `country`, `type`, `other_geographic`, `county_parish_code/_text/_coding_system`, `census_tract/_text/_coding_system`, `representation_code`, `effective_date`, `expiration_date`, `expiration_reason/_text/_coding_system`, `temporary_indicator`, `bad_address_indicator`, `usage`, `addressee`, `comment`, `preference_order`, `protection_code/_text/_coding_system`, `identifier` |
| XCN | Composite ID/name | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | `id`, `family_name`, `given_name`, `middle_name`, `suffix`, `prefix`, `degree`, `source_table`, `assigning_authority/_universal_id/_universal_id_type`, `name_type_code`, `check_digit`, `check_digit_scheme`, `identifier_type_code`, `assigning_facility/_universal_id/_universal_id_type`, `name_representation_code`, `name_assembly_order`, `effective_date`, `expiration_date`, `professional_suffix` |
| XON | Organization name | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | `name`, `type_code`, `id`, `check_digit`, `check_digit_scheme`, `assigning_authority/_universal_id/_universal_id_type`, `id_type_code`, `assigning_facility/_universal_id/_universal_id_type`, `name_rep_code`, `identifier` |
| HD | Hierarchic designator | `STRUCT` | `namespace_id`, `universal_id`, `universal_id_type` |
| EI | Entity identifier | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | `entity_identifier`, `namespace_id`, `universal_id`, `universal_id_type` |
| EIP | Entity identifier pair | `STRUCT` (`0..1`) or `ARRAY<STRUCT>` (`0..*`) | Nested: `placer_assigned_identifier STRUCT<entity_identifier, namespace_id, universal_id, universal_id_type>`, `filler_assigned_identifier STRUCT<…same…>`. Single-occurrence EIP at SPM.2, OBR.29, SCH.28; repeating EIP at SPM.3, OBR.54, OBX.33, ORC.8. |
| DLN | Driver's license number | `STRUCT` | `number` (DLN.1 ST), `issuing_state` (DLN.2 IS, Table 0333), `expiration_date` (DLN.3 DT). Used by `pid.drivers_license`. |
| TQ | Timing/quantity | `ARRAY<STRUCT<...TQ fields...>>` | Deprecated composite (v2.5+); repeating array. Fields: `quantity`, `quantity_units`, `interval_repeat_pattern`, `interval_explicit_time`, `duration`, `start_datetime`, `end_datetime`, `priority`, `condition`, `text`, `conjunction`, `order_sequencing`, `occurrence_duration`, `total_occurrences`. Used by `orc.quantity_timing` (ORC-7), `obr.quantity_timing` (OBR-27). |
| SPS | Specimen source | `STRUCT` | Deprecated composite (OBR-15, v2.7+): `code` (SPS.1.1), `text` (SPS.1.2), `additives` (SPS.2.1), `collection_method` (SPS.3), `body_site` (SPS.4.1), `site_modifier` (SPS.5.1), `collection_method_modifier` (SPS.6.1), `role` (SPS.7.1). Used by `obr.specimen_source`. |
| OG | Observation grouper | `STRUCT` | OBX-4 (v2.8.2+): `sub_identifier` (OG.1 ST), `group` (OG.2 NM), `sequence` (OG.3 NM), `identifier` (OG.4 ST). Used by `obx.observation_sub_id`. |
| CX | Extended composite ID | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | `id`, `check_digit`, `check_digit_scheme`, `assigning_authority/_universal_id/_universal_id_type`, `type_code`, `assigning_facility/_universal_id/_universal_id_type`, `effective_date`, `expiration_date`, `assigning_jurisdiction`, `assigning_agency`, `security_check`, `security_check_scheme` |
| PL | Person location | `STRUCT` (or `ARRAY<STRUCT>` if repeating, e.g. `pr1.treating_organizational_unit`) | `point_of_care` (HD.1 of PL.1), `room` (PL.2.1), `bed` (PL.3.1), `facility` (PL.4.1), `status` (PL.5), `type` (PL.6), `building` (PL.7.1), `floor` (PL.8.1), `description` (PL.9), `comprehensive_id` (PL.10.1), `assigning_authority` (PL.11.1). HD-typed sub-components store HD.1 namespace ID. Used by `pv1.assigned_patient_location`, `pv1.prior_patient_location`, `pv1.temporary_location`, `pv1.pending_location`, `pv1.prior_temporary_location`, `pv2.prior_pending_location`, `orc.enterers_location`, `ft1.assigned_patient_location`, `sch.placer_contact_location`, `sch.filler_contact_location`, `sch.entered_by_location`. |
| CP | Composite price | `STRUCT` | `price` (CP.1.1 MO), `currency` (CP.1.2), `price_type` (CP.2, Table 0205), `from_value` (CP.3, NM), `to_value` (CP.4, NM), `range_units` (CP.5.1, CWE code), `range_units_text` (CP.5.2), `range_units_coding_system` (CP.5.3), `range_type` (CP.6, Table 0298). Used by `dg1.outlier_cost`, `in1.policy_deductible`, `in1.policy_limit_amount`, `in1.room_rate_semi_private`, `in1.room_rate_private`, `gt1.guarantor_household_annual_income`, `ft1.transaction_amount_extended`, `ft1.transaction_amount_unit`, `ft1.insurance_amount`, `ft1.unit_cost`. |
| CQ | Composite quantity with units | `STRUCT` | `quantity` (CQ.1, NM), `units` (CQ.2.1, CWE units code). Used by `ft1.ndc_qty_and_uom`, `obr.collection_volume`, `spm.specimen_collection_amount`, `spm.specimen_current_quantity`. |
| DR | Date/time range | STRING (×2) | The one composite kept flat: `{prefix}_start` (DR.1, DTM) and `{prefix}_end` (DR.2, DTM). Used by `ft1.transaction_date_start/_end`, `spm.specimen_collection_datetime_start/_end`, `orc.order_status_date_range_start/_end`. |
| PT | Processing type | `STRUCT` | `id` (PT.1, Table 0103), `mode` (PT.2, Table 0207). Used by `msh.processing_id`. |
| VID | Version identifier | `STRUCT` | `id` (VID.1), `internationalization`/`_text`/`_coding_system` (VID.2 CWE), `international_version`/`_text`/`_coding_system` (VID.3 CWE). Used by `msh.version_id`. |
| AUI | Authorization information | `STRUCT` | `number` (AUI.1 ST), `date` (AUI.2 DT), `source` (AUI.3 ST). Used by `in1.authorization_information`. |
| DLD | Discharge location and date | `STRUCT` | `code` (DLD.1.1 CWE.1), `effective_date` (DLD.2 DTM). Used by `pv1.discharged_to_location`. |
| FC | Financial class | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | `code`, `text`, `coding_system`, `alt_code`, `alt_text`, `alt_coding_system`, `coding_system_version`, `alt_coding_system_version`, `original_text` (FC.1 CWE), `effective_date` (FC.2 DTM). Used by `gt1.guarantor_financial_class` (single) and `pv1.financial_class` (`ARRAY<STRUCT>` per spec). |
| JCC | Job code/class | `STRUCT` | `code`/`text`/`coding_system` (JCC.1 CWE), `class`/`class_text`/`class_coding_system` (JCC.2 CWE), `description` (JCC.3 TX). Used by `nk1.job_code`, `gt1.job_code_class`. |
| MOC | Money and code | `STRUCT` | `monetary_amount` (MOC.1.1 MO.1 NM), `monetary_amount_currency` (MOC.1.2 MO.2 ID), `charge_code`/`charge_code_text`/`charge_code_coding_system` (MOC.2 CWE). Used by `obr.charge_to_practice`. |
| PRL | Parent result link | `STRUCT` | `code`/`text`/`coding_system` (PRL.1 CWE), `sub_id` (PRL.2 ST), `descriptor` (PRL.3 TX). Used by `obr.parent_result`. |
| MO | Money | `STRUCT` | `amount` (MO.1 NM), `currency` (MO.2 ISO 4217 ID). |
| NDL | Name with date and location | `STRUCT` (or `ARRAY<STRUCT>` if repeating) | `id`, `family_name`, `given_name`, `middle_name`, `suffix`, `prefix`, `degree` (CNN sub-components of NDL.1), `start_datetime` (NDL.2), `end_datetime` (NDL.3), `point_of_care`, `room`, `bed`, `facility`, `location_status`, `patient_location_type`, `building`, `floor`. Used by `obr.principal_result_interpreter` (single); `obr.assistant_result_interpreter`, `obr.technician`, `obr.transcriptionist` (`ARRAY<STRUCT>` per spec). |

**Key rules for composite type extraction:**
- Every component of a composite type is preserved — do not extract only component 1 and discard the rest.
- **Cardinality-`1` composites** (separated by `~` only if the sender erroneously sends multiple) are modeled as one nested `STRUCT` column named `<prefix>`. Only the first repetition is captured for these; the full raw value is preserved in `raw_segment`.
- **Cardinality-`*` composites** (truly repeating per HL7 spec — e.g. `patient_names`, `race`, `ethnic_group`, `citizenship`, `interpretation_codes`, `character_set`, `message_profile_identifiers`, `allergy_reaction`) are stored as Spark `ARRAY<STRUCT>` columns with one entry per repetition via the `_xpn_array_fields()` / `_cwe_array_fields()` / `_ei_array_fields()` / `_xtn_array_fields()` helpers. Every repetition (`~`-separated) is preserved — nothing is silently dropped. See the "Non-string column types" subsection below for the full enumeration of array columns.
- **Every CWE / CE / CNE field is modeled losslessly** — even when the spec marks it `0..1` and bound to a small enumeration (e.g. `DG1-6` diagnosis type with only three legal codes `A`/`W`/`F`). The HL7 v2.x harmonization made every coded element CWE-typed, so a sender may populate display text, alternate codes, coding-system version, etc. We always model all 9 CWE components as struct fields `code` (CWE.1), `text` (CWE.2), `coding_system` (CWE.3), `alt_code` (CWE.4), `alt_text` (CWE.5), `alt_coding_system` (CWE.6), `coding_system_version` (CWE.7), `alt_coding_system_version` (CWE.8), `original_text` (CWE.9), accessed as `<prefix>.code`, `<prefix>.text`, … so nothing past the code is discarded.
- When a component is itself a composite type (e.g., HD inside CX.4 or XON.6), its sub-components (separated by `&`) are flattened into named struct fields (e.g. `assigning_authority`, `assigning_authority_universal_id`, `assigning_authority_universal_id_type`).

### Lenient parsing of `ST 0..*` fields that real senders emit as `CWE`-shape

Two HL7 v2.9 fields — **`AL1-5` (Patient Allergy Reaction)** and **`IAM-5` (Patient Adverse Reaction)** — are spec-typed as `ST 0..*` (repeating plain string), but in practice EHRs routinely emit `CWE`-shape values here, e.g. `HIV^Hives^HL70129~RSH^Rash^HL70129`. We model these leniently as `ARRAY<STRUCT<...9 CWE components...>>`:

- When the sender emits **plain ST**, the value lands in element `[0].code` with the other 8 subfields `NULL`.
- When the sender emits **CWE-shape**, all components (`code`, `text`, `coding_system`, …) populate.
- Either way, **every `~`-separated repetition is preserved**.

This means `SELECT allergy_reaction[0].code FROM al1` works for both kinds of sender, and `SELECT explode(allergy_reaction) FROM al1` always recovers every reaction the sender included.

### Lossless CWE / CE / CNE extraction even for "small enumeration" fields

A subtlety of HL7 v2.x is that data type, cardinality, and value-set binding are orthogonal. Many fields are spec-typed `CWE` (or `CE` / `CNE`) — meaning the wire format allows 9 components (`code^text^coding_system^alt_code^alt_text^alt_coding_system^coding_system_version^alt_coding_system_version^original_text`) — even when bound to a tiny enumeration in the standard (e.g. `DG1-6` diagnosis type permits only `A` / `W` / `F` from HL7 table 0052). Senders are still allowed to populate `text`, `coding_system`, etc.; older builds of this connector dropped those values by extracting only component 1.

Every CWE / CE / CNE field across all 23 segment tables is now modeled with the full 9-component decomposition (or `ARRAY<STRUCT<…>>` for `0..*` cardinality). Fields promoted in this round include — non-exhaustively:

- `dg1` — `diagnosis_type` (DG1-6), `drg_grouper_review_code` (DG1-10), `diagnosis_classification` (DG1-17), `drg_diagnosis_determination_status` (DG1-25), `present_on_admission_indicator` (DG1-26).
- `pv1` — `preadmit_test_indicator` (PV1-12), `readmission_indicator` (PV1-13), `contract_code` (PV1-24, now `ARRAY<STRUCT>`), `interest_code` (PV1-28), `transfer_to_bad_debt_code` (PV1-29), `bad_debt_agency_code` (PV1-31), `delete_account_indicator` (PV1-34), `bed_status` (PV1-40).
- `pv2` — `purge_status_code` (PV2-16), `expected_discharge_disposition` (PV2-27).
- `nk1` — `student_indicator` (NK1-24), `job_status` (NK1-34), `handicap` (NK1-36), `vip_indicator` (NK1-39).
- `pr1` — `procedure_functional_type` (PR1-6), `anesthesia_code` (PR1-9), `procedure_drg_type` (PR1-17).
- `obr` — `relevant_clinical_information` (OBR-13, now `ARRAY<STRUCT>`).
- `gt1` — `guarantor_type` (GT1-10), `guarantor_employment_status` (GT1-20), `living_dependency` (GT1-33), `ambulatory_status` (GT1-34, `ARRAY<STRUCT>`), `living_arrangement` (GT1-37), `student_indicator` (GT1-40), `contact_relationship` (GT1-48), `handicap` (GT1-52), `job_status` (GT1-53), `vip_indicator` (GT1-57).
- `in1` — `plan_type` (IN1-15), `assignment_of_benefits` (IN1-20), `coordination_of_benefits` (IN1-21), `release_information_code` (IN1-27), `type_of_agreement_code` (IN1-31), `billing_status` (IN1-32), `company_plan_code` (IN1-35), `prior_insurance_plan_id` (IN1-46), `coverage_type` (IN1-47), `handicap` (IN1-48), `signature_code` (IN1-50), `vip_indicator` (IN1-53).
- `ft1` — `transaction_type` (FT1-6), `insurance_plan_id` (FT1-14), `fee_schedule` (FT1-17), `patient_type` (FT1-18), `advanced_beneficiary_notice_code` (FT1-27), `medically_necessary_dup_proc_reason` (FT1-28), `ndc_code` (FT1-29), `item_number` (FT1-34), `special_processing_code` (FT1-36, `ARRAY<STRUCT>`), `clinic_code` (FT1-37), `service_provider_taxonomy_code` (FT1-40), `revenue_code` (FT1-41), `dme_certificate_of_medical_necessity_transmission_code` (FT1-44), `dme_certification_type_code` (FT1-45), `dme_frequency_code` (FT1-53), `dme_condition_indicator_code` (FT1-55, `ARRAY<STRUCT>`), `service_reason_code` (FT1-56).
- `rxa` — `administered_strength_units` (RXA-14), `administered_drug_strength_volume_units` (RXA-24), `administered_barcode_identifier` (RXA-25).
- `txa` — `document_type` (TXA-2).

For `0..1` CWE fields the column is a `STRUCT` whose `code` field holds CWE.1 — e.g. `dg1.diagnosis_type.code` returns `"A"` / `"W"` / `"F"` — alongside the other 8 fields (`text`, `coding_system`, `alt_code`, `alt_text`, `alt_coding_system`, `coding_system_version`, `alt_coding_system_version`, `original_text`).

For `0..*` CWE fields (`pv1.contract_code`, `obr.relevant_clinical_information`, `gt1.ambulatory_status`, `ft1.special_processing_code`, `ft1.dme_condition_indicator_code`) the column is `ARRAY<STRUCT<…9 fields…>>`. Use the array-querying patterns below; e.g. `pv1.contract_code[0].code` for the first repetition.

The one intentional exception is **`OBX-5` (observation value)**, which is spec-typed `Varies` — its actual data type is whatever `OBX-2` (`value_type`) says it is. That field stays a plain STRING; consumers must interpret it based on the sibling `value_type` column.

### Lossless `0..*` extraction for XCN / CX / XAD / XON / XTN / EIP composite fields

> **Breaking change.** Earlier builds of this connector flattened spec-typed `0..*` composite fields (provider names, identifier lists, addresses, organization names, telecoms, EIPs) into the first repetition's STRING sub-columns — e.g. `pv1.attending_doctor_id` (STRING). Every additional repetition was silently dropped, and even within the first repetition the structure was lossy for callers wanting downstream sub-components. As of this release, **74 fields across 18 segments** are promoted from flat helpers to `ARRAY<STRUCT<...>>`. Downstream queries that referenced `<prefix>_<subfield>` columns (e.g. `attending_doctor_id`, `attending_doctor_family_name`) **must be rewritten** to use array element access (`attending_doctor[0].id`, `attending_doctor[0].family_name`). See "Querying array-of-struct columns" below for SQL patterns.

The full list of newly-promoted fields, grouped by composite type:

- **XCN 0..* (24 fields)** — `dg1.diagnosing_clinician` (DG1-16), `evn.operator` (EVN-5), `ft1.performed_by`/`ordered_by`/`entered_by` (FT1-20/21/24), `in1.verification_by` (IN1-30), `obr.collector` (OBR-10), `obx.responsible_observer` (OBX-16), `pv1.attending_doctor`/`referring_doctor`/`consulting_doctor`/`admitting_doctor` (PV1-7/8/9/17), `pv2.referral_source` (PV2-13), `rxa.administering_provider` (RXA-10), `sch.placer_contact_person`/`filler_contact_person`/`entered_by_person` (SCH-12/16/20), `txa.primary_activity_provider`/`originator`/`assigned_document_authenticator`/`transcriptionist`/`distributed_copies` (TXA-5/9/10/11/23).
- **CX 0..* / 1..* (13 fields)** — `gt1.guarantor_number`/`guarantor_employee_id_number`/`guarantor_employer_id_number` (GT1-2/19/29), `in1.insureds_id_number` (IN1-49), `mrg.prior_patient_id` (MRG-1, `1..*`), `mrg.prior_alternate_visit_id` (MRG-6), `nk1.associated_party_identifiers` (NK1-33), `obr.alternate_placer_order` (OBR-53), `orc.alternate_placer_order_number` (ORC-33), `pd1.duplicate_patient` (PD1-10), `pid.patient_id` (PID-3, `1..*`), `pid.mothers_identifier` (PID-21), `pv1.alternate_visit_id` (PV1-50), `spm.accession_id`/`other_specimen_id` (SPM-30/31).
- **XAD 0..* (8 fields)** — `gt1.guarantor_address`/`guarantor_employer_address` (GT1-5/17), `in1.insurance_company_address`/`insureds_address`/`insureds_employers_address` (IN1-5/19/44), `nk1.address`/`contact_persons_address` (NK1-4/32), `pid.address` (PID-11), `sch.placer_contact_address`/`filler_contact_address` (SCH-14/18).
- **XTN 0..* (11 fields)** — `gt1.guarantor_ph_num_home`/`guarantor_ph_num_business`/`guarantor_employer_phone_number`/`contact_persons_telephone_number` (GT1-6/7/18/46), `in1.insurance_co_phone_number` (IN1-7), `nk1.phone_number`/`business_phone`/`contact_person_telephone` (NK1-5/6/31), `pid.home_phone`/`business_phone`/`patient_telecom` (PID-13/14/40), `sch.entered_by_phone_number` (SCH-21).
- **XON 0..* (8 fields)** — `gt1.guarantor_organization_name`/`guarantor_employers_org_name` (GT1-21/51), `in1.insurance_company_name`/`group_name`/`insureds_group_emp_name` (IN1-4/9/11), `nk1.organization_name` (NK1-13), `pd1.patient_primary_facility`/`place_of_worship` (PD1-3/14), `pv2.clinic_organization` (PV2-23).
- **CWE 0..* (4 additional fields)** — `nte.coded_comment` (NTE-9), `obx.observation_value_absent_reason` (OBX-32), `pid.tribal_citizenship` (PID-39), `txa.folder_assignment` (TXA-24). (These were missed in the earlier CWE pass.)
- **EIP 0..* (4 fields)** — `obx.observation_related_specimen` (OBX-33), `orc.parent_order` (ORC-8), `spm.specimen_parent_ids` (SPM-3), `obr.parent_order` (OBR-54). These are modeled as `ARRAY<STRUCT<placer_assigned_identifier: STRUCT<...4 EI fields...>, filler_assigned_identifier: STRUCT<...4 EI fields...>>>`.
- **EIP 0..1 expanded to 8 flat columns (3 fields)** — `spm.specimen_id_*` (SPM-2), `obr.parent_results_observation_identifier_*` (OBR-29), `sch.alternate_placer_order_group_number_*` (SCH-28).
- **DLN 0..1 (1 field)** — `pid.drivers_license_*` (PID-20). DLN composite expanded into 3 flat columns: `drivers_license_number`, `drivers_license_issuing_state`, `drivers_license_expiration_date`.
- **TQ 0..* (2 fields)** — `orc.quantity_timing` (ORC-7), `obr.quantity_timing` (OBR-27). TQ stored as `ARRAY<STRUCT<...>>`.
- **SPS 0..1 (1 field)** — `obr.specimen_source_*` (OBR-15). SPS expanded into 8 flat columns.
- **MOC 0..1 (1 field)** — `obr.charge_to_practice_*` (OBR-23). MOC expanded into 5 flat columns.
- **OG 0..1 (1 field)** — `obx.observation_sub_id*` (OBX-4). OG expanded into 4 flat columns (backward-compatible with legacy ST sub-IDs in OG.1).

**Field renames in this update:** `nk1.nk_birth_place` renamed to `nk1.birth_place` (NK1-38) to match PID.23 naming convention. `obr.action_code` (OBR-55), `obx.action_code` (OBX-31), `orc.action_code` (ORC-35), `spm.action_code` (SPM-35) are new columns, not renames from prior columns.

`xtn` only required flipping the schema/extractor calls — the `_xtn_array_*` helper already existed from an earlier round. The other types (`_xcn_array_*`, `_xon_array_*`, `_xad_array_*`, `_cx_array_*`, `_eip_array_*`) are newly introduced.

### Querying array-of-struct columns

For any `ARRAY<STRUCT<...>>` column (`pid.race`, `pid.patient_id`, `pv1.attending_doctor`, `al1.allergy_reaction`, `obx.observation_related_specimen`, etc.), use one of these patterns:

```sql
-- First repetition only (closest analog to the old flat columns)
SELECT
  message_id,
  race[0].code             AS race_code,
  race[0].text             AS race_text,
  patient_id[0].id         AS mrn,
  patient_id[0].type_code  AS id_type
FROM pid;

-- Pull a single primary attending physician's identifier/name from PV1
SELECT
  message_id,
  attending_doctor[0].id           AS attending_id,
  attending_doctor[0].family_name  AS attending_family_name,
  attending_doctor[0].given_name   AS attending_given_name
FROM pv1;

-- Fan out one row per repetition
SELECT
  message_id,
  reaction.code,
  reaction.text
FROM al1
LATERAL VIEW explode(allergy_reaction) AS reaction;

SELECT
  message_id,
  d.id AS clinician_id,
  d.family_name
FROM dg1
LATERAL VIEW explode(diagnosing_clinician) AS d;

-- Count repetitions or filter by any one of them
SELECT message_id, size(citizenship) AS num_citizenships FROM pid;
SELECT message_id FROM pid
WHERE exists(race, r -> r.code = '2028-9');

-- EIP (paired EI) — observation-related specimen on OBX-33
SELECT
  message_id,
  observation_related_specimen[0].placer_assigned_identifier.entity_identifier AS placer_spec_id,
  observation_related_specimen[0].filler_assigned_identifier.entity_identifier AS filler_spec_id
FROM obx;
```

**Non-string column types:**

- `set_id` is stored as `BIGINT`.
- Datetime fields parsed from HL7 DTM format are stored as `TIMESTAMP`.
- Truly-repeating HL7 fields (cardinality `*`, separated by `~`) are stored as Spark `ARRAY` columns rather than collapsed to first-repetition strings. There are 140+ such columns spanning effectively every segment table. Eight element shapes:
  - `ARRAY<STRUCT<...>>` of **XPN (person name)** structs — one entry per repetition of person-name fields, e.g. `pid.patient_names`, `pid.mothers_maiden_names`, `nk1.contact_persons`, `gt1.guarantor_names`, `mrg.prior_patient_names`. The struct has 14 fields (`family_name`, `given_name`, `middle_name`, `suffix`, `prefix`, `degree`, `name_type_code`, `name_representation_code`, `name_context`, `name_assembly_order`, `name_effective_date`, `name_expiration_date`, `professional_suffix`, `called_by`).
  - `ARRAY<STRUCT<...>>` of **CWE / CNE** structs (9 components: `code`, `text`, `coding_system`, `alt_code`, `alt_text`, `alt_coding_system`, `coding_system_version`, `alt_coding_system_version`, `original_text`) — one entry per repetition. Examples: `pid.race`, `pid.ethnic_group`, `pid.citizenship`, `pid.identity_reliability_code`, `nk1.living_dependency`, `nk1.ambulatory_status`, `nk1.citizenship`, `nk1.ethnic_group`, `nk1.contact_reason`, `nk1.race`, `pd1.living_dependency`, `pd1.advance_directive_code`, `pv1.ambulatory_status`, `pv1.contract_code`, `pv2.visit_user_code`, `pv2.notify_clergy_code`, `pv2.recreational_drug_use_code`, `pv2.precaution_code`, `pv2.advance_directive_code_pv2`, `obr.relevant_clinical_information`, `obr.reason_for_study`, `obr.transport_logistics`, `obr.collectors_comment`, `obr.planned_patient_transport_comment`, `obr.procedure_code_modifier`, `obr.placer_supplemental_service_info`, `obr.filler_supplemental_service_info`, `obx.interpretation_codes`, `obx.observation_method`, `obx.observation_site`, `obx.local_process_control`, `al1.allergy_reaction` (lenient ST), `iam.allergy_reaction` (lenient ST), `pr1.procedure_code_modifier`, `pr1.tissue_type_code`, `spm.specimen_type_modifier`, `spm.specimen_additives`, `spm.specimen_source_site_modifier`, `spm.specimen_role`, `spm.specimen_handling_code`, `spm.specimen_risk_code`, `spm.specimen_reject_reason`, `spm.specimen_condition`, `gt1.ambulatory_status`, `gt1.citizenship`, `gt1.ethnic_group`, `gt1.guarantor_race`, `ft1.diagnosis_code`, `ft1.ft1_procedure_code_modifier`, `ft1.special_processing_code`, `ft1.dme_condition_indicator_code`, `rxa.administration_notes`, `rxa.substance_manufacturer_name`, `rxa.substance_treatment_refusal_reason`, `rxa.indication`, `msh.security_handling_instructions`, `msh.special_access_restriction`.
  - `ARRAY<STRUCT<...>>` of **EI (entity identifier)** structs (4 components: `entity_identifier`, `namespace_id`, `universal_id`, `universal_id_type`) — e.g. `msh.message_profile_identifiers`, `obx.equipment_instance_identifier`, `sch.sch_placer_order_number`, `sch.sch_filler_order_number`, `txa.placer_order_number`.
  - `ARRAY<STRUCT<...>>` of **XTN (telecom)** structs (18 components) — `pid.home_phone`, `pid.business_phone`, `pid.patient_telecom`, `nk1.phone_number`, `nk1.business_phone`, `nk1.contact_person_telephone`, `in1.insurance_co_phone_number`, `gt1.guarantor_ph_num_home`, `gt1.guarantor_ph_num_business`, `gt1.guarantor_employer_phone_number`, `gt1.contact_persons_telephone_number`, `sch.entered_by_phone_number`, `obr.order_callback_phone`, `orc.call_back_phone`.
  - `ARRAY<STRUCT<...>>` of **XCN (person identifier + name)** structs (23 components: `id`, `family_name`, `given_name`, `middle_name`, …) — `dg1.diagnosing_clinician`, `evn.operator`, `ft1.performed_by`, `ft1.ordered_by`, `ft1.entered_by`, `in1.verification_by`, `obr.collector`, `obx.responsible_observer`, `pv1.attending_doctor`, `pv1.referring_doctor`, `pv1.consulting_doctor`, `pv1.admitting_doctor`, `pv2.referral_source`, `rxa.administering_provider`, `sch.placer_contact_person`, `sch.filler_contact_person`, `sch.entered_by_person`, `txa.primary_activity_provider`, `txa.originator`, `txa.assigned_document_authenticator`, `txa.transcriptionist`, `txa.distributed_copies`.
  - `ARRAY<STRUCT<...>>` of **CX (extended composite ID)** structs (16 components: `id`, `check_digit`, `assigning_authority`, `type_code`, …) — `pid.patient_id`, `pid.mothers_identifier`, `mrg.prior_patient_id`, `mrg.prior_alternate_visit_id`, `pv1.alternate_visit_id`, `obr.alternate_placer_order` (OBR-53), `orc.alternate_placer_order_number` (ORC-33), `gt1.guarantor_number`, `gt1.guarantor_employee_id_number`, `gt1.guarantor_employer_id_number`, `in1.insureds_id_number`, `nk1.associated_party_identifiers`, `pd1.duplicate_patient`, `spm.accession_id` (SPM-30), `spm.other_specimen_id` (SPM-31).
  - `ARRAY<STRUCT<...>>` of **XAD (address)** structs (30 components: `street`, `city`, `state`, `zip`, …) — `pid.address`, `nk1.address`, `nk1.contact_persons_address`, `gt1.guarantor_address`, `gt1.guarantor_employer_address`, `in1.insurance_company_address`, `in1.insureds_address`, `in1.insureds_employers_address`, `sch.placer_contact_address`, `sch.filler_contact_address`.
  - `ARRAY<STRUCT<...>>` of **XON (organization name + id)** structs (14 components: `name`, `type_code`, `id`, `assigning_authority`, …) — `in1.insurance_company_name`, `in1.group_name`, `in1.insureds_group_emp_name`, `gt1.guarantor_organization_name`, `gt1.guarantor_employers_org_name`, `nk1.organization_name`, `pd1.patient_primary_facility`, `pd1.place_of_worship`, `pv2.clinic_organization`.
  - `ARRAY<STRUCT<placer_assigned_identifier: STRUCT<...EI...>, filler_assigned_identifier: STRUCT<...EI...>>>` of **EIP (entity identifier pair)** structs — `obx.observation_related_specimen` (OBX-33), `orc.parent_order` (ORC-8), `spm.specimen_parent_ids` (SPM-3), `obr.parent_order` (OBR-54). Access as `obx.observation_related_specimen[0].placer_assigned_identifier.entity_identifier`.
  - `ARRAY<STRING>` for repeating simple ID/IS fields — `msh.character_set`, `obx.nature_of_abnormal_test`.

Non-repeating composite fields (single occurrence) remain flattened into separate `STRING` columns rather than wrapped in single-element arrays, matching the convention in the API doc's composite-type table above. The full unparsed value is always preserved in `raw_segment`.

## Troubleshooting

**Both modes:**
- **Missing segments** — Not all messages contain all segment types. ADT messages typically include MSH, EVN, PID, PV1 but not OBR/OBX. ORU messages include ORC, OBR, OBX but may omit EVN, AL1, DG1.
- **Slow ingestion** — Reduce `window_seconds` to process smaller batches.

**GCP mode:**
- **Auth errors** — Verify `service_account_json` is the complete JSON key and the service account has Healthcare API permissions.
- **No data** — Check that `location`, `dataset_id`, and `hl7v2_store_id` are correct in the GCP Console.
- **Quota limits** — Check the [GCP quota console](https://console.cloud.google.com/iam-admin/quotas).

**Volume mode:**
- **Volume path not found** — Verify `volume_path` is an absolute UC path starting with `/Volumes/`, the Volume exists, and the Spark executor identity (not just the workspace user who authored the pipeline) has `READ VOLUME` on it. A missing FUSE mount or missing grant surfaces as "file not found" rather than an explicit permission error.
- **No data picked up** — Confirm files actually exist under `volume_path` (`ls /Volumes/cat/sch/vol/path/`) and their mtimes are in the past. The cursor uses strict `>` on mtime, so a file with mtime exactly equal to `start_timestamp` is excluded by design. If you set `file_glob`, double-check that filenames match (e.g. `*.hl7` won't match `message_1.txt`).
- **Subdirectories ignored** — Set `recursive` to `true` in the connection to walk subdirectories of `volume_path`.
- **Parse errors** — Files must be raw HL7 pipe-delimited text starting with `MSH|`. Base64-encoded payloads aren't auto-decoded in volume mode; decode upstream before landing the file.
- **Files re-ingested unexpectedly** — Check whether something is re-uploading files with a current mtime; the connector treats every advancing mtime as a new event window. Use new filenames for re-uploads if you want them ignored.

### Debugging tools

`gcpreader.py` is a standalone CLI for fetching and pretty-printing HL7 v2 messages directly from a GCP Healthcare API HL7v2 store — useful when you want to sanity-check credentials, inspect raw message structure, or verify what the connector will see, without spinning up Spark.

```bash
# With a config file (same format as connector connection options)
python src/databricks/labs/community_connector/sources/hl7_v2/gcpreader.py \
    --config /path/to/dev_config_gcp.json --limit 5

# With individual arguments
python src/databricks/labs/community_connector/sources/hl7_v2/gcpreader.py \
    --project-id my-project --location us-central1 \
    --dataset-id my-dataset --hl7v2-store-id my-store \
    --service-account-json /path/to/sa-key.json --limit 5
```

It's intentionally not part of the connector runtime (excluded from the bundled `_generated_*` source) and is purely a developer convenience.

## UI Setup Guide

This section walks through creating HL7 v2 ingestion pipelines via the Databricks UI using the **Custom Connector** flow. Until the HL7 v2 connector is published to the main Lakeflow Community Connectors repository, you point the UI to a fork that hosts the source.

Both GCP and Volume modes use the same source code and Git repo — the only difference is which connection (and credentials) the pipeline uses.

### Creating Pipeline #1: GCP Mode

#### Step 1 — Add the Custom Connector

1. In your Databricks workspace, click **Jobs & Pipelines** in the left sidebar.
2. Click **Create new** dropdown > **Ingestion pipeline** ("Ingest data from apps, databases and files").
3. In the "Add data" screen, scroll down to the **Community connectors** section.
4. Click **Custom Connector**.
5. In the "Add custom connector" dialog:
   - **Source name**: `hl7_v2`
   - **Git Repository URL**: the fork hosting this connector (e.g. `https://github.com/<your-fork>/lakeflow-community-connectors`)
6. Click **Add Connector**.
7. The HL7 v2 connector now appears in the Community connectors list.

#### Step 2 — Select the Connector and Configure Connection

1. Click on your **hl7_v2** connector in the Community connectors section.
2. This opens the connection setup wizard — "Ingest data from hl7_v2".
3. Under "Connection to the source", click **Create connection**.
4. Give it a name, e.g. `hl7_v2_gcp_connection`.
5. The UI shows the structured form from `connector_spec.yaml`. Fill in the parameters below.

**Where to find GCP connection parameters**

| Parameter | Value to enter | Where to find it |
|---|---|---|
| `source_type` | `gcp` (or leave blank — defaults to `gcp`) | N/A |
| `project_id` | Your GCP project ID (e.g. `my-gcp-project`) | **GCP Console** — visible in the project selector dropdown at the top of the page |
| `location` | GCP region (e.g. `us-central1`, `northamerica-northeast1`) | **GCP Console** → **Healthcare > Datasets** → click your dataset — the location is shown in the breadcrumb next to the dataset name |
| `dataset_id` | Healthcare API dataset ID (e.g. `My_Clinical_Dataset`) | **GCP Console** → **Healthcare > Datasets** → the dataset name listed on the page |
| `hl7v2_store_id` | HL7v2 store name (e.g. `ehr_messages`) | **GCP Console** → **Healthcare > Datasets** → click your dataset → click the HL7v2 store — the store ID is in the Details section |
| `service_account_json` | Paste the **entire contents** of your GCP service account JSON key file | **GCP Console** → **IAM & Admin > Service Accounts** → select or create a service account → **Keys** tab → **Add Key** → **Create new key** → **JSON**. A `.json` file downloads — paste its full contents. |

> **Service account permissions required**: The service account must have `healthcare.hl7V2Messages.list` and `healthcare.hl7V2Messages.get`. The easiest way is to grant the **Healthcare HL7v2 Message Editor** role.

> **Tip**: The full resource path for your HL7v2 store follows this pattern — you can find all four values from this single string on the store details page:
> ```
> projects/{project_id}/locations/{location}/datasets/{dataset_id}/hl7V2Stores/{hl7v2_store_id}
> ```

**When to add `externalOptionsAllowList`**

If the UI shows only generic key-value pairs (i.e., it couldn't load the spec from your repo), you must add `externalOptionsAllowList` as an explicit key-value entry:
- **Key**: `externalOptionsAllowList`
- **Value**: `segment_type,window_seconds,start_timestamp,max_records_per_batch`

If the UI shows the structured form (with labeled fields from `connector_spec.yaml`), it is typically handled automatically. See [Table-Level Options](#table-level-options) above for what each option controls.

6. Click **Next** to create the connection.

#### Step 3 — Configure Ingestion Setup

1. **Pipeline name**: Enter a descriptive name (e.g. `hl7_v2_gcp_pipeline`).
2. **Event log location**:
   - **Catalog**: Select a catalog (e.g. `main` or your user catalog).
   - **Schema**: Select or create a schema (e.g. `hl7_gcp`).
3. **Root path**: Enter the workspace path where the pipeline will store its source code, metadata, and checkpoint information.
   - Example: `/Users/your.email@company.com/hl7_v2_gcp_pipeline`
   - This is a workspace path (not a DBFS or Volume path).
4. Click **Create** to create the ingestion pipeline.

#### Step 4 — Configure Pipeline Source Code

Once created, you are redirected to the pipeline details page.

1. Click **Open in Editor** to edit the auto-generated `ingest.py`.
2. The auto-generated skeleton won't have your table configuration — **you must replace it** with the actual pipeline spec.
3. Paste the following, adjusting `connection_name` and tables as needed (see [Supported Objects](#supported-objects) above for the full list of segment types):

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

source_name = "hl7_v2"
connection_name = "hl7_v2_gcp_connection"

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")
register(spark, source_name)

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {"table": {"source_table": "msh", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "pid", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "pv1", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "obx", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "obr", "table_configuration": {"window_seconds": "86400"}}},
    ],
}

ingest(spark, pipeline_spec)
```

4. Save your changes.

#### Step 5 — Run the Pipeline

1. Return to the pipeline details page.
2. Click **Start** to kick off the first ingestion run.
3. After the first run, you'll see a visual DAG showing each segment table.

### Creating Pipeline #2: Volume Mode

#### Step 1 — Add the Custom Connector (if not already added)

If you already added the custom connector during the GCP setup, it persists — you don't need to add it again. Just create a new ingestion pipeline:

1. **Jobs & Pipelines** > **Create new** > **Ingestion pipeline**.
2. Find **hl7_v2** in the Community connectors list and click on it.

If you haven't added it yet, follow Step 1 from the GCP section above (same source name and Git repo URL).

#### Step 2 — Prepare the UC Volume

1. In Catalog Explorer, create (or pick) a Volume under the catalog/schema where HL7 files will land — e.g. `/Volumes/catalog/schema/hl7_inbound`.
2. Land your HL7 files in that Volume. Most teams point an SFTP/MLLP gateway, file-share sync, or a one-off `databricks fs cp` at this path. One file per HL7 message is recommended; batch files (multiple `MSH|...` blocks in one file) are also supported.
3. Grant the Spark executor identity (the pipeline's run-as service principal or user) `READ VOLUME` on this Volume. File reads happen on executors, so it's the executor identity — not the workspace user who authored the pipeline — that needs the grant. This mirrors the permission model used by the `dicomweb` connector for `WRITE FILES`.

#### Step 3 — Create the Volume Connection

1. Under "Connection to the source", click **Create connection**.
2. Name it, e.g. `hl7_v2_volume_connection`.
3. Fill in the parameters (see [Volume Mode](#volume-mode) above for details):

| Parameter | Value to enter |
|---|---|
| `source_type` | `volume` (**required** — this switches the connector to Volume mode) |
| `volume_path` | Absolute UC Volume path (e.g. `/Volumes/catalog/schema/hl7_inbound/`) |
| `file_glob` | *(optional)* Filename glob, e.g. `*.hl7` |
| `recursive` | *(optional)* `true` to descend into subdirectories; defaults to `false` |

> If the UI shows only generic key-value pairs, also add `externalOptionsAllowList` = `segment_type,window_seconds,start_timestamp,max_records_per_batch`.

4. Click **Next** to create the connection.

#### Step 4 — Configure Ingestion Setup

1. **Pipeline name**: e.g. `hl7_v2_volume_pipeline`.
2. **Event log location**:
   - **Catalog**: Select your catalog.
   - **Schema**: e.g. `hl7_volume`.
3. **Root path**: Workspace path for pipeline assets.
   - Example: `/Users/your.email@company.com/hl7_v2_volume_pipeline`
4. Click **Create**.

#### Step 5 — Configure Pipeline Source Code

1. Click **Open in Editor**.
2. Replace the auto-generated code with:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

source_name = "hl7_v2"
connection_name = "hl7_v2_volume_connection"

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")
register(spark, source_name)

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {"table": {"source_table": "msh", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "pid", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "pv1", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "obx", "table_configuration": {"window_seconds": "86400"}}},
        {"table": {"source_table": "obr", "table_configuration": {"window_seconds": "86400"}}},
    ],
}

ingest(spark, pipeline_spec)
```

3. Save your changes.

#### Step 6 — Run the Pipeline

Click **Start** on the pipeline page.

### Key Points

- **Both pipelines use the same Custom Connector** (same Git repo and source name `hl7_v2`). The connection parameters determine GCP vs Volume mode.
- **The Custom Connector only needs to be added once** — after that, `hl7_v2` stays in your Community connectors list for future pipelines.
- **The `ingest.py` replacement is critical** — the auto-generated skeleton won't work without your pipeline spec.

## Sample Data Attribution

The sample messages under `tests/unit/sources/hl7_v2/samples/` include messages adapted from [CDCgov/lib-hl7v2-bumblebee](https://github.com/CDCgov/lib-hl7v2-bumblebee) (Apache License 2.0). Additional synthetic messages were created for segment types not present in the CDC corpus.

## References

- [Google Cloud Healthcare API — HL7v2 Concepts](https://cloud.google.com/healthcare-api/docs/concepts/hl7v2)
- [HL7 Version 2.9 Specification](https://www.hl7.eu/HL7v2x/v29/hl7v29.htm)
- [Databricks Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
