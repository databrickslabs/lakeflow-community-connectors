# **HL7 v2 API Documentation**

## **Authorization**

The HL7 v2 connector reads messages from the **Google Cloud Healthcare API** (HL7v2 store) using a **Google Cloud service account**. Authentication uses OAuth2 Bearer tokens obtained from a service account JSON key.

**Auth Method**: Service Account JSON Key → `google-auth` library → Bearer token

**Python (google-auth):**
```python
import google.oauth2.service_account
import google.auth.transport.requests

info = json.loads(service_account_json)  # full JSON string of SA key
creds = google.oauth2.service_account.Credentials.from_service_account_info(
    info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
creds.refresh(google.auth.transport.requests.Request())
# Use creds.token as Bearer token in Authorization header
```

**Required OAuth Scope**: `https://www.googleapis.com/auth/cloud-healthcare` or `https://www.googleapis.com/auth/cloud-platform`

**Required IAM Permissions**:
- `healthcare.hl7V2Messages.list`
- `healthcare.hl7V2Messages.get`

**Connection Parameters:**

| Parameter | Required | Description |
|---|---|---|
| `project_id` | Yes | GCP project ID containing the Healthcare dataset |
| `location` | Yes | GCP region, e.g. `us-central1` |
| `dataset_id` | Yes | Healthcare API dataset ID |
| `hl7v2_store_id` | Yes | HL7v2 store name within the dataset |
| `service_account_json` | Yes | Full JSON content of a GCP service account key file |

---

## **Object List**

The object list is **static** — defined by the HL7 v2 specification. Each segment type maps to one table. Schemas follow the HL7 v2.9 specification (the latest version, which is a superset of all prior versions). The connector supports the following segments, organized by functional domain:

### Patient Administration

| Table Name | HL7 Segment | Fields | Description |
|---|---|---|---|
| `msh` | MSH | 28 | Message Header — one row per HL7 message |
| `evn` | EVN | 7 | Event Type — trigger event metadata |
| `pid` | PID | 40 | Patient Identification — demographics and identifiers |
| `pd1` | PD1 | 23 | Patient Additional Demographic — living will, organ donor, primary facility |
| `pv1` | PV1 | 54 | Patient Visit — encounter/admission data |
| `pv2` | PV2 | 50 | Patient Visit Additional — admit reason, expected dates, mode of arrival |
| `nk1` | NK1 | 41 | Next of Kin / Associated Parties |
| `mrg` | MRG | 7 | Merge Patient Information — prior identifiers for patient merges |

### Clinical

| Table Name | HL7 Segment | Fields | Description |
|---|---|---|---|
| `al1` | AL1 | 6 | Patient Allergy Information (snapshot mode) |
| `iam` | IAM | 30 | Patient Adverse Reaction Information (action code mode — newer replacement for AL1) |
| `dg1` | DG1 | 26 | Diagnosis — admitting, working, and final diagnoses |
| `pr1` | PR1 | 25 | Procedures — surgical, diagnostic, and therapeutic procedures |

### Orders & Results

| Table Name | HL7 Segment | Fields | Description |
|---|---|---|---|
| `orc` | ORC | 38 | Common Order — order control, status, and provider info |
| `obr` | OBR | 55 | Observation Request — lab/radiology orders |
| `obx` | OBX | 33 | Observation Result — individual test results |
| `nte` | NTE | 9 | Notes and Comments — free-text annotations attached to orders/results |
| `spm` | SPM | 35 | Specimen — specimen type, collection, and handling details |

### Financial / Insurance

| Table Name | HL7 Segment | Fields | Description |
|---|---|---|---|
| `in1` | IN1 | 55 | Insurance — policy coverage and billing information |
| `gt1` | GT1 | 57 | Guarantor — financially responsible party |
| `ft1` | FT1 | 56 | Financial Transaction — charges, payments, adjustments |

### Pharmacy

| Table Name | HL7 Segment | Fields | Description |
|---|---|---|---|
| `rxa` | RXA | 29 | Pharmacy/Treatment Administration — medication administration records |

### Scheduling

| Table Name | HL7 Segment | Fields | Description |
|---|---|---|---|
| `sch` | SCH | 28 | Scheduling Activity Information — appointment details |

### Documents

| Table Name | HL7 Segment | Fields | Description |
|---|---|---|---|
| `txa` | TXA | 28 | Transcription Document Header — document metadata and status |

> **Note on field counts.** The field count in each row above reflects the number of HL7 segment field positions (e.g. MSH.1 through MSH.28), not the number of Spark columns. Composite types (CWE, XCN, CX, EIP, SPS, TQ, MOC, DLN, OG, etc.) expand into multiple Spark columns per position — the actual Spark schema has many more columns than the HL7 field count. See the per-segment tables below for the full column breakdown.

Additionally, arbitrary **Z-segments** (site-specific custom segments) can be ingested using the `segment_type` table option, producing a generic schema with `field_1` … `field_25` columns.

Not all messages contain all segments. For example, ADT messages typically include MSH, EVN, PID, PD1, PV1, PV2, NK1, AL1, DG1 but not OBR/OBX; ORU messages include MSH, PID, ORC, OBR, OBX, SPM but may omit EVN, AL1, DG1, NK1; DFT messages include MSH, EVN, PID, PV1, FT1, DG1, GT1, IN1.

---

## **Object Schema**

Schemas are **static** — defined by the **HL7 v2.9 specification** (the latest version, a strict superset of v2.1–v2.8). They are embedded in `hl7_v2_schemas.py` based on the segment definitions below; there is no API to discover them at runtime.

**A note on spec version and the `Source:` links per segment.** The per-segment links below point to the Caristix HL7 v2.5.1 reference because that's the most widely-used freely-available HL7 v2 documentation. Since v2.9 is backwards-compatible with v2.5.1, every field documented in those v2.5.1 references is also a valid v2.9 field. The field tables below include the full v2.9 field set — fields added in v2.6+ are explicitly marked with `(added in v2.X)` in their descriptions where it matters. Notable additions vs v2.5.1: OBX adds fields 26–33 (v2.8+/v2.9+), OBR adds fields 51–55 (v2.8+/v2.9+), ORC adds fields 32–38 (v2.6+/v2.9+), SPM adds fields 30–35 (v2.6+/v2.9+), NTE adds fields 5–9 (v2.6+), IAM adds fields 21–30 (v2.6+), PR1 adds fields 21–25 (v2.6+/v2.7+), PV1 adds fields 53–54 (v2.8+/v2.9+), PV2 adds field 50 (v2.9+), NK1 adds fields 40–41 (v2.7+), PD1 adds fields 22–23 (v2.8+/v2.9+), MSH adds fields 22–28 (v2.6+/v2.7+), IN1 adds fields 54–55 (v2.8+/v2.9+), RXA adds fields 27–29 (v2.6+/v2.9+), SCH adds field 28 (v2.9+), TXA adds fields 24–28 (v2.6+/v2.8+/v2.9+).

**A note on column naming.** Spark column names in `hl7_v2_schemas.py` are pragmatic shortenings of HL7 spec field names, chosen for readability and consistency with the connector's existing conventions. The mapping is usually obvious but a few cases need a bridge note for future maintainers:

| HL7 spec field | Spark column prefix | Why |
|---|---|---|
| OBX.8 "Abnormal Flags" (v2.5.1) / "Interpretation Codes" (v2.7+) | `interpretation_codes` | Code follows v2.7+ rename; same field position, type changed from IS to CWE |
| OBR.4 "Universal Service Identifier" | `service` | Shorter and unambiguous in OBR context |
| PID.3 "Patient Identifier List" | `patient_id` (and `patient_id_namespace_id`, etc.) | Matches industry-standard MRN terminology |
| PID.18 "Patient Account Number" | `patient_account` | Shorter; suffix `_number` is implicit |
| PID.20 "Driver's License Number" (DLN) | `drivers_license_number`, `drivers_license_issuing_state`, `drivers_license_expiration_date` | DLN composite expanded into 3 flat columns |
| MRG.1 "Prior Patient Identifier List" | `prior_patient_id` | Mirrors PID.3 |
| ORC.26 "Advanced Beneficiary Notice Override Reason" | `abn_override_reason` | Standard healthcare abbreviation |
| OBR.23 "Charge to Practice" (MOC) | `charge_to_practice_monetary_amount`, `charge_to_practice_monetary_amount_currency`, `charge_to_practice_charge_code`, `charge_to_practice_charge_code_text`, `charge_to_practice_charge_code_coding_system` | MOC expanded into 5 flat columns |
| OBR.15 "Specimen Source" (SPS) | `specimen_source`, `specimen_source_text`, `specimen_source_additives`, `specimen_source_collection_method`, `specimen_source_body_site`, `specimen_source_site_modifier`, `specimen_source_collection_method_modifier`, `specimen_source_role` | SPS expanded into 8 flat columns |
| OBR.27 / ORC.7 "Quantity/Timing" (TQ) | `quantity_timing` (`ARRAY<STRUCT>`) | TQ repeating; stored as array-of-struct |
| OBR.29 "Parent" (EIP) | `parent_results_observation_identifier_placer_assigned_identifier`, `…_filler_assigned_identifier`, etc. | EIP expanded into 8 flat columns (4 per EI component) |
| OBX.4 "Observation Sub-ID" (OG v2.8.2+) | `observation_sub_id`, `observation_sub_id_group`, `observation_sub_id_sequence`, `observation_sub_id_identifier` | OG expanded into 4 flat columns; OG.1 is backward-compatible with legacy ST sub-IDs |
| NK1.33 "Next of Kin / Associated Parties Identifiers" | `associated_party_identifiers` | Shortened from HL7 spec label |
| NK1.38 "Birth Place" | `birth_place` | Field renamed from prior `nk_birth_place` to match PID convention |
| RXA.1 "Give Sub-ID Counter" | `administration_sub_id_counter` | Disambiguates from RXA.2 / RXA.3 |
| MSH.9 "Message Type" (CWE composite) | `message_code` + `trigger_event` + `message_structure` | The 3 components are exposed as separate columns |
| MSH.20 "Alternate Character Set Handling Scheme" | `alt_character_set_handling` | Shorter; same field |

Composite fields always expand into multiple Spark columns (one per component / sub-component). See **Field Type Mapping** below for the per-type expansion rules.

Every table includes four common columns for traceability and joining:

| Column | Source | Type | Description |
|---|---|---|---|
| `message_id` | MSH-10 | STRING | Unique message control ID — primary join key across all tables |
| `message_timestamp` | MSH-7 | STRING | Message date/time (raw HL7 DTM string, e.g. `20240115120000`) |
| `hl7_version` | MSH-12 | STRING | HL7 version string (e.g. `2.5.1`) |
| `source_file` | filename | STRING | Source `.hl7` filename for traceability |

### MSH — Message Header (28 fields)

Source: [Caristix HL7v2.5.1 MSH](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/MSH)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| MSH.1 | Field Separator | ST | R | 1 | 1 | Always `\|` |
| MSH.2 | Encoding Characters | ST | R | 4 | 1 | Default `^~\&` — component, repetition, escape, subcomponent separators |
| MSH.3 | Sending Application | HD | O | 227 | 1 | Source application identifier. Spark columns: `sending_application` (HD.1), `sending_application_universal_id` (HD.2), `sending_application_universal_id_type` (HD.3) |
| MSH.4 | Sending Facility | HD | O | 227 | 1 | Source facility identifier. Spark columns: `sending_facility`, `sending_facility_universal_id`, `sending_facility_universal_id_type` |
| MSH.5 | Receiving Application | HD | O | 227 | 1 | Destination application identifier. Spark columns: `receiving_application`, `receiving_application_universal_id`, `receiving_application_universal_id_type` |
| MSH.6 | Receiving Facility | HD | O | 227 | 1 | Destination facility identifier. Spark columns: `receiving_facility`, `receiving_facility_universal_id`, `receiving_facility_universal_id_type` |
| MSH.7 | Date/Time Of Message | TS | R | 26 | 1 | Message timestamp (YYYYMMDDHHMMSS format) |
| MSH.8 | Security | ST | O | 40 | 1 | Security classification |
| MSH.9 | Message Type | MSG | R | 15 | 1 | Message type (e.g. `ADT^A01^ADT_A01`) — components stored as: `message_code` (MSH.9.1), `trigger_event` (MSH.9.2), `message_structure` (MSH.9.3) |
| MSH.10 | Message Control ID | ST | R | 20 | 1 | Unique message identifier — used as join key across all tables |
| MSH.11 | Processing ID | PT | R | 3 | 1 | `P` (production), `T` (test), `D` (debug). Spark columns: `processing_id` (PT.1), `processing_id_mode` (PT.2) |
| MSH.12 | Version ID | VID | R | 60 | 1 | HL7 version (e.g. `2.5.1`). Spark columns: `version_id` (VID.1), `version_id_internationalization*` (VID.2 CWE — 3 cols), `version_id_international_version*` (VID.3 CWE — 3 cols) |
| MSH.13 | Sequence Number | NM | O | 15 | 1 | Sequence number for continuous messaging |
| MSH.14 | Continuation Pointer | ST | O | 180 | 1 | Continuation pointer for fragmented messages |
| MSH.15 | Accept Acknowledgment Type | ID | O | 2 | 1 | `AL` (always), `NE` (never), `ER` (error/reject), `SU` (success) |
| MSH.16 | Application Acknowledgment Type | ID | O | 2 | 1 | Same values as MSH.15 |
| MSH.17 | Country Code | ID | O | 3 | 1 | ISO 3166 country code |
| MSH.18 | Character Set | ID | O | 16 | * | Character encoding (e.g. `ASCII`, `8859/1`, `UNICODE UTF-8`). Stored as `ARRAY<STRING>` |
| MSH.19 | Principal Language Of Message | CWE | O | 250 | 1 | Language code. Spark columns: `principal_language`, `principal_language_text`, `principal_language_coding_system`, `principal_language_alt_code`, `principal_language_alt_text`, `principal_language_alt_coding_system`, `principal_language_coding_system_version`, `principal_language_alt_coding_system_version`, `principal_language_original_text` |
| MSH.20 | Alternate Character Set Handling Scheme | ID | O | 20 | 1 | `ISO 2022-1994` or `2.3` |
| MSH.21 | Message Profile Identifier | EI | O | 427 | * | Conformance profile IDs. Stored as `ARRAY<STRUCT<entity_identifier, namespace_id, universal_id, universal_id_type>>` |
| MSH.22 | Sending Responsible Organization | XON | O | 567 | 1 | (added in v2.6) Sending responsible organization. Spark columns: `sending_responsible_org`, `sending_responsible_org_type_code`, `sending_responsible_org_id`, `sending_responsible_org_check_digit`, `sending_responsible_org_check_digit_scheme`, `sending_responsible_org_assigning_authority*` (HD — 3 cols), `sending_responsible_org_id_type_code`, `sending_responsible_org_assigning_facility*` (HD — 3 cols), `sending_responsible_org_name_rep_code`, `sending_responsible_org_identifier` |
| MSH.23 | Receiving Responsible Organization | XON | O | 567 | 1 | (added in v2.6) Receiving responsible organization. Spark columns: `receiving_responsible_org`, `receiving_responsible_org_type_code`, `receiving_responsible_org_id`, `receiving_responsible_org_check_digit`, `receiving_responsible_org_check_digit_scheme`, `receiving_responsible_org_assigning_authority*` (HD — 3 cols), `receiving_responsible_org_id_type_code`, `receiving_responsible_org_assigning_facility*` (HD — 3 cols), `receiving_responsible_org_name_rep_code`, `receiving_responsible_org_identifier` |
| MSH.24 | Sending Network Address | HD | O | 227 | 1 | (added in v2.6) Network address of sending application. Spark columns: `sending_network_address`, `sending_network_address_universal_id`, `sending_network_address_universal_id_type` |
| MSH.25 | Receiving Network Address | HD | O | 227 | 1 | (added in v2.6) Network address of receiving application. Spark columns: `receiving_network_address`, `receiving_network_address_universal_id`, `receiving_network_address_universal_id_type` |
| MSH.26 | Security Classification Tag | CWE | O | 705 | 1 | (added in v2.7) Data classification label for security. Spark columns: `security_classification_tag`, `security_classification_tag_text`, `security_classification_tag_coding_system`, `security_classification_tag_alt_code`, `security_classification_tag_alt_text`, `security_classification_tag_alt_coding_system`, `security_classification_tag_coding_system_version`, `security_classification_tag_alt_coding_system_version`, `security_classification_tag_original_text` |
| MSH.27 | Security Handling Instructions | CWE | O | 705 | * | (added in v2.7) Handling instructions for secure messaging. Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |
| MSH.28 | Special Access Restriction Instructions | CWE | O | 705 | * | (added in v2.7.1) Special access restriction codes. Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |

### EVN — Event Type (7 fields)

Source: [Caristix HL7v2.5.1 EVN](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/EVN)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| EVN.1 | Event Type Code | ID | B | 3 | 1 | Deprecated — use MSH.9 (Table 0003) |
| EVN.2 | Recorded Date/Time | TS | R | 26 | 1 | When the event was recorded in the system |
| EVN.3 | Date/Time Planned Event | TS | O | 26 | 1 | Planned event datetime |
| EVN.4 | Event Reason Code | IS | O | 3 | 1 | Reason for the event (Table 0062) |
| EVN.5 | Operator ID | XCN | O | 250 | * | User who triggered the event (comp 1 = ID) (Table 0188) |
| EVN.6 | Event Occurred | TS | O | 26 | 1 | When the clinical event actually occurred |
| EVN.7 | Event Facility | HD | O | 241 | 1 | Facility where event occurred |

### PID — Patient Identification (40 fields)

Source: [Caristix HL7v2.5.1 PID](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/PID)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| PID.1 | Set ID - PID | SI | O | 4 | 1 | Sequence number |
| PID.2 | Patient ID | CX | B | 20 | 1 | Deprecated — use PID.3 |
| PID.3 | Patient Identifier List | CX | R | 250 | * | MRN and other patient identifiers. CX expanded into 16 flat columns per repetition. Stored as `ARRAY<STRUCT<...16 CX fields...>>` |
| PID.4 | Alternate Patient ID - PID | CX | B | 20 | * | Deprecated — use PID.3 |
| PID.5 | Patient Name | XPN | R | 250 | * | Family name (comp 1), given name (comp 2), middle (comp 3). Stored as `ARRAY<STRUCT<...XPN fields...>>` |
| PID.6 | Mother's Maiden Name | XPN | O | 250 | * | Mother's maiden name |
| PID.7 | Date/Time of Birth | TS | O | 26 | 1 | DOB in YYYYMMDD format. Spark column: `date_of_birth` (TimestampType) |
| PID.8 | Administrative Sex | CWE | O | 250 | 1 | `M`, `F`, `U`, `A`, `N`, `O` (Table 0001). Spark columns: `administrative_sex`, `administrative_sex_text`, `administrative_sex_coding_system`, `administrative_sex_alt_code`, `administrative_sex_alt_text`, `administrative_sex_alt_coding_system`, `administrative_sex_coding_system_version`, `administrative_sex_alt_coding_system_version`, `administrative_sex_original_text` |
| PID.9 | Patient Alias | XPN | B | 250 | * | Deprecated. Spark column: `patient_alias` |
| PID.10 | Race | CWE | O | 250 | * | Race code (Table 0005). Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |
| PID.11 | Patient Address | XAD | O | 250 | * | Street (comp 1), city (comp 3), state (comp 4), zip (comp 5). Stored as `ARRAY<STRUCT<...XAD fields...>>` |
| PID.12 | County Code | IS | B | 4 | 1 | Deprecated. Spark column: `county_code` |
| PID.13 | Phone Number - Home | XTN | O | 250 | * | Home phone number. Stored as `ARRAY<STRUCT<...18 XTN fields...>>` |
| PID.14 | Phone Number - Business | XTN | O | 250 | * | Business phone number. Stored as `ARRAY<STRUCT<...18 XTN fields...>>` |
| PID.15 | Primary Language | CWE | O | 250 | 1 | Primary language (Table 0296). Spark columns: `primary_language`, `primary_language_text`, etc. (9 CWE cols) |
| PID.16 | Marital Status | CWE | O | 250 | 1 | Marital status (Table 0002). Spark columns: `marital_status`, `marital_status_text`, etc. (9 CWE cols) |
| PID.17 | Religion | CWE | O | 250 | 1 | Religion (Table 0006). Spark columns: `religion`, `religion_text`, etc. (9 CWE cols) |
| PID.18 | Patient Account Number | CX | O | 250 | 1 | Account number. Spark columns: `patient_account_number`, `patient_account_number_check_digit`, etc. (16 CX cols) |
| PID.19 | SSN Number - Patient | ST | B | 16 | 1 | Deprecated — use PID.3. Spark column: `ssn` |
| PID.20 | Driver's License Number | DLN | B | 25 | 1 | DLN composite expanded into 3 flat columns: `drivers_license_number` (DLN.1, ST), `drivers_license_issuing_state` (DLN.2, IS — Table 0333), `drivers_license_expiration_date` (DLN.3, DT — TimestampType) |
| PID.21 | Mother's Identifier | CX | O | 250 | * | Mother's patient identifier. Stored as `ARRAY<STRUCT<...16 CX fields...>>` |
| PID.22 | Ethnic Group | CWE | O | 250 | * | Ethnicity (Table 0189). Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |
| PID.23 | Birth Place | ST | O | 250 | 1 | Birth location. Spark column: `birth_place` |
| PID.24 | Multiple Birth Indicator | ID | O | 1 | 1 | Y/N (Table 0136). Spark column: `multiple_birth_indicator` |
| PID.25 | Birth Order | NM | O | 2 | 1 | Birth order for multiple births. Spark column: `birth_order` (IntegerType) |
| PID.26 | Citizenship | CWE | O | 250 | * | Citizenship (Table 0171). Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |
| PID.27 | Veterans Military Status | CWE | O | 250 | 1 | Veterans status (Table 0172). Spark columns: `veterans_military_status`, etc. (9 CWE cols) |
| PID.28 | Nationality | CWE | B | 250 | 1 | Deprecated. Spark columns: `nationality`, etc. (9 CWE cols) |
| PID.29 | Patient Death Date and Time | TS | O | 26 | 1 | Death datetime. Spark column: `patient_death_datetime` (TimestampType) |
| PID.30 | Patient Death Indicator | ID | O | 1 | 1 | Y/N (Table 0136). Spark column: `patient_death_indicator` |
| PID.31 | Identity Unknown Indicator | ID | O | 1 | 1 | Y/N (Table 0136). Spark column: `identity_unknown_indicator` |
| PID.32 | Identity Reliability Code | CWE | O | 250 | * | Reliability code (Table 0445). Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |
| PID.33 | Last Update Date/Time | TS | O | 26 | 1 | Last demographics update. Spark column: `last_update_datetime` (TimestampType) |
| PID.34 | Last Update Facility | HD | O | 241 | 1 | Facility that last updated. Spark columns: `last_update_facility`, `last_update_facility_universal_id`, `last_update_facility_universal_id_type` |
| PID.35 | Species Code | CWE | C | 250 | 1 | Veterinary use (Table 0446). Spark columns: `species_code`, `species_code_text`, etc. (9 CWE cols) |
| PID.36 | Breed Code | CWE | C | 250 | 1 | Veterinary use (Table 0447). Spark columns: `breed_code`, `breed_code_text`, etc. (9 CWE cols) |
| PID.37 | Strain | ST | O | 80 | 1 | Veterinary use. Spark column: `strain` |
| PID.38 | Production Class Code | CWE | O | 250 | 2 | Veterinary use (Table 0429). Spark columns: `production_class_code`, `production_class_code_text`, etc. (9 CWE cols) |
| PID.39 | Tribal Citizenship | CWE | O | 250 | * | Tribal citizenship (Table 0171). Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |
| PID.40 | Patient Telecommunication Information | XTN | O | 2915 | * | (added in v2.7) Patient telecommunication. Stored as `ARRAY<STRUCT<...18 XTN fields...>>`. Spark column array prefix: `patient_telecom` |

### PD1 — Patient Additional Demographic (23 fields)

Source: [Caristix HL7v2.5.1 PD1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/PD1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| PD1.1 | Living Dependency | IS | O | 2 | * | Living dependency code (Table 0223) |
| PD1.2 | Living Arrangement | IS | O | 2 | 1 | Living arrangement code (Table 0220) |
| PD1.3 | Patient Primary Facility | XON | O | 250 | * | Primary care facility name |
| PD1.4 | Patient Primary Care Provider Name and ID No. | XCN | B | 250 | * | Deprecated — primary care provider |
| PD1.5 | Student Indicator | IS | O | 2 | 1 | Student status (Table 0231) |
| PD1.6 | Handicap | IS | O | 2 | 1 | Handicap code (Table 0295) |
| PD1.7 | Living Will Code | IS | O | 2 | 1 | Living will status (Table 0315) |
| PD1.8 | Organ Donor Code | IS | O | 2 | 1 | Organ donor status (Table 0316) |
| PD1.9 | Separate Bill | ID | O | 1 | 1 | Y/N separate billing (Table 0136) |
| PD1.10 | Duplicate Patient | CX | O | 250 | * | Duplicate patient identifiers |
| PD1.11 | Publicity Code | CE | O | 250 | 1 | Publicity code (Table 0215) |
| PD1.12 | Protection Indicator | ID | O | 1 | 1 | Y/N protection (Table 0136) |
| PD1.13 | Protection Indicator Effective Date | DT | O | 8 | 1 | Protection effective date |
| PD1.14 | Place of Worship | XON | O | 250 | * | Place of worship |
| PD1.15 | Advance Directive Code | CE | O | 250 | * | Advance directive code (Table 0435) |
| PD1.16 | Immunization Registry Status | IS | O | 1 | 1 | Immunization registry status (Table 0441) |
| PD1.17 | Immunization Registry Status Effective Date | DT | O | 8 | 1 | Registry status effective date |
| PD1.18 | Publicity Code Effective Date | DT | O | 8 | 1 | Publicity code effective date |
| PD1.19 | Military Branch | IS | O | 5 | 1 | Military branch (Table 0140) |
| PD1.20 | Military Rank/Grade | IS | O | 2 | 1 | Military rank/grade (Table 0141) |
| PD1.21 | Military Status | CWE | O | 705 | 1 | Military status (Table 0142). Spark columns: `military_status`, `military_status_text`, `military_status_coding_system`, `military_status_alt_code`, `military_status_alt_text`, `military_status_alt_coding_system`, `military_status_coding_system_version`, `military_status_alt_coding_system_version`, `military_status_original_text` |
| PD1.22 | Advance Directive Last Verified Date | DT | O | 8 | 1 | (added in v2.8) Date advance directive was last verified. Spark column: `advance_directive_last_verified_date` (TimestampType) |
| PD1.23 | Retirement Date | DT | O | 8 | 1 | (added in v2.9) Date the patient retired. Spark column: `retirement_date` (TimestampType) |

### PV1 — Patient Visit (54 fields)

Source: [Caristix HL7v2.5.1 PV1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/PV1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| PV1.1 | Set ID - PV1 | SI | O | 4 | 1 | Sequence number |
| PV1.2 | Patient Class | IS | R | 1 | 1 | `I` (inpatient), `O` (outpatient), `E` (emergency), `P` (preadmit), `R` (recurring), `B` (obstetrics) (Table 0004) |
| PV1.3 | Assigned Patient Location | PL | O | 80 | 1 | All 11 PL components flattened via `_pl_fields()` (point_of_care, room, bed, facility, status, type, building, floor, description, comprehensive_id, assigning_authority) |
| PV1.4 | Admission Type | IS | O | 2 | 1 | Admission type (Table 0007) |
| PV1.5 | Preadmit Number | CX | O | 250 | 1 | Pre-admission identifier |
| PV1.6 | Prior Patient Location | PL | O | 80 | 1 | Previous location |
| PV1.7 | Attending Doctor | XCN | O | 250 | * | Attending physician ID (comp 1), family name (comp 2), given name (comp 3) (Table 0010) |
| PV1.8 | Referring Doctor | XCN | O | 250 | * | Referring physician (Table 0010) |
| PV1.9 | Consulting Doctor | XCN | B | 250 | * | Deprecated — use ROL segment |
| PV1.10 | Hospital Service | IS | O | 3 | 1 | Service type (Table 0069) |
| PV1.11 | Temporary Location | PL | O | 80 | 1 | Temporary location |
| PV1.12 | Preadmit Test Indicator | IS | O | 2 | 1 | Pre-admission testing (Table 0087) |
| PV1.13 | Re-admission Indicator | IS | O | 2 | 1 | Re-admission flag (Table 0092) |
| PV1.14 | Admit Source | IS | O | 6 | 1 | Source of admission (Table 0023) |
| PV1.15 | Ambulatory Status | IS | O | 2 | * | Ambulatory status (Table 0009) |
| PV1.16 | VIP Indicator | IS | O | 2 | 1 | VIP status (Table 0099) |
| PV1.17 | Admitting Doctor | XCN | O | 250 | * | Admitting physician (Table 0010) |
| PV1.18 | Patient Type | IS | O | 2 | 1 | Patient type (Table 0018) |
| PV1.19 | Visit Number | CX | O | 250 | 1 | Encounter/visit identifier (comp 1 = ID) |
| PV1.20 | Financial Class | FC | O | 50 | * | Financial class |
| PV1.21 | Charge Price Indicator | IS | O | 2 | 1 | Charge price indicator (Table 0032) |
| PV1.22 | Courtesy Code | IS | O | 2 | 1 | Courtesy code (Table 0045) |
| PV1.23 | Credit Rating | IS | O | 2 | 1 | Credit rating (Table 0046) |
| PV1.24 | Contract Code | IS | O | 2 | * | Contract code (Table 0044) |
| PV1.25 | Contract Effective Date | DT | O | 8 | * | Contract effective date |
| PV1.26 | Contract Amount | NM | O | 12 | * | Contract amount |
| PV1.27 | Contract Period | NM | O | 3 | * | Contract period in days |
| PV1.28 | Interest Code | IS | O | 2 | 1 | Interest code (Table 0073) |
| PV1.29 | Transfer to Bad Debt Code | IS | O | 4 | 1 | Bad debt code (Table 0110) |
| PV1.30 | Transfer to Bad Debt Date | DT | O | 8 | 1 | Date of bad debt transfer |
| PV1.31 | Bad Debt Agency Code | IS | O | 10 | 1 | Collection agency (Table 0021) |
| PV1.32 | Bad Debt Transfer Amount | NM | O | 12 | 1 | Amount transferred |
| PV1.33 | Bad Debt Recovery Amount | NM | O | 12 | 1 | Amount recovered |
| PV1.34 | Delete Account Indicator | IS | O | 1 | 1 | Account deletion flag (Table 0111) |
| PV1.35 | Delete Account Date | DT | O | 8 | 1 | Account deletion date |
| PV1.36 | Discharge Disposition | IS | O | 3 | 1 | Discharge disposition (Table 0112) |
| PV1.37 | Discharged to Location | DLD | O | 47 | 1 | Discharge location |
| PV1.38 | Diet Type | CE | O | 250 | 1 | Diet type (Table 0114) |
| PV1.39 | Servicing Facility | IS | O | 2 | 1 | Servicing facility (Table 0115) |
| PV1.40 | Bed Status | IS | B | 1 | 1 | Deprecated |
| PV1.41 | Account Status | IS | O | 2 | 1 | Account status (Table 0117) |
| PV1.42 | Pending Location | PL | O | 80 | 1 | Pending transfer location |
| PV1.43 | Prior Temporary Location | PL | O | 80 | 1 | Previous temporary location |
| PV1.44 | Admit Date/Time | TS | O | 26 | 1 | Admission datetime |
| PV1.45 | Discharge Date/Time | TS | O | 26 | * | Discharge datetime |
| PV1.46 | Current Patient Balance | NM | O | 12 | 1 | Current balance |
| PV1.47 | Total Charges | NM | O | 12 | 1 | Total charges |
| PV1.48 | Total Adjustments | NM | O | 12 | 1 | Total adjustments |
| PV1.49 | Total Payments | NM | O | 12 | 1 | Total payments |
| PV1.50 | Alternate Visit ID | CX | O | 250 | 1 | Alternate visit identifier |
| PV1.51 | Visit Indicator | IS | O | 1 | 1 | Visit indicator (Table 0326) |
| PV1.52 | Other Healthcare Provider | XCN | B | 250 | * | Deprecated — use ROL segment |
| PV1.53 | Service Episode Description | ST | O | 199 | 1 | (added in v2.8) Free-text description of the service episode. Spark column: `service_episode_description` |
| PV1.54 | Service Episode Identifier | CX | O | 250 | 1 | (added in v2.9) Service episode identifier. CX expanded into 16 flat columns: `service_episode_identifier`, `service_episode_identifier_check_digit`, `service_episode_identifier_check_digit_scheme`, `service_episode_identifier_assigning_authority*` (HD — 3 cols), `service_episode_identifier_type_code`, `service_episode_identifier_assigning_facility*` (HD — 3 cols), `service_episode_identifier_effective_date`, `service_episode_identifier_expiration_date`, `service_episode_identifier_assigning_jurisdiction`, `service_episode_identifier_assigning_agency`, `service_episode_identifier_security_check`, `service_episode_identifier_security_check_scheme` |

### PV2 — Patient Visit Additional Information (50 fields)

Source: [Caristix HL7v2.5.1 PV2](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/PV2)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| PV2.1 | Prior Pending Location | PL | C | 80 | 1 | Prior pending location |
| PV2.2 | Accommodation Code | CE | O | 250 | 1 | Accommodation code (Table 0129) |
| PV2.3 | Admit Reason | CE | O | 250 | 1 | Reason for admission |
| PV2.4 | Transfer Reason | CE | O | 250 | 1 | Reason for transfer |
| PV2.5 | Patient Valuables | ST | O | 25 | * | Patient's valuable items |
| PV2.6 | Patient Valuables Location | ST | O | 25 | 1 | Location of patient valuables |
| PV2.7 | Visit User Code | IS | O | 2 | * | Visit user code (Table 0130) |
| PV2.8 | Expected Admit Date/Time | TS | O | 26 | 1 | Expected admission datetime |
| PV2.9 | Expected Discharge Date/Time | TS | O | 26 | 1 | Expected discharge datetime |
| PV2.10 | Estimated Length of Inpatient Stay | NM | O | 3 | 1 | Estimated stay in days |
| PV2.11 | Actual Length of Inpatient Stay | NM | O | 3 | 1 | Actual stay in days |
| PV2.12 | Visit Description | ST | O | 50 | 1 | Visit description |
| PV2.13 | Referral Source Code | XCN | O | 250 | * | Referral source |
| PV2.14 | Previous Service Date | DT | O | 8 | 1 | Previous service date |
| PV2.15 | Employment Illness Related Indicator | ID | O | 1 | 1 | Y/N employment illness (Table 0136) |
| PV2.16 | Purge Status Code | IS | O | 1 | 1 | Purge status (Table 0213) |
| PV2.17 | Purge Status Date | DT | O | 8 | 1 | Purge status date |
| PV2.18 | Special Program Code | IS | O | 2 | 1 | Special program (Table 0214) |
| PV2.19 | Retention Indicator | ID | O | 1 | 1 | Y/N retention (Table 0136) |
| PV2.20 | Expected Number of Insurance Plans | NM | O | 1 | 1 | Number of insurance plans |
| PV2.21 | Visit Publicity Code | IS | O | 1 | 1 | Visit publicity (Table 0215) |
| PV2.22 | Visit Protection Indicator | ID | O | 1 | 1 | Y/N visit protection (Table 0136) |
| PV2.23 | Clinic Organization Name | XON | O | 250 | * | Clinic organization |
| PV2.24 | Patient Status Code | IS | O | 2 | 1 | Patient status (Table 0216) |
| PV2.25 | Visit Priority Code | IS | O | 1 | 1 | Visit priority (Table 0217) |
| PV2.26 | Previous Treatment Date | DT | O | 8 | 1 | Previous treatment date |
| PV2.27 | Expected Discharge Disposition | IS | O | 2 | 1 | Expected discharge disposition (Table 0112) |
| PV2.28 | Signature on File Date | DT | O | 8 | 1 | Signature on file date |
| PV2.29 | First Similar Illness Date | DT | O | 8 | 1 | First similar illness date |
| PV2.30 | Patient Charge Adjustment Code | CE | O | 250 | 1 | Patient charge adjustment (Table 0218) |
| PV2.31 | Recurring Service Code | IS | O | 2 | 1 | Recurring service (Table 0219) |
| PV2.32 | Billing Media Code | ID | O | 1 | 1 | Y/N billing media (Table 0136) |
| PV2.33 | Expected Surgery Date and Time | TS | O | 26 | 1 | Expected surgery datetime |
| PV2.34 | Military Partnership Code | ID | O | 1 | 1 | Y/N military partnership (Table 0136) |
| PV2.35 | Military Non-Availability Code | ID | O | 1 | 1 | Y/N military non-availability (Table 0136) |
| PV2.36 | Newborn Baby Indicator | ID | O | 1 | 1 | Y/N newborn (Table 0136) |
| PV2.37 | Baby Detained Indicator | ID | O | 1 | 1 | Y/N baby detained (Table 0136) |
| PV2.38 | Mode of Arrival Code | CE | O | 250 | 1 | Mode of arrival (Table 0430) |
| PV2.39 | Recreational Drug Use Code | CE | O | 250 | * | Recreational drug use (Table 0431) |
| PV2.40 | Admission Level of Care Code | CE | O | 250 | 1 | Admission level of care (Table 0432) |
| PV2.41 | Precaution Code | CE | O | 250 | * | Precaution code (Table 0433) |
| PV2.42 | Patient Condition Code | CE | O | 250 | 1 | Patient condition (Table 0434) |
| PV2.43 | Living Will Code | IS | O | 2 | 1 | Living will (Table 0315) |
| PV2.44 | Organ Donor Code | IS | O | 2 | 1 | Organ donor (Table 0316) |
| PV2.45 | Advance Directive Code | CE | O | 250 | * | Advance directive (Table 0435) |
| PV2.46 | Patient Status Effective Date | DT | O | 8 | 1 | Patient status effective date |
| PV2.47 | Expected LOA Return Date/Time | TS | O | 26 | 1 | Expected leave of absence return |
| PV2.48 | Expected Pre-admission Testing Date/Time | TS | C | 26 | 1 | Expected pre-admission testing datetime |
| PV2.49 | Notify Clergy Code | CWE | O | 705 | * | Notify clergy code (Table 0534). Stored as `ARRAY<STRUCT<...9 CWE fields...>>` |
| PV2.50 | Advance Directive Last Verified Date | DT | O | 8 | 1 | (added in v2.9) Date advance directive was last verified. Spark column: `advance_directive_last_verified_date` (TimestampType) |

### NK1 — Next of Kin / Associated Parties (41 fields)

Source: [Caristix HL7v2.5.1 NK1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/NK1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| NK1.1 | Set ID - NK1 | SI | R | 4 | 1 | Sequence number |
| NK1.2 | NK Name | XPN | O | 250 | * | Contact name (comp 1 = family, comp 2 = given) |
| NK1.3 | Relationship | CE | O | 250 | 1 | Relationship to patient (comp 1 = code, comp 2 = text) (Table 0063) |
| NK1.4 | Address | XAD | O | 250 | * | Contact address |
| NK1.5 | Phone Number | XTN | O | 250 | * | Home phone number |
| NK1.6 | Business Phone Number | XTN | O | 250 | * | Work phone number |
| NK1.7 | Contact Role | CE | O | 250 | 1 | Contact role (Table 0131) |
| NK1.8 | Start Date | DT | O | 8 | 1 | Relationship start date |
| NK1.9 | End Date | DT | O | 8 | 1 | Relationship end date |
| NK1.10 | Job Title | ST | O | 60 | 1 | Job title |
| NK1.11 | Job Code/Class | JCC | O | 20 | 1 | Job code/class |
| NK1.12 | Employee Number | CX | O | 250 | 1 | Employee number |
| NK1.13 | Organization Name | XON | O | 250 | * | Organization name |
| NK1.14 | Marital Status | CE | O | 250 | 1 | Marital status (Table 0002) |
| NK1.15 | Administrative Sex | IS | O | 1 | 1 | Sex (Table 0001) |
| NK1.16 | Date/Time of Birth | TS | O | 26 | 1 | Date of birth |
| NK1.17 | Living Dependency | IS | O | 2 | * | Living dependency (Table 0223) |
| NK1.18 | Ambulatory Status | IS | O | 2 | * | Ambulatory status (Table 0009) |
| NK1.19 | Citizenship | CE | O | 250 | * | Citizenship (Table 0171) |
| NK1.20 | Primary Language | CE | O | 250 | 1 | Primary language (Table 0296) |
| NK1.21 | Living Arrangement | IS | O | 2 | 1 | Living arrangement (Table 0220) |
| NK1.22 | Publicity Code | CE | O | 250 | 1 | Publicity code (Table 0215) |
| NK1.23 | Protection Indicator | ID | O | 1 | 1 | Y/N protection (Table 0136) |
| NK1.24 | Student Indicator | IS | O | 2 | 1 | Student status (Table 0231) |
| NK1.25 | Religion | CE | O | 250 | 1 | Religion (Table 0006) |
| NK1.26 | Mother's Maiden Name | XPN | O | 250 | * | Mother's maiden name |
| NK1.27 | Nationality | CE | O | 250 | 1 | Nationality (Table 0212) |
| NK1.28 | Ethnic Group | CE | O | 250 | * | Ethnic group (Table 0189) |
| NK1.29 | Contact Reason | CE | O | 250 | * | Reason for contact (Table 0222) |
| NK1.30 | Contact Person's Name | XPN | O | 250 | * | Contact person name |
| NK1.31 | Contact Person's Telephone Number | XTN | O | 250 | * | Contact person phone |
| NK1.32 | Contact Person's Address | XAD | O | 250 | * | Contact person address |
| NK1.33 | Identifiers | CX | O | 250 | * | NK identifiers |
| NK1.34 | Job Status | IS | O | 2 | 1 | Job status (Table 0311) |
| NK1.35 | Race | CE | O | 250 | * | Race (Table 0005) |
| NK1.36 | Handicap | IS | O | 2 | 1 | Handicap (Table 0295) |
| NK1.37 | Social Security Number | ST | O | 16 | 1 | SSN. Spark column: `contact_ssn` |
| NK1.38 | Birth Place | ST | O | 250 | 1 | (added in v2.6) Birth place of the next of kin. Spark column: `birth_place` |
| NK1.39 | VIP Indicator | CWE | O | 705 | 1 | VIP indicator (Table 0099). Spark columns: `vip_indicator`, `vip_indicator_text`, `vip_indicator_coding_system`, `vip_indicator_alt_code`, `vip_indicator_alt_text`, `vip_indicator_alt_coding_system`, `vip_indicator_coding_system_version`, `vip_indicator_alt_coding_system_version`, `vip_indicator_original_text` |
| NK1.40 | Telecommunication Information | XTN | O | 2915 | 1 | (added in v2.7) Next of kin telecommunication information. Spark columns: `telecommunication_info_number`, `telecommunication_info_use_code`, `telecommunication_info_equipment_type`, `telecommunication_info_communication_address`, `telecommunication_info_country_code`, `telecommunication_info_area_code`, `telecommunication_info_local_number`, `telecommunication_info_extension`, and 10 more XTN component columns prefixed `telecommunication_info_*` |
| NK1.41 | Contact Person's Telecommunication Information | XTN | O | 2915 | 1 | (added in v2.7) Contact person telecommunication information. Spark columns follow the same XTN expansion as NK1.40 with prefix `contact_telecommunication_info_*` |

### MRG — Merge Patient Information (7 fields)

Source: [Caristix HL7v2.5.1 MRG](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/MRG)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| MRG.1 | Prior Patient Identifier List | CX | R | 250 | * | Prior patient identifiers (comp 1 = ID value) |
| MRG.2 | Prior Alternate Patient ID | CX | B | 250 | * | Deprecated — prior alternate ID |
| MRG.3 | Prior Patient Account Number | CX | O | 250 | 1 | Prior account number |
| MRG.4 | Prior Patient ID | CX | B | 250 | 1 | Deprecated — prior patient ID |
| MRG.5 | Prior Visit Number | CX | O | 250 | 1 | Prior visit number |
| MRG.6 | Prior Alternate Visit ID | CX | O | 250 | 1 | Prior alternate visit ID |
| MRG.7 | Prior Patient Name | XPN | O | 250 | * | Prior patient name (comp 1 = family, comp 2 = given) |

### AL1 — Patient Allergy Information (6 fields)

Source: [Caristix HL7v2.5.1 AL1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/AL1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| AL1.1 | Set ID - AL1 | SI | R | 4 | 1 | Sequence number |
| AL1.2 | Allergen Type Code | CE | O | 250 | 1 | `DA` (drug allergy), `FA` (food allergy), `EA` (environmental allergy), `MA` (miscellaneous allergy), `MC` (miscellaneous contraindication), `LA` (animal allergy), `PA` (plant allergy) (Table 0127) |
| AL1.3 | Allergen Code/Mnemonic/Description | CE | R | 250 | 1 | Allergen identifier (comp 1 = code, comp 2 = text, comp 3 = coding system) |
| AL1.4 | Allergy Severity Code | CE | O | 250 | 1 | `SV` (severe), `MO` (moderate), `MI` (mild), `U` (unknown) (Table 0128) |
| AL1.5 | Allergy Reaction Code | ST | O | 15 | * | Reaction description (e.g. `HIVES`, `RASH`, `ANAPHYLAXIS`) |
| AL1.6 | Identification Date | DT | B | 8 | 1 | Deprecated — date allergy was identified |

### IAM — Patient Adverse Reaction Information (30 fields)

Source: [Caristix HL7v2.5.1 IAM](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/IAM)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| IAM.1 | Set ID - IAM | SI | R | 4 | 1 | Sequence number |
| IAM.2 | Allergen Type Code | CWE | O | 705 | 1 | Allergen type (Table 0127) — same values as AL1.2. Spark columns: `allergen_type_code`, `allergen_type_code_text`, etc. (9 CWE cols) |
| IAM.3 | Allergen Code/Mnemonic/Description | CWE | R | 705 | 1 | Allergen identifier. Spark columns: `allergen_code`, `allergen_code_text`, etc. (9 CWE cols) |
| IAM.4 | Allergy Severity Code | CWE | O | 705 | 1 | Allergy severity (Table 0128). Spark columns: `allergy_severity_code`, `allergy_severity_code_text`, etc. (9 CWE cols) |
| IAM.5 | Allergy Reaction Code | ST | O | 15 | * | Reaction description. Stored as `ARRAY<STRUCT<...9 CWE fields...>>` (modeled leniently as CWE-shape for real-world senders that emit CWE values) |
| IAM.6 | Allergy Action Code | CWE | R | 705 | 1 | Action code: `A` (add), `D` (delete), `U` (update) (Table 0323). Spark columns: `allergy_action_code`, `allergy_action_code_text`, etc. (9 CWE cols) |
| IAM.7 | Allergy Unique Identifier | EI | C | 427 | 1 | Unique allergy identifier. Spark columns: `allergy_unique_identifier`, `allergy_unique_identifier_namespace_id`, `allergy_unique_identifier_universal_id`, `allergy_unique_identifier_universal_id_type` |
| IAM.8 | Action Reason | ST | O | 60 | 1 | Reason for the action. Spark column: `action_reason` |
| IAM.9 | Sensitivity to Causative Agent Code | CWE | O | 705 | 1 | Sensitivity code (Table 0436). Spark columns: `sensitivity_to_causative_agent_code`, etc. (9 CWE cols) |
| IAM.10 | Allergen Group Code/Mnemonic/Description | CWE | O | 705 | 1 | Allergen group. Spark columns: `allergen_group_code`, etc. (9 CWE cols) |
| IAM.11 | Onset Date | DT | O | 8 | 1 | Allergy onset date. Spark column: `onset_date` (TimestampType) |
| IAM.12 | Onset Date Text | ST | O | 60 | 1 | Free-text onset date description |
| IAM.13 | Reported Date/Time | TS | O | 8 | 1 | When the allergy was reported |
| IAM.14 | Reported By | XCN | O | 250 | 1 | Person who reported. Spark columns: `reported_by_id`, `reported_by_family_name`, `reported_by_given_name`, etc. (23 XCN cols) |
| IAM.15 | Relationship to Patient Code | CWE | O | 705 | 1 | Reporter's relationship (Table 0063). Spark columns: `relationship_to_patient_code`, etc. (9 CWE cols) |
| IAM.16 | Alert Device Code | CWE | O | 705 | 1 | Alert device (Table 0437). Spark columns: `alert_device_code`, etc. (9 CWE cols) |
| IAM.17 | Allergy Clinical Status Code | CWE | O | 705 | 1 | Clinical status (Table 0438). Spark columns: `allergy_clinical_status_code`, etc. (9 CWE cols) |
| IAM.18 | Statused by Person | XCN | O | 250 | 1 | Person who set status. Spark columns: `statused_by_person_id`, `statused_by_person_family_name`, etc. (23 XCN cols) |
| IAM.19 | Statused by Organization | XON | O | 250 | 1 | Organization that set status. Spark columns: `statused_by_organization`, `statused_by_organization_type_code`, etc. (14 XON cols) |
| IAM.20 | Statused at Date/Time | TS | O | 8 | 1 | Status datetime |
| IAM.21 | Inactivated by Person | XCN | O | 250 | 1 | (added in v2.6) Person who inactivated the record. Spark columns: `inactivated_by_person_id`, `inactivated_by_person_family_name`, etc. (23 XCN cols) |
| IAM.22 | Inactivated Date/Time | TS | O | 26 | 1 | (added in v2.6) Date/time the record was inactivated. Spark column: `inactivated_datetime` |
| IAM.23 | Initially Recorded by Person | XCN | O | 250 | 1 | (added in v2.6) Person who initially recorded the reaction. Spark columns: `initially_recorded_by_person_id`, `initially_recorded_by_person_family_name`, etc. (23 XCN cols) |
| IAM.24 | Initially Recorded Date/Time | TS | O | 26 | 1 | (added in v2.6) Date/time the reaction was initially recorded. Spark column: `initially_recorded_datetime` |
| IAM.25 | Modified by Person | XCN | O | 250 | 1 | (added in v2.6) Person who last modified the record. Spark columns: `modified_by_person_id`, `modified_by_person_family_name`, etc. (23 XCN cols) |
| IAM.26 | Modified Date/Time | TS | O | 26 | 1 | (added in v2.6) Date/time the record was last modified. Spark column: `modified_datetime` |
| IAM.27 | Clinician Identified Allergen Code | CWE | O | 705 | 1 | (added in v2.6) Allergen code as identified by the clinician. Spark columns: `clinician_identified_allergen_code`, etc. (9 CWE cols) |
| IAM.28 | Initially Recorded by Organization | XON | O | 250 | 1 | (added in v2.6) Organization that initially recorded the reaction. Spark columns: `initially_recorded_by_organization`, etc. (14 XON cols) |
| IAM.29 | Modified by Organization | XON | O | 250 | 1 | (added in v2.6) Organization that last modified the record. Spark columns: `modified_by_organization`, etc. (14 XON cols) |
| IAM.30 | Inactivated by Organization | XON | O | 250 | 1 | (added in v2.6) Organization that inactivated the record. Spark columns: `inactivated_by_organization`, etc. (14 XON cols) |

### DG1 — Diagnosis (26 fields)

Source: [Caristix HL7v2.5.1 DG1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/DG1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| DG1.1 | Set ID - DG1 | SI | R | 4 | 1 | Sequence number |
| DG1.2 | Diagnosis Coding Method | ID | B | 2 | 1 | Deprecated (Table 0053) |
| DG1.3 | Diagnosis Code - DG1 | CE | O | 250 | 1 | ICD-10 or other diagnosis code (comp 1 = code, comp 2 = text, comp 3 = coding system) (Table 0051) |
| DG1.4 | Diagnosis Description | ST | B | 40 | 1 | Deprecated — use DG1.3 comp 2 |
| DG1.5 | Diagnosis Date/Time | TS | O | 26 | 1 | When diagnosis was recorded |
| DG1.6 | Diagnosis Type | IS | R | 2 | 1 | `A` (admitting), `W` (working), `F` (final) (Table 0052) |
| DG1.7 | Major Diagnostic Category | CE | B | 250 | 1 | Deprecated (Table 0118) |
| DG1.8 | Diagnostic Related Group | CE | B | 250 | 1 | Deprecated (Table 0055) |
| DG1.9 | DRG Approval Indicator | ID | B | 1 | 1 | Deprecated (Table 0136) |
| DG1.10 | DRG Grouper Review Code | IS | B | 2 | 1 | Deprecated (Table 0056) |
| DG1.11 | Outlier Type | CE | B | 250 | 1 | Deprecated (Table 0083) |
| DG1.12 | Outlier Days | NM | B | 3 | 1 | Deprecated |
| DG1.13 | Outlier Cost | CP | B | 12 | 1 | Deprecated |
| DG1.14 | Grouper Version And Type | ST | B | 4 | 1 | Deprecated |
| DG1.15 | Diagnosis Priority | ID | O | 2 | 1 | Priority code (Table 0359) |
| DG1.16 | Diagnosing Clinician | XCN | O | 250 | * | Diagnosing physician |
| DG1.17 | Diagnosis Classification | IS | O | 3 | 1 | Classification (Table 0228) |
| DG1.18 | Confidential Indicator | ID | O | 1 | 1 | Y/N confidentiality (Table 0136) |
| DG1.19 | Attestation Date/Time | TS | O | 26 | 1 | Attestation datetime |
| DG1.20 | Diagnosis Identifier | EI | C | 427 | 1 | Unique diagnosis identifier. Spark columns: `diagnosis_identifier`, `diagnosis_identifier_namespace_id`, `diagnosis_identifier_universal_id`, `diagnosis_identifier_universal_id_type` |
| DG1.21 | Diagnosis Action Code | ID | C | 1 | 1 | `A` (add), `D` (delete), `U` (update) (Table 0206). Spark column: `diagnosis_action_code` |
| DG1.22 | Parent Diagnosis | EI | C | 427 | 1 | (added in v2.7) Parent diagnosis instance identifier. Spark columns: `parent_diagnosis`, `parent_diagnosis_namespace_id`, `parent_diagnosis_universal_id`, `parent_diagnosis_universal_id_type` |
| DG1.23 | DRG CCL Value Code | CWE | O | 705 | 1 | (added in v2.7) DRG complication/comorbidity level. Spark columns: `drg_ccl_value_code`, `drg_ccl_value_code_text`, `drg_ccl_value_code_coding_system`, `drg_ccl_value_code_alt_code`, `drg_ccl_value_code_alt_text`, `drg_ccl_value_code_alt_coding_system`, `drg_ccl_value_code_coding_system_version`, `drg_ccl_value_code_alt_coding_system_version`, `drg_ccl_value_code_original_text` |
| DG1.24 | DRG Grouping Usage | ID | O | 20 | 1 | (added in v2.7) Whether this diagnosis was used in DRG grouping: `Y` or `N`. Spark column: `drg_grouping_usage` |
| DG1.25 | DRG Diagnosis Determination Status | CWE | O | 705 | 1 | (added in v2.7) DRG diagnosis determination status. Spark columns: `drg_diagnosis_determination_status`, `drg_diagnosis_determination_status_text`, `drg_diagnosis_determination_status_coding_system`, `drg_diagnosis_determination_status_alt_code`, `drg_diagnosis_determination_status_alt_text`, `drg_diagnosis_determination_status_alt_coding_system`, `drg_diagnosis_determination_status_coding_system_version`, `drg_diagnosis_determination_status_alt_coding_system_version`, `drg_diagnosis_determination_status_original_text` |
| DG1.26 | Present on Admission Indicator | CWE | O | 705 | 1 | (added in v2.7) Present-on-admission indicator: `Y` (yes), `N` (no), `U` (unknown), `W` (clinically undetermined). Spark columns: `present_on_admission_indicator`, `present_on_admission_indicator_text`, `present_on_admission_indicator_coding_system`, `present_on_admission_indicator_alt_code`, `present_on_admission_indicator_alt_text`, `present_on_admission_indicator_alt_coding_system`, `present_on_admission_indicator_coding_system_version`, `present_on_admission_indicator_alt_coding_system_version`, `present_on_admission_indicator_original_text` |

### PR1 — Procedures (25 fields)

Source: [Caristix HL7v2.5.1 PR1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/PR1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| PR1.1 | Set ID - PR1 | SI | R | 4 | 1 | Sequence number |
| PR1.2 | Procedure Coding Method | IS | B | 3 | 1 | Deprecated (Table 0089) |
| PR1.3 | Procedure Code | CE | R | 250 | 1 | CPT/ICD procedure code (comp 1 = code, comp 2 = text, comp 3 = coding system) (Table 0088) |
| PR1.4 | Procedure Description | ST | B | 40 | 1 | Deprecated — use PR1.3 comp 2 |
| PR1.5 | Procedure Date/Time | TS | R | 26 | 1 | When the procedure was performed |
| PR1.6 | Procedure Functional Type | IS | O | 2 | 1 | Functional type (Table 0230) |
| PR1.7 | Procedure Minutes | NM | O | 4 | 1 | Duration in minutes |
| PR1.8 | Anesthesiologist | XCN | B | 250 | * | Deprecated — anesthesiologist (Table 0010) |
| PR1.9 | Anesthesia Code | IS | O | 2 | 1 | Anesthesia code (Table 0019) |
| PR1.10 | Anesthesia Minutes | NM | O | 4 | 1 | Anesthesia duration in minutes |
| PR1.11 | Surgeon | XCN | B | 250 | * | Deprecated — surgeon (Table 0010) |
| PR1.12 | Procedure Practitioner | XCN | B | 250 | * | Deprecated — practitioner (Table 0010) |
| PR1.13 | Consent Code | CE | O | 250 | 1 | Consent code (Table 0059) |
| PR1.14 | Procedure Priority | ID | O | 2 | 1 | Priority (Table 0418) |
| PR1.15 | Associated Diagnosis Code | CE | O | 250 | 1 | Associated diagnosis (Table 0051) |
| PR1.16 | Procedure Code Modifier | CE | O | 250 | * | Procedure modifier (Table 0340) |
| PR1.17 | Procedure DRG Type | IS | O | 20 | 1 | DRG type (Table 0416) |
| PR1.18 | Tissue Type Code | CE | O | 250 | * | Tissue type (Table 0417) |
| PR1.19 | Procedure Identifier | EI | C | 427 | 1 | Unique procedure identifier |
| PR1.20 | Procedure Action Code | ID | C | 1 | 1 | `A` (add), `D` (delete), `U` (update) (Table 0206). Spark column: `procedure_action_code` |
| PR1.21 | DRG Procedure Determination Status | CWE | O | 705 | 1 | (added in v2.6) DRG procedure determination status. Spark columns: `drg_procedure_determination_status`, `drg_procedure_determination_status_text`, etc. (9 CWE cols) |
| PR1.22 | DRG Procedure Relevance | CWE | O | 705 | 1 | (added in v2.6) DRG procedure relevance. Spark columns: `drg_procedure_relevance`, `drg_procedure_relevance_text`, etc. (9 CWE cols) |
| PR1.23 | Treating Organizational Unit | PL | O | 80 | * | (added in v2.7) Treating organizational unit — repeating PL. Stored as `ARRAY<STRUCT<...11 PL fields...>>`. Spark column array: `treating_organizational_unit` |
| PR1.24 | Respiratory Within Surgery | ID | O | 1 | 1 | (added in v2.7) Respiratory within surgery indicator. Spark column: `respiratory_within_surgery` |
| PR1.25 | Parent Procedure ID | EI | C | 427 | 1 | (added in v2.7) Parent procedure instance identifier. Spark columns: `parent_procedure_id`, `parent_procedure_id_namespace_id`, `parent_procedure_id_universal_id`, `parent_procedure_id_universal_id_type` |

### ORC — Common Order (38 fields)

Source: [Caristix HL7v2.5.1 ORC](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/ORC)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| ORC.1 | Order Control | ID | R | 2 | 1 | Order control code: `NW` (new), `CA` (cancel), `DC` (discontinue), `XO` (change), `HD` (hold), `RL` (release hold), `SC` (status changed), `RE` (observations) (Table 0119) |
| ORC.2 | Placer Order Number | EI | C | 22 | 1 | Order number from placer (comp 1 = entity ID) |
| ORC.3 | Filler Order Number | EI | C | 22 | 1 | Order number from filler (comp 1 = entity ID) |
| ORC.4 | Placer Group Number | EI | O | 22 | 1 | Placer group number |
| ORC.5 | Order Status | ID | O | 2 | 1 | Order status (Table 0038) |
| ORC.6 | Response Flag | ID | O | 1 | 1 | Response flag (Table 0121) |
| ORC.7 | Quantity/Timing | TQ | B | 200 | * | Deprecated in v2.5 — use TQ1/TQ2. TQ composite stored as `ARRAY<STRUCT<...TQ fields...>>`. Spark column: `quantity_timing` |
| ORC.8 | Parent Order | EIP | O | 200 | * | Parent order reference. EIP stored as `ARRAY<STRUCT<placer_assigned_identifier EI, filler_assigned_identifier EI>>`. Spark column: `parent_order` |
| ORC.9 | Date/Time of Transaction | TS | O | 26 | 1 | Transaction datetime |
| ORC.10 | Entered By | XCN | O | 250 | * | Person who entered the order |
| ORC.11 | Verified By | XCN | O | 250 | * | Person who verified the order |
| ORC.12 | Ordering Provider | XCN | O | 250 | * | Ordering clinician (comp 1 = ID, comp 2 = family, comp 3 = given) |
| ORC.13 | Enterer's Location | PL | O | 80 | 1 | Location where order was entered |
| ORC.14 | Call Back Phone Number | XTN | O | 250 | 2 | Callback phone |
| ORC.15 | Order Effective Date/Time | TS | O | 26 | 1 | Order effective datetime |
| ORC.16 | Order Control Code Reason | CE | O | 250 | 1 | Reason for the order control action |
| ORC.17 | Entering Organization | CE | O | 250 | 1 | Organization that entered the order |
| ORC.18 | Entering Device | CE | O | 250 | 1 | Device used to enter the order |
| ORC.19 | Action By | XCN | O | 250 | * | Person who actioned the order |
| ORC.20 | Advanced Beneficiary Notice Code | CE | O | 250 | 1 | ABN code (Table 0339) |
| ORC.21 | Ordering Facility Name | XON | O | 250 | * | Ordering facility name |
| ORC.22 | Ordering Facility Address | XAD | O | 250 | * | Ordering facility address |
| ORC.23 | Ordering Facility Phone Number | XTN | O | 250 | * | Ordering facility phone |
| ORC.24 | Ordering Provider Address | XAD | O | 250 | * | Ordering provider address |
| ORC.25 | Order Status Modifier | CWE | O | 250 | 1 | Order status modifier |
| ORC.26 | Advanced Beneficiary Notice Override Reason | CWE | C | 60 | 1 | ABN override reason (Table 0552) |
| ORC.27 | Filler's Expected Availability Date/Time | TS | O | 26 | 1 | Expected availability datetime |
| ORC.28 | Confidentiality Code | CWE | O | 250 | 1 | Confidentiality code (Table 0177) |
| ORC.29 | Order Type | CWE | O | 250 | 1 | Order type (Table 0482) |
| ORC.30 | Enterer Authorization Mode | CNE | O | 250 | 1 | Authorization mode (Table 0483) |
| ORC.31 | Parent Universal Service Identifier | CWE | O | 250 | 1 | Parent service identifier. Spark columns: `parent_universal_service_id`, `parent_universal_service_id_text`, etc. (9 CWE cols) |
| ORC.32 | Advanced Beneficiary Notice Date | DT | O | 8 | 1 | (added in v2.6) Advanced beneficiary notice date. Spark column: `advanced_beneficiary_notice_date` (TimestampType) |
| ORC.33 | Alternate Placer Order Number | CX | O | 250 | * | (added in v2.6) Alternate placer order number. Stored as `ARRAY<STRUCT<...16 CX fields...>>`. Spark column: `alternate_placer_order_number` |
| ORC.34 | Order Workflow Profile | CWE | O | 705 | * | (added in v2.9) Order workflow profile. Stored as `ARRAY<STRUCT<...9 CWE fields...>>`. Spark column: `order_workflow_profile` |
| ORC.35 | Action Code | ID | O | 2 | 1 | (added in v2.9) Order action code (Table 0206): `A` (add), `D` (delete), `U` (update), `X` (no change. Spark column: `action_code` |
| ORC.36 | Order Status Date Range | DR | O | 52 | 1 | (added in v2.9) Order status date range. DR type stored as two columns: `order_status_date_range_start` (DR.1 — TimestampType), `order_status_date_range_end` (DR.2 — TimestampType) |
| ORC.37 | Order Creation Date/Time | DTM | O | 24 | 1 | (added in v2.9) Date/time the order was created. Spark column: `order_creation_datetime` (TimestampType) |
| ORC.38 | Filler Order Group Number | EI | O | 427 | 1 | (added in v2.9) Filler order group number. Spark columns: `filler_order_group_number`, `filler_order_group_number_namespace_id`, `filler_order_group_number_universal_id`, `filler_order_group_number_universal_id_type` |

### OBR — Observation Request (55 fields)

Source: [Caristix HL7v2.5.1 OBR](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/OBR)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| OBR.1 | Set ID - OBR | SI | O | 4 | 1 | Sequence number |
| OBR.2 | Placer Order Number | EI | C | 22 | 1 | Order number assigned by placer |
| OBR.3 | Filler Order Number | EI | C | 22 | 1 | Order number assigned by filler |
| OBR.4 | Universal Service Identifier | CE | R | 250 | 1 | Ordered test (comp 1 = code, comp 2 = text, comp 3 = coding system) |
| OBR.5 | Priority - OBR | ID | B | 2 | 1 | Deprecated |
| OBR.6 | Requested Date/Time | TS | B | 26 | 1 | Deprecated |
| OBR.7 | Observation Date/Time | TS | C | 26 | 1 | Specimen collection / observation datetime |
| OBR.8 | Observation End Date/Time | TS | O | 26 | 1 | End of observation period |
| OBR.9 | Collection Volume | CQ | O | 20 | 1 | Specimen volume |
| OBR.10 | Collector Identifier | XCN | O | 250 | * | Specimen collector |
| OBR.11 | Specimen Action Code | ID | O | 1 | 1 | Action code |
| OBR.12 | Danger Code | CE | O | 250 | 1 | Danger/hazard code |
| OBR.13 | Relevant Clinical Information | ST | O | 300 | 1 | Clinical info for interpretation |
| OBR.14 | Specimen Received Date/Time | TS | B | 26 | 1 | Deprecated |
| OBR.15 | Specimen Source | SPS | B | 300 | 1 | Deprecated in v2.7 — use SPM segment for v2.7+. SPS composite is retained for backward-compatible parsing. Expanded into 8 flat columns: `specimen_source` (SPS.1.1 CWE.1 — source code), `specimen_source_text` (SPS.1.2), `specimen_source_additives` (SPS.2.1), `specimen_source_collection_method` (SPS.3 TX), `specimen_source_body_site` (SPS.4.1), `specimen_source_site_modifier` (SPS.5.1), `specimen_source_collection_method_modifier` (SPS.6.1), `specimen_source_role` (SPS.7.1) |
| OBR.16 | Ordering Provider | XCN | O | 250 | * | Ordering clinician (comp 1 = ID, comp 2 = family name, comp 3 = given name) |
| OBR.17 | Order Callback Phone Number | XTN | O | 250 | 2 | Callback phone |
| OBR.18 | Placer Field 1 | ST | O | 60 | 1 | Placer-defined field |
| OBR.19 | Placer Field 2 | ST | O | 60 | 1 | Placer-defined field |
| OBR.20 | Filler Field 1 | ST | O | 60 | 1 | Filler-defined field |
| OBR.21 | Filler Field 2 | ST | O | 60 | 1 | Filler-defined field |
| OBR.22 | Results Rpt/Status Chng - Date/Time | TS | C | 26 | 1 | Result report datetime |
| OBR.23 | Charge to Practice | MOC | O | 40 | 1 | Charge information. MOC expanded into 5 flat columns: `charge_to_practice_monetary_amount` (MOC.1.1 MO.1 — quantity NM), `charge_to_practice_monetary_amount_currency` (MOC.1.2 MO.2 — ISO 4217 ID), `charge_to_practice_charge_code` (MOC.2.1 CWE.1), `charge_to_practice_charge_code_text` (MOC.2.2 CWE.2), `charge_to_practice_charge_code_coding_system` (MOC.2.3 CWE.3) |
| OBR.24 | Diagnostic Serv Sect ID | ID | O | 10 | 1 | Diagnostic service section |
| OBR.25 | Result Status | ID | C | 1 | 1 | `F` (final), `P` (preliminary), `C` (correction), `X` (cancelled), `I` (pending), `S` (partial), `R` (results entered), `O` (order received) |
| OBR.26 | Parent Result | PRL | O | 400 | 1 | Parent result link |
| OBR.27 | Quantity/Timing | TQ | B | 200 | * | Deprecated in v2.5 — use TQ1/TQ2. TQ composite stored as `ARRAY<STRUCT<...TQ fields...>>`. Spark column: `quantity_timing` |
| OBR.28 | Result Copies To | XCN | O | 250 | * | Copy-to recipients. Stored as `ARRAY<STRUCT<...23 XCN fields...>>` |
| OBR.29 | Parent | EIP | O | 200 | 1 | Parent results observation identifier (links a child result to the parent observation). EIP expanded into 8 flat columns: `parent_results_observation_identifier_placer_assigned_identifier` (EIP.1.1), `parent_results_observation_identifier_placer_assigned_identifier_namespace_id` (EIP.1.2), `parent_results_observation_identifier_placer_assigned_identifier_universal_id` (EIP.1.3), `parent_results_observation_identifier_placer_assigned_identifier_universal_id_type` (EIP.1.4), `parent_results_observation_identifier_filler_assigned_identifier` (EIP.2.1), `parent_results_observation_identifier_filler_assigned_identifier_namespace_id` (EIP.2.2), `parent_results_observation_identifier_filler_assigned_identifier_universal_id` (EIP.2.3), `parent_results_observation_identifier_filler_assigned_identifier_universal_id_type` (EIP.2.4) |
| OBR.30 | Transportation Mode | ID | O | 20 | 1 | Specimen transport mode |
| OBR.31 | Reason for Study | CE | O | 250 | * | Reason for study |
| OBR.32 | Principal Result Interpreter | NDL | O | 200 | 1 | Principal interpreter |
| OBR.33 | Assistant Result Interpreter | NDL | O | 200 | * | Assistant interpreter |
| OBR.34 | Technician | NDL | O | 200 | * | Technician |
| OBR.35 | Transcriptionist | NDL | O | 200 | * | Transcriptionist |
| OBR.36 | Scheduled Date/Time | TS | O | 26 | 1 | Scheduled datetime |
| OBR.37 | Number of Sample Containers | NM | O | 4 | 1 | Container count |
| OBR.38 | Transport Logistics of Collected Sample | CE | O | 250 | * | Transport logistics |
| OBR.39 | Collector's Comment | CE | O | 250 | * | Collector comments |
| OBR.40 | Transport Arrangement Responsibility | CE | O | 250 | 1 | Transport responsibility |
| OBR.41 | Transport Arranged | ID | O | 30 | 1 | Transport status |
| OBR.42 | Escort Required | ID | O | 1 | 1 | Escort required flag |
| OBR.43 | Planned Patient Transport Comment | CE | O | 250 | * | Transport comments |
| OBR.44 | Procedure Code | CE | O | 250 | 1 | Procedure code |
| OBR.45 | Procedure Code Modifier | CE | O | 250 | * | Procedure modifier |
| OBR.46 | Placer Supplemental Service Information | CE | O | 250 | * | Placer supplemental info |
| OBR.47 | Filler Supplemental Service Information | CE | O | 250 | * | Filler supplemental info |
| OBR.48 | Medically Necessary Duplicate Procedure Reason | CWE | C | 250 | 1 | Duplicate procedure reason |
| OBR.49 | Result Handling | IS | O | 2 | 1 | Result handling instructions |
| OBR.50 | Parent Universal Service Identifier | CWE | O | 250 | 1 | Parent service identifier. Spark columns: `parent_universal_service_id`, `parent_universal_service_id_text`, `parent_universal_service_id_coding_system`, `parent_universal_service_id_alt_code`, `parent_universal_service_id_alt_text`, `parent_universal_service_id_alt_coding_system`, `parent_universal_service_id_coding_system_version`, `parent_universal_service_id_alt_coding_system_version`, `parent_universal_service_id_original_text` |
| OBR.51 | Observation Group Identifier | EI | O | 427 | 1 | (added in v2.8) Groups related OBX observations under this OBR. Spark columns: `observation_group`, `observation_group_namespace_id`, `observation_group_universal_id`, `observation_group_universal_id_type` |
| OBR.52 | Parent Observation Group Identifier | EI | O | 427 | 1 | (added in v2.8) Observation group for the parent OBR. Spark columns: `parent_observation_group`, `parent_observation_group_namespace_id`, `parent_observation_group_universal_id`, `parent_observation_group_universal_id_type` |
| OBR.53 | Alternate Placer Order Number | CX | O | 250 | * | (added in v2.8) Alternate placer order numbers. Stored as `ARRAY<STRUCT<...16 CX fields...>>`. Spark column: `alternate_placer_order` |
| OBR.54 | Parent Order | EIP | O | 427 | * | (added in v2.9) Parent order identifiers (repeating EIP). Stored as `ARRAY<STRUCT<placer_assigned_identifier EI, filler_assigned_identifier EI>>`. Spark column: `parent_order` |
| OBR.55 | Action Code | ID | O | 2 | 1 | (added in v2.9) Action code for this observation request (Table 0206): `A` (add), `D` (delete), `U` (update), `X` (no change). Spark column: `action_code` |

### OBX — Observation/Result (33 fields)

Source: [Caristix HL7v2.5.1 OBX](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/OBX)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| OBX.1 | Set ID - OBX | SI | O | 4 | 1 | Sequence number |
| OBX.2 | Value Type | ID | C | 2 | 1 | `NM` (numeric), `ST` (string), `TX` (text), `CWE` (coded), `SN` (structured numeric), `FT` (formatted text), `ED` (encapsulated data) (Table 0125) |
| OBX.3 | Observation Identifier | CE | R | 250 | 1 | LOINC or local code (comp 1 = code, comp 2 = display text, comp 3 = coding system) |
| OBX.4 | Observation Sub-ID | OG | C | 20 | 1 | Sub-identifier for related observations. Upgraded to OG type in v2.8.2+ (OG.1 = original ST sub-ID is backward-compatible). Expanded into 4 flat columns: `observation_sub_id` (OG.1 — original sub-identifier ST, backward-compatible with legacy ST values), `observation_sub_id_group` (OG.2 — group NM), `observation_sub_id_sequence` (OG.3 — sequence NM), `observation_sub_id_identifier` (OG.4 — identifier ST) |
| OBX.5 | Observation Value | VARIES | C | 65536 | * | Result value — type determined by OBX.2 |
| OBX.6 | Units | CE | O | 250 | 1 | Units of measure (comp 1 = code, comp 2 = text, comp 3 = coding system) |
| OBX.7 | References Range | ST | O | 60 | 1 | Normal range (e.g. `3.5-5.0`, `>=18`) |
| OBX.8 | Interpretation Codes | CWE | O | 705 | * | Renamed from "Abnormal Flags" (IS) in v2.7+; same field position. Values: `N` (normal), `H` (high), `L` (low), `HH` (critical high), `LL` (critical low), `A` (abnormal) (Table 0078). Code column: `interpretation_codes` (`ARRAY<STRUCT>` — one CWE struct per repetition) |
| OBX.9 | Probability | NM | O | 5 | 1 | Probability (0–1) |
| OBX.10 | Nature of Abnormal Test | ID | O | 2 | * | Nature of abnormal test (Table 0080). Code column: `nature_of_abnormal_test` (`ARRAY<STRING>`) |
| OBX.11 | Observation Result Status | ID | R | 1 | 1 | `F` (final), `P` (preliminary), `C` (correction), `D` (delete), `I` (pending), `X` (cancelled), `W` (post to wrong patient) (Table 0085) |
| OBX.12 | Effective Date of Reference Range | TS | O | 26 | 1 | Reference range effective date |
| OBX.13 | User Defined Access Checks | ST | O | 20 | 1 | Access check code |
| OBX.14 | Date/Time of the Observation | TS | O | 26 | 1 | Observation datetime |
| OBX.15 | Producer's ID | CE | O | 250 | 1 | Lab/producer identifier |
| OBX.16 | Responsible Observer | XCN | O | 250 | * | Responsible clinician |
| OBX.17 | Observation Method | CE | O | 250 | * | Method used |
| OBX.18 | Equipment Instance Identifier | EI | O | 22 | * | Equipment ID |
| OBX.19 | Date/Time of the Analysis | TS | O | 26 | 1 | Analysis datetime |
| OBX.20 | Observation Site | CWE | O | 705 | * | (added in v2.7) Anatomical site where the observation was made. Code columns: `observation_site_*` (CWE expansion) |
| OBX.21 | Observation Instance Identifier | EI | O | 427 | 1 | (added in v2.7) Unique identifier for this specific observation instance. Code columns: `observation_instance_identifier_*` (EI expansion) |
| OBX.22 | Mood Code | CNE | C | 705 | 1 | (added in v2.7) Mood of the observation (e.g. EVN=event, RQO=requested) (Table 0725). Code columns: `mood_code_*` (CNE/CWE expansion) |
| OBX.23 | Performing Organization Name | XON | O | 567 | 1 | Performing lab name |
| OBX.24 | Performing Organization Address | XAD | O | 631 | 1 | Performing lab address |
| OBX.25 | Performing Organization Medical Director | XCN | O | 3002 | 1 | Medical director. Spark columns: `performing_org_medical_director_id`, `performing_org_medical_director_family_name`, `performing_org_medical_director_given_name`, `performing_org_medical_director_middle_name`, `performing_org_medical_director_suffix`, `performing_org_medical_director_prefix`, `performing_org_medical_director_degree`, `performing_org_medical_director_source_table`, `performing_org_medical_director_assigning_authority*` (HD — 3 cols), `performing_org_medical_director_name_type_code`, `performing_org_medical_director_check_digit`, `performing_org_medical_director_check_digit_scheme`, `performing_org_medical_director_identifier_type_code`, `performing_org_medical_director_assigning_facility*` (HD — 3 cols), `performing_org_medical_director_name_representation_code`, `performing_org_medical_director_name_assembly_order`, `performing_org_medical_director_effective_date`, `performing_org_medical_director_expiration_date`, `performing_org_medical_director_professional_suffix` |
| OBX.26 | Patient Results Release Category Code | CWE | O | 705 | 1 | (added in v2.8) Category controlling release of results to the patient. Spark column: `patient_results_release_category` (string, CWE.1 code only) |
| OBX.27 | Root Cause | CWE | O | 705 | 1 | (added in v2.8) Root cause code for the observation. Spark columns: `root_cause`, `root_cause_text`, `root_cause_coding_system`, `root_cause_alt_code`, `root_cause_alt_text`, `root_cause_alt_coding_system`, `root_cause_coding_system_version`, `root_cause_alt_coding_system_version`, `root_cause_original_text` |
| OBX.28 | Local Process Control | CWE | O | 705 | * | (added in v2.8) Site-defined local process control codes. Stored as `ARRAY<STRUCT<...9 CWE fields...>>`. Spark column: `local_process_control` |
| OBX.29 | Observation Type | ID | O | 3 | 1 | (added in v2.8.2) Observation type code (Table 0936). Spark column: `observation_type` |
| OBX.30 | Observation Sub-Type | ID | O | 3 | 1 | (added in v2.8.2) Observation sub-type code (Table 0937). Spark column: `observation_sub_type` |
| OBX.31 | Action Code | ID | O | 2 | 1 | (added in v2.9) Action code for this observation result (Table 0206): `A` (add), `D` (delete), `U` (update), `X` (no change). Spark column: `action_code` |
| OBX.32 | Observation Value Absent Reason | CWE | O | 705 | * | (added in v2.9) Reason the observation value is absent. Stored as `ARRAY<STRUCT<...9 CWE fields...>>`. Spark column: `observation_value_absent_reason` |
| OBX.33 | Observation Related Specimen Identifier | EIP | O | 427 | * | (added in v2.9) Relates this OBX to a SPM specimen by EIP pair. Stored as `ARRAY<STRUCT<placer_assigned_identifier EI, filler_assigned_identifier EI>>`. Spark column: `observation_related_specimen` |

### NTE — Notes and Comments (9 fields)

Source: [Caristix HL7v2.5.1 NTE](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/NTE)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| NTE.1 | Set ID - NTE | SI | O | 4 | 1 | Sequence number |
| NTE.2 | Source of Comment | ID | O | 8 | 1 | `L` (ancillary/filler), `P` (orderer/placer), `O` (other) (Table 0105) |
| NTE.3 | Comment | FT | O | 65536 | * | Free-text comment/note content. Stored as `ARRAY<STRING>`. Spark column: `comment` |
| NTE.4 | Comment Type | CWE | O | 705 | 1 | Comment type (Table 0364). Spark columns: `comment_type`, `comment_type_text`, `comment_type_coding_system`, `comment_type_alt_code`, `comment_type_alt_text`, `comment_type_alt_coding_system`, `comment_type_coding_system_version`, `comment_type_alt_coding_system_version`, `comment_type_original_text` |
| NTE.5 | Entered By | XCN | O | 3002 | 1 | (added in v2.6) Person who entered the note. Spark columns: `entered_by_id`, `entered_by_family_name`, `entered_by_given_name`, `entered_by_middle_name`, `entered_by_suffix`, `entered_by_prefix`, `entered_by_degree`, `entered_by_source_table`, `entered_by_assigning_authority*` (HD — 3 cols), `entered_by_name_type_code`, `entered_by_check_digit`, `entered_by_check_digit_scheme`, `entered_by_identifier_type_code`, `entered_by_assigning_facility*` (HD — 3 cols), `entered_by_name_representation_code`, `entered_by_name_assembly_order`, `entered_by_effective_date`, `entered_by_expiration_date`, `entered_by_professional_suffix` |
| NTE.6 | Entered Date/Time | DTM | O | 24 | 1 | (added in v2.6) Date/time the note was entered. Spark column: `entered_datetime` (TimestampType) |
| NTE.7 | Effective Start Date | DTM | O | 24 | 1 | (added in v2.6) Effective start date of the note. Spark column: `effective_start_date` (TimestampType) |
| NTE.8 | Expiration Date | DTM | O | 24 | 1 | (added in v2.6) Expiration date of the note. Spark column: `expiration_date` (TimestampType) |
| NTE.9 | Coded Comment | CWE | O | 705 | * | (added in v2.9) Coded form of the comment. Stored as `ARRAY<STRUCT<...9 CWE fields...>>`. Spark column: `coded_comment` |

### SPM — Specimen (35 fields)

Source: [Caristix HL7v2.5.1 SPM](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/SPM)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| SPM.1 | Set ID - SPM | SI | O | 4 | 1 | Sequence number |
| SPM.2 | Specimen ID | EIP | O | 80 | 1 | Specimen identifier pair (single instance). EIP expanded into 8 flat columns: `specimen_id_placer_assigned_identifier` (EIP.1.1), `specimen_id_placer_assigned_identifier_namespace_id` (EIP.1.2), `specimen_id_placer_assigned_identifier_universal_id` (EIP.1.3), `specimen_id_placer_assigned_identifier_universal_id_type` (EIP.1.4), `specimen_id_filler_assigned_identifier` (EIP.2.1), `specimen_id_filler_assigned_identifier_namespace_id` (EIP.2.2), `specimen_id_filler_assigned_identifier_universal_id` (EIP.2.3), `specimen_id_filler_assigned_identifier_universal_id_type` (EIP.2.4) |
| SPM.3 | Specimen Parent IDs | EIP | O | 80 | * | Parent specimen identifiers. Stored as `ARRAY<STRUCT<placer_assigned_identifier EI, filler_assigned_identifier EI>>`. Spark column: `specimen_parent_ids` |
| SPM.4 | Specimen Type | CWE | R | 250 | 1 | Specimen type (Table 0487) |
| SPM.5 | Specimen Type Modifier | CWE | O | 250 | * | Specimen type modifier (Table 0541) |
| SPM.6 | Specimen Additives | CWE | O | 250 | * | Additives/preservatives (Table 0371) |
| SPM.7 | Specimen Collection Method | CWE | O | 250 | 1 | Collection method (Table 0488) |
| SPM.8 | Specimen Source Site | CWE | O | 250 | 1 | Source body site |
| SPM.9 | Specimen Source Site Modifier | CWE | O | 250 | * | Source site modifier (Table 0542) |
| SPM.10 | Specimen Collection Site | CWE | O | 250 | 1 | Collection site (Table 0543) |
| SPM.11 | Specimen Role | CWE | O | 250 | * | Specimen role (Table 0369) |
| SPM.12 | Specimen Collection Amount | CQ | O | 20 | 1 | Collection amount with units |
| SPM.13 | Grouped Specimen Count | NM | C | 6 | 1 | Number of grouped specimens |
| SPM.14 | Specimen Description | ST | O | 250 | * | Free-text specimen description |
| SPM.15 | Specimen Handling Code | CWE | O | 250 | * | Handling instructions (Table 0376) |
| SPM.16 | Specimen Risk Code | CWE | O | 250 | * | Risk codes (Table 0489) |
| SPM.17 | Specimen Collection Date/Time | DR | O | 26 | 1 | Collection date/time range |
| SPM.18 | Specimen Received Date/Time | TS | O | 26 | 1 | When specimen was received |
| SPM.19 | Specimen Expiration Date/Time | TS | O | 26 | 1 | Specimen expiration datetime |
| SPM.20 | Specimen Availability | ID | O | 1 | 1 | Y/N availability (Table 0136) |
| SPM.21 | Specimen Reject Reason | CWE | O | 250 | * | Reject reason (Table 0490) |
| SPM.22 | Specimen Quality | CWE | O | 250 | 1 | Quality assessment (Table 0491) |
| SPM.23 | Specimen Appropriateness | CWE | O | 250 | 1 | Appropriateness assessment (Table 0492) |
| SPM.24 | Specimen Condition | CWE | O | 250 | * | Specimen condition (Table 0493) |
| SPM.25 | Specimen Current Quantity | CQ | O | 20 | 1 | Current quantity |
| SPM.26 | Number of Specimen Containers | NM | O | 4 | 1 | Container count |
| SPM.27 | Container Type | CWE | O | 250 | 1 | Container type |
| SPM.28 | Container Condition | CWE | O | 250 | 1 | Container condition (Table 0544) |
| SPM.29 | Specimen Child Role | CWE | O | 250 | 1 | Child role (Table 0494). Spark columns: `specimen_child_role`, `specimen_child_role_text`, `specimen_child_role_coding_system`, `specimen_child_role_alt_code`, `specimen_child_role_alt_text`, `specimen_child_role_alt_coding_system`, `specimen_child_role_coding_system_version`, `specimen_child_role_alt_coding_system_version`, `specimen_child_role_original_text` |
| SPM.30 | Accession ID | CX | O | 250 | * | (added in v2.6) Accession identifier. Stored as `ARRAY<STRUCT<...16 CX fields...>>`. Spark column: `accession_id` |
| SPM.31 | Other Specimen ID | CX | O | 250 | * | (added in v2.6) Other specimen identifiers. Stored as `ARRAY<STRUCT<...16 CX fields...>>`. Spark column: `other_specimen_id` |
| SPM.32 | Shipment ID | EI | O | 427 | 1 | (added in v2.6) Shipment identifier. Spark columns: `shipment_id`, `shipment_id_namespace_id`, `shipment_id_universal_id`, `shipment_id_universal_id_type` |
| SPM.33 | Culture Start Date/Time | DTM | O | 24 | 1 | (added in v2.9) Culture start date/time. Spark column: `culture_start_datetime` (TimestampType) |
| SPM.34 | Culture Final Date/Time | DTM | O | 24 | 1 | (added in v2.9) Culture final date/time. Spark column: `culture_final_datetime` (TimestampType) |
| SPM.35 | Action Code | ID | O | 2 | 1 | (added in v2.9) Action code for this specimen (Table 0206): `A` (add), `D` (delete), `U` (update), `X` (no change). Spark column: `action_code` |

### IN1 — Insurance (55 fields)

Source: [Caristix HL7v2.5.1 IN1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/IN1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| IN1.1 | Set ID - IN1 | SI | R | 4 | 1 | Sequence number |
| IN1.2 | Insurance Plan ID | CE | R | 250 | 1 | Insurance plan (Table 0072) |
| IN1.3 | Insurance Company ID | CX | R | 250 | * | Insurance company identifier |
| IN1.4 | Insurance Company Name | XON | O | 250 | * | Insurance company name |
| IN1.5 | Insurance Company Address | XAD | O | 250 | * | Insurance company address |
| IN1.6 | Insurance Co Contact Person | XPN | O | 250 | * | Contact person |
| IN1.7 | Insurance Co Phone Number | XTN | O | 250 | * | Phone number |
| IN1.8 | Group Number | ST | O | 12 | 1 | Group/policy group number |
| IN1.9 | Group Name | XON | O | 250 | * | Group name |
| IN1.10 | Insured's Group Emp ID | CX | O | 250 | * | Insured's group employer ID |
| IN1.11 | Insured's Group Emp Name | XON | O | 250 | * | Insured's group employer name |
| IN1.12 | Plan Effective Date | DT | O | 8 | 1 | Plan effective date |
| IN1.13 | Plan Expiration Date | DT | O | 8 | 1 | Plan expiration date |
| IN1.14 | Authorization Information | AUI | O | 239 | 1 | Authorization info |
| IN1.15 | Plan Type | IS | O | 3 | 1 | Plan type (Table 0086) |
| IN1.16 | Name Of Insured | XPN | O | 250 | * | Insured person's name |
| IN1.17 | Insured's Relationship To Patient | CE | O | 250 | 1 | Relationship (Table 0063) |
| IN1.18 | Insured's Date Of Birth | TS | O | 26 | 1 | Insured's DOB |
| IN1.19 | Insured's Address | XAD | O | 250 | * | Insured's address |
| IN1.20 | Assignment Of Benefits | IS | O | 2 | 1 | Assignment of benefits (Table 0135) |
| IN1.21 | Coordination Of Benefits | IS | O | 2 | 1 | Coordination of benefits (Table 0173) |
| IN1.22 | Coord Of Ben. Priority | ST | O | 2 | 1 | COB priority |
| IN1.23 | Notice Of Admission Flag | ID | O | 1 | 1 | Y/N notice of admission (Table 0136) |
| IN1.24 | Notice Of Admission Date | DT | O | 8 | 1 | Admission notice date |
| IN1.25 | Report Of Eligibility Flag | ID | O | 1 | 1 | Y/N eligibility report (Table 0136) |
| IN1.26 | Report Of Eligibility Date | DT | O | 8 | 1 | Eligibility report date |
| IN1.27 | Release Information Code | IS | O | 2 | 1 | Release info code (Table 0093) |
| IN1.28 | Pre-Admit Cert (PAC) | ST | O | 15 | 1 | Pre-admission certification |
| IN1.29 | Verification Date/Time | TS | O | 26 | 1 | Verification datetime |
| IN1.30 | Verification By | XCN | O | 250 | * | Verified by |
| IN1.31 | Type Of Agreement Code | IS | O | 2 | 1 | Agreement type (Table 0098) |
| IN1.32 | Billing Status | IS | O | 2 | 1 | Billing status (Table 0022) |
| IN1.33 | Lifetime Reserve Days | NM | O | 4 | 1 | Lifetime reserve days |
| IN1.34 | Delay Before L.R. Day | NM | O | 4 | 1 | Delay before LR day |
| IN1.35 | Company Plan Code | IS | O | 8 | 1 | Company plan code (Table 0042) |
| IN1.36 | Policy Number | ST | O | 15 | 1 | Policy number |
| IN1.37 | Policy Deductible | CP | O | 12 | 1 | Policy deductible amount |
| IN1.38 | Policy Limit - Amount | CP | O | 12 | 1 | Policy limit amount |
| IN1.39 | Policy Limit - Days | NM | B | 4 | 1 | Policy limit days |
| IN1.40 | Room Rate - Semi-Private | CP | B | 12 | 1 | Deprecated — semi-private room rate |
| IN1.41 | Room Rate - Private | CP | B | 12 | 1 | Deprecated — private room rate |
| IN1.42 | Insured's Employment Status | CE | O | 250 | 1 | Employment status (Table 0066) |
| IN1.43 | Insured's Administrative Sex | IS | O | 1 | 1 | Sex (Table 0001) |
| IN1.44 | Insured's Employer's Address | XAD | O | 250 | * | Employer address |
| IN1.45 | Verification Status | ST | O | 2 | 1 | Verification status |
| IN1.46 | Prior Insurance Plan ID | IS | O | 8 | 1 | Prior plan ID (Table 0072) |
| IN1.47 | Coverage Type | IS | O | 3 | 1 | Coverage type (Table 0309) |
| IN1.48 | Handicap | IS | O | 2 | 1 | Handicap (Table 0295) |
| IN1.49 | Insured's ID Number | CX | O | 250 | * | Insured's ID |
| IN1.50 | Signature Code | IS | O | 1 | 1 | Signature code (Table 0535) |
| IN1.51 | Signature Code Date | DT | O | 8 | 1 | Signature code date |
| IN1.52 | Insured's Birth Place | ST | O | 250 | 1 | Insured's birth place. Spark column: `insureds_birth_place` |
| IN1.53 | VIP Indicator | CWE | O | 705 | 1 | VIP indicator (Table 0099). Spark columns: `vip_indicator`, `vip_indicator_text`, `vip_indicator_coding_system`, `vip_indicator_alt_code`, `vip_indicator_alt_text`, `vip_indicator_alt_coding_system`, `vip_indicator_coding_system_version`, `vip_indicator_alt_coding_system_version`, `vip_indicator_original_text` |
| IN1.54 | External Health Plan Identifiers | CWE | O | 705 | * | (added in v2.8) External health plan identifiers. Stored as `ARRAY<STRUCT<...9 CWE fields...>>`. Spark column: `external_health_plan_identifiers` |
| IN1.55 | Insurance Action Code | ID | O | 2 | 1 | (added in v2.9) Insurance action code (Table 0206): `A` (add), `D` (delete), `U` (update). Spark column: `insurance_action_code` |

### GT1 — Guarantor (57 fields)

Source: [Caristix HL7v2.5.1 GT1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/GT1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| GT1.1 | Set ID - GT1 | SI | R | 4 | 1 | Sequence number |
| GT1.2 | Guarantor Number | CX | O | 250 | * | Guarantor identifier |
| GT1.3 | Guarantor Name | XPN | R | 250 | * | Guarantor name (comp 1 = family, comp 2 = given) |
| GT1.4 | Guarantor Spouse Name | XPN | O | 250 | * | Spouse name |
| GT1.5 | Guarantor Address | XAD | O | 250 | * | Guarantor address |
| GT1.6 | Guarantor Ph Num - Home | XTN | O | 250 | * | Home phone |
| GT1.7 | Guarantor Ph Num - Business | XTN | O | 250 | * | Business phone |
| GT1.8 | Guarantor Date/Time Of Birth | TS | O | 26 | 1 | Date of birth |
| GT1.9 | Guarantor Administrative Sex | IS | O | 1 | 1 | Sex (Table 0001) |
| GT1.10 | Guarantor Type | IS | O | 2 | 1 | Guarantor type (Table 0068) |
| GT1.11 | Guarantor Relationship | CE | O | 250 | 1 | Relationship to patient (Table 0063) |
| GT1.12 | Guarantor SSN | ST | O | 11 | 1 | Social security number |
| GT1.13 | Guarantor Date - Begin | DT | O | 8 | 1 | Guarantor start date |
| GT1.14 | Guarantor Date - End | DT | O | 8 | 1 | Guarantor end date |
| GT1.15 | Guarantor Priority | NM | O | 2 | 1 | Priority |
| GT1.16 | Guarantor Employer Name | XPN | O | 250 | * | Employer name |
| GT1.17 | Guarantor Employer Address | XAD | O | 250 | * | Employer address |
| GT1.18 | Guarantor Employer Phone Number | XTN | O | 250 | * | Employer phone |
| GT1.19 | Guarantor Employee ID Number | CX | O | 250 | * | Employee ID |
| GT1.20 | Guarantor Employment Status | IS | O | 2 | 1 | Employment status (Table 0066) |
| GT1.21 | Guarantor Organization Name | XON | O | 250 | * | Organization name |
| GT1.22 | Guarantor Billing Hold Flag | ID | O | 1 | 1 | Y/N billing hold (Table 0136) |
| GT1.23 | Guarantor Credit Rating Code | CE | O | 250 | 1 | Credit rating (Table 0341) |
| GT1.24 | Guarantor Death Date And Time | TS | O | 26 | 1 | Death datetime |
| GT1.25 | Guarantor Death Flag | ID | O | 1 | 1 | Y/N death (Table 0136) |
| GT1.26 | Guarantor Charge Adjustment Code | CE | O | 250 | 1 | Charge adjustment (Table 0218) |
| GT1.27 | Guarantor Household Annual Income | CP | O | 10 | 1 | Household annual income |
| GT1.28 | Guarantor Household Size | NM | O | 3 | 1 | Household size |
| GT1.29 | Guarantor Employer ID Number | CX | O | 250 | * | Employer ID |
| GT1.30 | Guarantor Marital Status Code | CE | O | 250 | 1 | Marital status (Table 0002) |
| GT1.31 | Guarantor Hire Effective Date | DT | O | 8 | 1 | Hire date |
| GT1.32 | Employment Stop Date | DT | O | 8 | 1 | Employment stop date |
| GT1.33 | Living Dependency | IS | O | 2 | 1 | Living dependency (Table 0223) |
| GT1.34 | Ambulatory Status | IS | O | 2 | * | Ambulatory status (Table 0009) |
| GT1.35 | Citizenship | CE | O | 250 | * | Citizenship (Table 0171) |
| GT1.36 | Primary Language | CE | O | 250 | 1 | Primary language (Table 0296) |
| GT1.37 | Living Arrangement | IS | O | 2 | 1 | Living arrangement (Table 0220) |
| GT1.38 | Publicity Code | CE | O | 250 | 1 | Publicity code (Table 0215) |
| GT1.39 | Protection Indicator | ID | O | 1 | 1 | Y/N protection (Table 0136) |
| GT1.40 | Student Indicator | IS | O | 2 | 1 | Student status (Table 0231) |
| GT1.41 | Religion | CE | O | 250 | 1 | Religion (Table 0006) |
| GT1.42 | Mother's Maiden Name | XPN | O | 250 | * | Mother's maiden name |
| GT1.43 | Nationality | CE | O | 250 | 1 | Nationality (Table 0212) |
| GT1.44 | Ethnic Group | CE | O | 250 | * | Ethnic group (Table 0189) |
| GT1.45 | Contact Person's Name | XPN | O | 250 | * | Contact person name |
| GT1.46 | Contact Person's Telephone Number | XTN | O | 250 | * | Contact person phone |
| GT1.47 | Contact Reason | CE | O | 250 | 1 | Contact reason (Table 0222) |
| GT1.48 | Contact Relationship | IS | O | 3 | 1 | Contact relationship (Table 0063) |
| GT1.49 | Job Title | ST | O | 20 | 1 | Job title |
| GT1.50 | Job Code/Class | JCC | O | 20 | 1 | Job code/class |
| GT1.51 | Guarantor Employer's Organization Name | XON | O | 250 | * | Employer organization |
| GT1.52 | Handicap | IS | O | 2 | 1 | Handicap (Table 0295) |
| GT1.53 | Job Status | IS | O | 2 | 1 | Job status (Table 0311) |
| GT1.54 | Guarantor Financial Class | FC | O | 50 | 1 | Financial class |
| GT1.55 | Guarantor Race | CE | O | 250 | * | Race (Table 0005) |
| GT1.56 | Guarantor Birth Place | ST | O | 250 | 1 | Birth place |
| GT1.57 | VIP Indicator | IS | O | 2 | 1 | VIP indicator (Table 0099) |

### FT1 — Financial Transaction (56 fields: 1–31 baseline, 32–43 v2.6+, 44–56 v2.9+)

Source: [Caristix HL7v2.5.1 FT1](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/FT1)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| FT1.1 | Set ID - FT1 | SI | O | 4 | 1 | Sequence number |
| FT1.2 | Transaction ID | ST | O | 12 | 1 | Unique transaction identifier |
| FT1.3 | Transaction Batch ID | ST | O | 10 | 1 | Batch identifier |
| FT1.4 | Transaction Date | DR | R | 53 | 1 | Transaction date/time range |
| FT1.5 | Transaction Posting Date | TS | O | 26 | 1 | Posting datetime |
| FT1.6 | Transaction Type | IS | R | 8 | 1 | `CG` (charge), `CR` (credit), `PA` (payment), `AJ` (adjustment) (Table 0017) |
| FT1.7 | Transaction Code | CE | R | 250 | 1 | Transaction/charge code (Table 0132) |
| FT1.8 | Transaction Description | ST | B | 40 | 1 | Deprecated — transaction description |
| FT1.9 | Transaction Description - Alt | ST | B | 40 | 1 | Deprecated — alternate description |
| FT1.10 | Transaction Quantity | NM | O | 6 | 1 | Quantity |
| FT1.11 | Transaction Amount - Extended | CP | O | 12 | 1 | Extended amount (quantity × unit price) — all 6 CP components flattened |
| FT1.12 | Transaction Amount - Unit | CP | O | 12 | 1 | Unit price — all 6 CP components flattened |
| FT1.13 | Department Code | CE | O | 250 | 1 | Department (Table 0049) |
| FT1.14 | Insurance Plan ID | CE | O | 250 | 1 | Insurance plan (Table 0072) |
| FT1.15 | Insurance Amount | CP | O | 12 | 1 | Insurance amount |
| FT1.16 | Assigned Patient Location | PL | O | 80 | 1 | Patient location |
| FT1.17 | Fee Schedule | IS | O | 1 | 1 | Fee schedule (Table 0024) |
| FT1.18 | Patient Type | IS | O | 2 | 1 | Patient type (Table 0018) |
| FT1.19 | Diagnosis Code - FT1 | CE | O | 250 | * | Diagnosis code (Table 0051) |
| FT1.20 | Performed By Code | XCN | O | 250 | * | Performer (comp 1 = ID, comp 2 = family) (Table 0084) |
| FT1.21 | Ordered By Code | XCN | O | 250 | * | Ordering provider |
| FT1.22 | Unit Cost | CP | O | 12 | 1 | Unit cost |
| FT1.23 | Filler Order Number | EI | O | 427 | 1 | Filler order number |
| FT1.24 | Entered By Code | XCN | O | 250 | * | Entered by |
| FT1.25 | Procedure Code | CE | O | 250 | 1 | Procedure code (Table 0088) |
| FT1.26 | Procedure Code Modifier | CE | O | 250 | * | Procedure modifier (Table 0340) |
| FT1.27 | Advanced Beneficiary Notice Code | CE | O | 250 | 1 | ABN code (Table 0339) |
| FT1.28 | Medically Necessary Duplicate Procedure Reason | CWE | O | 250 | 1 | Duplicate procedure reason (Table 0476) |
| FT1.29 | NDC Code | CNE | O | 250 | 1 | National Drug Code (Table 0549) |
| FT1.30 | Payment Reference ID | CX | O | 250 | 1 | Payment reference — all 12 CX components flattened (ID + HD-decomposed assigning authority/facility) |
| FT1.31 | Transaction Reference Key | SI | O | 4 | * | Repeating SI; stored as `ARRAY<STRING>` of FT1-1 set-IDs linking payment to corresponding charges |
| FT1.32 | Performing Facility | XON | O | 250 | * | v2.6+. Repeating XON; stored as `ARRAY<STRUCT>` with all XON components per repetition |
| FT1.33 | Ordering Facility | XON | O | 250 | 1 | v2.6+. All XON components flattened |
| FT1.34 | Item Number | CWE | O | 250 | 1 | v2.6+. All CWE components flattened |
| FT1.35 | Model Number | ST | O | 250 | 1 | v2.6+ |
| FT1.36 | Special Processing Code | CWE | O | 250 | * | v2.6+. Repeating CWE array |
| FT1.37 | Clinic Code | CWE | O | 250 | 1 | v2.6+. All CWE components flattened |
| FT1.38 | Referral Number | CX | O | 250 | 1 | v2.6+. All 12 CX components flattened |
| FT1.39 | Authorization Number | CX | O | 250 | 1 | v2.6+. All 12 CX components flattened |
| FT1.40 | Service Provider Taxonomy Code | CWE | O | 250 | 1 | v2.6+. All CWE components flattened |
| FT1.41 | Revenue Code | CWE | O | 250 | 1 | v2.6+. All CWE components flattened (Table 0456) |
| FT1.42 | Prescription Number | ST | O | 20 | 1 | v2.6+ |
| FT1.43 | NDC Qty and UoM | CQ | O | 0 | 1 | v2.6+. CQ.1 (NM quantity) + CQ.2 (CWE units) flattened |
| FT1.44–FT1.56 | DME-related fields | various | O | – | – | v2.9+. Captured for forward compatibility; see `hl7_v2_schemas.py` for individual columns |

### RXA — Pharmacy/Treatment Administration (29 fields)

Source: [Caristix HL7v2.5.1 RXA](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/RXA)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| RXA.1 | Give Sub-ID Counter | NM | R | 4 | 1 | Sub-ID counter |
| RXA.2 | Administration Sub-ID Counter | NM | R | 4 | 1 | Administration sub-ID counter |
| RXA.3 | Date/Time Start of Administration | TS | R | 26 | 1 | Administration start datetime |
| RXA.4 | Date/Time End of Administration | TS | R | 26 | 1 | Administration end datetime |
| RXA.5 | Administered Code | CE | R | 250 | 1 | Drug/vaccine code (comp 1 = code, comp 2 = text) (Table 0292) |
| RXA.6 | Administered Amount | NM | R | 20 | 1 | Amount administered |
| RXA.7 | Administered Units | CE | C | 250 | 1 | Units of measure |
| RXA.8 | Administered Dosage Form | CE | O | 250 | 1 | Dosage form |
| RXA.9 | Administration Notes | CE | O | 250 | * | Administration notes |
| RXA.10 | Administering Provider | XCN | O | 250 | * | Provider who administered (comp 1 = ID) |
| RXA.11 | Administered-at Location | LA2 | C | 200 | 1 | Administration location |
| RXA.12 | Administered Per (Time Unit) | ST | C | 20 | 1 | Rate time unit |
| RXA.13 | Administered Strength | NM | O | 20 | 1 | Strength administered |
| RXA.14 | Administered Strength Units | CE | O | 250 | 1 | Strength units |
| RXA.15 | Substance Lot Number | ST | O | 20 | * | Lot number |
| RXA.16 | Substance Expiration Date | TS | O | 26 | * | Substance expiration date |
| RXA.17 | Substance Manufacturer Name | CE | O | 250 | * | Manufacturer (Table 0227) |
| RXA.18 | Substance/Treatment Refusal Reason | CE | O | 250 | * | Refusal reason |
| RXA.19 | Indication | CE | O | 250 | * | Indication for administration |
| RXA.20 | Completion Status | ID | O | 2 | 1 | `CP` (complete), `RE` (refused), `NA` (not administered), `PA` (partially administered) (Table 0322) |
| RXA.21 | Action Code - RXA | ID | O | 2 | 1 | Action code (Table 0323) |
| RXA.22 | System Entry Date/Time | TS | O | 26 | 1 | System entry datetime |
| RXA.23 | Administered Drug Strength Volume | NM | O | 5 | 1 | Strength volume |
| RXA.24 | Administered Drug Strength Volume Units | CWE | O | 250 | 1 | Strength volume units |
| RXA.25 | Administered Barcode Identifier | CWE | O | 60 | 1 | Barcode identifier |
| RXA.26 | Pharmacy Order Type | ID | O | 1 | 1 | Order type (Table 0480). Spark column: `pharmacy_order_type` |
| RXA.27 | Administer-at | PL | O | 80 | 1 | (added in v2.6) Physical location where the drug was administered. PL expanded into 11 flat columns: `administer_at_point_of_care`, `administer_at_room`, `administer_at_bed`, `administer_at_facility*` (HD — 3 cols), `administer_at_status`, `administer_at_type`, `administer_at_building`, `administer_at_floor`, `administer_at_description`, `administer_at_comprehensive_location_id` |
| RXA.28 | Administered-at Address | XAD | O | 250 | 1 | (added in v2.6) Address where the drug was administered. XAD expanded into multiple flat columns with prefix `administered_at_address_*` |
| RXA.29 | Administered Tag Identifier | EI | O | 427 | * | (added in v2.9) Tag identifier for the administered drug. Stored as `ARRAY<STRUCT<entity_identifier, namespace_id, universal_id, universal_id_type>>`. Spark column: `administered_tag_identifier` |

### SCH — Scheduling Activity Information (28 fields)

Source: [Caristix HL7v2.5.1 SCH](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/SCH)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| SCH.1 | Placer Appointment ID | EI | C | 75 | 1 | Appointment ID from placer (comp 1 = entity ID) |
| SCH.2 | Filler Appointment ID | EI | C | 75 | 1 | Appointment ID from filler (comp 1 = entity ID) |
| SCH.3 | Occurrence Number | NM | C | 5 | 1 | Occurrence number |
| SCH.4 | Placer Group Number | EI | O | 22 | 1 | Placer group number |
| SCH.5 | Schedule ID | CE | O | 250 | 1 | Schedule identifier |
| SCH.6 | Event Reason | CE | R | 250 | 1 | Event reason |
| SCH.7 | Appointment Reason | CE | O | 250 | 1 | Reason for appointment (Table 0276) |
| SCH.8 | Appointment Type | CE | O | 250 | 1 | Appointment type (Table 0277) |
| SCH.9 | Appointment Duration | NM | B | 20 | 1 | Deprecated — duration in minutes |
| SCH.10 | Appointment Duration Units | CE | B | 250 | 1 | Deprecated — duration units |
| SCH.11 | Appointment Timing Quantity | TQ | B | 200 | * | Deprecated — use TQ1/TQ2 |
| SCH.12 | Placer Contact Person | XCN | O | 250 | * | Placer contact (comp 1 = ID, comp 2 = family) |
| SCH.13 | Placer Contact Phone Number | XTN | O | 250 | 1 | Placer phone |
| SCH.14 | Placer Contact Address | XAD | O | 250 | * | Placer address |
| SCH.15 | Placer Contact Location | PL | O | 80 | 1 | Placer location |
| SCH.16 | Filler Contact Person | XCN | R | 250 | * | Filler contact (comp 1 = ID, comp 2 = family) |
| SCH.17 | Filler Contact Phone Number | XTN | O | 250 | 1 | Filler phone |
| SCH.18 | Filler Contact Address | XAD | O | 250 | * | Filler address |
| SCH.19 | Filler Contact Location | PL | O | 80 | 1 | Filler location |
| SCH.20 | Entered By Person | XCN | R | 250 | * | Person who entered (comp 1 = ID) |
| SCH.21 | Entered By Phone Number | XTN | O | 250 | * | Entered by phone |
| SCH.22 | Entered By Location | PL | O | 80 | 1 | Entered by location |
| SCH.23 | Parent Placer Appointment ID | EI | O | 75 | 1 | Parent placer appointment |
| SCH.24 | Parent Filler Appointment ID | EI | C | 75 | 1 | Parent filler appointment |
| SCH.25 | Filler Status Code | CE | O | 250 | 1 | Filler status (Table 0278) |
| SCH.26 | Placer Order Number | EI | C | 22 | * | Placer order number. Stored as `ARRAY<STRUCT<entity_identifier, namespace_id, universal_id, universal_id_type>>`. Spark column: `placer_order_number` |
| SCH.27 | Filler Order Number | EI | C | 22 | * | Filler order number. Stored as `ARRAY<STRUCT<entity_identifier, namespace_id, universal_id, universal_id_type>>`. Spark column: `filler_order_number` |
| SCH.28 | Alternate Placer Order Group Number | EIP | O | 427 | 1 | (added in v2.9) Alternate placer order group number (single EIP). EIP expanded into 8 flat columns: `alternate_placer_order_group_number_placer_assigned_identifier` (EIP.1.1), `alternate_placer_order_group_number_placer_assigned_identifier_namespace_id` (EIP.1.2), `alternate_placer_order_group_number_placer_assigned_identifier_universal_id` (EIP.1.3), `alternate_placer_order_group_number_placer_assigned_identifier_universal_id_type` (EIP.1.4), `alternate_placer_order_group_number_filler_assigned_identifier` (EIP.2.1), `alternate_placer_order_group_number_filler_assigned_identifier_namespace_id` (EIP.2.2), `alternate_placer_order_group_number_filler_assigned_identifier_universal_id` (EIP.2.3), `alternate_placer_order_group_number_filler_assigned_identifier_universal_id_type` (EIP.2.4) |

### TXA — Transcription Document Header (28 fields)

Source: [Caristix HL7v2.5.1 TXA](https://hl7-definition.caristix.com/v2/HL7v2.5.1/Segments/TXA)

| Position | Name | Data Type | Usage | Max Len | Rpt | Description |
|---|---|---|---|---|---|---|
| TXA.1 | Set ID - TXA | SI | R | 4 | 1 | Sequence number |
| TXA.2 | Document Type | IS | R | 30 | 1 | Document type (Table 0270) |
| TXA.3 | Document Content Presentation | ID | C | 2 | 1 | Content type (Table 0191) |
| TXA.4 | Activity Date/Time | TS | O | 26 | 1 | Activity datetime |
| TXA.5 | Primary Activity Provider Code/Name | XCN | C | 250 | * | Primary provider (comp 1 = ID) |
| TXA.6 | Origination Date/Time | TS | O | 26 | 1 | Origination datetime |
| TXA.7 | Transcription Date/Time | TS | C | 26 | 1 | Transcription datetime |
| TXA.8 | Edit Date/Time | TS | O | 26 | * | Edit datetime |
| TXA.9 | Originator Code/Name | XCN | O | 250 | * | Originator (comp 1 = ID) |
| TXA.10 | Assigned Document Authenticator | XCN | O | 250 | * | Assigned authenticator |
| TXA.11 | Transcriptionist Code/Name | XCN | C | 250 | * | Transcriptionist (comp 1 = ID) |
| TXA.12 | Unique Document Number | EI | R | 30 | 1 | Unique document identifier |
| TXA.13 | Parent Document Number | EI | C | 30 | 1 | Parent document identifier |
| TXA.14 | Placer Order Number | EI | O | 22 | * | Placer order number |
| TXA.15 | Filler Order Number | EI | O | 22 | 1 | Filler order number |
| TXA.16 | Unique Document File Name | ST | O | 30 | 1 | Document file name |
| TXA.17 | Document Completion Status | ID | R | 2 | 1 | `DI` (dictated), `DO` (documented), `IP` (in progress), `IN` (incomplete), `PA` (pre-authenticated), `AU` (authenticated), `LA` (legally authenticated) (Table 0271) |
| TXA.18 | Document Confidentiality Status | ID | O | 2 | 1 | Confidentiality status (Table 0272) |
| TXA.19 | Document Availability Status | ID | O | 2 | 1 | Availability status (Table 0273) |
| TXA.20 | Document Storage Status | ID | O | 2 | 1 | Storage status (Table 0275) |
| TXA.21 | Document Change Reason | ST | C | 30 | 1 | Reason for document change |
| TXA.22 | Authentication Person, Time Stamp | PPN | C | 250 | * | Authenticator with timestamp (PPN approximated as XCN for parsing). Stored as `ARRAY<STRUCT<...23 XCN fields...>>`. Spark column: `authentication_person_time_stamp` |
| TXA.23 | Distributed Copies | XCN | O | 250 | * | Recipients of copies. Stored as `ARRAY<STRUCT<...23 XCN fields...>>`. Spark column: `distributed_copies` |
| TXA.24 | Folder Assignment | CWE | O | 705 | * | (added in v2.6) Folder assignment codes. Stored as `ARRAY<STRUCT<...9 CWE fields...>>`. Spark column: `folder_assignment` |
| TXA.25 | Document Title | ST | O | 250 | * | (added in v2.6) Document titles. Stored as `ARRAY<STRING>`. Spark column: `document_title` |
| TXA.26 | Agreed Due Date/Time | DTM | O | 24 | 1 | (added in v2.8) Agreed due date/time. Spark column: `agreed_due_datetime` (TimestampType) |
| TXA.27 | Creating Facility | HD | O | 227 | 1 | (added in v2.9) Facility that created the document. Spark columns: `creating_facility`, `creating_facility_universal_id`, `creating_facility_universal_id_type` |
| TXA.28 | Creating Specialty | CWE | O | 705 | 1 | (added in v2.9) Specialty responsible for creating the document. Spark columns: `creating_specialty`, `creating_specialty_text`, `creating_specialty_coding_system`, `creating_specialty_alt_code`, `creating_specialty_alt_text`, `creating_specialty_alt_coding_system`, `creating_specialty_coding_system_version`, `creating_specialty_alt_coding_system_version`, `creating_specialty_original_text` |

---

## **Get Object Primary Keys**

Primary keys are **static** — not discoverable via API.

| Table | Primary Key | Description |
|---|---|---|
| `msh` | `message_id` | MSH-10 (Message Control ID) — one row per message |
| `evn` | `message_id` | One EVN per message |
| `pid` | `message_id` | One PID per message |
| `pd1` | `message_id` | One PD1 per message |
| `pv1` | `message_id` | One PV1 per message |
| `pv2` | `message_id` | One PV2 per message |
| `nk1` | `message_id`, `set_id` | Multiple NK1 per message (one per contact) |
| `mrg` | `message_id` | One MRG per message |
| `al1` | `message_id`, `set_id` | Multiple AL1 per message (one per allergy) |
| `iam` | `message_id`, `set_id` | Multiple IAM per message (one per adverse reaction) |
| `dg1` | `message_id`, `set_id` | Multiple DG1 per message (one per diagnosis) |
| `pr1` | `message_id`, `set_id` | Multiple PR1 per message (one per procedure) |
| `orc` | `message_id`, `set_id` | Multiple ORC per message (one per order) |
| `obr` | `message_id`, `set_id` | Multiple OBR per message (e.g. multi-order) |
| `obx` | `message_id`, `set_id` | Multiple OBX per message (one per result) |
| `nte` | `message_id`, `set_id` | Multiple NTE per message (one per comment) |
| `spm` | `message_id`, `set_id` | Multiple SPM per message (one per specimen) |
| `in1` | `message_id`, `set_id` | Multiple IN1 per message (one per insurance plan) |
| `gt1` | `message_id`, `set_id` | Multiple GT1 per message (one per guarantor) |
| `ft1` | `message_id`, `set_id` | Multiple FT1 per message (one per transaction) |
| `rxa` | `message_id`, `set_id` | Multiple RXA per message (one per administration) |
| `sch` | `message_id` | One SCH per message |
| `txa` | `message_id` | One TXA per message |

---

## **Object's Ingestion Type**

All tables use **`append`** ingestion:

- HL7 messages are fetched from the Google Cloud Healthcare API HL7v2 store.
- The connector tracks new messages by **`createTime`** (RFC3339 timestamp from the API — when the message was ingested into the HL7v2 store).
- Messages already processed are never re-fetched.
- No change feeds, upserts, or deletes — new messages always produce new rows.

| Table | Ingestion Type | Cursor | Notes |
|---|---|---|---|
| `msh` | `append` | `createTime` | One row per message |
| `evn` | `append` | `createTime` | One row per message with EVN |
| `pid` | `append` | `createTime` | One row per message with PID |
| `pd1` | `append` | `createTime` | One row per message with PD1 |
| `pv1` | `append` | `createTime` | One row per message with PV1 |
| `pv2` | `append` | `createTime` | One row per message with PV2 |
| `nk1` | `append` | `createTime` | Multiple rows per message possible |
| `mrg` | `append` | `createTime` | One row per merge message |
| `al1` | `append` | `createTime` | Multiple rows per message possible |
| `iam` | `append` | `createTime` | Multiple rows per message possible |
| `dg1` | `append` | `createTime` | Multiple rows per message possible |
| `pr1` | `append` | `createTime` | Multiple rows per message possible |
| `orc` | `append` | `createTime` | Multiple rows per message possible |
| `obr` | `append` | `createTime` | Multiple rows per message possible |
| `obx` | `append` | `createTime` | 8–50 rows per lab message typical |
| `nte` | `append` | `createTime` | Multiple rows per message possible |
| `spm` | `append` | `createTime` | Multiple rows per message possible |
| `in1` | `append` | `createTime` | Multiple rows per message possible |
| `gt1` | `append` | `createTime` | Multiple rows per message possible |
| `ft1` | `append` | `createTime` | Multiple rows per message possible |
| `rxa` | `append` | `createTime` | Multiple rows per message possible |
| `sch` | `append` | `createTime` | One row per scheduling message |
| `txa` | `append` | `createTime` | One row per document message |

---

## **Read API for Data Retrieval**

The HL7 v2 connector fetches messages from the **Google Cloud Healthcare API** via REST. Raw HL7 text is delivered base64-encoded in the `data` field and decoded before parsing.

### List Messages API

**Method**: `GET`

**URL**:
```
https://healthcare.googleapis.com/v1/projects/{project_id}/locations/{location}/datasets/{dataset_id}/hl7V2Stores/{hl7v2_store_id}/messages
```

**Query Parameters**:

| Parameter | Value / Example | Notes |
|---|---|---|
| `view` | `FULL` | Required to get raw `data` field. BASIC returns only `name`. |
| `pageSize` | `1000` | Max allowed value |
| `pageToken` | `<token from prior response>` | Pagination; omit on first request |
| `filter` | `createTime > "2024-01-01T00:00:00Z"` | Incremental filtering by createTime |
| `orderBy` | `sendTime asc` | Only `sendTime` is supported for ordering; ensures monotonic pagination |

**Example Request**:
```
GET https://healthcare.googleapis.com/v1/projects/my-project/locations/us-central1/datasets/my-dataset/hl7V2Stores/my-store/messages?view=FULL&pageSize=1000&filter=createTime>"2024-01-01T00:00:00Z"&orderBy=sendTime+asc
Authorization: Bearer <token>
```

**Response**:
```json
{
  "hl7V2Messages": [
    {
      "name": "projects/my-project/locations/us-central1/datasets/my-dataset/hl7V2Stores/my-store/messages/MSG_ID",
      "data": "<base64-encoded raw HL7 text>",
      "sendTime": "2024-01-15T12:00:00Z",
      "createTime": "2024-01-15T12:00:01Z",
      "messageType": "ADT",
      "patientIds": [{"patientId": "MRN12345", "type": {"typeCode": "MR"}}],
      "labels": {}
    }
  ],
  "nextPageToken": "<token for next page>"
}
```

**Decoding `data`**: Base64-decode the `data` field to get raw HL7 text, e.g.:
```python
import base64
hl7_text = base64.b64decode(msg["data"]).decode("utf-8", errors="replace")
```

### Get Single Message API

**Method**: `GET`

**URL**:
```
https://healthcare.googleapis.com/v1/{name}
```
where `name` = `projects/{project_id}/locations/{location}/datasets/{dataset_id}/hl7V2Stores/{hl7v2_store_id}/messages/{message_id}`

**Query Parameters**: `view=FULL` (defaults to FULL if omitted)

### HL7 v2 Message Structure (after decode)

An HL7 v2 message is a series of **segments**, each on its own line:

```
MSH|^~\&|EPIC|HOSPITAL|LAB|HOSPITAL|20240115120000||ADT^A01^ADT_A01|MSG00001|P|2.5.1
EVN|A01|20240115120000
PID|1||MRN12345^^^HOSP^MR||DOE^JOHN^A||19800101|M|||123 MAIN ST^^SPRINGFIELD^IL^62701
PD1|||SPRINGFIELD MEDICAL^^12345|54321^SMITH^JANE
PV1|1|I|ICU^101^A|E|||12345^SMITH^JANE|||MED
PV2||AC|^CHEST PAIN
NK1|1|DOE^JANE|SPO^SPOUSE||5551234567
AL1|1|DA|^PENICILLIN|SV|RASH
DG1|1||I10^Essential hypertension^I10|||A
IN1|1|BCBS^Blue Cross||BCBS OF IL
GT1|1||DOE^JOHN||123 MAIN ST^^SPRINGFIELD^IL^62701
```

### Delimiters

| Delimiter | Character | Position | Description |
|---|---|---|---|
| Segment terminator | `\r` (CR) | End of each line | Separates segments |
| Field separator | `\|` | MSH-1 | Separates fields within a segment |
| Component separator | `^` | MSH-2 position 1 | Separates components within a field |
| Repetition separator | `~` | MSH-2 position 2 | Separates repeating field values |
| Escape character | `\` | MSH-2 position 3 | Escapes special characters |
| Subcomponent separator | `&` | MSH-2 position 4 | Separates subcomponents within a component |

### Message Parsing Strategy

1. **Fetch messages** from the API with `view=FULL` and incremental filter on `createTime`
2. **Base64-decode** the `data` field to get raw HL7 text
3. **Parse**: split on segment terminator (`\r` or `\r\n` or `\n`), identify segment type by first 3 characters
4. **Extract MSH fields** first (MSH-7 timestamp, MSH-10 message ID, MSH-12 version) — these become common columns for all segment tables
5. **Multi-message data**: A single `data` field may contain multiple messages (each starting with `MSH`). The parser must detect MSH boundaries.
6. **Route segments** to appropriate tables based on segment type prefix

### Pagination / Batching

- **Cursor**: `createTime` (RFC3339 string from the API). After each batch, advance cursor to the latest `createTime` seen.
- **Batch size**: Configurable via `max_records_per_batch` table option (default 10,000 records). The connector stops fetching pages once this limit is reached.
- **Page size**: Max 1000 messages per API page. Use `pageToken` from `nextPageToken` to get the next page.
- **Rate limits**: TBD — Google Cloud Healthcare API quota limits vary by project; consult GCP quota console.

### Deleted Records

Not applicable. HL7 messages in the GCP Healthcare API store are not deleted during normal operation. There is no delete synchronization — once a message is ingested, it persists in the Delta table.

---

## **Field Type Mapping**

HL7 v2 defines its own data types. The connector maps them to Spark SQL types for Delta table storage.

### Primitive Data Types

| HL7 Type | Name | Spark SQL Type | Description |
|---|---|---|---|
| ST | String Data | STRING | Plain text, no components |
| TX | Text Data | STRING | Free-form text, may contain formatting |
| FT | Formatted Text | STRING | Text with HL7 formatting commands |
| NM | Numeric | STRING | Numeric string (may include sign, decimal) |
| SI | Sequence ID | STRING | Non-negative integer string |
| ID | Coded Value (HL7 tables) | STRING | Value from an HL7-defined table |
| IS | Coded Value (user tables) | STRING | Value from a user-defined table |
| DT | Date | STRING | `YYYY[MM[DD]]` format |
| TM | Time | STRING | `HH[MM[SS[.SSSS]]][+/-ZZZZ]` format |
| TS | Time Stamp | STRING | `YYYY[MM[DD[HH[MM[SS[.SSSS]]]]]][+/-ZZZZ]` — parsed from comp 1 |
| VARIES | Variable | STRING | Type determined at runtime by context (used for OBX-5) |

### Composite Data Types

**IMPORTANT — Extraction Rule:** Every component of a composite data type MUST be extracted into its own column. Do NOT extract only component 1 and discard the rest. The "Connector Extraction" column below lists ALL components that must be captured.

For repeating fields (separated by `~`), use `get_rep_component` instead of `get_component` to prevent the repetition separator from bleeding into component values.

Two extraction strategies are used depending on the HL7 cardinality:

- **Truly repeating fields** (HL7 cardinality `*` — fields where the spec allows multiple distinct values, e.g. PID-5 `patient_names`, NK1-7 `contact_persons`, OBX-8 `interpretation_codes`, MSH-18 `character_set`, MSH-21 `message_profile_identifiers`) are extracted as Spark `ARRAY<STRUCT<...>>` or `ARRAY<STRING>` columns with **one entry per repetition** — no data is lost. The helpers `_xpn_array_fields()`, `_cwe_array_fields()`, and `_ei_array_fields()` implement this and the corresponding `_xpn_array_schema()` / `_cwe_array_schema()` / `_ei_array_schema()` helpers declare the Spark types. There are 24 such columns across 11 tables (PID, NK1, GT1, IN1, MRG, MSH, PD1, PV1, PV2, OBR, OBX) — see the README's "Non-string column types" section for the full enumeration.
- **Non-repeating composite fields** (HL7 cardinality `1`, e.g. PID-7 `birth_date`, PID-8 `administrative_sex`) are flattened into individual `STRING` columns via `_xpn_fields()` / scalar extractors. Only the first repetition is captured if a sender unexpectedly puts a `~` in a cardinality-1 field; the full raw value is preserved in `raw_segment` for forensic reconstruction.

**IMPORTANT — Sub-component Extraction Rule:** When a component is itself a composite data type (e.g. HD or EI inside CX, XCN, or XON), its sub-components (separated by `&`) MUST also be extracted into individual columns using `get_sub_component(field, comp, sub)` or `get_rep_sub_component(field, rep, comp, sub)`. The most common case is **Assigning Authority (HD)** and **Assigning Facility (HD)** inside CX, XCN, and XON types — these must always be broken down into `namespace_id` (sub 1), `universal_id` (sub 2), and `universal_id_type` (sub 3). The raw component value (which already has `&` in it) is kept as well for convenience.

| HL7 Type | Name | Components | Connector Extraction |
|---|---|---|---|
| CE | Coded Element | identifier ^ text ^ coding system ^ alt-id ^ alt-text ^ alt-coding-system | ALL 6: code, text, coding_system, alt_code, alt_text, alt_coding_system |
| CWE | Coded with Exceptions | Same as CE plus additional components | ALL 6 (same as CE): code, text, coding_system, alt_code, alt_text, alt_coding_system |
| CNE | Coded with No Exceptions | Same as CE plus additional components | ALL 6 (same as CE): code, text, coding_system, alt_code, alt_text, alt_coding_system |
| CX | Extended Composite ID | ID ^ check digit ^ check digit scheme ^ assigning authority (HD) ^ ID type ^ assigning facility (HD) ^ effective date ^ expiration date ^ assigning jurisdiction ^ assigning agency | Key components: id_value, check_digit, assigning_authority (+ HD sub-components: universal_id, universal_id_type), type_code |
| XPN | Extended Person Name | family (FN) ^ given ^ middle ^ suffix ^ prefix ^ degree (W) ^ name type code ^ name rep code ^ name context (CWE) ^ validity range (W) ^ assembly order ^ effective date ^ expiration date ^ professional suffix ^ called by | ALL 14 active components (skip comp 10, withdrawn): family_name (1), given_name (2), middle_name (3), suffix (4), prefix (5), degree (6), name_type_code (7), name_representation_code (8), name_context (9), name_assembly_order (11), name_effective_date (12), name_expiration_date (13), professional_suffix (14), called_by (15). **CRITICAL**: suffix=comp 4, prefix=comp 5. In XCN these shift by +1 (suffix=5, prefix=6). Use the `_xpn_fields()` helper and `_xpn_schema()` helper for every XPN field to ensure completeness. |
| XAD | Extended Address | street ^ other ^ city ^ state ^ zip ^ country ^ address type ^ other geographic ^ county ^ census tract ^ address representation | Components: street, other_designation, city, state, zip, country, type |
| XTN | Extended Telecom Number | telephone ^ telecom use ^ telecom equipment ^ email ^ country code ^ area code ^ local number ^ extension ^ any text | Comp 1 (number) or individual components as needed |
| XCN | Extended Composite ID & Name | ID ^ family ^ given ^ middle ^ suffix ^ prefix ^ degree ^ source table ^ assigning authority ^ name type ^ check digit ^ check digit scheme ^ identifier type ^ assigning facility | Components: id, family_name, given_name, prefix |
| XON | Extended Composite Name & ID for Orgs | organization name ^ org name type ^ ID number ^ check digit ^ check digit scheme ^ assigning authority (HD) ^ identifier type ^ assigning facility (HD) ^ name rep code ^ org identifier | ALL 10: name, type_code, id, check_digit, check_digit_scheme, assigning_authority (+ HD sub-components), id_type_code, assigning_facility (+ HD sub-components), name_rep_code, identifier |
| HD | Hierarchic Designator | namespace ID ^ universal ID ^ universal ID type | ALL 3: namespace_id, universal_id, universal_id_type |
| EI | Entity Identifier | entity ID ^ namespace ID ^ universal ID ^ universal ID type | ALL 4: entity_id, namespace_id, universal_id, universal_id_type |
| EIP | Entity Identifier Pair | placer assigned identifier (EI) ^ filler assigned identifier (EI) | **Single-occurrence EIP (0..1)** — expanded into 8 flat columns via `_eip_fields()` / `_eip_schema()`: `<prefix>_placer_assigned_identifier` + 3 EI sub-cols, `<prefix>_filler_assigned_identifier` + 3 EI sub-cols. Used by `spm.specimen_id_*` (SPM-2), `obr.parent_results_observation_identifier_*` (OBR-29), `sch.alternate_placer_order_group_number_*` (SCH-28). **Repeating EIP (0..\*)** — modeled as `ARRAY<STRUCT<placer_assigned_identifier: STRUCT<...4 EI fields...>, filler_assigned_identifier: STRUCT<...4 EI fields...>>>` via `_eip_array_fields()` / `_eip_array_schema()`. Used by `spm.specimen_parent_ids` (SPM-3), `obx.observation_related_specimen` (OBX-33), `obr.parent_order` (OBR-54), `orc.parent_order` (ORC-8). |
| DLN | Driver's License Number | license number (ST) ^ issuing state/province (IS) ^ expiration date (DT) | Expanded into 3 flat columns via `_dln_fields()` / `_dln_schema()`: `<prefix>` (DLN.1 ST), `<prefix>_issuing_state` (DLN.2 IS Table 0333), `<prefix>_expiration_date` (DLN.3 DT → TIMESTAMP). Used by `pid.drivers_license_*` (PID-20). |
| TQ | Timing/Quantity | quantity (CQ) ^ interval (RI) ^ duration (ST) ^ start datetime (DTM) ^ end datetime (DTM) ^ priority (ST) ^ condition (ST) ^ text (TX) ^ conjunction (ID) ^ order sequencing (OSD) ^ occurrence duration (CWE) ^ total occurrences (NM) | Stored as repeating `ARRAY<STRUCT<...TQ fields...>>` via `_tq_array_fields()` / `_tq_array_schema()`. Used by `orc.quantity_timing` (ORC-7), `obr.quantity_timing` (OBR-27). |
| SPS | Specimen Source | specimen source (CWE) ^ additives (CWE) ^ collection method (TX) ^ body site (CWE) ^ site modifier (CWE) ^ collection method modifier (CWE) ^ specimen role (CWE) | Expanded into 8 flat columns: `specimen_source` (SPS.1.1), `specimen_source_text` (SPS.1.2), `specimen_source_additives` (SPS.2.1), `specimen_source_collection_method` (SPS.3), `specimen_source_body_site` (SPS.4.1), `specimen_source_site_modifier` (SPS.5.1), `specimen_source_collection_method_modifier` (SPS.6.1), `specimen_source_role` (SPS.7.1). Used by `obr.specimen_source_*` (OBR-15). |
| OG | Observation Grouper | original sub-ID (ST) ^ group (NM) ^ sequence (NM) ^ identifier (ST) | Expanded into 4 flat columns: `observation_sub_id` (OG.1 — backward-compatible ST sub-ID), `observation_sub_id_group` (OG.2 NM), `observation_sub_id_sequence` (OG.3 NM), `observation_sub_id_identifier` (OG.4 ST). Used by `obx.observation_sub_id*` (OBX-4, v2.8.2+). |
| PL | Person Location | point of care (HD) ^ room (HD) ^ bed (HD) ^ facility (HD) ^ location status (IS) ^ person location type (IS) ^ building (HD) ^ floor (HD) ^ location description (ST) ^ comprehensive location id (EI) ^ assigning authority (HD) | ALL 11 components flattened via `_pl_fields()` / `_pl_schema()`: point_of_care, room, bed, facility, status, type, building, floor, description, comprehensive_id, assigning_authority. Each HD sub-component stores HD.1 namespace ID. For repeating PL fields (e.g. `pr1.treating_organizational_unit`), use `_pl_array_fields()` / `_pl_array_schema()` to produce `ARRAY<STRUCT<...>>`. |
| MSG | Message Type | message code ^ trigger event ^ message structure | Comp 1 (message code), Comp 2 (trigger event), Comp 3 (message structure) — all flattened into separate columns. |
| PT | Processing Type | processing ID ^ processing mode | ALL 2 components flattened via `_pt_fields()` / `_pt_schema()`: `{prefix}` (PT.1 ID), `{prefix}_mode` (PT.2 ID). Used by `msh.processing_id*`. |
| VID | Version Identifier | version ID ^ internationalization code (CWE) ^ international version ID (CWE) | ALL 3 components flattened via `_vid_fields()` / `_vid_schema()` (7 columns): `{prefix}` (VID.1), then 3 CWE sub-components each for VID.2 (`_internationalization*`) and VID.3 (`_international_version*`). Used by `msh.version_id*`. |
| DR | Date/Time Range | range start datetime ^ range end datetime | Both components flattened as two `_ts` columns: `{prefix}_start` (DR.1, DTM) and `{prefix}_end` (DR.2, DTM). Used by `ft1.transaction_date_*`, `spm.specimen_collection_datetime_*`, `orc.order_status_date_range_*`. |
| CQ | Composite Quantity with Units | quantity (NM) ^ units (CWE) | ALL 2 components flattened via `_cq_fields()` / `_cq_schema()`: `{prefix}` (CQ.1 NM quantity), `{prefix}_units` (CQ.2.1 CWE code). Used by `ft1.ndc_qty_and_uom*`, `obr.collection_volume*`, `spm.specimen_collection_amount*`, `spm.specimen_current_quantity*`. |
| CP | Composite Price | price (MO) ^ price type (ID) ^ from value (NM) ^ to value (NM) ^ range units (CWE) ^ range type (ID) | ALL 6 components flattened via `_cp_fields()` / `_cp_schema()` (7 columns including the MO currency sub-component): `{prefix}` (price quantity, CP.1.1), `{prefix}_currency` (ISO 4217 from CP.1.2), `{prefix}_price_type`, `{prefix}_from_value`, `{prefix}_to_value`, `{prefix}_range_units` (CWE code), `{prefix}_range_type`. |
| FC | Financial Class | financial class code (CWE) ^ effective date (DTM) | ALL 2 components flattened via `_fc_fields()` / `_fc_schema()` (10 columns) — same shape as DLD. Used by `gt1.guarantor_financial_class*` (single). For repeating FC (e.g. `pv1.financial_class`), use `_fc_array_fields()` / `_fc_array_schema()` to produce `ARRAY<STRUCT<...>>`. |
| AUI | Authorization Information | authorization number (ST) ^ date (DT) ^ source (ST) | ALL 3 components flattened via `_aui_fields()` / `_aui_schema()` (3 columns): `{prefix}` (AUI.1 ST), `{prefix}_date` (AUI.2 DT, parsed to timestamp), `{prefix}_source` (AUI.3 ST). Used by `in1.authorization_information*`. |
| JCC | Job Code/Class | job code (CWE) ^ job class (CWE) ^ job description (TX) | ALL 3 components flattened via `_jcc_fields()` / `_jcc_schema()` (7 columns): job code CWE (3 cols), job class CWE (3 cols), description (1 col). Used by `nk1.job_code*`, `gt1.job_code_class*`. |
| MOC | Charge to Practice | amount (MO) ^ charge code (CWE) | ALL 2 components flattened via `_moc_fields()` / `_moc_schema()` (5 columns): `{prefix}_amount`, `{prefix}_currency`, `{prefix}_code`, `{prefix}_code_text`, `{prefix}_code_coding_system`. Used by `obr.charge_to_practice*`. |
| PRL | Parent Result Link | parent observation ID (CWE) ^ parent observation sub-ID (ST) ^ parent observation descriptor (TX) | ALL 3 components flattened via `_prl_fields()` / `_prl_schema()` (5 columns): CWE (3 cols), sub_id, descriptor. Used by `obr.parent_result*`. |
| PPN | Performing Person Time Stamp | ID ^ family ^ given ^ middle ^ suffix ^ prefix ^ degree ^ source table ^ assigning authority ^ name type ^ check digit ^ check digit scheme ^ identifier type ^ assigning facility ^ date/time action performed | Modeled as XCN-shape STRING flatten where used (rare in repo). |
| LA2 | Location with Address Variation 2 | point of care ^ room ^ bed ^ facility ^ location status ^ patient location type ^ building ^ floor ^ street ^ other ^ city ^ state ^ zip ^ country | Comp 1-3 (location), Comp 9-13 (address) |
| NDL | Name with Date and Location | name (CNN) ^ start datetime (DTM) ^ end datetime (DTM) ^ point of care (IS) ^ room (IS) ^ bed (IS) ^ facility (HD) ^ location status (IS) ^ patient location type (IS) ^ building (IS) ^ floor (IS) | ALL 11 components flattened via `_ndl_fields()` / `_ndl_schema()` (17 columns including CNN sub-components: `id`, `family_name`, `given_name`, `middle_name`, `suffix`, `prefix`, `degree`). For repeating NDL (e.g. OBR-33/34/35), use `_ndl_array_fields()` / `_ndl_array_schema()` to produce `ARRAY<STRUCT<...>>`. Used by `obr.principal_result_interpreter*`, `obr.assistant_result_interpreter`, `obr.technician`, `obr.transcriptionist`. |
| DLD | Discharge to Location & Date | discharge to location (CWE) ^ effective date (DTM) | ALL 2 components flattened via `_dld_fields()` / `_dld_schema()` (10 columns): CWE (9 cols) + DTM. Used by `pv1.discharged_to_location*`. |
| SPS | Specimen Source | specimen source ^ additives ^ specimen collection method ^ body site ^ site modifier ^ collection method modifier ^ specimen role | Expanded into 8 flat columns — see the DLN/TQ/SPS/OG/MOC sub-section above for the complete description. |

### General Mapping Rule

All HL7 fields are stored as **STRING** in the Delta tables. The connector extracts **all** components from composite types (as documented above) and stores them as individual string columns. No type coercion is performed at ingestion time — downstream SQL queries can cast as needed (e.g. `CAST(date_of_birth AS DATE)`).

When a component is itself a composite type (e.g. HD inside CX.4, XON.6, XON.8), the connector extracts sub-components using `get_sub_component` / `get_rep_sub_component` and stores each sub-component as its own column. The raw component value (containing `&` separators) is also preserved for convenience.

---

## **Sources and References**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://docs.cloud.google.com/healthcare-api/docs/concepts/hl7v2 | 2026-03-29 | High | HL7v2 store/message concepts, sendTime, messageType, patientIds |
| Official Docs | https://docs.cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.hl7V2Stores.messages/list | 2026-03-29 | High | List API params, view=FULL, pageSize max 1000, filter syntax, response schema |
| Official Docs | https://docs.cloud.google.com/healthcare-api/docs/how-tos/hl7v2-messages | 2026-03-29 | High | How-to: listing, getting, filtering, pagination with pageToken, auth |
| Official Docs | https://docs.cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.hl7V2Stores.messages/get | 2026-03-29 | High | GET single message API, view parameter |
| User-provided documentation | https://hl7-definition.caristix.com/v2/ | 2026-03-27 | Highest | Primary reference for all segment definitions |
| Caristix HL7 Definition API — MSH | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/MSH | 2026-03-27 | High | MSH segment: 21 v2.5.1 fields; schema extended to 28 fields for v2.6/v2.7 additions (MSH.22–28). data types, usage, max lengths |
| Caristix HL7 Definition API — EVN | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/EVN | 2026-03-27 | High | EVN segment: 7 fields, data types, usage |
| Caristix HL7 Definition API — PID | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/PID | 2026-03-27 | High | PID segment: 39 v2.5.1 fields; schema extended to 40 fields for PID.40 (v2.7). DLN expansion at PID.20. data types, usage |
| Caristix HL7 Definition API — PD1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/PD1 | 2026-03-27 | High | PD1 segment: 21 v2.5.1 fields; schema extended to 23 fields for PD1.22–23 (v2.8/v2.9). data types, usage |
| Caristix HL7 Definition API — PV1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/PV1 | 2026-03-27 | High | PV1 segment: 52 v2.5.1 fields; schema extended to 54 fields for PV1.53–54 (v2.8/v2.9). data types, usage |
| Caristix HL7 Definition API — PV2 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/PV2 | 2026-03-27 | High | PV2 segment: 49 v2.5.1 fields; schema extended to 50 fields for PV2.50 (v2.9). data types, usage |
| Caristix HL7 Definition API — NK1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/NK1 | 2026-03-27 | High | NK1 segment: 39 v2.5.1 fields; schema extended to 41 fields for NK1.40–41 (v2.7); NK1.38 `birth_place` (was `nk_birth_place`). data types, usage |
| Caristix HL7 Definition API — MRG | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/MRG | 2026-03-27 | High | MRG segment: 7 fields, data types, usage |
| Caristix HL7 Definition API — AL1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/AL1 | 2026-03-27 | High | AL1 segment: 6 fields, data types, usage |
| Caristix HL7 Definition API — IAM | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/IAM | 2026-03-27 | High | IAM segment: 20 v2.5.1 fields; schema extended to 30 fields for IAM.21–30 (v2.6). data types, usage |
| Caristix HL7 Definition API — DG1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/DG1 | 2026-03-27 | High | DG1 segment: 21 v2.5.1 fields; schema extended to 26 fields for DG1.22–26 (v2.7). data types, usage |
| Caristix HL7 Definition API — PR1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/PR1 | 2026-03-27 | High | PR1 segment: 20 v2.5.1 fields; schema extended to 25 fields for PR1.21–25 (v2.6/v2.7). data types, usage |
| Caristix HL7 Definition API — ORC | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/ORC | 2026-03-27 | High | ORC segment: 31 v2.5.1 fields; schema extended to 38 fields for ORC.32–38 (v2.6/v2.9). TQ expansion at ORC.7, EIP expansion at ORC.8. data types, usage |
| Caristix HL7 Definition API — OBR | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/OBR | 2026-03-27 | High | OBR segment: 50 v2.5.1 fields; schema extended to 55 fields for OBR.51–55 (v2.8/v2.9). SPS expansion at OBR.15, MOC expansion at OBR.23, TQ expansion at OBR.27, EIP expansion at OBR.29. data types, usage |
| Caristix HL7 Definition API — OBX | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/OBX | 2026-03-27 | High | OBX segment: 25 v2.5.1 fields; schema extended to 33 fields for OBX.26–33 (v2.8/v2.9). OG expansion at OBX.4. data types, usage |
| Caristix HL7 Definition API — NTE | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/NTE | 2026-03-27 | High | NTE segment: 4 v2.5.1 fields; schema extended to 9 fields for NTE.5–9 (v2.6/v2.9). data types, usage |
| Caristix HL7 Definition API — SPM | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/SPM | 2026-03-27 | High | SPM segment: 29 v2.5.1 fields; schema extended to 35 fields for SPM.30–35 (v2.6/v2.9). EIP expansion at SPM.2 and SPM.3. data types, usage |
| Caristix HL7 Definition API — IN1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/IN1 | 2026-03-27 | High | IN1 segment: 53 v2.5.1 fields; schema extended to 55 fields for IN1.54–55 (v2.8/v2.9). data types, usage |
| Caristix HL7 Definition API — GT1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/GT1 | 2026-03-27 | High | GT1 segment: 57 fields (no v2.9 additions). data types, usage |
| Caristix HL7 Definition API — FT1 | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/FT1 | 2026-03-27 | High | FT1 segment: 31 v2.5.1 fields; schema extended to 56 fields for FT1.32–56 (v2.6/v2.9). data types, usage |
| Caristix HL7 Definition API — RXA | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/RXA | 2026-03-27 | High | RXA segment: 26 v2.5.1 fields; schema extended to 29 fields for RXA.27–29 (v2.6/v2.9). data types, usage |
| Caristix HL7 Definition API — SCH | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/SCH | 2026-03-27 | High | SCH segment: 27 v2.5.1 fields; schema extended to 28 fields for SCH.28 (v2.9, EIP expansion). data types, usage |
| Caristix HL7 Definition API — TXA | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments/TXA | 2026-03-27 | High | TXA segment: 23 v2.5.1 fields; schema extended to 28 fields for TXA.24–28 (v2.6/v2.8/v2.9). data types, usage |
| Caristix HL7 Definition API — Segments List | https://hl7-definition.caristix.com/v2-api/1/HL7v2.5.1/Segments | 2026-03-27 | High | Complete list of all HL7 v2.5.1 segments — used to select the 23 most clinically relevant |

### Usage Legend

| Code | Meaning |
|---|---|
| R | Required |
| O | Optional |
| C | Conditional — required under certain conditions |
| B | Backward compatible — deprecated, retained for compatibility |
