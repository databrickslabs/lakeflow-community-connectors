# Lakeflow HL7 v2 Community Connector

The Lakeflow HL7 v2 Connector parses HL7 v2 pipe-delimited messages and loads each segment type into its own Delta table, enabling structured analytics over clinical data. The connector supports incremental append-only ingestion, automatically tracking new messages by their send time.

> **Getting started?** See the [UI Setup Guide](UI_SETUP_GUIDE.md) for step-by-step pipeline creation instructions.

## Supported Source Modes

| Mode | `source_type` value | Description |
|---|---|---|
| **GCP Healthcare API** | `gcp` (default) | Fetches HL7 v2 messages from a Google Cloud Healthcare API HL7v2 store via REST |
| **Delta Table** | `delta` | Reads pre-loaded HL7 v2 messages from a Bronze Delta table in Databricks |

## Prerequisites

**GCP Mode** — A GCP project with the Cloud Healthcare API enabled, an HL7v2 store containing messages, and a service account key with `healthcare.hl7V2Messages.list` and `healthcare.hl7V2Messages.get` permissions.

**Delta Mode** — A Delta table in Unity Catalog containing raw HL7 v2 messages (see [Delta Table Schema](#delta-table-schema) below).

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

### Delta Mode

| Parameter | Required | Description | Example |
|---|---|---|---|
| `source_type` | Yes | Must be `delta` | `delta` |
| `delta_table_name` | Yes | Fully-qualified Delta table name | `my_catalog.bronze.hl7_raw` |
| `databricks_host` | Yes | Databricks workspace URL | `https://my-workspace.cloud.databricks.com` |
| `databricks_token` | Yes | Personal access token or service principal token | `dapi...` |
| `sql_warehouse_id` | Yes | SQL warehouse ID for querying the table | `01370556fad60fda` |
| `delta_query_mode` | No | `preload` (default) loads the table into memory at init. `per_window` queries per micro-batch — use for large tables. | `per_window` |

> **Why are Databricks credentials needed?** The Spark Python Data Source API runs connector code in a subprocess without SparkSession. The connector queries the Delta table via the SQL Statement Execution REST API, which requires explicit credentials.

### Delta Table Schema

Your Bronze Delta table must have these columns:

| Column | Type | Required | Description |
|---|---|---|---|
| `data` | STRING | Yes | Raw HL7 v2 message text (pipe-delimited, e.g., `MSH\|^~\\&\|...`) |
| `createTime` | STRING or TIMESTAMP | Yes | Timestamp used as the incremental cursor (e.g., `2024-01-15T10:30:00Z`) |
| `name` | STRING | No | Source identifier for traceability (stored in `source_file` column) |

**Example: Creating the Bronze table from Volume files**

```sql
CREATE TABLE my_catalog.my_schema.hl7_raw AS
SELECT
  value                            AS data,
  _metadata.file_modification_time AS createTime,
  _metadata.file_name              AS name
FROM read_files(
  '/Volumes/my_catalog/my_schema/hl7_volume/',
  format => 'text',
  wholeText => true
);
```

> **Important:** `wholeText => true` is required. Without it, `read_files` splits each line into a separate row. HL7 messages are multi-line (one line per segment), so the entire file must be read as a single row for the connector to parse all segments.

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

Z-segments produce a generic schema with columns `field_1` through `field_25` plus standard metadata columns.

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
| `source_file` | Source identifier — GCP API resource name or Delta `name` column value |
| `send_time` | Original HL7 message send time from the API in RFC 3339 format |
| `create_time` | GCP `createTime` (when the message was ingested); used as the incremental cursor |
| `raw_segment` | Original unparsed HL7 segment text |

### Incremental Ingestion

All tables use **append-only** ingestion. HL7 v2 is a messaging protocol — every message is an immutable event. The connector tracks progress via `createTime` using a sliding time window, advancing the cursor after each batch. Messages already processed are never re-fetched.

## Data Type Mapping

All HL7 v2 fields are stored as `STRING`. The connector extracts **all** components from composite types into individual columns. Downstream SQL can cast as needed.

| HL7 Data Type | Examples | Databricks Type | Extraction pattern |
|---|---|---|---|
| ST, TX, FT, IS, ID | Plain text, coded values | STRING | Stored as-is |
| NM, SI | Numeric strings | STRING | Cast with `CAST(col AS INT)` as needed |
| TS, DT, TM | Timestamps, dates, times | STRING | HL7 DTM format; datetime fields like `date_of_birth` are parsed to `TIMESTAMP` |
| CE, CWE, CNE | Coded elements | STRING | All 6 components: `code`, `text`, `coding_system`, `alt_code`, `alt_text`, `alt_coding_system` |
| XPN | Person name | STRING | ALL 14 active components via `_xpn_fields()` helper: `family_name` (1), `given_name` (2), `middle_name` (3), `suffix` (4), `prefix` (5), `degree` (6), `name_type_code` (7), `name_representation_code` (8), `name_context` (9), `name_assembly_order` (11), `name_effective_date` (12), `name_expiration_date` (13), `professional_suffix` (14), `called_by` (15). |
| XAD | Address | STRING | Components: `street`, `other_designation`, `city`, `state`, `zip`, `country`, `type` |
| XCN | Composite ID/name | STRING | Components: `id`, `family_name`, `given_name`, `prefix` |
| XON | Organization name | STRING | All 10 components: `name`, `type_code`, `id`, `check_digit`, `check_digit_scheme`, `assigning_authority`, `id_type_code`, `assigning_facility`, `name_rep_code`, `identifier` |
| HD | Hierarchic designator | STRING | All 3 components: `namespace_id`, `universal_id`, `universal_id_type` |
| EI | Entity identifier | STRING | All 4 components: `entity_id`, `namespace_id`, `universal_id`, `universal_id_type` |
| CX | Extended composite ID | STRING | Key components: `id_value`, `check_digit`, `assigning_authority`, `type_code` |
| PL | Person location | STRING | Components: `point_of_care`, `room`, `bed`, `facility`, `status`, `type` |

**Key rules for composite type extraction:**
- Every component of a composite type gets its own column — do not extract only component 1 and discard the rest.
- For repeating fields (separated by `~`), use `get_rep_component` (not `get_component`) to avoid the repetition separator bleeding into component values. Only the first repetition is captured; the full value is in `raw_segment`.
- When a component is itself a composite type (e.g., HD inside CX.4 or XON.6), its sub-components (separated by `&`) are also extracted into individual columns using `get_sub_component`. The raw component value is preserved as well.

`set_id` is stored as `BIGINT`. Datetime fields parsed from HL7 DTM format are stored as `TIMESTAMP`.

## Troubleshooting

**Both modes:**
- **Missing segments** — Not all messages contain all segment types. ADT messages typically include MSH, EVN, PID, PV1 but not OBR/OBX. ORU messages include ORC, OBR, OBX but may omit EVN, AL1, DG1.
- **Slow ingestion** — Reduce `window_seconds` to process smaller batches.

**GCP mode:**
- **Auth errors** — Verify `service_account_json` is the complete JSON key and the service account has Healthcare API permissions.
- **No data** — Check that `location`, `dataset_id`, and `hl7v2_store_id` are correct in the GCP Console.
- **Quota limits** — Check the [GCP quota console](https://console.cloud.google.com/iam-admin/quotas).

**Delta mode:**
- **Table not found** — Verify `delta_table_name` is fully-qualified (e.g., `catalog.schema.table`) with `SELECT` permissions.
- **No data** — Confirm `createTime` values exist and `data` starts with `MSH|`.
- **Parse errors** — Ensure `data` is raw pipe-delimited text, not base64. Decode if needed: `UPDATE t SET data = cast(unbase64(data) as STRING)`.
- **Out of memory** — Set `delta_query_mode` to `per_window` in the connection for large tables.

## Sample Data Attribution

The sample messages under `tests/unit/sources/hl7_v2/samples/` include messages adapted from [CDCgov/lib-hl7v2-bumblebee](https://github.com/CDCgov/lib-hl7v2-bumblebee) (Apache License 2.0). Additional synthetic messages were created for segment types not present in the CDC corpus.

## References

- [Google Cloud Healthcare API — HL7v2 Concepts](https://cloud.google.com/healthcare-api/docs/concepts/hl7v2)
- [HL7 Version 2.9 Specification](https://www.hl7.eu/HL7v2x/v29/hl7v29.htm)
- [Databricks Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
