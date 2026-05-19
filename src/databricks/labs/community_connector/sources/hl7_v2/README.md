# Lakeflow HL7 v2 Community Connector

The Lakeflow HL7 v2 Connector parses HL7 v2 pipe-delimited messages and loads each segment type into its own Delta table, enabling structured analytics over clinical data. The connector supports incremental append-only ingestion, automatically tracking new messages by their send time.

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
| CE, CWE, CNE | Coded elements | STRING (or `ARRAY<STRUCT>` if repeating) | All 6 components: `code`, `text`, `coding_system`, `alt_code`, `alt_text`, `alt_coding_system` |
| XPN | Person name | STRING (or `ARRAY<STRUCT>` if repeating) | ALL 14 active components via `_xpn_fields()` helper: `family_name` (1), `given_name` (2), `middle_name` (3), `suffix` (4), `prefix` (5), `degree` (6), `name_type_code` (7), `name_representation_code` (8), `name_context` (9), `name_assembly_order` (11), `name_effective_date` (12), `name_expiration_date` (13), `professional_suffix` (14), `called_by` (15). |
| XAD | Address | STRING | Components: `street`, `other_designation`, `city`, `state`, `zip`, `country`, `type` |
| XCN | Composite ID/name | STRING | Components: `id`, `family_name`, `given_name`, `prefix` |
| XON | Organization name | STRING | All 10 components: `name`, `type_code`, `id`, `check_digit`, `check_digit_scheme`, `assigning_authority`, `id_type_code`, `assigning_facility`, `name_rep_code`, `identifier` |
| HD | Hierarchic designator | STRING | All 3 components: `namespace_id`, `universal_id`, `universal_id_type` |
| EI | Entity identifier | STRING (or `ARRAY<STRUCT>` if repeating) | All 4 components: `entity_id`, `namespace_id`, `universal_id`, `universal_id_type` |
| CX | Extended composite ID | STRING | Key components: `id_value`, `check_digit`, `assigning_authority`, `type_code` |
| PL | Person location | STRING | Components: `point_of_care`, `room`, `bed`, `facility`, `status`, `type` |

**Key rules for composite type extraction:**
- Every component of a composite type gets its own column — do not extract only component 1 and discard the rest.
- **Cardinality-`1` fields** (separated by `~` only if the sender erroneously sends multiple) are flattened into individual `STRING` columns via `_xpn_fields()` / `get_rep_component`. Only the first repetition is captured for these; the full raw value is preserved in `raw_segment`.
- **Cardinality-`*` fields** (truly repeating per HL7 spec — e.g. `patient_names`, `interpretation_codes`, `character_set`, `message_profile_identifiers`) are stored as Spark `ARRAY` columns with one entry per repetition via the `_xpn_array_fields()` / `_cwe_array_fields()` / `_ei_array_fields()` helpers. See the "Non-string column types" subsection below for the full enumeration of 24 array columns.
- When a component is itself a composite type (e.g., HD inside CX.4 or XON.6), its sub-components (separated by `&`) are also extracted into individual columns using `get_sub_component`. The raw component value is preserved as well.

**Non-string column types:**

- `set_id` is stored as `BIGINT`.
- Datetime fields parsed from HL7 DTM format are stored as `TIMESTAMP`.
- Truly-repeating HL7 fields (cardinality `*`, separated by `~`) are stored as Spark `ARRAY` columns rather than collapsed to first-repetition strings. There are 24 such columns across 11 tables (`pid`, `nk1`, `gt1`, `in1`, `mrg`, `msh`, `pd1`, `pv1`, `pv2`, `obr`, `obx`). Three element shapes:
  - `ARRAY<STRUCT<...>>` of **XPN (person name)** structs — one entry per repetition of person-name fields, e.g. `pid.patient_names`, `pid.mothers_maiden_names`, `nk1.contact_persons`, `gt1.guarantor_names`, `mrg.prior_patient_names`. The struct has 14 fields (`family_name`, `given_name`, `middle_name`, `suffix`, `prefix`, `degree`, `name_type_code`, `name_representation_code`, `name_context`, `name_assembly_order`, `name_effective_date`, `name_expiration_date`, `professional_suffix`, `called_by`).
  - `ARRAY<STRUCT<...>>` of **CWE / EI** structs — one entry per repetition of coded-element or entity-identifier fields, e.g. `obx.interpretation_codes`, `obr.reason_for_study`, `pv1.ambulatory_status`, `pv2.visit_user_code`, `pv2.notify_clergy_code`, `msh.security_handling_instructions`, `msh.special_access_restriction`, `pd1.living_dependency`, `msh.message_profile_identifiers`.
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

**Delta mode:**
- **Table not found** — Verify `delta_table_name` is fully-qualified (e.g., `catalog.schema.table`) with `SELECT` permissions.
- **No data** — Confirm `createTime` values exist and `data` starts with `MSH|`.
- **Parse errors** — Ensure `data` is raw pipe-delimited text, not base64. Decode if needed: `UPDATE t SET data = cast(unbase64(data) as STRING)`.
- **Out of memory** — Set `delta_query_mode` to `per_window` in the connection for large tables.

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

Both GCP and Delta modes use the same source code and Git repo — the only difference is which connection (and credentials) the pipeline uses.

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

### Creating Pipeline #2: Delta Mode

#### Step 1 — Add the Custom Connector (if not already added)

If you already added the custom connector during the GCP setup, it persists — you don't need to add it again. Just create a new ingestion pipeline:

1. **Jobs & Pipelines** > **Create new** > **Ingestion pipeline**.
2. Find **hl7_v2** in the Community connectors list and click on it.

If you haven't added it yet, follow Step 1 from the GCP section above (same source name and Git repo URL).

#### Step 2 — Create the Delta Connection

1. Under "Connection to the source", click **Create connection**.
2. Name it, e.g. `hl7_v2_delta_connection`.
3. Fill in the parameters (see [Delta Mode](#delta-mode) above for where to find each value):

| Parameter | Value to enter |
|---|---|
| `source_type` | `delta` (**required** — this switches the connector to Delta mode) |
| `delta_table_name` | Fully-qualified 3-level name (e.g. `my_catalog.bronze.hl7_raw`) |
| `databricks_host` | Your workspace URL (e.g. `https://my-workspace.cloud.databricks.com`) |
| `databricks_token` | A Databricks personal access token (starts with `dapi...`) |
| `sql_warehouse_id` | SQL warehouse ID (e.g. `01370556fad60fda`) |

> If the UI shows only generic key-value pairs, also add `externalOptionsAllowList` = `segment_type,window_seconds,start_timestamp,max_records_per_batch`.

4. Click **Next** to create the connection.

#### Step 3 — Configure Ingestion Setup

1. **Pipeline name**: e.g. `hl7_v2_delta_pipeline`.
2. **Event log location**:
   - **Catalog**: Select your catalog.
   - **Schema**: e.g. `hl7_delta`.
3. **Root path**: Workspace path for pipeline assets.
   - Example: `/Users/your.email@company.com/hl7_v2_delta_pipeline`
4. Click **Create**.

#### Step 4 — Configure Pipeline Source Code

1. Click **Open in Editor**.
2. Replace the auto-generated code with:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

source_name = "hl7_v2"
connection_name = "hl7_v2_delta_connection"

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

#### Step 5 — Run the Pipeline

Click **Start** on the pipeline page.

### Key Points

- **Both pipelines use the same Custom Connector** (same Git repo and source name `hl7_v2`). The connection credentials determine GCP vs Delta mode.
- **The Custom Connector only needs to be added once** — after that, `hl7_v2` stays in your Community connectors list for future pipelines.
- **The `ingest.py` replacement is critical** — the auto-generated skeleton won't work without your pipeline spec.

## Sample Data Attribution

The sample messages under `tests/unit/sources/hl7_v2/samples/` include messages adapted from [CDCgov/lib-hl7v2-bumblebee](https://github.com/CDCgov/lib-hl7v2-bumblebee) (Apache License 2.0). Additional synthetic messages were created for segment types not present in the CDC corpus.

## References

- [Google Cloud Healthcare API — HL7v2 Concepts](https://cloud.google.com/healthcare-api/docs/concepts/hl7v2)
- [HL7 Version 2.9 Specification](https://www.hl7.eu/HL7v2x/v29/hl7v29.htm)
- [Databricks Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
