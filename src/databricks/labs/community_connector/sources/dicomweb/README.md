# Lakeflow DICOMweb Community Connector

This documentation provides setup instructions and reference information for the DICOMweb source connector, which ingests DICOM study, series, and instance metadata from any DICOMweb-compliant VNA or PACS system into Databricks Delta tables. The connector also exposes a `diagnostics` table that probes server capabilities on every pipeline run.

## Prerequisites

- A **DICOMweb-compliant VNA or PACS system** (e.g., Orthanc, dcm4chee, Google Cloud Healthcare API, Azure Health Data Services, AWS HealthImaging, Sectra, Agfa Enterprise Imaging)
- **Network access** from your Databricks cluster to the DICOMweb endpoint
- **Credentials** if your system requires authentication:
  - HTTP Basic Auth: a username and password
  - Bearer token: a pre-obtained access token (e.g., from Azure AD, Google IAM, or Keycloak)
- (Optional) A **Unity Catalog Volume** if you want to download raw DICOM files (`.dcm`) or image frames (`.jpg`) via WADO-RS

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters when creating your Unity Catalog connection:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `base_url` | string | Yes | Base URL of the DICOMweb endpoint, including any path prefix. | `https://pacs.example.com/dicom-web` |
| `auth_type` | string | No | Authentication method. One of: `none`, `basic`, `bearer`. Defaults to `none`. | `basic` |
| `username` | string | Conditional | Username for HTTP Basic authentication. Required when `auth_type` is `basic`. | `svc-dicom` |
| `password` | string | Conditional | Password for HTTP Basic authentication. Required when `auth_type` is `basic`. Store as a Databricks Secret. | `secret('scope','dicom-password')` |
| `token` | string | Conditional | Bearer token for token-based authentication. Required when `auth_type` is `bearer`. Store as a Databricks Secret. | `secret('scope','dicom-token')` |
| `connection_name` | string | No | Human-readable name injected as the `connection_name` column in every row across all tables. Defaults to `base_url` when not set. Set this when ingesting from multiple DICOMweb sources into shared Delta tables to trace records back to their origin. | `orthanc-prod` |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector requires table-specific options, so this parameter must be set. | See full list below |

This connector supports the following table-specific options via `externalOptionsAllowList`:
`fetch_dicom_files,dicom_volume_path,lookback_days,page_size,max_records_per_batch,fetch_metadata,wado_mode`

> **Note**: Table-specific options such as `lookback_days`, `page_size`, or `fetch_dicom_files` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them at runtime.

### Obtaining Your Endpoint URL and Credentials

**Orthanc (self-hosted):**
1. Open the Orthanc Explorer UI (usually at `http://host:8042`).
2. The DICOMweb base URL is typically `http://host:8042/dicom-web`.
3. If REST API authentication is enabled, use `auth_type=basic` with the configured Orthanc credentials.

**dcm4chee (self-hosted):**
1. The DICOMweb base URL is typically `https://host:8443/dcm4chee-arc/aets/DCM4CHEE/rs`.
2. dcm4chee uses Keycloak for authentication. Obtain a service account token from your Keycloak admin and use `auth_type=bearer`.

**Google Cloud Healthcare API:**
1. The DICOMweb base URL follows the pattern: `https://healthcare.googleapis.com/v1/projects/{project}/locations/{location}/datasets/{dataset}/dicomStores/{store}/dicomWeb`.
2. Use `auth_type=bearer` with a Google Cloud access token. Token refresh must be handled externally (tokens typically expire after 1 hour).

**Azure Health Data Services:**
1. The DICOMweb base URL follows the pattern: `https://{workspace}-{service}.dicom.azurehealthcareapis.com/v2`.
2. Use `auth_type=bearer` with an Azure AD access token.

**Commercial VNA/PACS (Sectra, Agfa, Philips, etc.):**
1. Contact your PACS administrator for the DICOMweb (QIDO-RS/WADO-RS) endpoint URL.
2. Request a service account with read-only access to studies, series, and instances.
3. Confirm that the endpoint supports `StudyDate` as a QIDO-RS filter parameter.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `fetch_dicom_files,dicom_volume_path,lookback_days,page_size,max_records_per_batch,fetch_metadata,wado_mode` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API, for example:

```sql
CREATE CONNECTION `my-dicomweb-connection`
TYPE dicomweb
OPTIONS (
  base_url                 'https://your-pacs.example.com/dicom-web',
  auth_type                'none',
  connection_name          'my-pacs-prod',
  sourceName               'dicomweb',
  externalOptionsAllowList 'fetch_dicom_files,dicom_volume_path,lookback_days,page_size,max_records_per_batch,fetch_metadata,wado_mode'
  -- For Basic auth, add:
  -- auth_type 'basic',
  -- username  'svc-dicom',
  -- password  secret('my-scope', 'dicom-password')
  -- For Bearer token, add:
  -- auth_type 'bearer',
  -- token     secret('my-scope', 'dicom-token')
);
```

> **Important:** `externalOptionsAllowList` must include every option you intend to pass via `table_configuration` in the pipeline spec. Any option not listed here will be rejected at pipeline runtime with an *"Option X is not allowed ... and cannot be provided externally"* error.

## Supported Objects

The DICOMweb connector exposes a **static list** of four tables corresponding to the three levels of the DICOM information hierarchy plus a diagnostics table:

- `studies`
- `series`
- `instances`
- `diagnostics`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Cursor Field |
|-------|-------------|----------------|-------------|--------------|
| `studies` | Study-level metadata. One row per DICOM study (patient imaging encounter). | `cdc` | `StudyInstanceUID` | `StudyDate` |
| `series` | Series-level metadata. One row per image series within a study. | `cdc` | `SeriesInstanceUID` | `StudyDate` |
| `instances` | Instance (SOP) metadata. One row per individual DICOM file. | `cdc` | `SOPInstanceUID` | `StudyDate` |
| `diagnostics` | Capability probe results. One row per tested DICOMweb endpoint. | `cdc` | `endpoint` | `probe_timestamp` |

**Incremental sync strategy:** The `studies`, `series`, and `instances` tables use a `StudyDate`-based cursor. On each pipeline run, the connector queries for studies within a date range starting from the last known cursor (minus `lookback_days` for late-arriving records) through today. The `diagnostics` table re-probes all endpoints on every trigger.

**Delete handling:** DICOMweb (QIDO-RS) does not provide a delete-notification mechanism. Deleted studies, series, or instances will remain in the Delta table unless removed by a separate out-of-band process. The connector performs upserts (CDC) only.

### `studies` schema

| Column | Type | Description |
|--------|------|-------------|
| `StudyInstanceUID` | STRING (PK) | Globally unique study identifier |
| `PatientID` | STRING | Patient identifier in the PACS |
| `PatientName` | STRING | Patient name (Alphabetic component) |
| `StudyDate` | STRING | Study date in `YYYYMMDD` format (cursor field) |
| `StudyTime` | STRING | Study time in `HHMMSS.ffffff` format |
| `AccessionNumber` | STRING | Accession number assigned by radiology |
| `StudyDescription` | STRING | Free-text description of the study |
| `ModalitiesInStudy` | ARRAY\<STRING\> | Modalities present in the study (e.g., `["CT", "SR"]`) |
| `NumberOfStudyRelatedSeries` | BIGINT | Number of series in this study |
| `NumberOfStudyRelatedInstances` | BIGINT | Total number of DICOM instances in this study |
| `connection_name` | STRING | Lineage identifier (defaults to `base_url`) |

### `series` schema

| Column | Type | Description |
|--------|------|-------------|
| `SeriesInstanceUID` | STRING (PK) | Globally unique series identifier |
| `StudyInstanceUID` | STRING | Parent study UID |
| `StudyDate` | STRING | Study date in `YYYYMMDD` format (cursor field) |
| `SeriesNumber` | BIGINT | Series number within the study |
| `SeriesDescription` | STRING | Free-text description of the series |
| `Modality` | STRING | Imaging modality (CT, MR, US, CR, DX, etc.) |
| `BodyPartExamined` | STRING | Body part examined (CHEST, HEAD, etc.) |
| `SeriesDate` | STRING | Series acquisition date in `YYYYMMDD` format |
| `connection_name` | STRING | Lineage identifier (defaults to `base_url`) |

### `instances` schema

| Column | Type | Description |
|--------|------|-------------|
| `SOPInstanceUID` | STRING (PK) | Globally unique SOP instance identifier |
| `SeriesInstanceUID` | STRING | Parent series UID |
| `StudyInstanceUID` | STRING | Parent study UID |
| `SOPClassUID` | STRING | SOP Class UID (identifies DICOM IOD type, e.g., CT Image Storage) |
| `InstanceNumber` | BIGINT | Instance number within the series |
| `StudyDate` | STRING | Study date in `YYYYMMDD` format (cursor field) |
| `ContentDate` | STRING | Content creation date in `YYYYMMDD` format |
| `ContentTime` | STRING | Content creation time in `HHMMSS.ffffff` format |
| `dicom_file_path` | STRING | Path to `.dcm` or `.jpg` file in a Unity Catalog Volume (populated when `fetch_dicom_files` is enabled) |
| `metadata` | VARIANT | Full DICOM JSON for this instance (populated when `fetch_metadata` is enabled) |
| `connection_name` | STRING | Lineage identifier (defaults to `base_url`) |

**Special columns:**

- `dicom_file_path`: Populated only when the `fetch_dicom_files` table option is set to `true`. Files are written to the path specified by `dicom_volume_path`, organized as `{volume_path}/{StudyInstanceUID}/{SeriesInstanceUID}/{SOPInstanceUID}.dcm` (or `.jpg` for frame-based retrieval). If the download fails for a given instance, this field is set to `NULL` and the pipeline continues without interruption.

- `metadata`: Populated only when the `fetch_metadata` table option is set to `true`. Contains the complete DICOM JSON tag set for the instance as a VARIANT type. You can query individual DICOM tags using Databricks' semi-structured data access syntax:
  ```sql
  -- Extract the Modality tag from full metadata
  SELECT SOPInstanceUID, metadata:00080060.Value[0] AS Modality FROM instances;

  -- Filter for all CT instances
  SELECT * FROM instances WHERE metadata:00080060.Value[0] = 'CT';
  ```

### `diagnostics` schema

| Column | Type | Description |
|--------|------|-------------|
| `endpoint` | STRING (PK) | URL path pattern probed (e.g., `/studies/{uid}/series/{uid}/instances/{uid}`) |
| `category` | STRING | Service category: `QIDO-RS` or `WADO-RS` |
| `description` | STRING | Human-readable description of what the endpoint does |
| `supported` | STRING | Result: `yes`, `no`, `partial`, `unknown`, or `error` |
| `status_code` | BIGINT | HTTP status code from the probe request (null on network error) |
| `content_type` | STRING | Content-Type header from the response |
| `latency_ms` | BIGINT | Round-trip latency in milliseconds |
| `notes` | STRING | Additional context (error message, content-type detail, etc.) |
| `probe_timestamp` | STRING | ISO-8601 UTC timestamp of the probe run |
| `connection_name` | STRING | Lineage identifier (defaults to `base_url`) |

The `diagnostics` table probes every standard DICOMweb endpoint on your server and reports whether it is supported. This is useful for initial connectivity validation and ongoing health monitoring.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

The following options control how the connector reads data from the DICOMweb server. They are passed per-table in the `table_configuration` section.

| Option | Applies To | Required | Default | Description |
|--------|-----------|----------|---------|-------------|
| `page_size` | studies, series, instances | No | `100` | Number of records per QIDO-RS request. Increase for large PACS systems (e.g., `500` or `1000` if the server supports it). |
| `max_records_per_batch` | studies, series, instances | No | `500` | Maximum number of records to return per micro-batch. When this limit is reached mid-scan, the connector saves its position and resumes from that point on the next micro-batch. |
| `lookback_days` | studies, series, instances | No | `1` | Number of days to subtract from the cursor date on each run. This overlap window catches late-arriving or backdated studies that might otherwise be missed. |
| `fetch_dicom_files` | instances | No | `false` | When set to `true`, downloads each DICOM file (or image frame) via WADO-RS and writes it to the path specified by `dicom_volume_path`. |
| `dicom_volume_path` | instances | Conditional | -- | Unity Catalog Volume path where downloaded DICOM files are stored. Required when `fetch_dicom_files` is `true`. Example: `/Volumes/catalog/schema/dicom_files`. |
| `wado_mode` | instances | No | `auto` | Controls how DICOM files are retrieved. `auto`: tries full `.dcm` retrieval first, falls back to frame retrieval on HTTP 404/406/415. `full`: always retrieves the complete `.dcm` file. `frames`: always retrieves the first image frame as `.jpg`. Use `frames` for servers that only support frame-level retrieval (e.g., Static DICOMweb / S3 Static WADO deployments). |
| `fetch_metadata` | instances | No | `false` | When set to `true`, fetches the full DICOM JSON metadata for each instance via the WADO-RS metadata endpoint and stores it in the `metadata` column as VARIANT. |

## Data Type Mapping

The connector maps DICOM Value Representations (VR) to Databricks types as follows:

| DICOM VR | VR Name | Databricks Type | Notes |
|----------|---------|-----------------|-------|
| `UI` | Unique Identifier | STRING | DICOM UID in dot-notation (e.g., `1.2.840.10008.5.1.4.1.1.2`) |
| `DA` | Date | STRING | Format `YYYYMMDD`. Convert with `to_date(col, 'yyyyMMdd')` if needed. |
| `TM` | Time | STRING | Format `HHMMSS.ffffff` |
| `LO` | Long String | STRING | Max 64 characters |
| `SH` | Short String | STRING | Max 16 characters |
| `PN` | Person Name | STRING | Alphabetic component extracted from the DICOM JSON PersonName object |
| `CS` | Code String | STRING or ARRAY\<STRING\> | Single-valued CS fields are stored as STRING. Multi-valued CS fields (e.g., `ModalitiesInStudy`) are stored as ARRAY\<STRING\>. |
| `IS` | Integer String | BIGINT | Parsed from string representation |
| `DS` | Decimal String | STRING | Stored as string to preserve precision |
| `SQ` | Sequence of Items | VARIANT | Complex nested sequences are available in the `metadata` column when `fetch_metadata` is enabled |
| `OB`/`OW` | Binary Data | -- | Pixel data is not included in metadata tables. Use `fetch_dicom_files` to download binary content separately. |

**Important notes:**
- All date/time fields are stored as raw DICOM strings (`YYYYMMDD`, `HHMMSS.ffffff`). Convert in SQL with `to_date(StudyDate, 'yyyyMMdd')` as needed.
- Person names follow the DICOM convention `FamilyName^GivenName^MiddleName^NamePrefix^NameSuffix`. Only the Alphabetic component is extracted.
- Tags absent from the QIDO-RS response are stored as `NULL`.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` in your main pipeline file (e.g., `ingest.py`) with your connection name, destination catalog/schema, and table-specific options.

Example configuration ingesting all four tables:

```json
{
  "pipeline_spec": {
    "connection_name": "my-dicomweb-connection",
    "object": [
      {
        "table": {
          "source_table": "studies",
          "destination_catalog": "main",
          "destination_schema": "dicom_bronze",
          "table_configuration": {
            "lookback_days": "1",
            "page_size": "200"
          }
        }
      },
      {
        "table": {
          "source_table": "series",
          "destination_catalog": "main",
          "destination_schema": "dicom_bronze",
          "table_configuration": {
            "lookback_days": "1",
            "page_size": "200"
          }
        }
      },
      {
        "table": {
          "source_table": "instances",
          "destination_catalog": "main",
          "destination_schema": "dicom_bronze",
          "table_configuration": {
            "lookback_days": "1",
            "page_size": "200",
            "fetch_metadata": "true",
            "fetch_dicom_files": "true",
            "dicom_volume_path": "/Volumes/main/dicom_bronze/dicom_files",
            "wado_mode": "auto"
          }
        }
      },
      {
        "table": {
          "source_table": "diagnostics",
          "destination_catalog": "main",
          "destination_schema": "dicom_bronze"
        }
      }
    ]
  }
}
```

**Configuration notes:**
- `source_table` must be one of: `studies`, `series`, `instances`, `diagnostics`.
- Options such as `lookback_days`, `page_size`, `max_records_per_batch`, `fetch_dicom_files`, `dicom_volume_path`, `fetch_metadata`, and `wado_mode` go under `table_configuration` and must be listed in the connection's `externalOptionsAllowList`.
- All table option values are passed as strings (e.g., `"200"`, `"true"`).
- The `diagnostics` table requires no special table configuration options.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start small**: Begin by syncing only `studies` to verify connectivity and data volume before enabling `series` and `instances`.
- **Set a realistic initial date range**: On the first run, the connector defaults to scanning all history (from `19000101`). For large PACS systems with millions of instances, this can be very slow. Limit the initial scan by setting an appropriate `lookback_days` or running with a recent date range.
- **Use incremental sync**: After the initial load, the connector uses `StudyDate`-based cursors to fetch only new or recently modified records on each run. The `lookback_days` setting provides overlap to catch late-arriving or backdated studies.
- **Tune `page_size`**: The default of `100` works well for most servers. For large PACS systems, increasing to `500` or `1000` can reduce the total number of API calls. Reduce `page_size` if the server times out on large result sets.
- **Enable file retrieval selectively**: The `fetch_dicom_files` option issues one additional WADO-RS request per instance and should be enabled only when raw DICOM files are needed (e.g., for AI/ML image analysis pipelines). Each `.dcm` file can range from a few KB (text reports) to several hundred MB (CT/MR volumes).
- **Monitor Volume storage**: When using `fetch_dicom_files`, plan Unity Catalog Volume capacity based on the expected data volume. Files are organized by `{StudyUID}/{SeriesUID}/{SOPInstanceUID}.dcm`.
- **Schedule appropriately**: For near-real-time analytics, run every 15-30 minutes. For batch loads, a daily schedule is sufficient. Be aware that cloud-hosted DICOMweb endpoints (Azure, GCP) issue tokens that typically expire after 1 hour -- for long-running initial loads, token refresh must be handled externally.

#### Troubleshooting

**Common Issues:**

- **Empty QIDO-RS results (no records returned)**:
  - Verify the `base_url` includes the correct path prefix (e.g., `/dicom-web`, `/wado`, `/rs`).
  - Confirm the endpoint supports the `StudyDate` QIDO-RS filter parameter.
  - Try accessing `{base_url}/studies` directly in a browser or with `curl` to verify connectivity.

- **Authentication errors (HTTP 401 / 403)**:
  - For Basic auth: verify the username and password are stored correctly in Databricks Secrets.
  - For Bearer token: check that the token has not expired. Some systems issue short-lived tokens (typically 1 hour for cloud services).
  - Confirm the service account has read access to studies, series, and instances.

- **HTTP 400 on WADO-RS retrieval**:
  - Check that WADO-RS is enabled on the PACS (some systems enable QIDO-RS but not WADO-RS).
  - If using frame-based retrieval, ensure the server supports the requested media types.

- **`dicom_file_path` is NULL despite `fetch_dicom_files` being enabled**:
  - The connector handles download failures gracefully. If WADO-RS retrieval or the Volume write fails for a given instance, `dicom_file_path` is set to `NULL` and the pipeline continues.
  - Verify the cluster's service principal has write (`WRITE FILES`) privilege on the target Volume.
  - Check that WADO-RS is enabled on the PACS server.

- **Slow ingestion on first run**:
  - Set a recent start date or increase `lookback_days` from a recent cursor to limit the scan window.
  - Reduce `page_size` if the PACS is timing out on large result sets.
  - Ingest `studies` and `series` before enabling `instances`, which is typically the largest table.

- **Duplicate records after re-run**:
  - Use `SCD_TYPE_1` (the default) for upsert behavior on the primary key. This prevents duplicates.
  - Avoid `APPEND_ONLY` unless you specifically need a full audit log.

## References

- [DICOMweb Standard (DICOM PS3.18)](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/PS3.18.html)
- [QIDO-RS Specification](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_10.6.html)
- [WADO-RS Specification](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_10.4.html)
- [DICOMweb Overview](https://www.dicomstandard.org/using/dicomweb)
- [Orthanc DICOMweb Plugin](https://orthanc.uclouvain.be/book/plugins/dicomweb.html)
- [dcm4chee DICOMweb Documentation](https://dcm4che.atlassian.net/wiki/spaces/d2/pages/1835038/DICOMweb)
- [Google Cloud Healthcare API - DICOMweb](https://cloud.google.com/healthcare-api/docs/how-tos/dicomweb)
- [Azure Health Data Services - DICOM](https://learn.microsoft.com/en-us/azure/healthcare-apis/dicom/)
- [AWS HealthImaging - DICOMweb](https://docs.aws.amazon.com/healthimaging/latest/devguide/dicomweb.html)
- [Lakeflow Community Connectors](https://github.com/databrickslabs/lakeflow-community-connectors)
