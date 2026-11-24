# DICOMweb API Documentation

DICOMweb is the family of RESTful web services for medical imaging defined in **DICOM PS3.18 (Web Services)**. It exposes three core service types:

- **QIDO-RS** — Query based on ID for DICOM Objects (search/list)
- **WADO-RS** — Web Access to DICOM Objects (retrieve metadata and binary data)
- **STOW-RS** — Store Over the Web (write; out of scope for this connector)

The connector reads from the DICOM information object hierarchy: **Study → Series → Instance (SOP)**. Every DICOMweb-compliant server (Orthanc, dcm4che, Google Cloud Healthcare API, Azure Health Data Services, AWS HealthImaging, DICOMcloud, etc.) exposes this hierarchy via the same standard endpoints.

---

## Authorization

DICOMweb PS3.18 does not mandate a specific authentication scheme; each server implementation chooses its own. Security considerations are deferred to PS3.15. In practice, three patterns dominate, and the connector supports all three via the `auth_type` option.

### Preferred Method: Bearer Token

Set `Authorization: Bearer <token>` on every request. This is the modern standard used by cloud-hosted DICOMweb services (Google Cloud Healthcare, Azure Health Data Services, AWS HealthImaging).

```
Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
```

**Connector parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `auth_type` | Yes | Set to `bearer` |
| `token` | Yes | Static bearer token or short-lived JWT |

**Example request (bearer):**
```
GET https://your-pacs.example.com/dicom-web/studies?StudyDate=20240101-20240131&limit=100&offset=0
Accept: application/dicom+json
Authorization: Bearer eyJhbGciOiJSUzI1NiJ9...
```

### Alternative Method: HTTP Basic Auth

Username and password encoded in the `Authorization` header (RFC 7617). Used by self-hosted servers such as Orthanc and dcm4che.

```
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
```

**Connector parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `auth_type` | Yes | Set to `basic` |
| `username` | Yes | Username |
| `password` | Yes | Password |

**Example request (basic):**
```
GET https://orthanc.example.com/dicom-web/studies?StudyDate=20240101-20240131&limit=100&offset=0
Accept: application/dicom+json
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
```

### Alternative Method: No Authentication

Open endpoints (development, demo, or IP-whitelisted servers).

```
GET https://orthanc.uclouvain.be/demo/dicom-web/studies?limit=1
Accept: application/dicom+json
```

**Connector parameters:**
| Parameter | Required | Description |
|-----------|----------|-------------|
| `auth_type` | No | Set to `none` (default) |

**Note on OAuth 2.0:** The DICOM standard does not define a native OAuth flow for the connector itself to execute. Enterprise deployments using OAuth 2.0 / OIDC (e.g., Azure AD, Google IAM) issue short-lived access tokens externally. The connector accepts a pre-obtained bearer token via the `token` parameter. Token refresh must be handled at the pipeline/orchestration layer, not inside the connector.

**Known quirks:**
- Token lifetime on cloud services (Azure, GCP) is typically 1 hour. Long-running pipelines must refresh tokens externally.
- Some Orthanc installations require the DICOMweb plugin to be explicitly enabled before these endpoints are available.

---

## Object List

The DICOMweb connector exposes a static hierarchy of four tables. The object list is **not** retrieved from the server — it is fixed by the DICOM standard.

| Table | DICOM Level | Primary Service | Description |
|-------|-------------|-----------------|-------------|
| `studies` | Study | QIDO-RS | One row per DICOM study (patient imaging encounter) |
| `series` | Series | QIDO-RS | One row per image series within a study |
| `instances` | Instance (SOP) | QIDO-RS | One row per individual DICOM instance (image file) |
| `diagnostics` | N/A | QIDO-RS + WADO-RS | Capability probe: one row per tested endpoint |

The three DICOM data tables are layered hierarchically: every study contains one or more series, and every series contains one or more instances. The `instances` table is the most granular — it is also the largest by row count.

**Object list endpoint:** The connector exposes `list_tables()` returning `["studies", "series", "instances", "diagnostics"]` — no API call needed.

---

## Object Schema

DICOM JSON responses encode every attribute as an object keyed by an **8-character uppercase hexadecimal DICOM tag** (e.g., `"0020000D"` for StudyInstanceUID). Each attribute object contains:

```json
{
  "<8-char-hex-tag>": {
    "vr": "<VR-code>",
    "Value": [ <value1>, <value2>, ... ]
  }
}
```

- `vr`: The DICOM Value Representation (e.g., `"UI"`, `"DA"`, `"PN"`, `"CS"`).
- `Value`: Array containing zero or more values. Absent when the tag has no value.
- `BulkDataURI`: URI pointing to binary bulk data instead of inline value (used for pixel data).
- `InlineBinary`: Base64-encoded binary data for binary VRs.

Person name attributes (VR=`PN`) use a sub-object: `{"Alphabetic": "Doe^John", "Ideographic": "...", "Phonetic": "..."}`.

Attribute objects within a response are ordered by tag in ascending lexicographic order. Group Length attributes `(gggg,0000)` are excluded.

The connector's parser extracts a subset of these tags into flat Python dicts using human-readable keyword names. The schemas below list the extracted fields.

### studies Schema

QIDO-RS endpoint: `GET {base_url}/studies`

| Column | DICOM Tag | VR | Nullable | Description |
|--------|-----------|----|----------|-------------|
| `StudyInstanceUID` | 0020000D | UI | No | Unique identifier for the study (primary key) |
| `PatientID` | 00100020 | LO | Yes | Patient identifier |
| `PatientName` | 00100010 | PN | Yes | Patient name (Alphabetic component) |
| `StudyDate` | 00080020 | DA | Yes | Date study started (YYYYMMDD) |
| `StudyTime` | 00080030 | TM | Yes | Time study started (HHMMSS.ffffff) |
| `AccessionNumber` | 00080050 | SH | Yes | Accession number assigned by radiology |
| `StudyDescription` | 00081030 | LO | Yes | Free-text description of the study |
| `ModalitiesInStudy` | 00080061 | CS | Yes | List of modalities in the study (e.g., `["CT", "SR"]`) |
| `NumberOfStudyRelatedSeries` | 00201206 | IS | Yes | Count of series in this study |
| `NumberOfStudyRelatedInstances` | 00201208 | IS | Yes | Count of instances in this study |
| `connection_name` | N/A | — | Yes | Lineage: connector connection name (added by connector) |

**Example QIDO-RS response fragment (studies):**
```json
[
  {
    "0020000D": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3.604688119.971.1396285783.111"]},
    "00080020": {"vr": "DA", "Value": ["20240315"]},
    "00080030": {"vr": "TM", "Value": ["143000.000"]},
    "00080050": {"vr": "SH", "Value": ["ACC001"]},
    "00080061": {"vr": "CS", "Value": ["CT", "SR"]},
    "00081030": {"vr": "LO", "Value": ["Chest CT with contrast"]},
    "00100010": {"vr": "PN", "Value": [{"Alphabetic": "Doe^John"}]},
    "00100020": {"vr": "LO", "Value": ["PID12345"]},
    "00201206": {"vr": "IS", "Value": [2]},
    "00201208": {"vr": "IS", "Value": [250]}
  }
]
```

### series Schema

QIDO-RS endpoint: `GET {base_url}/studies/{StudyInstanceUID}/series`

| Column | DICOM Tag | VR | Nullable | Description |
|--------|-----------|----|----------|-------------|
| `SeriesInstanceUID` | 0020000E | UI | No | Unique identifier for the series (primary key) |
| `StudyInstanceUID` | 0020000D | UI | Yes | Parent study UID (propagated from parent study if missing) |
| `StudyDate` | 00080020 | DA | Yes | Study date (propagated from parent study if missing) |
| `SeriesNumber` | 00200011 | IS | Yes | Series number within the study |
| `SeriesDescription` | 0008103E | LO | Yes | Free-text description of the series |
| `Modality` | 00080060 | CS | Yes | Imaging modality (CT, MR, US, CR, DX, etc.) |
| `BodyPartExamined` | 00180015 | CS | Yes | Body part examined (CHEST, HEAD, etc.) |
| `SeriesDate` | 00080021 | DA | Yes | Date the series was acquired (YYYYMMDD) |
| `connection_name` | N/A | — | Yes | Lineage: connector connection name (added by connector) |

**Example QIDO-RS response fragment (series):**
```json
[
  {
    "0020000E": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3.604688119.971.1396285783.222"]},
    "0020000D": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3.604688119.971.1396285783.111"]},
    "00080021": {"vr": "DA", "Value": ["20240315"]},
    "00080060": {"vr": "CS", "Value": ["CT"]},
    "0008103E": {"vr": "LO", "Value": ["Axial Chest"]},
    "00200011": {"vr": "IS", "Value": [1]},
    "00180015": {"vr": "CS", "Value": ["CHEST"]}
  }
]
```

### instances Schema

QIDO-RS endpoint: `GET {base_url}/studies/{StudyInstanceUID}/series/{SeriesInstanceUID}/instances`

| Column | DICOM Tag | VR | Nullable | Description |
|--------|-----------|----|----------|-------------|
| `SOPInstanceUID` | 00080018 | UI | No | Unique identifier for the SOP instance (primary key) |
| `SeriesInstanceUID` | 0020000E | UI | Yes | Parent series UID (propagated from parent if missing) |
| `StudyInstanceUID` | 0020000D | UI | Yes | Parent study UID (propagated from parent if missing) |
| `SOPClassUID` | 00080016 | UI | Yes | SOP Class UID (identifies DICOM IOD type, e.g., CT Image Storage) |
| `InstanceNumber` | 00200013 | IS | Yes | Instance number within the series |
| `StudyDate` | 00080020 | DA | Yes | Study date (propagated from parent study if missing) |
| `ContentDate` | 00080023 | DA | Yes | Date content was created (YYYYMMDD) |
| `ContentTime` | 00080033 | TM | Yes | Time content was created (HHMMSS.ffffff) |
| `dicom_file_path` | N/A | — | Yes | Volume path to downloaded .dcm or frame file (populated when `fetch_dicom_files=true`) |
| `metadata` | N/A | — | Yes | Full DICOM JSON object (VariantType; populated when `fetch_metadata=true`) |
| `connection_name` | N/A | — | Yes | Lineage: connector connection name (added by connector) |

**Example QIDO-RS response fragment (instances):**
```json
[
  {
    "00080016": {"vr": "UI", "Value": ["1.2.840.10008.5.1.4.1.1.2"]},
    "00080018": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3.604688119.971.1396285783.333"]},
    "0020000D": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3.604688119.971.1396285783.111"]},
    "0020000E": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3.604688119.971.1396285783.222"]},
    "00200013": {"vr": "IS", "Value": [1]},
    "00080023": {"vr": "DA", "Value": ["20240315"]},
    "00080033": {"vr": "TM", "Value": ["143500.000"]}
  }
]
```

### diagnostics Schema

The `diagnostics` table is generated by probing all standard DICOMweb endpoints on the target server. It does not map to any DICOM standard resource.

| Column | Nullable | Description |
|--------|----------|-------------|
| `endpoint` | No | URL path pattern (e.g., `/studies/{uid}/series/{uid}/instances/{uid}`) |
| `category` | Yes | Service category: `QIDO-RS` or `WADO-RS` |
| `description` | Yes | Human-readable description of the endpoint |
| `supported` | No | Result: `yes`, `no`, `unknown`, `error`, or `partial` |
| `status_code` | Yes | HTTP status code from the probe request |
| `content_type` | Yes | Content-Type header from the response |
| `latency_ms` | Yes | Round-trip latency in milliseconds |
| `notes` | Yes | Additional context (error message, content type detail) |
| `probe_timestamp` | No | ISO-8601 UTC timestamp of the probe run |
| `connection_name` | Yes | Lineage: connector connection name |

---

## Get Object Primary Keys

Primary keys are static — defined by the DICOM standard. No API call is needed to determine them.

| Table | Primary Key Column | DICOM Tag | Notes |
|-------|-------------------|-----------|-------|
| `studies` | `StudyInstanceUID` | 0020000D | DICOM UID, globally unique per the standard |
| `series` | `SeriesInstanceUID` | 0020000E | DICOM UID, globally unique per the standard |
| `instances` | `SOPInstanceUID` | 00080018 | DICOM SOP Instance UID, globally unique per the standard |
| `diagnostics` | `endpoint` | N/A | URL path pattern string |

DICOM UIDs are guaranteed unique by construction (based on organization-specific root OIDs and machine-generated suffixes). They are immutable once assigned and never reused.

---

## Object Ingestion Type

| Table | Ingestion Type | Cursor Field | Rationale |
|-------|---------------|--------------|-----------|
| `studies` | `cdc` | `StudyDate` | Studies may be updated after creation (addenda, corrections). Upsert on `StudyInstanceUID` handles this. Deletes are not exposed by QIDO-RS. |
| `series` | `cdc` | `StudyDate` | Series metadata can be amended. Same cursor strategy as studies (series are fetched via parent study's date range). |
| `instances` | `cdc` | `StudyDate` | Instance metadata can change (e.g., DICOM SR modifications). Same date-based cursor as studies. |
| `diagnostics` | `cdc` | `probe_timestamp` | Run on every pipeline trigger; latest probe results replace previous ones. |

**Why `cdc` and not `append`:** DICOM systems allow post-creation updates to study/series/instance metadata (e.g., corrected patient names, addendum reports). An upsert (CDC) strategy ensures the Delta table reflects the current state. Deletes are not modelled because QIDO-RS has no delete-notification endpoint; soft-delete or external tracking is required if delete propagation is needed.

---

## Read API for Data Retrieval

### QIDO-RS — Search Studies

**Endpoint:**
```
GET {base_url}/studies
```

**Required headers:**
```
Accept: application/dicom+json
```

**Query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `StudyDate` | DA range | No | Date filter. Format: `YYYYMMDD` (exact) or `YYYYMMDD-YYYYMMDD` (range). |
| `limit` | uint | No | Max records to return per page. Server-defined default if omitted. |
| `offset` | uint | No | Number of records to skip (zero-based). Default: 0. |
| `fuzzymatching` | bool | No | Enable fuzzy person-name matching. Default: false. |
| `includefield` | tag or `all` | No | Additional tags to include in the response. |
| Any DICOM attribute keyword | various | No | Filter on any searchable attribute (e.g., `PatientID=PID123`, `AccessionNumber=ACC001`). |

**Pagination:** Offset/limit (numeric). Advance `offset` by `limit` on each page until an empty array is returned or fewer than `limit` records are returned.

**Example request:**
```
GET https://your-pacs.example.com/dicom-web/studies?StudyDate=20240101-20240131&limit=100&offset=0
Accept: application/dicom+json
Authorization: Bearer <token>
```

**Example response:**
```json
[
  {
    "0020000D": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3..."]},
    "00080020": {"vr": "DA", "Value": ["20240115"]},
    "00100010": {"vr": "PN", "Value": [{"Alphabetic": "Doe^John"}]},
    "00100020": {"vr": "LO", "Value": ["PID12345"]},
    "00080061": {"vr": "CS", "Value": ["CT"]},
    "00201206": {"vr": "IS", "Value": [3]}
  }
]
```

**Empty result:** HTTP 204 No Content or HTTP 200 with an empty JSON array `[]`.

**Pagination warning:** If the result count exceeds the server's maximum, the server SHALL return a `Warning: 299` HTTP header. Clients should detect this and continue paginating.

---

### QIDO-RS — Search Series for a Study (Hierarchical)

**Endpoint:**
```
GET {base_url}/studies/{StudyInstanceUID}/series
```

This hierarchical endpoint is required by some server implementations (e.g., AWS S3 Static WADO Server) that do not support the flat `/series` endpoint. The connector always uses the hierarchical form.

**Required headers:**
```
Accept: application/dicom+json
```

**Query parameters:** Same as `/studies` minus `StudyDate` (already scoped by the study UID path parameter). `limit` and `offset` are optional.

**Example request:**
```
GET https://your-pacs.example.com/dicom-web/studies/1.2.840.113619.2.55.3.604688119.971.1396285783.111/series
Accept: application/dicom+json
Authorization: Bearer <token>
```

**Example response:**
```json
[
  {
    "0020000E": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3...series.1"]},
    "00080060": {"vr": "CS", "Value": ["CT"]},
    "00200011": {"vr": "IS", "Value": [1]},
    "0008103E": {"vr": "LO", "Value": ["Axial Chest"]}
  }
]
```

---

### QIDO-RS — Search Instances for a Series (Hierarchical)

**Endpoint:**
```
GET {base_url}/studies/{StudyInstanceUID}/series/{SeriesInstanceUID}/instances
```

**Required headers:**
```
Accept: application/dicom+json
```

**Query parameters:** `limit` and `offset` are optional. Additional attribute filters are supported.

**Example request:**
```
GET https://your-pacs.example.com/dicom-web/studies/1.2.840.../series/1.2.840.../instances
Accept: application/dicom+json
Authorization: Bearer <token>
```

**Example response:**
```json
[
  {
    "00080016": {"vr": "UI", "Value": ["1.2.840.10008.5.1.4.1.1.2"]},
    "00080018": {"vr": "UI", "Value": ["1.2.840.113619.2.55.3...instance.1"]},
    "00200013": {"vr": "IS", "Value": [1]},
    "00080023": {"vr": "DA", "Value": ["20240315"]}
  }
]
```

---

### WADO-RS — Retrieve Instance Metadata (JSON)

Returns the full DICOM JSON tag set for a single SOP instance — more complete than the QIDO-RS response which returns only a subset of tags.

**Endpoint:**
```
GET {base_url}/studies/{StudyInstanceUID}/series/{SeriesInstanceUID}/instances/{SOPInstanceUID}/metadata
```

**Required headers:**
```
Accept: application/dicom+json
```

**Response:** JSON array with one element containing the complete DICOM JSON object for the instance. Some servers return the object directly (not wrapped in an array).

**Example request:**
```
GET https://your-pacs.example.com/dicom-web/studies/1.2.840.../series/1.2.840.../instances/1.2.840.../metadata
Accept: application/dicom+json
Authorization: Bearer <token>
```

---

### WADO-RS — Retrieve Series Metadata (JSON)

Returns full DICOM JSON for all instances in a series in a single request.

**Endpoint:**
```
GET {base_url}/studies/{StudyInstanceUID}/series/{SeriesInstanceUID}/metadata
```

**Required headers:**
```
Accept: application/dicom+json
```

**Response:** JSON array with one element per SOP instance in the series.

The connector uses this endpoint (when `fetch_metadata=true`) to populate the `metadata` variant column on the `instances` table. A single request per series retrieves all instance metadata rather than requiring one request per instance.

**Example request:**
```
GET https://your-pacs.example.com/dicom-web/studies/1.2.840.../series/1.2.840.../metadata
Accept: application/dicom+json
Authorization: Bearer <token>
```

---

### WADO-RS — Retrieve Full DICOM Instance (Binary)

Downloads the complete DICOM PS3.10 binary file for a single instance.

**Endpoint:**
```
GET {base_url}/studies/{StudyInstanceUID}/series/{SeriesInstanceUID}/instances/{SOPInstanceUID}
```

**Required headers:**
```
Accept: multipart/related; type="application/dicom"
```

**Response content type:** `multipart/related; type="application/dicom"; boundary=<boundary>` — The DICOM binary is the first (and usually only) part. The connector extracts the first part and writes it as a `.dcm` file to the Unity Catalog Volume.

**Used by:** `fetch_dicom_files=true` with `wado_mode=full` or `wado_mode=auto`.

**Example request:**
```
GET https://your-pacs.example.com/dicom-web/studies/1.2.840.../series/1.2.840.../instances/1.2.840...
Accept: multipart/related; type="application/dicom"
Authorization: Bearer <token>
```

---

### WADO-RS — Retrieve Instance Frame (Binary)

Downloads a single pixel frame from a DICOM instance as an image. Used by frame-based servers (e.g., AWS S3 Static WADO Server) that do not serve the full `.dcm` file via the standard instance endpoint.

**Endpoint:**
```
GET {base_url}/studies/{StudyInstanceUID}/series/{SeriesInstanceUID}/instances/{SOPInstanceUID}/frames/{frame_number}
```

`frame_number` is 1-based. Use `1` for the first (or only) frame.

**Required headers:**
```
Accept: image/jpeg, image/jls, application/octet-stream
```

**Response content type:** `image/jpeg`, `image/jls`, `image/jp2`, `application/octet-stream`, or `multipart/related` (server-dependent).

**Used by:** `fetch_dicom_files=true` with `wado_mode=frames` or after auto-detection fallback.

---

### Incremental Sync Strategy

The connector uses a **StudyDate-based cursor** for all three data tables.

**Cursor field:** `StudyDate` (DICOM tag `00080020`, VR=`DA`, format `YYYYMMDD`)

**Cursor storage format:**
```json
{"study_date": "20240131", "page_offset": 0}
```

**Logic on each pipeline run:**

1. Read `study_date` from the stored offset (default: `"19000101"` on first run — retrieves all history).
2. Compute `effective_start = study_date - lookback_days` (default: 1 day lookback; handles late-arriving records and clock skew).
3. Build the date range filter: `StudyDate={effective_start}-{today}`.
4. Paginate QIDO-RS `/studies` with `StudyDate=<range>&limit=<page_size>&offset=<page_offset>`.
5. For `series`/`instances`: enumerate studies in that date range, then fetch child records hierarchically.
6. On completion, set next offset to `{"study_date": <today>, "page_offset": 0}`.

**Table options for tuning:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `page_size` | int | 100 | Records per QIDO-RS page |
| `lookback_days` | int | 1 | Days subtracted from cursor for overlap/late-arrival protection |
| `fetch_dicom_files` | bool | false | Download `.dcm` / frame binary to Unity Catalog Volume |
| `dicom_volume_path` | str | — | Volume path prefix for downloaded DICOM files (required when `fetch_dicom_files=true`) |
| `fetch_metadata` | bool | false | Fetch full DICOM JSON via WADO-RS `/metadata` and store in the `metadata` column |
| `wado_mode` | str | auto | `auto` (try full then fallback to frames), `full`, or `frames` |

**Delete handling:** QIDO-RS has no delete-notification mechanism. Deleted studies/instances will remain in the Delta table unless removed by a separate out-of-band process. The connector is `cdc` type (upserts only), not `cdc_with_deletes`.

**First-run behaviour:** The default `study_date` cursor is `"19000101"`, which effectively requests all studies from the beginning of time. On large servers this can be slow; set `start_date` in the pipeline spec to a more recent date for the first run.

---

### Rate Limits

DICOMweb PS3.18 does not define rate limits at the protocol level. Limits are server/platform-specific:

| Platform | Known Limit |
|----------|-------------|
| **Google Cloud Healthcare API** | Quota-based (`dicomweb_ops` per minute per region). Default quota is configurable; increases via GCP Console. Each DICOMweb operation (study, series, instance, frame) counts as one operation. |
| **Azure Health Data Services DICOM** | Capable of ingesting multiple thousands of images per second; no published per-second API request cap in Azure's public documentation. Subject to Azure subscription-level throttling. |
| **AWS HealthImaging** | QIDO-RS: max 10,000 records total per search scope; max `limit=1000` per request; max `offset=9000`. Records beyond the 10,000 mark are not accessible via QIDO-RS. |
| **Orthanc (self-hosted)** | No built-in rate limiting. Hardware and database backend are the practical limit. A reverse proxy (Nginx/HAProxy) can be configured to add throttling. |
| **dcm4che (self-hosted)** | No built-in rate limiting. |

**Practical guidance:** For cloud-hosted servers, use `page_size=100` (default) and avoid parallelising QIDO-RS requests. For self-hosted servers, larger page sizes (e.g., 500–1000) can reduce total round trips. The `fetch_dicom_files=true` option issues one additional WADO-RS request per instance and should be used selectively.

---

## Field Type Mapping

### DICOM VR to Python/Spark Types

DICOM JSON encodes all attributes with a `vr` field indicating the Value Representation. The connector maps these to the following Python and Spark types:

| DICOM VR | VR Name | JSON Data Type | Spark Type | Notes |
|----------|---------|----------------|------------|-------|
| `AE` | Application Entity | String | StringType | |
| `AS` | Age String | String | StringType | Format: nnnW/M/Y (e.g., `025Y`) |
| `AT` | Attribute Tag | String | StringType | 8-char hex, e.g., `"00100020"` |
| `CS` | Code String | String | StringType | Uppercase, padded; often an enum |
| `DA` | Date | String | StringType | Format: `YYYYMMDD` |
| `DS` | Decimal String | Number or String | StringType or DoubleType | String form used to preserve precision |
| `DT` | Date Time | String | StringType | Format: `YYYYMMDDHHMMSS.ffffff±HHMM` |
| `FL` | Floating Point Single | Number | FloatType | |
| `FD` | Floating Point Double | Number | DoubleType | |
| `IS` | Integer String | Number or String | IntegerType | Connector parses to int |
| `LO` | Long String | String | StringType | Max 64 chars |
| `LT` | Long Text | String | StringType | Max 10240 chars |
| `OB` | Other Byte | Base64 octet-stream | BinaryType / StringType (base64) | Pixel data; use WADO-RS to retrieve |
| `OD` | Other Double | Base64 octet-stream | BinaryType | |
| `OF` | Other Float | Base64 octet-stream | BinaryType | |
| `OL` | Other Long | Base64 octet-stream | BinaryType | |
| `OW` | Other Word | Base64 octet-stream | BinaryType | Pixel data |
| `PN` | Person Name | Object | StringType | Connector extracts `Alphabetic` component |
| `SH` | Short String | String | StringType | Max 16 chars |
| `SL` | Signed Long | Number | IntegerType | |
| `SQ` | Sequence of Items | Array of objects | VariantType / StringType (JSON) | Complex nested sequences |
| `SS` | Signed Short | Number | ShortType | |
| `ST` | Short Text | String | StringType | Max 1024 chars |
| `SV` | Signed 64-bit Very Long | Number or String | LongType | |
| `TM` | Time | String | StringType | Format: `HHMMSS.ffffff` |
| `UC` | Unlimited Characters | String | StringType | |
| `UI` | Unique Identifier | String | StringType | DICOM UID; globally unique dotted notation |
| `UL` | Unsigned Long | Number | LongType | |
| `UN` | Unknown | Base64 octet-stream | BinaryType | |
| `UR` | URI/URL | String | StringType | |
| `US` | Unsigned Short | Number | IntegerType | |
| `UT` | Unlimited Text | String | StringType | |
| `UV` | Unsigned 64-bit Very Long | Number or String | LongType | |

**Special behaviours:**

- **PN (Person Name):** QIDO-RS returns `[{"Alphabetic": "Doe^John"}]`. The connector extracts the `Alphabetic` component as a plain string. Components follow the DICOM `FamilyName^GivenName^MiddleName^NamePrefix^NameSuffix` convention.
- **CS (Code String):** Often acts as an enum. `Modality` values include `CT`, `MR`, `US`, `CR`, `DX`, `NM`, `PT`, `MG`, `XA`, `RF`, `SR`, `DOC`, etc.
- **DA (Date):** `YYYYMMDD` — no separators. The connector stores as string; callers can cast to `DATE` in SQL.
- **UI (Unique Identifier):** DICOM UIDs use a dotted numeric OID notation, e.g., `1.2.840.10008.5.1.4.1.1.2`. Maximum 64 characters.
- **IS/DS value arrays:** When the `Value` array contains multiple elements, it represents DICOM multi-value attributes. The connector takes the first element for scalar columns.
- **SQ (Sequence):** Complex nested sequences (e.g., Referenced Series Sequence) are not individually parsed into flat columns; they appear in the `metadata` variant column when `fetch_metadata=true`.
- **Null/absent values:** When a tag has no value, DICOM JSON omits the `Value` key entirely (only `vr` is present). The connector treats absent values as `None`.

---

## Known Quirks and Server Compatibility Notes

1. **Flat `/series` and `/instances` endpoints:** DICOMweb PS3.18 defines flat endpoints (`GET /series`, `GET /instances`) for cross-study queries. Many self-hosted servers (dcm4che, Orthanc, AWS S3 Static WADO) do NOT implement these. The connector exclusively uses hierarchical endpoints (`/studies/{uid}/series`, `/studies/{uid}/series/{uid}/instances`) for compatibility.

2. **WADO-RS auto-detection:** Some servers (e.g., AWS S3 Static WADO) return HTTP 404 or 406 on `GET /instances/{uid}` but do serve `GET /instances/{uid}/frames/1`. The connector's `wado_mode=auto` detects this: it tries the full endpoint first and falls back to frames on 404/406/415. This behaviour is cached per run.

3. **Empty responses:** Compliant servers should return HTTP 204 with no body when there are no results. Some servers return HTTP 200 with an empty body or an empty JSON array `[]`. The connector handles all three.

4. **PersonName encoding:** Some non-standard servers return person names as plain strings (`"Doe^John"`) rather than the DICOM JSON object `{"Alphabetic": "Doe^John"}`. The connector parser handles both forms.

5. **StudyDate cursor limitations:** QIDO-RS does not guarantee ordering by StudyDate, and the standard does not define a `sort` parameter. The `lookback_days` option compensates for this by re-querying recent dates on every run to catch any late-arriving records.

6. **ModalitiesInStudy as array:** DICOM tag `00080061` (CS VR) can contain multiple values. The connector stores this as `ArrayType(StringType)`.

7. **Token expiry during long runs:** Cloud services issue short-lived tokens (typically 1 hour). For large initial loads spanning multiple hours, tokens must be refreshed externally and the connector restarted.

8. **Large studies:** Studies with thousands of instances (e.g., CT angiograms) will produce many hierarchical sub-requests. The `fetch_dicom_files=true` option multiplies the request count further. Use `page_size=100` (default) and consider running instances and studies as separate pipeline tables.

9. **Orthanc demo server:** The public Orthanc demo at `https://orthanc.uclouvain.be/demo/dicom-web` is suitable for functional testing. It is rate-limited and contains only sample data.

10. **WADO-RS multipart boundary parsing:** The connector's MIME parser extracts the first part from `multipart/related` responses. Some servers omit or vary the boundary format; the parser falls back to returning the raw response body if the boundary cannot be located.

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs (DICOM PS3.18) | https://dicom.nema.org/medical/dicom/current/output/html/part18.html | 2026-03-02 | Highest | Service definitions: QIDO-RS, WADO-RS, STOW-RS; endpoint structure |
| Official Docs (DICOM PS3.18 QIDO-RS) | https://dicom.nema.org/dicom/2013/output/chtml/part18/sect_6.7.html | 2026-03-02 | Highest | Default return attributes for studies, series, instances; pagination parameters |
| Official Docs (DICOM PS3.18 Pagination) | https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_8.3.4.4.html | 2026-03-02 | Highest | offset/limit semantics, Warning 299 header |
| Official Docs (DICOM JSON Model) | https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.3.html | 2026-03-02 | Highest | Full VR to JSON data type mapping table |
| Official Docs (DICOM JSON Object Structure) | https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_f.2.2.html | 2026-03-02 | Highest | Tag keying format, Value array, PersonName sub-object |
| Official Docs (WADO-RS) | https://www.dicomstandard.org/using/dicomweb/retrieve-wado-rs-and-wado-uri | 2026-03-02 | Highest | WADO-RS endpoint list, Accept headers, multipart/related response format |
| Official Docs (QIDO-RS) | https://www.dicomstandard.org/using/dicomweb/query-qido-rs | 2026-03-02 | Highest | QIDO-RS endpoint list, query parameter table |
| Cloud Provider Docs (Google Cloud Healthcare) | https://docs.cloud.google.com/healthcare-api/quotas | 2026-03-02 | High | DICOMweb quota model (dicomweb_ops per minute), request size limits |
| Cloud Provider Docs (AWS HealthImaging) | https://docs.aws.amazon.com/healthimaging/latest/devguide/dicomweb-search.html | 2026-03-02 | High | 10,000 record limit, max limit=1000 per request, max offset=9000 |
| Wikipedia | https://en.wikipedia.org/wiki/DICOMweb | 2026-03-02 | Medium | Historical context, service overview, standard evolution |
| PMC Article | https://pmc.ncbi.nlm.nih.gov/articles/PMC5959831/ | 2026-03-02 | Medium | Background on DICOMweb adoption and use cases |
| Connector implementation | `src/databricks/labs/community_connector/sources/dicomweb/dicomweb.py` | 2026-03-02 | High | Actual endpoint calls, cursor strategy, WADO mode auto-detection logic |
| Connector implementation | `src/databricks/labs/community_connector/sources/dicomweb/dicomweb_client.py` | 2026-03-02 | High | HTTP headers used, auth patterns, multipart parsing, timeout values |
| Connector implementation | `src/databricks/labs/community_connector/sources/dicomweb/dicomweb_schemas.py` | 2026-03-02 | High | Final Spark schema field names and types |
| Connector spec | `src/databricks/labs/community_connector/sources/dicomweb/connector_spec.yaml` | 2026-03-02 | High | Connection parameters and table options allowlist |
| Auth guide (rjh's Substack) | https://rjhorniii.substack.com/p/dicomweb-user-authentication | 2026-03-02 | Medium | OAuth/OIDC token patterns for DICOMweb |

**Source prioritisation notes:**
- DICOM PS3.18 official NEMA documentation is the authoritative source for all endpoint paths, parameter names, response formats, and VR type mappings.
- The connector implementation files (`dicomweb.py`, `dicomweb_client.py`) are the ground-truth reference for how the standard is implemented and any server-specific quirks addressed.
- AWS HealthImaging documentation confirmed the 10,000-record QIDO-RS limit (not mentioned in the standard itself).
- Google Cloud quotas documentation confirmed the `dicomweb_ops` quota model.
- No conflicts between official DICOM docs and the connector implementation were found.
