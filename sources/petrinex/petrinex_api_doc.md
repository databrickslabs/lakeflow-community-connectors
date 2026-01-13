# **Petrinex API Documentation**

## **Authorization**

- **Chosen method**: No authentication required - Petrinex PublicData is freely accessible.
- **Base URL**: `https://www.petrinex.ca/PD/`
- **Auth placement**: Not applicable (public data).
- **Required access**: None - data is publicly available without registration.

Example data access (via Python wrapper):

```python
from petrinex import PetrinexVolumetricsClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")

# Load files updated after a date
df = client.read_spark_df(updated_after="2025-01-01")

# Or load all historical data from a production month
df = client.read_spark_df(from_date="2023-01")
```

Notes:
- No API keys, tokens, or registration required
- Rate limiting is not documented but be respectful of the public service
- Data is scraped from the Petrinex PublicData web interface


## **Object List**

For connector purposes, we treat specific Petrinex data files as **objects/tables**.  
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Source | Ingestion Type |
|------------|-------------|----------------|----------------|
| `volumetrics` | Monthly oil & gas production volumes by facility | Petrinex volumetric CSV files (jurisdiction AB) | `cdc` (upserts based on composite key with file update tracking) |

**Connector scope for initial implementation**:
- Step 1 focuses on the `volumetrics` object for Alberta (AB jurisdiction).
- Petrinex provides additional datasets (well tests, facility data, other jurisdictions) that could be added in future extensions.

High-level notes:
- **Volumetrics**: The primary production reporting dataset, containing monthly oil, gas, and water volumes.
- **Monthly granularity**: Data is published monthly, typically 2-3 months after the production month.
- **Revised submissions**: Operators frequently revise historical submissions, requiring CDC with file update tracking.
- **Large files**: Volumetric files can be several GB, requiring memory-efficient processing.


## **Object Schema**

### General notes

- Petrinex provides CSV files via their PublicData portal, scraped and loaded by the [petrinex-python-api wrapper](https://github.com/guanjieshen/petrinex-python-api).
- For the connector, we define **tabular schemas** per object, derived from the CSV structure.
- All fields are initially StringType to preserve data fidelity; downstream processing can cast to appropriate types.

### `volumetrics` object (primary table)

**Source**:  
Petrinex PublicData volumetric CSV files (accessed via `client.read_spark_df()` in the Python wrapper)

**Key behavior**:
- Returns monthly production volumes for oil, gas, and water by facility.
- Supports date range filtering via `from_date` (production month) or `updated_after` (file update date).
- Files can be updated after initial publication due to operator revisions.
- Large datasets requiring incremental loading and checkpointing.

**High-level schema (connector view)**:

Top-level fields (from Petrinex CSV):

| Column Name | Type | Description |
|------------|------|-------------|
| `BA_ID` | string | Battery/Facility ID (part of composite key) |
| `Licence_No` | string | Well licence number |
| `Licensee_BA_Code` | string | Operator BA code |
| `Licensee_Full_Name` | string | Operator full legal name |
| `Licencee_Abbr_Name` | string | Operator abbreviated name |
| `ProductionMonth` | string | Production month in YYYY-MM format (part of composite key) |
| `PoolCode` | string | Geological pool code |
| `Product` | string | Product type: oil, gas, water, etc. (part of composite key) |
| `Activity` | string | Activity type: production, injection, flaring, etc. (part of composite key) |
| `Source` | string | Data source indicator (part of composite key) |
| `Volume` | string | Volume amount (numeric but stored as string) |
| `EnergyGJ` | string | Energy content in gigajoules (numeric but stored as string) |
| `Hours` | string | Operating hours for the month (numeric but stored as string) |

**Additional connector-derived fields**:

| Column Name | Type | Description |
|------------|------|-------------|
| `production_month` | string | Normalized production month (YYYY-MM) |
| `file_updated_ts` | timestamp (UTC) | Timestamp when Petrinex updated the source file. Used for incremental loading. |
| `source_url` | string | Download URL for the source file (for audit/traceability) |
| `ingestion_time` | timestamp (UTC) | Connector-generated timestamp indicating when the row was last ingested/updated. Used for SCD Type 1 sequencing. |

**Data characteristics**:
- **Monthly granularity**: One record per facility per product per month.
- **Composite key**: Combination of BA_ID, ProductionMonth, Product, Activity, and Source.
- **String types**: Most fields are string to handle variability in CSV data (e.g., missing values, special characters).
- **Nullable fields**: Many fields can be null/empty in the source data.
- **Large volume**: Files can contain millions of records per month.
- **Frequent revisions**: Operators revise historical months, requiring CDC tracking.

**Example record** (connector representation):

```json
{
  "BA_ID": "AA/01-02-003-04W5/0",
  "Licence_No": "0123456",
  "Licensee_BA_Code": "ACME",
  "Licensee_Full_Name": "ACME OIL & GAS LTD",
  "Licencee_Abbr_Name": "ACME",
  "ProductionMonth": "2024-01",
  "PoolCode": "LEDUC",
  "Product": "OIL",
  "Activity": "Production",
  "Source": "Facility",
  "Volume": "1234.56",
  "EnergyGJ": "0",
  "Hours": "744",
  "production_month": "2024-01",
  "file_updated_ts": "2024-03-15T10:30:00Z",
  "source_url": "https://www.petrinex.ca/PD/...",
  "ingestion_time": "2025-01-15T10:30:00Z"
}
```

> The columns listed above define the **complete connector schema** for the `volumetrics` table.  
> Additional Petrinex fields may exist and can be added as needed.


## **Get Object Primary Key**

There is no dedicated metadata endpoint to get the primary key for the `volumetrics` object.  
Instead, the primary key is defined **statically** based on the resource schema and domain knowledge.

- **Primary key for `volumetrics`**: Composite key
  - `BA_ID` - Facility identifier
  - `ProductionMonth` - Production month (YYYY-MM)
  - `Product` - Product type
  - `Activity` - Activity type
  - `Source` - Data source
  
  Property: Uniquely identifies a volumetric record.
  Rationale: A facility can report multiple products and activities in a given month.

The connector will:
- Use all five fields as the composite primary key for upserts when ingestion type is `cdc`.
- Track `file_updated_ts` to determine which files have been processed.
- Use `ingestion_time` to sequence records for SCD Type 1 merges.


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Ingestion type for Petrinex volumetrics:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `volumetrics` | `cdc` | Volumetric data has a stable composite primary key and can be updated after initial publication due to operator revisions. The connector tracks file update timestamps to efficiently process only changed files, making CDC with upserts the appropriate pattern. |

For `volumetrics`:
- **Primary key**: Composite (BA_ID, ProductionMonth, Product, Activity, Source)
- **Cursor field**: `file_updated_ts` (used for watermark tracking)
- **Sequence field**: `ingestion_time` (connector-generated; determines which version is newest for SCD Type 1)
- **Sort order**: Natural sort by primary key fields
- **Deletes**: Petrinex does not delete historical records; closed facilities remain in the dataset.
- **Updates**: Production volumes can be updated for months/years after initial publication as operators revise submissions.


## **Read API for Data Retrieval**

### Primary read endpoint for `volumetrics`

- **Access method**: Via [petrinex-python-api wrapper](https://github.com/guanjieshen/petrinex-python-api)
- **Function**: `PetrinexVolumetricsClient.read_spark_df(updated_after=..., from_date=...)`
- **Underlying endpoint**: Petrinex PublicData web scraping (exact URLs not publicly documented)

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `updated_after` | string | no | - | Load only files updated after this date (format: YYYY-MM-DD). For incremental updates. |
| `from_date` | string | no | - | Load all production data from this month onwards (format: YYYY-MM). For historical backfills. |
| `request_timeout_s` | int | no | 60 | HTTP request timeout in seconds |

**Return type**:
- PySpark DataFrame with volumetric data
- Includes provenance columns: `production_month`, `file_updated_ts`, `source_url`

**Incremental strategy**:
- On the first run, the connector:
  - Uses `from_date` for historical backfill, or
  - Defaults to 6 months ago if no `from_date` is provided.
- On subsequent runs:
  - Uses the maximum `file_updated_ts` from the previous sync as the watermark.
  - Fetches files updated since the watermark.
  - Applies upserts based on composite primary key, with `ingestion_time` determining the newest record for SCD Type 1.

Example requests:

```python
from petrinex import PetrinexVolumetricsClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")

# Initial historical load
df = client.read_spark_df(from_date="2023-01")

# Incremental load (files updated after a date)
df = client.read_spark_df(updated_after="2025-01-01")
```

**Handling updates**:
- Petrinex files are updated as operators revise historical submissions.
- The connector captures updates by:
  - Tracking `file_updated_ts` for each file processed.
  - Fetching only files updated since the last watermark.
  - Using `ingestion_time` to determine which version is newest.
  - Upserting records with the latest `ingestion_time` for each composite key.

**Memory efficiency**:
- The petrinex package implements automatic checkpointing every 10 files.
- Incremental DataFrame unions prevent loading all data into memory at once.
- Typical usage: 16-32 GB driver memory for historical loads.

**Date behavior**:
- `from_date` is inclusive (e.g., "2023-01" loads January 2023 onwards).
- `updated_after` is exclusive (e.g., "2025-01-01" loads files updated after that date).
- The wrapper handles nested ZIP files, encoding issues, and malformed CSV rows automatically.


## **Field Type Mapping**

### General mapping (Petrinex CSV → connector logical types)

| Petrinex Type | Example Fields | Connector Logical Type | Notes |
|---------------|----------------|------------------------|-------|
| string | Most fields | string | Preserved as string to handle CSV variability. Cast downstream as needed. |
| timestamp | `file_updated_ts`, `ingestion_time` | timestamp (UTC) | Generated by the wrapper or connector. |

### Special behaviors and constraints

- **All fields are strings**:
  - Petrinex CSV files have inconsistent data types and formats.
  - The connector preserves data as string to avoid parsing errors.
  - Downstream processing should cast to numeric/date types as needed.
  
- **Provenance fields**:
  - `file_updated_ts` tracks when Petrinex updated the source file (from file metadata).
  - `ingestion_time` tracks when the connector processed the record.
  - `source_url` provides traceability back to the source file.

- **Null handling**:
  - Empty strings in CSV are preserved as empty strings (not converted to null).
  - Downstream processing should handle empty strings appropriately.

- **Data quality**:
  - Petrinex data can contain errors, missing values, and inconsistencies.
  - Validation should be performed downstream.


## **Write API**

Petrinex PublicData is **read-only** for public access. There is no write API.

- Production data is submitted by operators via a separate Petrinex interface.
- Data is published to the PublicData portal after regulatory processing.
- Users cannot modify or delete data via the public interface.


## **Known Quirks & Edge Cases**

- **Large files**:
  - Volumetric files can be several GB, containing millions of records.
  - Adequate driver memory (16-32 GB) is recommended for historical loads.

- **Encoding issues**:
  - Petrinex files use latin-1 encoding.
  - The petrinex package handles this automatically.

- **Nested ZIP files**:
  - Petrinex publishes data in nested ZIP archives.
  - The petrinex package extracts these automatically.

- **Malformed CSV rows**:
  - Some CSV files contain malformed rows.
  - The petrinex package skips bad rows with `on_bad_lines="skip"`.

- **Frequent revisions**:
  - Operators revise historical submissions regularly (months or years later).
  - The connector's CDC approach with `file_updated_ts` captures these revisions.

- **Publication lag**:
  - Petrinex data is published 2-3 months after the production month.
  - Recent months may be incomplete or subject to revision.

- **No rate limiting documented**:
  - Petrinex does not document rate limits.
  - Be respectful of their infrastructure; daily/weekly schedules are recommended.

- **Jurisdiction-specific**:
  - This connector focuses on Alberta (AB) data.
  - Other Canadian jurisdictions (BC, SK) could be added as extensions.

- **String types**:
  - Most fields are StringType to handle CSV variability.
  - Downstream processing should validate and cast to appropriate types.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| GitHub Repository | https://github.com/guanjieshen/petrinex-python-api | 2026-01-07 | High | Python wrapper API, method signatures, parameters, and example usage. |
| Petrinex Website | https://www.petrinex.ca | 2026-01-07 | Medium | PublicData portal structure and data availability. |
| PyPI Package | https://pypi.org/project/petrinex/ | 2026-01-07 | High | Package availability, version history, and dependencies. |


## **Sources and References**

- **petrinex-python-api GitHub Repository** (highest confidence)
  - https://github.com/guanjieshen/petrinex-python-api
  - MIT License
  - Provides Python wrapper for Petrinex PublicData

- **Petrinex Website**
  - https://www.petrinex.ca
  - Alberta Energy Regulator's petroleum data portal

- **PyPI Package**
  - https://pypi.org/project/petrinex/
  - Installation and version information

**Connector implementation reference**:
- `sources/petrinex/petrinex.py` - Databricks connector implementation
- `sources/petrinex/README.md` - Connector user guide

When conflicts arise, **the petrinex-python-api GitHub repository** is treated as the source of truth for API behavior, method names, and data structures.

