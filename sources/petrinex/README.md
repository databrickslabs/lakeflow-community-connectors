# Petrinex Connector for Databricks

Ingest Alberta oil & gas production data from [Petrinex PublicData](https://www.petrinex.ca/PD/Pages/default.aspx) into Databricks Unity Catalog with automatic incremental updates.

## Supported Data

**Currently Available:**
- **`volumetrics`** - Monthly oil & gas production volumes from Alberta operators

**Extensible:** This connector can be extended to support additional Petrinex datasets (e.g., well tests, facility data). Contributions welcome!

## What You Get

- 🛢️ **Monthly production data** - Oil, gas, and water volumes by facility
- 🔄 **Automatic updates** - Captures revised submissions and new production months
- 📊 **Historical backfill** - Load data from any production month with automatic merges
- ⚡ **Memory efficient** - Incremental loading with auto-checkpointing for large datasets
- 🔓 **No authentication** - Public data, no API key required

## Quick Start

### 1. No API Key Needed

Petrinex data is publicly available - no sign-up or authentication required!

### 2. Create Connection in Databricks

Create a Unity Catalog connection (no credentials needed):

```sql
CREATE CONNECTION petrinex_connector
TYPE lakeflow;
```

### 3. Configure Your Pipeline

**Important:** Add `petrinex` as a Python package in your pipeline configuration:

```python
# In your pipeline settings, add:
libraries = [
    {"pypi": {"package": "petrinex>=0.2.0"}}
]
```

**Minimal setup (uses defaults):**

```python
pipeline_spec = {
    "connection_name": "petrinex_connector",
    "objects": [
        {
            "table": {
                "source_table": "volumetrics",
                "destination_table": "petrinex_volumetrics",
            }
        }
    ],
}
```

**With all table configuration options:**

```python
pipeline_spec = {
    "connection_name": "petrinex_connector",
    "objects": [
        {
            "table": {
                "source_table": "volumetrics",
                "destination_catalog": "main",
                "destination_schema": "energy",
                "destination_table": "petrinex_volumetrics",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "from_date": "2023-01",
                    "updated_after": "2025-01-01",
                    "request_timeout_s": "60"
                }
            }
        }
    ],
}
```

### 4. Schedule Your Pipeline

The connector supports both execution modes within Databricks Workflows:

**Triggered Mode** - Runs on a schedule (daily, weekly, etc.) and performs incremental updates each time it executes. Best for standard production workloads.

```python
# Example: Daily updates
schedule = {
    "quartz_cron_expression": "0 0 * * ?",  # Daily at midnight
    "timezone_id": "America/Edmonton"
}
```

**Continuous Mode** - Continuously polls Petrinex and updates your table as new files are published. Best for near-real-time monitoring.

```python
# Example: Continuous streaming
schedule = {
    "continuous": {
        "pause_status": "UNPAUSED"
    }
}
```

Both modes automatically handle incremental loading based on Petrinex file update timestamps.

## Configuration Options

### Table Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `scd_type` | SCD_TYPE_1 | Supports SCD Type 1 and Type 2 |
| `from_date` | 6 months ago | Production month to start from (YYYY-MM format, e.g., "2023-01") |
| `updated_after` | Calculated from watermark | Load only files updated after this date (YYYY-MM-DD) |
| `request_timeout_s` | 60 | HTTP request timeout in seconds (increase for slow networks) |

**Key insights:**
- **`from_date`** - Use this for initial historical backfills (e.g., "2020-01" loads all data from January 2020 onwards)
- **`updated_after`** - Use this for incremental updates (e.g., "2025-01-01" loads only files Petrinex updated after Jan 1, 2025)
- The connector automatically tracks file update timestamps to avoid reprocessing unchanged data

### Choosing Your Schedule

| Schedule | Best For |
|----------|----------|
| **Daily (recommended)** | **Standard production use** - Petrinex publishes monthly, so daily checks are sufficient |
| Weekly | Cost-optimized for non-urgent data needs |
| Continuous | Real-time monitoring of new submissions/revisions |

## Output Schema

### `volumetrics` Table

The connector outputs data from Petrinex volumetric files with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `BA_ID` | string | Battery/Facility ID |
| `Licence_No` | string | Well licence number |
| `Licensee_BA_Code` | string | Operator BA code |
| `Licensee_Full_Name` | string | Operator full name |
| `Licencee_Abbr_Name` | string | Operator abbreviated name |
| `ProductionMonth` | string | Production month (YYYY-MM) |
| `PoolCode` | string | Geological pool code |
| `Product` | string | Product type (oil, gas, water, etc.) |
| `Activity` | string | Activity type (production, injection, etc.) |
| `Source` | string | Data source |
| `Volume` | string | Volume amount |
| `EnergyGJ` | string | Energy in gigajoules |
| `Hours` | string | Operating hours |
| `production_month` | string | Production month (normalized) |
| `file_updated_ts` | timestamp | When Petrinex updated this file |
| `source_url` | string | Download URL |
| `ingestion_time` | timestamp | When this record was last ingested (used for SCD sequencing) |

**Note:** Most fields are StringType to preserve data as published by Petrinex. Cast to numeric types in downstream processing as needed.

## How It Works

### Initial Load
Fetches from `from_date` (or last 6 months) using Petrinex file metadata, creating your baseline dataset.

### Incremental Updates
Each run fetches files updated since the last watermark, ensuring:
- **New production months** are captured as operators submit data
- **Revised submissions** are merged in (operators often revise historical months)
- **Efficient processing** - only changed files are downloaded and processed

**Example (daily schedule):**
- **Run 1 (Jan 15):** Fetch all files from 2024-07 → latest (initial load)
- **Run 2 (Jan 16):** Fetch only files updated since Jan 15 (incremental)
- **Run 3 (Jan 17):** Fetch only files updated since Jan 16 (incremental)

## Common Scenarios

### Starting Fresh with Historical Data

```json
"table_configuration": {
  "scd_type": "SCD_TYPE_1",
  "from_date": "2020-01"
}
```

### Incremental Updates Only (Skip Historical)

```json
"table_configuration": {
  "scd_type": "SCD_TYPE_1",
  "updated_after": "2025-01-01"
}
```

### Recovery from Pipeline Downtime

The connector automatically handles recovery - just restart your pipeline. It will fetch all files updated since the last successful run.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **High memory usage** | Petrinex files are large. Increase driver memory: `spark.conf.set("spark.driver.memory", "32g")` |
| **Timeout errors** | Increase `request_timeout_s` to 120 or higher for slow networks |
| **Missing data** | Check Petrinex website - data may not be published yet |
| **Encoding errors** | Handled automatically by the petrinex package |

## Important Notes

- **SCD Type 1 (recommended)** - Latest values overwrite previous records. Best for most use cases.
- **SCD Type 2** - Tracks historical changes using `ingestion_time`. Will create records each time files are refetched.
- **Production months** - Petrinex data is published monthly, typically 2-3 months after the production month.
- **Revisions are common** - Operators frequently revise historical submissions. The connector captures these automatically.
- **Large datasets** - Petrinex volumetric files can be several GB. Ensure adequate driver memory (16-32 GB recommended).
- **No rate limiting** - Petrinex is public data with no documented rate limits, but be respectful of their infrastructure.

## Resources

- **Petrinex Python API:** https://github.com/guanjieshen/petrinex-python-api
- **Petrinex Website:** https://www.petrinex.ca
- **Connector Code:** `sources/petrinex/petrinex.py`
- **Extend for other tables:** The connector architecture supports adding additional Petrinex datasets (well tests, facility data, etc.). See `petrinex.py` for implementation details.

