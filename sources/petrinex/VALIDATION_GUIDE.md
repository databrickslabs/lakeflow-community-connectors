# Petrinex Connector - Data Validation Guide

This guide helps you validate that the Petrinex connector is pulling data correctly.

## Why Validate in Databricks?

The connector requires:
- **PySpark 3.0+** (has compatibility issues on macOS with Apple Silicon)
- **petrinex package** (downloads large files from Petrinex)
- **Network access** (to fetch data from Petrinex PublicData)

These are all available in Databricks, making it the ideal environment for validation.

## Quick Validation (5 minutes)

### Step 1: Create a Databricks Notebook

1. Open your Databricks workspace
2. Create a new Python notebook
3. Attach to a cluster (recommended: DBR 13.3+ LTS)

### Step 2: Install the petrinex Package

```python
%pip install petrinex
```

### Step 3: Test Direct Data Fetch

```python
from petrinex import PetrinexVolumetricsClient
from datetime import datetime, timedelta

# Create client
client = PetrinexVolumetricsClient(spark=spark, jurisdiction="AB")

# Fetch last 3 months of data (adjust as needed)
three_months_ago = (datetime.now() - timedelta(days=90)).strftime("%Y-%m")
print(f"Fetching data from {three_months_ago}...")

df = client.read_spark_df(from_date=three_months_ago)

# Display results
print(f"Records fetched: {df.count():,}")
display(df.limit(10))
```

**Expected Results:**
- ✓ Data fetch completes (may take 1-2 minutes)
- ✓ Thousands to millions of records returned
- ✓ Schema includes: BA_ID, ProductionMonth, Product, Activity, Volume, file_updated_ts, source_url

### Step 4: Test the Connector

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<your-repo>/lakeflow-community-connectors/sources/petrinex")

from petrinex import LakeflowConnect

# Create connector
connector = LakeflowConnect({})

# Test list_tables
print("Supported tables:", connector.list_tables())

# Test get_table_schema
schema = connector.get_table_schema("volumetrics", {})
print(f"\nSchema has {len(schema.fields)} fields:")
for field in schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# Test read_table_metadata
metadata = connector.read_table_metadata("volumetrics", {})
print(f"\nMetadata:")
print(f"  Primary keys: {metadata['primary_keys']}")
print(f"  Cursor field: {metadata['cursor_field']}")
print(f"  Sequence by: {metadata['sequence_by']}")
print(f"  Ingestion type: {metadata['ingestion_type']}")

# Test read_table (fetch data)
three_months_ago = (datetime.now() - timedelta(days=90)).strftime("%Y-%m")
table_options = {"from_date": three_months_ago}

print(f"\nFetching data through connector from {three_months_ago}...")
records_iter, next_offset = connector.read_table("volumetrics", {}, table_options)

# Collect and display
records = list(records_iter)
print(f"Records fetched: {len(records):,}")
print(f"Next offset: {next_offset}")

if len(records) > 0:
    print("\nSample record:")
    sample = records[0]
    for key, value in list(sample.items())[:10]:
        print(f"  {key}: {value}")
```

**Expected Results:**
- ✓ list_tables returns ['volumetrics']
- ✓ Schema has 17 fields including BA_ID, Product, ingestion_time
- ✓ Metadata shows composite primary key and CDC ingestion type
- ✓ Data fetch returns records with all required fields
- ✓ next_offset contains high_watermark timestamp

## Full Pipeline Validation

### Step 5: Test with Full Pipeline Spec

Create a notebook with this configuration:

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "petrinex"

# Pipeline configuration
pipeline_spec = {
    "connection_name": "petrinex_test",
    "objects": [
        {
            "table": {
                "source_table": "volumetrics",
                "destination_catalog": "main",
                "destination_schema": "petrinex_validation",
                "destination_table": "volumetrics_test",
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",
                    "from_date": "2024-10",  # Last few months
                    "request_timeout_s": "120"
                }
            }
        }
    ]
}

# Register and run
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Ingest data
ingest(spark, pipeline_spec)

# Verify results
result_df = spark.table("main.petrinex_validation.volumetrics_test")
print(f"Total records in target table: {result_df.count():,}")
display(result_df.limit(10))
```

**Expected Results:**
- ✓ Pipeline runs successfully
- ✓ Target table is created in Unity Catalog
- ✓ Data is loaded with all fields
- ✓ Subsequent runs perform incremental updates

## Validation Checklist

Use this checklist to confirm the connector is working:

### Package & Environment
- [ ] petrinex package installed successfully
- [ ] PySpark available (should be automatic in Databricks)
- [ ] Cluster has adequate memory (16GB+ driver recommended)

### Direct Client Test
- [ ] PetrinexVolumetricsClient can be instantiated
- [ ] Data fetch completes without errors
- [ ] Records are returned (thousands to millions)
- [ ] Provenance fields present (production_month, file_updated_ts, source_url)

### Connector Interface
- [ ] list_tables() returns ['volumetrics']
- [ ] get_table_schema() returns valid StructType with 17 fields
- [ ] read_table_metadata() returns correct primary_keys, cursor_field, sequence_by
- [ ] read_table() successfully fetches data

### Data Quality
- [ ] Records have all required fields (BA_ID, Product, Activity, etc.)
- [ ] ingestion_time is populated (current timestamp)
- [ ] file_updated_ts is populated (Petrinex file timestamp)
- [ ] production_month matches expected format (YYYY-MM)
- [ ] Primary key fields are not null

### Pipeline Integration
- [ ] Unity Catalog connection can be created
- [ ] Pipeline spec validates successfully
- [ ] ingest() runs without errors
- [ ] Target table is created in Unity Catalog
- [ ] Data is loaded correctly
- [ ] Incremental updates work (run twice, verify count increases)

## Troubleshooting

### Issue: "petrinex package not found"
**Solution:** Run `%pip install petrinex` in your notebook

### Issue: "Timeout errors"
**Solution:** Increase `request_timeout_s` to 180 or higher in table_configuration

### Issue: "Out of memory"
**Solution:** 
- Increase driver memory to 32GB
- Reduce date range (fetch fewer months)

### Issue: "No data returned"
**Solution:**
- Check Petrinex website - recent months may not be published yet
- Try an older date range (e.g., 6 months ago)

### Issue: "Permission errors on local machine"
**Solution:** 
- This is expected - PySpark has issues on some local environments
- Always validate in Databricks

## Expected Data Characteristics

When validation succeeds, you should see:

**Volume:**
- Historical load (1 year): ~10-50 million records
- Monthly load: ~1-5 million records
- File sizes: 100MB-2GB per month

**Schema:**
- 17 fields total
- 13 core Petrinex fields (BA_ID, Product, Volume, etc.)
- 4 provenance fields (production_month, file_updated_ts, source_url, ingestion_time)

**Data Types:**
- Most fields are StringType (to preserve CSV fidelity)
- Timestamps: file_updated_ts, ingestion_time
- Cast to numeric types in downstream processing

**Primary Key:**
- Composite: (BA_ID, ProductionMonth, Product, Activity, Source)
- Uniquely identifies each volumetric record

## Success Criteria

✅ The connector is working correctly if:

1. **Direct client fetch** returns data from Petrinex
2. **Connector interface** methods all succeed
3. **Data quality** checks pass (all fields present)
4. **Pipeline integration** loads data into Unity Catalog
5. **Incremental updates** work on subsequent runs

## Next Steps

Once validation passes:

1. **Configure production pipeline** with appropriate:
   - `from_date` for historical backfill
   - `scd_type` (SCD_TYPE_1 recommended)
   - Schedule (daily recommended)

2. **Set up monitoring** for:
   - Record counts per run
   - Pipeline execution time
   - Failed runs

3. **Document for your team**:
   - Where the data lives (catalog.schema.table)
   - Update frequency
   - Data freshness (Petrinex lag ~2-3 months)

## Support

If validation fails:
1. Check this guide's troubleshooting section
2. Review connector logs in Databricks
3. Verify Petrinex website accessibility
4. Check cluster configuration (memory, DBR version)

## Additional Resources

- **Connector README:** `sources/petrinex/README.md`
- **API Documentation:** `sources/petrinex/petrinex_api_doc.md`
- **Configuration Guide:** `sources/petrinex/configs/README.md`
- **Petrinex Website:** https://www.petrinex.ca
- **Python API:** https://github.com/guanjieshen/petrinex-python-api

