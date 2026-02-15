# NEXT STEPS: Testing the Credential Fix

## What We Just Fixed ✅
**ROOT CAUSE** (identified by Perplexity & Gemini):
- Databricks stores connection credentials in `properties` field
- PySpark DataSource connectors receive `options` field (NOT `properties`)
- The connector was validating credentials in `__init__()` before `table_configuration` was merged
- Result: Connector crashed during metadata discovery phase

**SOLUTION**:
- Moved credential validation from `__init__()` to `_get_access_token()` (lazy/just-in-time)
- Now connector can initialize for metadata discovery without credentials
- Credentials get passed via `table_configuration` and merged into options later
- Validation only happens when actually connecting to Teams API

## How It Works Now
1. Pipeline starts → connector initializes (no credentials needed yet)
2. Metadata discovery happens (`_lakeflow_metadata` table)
3. Pipeline reads actual table → `table_configuration` credentials merge into options
4. Connector calls `_get_access_token()` → validates credentials → connects to Teams

## What You Need to Do in Databricks

### Step 1: Pull Latest Code from GitHub
**In your Databricks workspace:**
1. Navigate to Repos
2. Find your `lakeflow-community-connectors-teams` repo
3. Click the Git icon or branch dropdown
4. Pull latest changes from `master` branch

### Step 2: Update Your Pipeline Code
**Edit your `ingest.py` to pass credentials via `table_configuration`:**

```python
# Import and register the Microsoft Teams connector
from sources.microsoft_teams._generated_microsoft_teams_python_source import register_lakeflow_source
register_lakeflow_source(spark)

# Import the ingestion pipeline
from pipeline.ingestion_pipeline import ingest

# Pipeline specification - WITH CREDENTIALS IN TABLE CONFIG
pipeline_spec = {
    "connection_name": "microsoft_teams_connection",
    "objects": [
        {
            "table": {
                "source_table": "teams",
                "table_configuration": {
                    "tenant_id": "YOUR_TENANT_ID",
                    "client_id": "YOUR_CLIENT_ID",
                    "client_secret": "YOUR_CLIENT_SECRET"
                }
            }
        }
    ]
}

# Run the ingestion
ingest(spark, pipeline_spec)
```

### Step 3: Run Your Pipeline
1. Start the pipeline
2. Check the output for DEBUG messages showing what options are received
3. Pipeline should now successfully connect to Microsoft Teams API

## Expected Outcome

The pipeline should now:
1. ✅ Initialize the connector (no error during metadata discovery)
2. ✅ Receive credentials via `table_configuration` when reading the actual table
3. ✅ Validate credentials in `_get_access_token()`
4. ✅ Successfully authenticate with Microsoft Teams API
5. ✅ Fetch teams data

If you see different errors (like auth failures, permission issues, etc.), share them and we'll debug those next!
