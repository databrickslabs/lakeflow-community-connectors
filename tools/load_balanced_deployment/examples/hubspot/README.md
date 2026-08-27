# HubSpot Connector - Load Balanced Deployment Examples

This directory contains preset CSV files and configuration for deploying the HubSpot connector with load-balanced multi-pipeline architecture.

## Available Preset CSV Files

### 1. `preset_all_tables.csv`
Single pipeline with all HubSpot tables.
- **Use case:** Simple deployment, all tables in one pipeline
- **Tables:** 10 tables total

### 2. `preset_by_ingestion_type.csv`
Pipelines grouped by ingestion type (all are snapshot).
- **Use case:** Standard deployment (single snapshot pipeline)
- **Pipelines:** 1 (snapshot)

### 3. `preset_by_category_and_ingestion.csv`
Pipelines grouped by logical category AND ingestion type.
- **Use case:** Maximum parallelism and logical separation
- **Pipelines:** 2 groups
  - `crm_objects_snapshot`: contacts, companies, deals, tickets, deal_split
  - `engagement_snapshot`: calls, emails, meetings, tasks, notes

## Category Mapping

The HubSpot connector doesn't have built-in `TABLES_*` attributes, so we use a category prefix map to classify tables:

**Category Definitions:**
- **crm_objects**: Core CRM entities (contacts, companies, deals, tickets, deal_split)
- **engagement**: Activity/engagement records (calls, emails, meetings, tasks, notes)

See `category_prefix_map.json` for the full mapping.

## Automatic Discovery with Category Prefix Map

To automatically discover and classify HubSpot tables with custom categories:

```bash
python tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name hubspot \
  --config-json '{"access_token": "your_hubspot_token"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat tools/load_balanced_deployment/examples/hubspot/category_prefix_map.json)" \
  --output-csv hubspot_discovered.csv
```

**Alternative: Inline JSON**
```bash
python tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name hubspot \
  --config-json '{"access_token": "xxx"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json '{"contact":"crm_objects","compan":"crm_objects","deal":"crm_objects","ticket":"crm_objects","call":"engagement","email":"engagement","meeting":"engagement","task":"engagement","note":"engagement"}' \
  --output-csv hubspot_discovered.csv
```

**Note on Prefix Matching:**
- `compan` matches both `companies` and `company` (if present)
- `deal` matches both `deals` and `deal_split`
- Prefix matching is case-insensitive and uses the first token before `_` in table names

## Deployment Instructions

### Step 1: Choose or Generate CSV
```bash
# Option A: Use preset CSV
cp tools/load_balanced_deployment/examples/hubspot/preset_by_category_and_ingestion.csv my_hubspot_deployment.csv

# Option B: Generate from live connector
python tools/load_balanced_deployment/discover_and_classify_tables.py \
  --connector-name hubspot \
  --config-json '{"access_token": "xxx"}' \
  --group-by category_and_ingestion_type \
  --category-prefix-map-json "$(cat tools/load_balanced_deployment/examples/hubspot/category_prefix_map.json)" \
  --output-csv my_hubspot_deployment.csv
```

### Step 2: Customize (Optional)
Edit the CSV to:
- Add association filtering via table_configuration
- Adjust sync schedules
- Add property filters
- Configure pagination settings

### Step 3: Deploy Pipelines
```bash
python tools/load_balanced_deployment/deploy_load_balanced_pipelines.py \
  --csv-file my_hubspot_deployment.csv \
  --connector-name hubspot \
  --connection-name my_hubspot_connection \
  --catalog main \
  --target_schema hubspot_bronze \
  --base-pipeline-name "HubSpot Load Balanced"
```

## Table Schema

| Column | Description | Example |
|--------|-------------|---------|
| pipeline_name | Unique name for the pipeline | `crm_objects_snapshot` |
| table_name | Source table name | `contacts` |
| destination_table | Target table name (optional) | `contacts` |
| scd_type | Ingestion mode | `SCD_TYPE_1` |
| primary_keys | Comma-separated PKs | `id` |
| table_configuration | JSON options (optional) | `{"properties": ["email", "firstname"]}` |
| schedule | Cron expression | `0 */4 * * *` (every 4 hours) |
| weight | Pipeline priority | `10` |

## Ingestion Types by Table

**All tables use Snapshot (SCD_TYPE_1):**
- contacts
- companies
- deals
- tickets
- deal_split
- calls
- emails
- meetings
- tasks
- notes

HubSpot tracks changes internally via `updatedAt` timestamps, so snapshot mode with cursor-based incremental reads is the recommended approach.

## Recommended Schedules

- **CRM Objects (high volume):** Every 4 hours (`0 */4 * * *`)
- **Engagement records (medium volume):** Every 6 hours (`0 */6 * * *`)

Adjust based on your HubSpot API limits and data freshness requirements.

## Advanced Table Configuration

### Property Filtering
Limit which properties to sync (reduces API calls and storage):
```json
{"properties": ["email", "firstname", "lastname", "company"]}
```

### Association Filtering
Sync associated records:
```json
{"associations": ["companies", "deals"]}
```

### Combined Example
```csv
pipeline_name,table_name,destination_table,scd_type,primary_keys,table_configuration,schedule,weight
crm_objects_snapshot,contacts,contacts,SCD_TYPE_1,id,"{\"properties\": [\"email\", \"firstname\", \"lastname\"], \"associations\": [\"companies\"]}",0 */4 * * *,10
```

## Notes

- HubSpot API has rate limits based on your subscription tier
- The connector uses cursor-based pagination with `updatedAt` field for incremental reads
- All HubSpot objects use `id` as the primary key
- The connector requires a HubSpot Private App Access Token or OAuth token
- Custom objects are automatically discovered via the HubSpot API
