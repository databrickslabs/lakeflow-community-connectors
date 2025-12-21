# HubSpot example: objects → CSV → balanced ingestion pipelines

HubSpot objects are typically known ahead of time (contacts/companies/deals/etc.).
This example emits a standard CSV with one row per object and a simple proxy weight map.

## Discovery script

- `discover_hubspot_tables.py`

### Example

```bash
python tools/ingestion_dab_generator/examples/hubspot/discover_hubspot_tables.py \
  --output-csv /tmp/hubspot_tables.csv \
  --dest-catalog main \
  --dest-schema bronze \
  --schedule "0 */2 * * *"   # every 2 hours
```

Optional: include custom objects:

```bash
python tools/ingestion_dab_generator/examples/hubspot/discover_hubspot_tables.py \
  --include-custom \
  --access-token "$HUBSPOT_TOKEN" \
  --output-csv /tmp/hubspot_tables.csv
```

## Generate YAML (auto load-balance)

```bash
python tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py \
  --input-csv /tmp/hubspot_tables.csv \
  --output-yaml /tmp/hubspot_ingestion.yml \
  --connector-name hubspot \
  --connection-name <YOUR_HUBSPOT_CONNECTION_NAME> \
  --dest-catalog main \
  --dest-schema bronze \
  --num-pipelines 2 \
  --emit-jobs
```

## Notes

- HubSpot auth is connection-level (`access_token`). Table options are typically not required.
- The weights in the discovery script are placeholders; tune them with observed row counts or runtime.
