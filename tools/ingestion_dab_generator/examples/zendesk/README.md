# Zendesk example: tables → CSV → balanced ingestion pipelines

Zendesk tables are known ahead of time (see `sources/zendesk/zendesk.py`).
This example emits a standard CSV with one row per table and a simple proxy weight map.

## Discovery script

- `discover_zendesk_tables.py`

### Example

```bash
python tools/ingestion_dab_generator/examples/zendesk/discover_zendesk_tables.py \
  --output-csv /tmp/zendesk_tables.csv \
  --dest-catalog main \
  --dest-schema bronze \
  --schedule "0 */2 * * *"   # every 2 hours
```

## Generate YAML (auto load-balance)

```bash
python tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py \
  --input-csv /tmp/zendesk_tables.csv \
  --output-yaml /tmp/zendesk_ingestion.yml \
  --connector-name zendesk \
  --connection-name <YOUR_ZENDESK_CONNECTION_NAME> \
  --dest-catalog main \
  --dest-schema bronze \
  --num-pipelines 2 \
  --emit-jobs
```

## Notes

- Zendesk auth is connection-level (subdomain/email/api_token). Table options are typically not required.
- The weights in the discovery script are placeholders; tune them with observed row counts or runtime.
