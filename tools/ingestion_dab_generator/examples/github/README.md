# GitHub example: discover repos → CSV → balanced ingestion pipelines

This example demonstrates the reusable pattern:

1) **Discovery adapter** produces a standard CSV (one row per unit of work)
2) **Generic generator** turns the CSV into DAB YAML for ingestion pipelines

## Discovery script

- `discover_github_repos.py`

It lists repositories for a GitHub org/user and emits one CSV row per repo for a selected table (default: `issues`).

### Example

```bash
python tools/ingestion_dab_generator/examples/github/discover_github_repos.py \
  --token "$GITHUB_TOKEN" \
  --org databrickslabs \
  --table issues \
  --start-date 2024-01-01T00:00:00Z \
  --output-csv /tmp/github_issues.csv \
  --dest-catalog main \
  --dest-schema bronze \
  --schedule "*/30 * * * *"
```

This emits rows with:
- `table_options_json`: `{ "owner": "<org>", "repo": "<repo>", ... }`
- `weight`: proxy metric for load balancing (open issues + repo size)

## Generate YAML (auto load-balance)

```bash
python tools/ingestion_dab_generator/generate_ingestion_dab_yaml.py \
  --input-csv /tmp/github_issues.csv \
  --output-yaml /tmp/github_ingestion.yml \
  --connector-name github \
  --connection-name <YOUR_GITHUB_CONNECTION_NAME> \
  --dest-catalog main \
  --dest-schema bronze \
  --num-pipelines 3 \
  --emit-jobs
```

## Notes

- The GitHub connector requires a PAT at connection level; create a UC connection and set `externalOptionsAllowList` per `sources/github/README.md`.
- This example uses “one destination table per repo” to keep units independent and balanceable.
