---
name: deploy-connector
description: Deploy a fully developed connector to a Databricks workspace — create a UC connection and ingestion pipeline using the community-connector CLI.
---

# Deploy Connector

Deploy the **{{source_name}}** connector to a Databricks workspace by creating a UC connection and an ingestion pipeline.

## Prerequisites

- Connector implementation at `src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}.py`
- Connector spec at `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`
- Connector README at `src/databricks/labs/community_connector/sources/{{source_name}}/README.md`
- Databricks CLI configured with a workspace profile (see `tools/community_connector/README.md`)
- Python 3.10+

---

## Step 1 — Ensure dev_config.json exists

Check if `tests/unit/sources/{{source_name}}/configs/dev_config.json` exists.

- **If it exists:** proceed to Step 2.
- **If it does not exist:** trigger the `authenticate-source` skill for {{source_name}} and wait for it to complete before proceeding. The skill will generate the connector spec (if needed), collect credentials via a browser form, and validate auth.

---

## Step 2 — Collect deployment parameters from the user

Ask the user (via `AskQuestion`) for the following deployment parameters:

### Required

| Parameter | Description | Example |
|-----------|-------------|---------|
| `connection_name` | Name for the UC connection | `my_{{source_name}}_conn` |
| `pipeline_name` | Name for the ingestion pipeline | `my_{{source_name}}_pipeline` |

### Optional

| Parameter | Description | Default |
|-----------|-------------|---------|
| `catalog` | UC target catalog | pipeline default |
| `target` | Target schema | pipeline default |
| `tables` | List of tables to ingest with optional per-table configuration | all tables (uses default template) |

If the user wants to specify tables, collect them in **pipeline spec** format:

```yaml
objects:
  - table:
      source_table: "<TABLE_NAME>"
      table_configuration:
        scd_type: "<SCD_TYPE_1 | SCD_TYPE_2 | APPEND_ONLY>"
        primary_keys:
          - "<PK_COL>"
        # additional table options as needed
```

To help the user choose tables, you can read the connector's README or run `connector.list_tables()` to show available tables.

---

## Step 3 — Install the CLI tool

Ensure the community-connector CLI is installed:

```bash
cd tools/community_connector && pip install -e . && cd ../..
```

---

## Step 4 — Create the UC connection

Read `tests/unit/sources/{{source_name}}/configs/dev_config.json` and use its contents as the connection options.

```bash
community-connector create_connection {{source_name}} <CONNECTION_NAME> \
  -o '<DEV_CONFIG_JSON_CONTENTS>' \
  --spec src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml
```

- `<CONNECTION_NAME>`: the connection name from Step 2.
- `<DEV_CONFIG_JSON_CONTENTS>`: the full JSON string from `dev_config.json`.
- The `--spec` flag points to the local connector spec for validation.

Verify the output shows `✓ Connection created!`. If it fails, report the error clearly.

---

## Step 5 — Create the ingestion pipeline

Build and run the `create_pipeline` command based on the user's choices from Step 2.

### If the user did NOT specify tables (simple mode):

```bash
community-connector create_pipeline {{source_name}} <PIPELINE_NAME> \
  -n <CONNECTION_NAME> \
  [-c <CATALOG>] [-t <TARGET>]
```

### If the user specified tables (pipeline spec mode):

1. Create a temporary pipeline spec file at `tests/unit/sources/{{source_name}}/configs/pipeline_spec.yaml` with the user's table configuration:

```yaml
connection_name: "<CONNECTION_NAME>"
objects:
  - table:
      source_table: "<TABLE_1>"
      table_configuration:
        scd_type: "SCD_TYPE_1"
  - table:
      source_table: "<TABLE_2>"
      table_configuration:
        scd_type: "SCD_TYPE_2"
        primary_keys:
          - "id"
```

2. Run:

```bash
community-connector create_pipeline {{source_name}} <PIPELINE_NAME> \
  -ps tests/unit/sources/{{source_name}}/configs/pipeline_spec.yaml \
  [-c <CATALOG>] [-t <TARGET>]
```

3. After successful creation, delete the temporary pipeline spec file.

Verify the output shows `✓ Pipeline created!` and note the **Pipeline URL** and **Pipeline ID**.

---

## Step 6 — Report results

Present the deployment summary to the user:

```
Deployment complete for {{source_name}}!

Connection:
  Name: <CONNECTION_NAME>

Pipeline:
  Name: <PIPELINE_NAME>
  URL:  <PIPELINE_URL>
  ID:   <PIPELINE_ID>

Next steps:
  - Open the Pipeline URL to view and run the pipeline
  - Or run: community-connector run_pipeline <PIPELINE_NAME>
```

---

## Rules

- Steps run **sequentially** — each depends on the prior step's output.
- If a CLI command fails, report the error clearly — do not retry silently.
- Do not modify the connector source code, spec, or README during deployment.
- Clean up any temporary files (e.g., `pipeline_spec.yaml`) after use.
- Do not push to git — deployment is a workspace operation, not a code change.
