---
name: deploy-connector
description: Guide the user through creating or updating a pipeline for a source connector — read the docs, build a pipeline spec interactively, and run create_pipeline or update_pipeline.
args:
  - name: source_name
    description: The name of the source connector (e.g. github, stripe, appsflyer)
    required: true
  - name: connection_name
    description: The Unity Catalog connection name to use. If omitted, the user will be prompted for it.
    required: false
  - name: mode
    description: "Whether to create a new pipeline or update an existing one. Values: 'create' or 'update'. If omitted, the user will be prompted."
    required: false
  - name: pipeline_name
    description: The name of the existing pipeline to update. Only used when mode is 'update'. If omitted in update mode, the user will be prompted.
    required: false
---

# Deploy Connector

Create or update an ingestion pipeline for **{{source_name}}** by reading its docs, interactively building a pipeline spec, and running the CLI.

## Prerequisites

- Connector source, spec (`connector_spec.yaml`), and README exist under `src/databricks/labs/community_connector/sources/{{source_name}}/`
- Databricks CLI configured, Python 3.10+

---

## Step 0 — Determine operation mode

If `{{mode}}` is `create` or `update`, use it. Otherwise ask via `AskQuestion`.

If **update**, also collect the pipeline name (unless `{{pipeline_name}}` was provided).

---

## Step 1 — Read the connector documentation

Read these files to understand the source:

1. `src/databricks/labs/community_connector/sources/{{source_name}}/README.md`
2. `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`

Extract: **supported tables** (descriptions, ingestion types, primary keys), **required/optional table level options**, and **connection parameters**.

---

## Step 2 — Collect deployment parameters

Use `AskQuestion` for structured choices; text prompts otherwise.

### 2a. UC connection name

If `{{connection_name}}` provided, use it. Otherwise ask if the user has one.

- **Yes**: ask for the name.
- **No**: show the `create_connection` command:
  ```
  community-connector create_connection {{source_name}} <CONNECTION_NAME> -o '<CREDENTIALS_JSON>'
  ```
  List each required/optional **credential** parameter from the spec (e.g. `token`, `api_key`) with a short description. Do **not** ask for `externalOptionsAllowList` — the CLI reads the connector spec and adds it automatically. Ask the user to run the command and provide the connection name.

### 2b. Default destination catalog and schema

Ask for **catalog** (e.g. `main`) and **schema** (e.g. `raw_github`). Both optional — omitted values use pipeline defaults.

### 2c. Pipeline name

Create mode: ask user to choose a name. Update mode: confirm the name from Step 0.

### 2d. Tables to ingest

Present the supported tables from Step 1 with brief descriptions. Let the user pick; offer an "all tables" shortcut.

### 2e. Per-table configuration

For each selected table, check the README for two categories of options:

**Destination overrides** (`destination_catalog`, `destination_schema`, `destination_table`) — set on the `table` object. Mention these are available but don't actively prompt; only include if the user requests per-table overrides.

**Source-specific and common options** (set inside `table_configuration`):
- **Required**: list each with description, ask for values (e.g. `owner`/`repo` for GitHub)
- **Optional**: list each with description and default, ask if the user wants to set any (includes `scd_type`, `primary_keys`, `sequence_by`, plus source-specific options e.g. `start_date` to filter, `max_records_per_batch` to control batch size)

If multiple tables share options (e.g. same `owner`/`repo`), ask once and reuse — confirm with user.

---

## Step 3 — Generate the pipeline spec

Build the spec JSON:

```json
{
  "connection_name": "<CONNECTION_NAME>",
  "objects": [
    {
      "table": {
        "source_table": "<TABLE_NAME>",
        "destination_catalog": "<CATALOG>",
        "destination_schema": "<SCHEMA>",
        "destination_table": "<TABLE>",
        "table_configuration": { "<key>": "<value>" }
      }
    }
  ]
}
```

- `connection_name` and `source_table` are always required.
- Omit `destination_*` fields unless the user set per-table overrides.
- Omit `table_configuration` if empty.

Show the spec to the user for review before proceeding.

---

## Step 4 — Ensure the CLI tool is available

Run `community-connector --help`. If it fails, install:

```bash
cd tools/community_connector && pip install -e . && cd ../..
```

---

## Step 5 — Deploy the pipeline

Use `create_pipeline` or `update_pipeline` based on the mode from Step 0.

1. Write the spec to `tests/unit/sources/{{source_name}}/configs/{PIPELINE_NAME}_spec.json`.

2. Run:
   ```bash
   community-connector <create_pipeline|update_pipeline> {{source_name}} <PIPELINE_NAME> \
     -ps tests/unit/sources/{{source_name}}/configs/{PIPELINE_NAME}_spec.json \
     [-c <CATALOG>] [-t <TARGET>]
   ```
   Include `-c`/`-t` only if catalog/schema were provided in Step 2b.

3. After success, delete the spec file.

4. Capture the **Pipeline URL** and **Pipeline ID** from the output.

---

## Step 6 — Report results

```
Pipeline <created|updated> for {{source_name}}!

Connection: <CONNECTION_NAME>
Pipeline:  <PIPELINE_NAME>
URL:       <PIPELINE_URL>
ID:        <PIPELINE_ID>

Tables: <TABLE_1>, <TABLE_2>, ...

Next steps:
  - Open the Pipeline URL to view the pipeline
  - Or run: community-connector run_pipeline <PIPELINE_NAME>
```

---

## Rules

- Steps run **sequentially** — each depends on the prior step's output.
- Always read the connector README and spec first.
- If a CLI command fails, report the error clearly — do not retry silently.
- Do not modify connector source code, spec, or README during deployment.
- Clean up temporary files after use.
- Do not push to git.
