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

Create or update an ingestion pipeline for the **{{source_name}}** connector by reading its documentation, interactively building a pipeline spec, and running the CLI.

## Prerequisites

- Connector source at `src/databricks/labs/community_connector/sources/{{source_name}}/`
- Connector spec at `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`
- Connector README at `src/databricks/labs/community_connector/sources/{{source_name}}/README.md`
- Databricks CLI configured with a workspace profile
- Python 3.10+

---

## Step 0 — Determine operation mode

If `{{mode}}` was provided as `create` or `update`, use it and skip this step.

Otherwise, ask the user whether they want to **create a new pipeline** or **update an existing one** using `AskQuestion`.

If the user chooses **update**, ask for the **pipeline name** of the existing pipeline to update (unless `{{pipeline_name}}` was already provided).

Remember the chosen mode (`create` or `update`) for use in later steps.

---

## Step 1 — Read the connector documentation

Read these files to understand the source:

1. `src/databricks/labs/community_connector/sources/{{source_name}}/README.md` — supported tables, required/optional table options, connection parameters, pipeline spec examples
2. `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml` — connection parameters and `external_options_allowlist`

Extract and remember:
- The list of **supported tables** with their descriptions, ingestion types, and primary keys
- **Required and optional table-level options** for each table (e.g. `owner`, `repo`, `app_id`)
- **Connection parameters** (for the `create_connection` command hint)

---

## Step 2 — Collect deployment parameters from the user

Ask the user for the following information. Use `AskQuestion` where structured choices are possible; use text prompts otherwise.

### 2a. UC connection name

If `{{connection_name}}` was provided, use it and skip the rest of this sub-step.

Otherwise, ask whether the user already has a Unity Catalog connection for this source.

- **If yes**: ask for the connection name.
- **If no**: show them the command to create one, filling in the connection parameters from the connector spec:

```
community-connector create_connection {{source_name}} <CONNECTION_NAME> -o '<PARAMS_JSON>'
```

Where `<PARAMS_JSON>` contains the required connection parameters discovered in Step 1 (e.g. `{"token": "ghp_xxx"}` for GitHub, `{"api_token": "..."}` for AppsFlyer). List out each required and optional parameter with a short description so the user knows what to fill in.

Ask the user to run the command and provide the connection name they used.

### 2b. Default destination catalog and schema

Ask for:
- **catalog** — the Unity Catalog catalog for ingested tables (e.g. `main`)
- **schema** — the target schema (e.g. `raw_github`)

These are optional; if not provided they default to the pipeline's defaults.

### 2c. Pipeline name (Required)

In create mode, ask the user to choose a name (e.g. `my_github_pipeline`). In update mode, use the name collected in Step 0 and confirm it.

### 2d. Tables to ingest

Present the full list of supported tables discovered in Step 1 with a brief description of each. Let the user pick which tables they want to ingest. Offer an "all tables" shortcut.

### 2e. Per-table configuration

For each selected table, check the README for configuration options. There are two categories of per-table settings:

#### Destination overrides (set directly on the `table` object)

These override the pipeline-level defaults from Step 2b:

| Option | Description |
|---|---|
| `destination_catalog` | Target catalog for this specific table (defaults to pipeline's default) |
| `destination_schema` | Target schema for this specific table (defaults to pipeline's default) |
| `destination_table` | Target table name (defaults to `source_table`) |

Mention these are available but do not actively ask the user to configure them — most users rely on the pipeline-level defaults from Step 2b. Only include them in the spec if the user proactively requests per-table overrides.

#### Source-specific and common options (set inside `table_configuration`)

Present a clear summary for each table (or group of tables with the same options):

1. **Required options** — list each with its description. Ask the user to provide values. (e.g. `owner` and `repo` for GitHub repository-scoped tables, or `app_id` for AppsFlyer event reports)
2. **Optional options** — list each with its description and default value (if any). Ask whether the user wants to set any of them. This includes common options like `scd_type`, `primary_keys`, and `sequence_by`, as well as any source-specific options.

If a table has no required options but does have optional ones, still present the optional options so the user can decide whether to configure them.

If multiple tables share common options (e.g. all GitHub repo-scoped tables need the same `owner`/`repo`), ask once and reuse the values — but confirm with the user.

---

## Step 3 — Generate the pipeline spec

Build a pipeline spec JSON object from the collected information:

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
        "table_configuration": {
          "<option_key>": "<option_value>"
        }
      }
    }
  ]
}
```

Rules:
- `connection_name` is always required.
- Each selected table becomes an entry in `objects`.
- `source_table` is always required for each table.
- Only include `destination_catalog`, `destination_schema`, or `destination_table` if the user provided per-table overrides; omit them to use the pipeline defaults.
- Only include `table_configuration` if there are options to set.
- Omit optional options the user did not provide.

Show the generated spec to the user for review before proceeding. Let them request changes.

---

## Step 4 — Ensure the CLI tool is available

Run `community-connector --help` to check if the CLI is already installed. If the command succeeds, skip installation. If it fails (command not found), install it:

```bash
cd tools/community_connector && pip install -e . && cd ../..
```

---

## Step 5 — Deploy the pipeline

The create and update commands have identical syntax — only the verb differs. Use `create_pipeline` or `update_pipeline` based on the mode chosen in Step 0.

1. Write the spec to `tests/unit/sources/{{source_name}}/configs/{PIPELINE_NAME}_spec.json`.

2. Run:

```bash
community-connector <create_pipeline|update_pipeline> {{source_name}} <PIPELINE_NAME> \
  -ps tests/unit/sources/{{source_name}}/configs/pipeline_spec.json \
  [-c <CATALOG>] [-t <TARGET>]
```

Include `-c` and `-t` only if the user provided catalog/schema in Step 2b.

3. After successful execution, delete the temporary pipeline spec file.

4. Confirm `✓ Pipeline created!` or `✓ Pipeline updated!` and capture the **Pipeline URL** and **Pipeline ID**.

---

## Step 6 — Report results

Present the deployment summary:

```
Pipeline <created|updated> for {{source_name}}!

Connection: <CONNECTION_NAME>

Pipeline:
  Name: <PIPELINE_NAME>
  URL:  <PIPELINE_URL>
  ID:   <PIPELINE_ID>

Tables configured:
  - <TABLE_1>
  - <TABLE_2>
  ...

Next steps:
  - Open the Pipeline URL to view the pipeline
  - Or run: community-connector run_pipeline <PIPELINE_NAME>
```

---

## Rules

- Steps run **sequentially** — each depends on the prior step's output.
- Always read the connector README and spec first to provide source-specific guidance.
- If a CLI command fails, report the error clearly — do not retry silently.
- Do not modify the connector source code, spec, or README during deployment.
- Clean up any temporary files (e.g., `pipeline_spec.json`) after use.
- Do not push to git — deployment is a workspace operation, not a code change.
