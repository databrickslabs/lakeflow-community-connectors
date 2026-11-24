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

Create or update a **managed ingestion pipeline** for **{{source_name}}** by reading its docs, interactively building a full pipeline spec, and running the CLI. The CLI builds the connector's Python wheels, uploads them to a UC Volume, and creates a pipeline whose `ingestion_definition` is set from the spec — no repo clone.

## Prerequisites

- Connector source, spec (`connector_spec.yaml`), and README exist under `src/databricks/labs/community_connector/sources/{{source_name}}/`
- Databricks CLI configured, Python 3.10+

---

## Step 0 — Determine operation mode

If `{{mode}}` is `create` or `update`, use it. Otherwise ask via `AskUserQuestion`.

If **update**, also collect the pipeline name (unless `{{pipeline_name}}` was provided).

---

## Step 1 — Read the connector documentation

Read these files to understand the source:

1. `src/databricks/labs/community_connector/sources/{{source_name}}/README.md`
2. `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`

Extract: **supported tables** (descriptions, ingestion types, primary keys), **required/optional table level options**, and **connection parameters**.

---

## Step 2 — Collect deployment parameters

Use `AskUserQuestion` for structured choices; text prompts otherwise.

### 2a. UC connection name

If `{{connection_name}}` provided, use it. Otherwise ask if the user has one.

- **Yes**: ask for the name.
- **No**: show the `create_connection` command:
  ```
  community-connector create_connection {{source_name}} <CONNECTION_NAME> -o '<CREDENTIALS_JSON>'
  ```
  List each required/optional **credential** parameter from the spec (e.g. `token`, `api_key`) with a short description. Do **not** ask for `externalOptionsAllowList` — the CLI reads the connector spec and adds it automatically. Ask the user to run the command and provide the connection name.

  The command is the same whether the connector uses static credentials or OAuth — the CLI reads the auth mode and its required options from the spec's `connection.oauth` block, so you don't choose a mode here. The one runtime difference to flag for the user: an interactive flow (`u2m` / `u2m_per_user`) opens a browser at connection creation for them to log in and authorize (and the source's OAuth app must have the redirect URI registered), whereas `m2m` completes machine-to-machine with no browser.

### 2b. Default destination catalog and schema

Ask for **catalog** (e.g. `main`) and **schema** (e.g. `raw_example`). These set the pipeline-level `catalog`/`schema` and serve as the **default** destination for each table. Individual tables can override them with per-table `destination_catalog`/`destination_schema` (see 2e).

### 2c. Pipeline name

Create mode: ask user to choose a name. Update mode: confirm the name from Step 0.

### 2d. Tables to ingest

Present the supported tables from Step 1 with brief descriptions. Let the user pick; offer an "all tables" shortcut.

### 2e. Per-table configuration

For each selected table, check the README for three categories of options:

**Destination overrides** (`destination_table`, and optionally `destination_catalog`/`destination_schema`) — set on the `table` object. `destination_catalog`/`destination_schema` default to the values from 2b; only override per table if the user asks. Mention `destination_table` is available but don't actively prompt.

**Source-specific options** (set inside `connector_options.community_connector_options.options`):
- **Required**: list each with description, ask for values (e.g. `owner`/`repo` for GitHub, `category` for products, `window_seconds` for metrics)
- **Optional**: list each with description and default, ask if the user wants to set any (e.g. `start_date` to filter, `max_records_per_batch` to control batch size)

**Ingestion controls** (set inside `table_configuration`): `scd_type`, `primary_keys`, `sequence_by`. Ask only if the user wants to override the connector defaults.

If multiple tables share options (e.g. same `owner`/`repo`), ask once and reuse — confirm with user.

---

## Step 3 — Generate the full pipeline spec

> If the user just wants an **empty pipeline** (a connection but no tables yet,
> to add later), skip the spec entirely: run `create_pipeline` with only
> `-n <CONNECTION_NAME> -c <CATALOG> -t <SCHEMA>` (see Step 5). Otherwise build
> the spec below.

Build a **complete pipeline spec** as JSON or YAML. The CLI sends it to the
pipelines API as-is, only adding the managed-ingestion configuration flag, the
`ingestion_definition.source_type`, and the connector wheel `environment`. Since
the CLI does not backfill per-table destinations in full-spec mode, spell out
`destination_catalog`/`destination_schema` on every table (using the 2b values).

```yaml
name: <PIPELINE_NAME>
catalog: <CATALOG>
schema: <SCHEMA>
serverless: true
channel: PREVIEW
ingestion_definition:
  connection_name: <CONNECTION_NAME>
  objects:
    - table:
        source_schema: default
        source_table: <TABLE_NAME>
        destination_catalog: <CATALOG>
        destination_schema: <SCHEMA>
        destination_table: <TABLE>          # optional; defaults to source_table
        connector_options:
          community_connector_options:
            options:
              <source_option_key>: <value>  # e.g. owner, repo, start_date
        table_configuration:                 # optional ingestion controls
          scd_type: SCD_TYPE_1
          primary_keys: [<col>]
```

- Top-level `name`, `catalog`, `schema` are required; keep `serverless: true`
  and `channel: PREVIEW`.
- `connection_name` and `source_table` are always required per definition/table.
- `source_schema` defaults to `default` if omitted.
- **Source-specific options** (e.g. `owner`, `repo`, `start_date`) go under
  `connector_options.community_connector_options.options` — NOT
  `table_configuration`.
- `table_configuration` carries only ingestion controls (`scd_type`,
  `primary_keys`, `sequence_by`). Omit if empty.
- Do **not** add an `environment` block — the CLI builds and attaches the
  connector wheels. (Only include one if the wheels are already on a Volume and
  you want to skip the build; that suppresses the automatic wheel upload.)

Show the spec to the user for review before proceeding.

---

## Step 4 — Ensure the CLI tool is available

Run `community-connector --help`. If it fails, install:

```bash
cd tools/community_connector && pip install -e . && cd ../..
```

---

## Step 5 — Deploy the pipeline

Use `create_pipeline` or `update_pipeline` based on the mode from Step 0. Both
run managed ingestion, which builds + uploads the connector wheels
automatically.

1. Write the spec to `tests/unit/sources/{{source_name}}/configs/{PIPELINE_NAME}_spec.yaml`.

2. Run the appropriate command:

   **Create mode:**
   ```bash
   community-connector create_pipeline {{source_name}} <PIPELINE_NAME> \
     -ps tests/unit/sources/{{source_name}}/configs/{PIPELINE_NAME}_spec.yaml \
     [-v <VOLUME_PATH>]
   ```

   **Create empty pipeline (no spec, add tables later):**
   ```bash
   community-connector create_pipeline {{source_name}} <PIPELINE_NAME> \
     -n <CONNECTION_NAME> -c <CATALOG> -t <SCHEMA>
   ```

   **Update mode:**
   ```bash
   community-connector update_pipeline <PIPELINE_NAME> \
     -ps tests/unit/sources/{{source_name}}/configs/{PIPELINE_NAME}_spec.yaml \
     -s {{source_name}}
   ```

   - The full spec carries `name`, `catalog`, `schema`, and `connection_name`,
     so CLI options for those are optional.
   - `-v` overrides the wheel upload volume path (defaults to
     `/Volumes/<catalog>/<schema>/community_connector/packages`).
   - Pass `-p <WHEEL>` to reuse pre-built wheels instead of building.
   - `-s {{source_name}}` on update tells the CLI which connector wheels to
     build. **Omit both `-s` and `-p` on update to leave the packages
     untouched** — the CLI reuses the pipeline's existing
     `environment.dependencies` and only applies the new spec.

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
