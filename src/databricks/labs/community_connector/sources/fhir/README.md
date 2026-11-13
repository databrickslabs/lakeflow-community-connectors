# Lakeflow FHIR R4 Community Connector

Ingests clinical data from any FHIR R4-compliant server into Databricks Delta tables using CDC (Change Data Capture). Each resource type lands in its own table, with the whole FHIR resource stored in a `VARIANT` column. Supports any server implementing SMART on FHIR Backend Services, OAuth2 client credentials, or open access.

---

## Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Runtime Requirements](#runtime-requirements)
- [Authentication Setup](#authentication-setup)
  - [JWT Assertion — SMART Backend Services](#1-jwt-assertion--smart-backend-services)
  - [Client Secret — OAuth2 Client Credentials](#2-client-secret--oauth2-client-credentials)
  - [No Authentication — Open Servers](#3-no-authentication--open-servers)
- [Deploy to Databricks](#deploy-to-databricks)
  - [Step 1 — Install the CLI](#step-1--install-the-cli)
  - [Step 2 — Authenticate the CLI](#step-2--authenticate-the-cli-with-your-databricks-workspace)
  - [Step 3 — Create a Unity Catalog connection](#step-3--create-a-unity-catalog-connection)
  - [Step 4 — Create the pipeline](#step-4--create-the-pipeline)
  - [Step 5 — Run the pipeline](#step-5--run-the-pipeline)
- [Connection Parameters Reference](#connection-parameters-reference)
- [Table Configuration Reference](#table-configuration-reference)
- [Bulk Data Export](#bulk-data-export)
- [Deletes](#deletes)
- [Supported Resource Types](#supported-resource-types)
- [Schema Design](#schema-design)
  - [Table Columns](#table-columns)
  - [Querying the VARIANT Column](#querying-the-variant-column)
  - [Recommended Silver View](#recommended-silver-view)
  - [Data Skipping and Clustering](#data-skipping-and-clustering)
  - [Decimal Precision](#decimal-precision)
- [Migrating from the Typed Schema](#migrating-from-the-typed-schema)
- [Architecture](#architecture)
  - [Extending the Connector](#extending-the-connector)
- [Troubleshooting](#troubleshooting)

---

## Overview

[FHIR (Fast Healthcare Interoperability Resources)](https://hl7.org/fhir/R4/) is the HL7 standard for exchanging healthcare data electronically. This connector ingests FHIR R4 resources (Patient, Observation, Condition, etc.) from any compliant server into Databricks Delta tables.

**How ingestion works:**

- **First run**: full load — all records for each configured resource type are fetched.
- **Subsequent runs**: incremental sync — only records with `meta.lastUpdated` newer than the previous cursor are fetched, using the FHIR `_lastUpdated` search parameter.
- **Deletes** (opt-in with `track_deletes=true`): resources deleted on the server are removed from the table. See [Deletes](#deletes).

**Tested against:**

| FHIR Server | Auth Method |
|---|---|
| Epic | JWT Assertion (SMART Backend Services) |
| Cerner / Oracle Health | JWT Assertion (SMART Backend Services) |
| Azure Health Data Services | Client Secret |
| SMART-enabled managed FHIR services | Client Secret |
| HAPI FHIR (public) | None |

---

## Prerequisites

- A Databricks workspace with Unity Catalog enabled.
- A supported runtime — see [Runtime Requirements](#runtime-requirements).
- Network connectivity from the workspace to your FHIR server's base URL.
- Credentials appropriate for your FHIR server's auth method (see [Authentication Setup](#authentication-setup)).
- The FHIR server must support:
  - FHIR R4 (4.0.1)
  - `_lastUpdated` search parameter (required for incremental sync)
  - `_count` pagination parameter
  - Bundle searchset responses with `next` pagination links
  - Type-level `_history` with `_since` only if you enable `track_deletes`

---

## Runtime Requirements

The connector stores each resource in a `resource` column of type [VARIANT](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type). Native `VariantType` in the Spark Python Data Source API was added in Spark 4.0 / PySpark 4.0 ([SPARK-54337](https://issues.apache.org/jira/browse/SPARK-54337)).

**Supported runtimes:**

| Runtime | Status |
|---|---|
| Serverless SDP, channel `CURRENT` or `PREVIEW` | Supported. This is what `create_pipeline` deploys. |
| Classic compute, DBR 18.2 or later | Supported. Measured as a Lakeflow Job; classic SDP pipelines were not measured. |
| Classic DBR 17.3 LTS | Not supported. |

### Compatibility matrix (empirical, 2026-10-21)

Measured with a minimal pure-PySpark probe: one `Trigger.AvailableNow` streaming write of a `VARIANT` column into a Unity Catalog Delta table, then read back with `v:name[0].family::string`. Run as a Lakeflow Job (`spark_python_task`) on a classic single-user cluster or serverless jobs compute, then repeated inside serverless SDP pipelines (second table). Not yet verified through the connector framework's own reader classes.

| Runtime | Simple stream reader (what the connector uses) | Partitioned stream reader (`SupportsPartitionedStream`) |
|---|---|---|
| Classic DBR 17.3 LTS (Spark 4.0.0) | ❌ `Not implemented. VariantType` | ✅ |
| Classic DBR 18.2 (Spark 4.1.0) | ✅ | ✅ |
| Classic DBR 19 (Spark 4.2.0) | ✅ | ✅ |
| Serverless jobs, `environment_version: "4"` (Spark 4.2.0 server) | ✅ | ✅ |

**Inside serverless SDP pipelines** (same probe, each reader feeding its own `@dp.table` streaming table):

| SDP configuration | PySpark client / Spark server | Simple reader + VARIANT | Partitioned reader (VARIANT *or* STRING) |
|---|---|---|---|
| Serverless, channel `CURRENT`, default environment | 4.1.0 / 4.1.0 | ✅ | ✅ |
| Serverless, channel `PREVIEW`, default environment | 4.1.0 / 4.2.0 | ✅ | ❌ `ModuleNotFoundError: No module named 'pyspark.databricks.wsfs'` |
| Serverless, channel `CURRENT`, `environment_version: "4"` | 4.0.0 / 4.1.0 | ✅ | ❌ same `ModuleNotFoundError` |

The partitioned-reader failure is not VARIANT-related: the STRING control fails identically. It happens inside Databricks' Python planning worker (`pyspark/sql/worker/plan_data_source_read.py` → `worker_util.py` imports `pyspark.databricks.wsfs`) before any connector code runs, and only where the PySpark client and Spark server versions differ. **The connector uses the simple reader, which works with VARIANT in all three SDP configurations above**, so the connector works on serverless SDP. Note that the framework's default pipeline config deploys to channel `PREVIEW` (`tools/community_connector/.../default_config.yaml`).

On every runtime, values must be `VariantVal` objects (`VariantVal.parseJson(text)`); the `{"value": ..., "metadata": ...}` dict form fails. Decimal precision survives exactly: `5.10000000000000000001` reads back unchanged. The partitioned reader works on all four jobs runtimes, including 17.3 LTS; the connector uses the simple reader, which works on every supported runtime above.

### Caveat — CLI rendering of the pipeline spec

`pipeline_spec.json` accepts **only** `connection_name` and `objects` — the framework's `PipelineSpec` model is declared `extra="forbid"`, so any other key fails at pipeline init with `Extra inputs are not permitted`. Pipeline-level settings (`channel`, `serverless`, `continuous`, `development`, `environment`, …) belong in the CLI's `--config` file, not the spec. Boolean keys are doubly unsafe in the spec: the CLI renders it into `ingest.py` via `json.dumps()`, so `true`/`false` also become invalid Python identifiers (`NameError: name 'true' is not defined`).

See [Troubleshooting → `no such data type exists` / VariantType errors](#no-such-data-type-exists--varianttype-errors) for diagnostics.

---

## Authentication Setup

Choose the method that matches your FHIR server.

### 1. JWT Assertion — SMART Backend Services

**Use for:** Epic, Cerner, and any server implementing the [SMART Backend Services spec](https://hl7.org/fhir/smart-app-launch/backend-services.html). This is a machine-to-machine flow — no user login is required.

**How it works:** The connector signs a short-lived JWT with your RSA/EC private key and exchanges it for an access token at the FHIR server's token endpoint. The server validates the assertion against the public key you registered.

#### Step 1: Generate an RSA key pair

```bash
# Generate 2048-bit RSA private key
openssl genrsa -out private_key.pem 2048

# Extract the public key
openssl rsa -in private_key.pem -pubout -out public_key.pem
```

For ES384, generate an EC key instead:

```bash
openssl ecparam -name secp384r1 -genkey -noout -out private_key.pem
openssl ec -in private_key.pem -pubout -out public_key.pem
```

#### Step 2: Register the public key with your FHIR server

- **Epic**: Register via the [Epic App Orchard](https://appmarket.epic.com/) developer portal. Upload `public_key.pem` and note the assigned `client_id` and `key ID (kid)`.
- **Cerner**: Register via the [Cerner Code](https://code.cerner.com/) developer portal.
- After registration you will receive a **client ID** and must supply a **key ID (kid)** that identifies which key in your JWK Set the server should use to verify your assertions.

#### Step 3: Find your token endpoint

The connector can auto-discover the token endpoint from the FHIR server's SMART configuration:

```bash
curl https://{your-fhir-base-url}/.well-known/smart-configuration | python3 -m json.tool
# Look for: "token_endpoint": "https://..."
```

If the server does not publish a SMART configuration document, obtain the token URL from your server administrator and set `token_url` explicitly.

#### Step 4: Determine required scopes

SMART Backend Services scopes follow the pattern `system/{ResourceType}.{permission}`:

```
system/*.read         # read all resource types (recommended)
system/*.*            # read and write all resource types
system/Patient.read system/Observation.read   # specific resource types
```

#### Required connection parameters

| Parameter | Value |
|---|---|
| `auth_type` | `jwt_assertion` |
| `base_url` | Your FHIR server base URL |
| `client_id` | Client ID from your server registration |
| `private_key_pem` | Full PEM content of `private_key.pem`, including `-----BEGIN PRIVATE KEY-----` delimiters |
| `kid` | Key ID registered with the server (must match exactly) |
| `scope` | Space-separated SMART scopes (e.g. `system/*.read`) |
| `token_url` | Token endpoint URL (omit to auto-discover from `/.well-known/smart-configuration`) |
| `private_key_algorithm` | `RS384` (default) or `ES384` — must match your key type |

---

### 2. Client Secret — OAuth2 Client Credentials

**Use for:** Servers using symmetric OAuth2 client credentials, including managed FHIR cloud services and SMART v1 confidential clients.

**How it works:** The connector sends `client_id` and `client_secret` via HTTP Basic authentication to the token endpoint and receives an access token.

#### Required connection parameters

| Parameter | Value |
|---|---|
| `auth_type` | `client_secret` |
| `base_url` | Your FHIR server base URL |
| `client_id` | OAuth2 client ID |
| `client_secret` | OAuth2 client secret |
| `token_url` | Token endpoint URL (omit to auto-discover from `/.well-known/smart-configuration`) |
| `scope` | Space-separated scopes (required by most servers) |

> **Note on `token_url`:** Some managed FHIR services do not publish a `/.well-known/smart-configuration` document. If auto-discovery fails, set `token_url` explicitly.

---

### 3. No Authentication — Open Servers

**Use for:** Public FHIR servers, local development, or sandbox environments (e.g., [HAPI FHIR public test server](https://hapi.fhir.org/baseR4)).

#### Required connection parameters

| Parameter | Value |
|---|---|
| `auth_type` | `none` |
| `base_url` | Your FHIR server base URL |

> **Note:** Set `page_delay` to `1.0` when using a public shared server to avoid overwhelming it.

---

## Deploy to Databricks

### Before you start

You need:
- A Databricks workspace with **Unity Catalog enabled**
- Your FHIR server credentials ready (see [Authentication Setup](#authentication-setup) above)
- Python 3.10+ installed locally

---

### Step 1 — Install the CLI

Clone the repository and install the CLI from source:

```bash
git clone https://github.com/databrickslabs/lakeflow-community-connectors.git
cd lakeflow-community-connectors
cd tools/community_connector
pip install -e .
```

Verify the install:

```bash
community-connector --help
```

---

### Step 2 — Authenticate the CLI with your Databricks workspace

The CLI uses the [Databricks Python SDK](https://docs.databricks.com/dev-tools/sdk-python.html) and inherits the same authentication as the Databricks CLI.

**Option A — Databricks CLI profile (recommended):**

```bash
community-connector create_connection fhir ...
```

To use a non-default profile:

```bash
DATABRICKS_CONFIG_PROFILE=my-profile community-connector create_connection fhir ...
```

**Option B — Environment variables:**

```bash
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=your-personal-access-token
```

---

### Step 3 — Create a Unity Catalog connection

The connection stores your FHIR server credentials securely in Unity Catalog. Pass `--spec` pointing to the local `connector_spec.yaml` so the CLI can validate your options correctly.

Run the command for your auth method from the root of the cloned repo:

**No authentication** (open or development servers):

```bash
community-connector create_connection fhir my_fhir_conn \
  --spec src/databricks/labs/community_connector/sources/fhir/connector_spec.yaml \
  --options '{"base_url": "https://hapi.fhir.org/baseR4", "auth_type": "none"}'
```

**Client secret** (OAuth2 client credentials):

```bash
community-connector create_connection fhir my_fhir_conn \
  --spec src/databricks/labs/community_connector/sources/fhir/connector_spec.yaml \
  --options '{
    "base_url": "https://your-fhir-server.com/fhir/r4",
    "auth_type": "client_secret",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "token_url": "https://auth.example.com/oauth2/token",
    "scope": "system/*.read"
  }'
```

**JWT assertion** (SMART Backend Services — Epic, Cerner):

```bash
community-connector create_connection fhir my_fhir_conn \
  --spec src/databricks/labs/community_connector/sources/fhir/connector_spec.yaml \
  --options '{
    "base_url": "https://your-ehr.com/fhir/r4",
    "auth_type": "jwt_assertion",
    "client_id": "your-client-id",
    "private_key_pem": "-----BEGIN PRIVATE KEY-----\nMIIEvg...\n-----END PRIVATE KEY-----",  # gitleaks:allow
    "kid": "your-key-id",
    "scope": "system/*.read",
    "private_key_algorithm": "RS384"
  }'
```

> **PEM keys:** Inside a JSON string, newlines must be written as `\n`. Generate the correctly escaped value with:
> ```bash
> python3 -c "import json; print(json.dumps(open('private_key.pem').read()))"
> ```

---

### Step 4 — Create the pipeline

This command clones the connector source code from GitHub into your Databricks workspace (sparse checkout — only the FHIR connector and shared libraries are downloaded), creates an `ingest.py` notebook, and registers a serverless Lakeflow Spark Declarative Pipeline.

**Ingest all 14 default resource types:**

```bash
community-connector create_pipeline fhir my_fhir_pipeline \
  --connection-name my_fhir_conn \
  --repo-url https://github.com/databrickslabs/lakeflow-community-connectors.git \
  --catalog main \
  --target clinical
```

**Ingest a specific subset of resource types:**

Create a `pipeline_spec.json` file:

```json
{
  "connection_name": "my_fhir_conn",
  "objects": [
    {"table": {"source_table": "Patient"}},
    {"table": {"source_table": "Observation"}},
    {"table": {"source_table": "Condition"}},
    {"table": {"source_table": "Encounter"}}
  ]
}
```

Then pass it to the command:

```bash
community-connector create_pipeline fhir my_fhir_pipeline \
  --pipeline-spec pipeline_spec.json \
  --repo-url https://github.com/databrickslabs/lakeflow-community-connectors.git
```

> **Note:** Do **not** add boolean keys like `"serverless": true` to this JSON — the CLI embeds the spec verbatim into a Python file and JSON booleans render as Python `NameError`. Set those via the CLI `--config` file or rely on bundled defaults. See [Runtime Requirements → CLI rendering caveat](#caveat--cli-rendering-of-the-pipeline-spec).

To turn on delete tracking for one table, add a `table_configuration` (see [Deletes](#deletes)):

```json
{"table": {"source_table": "Patient", "table_configuration": {"track_deletes": "true"}}}
```

---

### Step 5 — Run the pipeline

```bash
community-connector run_pipeline my_fhir_pipeline
```

Or go to **Databricks UI → Jobs & pipelines → Pipelines**, find `my_fhir_pipeline`, and click **Start**.

**What happens on each run:**

| Run | Behaviour |
|---|---|
| First run | Full load — all records for each resource type are fetched |
| Subsequent runs | Incremental — only records with `lastUpdated` newer than the previous run's cursor are fetched (plus deletions, if `track_deletes` is on) |
| No new records | Pipeline completes immediately; cursor is unchanged |

---

## Connection Parameters Reference

These are set at the **connection level** (shared across all tables in the pipeline).

| Parameter | Required | Auth Method | Description |
|---|---|---|---|
| `base_url` | Yes | All | Base URL of the FHIR R4 server. Example: `https://hapi.fhir.org/baseR4` |
| `auth_type` | Yes | All | Authentication method. One of: `jwt_assertion`, `client_secret`, `none` |
| `token_url` | Conditional | `jwt_assertion`, `client_secret` | OAuth2 token endpoint. If omitted, auto-discovered from `{base_url}/.well-known/smart-configuration` |
| `client_id` | Conditional | `jwt_assertion`, `client_secret` | OAuth2 client ID registered with the FHIR server |
| `private_key_pem` | Conditional | `jwt_assertion` | RSA or EC private key in PEM format, including `BEGIN`/`END` delimiters |
| `kid` | Conditional | `jwt_assertion` | Key ID identifying the key pair registered with the server |
| `private_key_algorithm` | No | `jwt_assertion` | Signing algorithm. Default: `RS384`. Accepted: `RS384`, `ES384` |
| `client_secret` | Conditional | `client_secret` | OAuth2 client secret |
| `scope` | Conditional | `jwt_assertion` (required), `client_secret` (required for some servers) | Space-separated OAuth2 scopes. Example: `system/*.read` |

---

## Table Configuration Reference

These are set per-table inside `table_configuration` in the pipeline spec.

| Option | Default | Description |
|---|---|---|
| `resource_types` | the 14 defaults | Comma-separated FHIR resource types served by this table configuration. Any R4 type name works. Example: `Patient,Observation,Condition` |
| `page_size` | `100` | Number of resources requested per page (`_count` parameter). Reduce if the server rate-limits you. |
| `max_records_per_batch` | `1000` | Maximum records fetched per pipeline trigger. Controls run duration and API load. |
| `page_delay` | `0.0` | Seconds to sleep between paginated requests. Set to `1.0` for public shared servers. |
| `track_deletes` | `false` | `true` removes resources deleted on the server from the table. Requires type-level `_history` on the server. See [Deletes](#deletes). |

Bulk export options (`bulk_export_*`, `bulk_incremental_mode`, `bulk_overlap_seconds`) are listed under [Bulk Data Export](#bulk-data-export).

> **`max_records_per_batch` and timestamp ties:** The batch is never cut inside a run of records sharing the same `lastUpdated`, because the next read asks for `lastUpdated > cursor` and would skip the rest of the tie. A batch can therefore exceed `max_records_per_batch` by the size of that tie. If the server ignores `_sort=_lastUpdated` and a batch has to be truncated, the read fails rather than skip records; raise `max_records_per_batch` or use bulk export.

---

## Bulk Data Export

By default the connector reads resources through paginated REST searches. Setting `bulk_export_enabled=true` switches it to the [HL7 Bulk Data Access](https://hl7.org/fhir/uv/bulkdata/) `$export` workflow instead: kick off an asynchronous server-side job, poll until it completes, then download the resulting NDJSON files.

Bulk export is much more efficient for large initial loads, and it is the access path Epic and Cerner prefer. It is opt-in and off by default.

### Options

| Option | Default | Description |
|---|---|---|
| `bulk_export_enabled` | `false` | `true` switches from REST pagination to `$export`. |
| `bulk_export_scope` | `system` | `system` (server-wide), `patient` (Patient compartment), or `group` (requires `bulk_export_group_id`). |
| `bulk_export_group_id` | — | FHIR `Group` resource ID. Required when scope is `group`. |
| `bulk_incremental_mode` | `since` | How the cursor is applied — see below. |
| `bulk_export_poll_interval` | `30` | Seconds between status polls. The server's `Retry-After` takes precedence. |
| `bulk_export_timeout` | `14400` | Max seconds to wait for a job (4 hours). |
| `bulk_overlap_seconds` | `0` | Rewind the cursor by this many seconds after each export. See [transactionTime semantics](#transactiontime-semantics). |
| `bulk_export_ignore_errors` | `false` | `true` accepts an export whose manifest reports error/fatal `OperationOutcome`s. See [Export errors](#export-errors). |

Every option above must also appear in the connection's `externalOptionsAllowList`, which the shipped `connector_spec.yaml` already handles.

### Incremental modes

| Mode | Parameters sent | Use when |
|---|---|---|
| `since` | `_type` + `_since` | Default. Works on most servers. |
| `typefilter` | `_type` + `_typeFilter=<Type>?_lastUpdated=gt<cursor>` | The server does not support `_since` — notably **Epic**. |
| `full` | `_type` only | You want a complete re-export every trigger. The table is ingested as a snapshot, so resources deleted on the server are removed. |

`_type` is always sent. Per the Bulk Data IG a server *"SHALL return all supported resources"* when `_type` is omitted, and `_typeFilter` only constrains *"resource types referenced in the `_typeFilter` expression"* — so sending `_typeFilter` alone makes the server export every other resource type unfiltered. A server that rejects the two parameters together will fail at kick-off with a clear error rather than silently over-exporting.

> ⚠️ **`full` mode re-exports everything on every trigger.** Each trigger runs a complete server-side export and rewrites the table from it. Bulk exports are expensive and some servers rate-limit them hard — Epic enforces a **24-hour cooldown** between exports of the same group. Schedule `full` pipelines accordingly; prefer `since` or `typefilter` for routine incremental sync.

Switching an existing table between `full` and an incremental mode changes its ingestion type, which requires a full refresh of that table.

### Export errors

The manifest's `error` array mixes error, warning and information `OperationOutcome`s. Warnings and information are logged. An **error or fatal** outcome means some resources are missing from the export, so by default the read fails **without advancing the cursor**, and the next trigger runs the export again. An outcome with no severity, or an error file that cannot be downloaded, is treated the same way, because the connector cannot tell what was lost.

If the server reports a persistent error you have decided to accept, set `bulk_export_ignore_errors=true`. Errors are then logged and the export is ingested as-is.

### One export job per resource type

The framework runs each table as its own streaming query, so a pipeline covering N resource types kicks off **N independent export jobs per trigger**. Each job is scoped server-side with `_type`, so no data is downloaded twice — but on servers where a job takes hours, N jobs take N× as long. For many resource types, either narrow `resource_types` or use a low trigger frequency.

### transactionTime semantics

The cursor advances to the manifest's `transactionTime`. The spec permits servers to report this as either the export's *start* or *completion* instant, and the connector cannot tell which. On a completion-time server, resources written while the export ran can be missed.

If you suspect that, set `bulk_overlap_seconds` to comfortably exceed a typical export duration. The cursor is then rewound by that amount, re-reading a safe window; duplicates are absorbed by the upsert on `resource_id`. The cursor is never moved earlier than the previous cursor, so this cannot cause a re-read loop.

If a manifest omits `transactionTime` entirely, the connector falls back to a clock reading captured **before** kick-off, which risks duplicates rather than data loss.

### Per-server notes

| Server | Notes |
|---|---|
| **Epic** | No `_since` — use `bulk_incremental_mode=typefilter`. Does not populate `meta.lastUpdated`. 24-hour cooldown per group export. |
| **Cerner / Oracle Health** | Redirects file downloads to presigned S3; the connector drops the `Authorization` header when leaving the original host. Max 20k patients per group. |
| **HAPI** | Files are served as `Binary` resources; the connector sends `Accept: application/fhir+ndjson`. Verified against the public server: `transactionTime` comes back with a **non-UTC offset** (`-04:00`), and the manifest sets `requiresAccessToken: true` even though the files download without a token. Status polls answer `Retry-After: 120`, which the connector honours — so an export takes ~2 minutes regardless of `bulk_export_poll_interval`. Cleanup `DELETE` answers `202` for a job still queued but `404` once it has completed — the connector deletes after completion, so it sees the `404`. Cleanup is best-effort and ignores both. |
| **Azure Health Data Services** | Output URLs point at Blob Storage and need Azure credentials, not a FHIR token. Not supported by this connector yet. |
| **Google Cloud Healthcare** | Writes to `gs://` paths with no HL7-standard manifest. Not supported yet. |

---

## Deletes

Deletes are opt-in. Set `track_deletes=true` on a table to have the connector remove resources that were deleted on the server. The table is then ingested as `cdc_with_deletes`.

| Mode | Deleted resources |
|---|---|
| REST, or bulk `since` / `typefilter`, `track_deletes=false` (default) | Not removed. The table is upsert-only. |
| REST, or bulk `since` / `typefilter`, `track_deletes=true` | Removed. Read from each type's history (see below). |
| Bulk `full`, any `track_deletes` setting | Removed. The table is ingested as a snapshot, so each export replaces the previous one. `track_deletes` has no effect here. |

**How `track_deletes` works:**

- Once per trigger, the connector reads `GET [type]/_history?_since=<cursor>` and deletes each resource whose newest history entry is a DELETE. This applies in REST mode and in bulk `since` / `typefilter` modes.
- The server must support type-level `_history` with `_since`. HAPI does; Epic does not. If it does not, the read fails with an error naming the missing capability, rather than silently leaving deletes unapplied.
- The first trigger reads no deletes, because the initial load already reflects current state.
- Each read starts 5 minutes before the previous trigger began, to cover clock skew between the server and the pipeline. Re-reads are idempotent.
- HAPI's DELETE history entries carry no timestamp, so a delete is sequenced by the closest newer history entry. That is always later than the resource's last version, so the delete wins. A resource re-created after its deletion is not removed.

The bulk manifest's `deleted` array (Bulk Data v2) is not applied in incremental modes. The connector logs a warning with the file count. Use `track_deletes` or `full` mode instead.

---

## Supported Resource Types

Each name in `resource_types` becomes its own table. Any FHIR R4 resource type name works, with no code change. The default set is:

Patient, Observation, Condition, Encounter, Procedure, MedicationRequest, DiagnosticReport, AllergyIntolerance, Immunization, Coverage, CarePlan, Goal, Device, DocumentReference

Every table uses `resource_id` as its primary key and `last_updated` as its cursor. The ingestion type is `cdc`, `cdc_with_deletes` when `track_deletes` is on, or `snapshot` in bulk `full` mode.

---

## Schema Design

### Table Columns

Every resource-type table has the same six columns, so one schema serves all types:

| Column | Type | Description |
|---|---|---|
| `resource_type` | `STRING NOT NULL` | FHIR `resourceType`. Constant within a table, so `UNION ALL` across tables stays self-describing. |
| `resource_id` | `STRING NOT NULL` | FHIR `id`. Primary key. |
| `last_updated` | `TIMESTAMP` | `meta.lastUpdated`. The CDC cursor and the sequence key. |
| `resource` | `VARIANT` | The whole FHIR resource. |
| `fhir_version` | `STRING` | `fhirVersion` from the server's CapabilityStatement (`GET /metadata`). Null if the server does not return it. |
| `ingested_at` | `TIMESTAMP` | When the connector read the row. |

### Querying the VARIANT Column

Use the `:` path syntax for FHIR element names, `::` to cast, and `variant_get` or `variant_explode` for typed extraction. Replace `<catalog>.<schema>` with your pipeline's destination.

```sql
-- Path with a cast: first family name
SELECT resource_id, resource:name[0].family::string AS family_name
FROM <catalog>.<schema>.patient;

-- Date cast
SELECT resource_id, resource:birthDate::date AS birth_date
FROM <catalog>.<schema>.patient;

-- Typed extraction from a nested path
SELECT resource_id,
       variant_get(resource, '$.code.coding[0].code', 'string') AS code
FROM <catalog>.<schema>.observation;

-- One row per identifier
SELECT p.resource_id, i.pos, i.value:system::string AS id_system, i.value:value::string AS id_value
FROM <catalog>.<schema>.patient AS p,
     LATERAL variant_explode(p.resource:identifier) AS i;
```

A few rules to keep in mind:

- A VARIANT column cannot be used in `GROUP BY`, `ORDER BY`, `DISTINCT` or set operators. Extract a scalar with a path or `variant_get` first.
- FHIR element names are case-sensitive (`birthDate`, not `birthdate`).

### Recommended Silver View

For silver-layer tables, flatten each resource type into typed columns with a view (SQL-on-FHIR style). This keeps the bronze table complete and lets consumers query plain columns:

```sql
CREATE OR REPLACE VIEW <catalog>.<schema>.patient_typed AS
SELECT
  resource_id                        AS id,
  resource:name[0].family::string    AS family_name,
  resource:name[0].given[0]::string  AS given_name,
  resource:birthDate::date           AS birth_date,
  resource:birthDate::string         AS birth_date_raw,
  resource:gender::string            AS gender,
  last_updated
FROM <catalog>.<schema>.patient;
```

> **Partial dates:** FHIR `date` values may be just a year or a year and month (`"1956"`, `"1956-08"`). Casting them with `::date` silently yields the first of the period (`1956-01-01`). Keep the raw string alongside, as above, wherever precision matters. Measured on public HAPI on 2026-11-01: 20 of 15,637 birth dates were partial.

The `patient` queries in this section were run against a table loaded from public HAPI R4 by this connector on serverless SDP (2026-11-01).

### Data Skipping and Clustering

VARIANT has no min/max statistics, so Delta cannot skip files based on a path inside `resource`. Filter on `last_updated` or `resource_id` instead. They are plain columns with statistics, and filters on them let Delta prune files.

A VARIANT column cannot be a clustering key. To cluster a table, set `cluster_by` in `table_configuration` on a plain column:

```json
{"table": {"source_table": "Observation", "table_configuration": {"cluster_by": "last_updated"}}}
```

### Decimal Precision

- **Bulk:** the NDJSON line is stored verbatim, so FHIR decimals keep the precision the server sent. `5.10000000000000000001` keeps all of its digits.
- **REST:** the parsed JSON is re-serialised, so decimals are rounded to float64. Realistic clinical values such as `5.1` or `98.6` survive. Values with more significant digits than float64 holds do not.
- **Trailing zeros** (`1.500` becomes `1.5`) are lost on both paths. This is a property of VARIANT storage.

---

## Migrating from the Typed Schema

The typed per-resource schema has been replaced by the shared schema above. This is a breaking change:

- **Columns renamed:** `id` → `resource_id`, `lastUpdated` → `last_updated`.
- **Columns removed:** the typed per-resource columns, `raw_json` and `extension`. The full resource is now in `resource`, so read those fields with paths such as `resource:birthDate` or `variant_explode(resource:identifier)`.
- **Columns added:** `resource_type`, `resource` (VARIANT), `fhir_version`, `ingested_at`.
- **Option removed:** `profile`. The `uk_core` and `base_r4` profiles no longer exist. Remove `profile` from any `table_configuration`. UK-specific values such as the NHS number are read from `resource`.
- **Action required:** existing tables do not match the new schema. Run a **full refresh** of every table after upgrading.

---

## Architecture

The connector is one `LakeflowConnect` implementation. Each resource type is a table, and every table shares `RESOURCE_SCHEMA`.

| File | Purpose |
|---|---|
| `fhir.py` | `LakeflowConnect` implementation: table listing, REST and bulk reads, cursors, deletes |
| `fhir_bulk.py` | Bulk Data `$export` client: kick-off, polling, NDJSON download, error handling |
| `fhir_utils.py` | `RESOURCE_SCHEMA`, HTTP client with retries, SMART auth, bundle pagination, `extract_record` |
| `fhir_constants.py` | Default resource types, retry and timeout settings, history overlap, bulk limits |
| `connector_spec.yaml` | Connection parameters and the `externalOptionsAllowList` |

`_generated_fhir_python_source.py` is produced from these modules by `tools/scripts/merge_python_source.py fhir`. Edit the source modules, not the generated file.

### Extending the Connector

To ingest another FHIR R4 resource type, add its name to `resource_types`. No code change is needed, because every type uses the same schema. The server must support that type's search (and, for `track_deletes`, its `_history`).

---

## Troubleshooting

### `no such data type exists` / VariantType errors

If your pipeline run fails with one of:

- `AttributeError: type object 'VariantVal' has no attribute 'parseJson'`
- `java.lang.RuntimeException: Not implemented. VariantType`
- `MALFORMED_VARIANT` in `convert_variant`
- `DATA_SOURCE_INVALID_RETURN_TYPE` referencing the `resource` column
- A schema-time error mentioning "no such data type exists" / variant

…you are running on a runtime that predates PySpark 4.0 / VARIANT-in-DataSource support. Confirm with a one-cell notebook on the same compute:

```python
import pyspark
from pyspark.sql.types import VariantVal
print("pyspark:", pyspark.__version__)            # need 4.0+
print("dbr:", spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "n/a"))
print("parseJson:", hasattr(VariantVal, "parseJson"))  # must be True
```

As of 2026-10-21, `Not implemented. VariantType` reproduces only on **classic DBR 17.3 LTS**. The connector writes `VariantType` successfully on DBR 18.2, DBR 19, serverless jobs, and all three serverless SDP configurations tested. See [Runtime Requirements → Compatibility matrix](#compatibility-matrix-empirical-2026-10-21) for the per-runtime evidence.

Recommended action: move to serverless SDP or DBR 18.2+. DBR 17.3 LTS is not supported.

### `track_deletes=true needs the server to support GET ... /_history`

The connector raises this when `track_deletes=true` and the server rejects or does not implement type-level `GET [type]/_history` with `_since`. Epic is a known example. Leave `track_deletes` off for that server; upserts still work. Otherwise, check that the server supports history for the resource type.

### `invalid_grant` from token endpoint

The OAuth2 client is not configured for `client_credentials` flow. Confirm with your FHIR server or authorisation server administrator that the client is registered as a backend service client with the `client_credentials` grant type enabled.

### `invalid_scope` from token endpoint

The scope value does not match what is registered on the authorisation server. Scope values are case-sensitive and must be an exact match.

### Server does not support `_lastUpdated`

The connector raises:
```
FHIR server appears to have ignored the '_lastUpdated=gt{since}' filter
```
when it sends an incremental filter but receives records older than the cursor. Contact your FHIR server administrator — `_lastUpdated` is not mandatory in the FHIR spec and some servers do not implement it.

### HTTP 401 / 403 on resource requests

- **`jwt_assertion`**: Confirm the `kid` in your config exactly matches the key ID registered with the server. Confirm `private_key_pem` is the private key corresponding to the public key you registered.
- **`client_secret`**: Confirm `client_id` and `client_secret` are correct and have not expired.
- **All methods**: Confirm `scope` includes permissions for the resource types you are ingesting.

### Rate limiting (429 responses)

The connector retries with exponential backoff (5 → 10 → 20 → 40 → 80 seconds, max 5 retries). If rate limiting persists:

- Reduce `page_size`
- Reduce `max_records_per_batch`
- Increase the pipeline schedule interval

### Empty results on incremental sync

Expected when no records were created or modified since the last run.

### Resource type returns 404

FHIR resource type names are case-sensitive. Use exact casing: `Patient`, not `patient` or `PATIENT`.

---

## References

- [FHIR R4 Specification](https://hl7.org/fhir/R4/)
- [SMART on FHIR Backend Services](https://hl7.org/fhir/smart-app-launch/backend-services.html)
- [HAPI FHIR public test server](https://hapi.fhir.org/baseR4)
