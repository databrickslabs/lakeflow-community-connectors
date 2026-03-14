# Lakeflow FHIR R4 Community Connector

Ingests clinical data from any FHIR R4-compliant server into Databricks Delta tables using CDC (Change Data Capture). Supports any server implementing SMART on FHIR Backend Services, OAuth2 client credentials, or open access.

---

## Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
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
- [Supported Resource Types](#supported-resource-types)
- [Schema Reference](#schema-reference)
- [Troubleshooting](#troubleshooting)

---

## Overview

[FHIR (Fast Healthcare Interoperability Resources)](https://hl7.org/fhir/R4/) is the HL7 standard for exchanging healthcare data electronically. This connector ingests FHIR R4 resources (Patient, Observation, Condition, etc.) from any compliant server into Databricks Delta tables.

**How ingestion works:**

- **First run**: full load — all records for each configured resource type are fetched.
- **Subsequent runs**: incremental sync — only records with `meta.lastUpdated` newer than the previous cursor are fetched, using the FHIR `_lastUpdated` search parameter.

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
- Network connectivity from the workspace to your FHIR server's base URL.
- Credentials appropriate for your FHIR server's auth method (see [Authentication Setup](#authentication-setup)).
- The FHIR server must support:
  - FHIR R4 (4.0.1)
  - `_lastUpdated` search parameter (required for incremental sync)
  - `_count` pagination parameter
  - Bundle searchset responses with `next` pagination links

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

**How it works:** The connector sends `client_id` and `client_secret` via HTTP Basic authentication to the token endpoint and receives an access token. This is a machine-to-machine flow — no user login is required.

> **Why `client_credentials`, not `authorization_code`?**
> The `authorization_code` grant requires a human to authenticate interactively in a browser. A data pipeline running unattended on a schedule cannot do this. `client_credentials` is the correct OAuth2 grant for backend services.

#### Required connection parameters

| Parameter | Value |
|---|---|
| `auth_type` | `client_secret` |
| `base_url` | Your FHIR server base URL |
| `client_id` | OAuth2 client ID |
| `client_secret` | OAuth2 client secret |
| `token_url` | Token endpoint URL (omit to auto-discover from `/.well-known/smart-configuration`) |
| `scope` | Space-separated scopes (required by most servers — check your FHIR server's documentation) |

> **Note on `token_url`:** Some managed FHIR services do not publish a `/.well-known/smart-configuration` document. If auto-discovery fails, set `token_url` explicitly using the token endpoint provided by your service's documentation.

---

### 3. No Authentication — Open Servers

**Use for:** Public FHIR servers, local development, or sandbox environments that require no credentials (e.g., [HAPI FHIR public test server](https://hapi.fhir.org/baseR4)).

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
git clone https://github.com/FiifiB/lakeflow-community-connectors.git
cd lakeflow-community-connectors
cd tools/community_connector
pip install -e .
```

Verify the install:

```bash
community-connector --help
```

You should see the available commands listed: `create_connection`, `create_pipeline`, `run_pipeline`, etc.

---

### Step 2 — Authenticate the CLI with your Databricks workspace

The CLI uses the [Databricks Python SDK](https://docs.databricks.com/dev-tools/sdk-python.html) and inherits the same authentication as the Databricks CLI. It resolves credentials in this order:

**Option A — Databricks CLI profile (recommended):**

If you already have the Databricks CLI configured, the `community-connector` CLI picks up your default profile automatically with no extra flags:

```bash
community-connector create_connection fhir ...
```

To use a non-default profile, set the environment variable before the command:

```bash
DATABRICKS_CONFIG_PROFILE=my-profile community-connector create_connection fhir ...
```

Set up a profile with: `databricks configure --profile my-profile`

**Option B — Environment variables:**

```bash
export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=your-personal-access-token
```

To generate a personal access token: in Databricks go to **Settings → Developer → Access tokens → Generate new token**.

---

### Step 3 — Create a Unity Catalog connection

The connection stores your FHIR server credentials securely in Unity Catalog. Since the FHIR connector is not yet in the published Databricks repository, pass `--spec` pointing to the local `connector_spec.yaml` from the cloned fork so the CLI can validate your options correctly.

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

> **PEM keys:** Inside a JSON string, newlines must be written as `\n`. You can generate the correctly escaped value with:
> ```bash
> python3 -c "import json; print(json.dumps(open('private_key.pem').read()))"
> ```
> Copy the output (without the surrounding quotes) as the value for `private_key_pem`.

> **`token_url`:** This can be omitted for servers that publish `/.well-known/smart-configuration` — the connector will auto-discover the token endpoint. Set it explicitly if your server does not publish this document.

On success you will see:

```
Creating connection for source: fhir
Connection name: my_fhir_conn
Connection type: GENERIC_LAKEFLOW_CONNECT
  ✓ Connection created!

============================================================
Connection Name: my_fhir_conn
Connection ID:   <uuid>
============================================================
```

---

### Step 4 — Create the pipeline

This command clones the connector source code from GitHub into your Databricks workspace (sparse checkout — only the FHIR connector and shared libraries are downloaded), creates an `ingest.py` notebook, and registers a serverless Lakeflow Spark Declarative Pipeline.

Since the FHIR connector is not yet in the official published repository, pass `--repo-url` to point the CLI at the fork instead of the default `databrickslabs` repository:

**Ingest all 14 default resource types:**

```bash
community-connector create_pipeline fhir my_fhir_pipeline \
  --connection-name my_fhir_conn \
  --repo-url https://github.com/FiifiB/lakeflow-community-connectors.git \
  --catalog main \
  --target clinical
```

- `--repo-url` overrides the default repo with the fork containing the FHIR connector
- `--catalog` sets the target Unity Catalog catalog (default: `main`)
- `--target` sets the target schema (default: `default`)

**Ingest a specific subset of resource types, or customise options per table:**

Create a `pipeline_spec.json` file:

```json
{
  "connection_name": "my_fhir_conn",
  "objects": [
    {
      "table": {
        "source_table": "Patient",
        "destination_catalog": "main",
        "destination_schema": "clinical",
        "table_configuration": {
          "page_size": "100",
          "max_records_per_batch": "1000"
        }
      }
    },
    {
      "table": {
        "source_table": "Observation",
        "table_configuration": {
          "page_size": "50",
          "max_records_per_batch": "500"
        }
      }
    },
    {"table": {"source_table": "Condition"}},
    {"table": {"source_table": "Encounter"}}
  ]
}
```

Then pass it to the command:

```bash
community-connector create_pipeline fhir my_fhir_pipeline \
  --pipeline-spec pipeline_spec.json \
  --repo-url https://github.com/FiifiB/lakeflow-community-connectors.git
```

On success the CLI prints the pipeline ID and a direct link to it in your workspace.

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
| Subsequent runs | Incremental — only records with `lastUpdated` newer than the previous run's cursor are fetched |
| No new records | Pipeline completes immediately; cursor is unchanged |

---

### Update an existing pipeline

To change the resource types or table options after the pipeline has been created:

```bash
community-connector update_pipeline my_fhir_pipeline \
  --pipeline-spec updated_pipeline_spec.json
```

---

## Connection Parameters Reference

These are set at the **connection level** (shared across all tables in the pipeline).

| Parameter | Required | Auth Method | Description |
|---|---|---|---|
| `base_url` | Yes | All | Base URL of the FHIR R4 server. All resource requests are built from this. Example: `https://hapi.fhir.org/baseR4` |
| `auth_type` | Yes | All | Authentication method. One of: `jwt_assertion`, `client_secret`, `none` |
| `token_url` | Conditional | `jwt_assertion`, `client_secret` | OAuth2 token endpoint. If omitted, auto-discovered from `{base_url}/.well-known/smart-configuration` |
| `client_id` | Conditional | `jwt_assertion`, `client_secret` | OAuth2 client ID registered with the FHIR server |
| `private_key_pem` | Conditional | `jwt_assertion` | RSA or EC private key in PEM format, including `BEGIN`/`END` delimiters |
| `kid` | Conditional | `jwt_assertion` | Key ID identifying the key pair registered with the server. Must match exactly. |
| `private_key_algorithm` | No | `jwt_assertion` | Signing algorithm. Default: `RS384`. Accepted: `RS384`, `ES384` |
| `client_secret` | Conditional | `client_secret` | OAuth2 client secret |
| `scope` | Conditional | `jwt_assertion` (required), `client_secret` (required for some servers) | Space-separated OAuth2 scopes. Example: `system/*.read` |
| `externalOptionsAllowList` | Yes | All | Must be set to: `resource_types,page_size,max_records_per_batch,page_delay` |

---

## Table Configuration Reference

These are set per-table in `table_configuration` and must be listed in `externalOptionsAllowList`.

| Option | Default | Description |
|---|---|---|
| `page_size` | `100` | Number of resources requested per page (`_count` parameter sent to FHIR server). Reduce if the server rate-limits you. |
| `max_records_per_batch` | `1000` | Maximum records fetched per pipeline trigger. Controls run duration and API load. |
| `page_delay` | `0.0` | Seconds to sleep between paginated requests. Set to `1.0` for public shared servers (HAPI FHIR). Leave at `0.0` for private production EHRs. |
| `resource_types` | all 14 | Comma-separated list to limit which resource types are available. Example: `Patient,Observation,Condition` |

> **`max_records_per_batch` and data completeness:** If multiple records share the same `lastUpdated` timestamp and `max_records_per_batch` cuts mid-batch, records sharing the exact cursor value may be skipped on subsequent runs. Set `max_records_per_batch` generously (≥1000) to reduce this risk, especially for bulk-imported datasets where many records share a timestamp.

---

## Supported Resource Types

All 14 resource types use CDC ingestion with `id` as the primary key and `lastUpdated` as the cursor.

| Resource Type | Key Clinical Fields |
|---|---|
| `Patient` | `gender`, `birthDate`, `active`, `name_text`, `name_family` |
| `Observation` | `status`, `code_text`, `code_system`, `code_code`, `subject_reference`, `effective_datetime`, `value_quantity_value`, `value_quantity_unit`, `value_string`, `issued` |
| `Condition` | `clinical_status`, `verification_status`, `code_text`, `subject_reference`, `onset_datetime`, `recorded_date` |
| `Encounter` | `status`, `class_code`, `subject_reference`, `period_start`, `period_end` |
| `Procedure` | `status`, `code_text`, `subject_reference`, `performed_datetime` |
| `MedicationRequest` | `status`, `intent`, `medication_text`, `subject_reference`, `authored_on` |
| `DiagnosticReport` | `status`, `code_text`, `subject_reference`, `effective_datetime`, `issued` |
| `AllergyIntolerance` | `clinical_status`, `verification_status`, `code_text`, `patient_reference`, `recorded_date` |
| `Immunization` | `status`, `vaccine_code_text`, `patient_reference`, `occurrence_datetime` |
| `Coverage` | `status`, `beneficiary_reference`, `payor_reference`, `period_start`, `period_end` |
| `CarePlan` | `status`, `intent`, `subject_reference`, `period_start`, `period_end` |
| `Goal` | `lifecycle_status`, `description_text`, `subject_reference`, `start_date` |
| `Device` | `status`, `device_name`, `type_text`, `patient_reference` |
| `DocumentReference` | `status`, `type_text`, `subject_reference`, `date` |

Every table also includes:

| Column | Type | Description |
|---|---|---|
| `id` | `StringType` | FHIR resource ID (primary key) |
| `resourceType` | `StringType` | FHIR resource type name |
| `lastUpdated` | `TimestampType` | `meta.lastUpdated` — used as the CDC cursor |
| `raw_json` | `StringType` | Full FHIR resource as a JSON string — complete, lossless storage |

---

## Schema Reference

FHIR types map to Spark SQL types as follows:

| FHIR Type | Spark Type | Notes |
|---|---|---|
| `string`, `code`, `uri` | `StringType` | Status codes, identifiers, reference URLs |
| `instant`, `dateTime` | `TimestampType` | ISO 8601 with timezone. Example: `2024-01-15T10:30:00+00:00` |
| `date` | `StringType` | Partial dates (e.g. `2024-01` or `2024`) cannot map to a full timestamp |
| `boolean` | `BooleanType` | `true` / `false` |
| `decimal` | `DoubleType` | Numeric quantities such as lab values |
| `Reference.reference` | `StringType` | FHIR reference strings, e.g. `Patient/123` |

---

## Troubleshooting

### `invalid_grant` from token endpoint

The OAuth2 client is not configured for `client_credentials` flow. Confirm with your FHIR server or authorisation server administrator that the client is registered as a machine-to-machine (backend service) client with the `client_credentials` grant type enabled.

### `invalid_scope` from token endpoint

The scope value in your config does not match what is registered on the authorisation server. Check your FHIR server or authorisation server documentation for the exact scope strings supported. Scope values are case-sensitive and must be an exact match.

### Server does not support `_lastUpdated`

The connector raises:
```
FHIR server appears to have ignored the '_lastUpdated=gt{since}' filter
```
when it sends an incremental filter but receives records older than the cursor. This means the server ignores the `_lastUpdated` search parameter. Contact your FHIR server administrator — `_lastUpdated` is not mandatory in the FHIR spec and some servers do not implement it.

### HTTP 401 / 403 on resource requests

- **`jwt_assertion`**: Confirm the `kid` in your config exactly matches the key ID registered with the server. Confirm `private_key_pem` is the private key corresponding to the public key you registered.
- **`client_secret`**: Confirm `client_id` and `client_secret` are correct and have not expired or been rotated.
- **All methods**: Confirm `scope` includes permissions for the resource types you are ingesting.

### Empty results on incremental sync

This is expected when no records were created or modified since the last run. The cursor stays unchanged and the next run will check again from the same point.

### Rate limiting (429 responses)

The connector retries with exponential backoff (5 → 10 → 20 → 40 → 80 seconds, max 5 retries). If rate limiting persists:

- Reduce `page_size` (fewer records per API call)
- Reduce `max_records_per_batch` (fewer total records per pipeline trigger)
- Increase the pipeline schedule interval

### Resource type returns 404

FHIR resource type names are case-sensitive. Use exact casing: `Patient`, not `patient` or `PATIENT`. Confirm the server supports the resource type.

---

## References

- [FHIR R4 Specification](https://hl7.org/fhir/R4/)
- [SMART on FHIR Backend Services](https://hl7.org/fhir/smart-app-launch/backend-services.html)
- [SMART on FHIR Conformance / Discovery](https://hl7.org/fhir/smart-app-launch/conformance.html)
- [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html)
- [HAPI FHIR public test server](https://hapi.fhir.org/baseR4)
