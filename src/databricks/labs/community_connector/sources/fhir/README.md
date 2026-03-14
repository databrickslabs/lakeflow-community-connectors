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
- [Schema Design](#schema-design)
  - [Common Columns](#common-columns)
  - [Typed Columns — UK Core Profile (default)](#typed-columns--uk-core-profile-default)
  - [Profile System](#profile-system)
  - [FHIR Type Mappings](#fhir-type-mappings)
  - [Per-Resource Column Reference](#per-resource-column-reference)
- [Architecture](#architecture)
  - [Profile Registry](#profile-registry)
  - [Extending the Connector](#extending-the-connector)
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
git clone https://github.com/FiifiB/lakeflow-community-connectors.git
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
  --repo-url https://github.com/FiifiB/lakeflow-community-connectors.git \
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
  --repo-url https://github.com/FiifiB/lakeflow-community-connectors.git
```

**Use a specific schema profile** (see [Profile System](#profile-system)):

```json
{
  "connection_name": "my_fhir_conn",
  "objects": [
    {
      "table": {
        "source_table": "Patient",
        "table_configuration": {"profile": "base_r4"}
      }
    }
  ]
}
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
| Subsequent runs | Incremental — only records with `lastUpdated` newer than the previous run's cursor are fetched |
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
| `page_size` | `100` | Number of resources requested per page (`_count` parameter). Reduce if the server rate-limits you. |
| `max_records_per_batch` | `1000` | Maximum records fetched per pipeline trigger. Controls run duration and API load. |
| `page_delay` | `0.0` | Seconds to sleep between paginated requests. Set to `1.0` for public shared servers. |
| `resource_types` | all 14 | Comma-separated list to limit which resource types are available. Example: `Patient,Observation,Condition` |
| `profile` | `uk_core` | Schema profile to use for this table. One of: `uk_core` (default), `base_r4`. See [Profile System](#profile-system). |

> **`max_records_per_batch` and data completeness:** If multiple records share the same `lastUpdated` timestamp and `max_records_per_batch` cuts mid-batch, records sharing the exact cursor value may be skipped on subsequent runs. Set `max_records_per_batch` generously (≥1000) for bulk-imported datasets.

---

## Supported Resource Types

All 14 resource types use CDC ingestion with `id` as the primary key and `lastUpdated` as the cursor.

| Resource Type | Profile | Total Columns |
|---|---|---|
| `Patient` | UK Core 2.6.1 | 22 |
| `Observation` | UK Core 2.5.0 | 28 |
| `Condition` | UK Core 2.6.0 | 22 |
| `Encounter` | UK Core 2.5.0 | 22 |
| `Procedure` | UK Core 2.5.0 | 25 |
| `MedicationRequest` | UK Core 2.5.0 | 22 |
| `DiagnosticReport` | UK Core 2.5.0 | 20 |
| `AllergyIntolerance` | UK Core 2.5.0 | 21 |
| `Immunization` | UK Core 2.4.0 | 27 |
| `Coverage` | Base FHIR R4 | 19 |
| `CarePlan` | UK Core 2.2.0 | 20 |
| `Goal` | Base FHIR R4 | 19 |
| `Device` | UK Core 1.2.0 | 20 |
| `DocumentReference` | UK Core 2.2.0 | 19 |

Coverage and Goal have no UK Core profile — the base FHIR R4 spec is used directly.

---

## Schema Design

### Common Columns

Every table includes these four columns regardless of resource type or profile:

| Column | Type | Description |
|---|---|---|
| `id` | `StringType` | FHIR resource ID (primary key) |
| `resourceType` | `StringType` | FHIR resource type name |
| `lastUpdated` | `TimestampType` | `meta.lastUpdated` — CDC cursor |
| `raw_json` | `StringType` | Full FHIR resource as a JSON string — complete, lossless storage |

### Typed Columns — UK Core Profile (default)

Beyond the four common columns, each resource has typed columns extracted from the FHIR JSON. Repeated FHIR elements (cardinality `0..*` or `1..*`) are represented as `ArrayType(StructType(...))`, which you can query in Databricks SQL using standard array syntax:

```sql
-- Explode Patient names into rows
SELECT id, explode(name) AS n FROM clinical.patient;

-- Access first name directly
SELECT id, name[0].family AS surname FROM clinical.patient;

-- Filter by NHS number (UK Core Patient only)
SELECT * FROM clinical.patient WHERE nhs_number = '9000000009';

-- Explode Observation components (e.g. blood pressure panels)
SELECT id, component[0].value_quantity.value AS systolic,
           component[1].value_quantity.value AS diastolic
FROM clinical.observation
WHERE code.coding[0].code = '55284-4';
```

**Key structural types used across all schemas:**

| FHIR Type | Spark Representation |
|---|---|
| `Coding` | `StructType(system, code, display)` |
| `CodeableConcept` | `StructType(coding: ArrayType(Coding), text)` |
| `Reference` | `StructType(reference, display)` |
| `Period` | `StructType(start: Timestamp, end: Timestamp)` |
| `Identifier` | `StructType(system, value, use)` |
| `HumanName` | `StructType(family, given: ArrayType(String), text, use)` |
| `Address` | `StructType(use, line: ArrayType(String), city, district, postalCode, country)` |
| `ContactPoint` | `StructType(system, value, use)` |
| `Quantity` | `StructType(value: Double, unit, system, code)` |
| `Dosage` | `StructType(text, timing_code, dose_value: Double, dose_unit, route, site)` |

### Profile System

The connector ships with two schema profiles:

| Profile | Description | When to use |
|---|---|---|
| `uk_core` | UK Core R4 implementation guide (default) | UK-based EHRs, NHS data, servers publishing UK Core extensions |
| `base_r4` | Base FHIR R4 specification | Non-UK servers, international deployments, or when UK Core extensions are not present |

**Default:** `uk_core`. For most resources, both profiles produce identical schemas — UK Core only adds meaningful extra columns to `Patient` (see below). For all other resources, `uk_core` falls back to `base_r4` automatically.

**UK Core Patient extra columns** (beyond base_r4):

| Column | Type | Source |
|---|---|---|
| `nhs_number` | `StringType` | Extracted from `identifier` where `system = https://fhir.nhs.uk/Id/nhs-number` |
| `ethnic_category` | `CodeableConcept` | UK Core extension `Extension-UKCore-EthnicCategory` |
| `birth_sex` | `CodeableConcept` | UK Core extension `Extension-UKCore-BirthSex` |
| `death_notification_status` | `CodeableConcept` | UK Core extension `Extension-UKCore-DeathNotificationStatus` |
| `interpreter_required` | `BooleanType` | HL7 base extension `patient-interpreterRequired` |
| `residential_status` | `CodeableConcept` | UK Core extension `Extension-UKCore-ResidentialStatus` |

To use a specific profile per table, set it in `table_configuration`:

```json
{
  "table": {
    "source_table": "Patient",
    "table_configuration": {"profile": "base_r4"}
  }
}
```

### FHIR Type Mappings

FHIR primitive types map to Spark types as follows:

| FHIR Type | Spark Type | Notes |
|---|---|---|
| `string`, `code`, `uri`, `id` | `StringType` | Status codes, identifiers, reference URLs |
| `instant`, `dateTime` | `TimestampType` | ISO 8601 with timezone |
| `date` | `StringType` | Partial dates (e.g. `2024-01`) cannot be represented as Timestamp |
| `boolean` | `BooleanType` | |
| `decimal` | `DoubleType` | Lab values, quantities |
| `integer`, `positiveInt` | `LongType` | Counts, ranks |
| `Reference.reference` | `StringType` (inside Reference struct) | e.g. `Patient/123` |

> All typed columns are nullable. FHIR R4 has very few required fields, and even required fields may be absent from records exported by some servers. The `raw_json` column preserves all data losslessly.

### Per-Resource Column Reference

#### Patient

| Column | Type | FHIR Source |
|---|---|---|
| `identifier` | `ArrayType(Identifier)` | `Patient.identifier` |
| `active` | `BooleanType` | `Patient.active` |
| `name` | `ArrayType(HumanName)` | `Patient.name` |
| `telecom` | `ArrayType(ContactPoint)` | `Patient.telecom` |
| `gender` | `StringType` | `Patient.gender` |
| `birthDate` | `StringType` | `Patient.birthDate` |
| `deceased_boolean` | `BooleanType` | `Patient.deceasedBoolean` |
| `deceased_datetime` | `TimestampType` | `Patient.deceasedDateTime` |
| `address` | `ArrayType(Address)` | `Patient.address` |
| `maritalStatus` | `CodeableConcept` | `Patient.maritalStatus` |
| `generalPractitioner` | `ArrayType(Reference)` | `Patient.generalPractitioner` |
| `managingOrganization` | `Reference` | `Patient.managingOrganization` |
| *UK Core only:* `nhs_number` | `StringType` | NHS number identifier slice |
| *UK Core only:* `ethnic_category` | `CodeableConcept` | UK Core extension |
| *UK Core only:* `birth_sex` | `CodeableConcept` | UK Core extension |
| *UK Core only:* `death_notification_status` | `CodeableConcept` | UK Core extension |
| *UK Core only:* `interpreter_required` | `BooleanType` | HL7 base extension |
| *UK Core only:* `residential_status` | `CodeableConcept` | UK Core extension |

#### Observation

| Column | Type | FHIR Source |
|---|---|---|
| `status` | `StringType` | `Observation.status` |
| `category` | `ArrayType(CodeableConcept)` | `Observation.category` |
| `code` | `CodeableConcept` | `Observation.code` |
| `subject` | `Reference` | `Observation.subject` |
| `encounter` | `Reference` | `Observation.encounter` |
| `effective_datetime` | `TimestampType` | `Observation.effectiveDateTime` |
| `effective_period` | `Period` | `Observation.effectivePeriod` |
| `issued` | `TimestampType` | `Observation.issued` |
| `performer` | `ArrayType(Reference)` | `Observation.performer` |
| `value_quantity` | `Quantity` | `Observation.valueQuantity` |
| `value_codeable_concept` | `CodeableConcept` | `Observation.valueCodeableConcept` |
| `value_string` | `StringType` | `Observation.valueString` |
| `value_boolean` | `BooleanType` | `Observation.valueBoolean` |
| `value_integer` | `LongType` | `Observation.valueInteger` |
| `data_absent_reason` | `CodeableConcept` | `Observation.dataAbsentReason` |
| `interpretation` | `ArrayType(CodeableConcept)` | `Observation.interpretation` |
| `body_site` | `CodeableConcept` | `Observation.bodySite` |
| `method` | `CodeableConcept` | `Observation.method` |
| `specimen` | `Reference` | `Observation.specimen` |
| `device` | `Reference` | `Observation.device` |
| `reference_range` | `ArrayType(struct)` | `Observation.referenceRange` |
| `has_member` | `ArrayType(Reference)` | `Observation.hasMember` |
| `derived_from` | `ArrayType(Reference)` | `Observation.derivedFrom` |
| `component` | `ArrayType(struct)` | `Observation.component` — includes `code`, `value_quantity`, `value_string`, `value_codeable_concept`, `value_boolean`, `value_integer`, `data_absent_reason` |

For all other resource types, use `DESCRIBE TABLE <catalog>.<schema>.<table>` in Databricks SQL to see the full column list, or refer to `profiles/base_r4.py` in the source.

---

## Architecture

### Profile Registry

The connector uses a **profile registry** to decouple schema definitions from ingestion logic. This makes it straightforward to support different national or organisational FHIR implementation guides without touching the core ingestion pipeline.

**How it works:**

```
Lookup: get_schema("Patient", "uk_core")

PROFILE_CHAIN = ["uk_core", "base_r4"]

Step 1: Check registry for ("Patient", "uk_core") → found → return UK Core Patient schema (22 col + NHS fields)
Step 2: (not reached)

Lookup: get_schema("Observation", "uk_core")

Step 1: Check registry for ("Observation", "uk_core") → not found
Step 2: Check registry for ("Observation", "base_r4") → found → return base R4 Observation schema
```

Each resource schema and extractor function is registered as a pair keyed by `(resource_type, profile)`. If a resource has no profile-specific override, the lookup automatically falls back to the next profile in the chain.

**Source files:**

| File | Purpose |
|---|---|
| `fhir_types.py` | Shared Spark type building blocks (`CODING`, `CODEABLE_CONCEPT`, `REFERENCE`, etc.) and extractor helpers |
| `fhir_profile_registry.py` | Registry implementation — `register` decorator, `get_schema`, `extract`, `PROFILE_CHAIN` |
| `profiles/base_r4.py` | Base FHIR R4 schemas and extractors for all 14 resources, validated against [hl7.org/fhir/R4](https://hl7.org/fhir/R4/) |
| `profiles/uk_core.py` | UK Core overrides — currently adds NHS number and UK extensions to Patient |
| `fhir_schemas.py` | Public API — imports and re-exports `get_schema` and `FALLBACK_SCHEMA` |
| `fhir_utils.py` | HTTP client, auth, bundle pagination, and profile-aware `extract_record` |
| `fhir.py` | `LakeflowConnect` implementation — orchestrates ingestion |

### Extending the Connector

#### Adding a new FHIR resource type

1. Open `profiles/base_r4.py`
2. Add a schema constant and extractor function decorated with `@register("NewResource", "base_r4", schema)`
3. The resource is automatically available in all pipelines via the `resource_types` table option

No changes needed to `fhir.py`, `fhir_schemas.py`, or any other file.

#### Adding a new national profile (e.g. AU Core, US Core)

1. Create `profiles/au_core.py` with overrides only for resources that differ from base R4:

```python
from databricks.labs.community_connector.sources.fhir.fhir_profile_registry import register
from databricks.labs.community_connector.sources.fhir.profiles.base_r4 import _patient as _base_patient, _PATIENT_SCHEMA

# Only override resources where AU Core differs from base R4
@register("Patient", "au_core", _AU_PATIENT_SCHEMA)
def _patient_au(r: dict) -> dict:
    result = _base_patient(r)
    # add AU-specific fields...
    return result
```

2. Add `"au_core"` to `PROFILE_CHAIN` in `fhir_profile_registry.py`:

```python
PROFILE_CHAIN: list = ["au_core", "uk_core", "base_r4"]
```

3. Import `au_core` in `fhir_schemas.py` to trigger registration.

Resources not overridden in `au_core.py` will automatically fall back to `uk_core`, then `base_r4`. You only need to define what actually differs.

---

## Troubleshooting

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
- [UK Core FHIR R4 Implementation Guide](https://simplifier.net/guide/uk-core-implementation-guide-stu3-sequence)
- [HAPI FHIR public test server](https://hapi.fhir.org/baseR4)
