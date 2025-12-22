# Generic Lakeflow Connect Deployment for HTTP-Based Connectors

## Problem Statement

The Databricks Lakeflow `ingestion_definition` framework **does not automatically inject** credentials from Unity Catalog connection properties into custom connectors.

**Affected connectors:**
- OSI PI (OAuth with Service Principal)
- HubSpot (Private App access token)
- Zendesk (API token)
- Zoho CRM (OAuth refresh token)
- Salesforce (username/password/token)
- GitHub (Personal Access Token)
- Any custom HTTP-based connector requiring authentication

## Solution

Use **custom DLT notebooks** that:
1. Read secrets in driver context (where `dbutils` works)
2. Pass credentials as options to the connector
3. Deploy via Databricks Asset Bundles (DAB)

---

## Architecture

```
Metadata CSV
  ↓
Notebook Generator (generic, connector-agnostic)
  ↓
DLT Notebooks (one per pipeline group)
  ├─ Reads secrets from dbutils (driver)
  ├─ Passes to connector as options
  └─ Defines @dlt.table for each table
  ↓
DAB YAML Generator
  ↓
Pipeline Definitions (reference notebooks)
  ↓
Deploy via DAB
  ↓
Production DLT Pipelines with OAuth/Auth
```

---

## Quick Start

1) Prepare a metadata CSV with at least `source_table` and `pipeline_group`.

2) Generate notebooks:

```bash
python tools/notebook_based_deployment/generate_dlt_notebooks_generic.py \
  --connector-name <connector> \
  --input-csv <metadata.csv> \
  --output-dir dlt_notebooks/<connector> \
  --connector-path /Workspace/Users/<you>@company.com/connectors \
  --generate-all-groups
```

3) Upload notebooks and deploy using the generated DAB YAML (see per-connector examples).
