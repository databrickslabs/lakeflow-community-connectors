# QuickBooks Online community connector

This directory contains the first Lakeflow-native scaffold for a QuickBooks
Online Accounting API connector.

## Current status

Implemented:

- Unity Catalog OAuth connection specification.
- One QuickBooks `realm_id` per connection.
- Six discoverable tables: customers, vendors, accounts, items, invoices,
  and bills.
- Stable typed core fields plus a lossless `raw_json` payload.
- Complete `STARTPOSITION` / `MAXRESULTS` snapshot pagination.
- Bounded retries for throttling, transient HTTP failures, and network errors.
- Checkpointed inserts and updates for all six tables using bounded
  `MetaData.LastUpdatedTime` queries.
- Versioned per-table offsets, snapshot-to-incremental handoff, and replay-safe
  timestamp overlap.
- Explicit active-and-inactive reads for Customer, Vendor, Account, and Item.
- Replay-safe Invoice and Bill hard-delete tombstones through QuickBooks CDC.
- Fail-closed protection for QuickBooks CDC's 30-day horizon and 1,000-object
  response ceiling.
- Non-null `realm_id` on every row and tombstone, with `(realm_id, id)` as the
  composite destination key.
- Version-2 checkpoints bound to realm, table, and update/delete flow.

Validation available:

- Focused unit tests for configuration, pagination, retries, HTTP failures,
  response validation, and typed normalization.
- Source simulator corpus and the repository's generic connector contract
  suite for all six tables.
- Generated single-file Spark Python data source with a tested
  `register(spark, "quickbooks")` entry point.
- Live Intuit sandbox OAuth refresh and Customer snapshot validation.
- Serverless Databricks M1 Customer pipeline with exact source/destination ID
  parity.
- Serverless Databricks M2 pipeline with exact live source/destination ID
  parity for customers, vendors, accounts, items, invoices, and bills.
- Serverless Databricks M3 Customer CDC pipeline with successful bootstrap,
  no-change replay, synthetic insert, and sparse-update acceptance.
- Isolated serverless six-table M3 CDC pipeline with successful bootstrap and
  aggregate integrity validation for every entity.
- Isolated serverless M4 pipeline with live acceptance proving Invoice/Bill
  hard deletes remove destination rows while inactive Customer/Vendor rows
  remain queryable.
- `pipeline_spec.customer.yaml` for the M1 Customer smoke pipeline.
- `pipeline_spec.yaml` for the M2 six-table snapshot pipeline.
- `pipeline_spec.customer_cdc.yaml` for the isolated M3 Customer CDC pilot.
- `pipeline_spec.all_tables_cdc.yaml` for six-table M3 CDC ingestion.
- `pipeline_spec.all_tables_cdc_deletes.yaml` for M4 update, inactivation, and
  hard-delete ingestion.
- `pipeline_spec.multi_tenant.yaml` for the M5 tenant-isolated deployment
  pattern.

Not implemented or externally validated yet:

- Live synthetic insert/update acceptance for each non-Customer entity.
- Automated full reconciliation after an expired or saturated delete
  checkpoint.
- Production validation of Unity Catalog managed U2M with Intuit.

## Connection parameters

| Parameter | Required | Description |
|---|---:|---|
| `client_id` | yes | Intuit OAuth application client ID |
| `client_secret` | yes | Intuit OAuth application client secret |
| `realm_id` | yes | QuickBooks Online company ID |
| `environment` | no | `production` (default) or `sandbox` |
| `minor_version` | no | Accounting API minor version; defaults to `75` |

The Databricks control plane owns the OAuth authorization and token-refresh
boundary. The connector consumes the injected `access_token` and never receives
client credentials or refresh tokens. Use a separate connection, pipeline,
destination schema, and checkpoint chain for every QuickBooks realm. Tables
additionally use `(realm_id, id)` so identical QuickBooks IDs cannot collide.

## Table options

| Option | Default | Description |
|---|---:|---|
| `page_size` | `1000` | QuickBooks query page size, from 1 through 1000 |
| `incremental_overlap_seconds` | `60` | Per-table lower-bound replay overlap, from 0 through 3600 seconds |
| `max_incremental_window_seconds` | `86400` | Maximum per-table checkpoint window, from 60 through 604800 seconds |
| `max_records_per_batch` | `100000` | Best-effort admission-control target for incremental windows, from 1 through 10000000 records |
| `delete_overlap_seconds` | `60` | Invoice/Bill delete replay overlap, from 0 through 3600 seconds |
| `initial_delete_lookback_seconds` | `300` | Invoice/Bill bootstrap delete lookback, from 0 through 86400 seconds |

## Development

All six tables start with a complete snapshot followed by bounded update
queries with independent checkpoints. Customers, vendors, accounts, and items
use `cdc` because `Active=false` must remain queryable. Invoices and bills use
`cdc_with_deletes`, which adds independent tombstone flows.

Incremental update windows that exceed `max_records_per_batch` are retried
with progressively smaller upper time bounds. The connector drains an accepted
window completely before advancing its checkpoint. A one-second timestamp
cohort is indivisible and may exceed the configured target; this preserves all
rows sharing that cursor. QuickBooks' delete CDC endpoint cannot paginate, so
an oversized delete cohort fails without advancing its checkpoint.

Run the offline connector suite from the repository root:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/sources/quickbooks -q
```

## Workspace setup

Use the repository's generic `community-connector create_connection` command
to create one Unity Catalog connection per QuickBooks realm. Intuit requires an
exactly registered loopback redirect; when using port `8765`, register
`http://127.0.0.1:8765/callback` and pass `--redirect-port 8765`.

Once QuickBooks and Databricks credentials are current, deploy the Customer
smoke pipeline first, preserve the M2 snapshot pipeline for comparison, and
create an isolated six-table CDC pipeline:

```bash
community-connector create_pipeline quickbooks quickbooks_customer_m1 \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.customer.yaml

community-connector create_pipeline quickbooks quickbooks_six_table_m3_cdc \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.all_tables_cdc.yaml

community-connector create_pipeline quickbooks quickbooks_six_table_m4_deletes \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.all_tables_cdc_deletes.yaml

community-connector create_pipeline quickbooks quickbooks_tenant_m5 \
  --pipeline-spec \
  src/databricks/labs/community_connector/sources/quickbooks/pipeline_spec.multi_tenant.yaml
```
