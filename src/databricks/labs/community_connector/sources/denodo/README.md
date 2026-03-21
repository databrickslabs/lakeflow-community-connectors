# Denodo Virtual DataPort Connector

Ingest data from [Denodo](https://www.denodo.com/) Data Virtualization Platform
into Databricks Lakeflow via the PostgreSQL-compatible protocol.

## Prerequisites

- Denodo Virtual DataPort 9.4+ with PostgreSQL interface enabled (port 9996)
- `psycopg2-binary` installed on the Databricks cluster
- Denodo user with read access to the target Virtual Database

## Connection Parameters

| Parameter  | Required | Default  | Description                           |
|------------|----------|----------|---------------------------------------|
| host       | Yes      | —        | Denodo VDP server hostname or IP      |
| port       | No       | 9996     | PostgreSQL interface port             |
| database   | Yes      | —        | Virtual Database (VDB) name           |
| user       | Yes      | —        | Denodo username                       |
| password   | Yes      | —        | Denodo password (use Databricks Secrets) |
| ssl_mode   | No       | prefer   | SSL mode: disable, prefer, require    |

## Ingestion Options

| Option           | Default | Description                                      |
|------------------|---------|--------------------------------------------------|
| schemas          | (all)   | Comma-separated list of schemas to include        |
| tables           | (all)   | Comma-separated list of `schema.table` to include |
| exclude_tables   | (none)  | Comma-separated list of `schema.table` to exclude |
| sync_mode        | full    | `full` (snapshot) or `incremental` (cursor-based) |
| cursor_column    | —       | Default cursor column for incremental sync        |
| cursor_overrides | —       | Per-table cursor overrides: `s.t=col,s.t2=col2`  |
| batch_size       | 10000   | Max records per incremental batch                 |

## Table Discovery

The connector dynamically discovers all views in the configured Virtual
Database by querying `information_schema.tables`. Tables are identified
as `{schema}.{table_name}`.

## Sync Modes

### Full Load (snapshot)

Reads all rows from each view on every pipeline run. Best for small
reference tables or when complete refreshes are acceptable.

### Incremental (cursor-column CDC)

Uses a timestamp or monotonically increasing column to fetch only
new/changed rows since the last run. Configure `sync_mode=incremental`
and `cursor_column=updated_at` (or equivalent).

> **Note:** Deleted records are not detected. For true CDC, configure
> change tracking at the source system level and expose it through Denodo.

## Cluster Setup

Add `psycopg2-binary` to your Databricks cluster:

- **Libraries tab:** Add PyPI package `psycopg2-binary`
- **Or init script:** `pip install psycopg2-binary`
