# Lakeflow Community Connectors

Lakeflow community connectors are built on top of the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) and [Spark Declarative Pipeline (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). These connectors enable users to ingest data from various source systems.

Each connector consists of two parts:
1. Source-specific implementation
2. Shared library and SDP definition

## Connectors

This repo is currently focused on the `osipi` connector under `sources/osipi/`.

The connector interface is defined in `sources/interface/lakeflow_connect.py`.

The `libs/` and `pipeline/` directories include the shared source code across the connector.

## Deploy (Databricks workspace only; no local CLI)

If this repo is synced into a Databricks Repo, open and run:

- `tools/notebook_based_deployment/workspace_notebooks/deploy_notebook_based_dlt_pipelines.py`

That notebook:
- uploads the generated connector source into your workspace user folder
- generates and uploads one DLT notebook per `pipeline_group` from the CSV
- creates/updates DLT pipelines (and optional scheduled Jobs) via the Databricks SDK

## Tests

This directory includes generic shared test suites to validate any connector source implementation.


## Notes

- This repo intentionally omits other example connectors to stay minimal and OSIPI-focused.
