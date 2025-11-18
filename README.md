# Lakeflow Community Connectors

Lakeflow community connectors are built on top of the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) and [Spark Declarative Pipeline (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). These connectors enable users to ingest data from various source systems.

Each connector consists of two parts:
1. Source-specific implementation
2. Shared library and SDP definition

## Connectors

Check the `sources/` directory for available source connectors, which contain the source-specific implementation of an interface defined in `sources/interface/lakeflow_connect.py`.

The `libs/` and `pipeline/` directories include the shared source code across all source connectors.

## Create New Connectors

Users can follow the instructions in `prompts/vibe_coding_instruction.md` to create new connectors.

## Tests

This directory includes generic shared test suites to validate any connector source implementation.


## TODO:
1. Add dev guidelines
2. Add general instruction on how to use community connector (e.g. update spec and create a SDP for ingestion)
