"""
Integration test: end-to-end pipeline with real Orthanc demo endpoint.

Tests the full metadata → VARIANT transformation chain across four layers:

Layer 1 — Real HTTP
  DICOMwebClient hits the public Orthanc demo and returns raw records.
  Connector yields metadata as JSON strings in raw Python dicts.

Layer 2 — _apply_column_expressions framework utility (no HTTP)
  Verifies the generic _apply_column_expressions helper still works for
  connectors that declare column_expressions (other connectors, not DICOMweb).
  selectExpr('parse_json(metadata)') produces a VariantType column.

Layer 3 — Full e2e: real HTTP + parse_value() + VariantType directly
  With metadata declared as VariantType in INSTANCES_SCHEMA, parse_value()
  converts JSON strings to VariantVal objects — no selectExpr/parse_json()
  step needed.  The simplified flow:

      DICOMweb HTTP → connector records (metadata as JSON str in raw dict)
        → parse_value(record, INSTANCES_SCHEMA) → Row with metadata as VariantVal
        → spark.createDataFrame(rows, INSTANCES_SCHEMA) → VariantType column directly

Layer 4 — OSS Declarative Pipeline (pyspark.pipelines)
  Apache Spark 4.0 ships @sdp.table, @sdp.temporary_view, sdp.create_streaming_table,
  and @sdp.append_flow as OSS APIs (pyspark.pipelines).  Note: apply_changes() is
  a Databricks extension and is NOT available in OSS Spark.

  The pipeline execution engine (start_run) uses Spark Connect and requires a running
  server.  For testing purposes we use the graph_element_registration_context directly:
    1. Register pipeline elements with @sdp.table / @sdp.temporary_view using a
       capturing GraphElementRegistry.
    2. Extract the Flow objects and call flow.func() directly — this is exactly what
       the pipeline scheduler does at execution time.
    3. Verify that the resulting DataFrames have the correct schema and data.

  The new architecture: parse_value() handles VariantVal conversion, so @sdp.table
  bodies produce VariantType DataFrames directly without _apply_column_expressions.

Requirements
------------
  - pyspark >= 4.0  (VariantType + parse_json() + pyspark.pipelines; 4.1.1 is in .venv)
  - Network access to https://orthanc.uclouvain.be/demo/dicom-web

Run only integration tests:
  pytest tests/test_integration_variant.py -v -m integration

Run all tests (including integration):
  pytest tests/ -v
"""

from __future__ import annotations

import json

import pytest

# VariantType / VariantVal were introduced in Apache Spark 4.0 / pyspark 4.0.
try:
    from pyspark.sql.types import VariantType, VariantVal

    HAS_VARIANT = True
except ImportError:
    HAS_VARIANT = False

pytestmark = pytest.mark.integration

ORTHANC_BASE_URL = "https://orthanc.uclouvain.be/demo/dicom-web"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    """Local SparkSession shared across all tests in this module."""
    import os
    import sys

    from pyspark.sql import SparkSession

    # Pin worker Python to the same executable as the driver (the venv's Python).
    # Without this, Spark may spawn workers with the system Python, causing a
    # PYTHON_VERSION_MISMATCH error when driver and workers differ.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder.master("local[1]")
        .appName("dicomweb-integration-variant-test")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def orthanc_connector():
    """DICOMwebLakeflowConnect pointed at the public Orthanc demo."""
    from databricks.labs.community_connector.sources.dicomweb.dicomweb import DICOMwebLakeflowConnect

    return DICOMwebLakeflowConnect({"base_url": ORTHANC_BASE_URL, "auth_type": "none"})


# ---------------------------------------------------------------------------
# Layer 1 — Real HTTP connectivity
# ---------------------------------------------------------------------------


class TestOrthanc:
    """Verify the public Orthanc demo endpoint returns valid DICOM data."""

    def test_reads_studies(self, orthanc_connector):
        records_iter, next_offset = orthanc_connector.read_table("studies", {}, {"page_size": "5"})
        records = list(records_iter)
        assert len(records) > 0, "Expected at least one study from Orthanc demo"
        assert all("study_instance_uid" in r for r in records)
        assert "study_date" in next_offset

    def test_reads_series(self, orthanc_connector):
        records_iter, _ = orthanc_connector.read_table("series", {}, {"page_size": "3"})
        records = list(records_iter)
        assert len(records) > 0, "Expected at least one series from Orthanc demo"
        assert all("series_instance_uid" in r for r in records)

    def test_reads_instances(self, orthanc_connector):
        records_iter, _ = orthanc_connector.read_table("instances", {}, {"page_size": "3"})
        records = list(records_iter)
        assert len(records) > 0, "Expected at least one instance from Orthanc demo"
        assert all("sop_instance_uid" in r for r in records)

    def test_reads_instances_with_metadata(self, orthanc_connector):
        """fetch_metadata=true populates the metadata column as a JSON string in the raw dict."""
        records_iter, _ = orthanc_connector.read_table("instances", {}, {"page_size": "5", "fetch_metadata": "true"})
        records = list(records_iter)
        assert len(records) > 0

        records_with_meta = [r for r in records if r.get("metadata") is not None]
        assert len(records_with_meta) > 0, "Expected at least one instance with metadata populated"

        for r in records_with_meta:
            # Connector yields metadata as a plain JSON string in the raw Python dict.
            # parse_value() converts it to VariantVal when building the DataFrame.
            assert isinstance(r["metadata"], str), (
                f"Connector must yield metadata as str in raw dict, got {type(r['metadata']).__name__}"
            )
            parsed = json.loads(r["metadata"])
            assert isinstance(parsed, dict), "metadata must be a valid DICOM JSON object"


# ---------------------------------------------------------------------------
# Layer 2 — VariantVal conversion helper (no HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_VARIANT, reason="pyspark >= 4.0 required for VariantType")
class TestVariantValConversion:
    """
    Verify that VariantVal.parseJson() correctly converts DICOM JSON strings to
    VariantVal objects, which can then be used with VariantType columns in Spark.

    In the DICOMweb connector the metadata JSON string returned by the connector
    is converted to VariantVal before building the DataFrame. This is the
    standard approach for populating VariantType columns from Python data.
    """

    def test_parse_json_produces_variant_column(self, spark):
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType, VariantType

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("metadata", VariantType(), True),
            ]
        )
        data = [
            Row(id="1", metadata=VariantVal.parseJson('{"00080018": {"vr": "UI", "Value": ["1.2.3"]}}')),
            Row(id="2", metadata=VariantVal.parseJson('{"00080060": {"vr": "CS", "Value": ["CT"]}}')),
            Row(id="3", metadata=None),
        ]
        df = spark.createDataFrame(data, schema)

        meta_field = next(f for f in df.schema.fields if f.name == "metadata")
        assert isinstance(meta_field.dataType, VariantType), (
            f"Expected VariantType for 'metadata' column, got {meta_field.dataType}"
        )

    def test_non_metadata_columns_keep_original_type(self, spark):
        """VariantType only applies to the metadata column; all others stay unchanged."""
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType, VariantType

        schema = StructType(
            [
                StructField("sop_instance_uid", StringType(), False),
                StructField("metadata", VariantType(), True),
                StructField("connection_name", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [Row(sop_instance_uid="1.2.3", metadata=VariantVal.parseJson('{"tag": "val"}'), connection_name="my-conn")],
            schema,
        )

        assert [f.name for f in df.schema.fields] == ["sop_instance_uid", "metadata", "connection_name"]

        sop_field = next(f for f in df.schema.fields if f.name == "sop_instance_uid")
        conn_field = next(f for f in df.schema.fields if f.name == "connection_name")
        meta_field = next(f for f in df.schema.fields if f.name == "metadata")

        assert isinstance(sop_field.dataType, StringType)
        assert isinstance(conn_field.dataType, StringType)
        assert isinstance(meta_field.dataType, VariantType)

    def test_null_metadata_survives(self, spark):
        """Rows with null metadata must not raise errors with VariantType schema."""
        from pyspark.sql import Row
        from pyspark.sql.types import StringType, StructField, StructType, VariantType

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("metadata", VariantType(), True),
            ]
        )
        df = spark.createDataFrame([Row(id="1", metadata=None), Row(id="2", metadata=None)], schema)
        rows = df.collect()
        assert len(rows) == 2
        assert all(r["metadata"] is None for r in rows)


# ---------------------------------------------------------------------------
# Layer 3 — Full end-to-end: real HTTP + parse_value() + VariantType directly
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_VARIANT, reason="pyspark >= 4.0 required for VariantType")
class TestEndToEndVariantPipeline:
    """
    Full end-to-end pipeline with real Orthanc HTTP + VariantType metadata.

    The connector returns metadata as a JSON string in the raw dict. Before
    creating a Spark DataFrame with INSTANCES_SCHEMA (which declares metadata as
    VariantType), the JSON string must be converted to a VariantVal object using
    VariantVal.parseJson().  This is exactly what the Databricks pipeline runtime
    does when materialising Variant columns from Python data sources.

    Flow:
        real HTTP (JSON str in raw dict)
          → _to_variant_row()  (JSON str → VariantVal)
          → spark.createDataFrame(rows, INSTANCES_SCHEMA)
          → metadata column is VariantType
    """

    @staticmethod
    def _to_variant_row(record: dict):
        """Convert a connector record dict to a Row, converting metadata JSON → VariantVal."""
        from pyspark.sql import Row

        d = dict(record)
        if d.get("metadata") is not None:
            d["metadata"] = VariantVal.parseJson(d["metadata"])
        return Row(**d)

    def test_read_table_metadata_has_no_column_expressions(self, orthanc_connector):
        """read_table_metadata must NOT return column_expressions for any table."""
        for table in ("studies", "series", "instances"):
            meta = orthanc_connector.read_table_metadata(table, {})
            assert "column_expressions" not in meta, (
                f"read_table_metadata('{table}') must not return column_expressions"
            )

    def test_instances_metadata_is_variant_directly(self, spark, orthanc_connector):
        """After VariantVal.parseJson() conversion, metadata DataFrame has VariantType."""
        from pyspark.sql.types import VariantType

        from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import INSTANCES_SCHEMA

        records_iter, _ = orthanc_connector.read_table("instances", {}, {"page_size": "5", "fetch_metadata": "true"})
        records = list(records_iter)
        assert len(records) > 0

        rows = [self._to_variant_row(r) for r in records]
        df = spark.createDataFrame(rows, INSTANCES_SCHEMA)

        meta_field = next(f for f in df.schema.fields if f.name == "metadata")
        assert isinstance(meta_field.dataType, VariantType), (
            f"metadata must be VariantType after VariantVal.parseJson() conversion, got {meta_field.dataType}"
        )

    def test_full_chain_no_selectexpr_needed(self, spark, orthanc_connector):
        """
        Full chain: real HTTP → VariantVal conversion → DataFrame with VariantType metadata.
        No selectExpr / parse_json() transformation required.
        """
        from pyspark.sql.types import VariantType

        from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import INSTANCES_SCHEMA

        records_iter, _ = orthanc_connector.read_table("instances", {}, {"page_size": "5", "fetch_metadata": "true"})
        records = list(records_iter)
        assert len(records) > 0
        assert any(r.get("metadata") is not None for r in records), (
            "At least one instance must have metadata for a meaningful test"
        )

        rows = [self._to_variant_row(r) for r in records]
        df = spark.createDataFrame(rows, INSTANCES_SCHEMA)

        meta_field = next(f for f in df.schema.fields if f.name == "metadata")
        assert isinstance(meta_field.dataType, VariantType), (
            f"metadata must be VariantType, got {meta_field.dataType}"
        )

        collected = df.collect()
        assert len(collected) == len(records)
        assert all(r["sop_instance_uid"] is not None for r in collected)


# ---------------------------------------------------------------------------
# Layer 4 — OSS Declarative Pipeline (pyspark.pipelines)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_VARIANT, reason="pyspark >= 4.0 required for VariantType")
class TestDeclarativePipeline:
    """
    Tests for the OSS declarative pipeline API (pyspark.pipelines, Spark 4.0+).

    The pipeline execution engine (spark_connect_pipeline.start_run) uses Spark Connect
    and requires a running server.  We test the pipeline definition and execution logic
    directly using graph_element_registration_context + direct flow.func() invocation,
    which is exactly what the pipeline scheduler does at runtime.

    Note: apply_changes() is a Databricks extension absent from OSS Spark.  The OSS
    equivalents used here are @sdp.table (batch replace) and @sdp.append_flow (streaming
    append).  These are sufficient to test the VariantType metadata pipeline.

    New architecture: parse_value() converts JSON → VariantVal, so @sdp.table bodies
    produce VariantType DataFrames directly — no _apply_column_expressions needed.
    """

    # ------------------------------------------------------------------
    # Helper: minimal in-memory GraphElementRegistry
    # ------------------------------------------------------------------

    @staticmethod
    def _make_capturing_registry():
        """Return a (registry, outputs_list, flows_list) triple for inspection."""
        from pathlib import Path

        from pyspark.pipelines.flow import Flow
        from pyspark.pipelines.graph_element_registry import GraphElementRegistry
        from pyspark.pipelines.output import Output

        outputs: list = []
        flows: list = []

        class CapturingRegistry(GraphElementRegistry):
            def register_output(self, output: Output) -> None:
                outputs.append(output)

            def register_flow(self, flow: Flow) -> None:
                flows.append(flow)

            def register_sql(self, sql_text: str, file_path: Path) -> None:
                pass

        return CapturingRegistry(), outputs, flows

    # ------------------------------------------------------------------
    # Registration tests (no Spark needed)
    # ------------------------------------------------------------------

    def test_streaming_table_and_append_flow_register(self):
        """sdp.create_streaming_table + @sdp.append_flow register the correct graph elements."""
        import pyspark.pipelines as sdp
        from pyspark.pipelines.graph_element_registry import graph_element_registration_context
        from pyspark.pipelines.output import StreamingTable

        registry, outputs, flows = self._make_capturing_registry()

        with graph_element_registration_context(registry):
            sdp.create_streaming_table(name="dicom_instances")

            @sdp.append_flow(target="dicom_instances", name="instances_flow")
            def instances_flow_fn():
                return None  # body not called during registration

        output_names = {o.name for o in outputs}
        flow_names = {f.name for f in flows}

        assert "dicom_instances" in output_names
        assert isinstance(next(o for o in outputs if o.name == "dicom_instances"), StreamingTable)
        assert "instances_flow" in flow_names

    def test_table_and_temporary_view_register(self):
        """@sdp.table + @sdp.temporary_view register the correct graph elements."""
        import pyspark.pipelines as sdp
        from pyspark.pipelines.graph_element_registry import graph_element_registration_context
        from pyspark.pipelines.output import TemporaryView

        registry, outputs, flows = self._make_capturing_registry()

        with graph_element_registration_context(registry):

            @sdp.temporary_view(name="instances_raw")
            def instances_raw_fn():
                return None

            @sdp.table(name="instances_final")
            def instances_final_fn():
                return None

        output_names = {o.name for o in outputs}
        assert "instances_raw" in output_names
        assert "instances_final" in output_names
        assert isinstance(next(o for o in outputs if o.name == "instances_raw"), TemporaryView)
        assert len(flows) == 2

    # ------------------------------------------------------------------
    # Execution tests (Spark required)
    # ------------------------------------------------------------------

    def test_sdp_table_body_produces_variant_from_real_data(self, spark, orthanc_connector):
        """
        Simulate the pipeline scheduler: capture an @sdp.table definition then call
        flow.func() directly with the test SparkSession.

        Pipeline topology being tested:
          [Orthanc DICOMweb]
              ↓  (HTTP / QIDO-RS + WADO-RS)
          @sdp.temporary_view("instances_raw")    — VariantVal.parseJson() → VariantType DF
              ↓  (no transformation needed)
          @sdp.table("instances")                 — reads VariantType DF as-is
        """
        import pyspark.pipelines as sdp
        from pyspark.pipelines.graph_element_registry import graph_element_registration_context
        from pyspark.sql import Row
        from pyspark.sql.types import VariantType

        from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import (
            INSTANCES_SCHEMA,
        )

        def to_variant_row(record):
            d = dict(record)
            if d.get("metadata") is not None:
                d["metadata"] = VariantVal.parseJson(d["metadata"])
            return Row(**d)

        registry, _, flows = self._make_capturing_registry()

        with graph_element_registration_context(registry):

            @sdp.temporary_view(name="instances_raw")
            def instances_raw_fn():
                records_iter, _ = orthanc_connector.read_table(
                    "instances", {}, {"page_size": "5", "fetch_metadata": "true"}
                )
                records = list(records_iter)
                rows = [to_variant_row(r) for r in records]
                return spark.createDataFrame(rows, INSTANCES_SCHEMA)

            @sdp.table(name="instances")
            def instances_fn():
                return spark.read.table("instances_raw")

        raw_flow = next(f for f in flows if f.name == "instances_raw")
        raw_df = raw_flow.func()

        meta_raw = next(f for f in raw_df.schema.fields if f.name == "metadata")
        assert isinstance(meta_raw.dataType, VariantType), (
            f"instances_raw view must have metadata as VariantType, got {meta_raw.dataType}"
        )

        raw_df.createOrReplaceTempView("instances_raw")

        table_flow = next(f for f in flows if f.name == "instances")
        result_df = table_flow.func()

        meta_final = next(f for f in result_df.schema.fields if f.name == "metadata")
        assert isinstance(meta_final.dataType, VariantType), (
            f"@sdp.table('instances') must have VariantType for metadata, got {meta_final.dataType}"
        )

        collected = result_df.collect()
        assert len(collected) > 0
        assert all(r["sop_instance_uid"] is not None for r in collected)

    def test_sdp_append_flow_body_produces_variant(self, spark, orthanc_connector):
        """
        Streaming pattern: sdp.create_streaming_table + @sdp.append_flow.
        Mirrors the Databricks pipeline pattern; metadata JSON → VariantVal before
        creating the DataFrame.
        """
        import pyspark.pipelines as sdp
        from pyspark.pipelines.graph_element_registry import graph_element_registration_context
        from pyspark.sql import Row
        from pyspark.sql.types import VariantType

        from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import (
            INSTANCES_SCHEMA,
        )

        def to_variant_row(record):
            d = dict(record)
            if d.get("metadata") is not None:
                d["metadata"] = VariantVal.parseJson(d["metadata"])
            return Row(**d)

        registry, _, flows = self._make_capturing_registry()

        with graph_element_registration_context(registry):
            sdp.create_streaming_table(name="instances_stream")

            @sdp.append_flow(target="instances_stream", name="instances_append")
            def instances_append_fn():
                records_iter, _ = orthanc_connector.read_table(
                    "instances", {}, {"page_size": "5", "fetch_metadata": "true"}
                )
                records = list(records_iter)
                rows = [to_variant_row(r) for r in records]
                return spark.createDataFrame(rows, INSTANCES_SCHEMA)

        append_flow = next(f for f in flows if f.name == "instances_append")
        result_df = append_flow.func()

        meta_field = next(f for f in result_df.schema.fields if f.name == "metadata")
        assert isinstance(meta_field.dataType, VariantType), (
            f"@sdp.append_flow body must produce VariantType for metadata, got {meta_field.dataType}"
        )

        collected = result_df.collect()
        assert len(collected) > 0
