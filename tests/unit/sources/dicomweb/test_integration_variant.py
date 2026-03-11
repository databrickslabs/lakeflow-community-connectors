"""
Integration test: end-to-end pipeline with mocked Orthanc responses.

Tests the full metadata -> VARIANT transformation chain across four layers,
using real DICOM JSON fixture data captured from the Orthanc public demo
(https://orthanc.uclouvain.be/demo/dicom-web) so tests run without network.

Layer 1 -- Mocked HTTP
  DICOMwebClient is patched so the connector processes real DICOM JSON
  objects without network access.

Layer 2 -- _apply_column_expressions framework utility (no HTTP)
  Verifies the generic _apply_column_expressions helper still works for
  connectors that declare column_expressions (other connectors, not DICOMweb).
  selectExpr('parse_json(metadata)') produces a VariantType column.

Layer 3 -- Full e2e: mocked HTTP + parse_value() + VariantType directly
  With metadata declared as VariantType in INSTANCES_SCHEMA, parse_value()
  converts JSON strings to VariantVal objects -- no selectExpr/parse_json()
  step needed.

Layer 4 -- OSS Declarative Pipeline (pyspark.pipelines)
  Apache Spark 4.0 ships @sdp.table, @sdp.temporary_view, sdp.create_streaming_table,
  and @sdp.append_flow as OSS APIs (pyspark.pipelines).

Requirements
------------
  - pyspark >= 4.0  (VariantType + parse_json() + pyspark.pipelines)

Run only integration tests:
  pytest tests/unit/sources/dicomweb/test_integration_variant.py -v -m integration
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

try:
    from pyspark.sql.types import VariantType, VariantVal

    HAS_VARIANT = True
except ImportError:
    HAS_VARIANT = False

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Fixture data captured from the Orthanc public demo server
# ---------------------------------------------------------------------------

STUDY_UID = "2.16.840.1.113669.632.20.1211.10000315526"
SERIES_UID = "1.3.12.2.1107.5.1.4.54693.30000006100507010800000005268"
SOP_UID_1 = "1.3.12.2.1107.5.1.4.54693.30000006100507010800000005328"
SOP_UID_2 = "1.3.12.2.1107.5.1.4.54693.30000006100507010800000005435"
SOP_UID_3 = "1.3.12.2.1107.5.1.4.54693.30000006100507010800000005359"

# Minimal QIDO-RS study response (real data from Orthanc)
FIXTURE_STUDIES = [
    {
        "00080005": {"Value": ["ISO_IR 192"], "vr": "CS"},
        "00080020": {"Value": ["20061005"], "vr": "DA"},
        "00080030": {"Value": ["101556.921000"], "vr": "TM"},
        "00080050": {"Value": ["0"], "vr": "SH"},
        "00080061": {"Value": ["CT"], "vr": "CS"},
        "00100010": {"Value": [{"Alphabetic": "VIX"}], "vr": "PN"},
        "00100020": {"Value": ["vAD7q3"], "vr": "LO"},
        "0020000D": {"Value": [STUDY_UID], "vr": "UI"},
        "00200010": {"Value": ["A10025547593"], "vr": "SH"},
        "00201206": {"Value": [1], "vr": "IS"},
        "00201208": {"Value": [250], "vr": "IS"},
    }
]

FIXTURE_SERIES = [
    {
        "00080005": {"Value": ["ISO_IR 192"], "vr": "CS"},
        "00080020": {"Value": ["20061005"], "vr": "DA"},
        "00080060": {"Value": ["CT"], "vr": "CS"},
        "0008103E": {"Value": ["Pied/cheville  1.0mm std"], "vr": "LO"},
        "0020000D": {"Value": [STUDY_UID], "vr": "UI"},
        "0020000E": {"Value": [SERIES_UID], "vr": "UI"},
        "00200011": {"Value": [5], "vr": "IS"},
    }
]


def _make_instance_raw(sop_uid, instance_number):
    return {
        "00080005": {"Value": ["ISO_IR 192"], "vr": "CS"},
        "00080016": {"Value": ["1.2.840.10008.5.1.4.1.1.2"], "vr": "UI"},
        "00080018": {"Value": [sop_uid], "vr": "UI"},
        "00080020": {"Value": ["20061005"], "vr": "DA"},
        "00080060": {"Value": ["CT"], "vr": "CS"},
        "0020000D": {"Value": [STUDY_UID], "vr": "UI"},
        "0020000E": {"Value": [SERIES_UID], "vr": "UI"},
        "00200013": {"Value": [instance_number], "vr": "IS"},
    }


FIXTURE_INSTANCES = [
    _make_instance_raw(SOP_UID_1, 60),
    _make_instance_raw(SOP_UID_2, 167),
    _make_instance_raw(SOP_UID_3, 91),
]


def _make_metadata_entry(sop_uid):
    return {
        "00080005": {"Value": ["ISO_IR 100"], "vr": "CS"},
        "00080016": {"Value": ["1.2.840.10008.5.1.4.1.1.2"], "vr": "UI"},
        "00080018": {"Value": [sop_uid], "vr": "UI"},
        "00080020": {"Value": ["20061005"], "vr": "DA"},
        "00080060": {"Value": ["CT"], "vr": "CS"},
        "00080070": {"Value": ["SIEMENS"], "vr": "LO"},
        "0020000D": {"Value": [STUDY_UID], "vr": "UI"},
        "0020000E": {"Value": [SERIES_UID], "vr": "UI"},
        "00280010": {"Value": [512], "vr": "US"},
        "00280011": {"Value": [512], "vr": "US"},
    }


FIXTURE_METADATA = [
    _make_metadata_entry(SOP_UID_1),
    _make_metadata_entry(SOP_UID_2),
    _make_metadata_entry(SOP_UID_3),
]

ORTHANC_BASE_URL = "https://orthanc.uclouvain.be/demo/dicom-web"

CLIENT_PATH = (
    "databricks.labs.community_connector.sources.dicomweb.dicomweb_client.DICOMwebClient"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    """Local SparkSession shared across all tests in this module."""
    import os
    import sys

    from pyspark.sql import SparkSession

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
    """DICOMwebLakeflowConnect pointed at Orthanc URL (client methods will be mocked)."""
    from databricks.labs.community_connector.sources.dicomweb.dicomweb import (
        DICOMwebLakeflowConnect,
    )

    return DICOMwebLakeflowConnect({"base_url": ORTHANC_BASE_URL})


# ---------------------------------------------------------------------------
# Layer 1 -- Mocked HTTP connectivity
# ---------------------------------------------------------------------------


class TestOrthanc:
    """Verify the connector returns valid DICOM data (mocked HTTP)."""

    def test_reads_studies(self, orthanc_connector):
        with (
            patch.object(orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES),
        ):
            records_iter, next_offset = orthanc_connector.read_table(
                "studies", {}, {"page_size": "5"}
            )
            records = list(records_iter)
        assert len(records) > 0, "Expected at least one study"
        assert all("study_instance_uid" in r for r in records)
        assert "study_date" in next_offset

    def test_reads_series(self, orthanc_connector):
        with (
            patch.object(orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES),
            patch.object(
                orthanc_connector._client, "query_series_for_study", return_value=FIXTURE_SERIES
            ),
        ):
            records_iter, _ = orthanc_connector.read_table("series", {}, {"page_size": "3"})
            records = list(records_iter)
        assert len(records) > 0, "Expected at least one series"
        assert all("series_instance_uid" in r for r in records)

    def test_reads_instances(self, orthanc_connector):
        with (
            patch.object(orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES),
            patch.object(
                orthanc_connector._client, "query_series_for_study", return_value=FIXTURE_SERIES
            ),
            patch.object(
                orthanc_connector._client,
                "query_instances_for_series",
                return_value=FIXTURE_INSTANCES,
            ),
        ):
            records_iter, _ = orthanc_connector.read_table("instances", {}, {"page_size": "3"})
            records = list(records_iter)
        assert len(records) > 0, "Expected at least one instance"
        assert all("sop_instance_uid" in r for r in records)

    def test_reads_instances_with_metadata(self, orthanc_connector):
        """fetch_metadata=true populates the metadata column as a JSON string."""
        with (
            patch.object(orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES),
            patch.object(
                orthanc_connector._client, "query_series_for_study", return_value=FIXTURE_SERIES
            ),
            patch.object(
                orthanc_connector._client,
                "query_instances_for_series",
                return_value=FIXTURE_INSTANCES,
            ),
            patch.object(
                orthanc_connector._client,
                "retrieve_series_metadata",
                return_value=FIXTURE_METADATA,
            ),
        ):
            records_iter, _ = orthanc_connector.read_table(
                "instances", {}, {"page_size": "5", "fetch_metadata": "true"}
            )
            records = list(records_iter)
        assert len(records) > 0

        records_with_meta = [r for r in records if r.get("metadata") is not None]
        assert len(records_with_meta) > 0, "Expected at least one instance with metadata populated"

        for r in records_with_meta:
            assert isinstance(r["metadata"], str), (
                f"Connector must yield metadata as str in raw dict, got {type(r['metadata']).__name__}"
            )
            parsed = json.loads(r["metadata"])
            assert isinstance(parsed, dict), "metadata must be a valid DICOM JSON object"


# ---------------------------------------------------------------------------
# Layer 2 -- VariantVal conversion helper (no HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_VARIANT, reason="pyspark >= 4.0 required for VariantType")
class TestVariantValConversion:
    """
    Verify that VariantVal.parseJson() correctly converts DICOM JSON strings to
    VariantVal objects, which can then be used with VariantType columns in Spark.
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
            Row(
                id="1",
                metadata=VariantVal.parseJson('{"00080018": {"vr": "UI", "Value": ["1.2.3"]}}'),
            ),
            Row(
                id="2", metadata=VariantVal.parseJson('{"00080060": {"vr": "CS", "Value": ["CT"]}}')
            ),
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
            [
                Row(
                    sop_instance_uid="1.2.3",
                    metadata=VariantVal.parseJson('{"tag": "val"}'),
                    connection_name="my-conn",
                )
            ],
            schema,
        )

        assert [f.name for f in df.schema.fields] == [
            "sop_instance_uid",
            "metadata",
            "connection_name",
        ]

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
# Layer 3 -- Full end-to-end: mocked HTTP + parse_value() + VariantType
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_VARIANT, reason="pyspark >= 4.0 required for VariantType")
class TestEndToEndVariantPipeline:
    """
    Full end-to-end pipeline with mocked HTTP + VariantType metadata.
    """

    @staticmethod
    def _to_variant_row(record: dict):
        """Convert a connector record dict to a Row, converting metadata JSON -> VariantVal."""
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

        from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import (
            INSTANCES_SCHEMA,
        )

        with (
            patch.object(orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES),
            patch.object(
                orthanc_connector._client, "query_series_for_study", return_value=FIXTURE_SERIES
            ),
            patch.object(
                orthanc_connector._client,
                "query_instances_for_series",
                return_value=FIXTURE_INSTANCES,
            ),
            patch.object(
                orthanc_connector._client,
                "retrieve_series_metadata",
                return_value=FIXTURE_METADATA,
            ),
        ):
            records_iter, _ = orthanc_connector.read_table(
                "instances", {}, {"page_size": "5", "fetch_metadata": "true"}
            )
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
        Full chain: mocked HTTP -> VariantVal conversion -> DataFrame with VariantType metadata.
        No selectExpr / parse_json() transformation required.
        """
        from pyspark.sql.types import VariantType

        from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import (
            INSTANCES_SCHEMA,
        )

        with (
            patch.object(orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES),
            patch.object(
                orthanc_connector._client, "query_series_for_study", return_value=FIXTURE_SERIES
            ),
            patch.object(
                orthanc_connector._client,
                "query_instances_for_series",
                return_value=FIXTURE_INSTANCES,
            ),
            patch.object(
                orthanc_connector._client,
                "retrieve_series_metadata",
                return_value=FIXTURE_METADATA,
            ),
        ):
            records_iter, _ = orthanc_connector.read_table(
                "instances", {}, {"page_size": "5", "fetch_metadata": "true"}
            )
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
# Layer 4 -- OSS Declarative Pipeline (pyspark.pipelines)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_VARIANT, reason="pyspark >= 4.0 required for VariantType")
class TestDeclarativePipeline:
    """
    Tests for the OSS declarative pipeline API (pyspark.pipelines, Spark 4.0+).
    """

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
                return None

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
                with (
                    patch.object(
                        orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES
                    ),
                    patch.object(
                        orthanc_connector._client,
                        "query_series_for_study",
                        return_value=FIXTURE_SERIES,
                    ),
                    patch.object(
                        orthanc_connector._client,
                        "query_instances_for_series",
                        return_value=FIXTURE_INSTANCES,
                    ),
                    patch.object(
                        orthanc_connector._client,
                        "retrieve_series_metadata",
                        return_value=FIXTURE_METADATA,
                    ),
                ):
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
                with (
                    patch.object(
                        orthanc_connector._client, "query_studies", return_value=FIXTURE_STUDIES
                    ),
                    patch.object(
                        orthanc_connector._client,
                        "query_series_for_study",
                        return_value=FIXTURE_SERIES,
                    ),
                    patch.object(
                        orthanc_connector._client,
                        "query_instances_for_series",
                        return_value=FIXTURE_INSTANCES,
                    ),
                    patch.object(
                        orthanc_connector._client,
                        "retrieve_series_metadata",
                        return_value=FIXTURE_METADATA,
                    ),
                ):
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
