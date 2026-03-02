"""
Unit tests for DICOMwebLakeflowConnect, dicomweb_parser, and dicomweb_schemas.
HTTP calls are mocked via unittest.mock.patch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.community_connector.sources.dicomweb.dicomweb import (
    DICOMwebLakeflowConnect,
    _subtract_days,
)
from databricks.labs.community_connector.sources.dicomweb.dicomweb_parser import (
    parse_instance,
    parse_series,
    parse_study,
)
from databricks.labs.community_connector.sources.dicomweb.dicomweb_schemas import (
    DIAGNOSTICS_SCHEMA,
    INSTANCES_SCHEMA,
    SERIES_SCHEMA,
    STUDIES_SCHEMA,
    get_schema,
)

# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------


class TestParser:
    def test_parse_study_full(self, studies_response):
        record = parse_study(studies_response[0])
        assert record["study_instance_uid"] == "1.2.840.113619.2.5.1762583153.215519.978957063.78"
        assert record["study_date"] == "20231215"
        assert record["patient_name"] == "Doe^John"
        assert record["patient_id"] == "PID-001"
        assert record["accession_number"] == "ACC123456"
        assert record["modalities_in_study"] == ["CT", "SR"]
        assert record["number_of_study_related_series"] == 3
        assert record["number_of_study_related_instances"] == 450

    def test_parse_study_missing_optional_fields(self):
        minimal = {
            "0020000D": {"vr": "UI", "Value": ["1.2.3"]},
        }
        record = parse_study(minimal)
        assert record["study_instance_uid"] == "1.2.3"
        assert record["patient_name"] is None
        assert record["study_date"] is None

    def test_parse_series(self, series_response):
        record = parse_series(series_response[0])
        assert record["series_instance_uid"] == "1.3.12.2.1107.5.2.32.35162.2013120811373024696203156"
        assert record["modality"] == "CT"
        assert record["series_number"] == 1
        assert record["body_part_examined"] == "CHEST"

    def test_parse_instance(self, instances_response):
        record = parse_instance(instances_response[0])
        assert record["sop_instance_uid"] == "1.2.840.113619.2.5.1762583153.215519.978957063.78.1.1"
        assert record["instance_number"] == 1
        assert record["content_date"] == "20231215"
        assert record["dicom_file_path"] is None  # not yet populated
        assert record["metadata"] is None  # not yet populated

    def test_parse_pn_no_alphabetic(self):
        """PN tag without Alphabetic — fall back gracefully."""
        obj = {
            "00100010": {"vr": "PN", "Value": [{"Ideographic": "山田"}]},
            "0020000D": {"vr": "UI", "Value": ["1.2.3"]},
        }
        record = parse_study(obj)
        assert record["patient_name"] == "山田"

    def test_parse_empty_value_array(self):
        obj = {
            "0020000D": {"vr": "UI", "Value": []},
        }
        record = parse_study(obj)
        assert record["study_instance_uid"] is None

    def test_tag_case_insensitive(self):
        """Tags can be lowercase in some responses."""
        obj = {
            "0020000d": {"vr": "UI", "Value": ["1.2.3"]},
        }
        record = parse_study(obj)
        assert record["study_instance_uid"] == "1.2.3"


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------


class TestSchemas:
    def test_get_schema_studies(self):
        schema = get_schema("studies")
        field_names = [f.name for f in schema.fields]
        assert "study_instance_uid" in field_names
        assert "modalities_in_study" in field_names

    def test_get_schema_series(self):
        schema = get_schema("series")
        field_names = [f.name for f in schema.fields]
        assert "series_instance_uid" in field_names
        assert "modality" in field_names

    def test_get_schema_instances(self):
        from pyspark.sql.types import VariantType

        schema = get_schema("instances")
        field_names = [f.name for f in schema.fields]
        assert "sop_instance_uid" in field_names
        assert "dicom_file_path" in field_names
        assert "metadata" in field_names
        meta_field = next(f for f in schema.fields if f.name == "metadata")
        assert isinstance(meta_field.dataType, VariantType), (
            "metadata must be VariantType — parse_value() converts JSON strings to VariantVal"
        )

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            get_schema("patients")

    def test_study_instance_uid_not_nullable(self):
        uid_field = next(f for f in STUDIES_SCHEMA.fields if f.name == "study_instance_uid")
        assert uid_field.nullable is False

    def test_dicom_file_path_nullable(self):
        fp_field = next(f for f in INSTANCES_SCHEMA.fields if f.name == "dicom_file_path")
        assert fp_field.nullable is True


# ---------------------------------------------------------------------------
# Connector tests
# ---------------------------------------------------------------------------


class TestConnector:
    def test_missing_base_url_raises(self):
        with pytest.raises(ValueError, match="base_url"):
            DICOMwebLakeflowConnect({})

    def test_list_tables(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        tables = connector.list_tables()
        assert set(tables) == {"studies", "series", "instances", "diagnostics"}

    def test_get_table_schema_studies(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        schema = connector.get_table_schema("studies", {})
        assert schema == STUDIES_SCHEMA

    def test_get_table_schema_instances(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        schema = connector.get_table_schema("instances", {})
        assert schema == INSTANCES_SCHEMA

    def test_read_table_metadata(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        for table in ("studies", "series", "instances"):
            meta = connector.read_table_metadata(table, {})
            assert "primary_keys" in meta
            assert meta["cursor_field"] == "study_date"
            assert meta["ingestion_type"] == "cdc"
            # column_expressions not needed: metadata is VariantType in the schema
            # and parse_value() handles the JSON string → VariantVal conversion
            assert "column_expressions" not in meta

    def test_read_table_studies(self, dicomweb_options, studies_response):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        # First call returns data, second returns empty (pagination end)
        connector._client.query_studies = MagicMock(side_effect=[studies_response, []])
        records_iter, next_offset = connector.read_table("studies", {}, {})
        records = list(records_iter)
        assert len(records) == 2
        assert records[0]["study_instance_uid"] == "1.2.840.113619.2.5.1762583153.215519.978957063.78"
        assert "study_date" in next_offset
        assert next_offset["page_offset"] == 0
        # connection_name defaults to base_url when not explicitly set
        assert all(r["connection_name"] == dicomweb_options["base_url"] for r in records)

    def test_read_table_series(self, dicomweb_options, studies_response, series_response):
        """Series now uses hierarchical: studies → query_series_for_study per study."""
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        # Two studies returned, then pagination ends
        connector._client.query_studies = MagicMock(side_effect=[studies_response, []])
        # Each study returns 3 series
        connector._client.query_series_for_study = MagicMock(return_value=series_response)
        records_iter, _ = connector.read_table("series", {}, {})
        records = list(records_iter)
        # 2 studies × 3 series = 6 total
        assert len(records) == 6
        assert records[0]["series_instance_uid"] is not None
        # query_series_for_study called once per study
        assert connector._client.query_series_for_study.call_count == 2

    def test_read_table_instances_no_files(
        self, dicomweb_options, studies_response, series_response, instances_response
    ):
        """Instances now uses hierarchical: studies → series → instances."""
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_studies = MagicMock(side_effect=[studies_response, []])
        connector._client.query_series_for_study = MagicMock(return_value=series_response[:1])  # 1 series per study
        connector._client.query_instances_for_series = MagicMock(return_value=instances_response)
        records_iter, _ = connector.read_table("instances", {}, {})
        records = list(records_iter)
        # 2 studies × 1 series × 3 instances = 6
        assert len(records) == 6
        assert all(r["dicom_file_path"] is None for r in records)
        assert all(r["metadata"] is None for r in records)
        assert all(r["connection_name"] == dicomweb_options["base_url"] for r in records)

    def test_read_table_instances_fetch_files_missing_volume_raises(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        with pytest.raises(ValueError, match="dicom_volume_path"):
            records_iter, _ = connector.read_table(
                "instances",
                {},
                {"fetch_dicom_files": "true"},
            )
            list(records_iter)  # iterator must be consumed to trigger

    def test_read_table_instances_fetch_files_full_mode(
        self, dicomweb_options, studies_response, series_response, instances_response, tmp_path
    ):
        """fetch_dicom_files=true with wado_mode=full downloads .dcm files."""
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_studies = MagicMock(side_effect=[studies_response[:1], []])
        connector._client.query_series_for_study = MagicMock(return_value=series_response[:1])
        connector._client.query_instances_for_series = MagicMock(return_value=instances_response)
        connector._client.retrieve_instance = MagicMock(return_value=b"DICMDATA")

        records_iter, _ = connector.read_table(
            "instances",
            {},
            {"fetch_dicom_files": "true", "dicom_volume_path": str(tmp_path), "wado_mode": "full"},
        )
        records = list(records_iter)
        assert len(records) == 3
        assert all(r["dicom_file_path"] is not None for r in records)
        assert all(r["dicom_file_path"].endswith(".dcm") for r in records)
        for r in records:
            assert pathlib.Path(r["dicom_file_path"]).exists()

    def test_read_table_instances_fetch_files_frames_mode(
        self, dicomweb_options, studies_response, series_response, instances_response, tmp_path
    ):
        """fetch_dicom_files=true with wado_mode=frames downloads .jpg frame files."""
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_studies = MagicMock(side_effect=[studies_response[:1], []])
        connector._client.query_series_for_study = MagicMock(return_value=series_response[:1])
        connector._client.query_instances_for_series = MagicMock(return_value=instances_response)
        connector._client.retrieve_instance_frames = MagicMock(return_value=b"\xff\xd8\xff\xe0JFIF")

        records_iter, _ = connector.read_table(
            "instances",
            {},
            {"fetch_dicom_files": "true", "dicom_volume_path": str(tmp_path), "wado_mode": "frames"},
        )
        records = list(records_iter)
        assert len(records) == 3
        assert all(r["dicom_file_path"] is not None for r in records)
        assert all(r["dicom_file_path"].endswith(".jpg") for r in records)
        connector._client.retrieve_instance_frames.assert_called()
        connector._client.retrieve_instance = MagicMock()  # should NOT be called
        connector._client.retrieve_instance.assert_not_called()

    def test_read_table_instances_wado_auto_fallback_to_frames(
        self, dicomweb_options, studies_response, series_response, instances_response, tmp_path
    ):
        """wado_mode=auto: 404 from full endpoint triggers frame retrieval and caches detection."""
        import urllib.error
        from io import BytesIO

        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_studies = MagicMock(side_effect=[studies_response[:1], []])
        connector._client.query_series_for_study = MagicMock(return_value=series_response[:1])
        connector._client.query_instances_for_series = MagicMock(return_value=instances_response[:1])

        # Full WADO-RS returns 404 → auto-detects frames mode
        http_error = urllib.error.HTTPError(
            url="https://dicomweb.example.com/studies/x/series/y/instances/z",
            code=404,
            msg="Not Found",
            hdrs=MagicMock(get=lambda k, d="": d),
            fp=BytesIO(b""),
        )
        connector._client.retrieve_instance = MagicMock(side_effect=http_error)
        connector._client.retrieve_instance_frames = MagicMock(return_value=b"\xff\xd8\xff\xe0JFIF")

        records_iter, _ = connector.read_table(
            "instances",
            {},
            {"fetch_dicom_files": "true", "dicom_volume_path": str(tmp_path), "wado_mode": "auto"},
        )
        records = list(records_iter)
        assert len(records) == 1
        assert records[0]["dicom_file_path"].endswith(".jpg")
        # Connector should have cached the detected mode
        assert connector._wado_mode_detected == "frames"

    def test_read_table_instances_fetch_metadata(
        self, dicomweb_options, studies_response, series_response, instances_response
    ):
        """fetch_metadata=true fetches WADO-RS series metadata and populates the metadata column."""
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_studies = MagicMock(side_effect=[studies_response[:1], []])
        connector._client.query_series_for_study = MagicMock(return_value=series_response[:1])
        connector._client.query_instances_for_series = MagicMock(return_value=instances_response)

        # Build metadata response keyed by sop_instance_uid from instances fixture
        sop_uid = instances_response[0]["00080018"]["Value"][0]
        meta_obj = {"00080018": {"vr": "UI", "Value": [sop_uid]}, "00080060": {"vr": "CS", "Value": ["CT"]}}
        connector._client.retrieve_series_metadata = MagicMock(return_value=[meta_obj])

        records_iter, _ = connector.read_table("instances", {}, {"fetch_metadata": "true"})
        records = list(records_iter)
        # First record should have metadata populated (matches sop_uid from fixture)
        assert records[0]["metadata"] is not None
        import json as _json

        # metadata is always a JSON string (StringType column)
        meta = records[0]["metadata"]
        assert isinstance(meta, str)
        parsed = _json.loads(meta)
        assert "00080018" in parsed

    def test_build_metadata_map_returns_json_string(self, dicomweb_options):
        """_build_metadata_map returns plain JSON strings.

        The schema declares metadata as VariantType; parse_value() in utils.py
        converts each JSON string to VariantVal before the row is yielded to Spark.
        """
        import json as _json

        connector = DICOMwebLakeflowConnect(dicomweb_options)
        meta_obj = {
            "00080018": {"vr": "UI", "Value": ["1.2.840.10008.1.2.3"]},
            "00080020": {"vr": "DA", "Value": ["20231215"]},
        }
        connector._client.retrieve_series_metadata = MagicMock(return_value=[meta_obj])

        result = connector._build_metadata_map("study-uid", "series-uid")
        assert len(result) == 1
        value = next(iter(result.values()))

        # Must always be a plain JSON string
        assert isinstance(value, str), (
            f"Expected str, got {type(value).__name__}. "
            "A non-string type would crash Spark's StringType column converter."
        )
        parsed = _json.loads(value)
        assert "00080018" in parsed

    def test_connection_name_explicit(self, studies_response):
        """Explicit connection_name option overrides the base_url default."""
        opts = {
            "base_url": "https://dicomweb.example.com",
            "auth_type": "none",
            "connection_name": "my-pacs-prod",
        }
        connector = DICOMwebLakeflowConnect(opts)
        connector._client.query_studies = MagicMock(side_effect=[studies_response, []])
        records_iter, _ = connector.read_table("studies", {}, {})
        records = list(records_iter)
        assert all(r["connection_name"] == "my-pacs-prod" for r in records)

    def test_read_table_pagination(self, dicomweb_options):
        page1 = [{"0020000D": {"vr": "UI", "Value": [f"1.2.{i}"]}} for i in range(2)]
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        # Simulate: page1 (full, size=2), page2 (full, size=2), page3 (empty)
        connector._client.query_studies = MagicMock(side_effect=[page1, page1, []])
        records_iter, _ = connector.read_table("studies", {}, {"page_size": "2"})
        records = list(records_iter)
        assert len(records) == 4  # 2 + 2 pages

    def test_read_table_unknown_table_raises(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        with pytest.raises(ValueError, match="Unknown table"):
            connector.read_table("patients", {}, {})

    def test_read_table_with_start_offset(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        connector._client.query_studies = MagicMock(return_value=[])
        records_iter, next_offset = connector.read_table(
            "studies",
            {"study_date": "20231201", "page_offset": 50},
            {},
        )
        list(records_iter)
        call_args = connector._client.query_studies.call_args
        assert call_args.kwargs.get("offset") == 50 or call_args.args[2] == 50


# ---------------------------------------------------------------------------
# Diagnostics table tests
# ---------------------------------------------------------------------------


class TestDiagnostics:
    def test_get_schema_diagnostics(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        schema = connector.get_table_schema("diagnostics", {})
        assert schema == DIAGNOSTICS_SCHEMA
        field_names = [f.name for f in schema.fields]
        assert "endpoint" in field_names
        assert "supported" in field_names
        assert "status_code" in field_names
        assert "latency_ms" in field_names
        assert "probe_timestamp" in field_names

    def test_read_table_metadata_diagnostics(self, dicomweb_options):
        connector = DICOMwebLakeflowConnect(dicomweb_options)
        meta = connector.read_table_metadata("diagnostics", {})
        assert meta["primary_keys"] == ["endpoint"]
        assert meta["cursor_field"] == "probe_timestamp"
        assert meta["ingestion_type"] == "cdc"

    def test_read_table_diagnostics_yields_probe_records(self, dicomweb_options):
        """Diagnostics read_table probes endpoints and yields one record per endpoint."""
        connector = DICOMwebLakeflowConnect(dicomweb_options)

        # Stub the probe_endpoint to return a predictable result
        connector._client.probe_endpoint = MagicMock(
            return_value={
                "status_code": 200,
                "content_type": "application/dicom+json",
                "latency_ms": 42,
                "error": None,
            }
        )
        # Stub query_studies so UID discovery succeeds
        connector._client.query_studies = MagicMock(
            return_value=[
                {
                    "0020000D": {"vr": "UI", "Value": ["1.2.3.4.5"]},
                    "00080020": {"vr": "DA", "Value": ["20231215"]},
                }
            ]
        )
        connector._client.query_series_for_study = MagicMock(
            return_value=[
                {
                    "0020000E": {"vr": "UI", "Value": ["1.2.3.4.5.6"]},
                }
            ]
        )
        connector._client.query_instances_for_series = MagicMock(
            return_value=[
                {
                    "00080018": {"vr": "UI", "Value": ["1.2.3.4.5.6.7"]},
                }
            ]
        )

        records_iter, next_offset = connector.read_table("diagnostics", {}, {})
        records = list(records_iter)

        # Should have at least one record per probed endpoint
        assert len(records) > 0
        # Every record has the required fields
        for rec in records:
            assert "endpoint" in rec
            assert "supported" in rec
            assert "probe_timestamp" in rec
            assert rec["supported"] in ("yes", "no", "unknown", "error", "partial")
            assert rec["connection_name"] == dicomweb_options["base_url"]
        # next_offset contains probe_timestamp
        assert "probe_timestamp" in next_offset

    def test_read_table_diagnostics_marks_error_on_exception(self, dicomweb_options):
        """When probe_endpoint returns an error, the record shows supported=error."""
        connector = DICOMwebLakeflowConnect(dicomweb_options)

        connector._client.probe_endpoint = MagicMock(
            return_value={
                "status_code": None,
                "content_type": None,
                "latency_ms": 5000,
                "error": "Connection timed out",
            }
        )
        connector._client.query_studies = MagicMock(return_value=[])
        connector._client.query_series_for_study = MagicMock(return_value=[])
        connector._client.query_instances_for_series = MagicMock(return_value=[])

        records_iter, _ = connector.read_table("diagnostics", {}, {})
        records = list(records_iter)

        error_records = [r for r in records if r["supported"] == "error"]
        assert len(error_records) > 0
        assert "Connection timed out" in error_records[0]["notes"]


# ---------------------------------------------------------------------------
# Utility tests
# ---------------------------------------------------------------------------


class TestUtilities:
    def test_subtract_days_normal(self):
        assert _subtract_days("20231215", 5) == "20231210"

    def test_subtract_days_zero(self):
        assert _subtract_days("20231215", 0) == "20231215"

    def test_subtract_days_default_start(self):
        from databricks.labs.community_connector.sources.dicomweb.dicomweb import DEFAULT_START_DATE

        assert _subtract_days(DEFAULT_START_DATE, 10) == DEFAULT_START_DATE

    def test_subtract_days_cross_month(self):
        assert _subtract_days("20231201", 3) == "20231128"

    def test_subtract_days_invalid_date(self):
        # Should return input unchanged on invalid date
        result = _subtract_days("invalid", 5)
        assert result == "invalid"


import pathlib  # noqa: E402 (needed for the fetch_files test above)
