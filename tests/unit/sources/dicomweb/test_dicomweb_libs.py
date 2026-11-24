"""
Pure unit tests for DICOMweb connector internals — no server required.

TestParser, TestSchemas, TestConnectorUnit, TestDiagnostics, TestUtilities
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from databricks.labs.community_connector.sources.dicomweb.dicomweb import (
    DEFAULT_START_DATE,
    DICOMwebLakeflowConnect,
    _parse_qido_filters,
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
        assert (
            record["series_instance_uid"] == "1.3.12.2.1107.5.2.32.35162.2013120811373024696203156"
        )
        assert record["modality"] == "CT"
        assert record["series_number"] == 1
        assert record["body_part_examined"] == "CHEST"

    def test_parse_instance(self, instances_response):
        record = parse_instance(instances_response[0])
        assert record["sop_instance_uid"] == "1.2.840.113619.2.5.1762583153.215519.978957063.78.1.1"
        assert record["instance_number"] == 1
        assert record["content_date"] == "20231215"
        assert record["dicom_file_path"] is None
        assert record["metadata"] is None

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
# Connector unit tests
# ---------------------------------------------------------------------------


class TestConnectorUnit:
    def test_missing_base_url_raises(self):
        with pytest.raises(ValueError, match="base_url"):
            DICOMwebLakeflowConnect({})

    def test_connection_name_defaults_to_base_url(self):
        connector = DICOMwebLakeflowConnect({"base_url": "https://dicomweb.example.com"})
        assert connector._connection_name == "https://dicomweb.example.com"

    def test_connection_name_explicit(self):
        connector = DICOMwebLakeflowConnect({
            "base_url": "https://dicomweb.example.com",
            "connection_name": "my-pacs-prod",
        })
        assert connector._connection_name == "my-pacs-prod"

    def test_build_metadata_map_returns_json_string(self):
        connector = DICOMwebLakeflowConnect({"base_url": "https://dicomweb.example.com"})
        meta_obj = {
            "00080018": {"vr": "UI", "Value": ["1.2.840.10008.1.2.3"]},
            "00080020": {"vr": "DA", "Value": ["20231215"]},
        }
        connector._client.retrieve_series_metadata = MagicMock(return_value=[meta_obj])

        result = connector._build_metadata_map("study-uid", "series-uid")
        assert len(result) == 1
        value = next(iter(result.values()))
        assert isinstance(value, str)
        parsed = json.loads(value)
        assert "00080018" in parsed


# ---------------------------------------------------------------------------
# QIDO-RS filter parsing & passthrough
# ---------------------------------------------------------------------------


class TestQidoFilterParsing:
    """_parse_qido_filters: JSON validation, type checking, and reserved-key rejection."""

    @pytest.mark.parametrize(
        "option_name",
        ["study_qido_filters", "series_qido_filters", "instance_qido_filters"],
    )
    def test_absent_or_empty_returns_empty_dict(self, option_name):
        assert _parse_qido_filters({}, option_name) == {}
        assert _parse_qido_filters({option_name: ""}, option_name) == {}
        assert _parse_qido_filters({option_name: "   "}, option_name) == {}

    def test_valid_json_dict_round_trips(self):
        result = _parse_qido_filters(
            {"study_qido_filters": '{"ModalitiesInStudy": "CT", "PatientID": "PID-1"}'},
            "study_qido_filters",
        )
        assert result == {"ModalitiesInStudy": "CT", "PatientID": "PID-1"}

    def test_list_value_preserved_for_multivalued_matching(self):
        result = _parse_qido_filters(
            {"series_qido_filters": '{"Modality": ["CT", "MR"]}'},
            "series_qido_filters",
        )
        assert result == {"Modality": ["CT", "MR"]}

    def test_malformed_json_raises_value_error(self):
        with pytest.raises(ValueError, match="malformed JSON"):
            _parse_qido_filters(
                {"study_qido_filters": '{"Modality": '},  # truncated
                "study_qido_filters",
            )

    def test_non_object_json_rejected(self):
        with pytest.raises(ValueError, match="must be a JSON object"):
            _parse_qido_filters(
                {"study_qido_filters": '["CT", "MR"]'},  # list, not dict
                "study_qido_filters",
            )
        with pytest.raises(ValueError, match="must be a JSON object"):
            _parse_qido_filters(
                {"study_qido_filters": '"CT"'},  # bare string
                "study_qido_filters",
            )

    @pytest.mark.parametrize(
        "raw, expected_match",
        [
            ('{"StudyDate": "20230101-20231231"}', "StudyDate"),
            ('{"studydate": "20230101"}', "studydate"),  # case-insensitive
            ('{"limit": 5}', "limit"),
            ('{"OFFSET": 100}', "OFFSET"),
        ],
    )
    def test_reserved_key_rejected(self, raw, expected_match):
        with pytest.raises(ValueError, match=expected_match):
            _parse_qido_filters({"study_qido_filters": raw}, "study_qido_filters")

    def test_unknown_option_name_is_programming_error(self):
        # Sanity guard: passing an unsupported option name surfaces as
        # AssertionError, not a silent miss.
        with pytest.raises(AssertionError):
            _parse_qido_filters({}, "patient_qido_filters")


class TestQidoFilterPassthrough:
    """Filters are forwarded to the DICOMwebClient query methods at every level."""

    @staticmethod
    def _connector_with_mocks() -> DICOMwebLakeflowConnect:
        connector = DICOMwebLakeflowConnect({"base_url": "https://dicomweb.example.com"})
        connector._client.query_studies = MagicMock(return_value=[])
        connector._client.query_series_for_study = MagicMock(return_value=[])
        connector._client.query_instances_for_series = MagicMock(return_value=[])
        return connector

    def test_studies_read_passes_study_filters(self):
        connector = self._connector_with_mocks()
        list(
            connector._read_studies_partition(
                {"date_range": "20230101-20231231"},
                {
                    "page_size": "200",
                    "study_qido_filters": '{"ModalitiesInStudy": "CT"}',
                },
            )
        )
        connector._client.query_studies.assert_called_once_with(
            "20230101-20231231",
            limit=200,
            offset=0,
            extra_filters={"ModalitiesInStudy": "CT"},
        )

    def test_series_partitioning_passes_study_filters(self):
        connector = self._connector_with_mocks()
        connector._partition_series(
            "20230101-20231231",
            {"study_qido_filters": '{"ModalitiesInStudy": "CT"}'},
        )
        # Driver enumeration of studies must carry the user's filter.
        _, kwargs = connector._client.query_studies.call_args
        assert kwargs["extra_filters"] == {"ModalitiesInStudy": "CT"}

    def test_series_read_passes_series_filters(self):
        connector = self._connector_with_mocks()
        list(
            connector._read_series_partition(
                {"studies": [{"uid": "1.2.3", "study_date": "20230101"}]},
                {"series_qido_filters": '{"Modality": "MR"}'},
            )
        )
        connector._client.query_series_for_study.assert_called_once_with(
            "1.2.3", extra_filters={"Modality": "MR"}
        )

    def test_instances_partitioning_passes_both_filters(self):
        connector = self._connector_with_mocks()
        # Driver enumerates one study with one series so we exercise both calls.
        connector._client.query_studies = MagicMock(
            return_value=[
                {
                    "0020000D": {"vr": "UI", "Value": ["1.2.3"]},
                    "00080020": {"vr": "DA", "Value": ["20230101"]},
                }
            ]
        )
        connector._client.query_series_for_study = MagicMock(
            return_value=[{"0020000E": {"vr": "UI", "Value": ["1.2.3.4"]}}]
        )

        connector._partition_instances(
            "20230101-20231231",
            {
                "study_qido_filters": '{"ModalitiesInStudy": "CT"}',
                "series_qido_filters": '{"Modality": "CT"}',
            },
        )
        _, study_kwargs = connector._client.query_studies.call_args
        _, series_kwargs = connector._client.query_series_for_study.call_args
        assert study_kwargs["extra_filters"] == {"ModalitiesInStudy": "CT"}
        assert series_kwargs["extra_filters"] == {"Modality": "CT"}

    def test_instances_read_passes_instance_filters(self):
        connector = self._connector_with_mocks()
        partition = {
            "series": [
                {"study_uid": "1.2.3", "series_uid": "1.2.3.4", "study_date": "20230101"}
            ]
        }
        list(
            connector._read_instances_partition(
                partition,
                {"instance_qido_filters": '{"includefield": "00080070"}'},
            )
        )
        connector._client.query_instances_for_series.assert_called_once_with(
            "1.2.3", "1.2.3.4", extra_filters={"includefield": "00080070"}
        )

    def test_no_filter_options_passes_empty_dict(self):
        """Backward compat: callers that omit qido_filters get extra_filters={}."""
        connector = self._connector_with_mocks()
        list(
            connector._read_studies_partition(
                {"date_range": "20230101-20231231"},
                {"page_size": "100"},
            )
        )
        _, kwargs = connector._client.query_studies.call_args
        assert kwargs["extra_filters"] == {}

    def test_invalid_filter_json_raises_at_read_time(self):
        connector = self._connector_with_mocks()
        with pytest.raises(ValueError, match="malformed JSON"):
            list(
                connector._read_studies_partition(
                    {"date_range": "20230101-20231231"},
                    {"study_qido_filters": "{not-json}"},
                )
            )


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

        connector._client.probe_endpoint = MagicMock(
            return_value={
                "status_code": 200,
                "content_type": "application/dicom+json",
                "latency_ms": 42,
                "error": None,
            }
        )
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

        assert len(records) > 0
        for rec in records:
            assert "endpoint" in rec
            assert "supported" in rec
            assert "probe_timestamp" in rec
            assert rec["supported"] in ("yes", "no", "unknown", "error", "partial")
            assert rec["connection_name"] == dicomweb_options["base_url"]
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
        assert _subtract_days(DEFAULT_START_DATE, 10) == DEFAULT_START_DATE

    def test_subtract_days_cross_month(self):
        assert _subtract_days("20231201", 3) == "20231128"

    def test_subtract_days_invalid_date(self):
        result = _subtract_days("invalid", 5)
        assert result == "invalid"
