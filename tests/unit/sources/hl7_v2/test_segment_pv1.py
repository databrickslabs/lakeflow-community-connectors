"""Tests for PV1 (Patient Visit) segment extraction.

PV1 contains encounter details: patient class, location, attending/admitting
doctors, visit number, admit/discharge datetimes. One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_pv1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestPV1Extraction:
    def test_adt_pv1(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "PV1", _extract_pv1)
        assert row["patient_class"] == "I"
        assert row["location_point_of_care"] == "MED"
        assert row["location_room"] == "101"
        assert row["attending_doctor"][0]["id"] == "DOC001"
        assert row["attending_doctor"][0]["family_name"] == "Smith"

    def test_comprehensive_pv1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "PV1", _extract_pv1)
        assert row["patient_class"] == "I"
        assert row["location_point_of_care"] == "ICU"
        assert row["hospital_service"] == "CCU"
        assert row["visit_number"] == "VN20240301001"
        assert row["admit_datetime"] is not None


class TestPV1MissingFields:
    def test_minimal_pv1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PV1|1|I"
        )
        row = _extract_pv1(msg.get_segment("PV1"))
        assert row["set_id"] == 1
        assert row["patient_class"] == "I"
        assert row["location_point_of_care"] is None
        assert row["attending_doctor"] is None
        assert row["hospital_service"] is None
        assert row["admit_datetime"] is None
        assert row["discharge_datetime"] is None
        assert row["visit_number"] is None

    def test_pv1_outpatient(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PV1|1|O|CLINIC^200^A"
        )
        row = _extract_pv1(msg.get_segment("PV1"))
        assert row["patient_class"] == "O"
        assert row["location_point_of_care"] == "CLINIC"
        assert row["location_room"] == "200"
        assert row["location_bed"] == "A"
