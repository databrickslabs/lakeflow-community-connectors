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
        assert row["assigned_patient_location_point_of_care"] == "MED"
        assert row["assigned_patient_location_room"] == "101"
        assert row["attending_doctor"][0]["id"] == "DOC001"
        assert row["attending_doctor"][0]["family_name"] == "Smith"

    def test_comprehensive_pv1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "PV1", _extract_pv1)
        assert row["patient_class"] == "I"
        assert row["assigned_patient_location_point_of_care"] == "ICU"
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
        assert row["assigned_patient_location_point_of_care"] is None
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
        assert row["assigned_patient_location_point_of_care"] == "CLINIC"
        assert row["assigned_patient_location_room"] == "200"
        assert row["assigned_patient_location_bed"] == "A"


class TestPV1NewComposites:
    """PV1-20 financial_class (FC array), PV1-37 discharged_to_location (DLD)."""

    def test_pv1_fc_array_and_dld(self):
        # PV1-20 FC repeating; PV1-37 DLD = CWE + DTM. Fill PV1 minimally up
        # to field 37 by skipping irrelevant intervening fields with empty pipes.
        fields = {1: "1", 2: "I", 20: "PPO&&L^20240101~HMO&Health Maint&L^20240301",
                  37: "HOME&Home discharge&L^20240505"}
        # PV1 max field index supplied = 37
        seg_fields = [fields.get(i, "") for i in range(1, 38)]
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PV1|" + "|".join(seg_fields)
        )
        row = _extract_pv1(msg.get_segment("PV1"))
        assert row["financial_class"][0]["code"] == "PPO"
        assert row["financial_class"][0]["effective_date"] is not None
        assert row["financial_class"][1]["code"] == "HMO"
        assert row["financial_class"][1]["text"] == "Health Maint"
        assert row["discharged_to_location"] == "HOME"
        assert row["discharged_to_location_effective_date"] is not None
