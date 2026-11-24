"""Tests for PV2 (Patient Visit — Additional Info) segment extraction.

PV2 has extended visit data: expected admit/discharge, visit description,
estimated LOS. One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_pv2
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestPV2Extraction:
    def test_comprehensive_pv2(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "PV2", _extract_pv2)
        assert row["expected_admit_datetime"] is not None
        assert row["expected_discharge_datetime"] is not None
        assert row["visit_description"] == "Medical assessment required"


class TestPV2MissingFields:
    def test_minimal_pv2(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PV2"
        )
        row = _extract_pv2(msg.get_segment("PV2"))
        assert row["prior_pending_location"] is None
        assert row["accommodation_code"] is None
        assert row["expected_admit_datetime"] is None
        assert row["expected_discharge_datetime"] is None
        assert row["visit_description"] is None
        assert row["estimated_length_of_inpatient_stay"] is None
        assert row["patient_valuables"] is None

    def test_pv2_patient_valuables_array(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PV2|||||wallet~watch~keys"
        )
        row = _extract_pv2(msg.get_segment("PV2"))
        assert row["patient_valuables"] == ["wallet", "watch", "keys"]

    def test_pv2_partial_fields(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PV2|||CHF^Congestive Heart Failure|||||||||Cardiac evaluation"
        )
        row = _extract_pv2(msg.get_segment("PV2"))
        assert row["admit_reason"]["code"] == "CHF"
        assert row["visit_description"] == "Cardiac evaluation"
