"""Tests for PD1 (Patient Additional Demographic) segment extraction.

PD1 contains supplementary patient data: living will, organ donor status,
primary care facility. One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_pd1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestPD1Extraction:
    def test_comprehensive_pd1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "PD1", _extract_pd1)
        assert row["patient_primary_facility"][0]["name"] == "CHICAGO PRIMARY CARE"
        assert row["patient_primary_care_provider"]["id"] == "8877665"


class TestPD1MissingFields:
    def test_minimal_pd1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PD1"
        )
        row = _extract_pd1(msg.get_segment("PD1"))
        assert row["living_dependency"] is None
        assert row["living_arrangement"] is None
        assert row["patient_primary_facility"] is None
        assert row["patient_primary_care_provider"] is None
        assert row["student_indicator"] is None
        assert row["organ_donor_code"] is None
        assert row["living_will_code"] is None

    def test_pd1_with_partial_fields(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PD1|||MAIN CLINIC^^^^^123||||Y|Y"
        )
        row = _extract_pd1(msg.get_segment("PD1"))
        assert row["patient_primary_facility"][0]["name"] == "MAIN CLINIC"
        assert row["organ_donor_code"]["code"] == "Y"
