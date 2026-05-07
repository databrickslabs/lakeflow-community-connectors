"""Tests for PID (Patient Identification) segment extraction.

PID contains core patient demographics: name, DOB, sex, MRN, address.
One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_pid
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestPIDExtraction:
    def test_adt_pid(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["patient_id"] == "MRN12345"
        assert row["patient_names"][0]["family_name"] == "Doe"
        assert row["patient_names"][0]["given_name"] == "John"
        assert row["date_of_birth"] is not None
        assert row["administrative_sex"] == "M"
        assert row["address_city"] == "Boston"
        assert row["address_state"] == "MA"

    def test_covid_pid_race(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["race"] == "2028-9"
        assert row["race_text"] == "Asian"
        assert row["administrative_sex"] == "F"

    def test_gc_pid_ethnicity(self):
        msg = parse_first(load_sample("sample_oru_gc_testing.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["race"] == "2076-8"
        assert row["administrative_sex"] == "M"
        assert row["ethnic_group"] == "H"

    def test_comprehensive_pid_full_fields(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["patient_names"][0]["family_name"] == "Martinez"
        assert row["patient_names"][0]["given_name"] == "Sofia"
        assert row["marital_status"] == "M"
        assert row["address_zip"] == "60614"
        assert row["ssn"] == "987-65-4321"

    def test_lyme_pid_ethnicity(self):
        msg = parse_first(load_sample("sample_oru_lyme.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["ethnic_group"] == "2135-2"
        assert row["race"] == "2054-5"


class TestPIDMissingFields:
    def test_minimal_pid(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1"
        )
        row = _extract_pid(msg.get_segment("PID"))
        assert row["set_id"] == 1
        assert row["patient_id"] is None
        assert row["patient_names"] is None
        assert row["date_of_birth"] is None
        assert row["administrative_sex"] is None
        assert row["race"] is None
        assert row["address_city"] is None
        assert row["ssn"] is None

    def test_pid_with_only_mrn(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1||MRN999^^^HOSP^MR"
        )
        row = _extract_pid(msg.get_segment("PID"))
        assert row["patient_id"] == "MRN999"
        assert row["patient_id_assigning_authority"] == "HOSP"
        assert row["patient_id_type_code"] == "MR"
        assert row["patient_names"] is None


class TestPIDEdgeCases:
    def test_pid_with_repetition_in_name(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1||MRN^^^HOSP||Smith^John~Jones^John||19800101|M"
        )
        row = _extract_pid(msg.get_segment("PID"))
        # Repeating XPN: both repetitions captured in the patient_names array.
        assert row["patient_names"][0]["family_name"] == "Smith"
        assert row["patient_names"][0]["given_name"] == "John"
        assert row["patient_names"][1]["family_name"] == "Jones"
        assert row["patient_names"][1]["given_name"] == "John"
