"""Tests for PID (Patient Identification) segment extraction.

PID contains core patient demographics: name, DOB, sex, MRN, address.
One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_pid
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestPIDExtraction:
    def test_adt_pid(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["patient_id"][0]["id"] == "MRN12345"
        assert row["patient_names"][0]["family_name"] == "Doe"
        assert row["patient_names"][0]["given_name"] == "John"
        assert row["date_of_birth"] is not None
        assert row["administrative_sex"]["code"] == "M"
        assert row["address"][0]["city"] == "Boston"
        assert row["address"][0]["state"] == "MA"

    def test_covid_pid_race(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        # PID-10 race is ArrayType<CWE> (0..* per spec); first rep's components in [0].
        assert row["race"][0]["code"] == "2028-9"
        assert row["race"][0]["text"] == "Asian"
        assert row["administrative_sex"]["code"] == "F"

    def test_gc_pid_ethnicity(self):
        msg = parse_first(load_sample("sample_oru_gc_testing.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["race"][0]["code"] == "2076-8"
        assert row["administrative_sex"]["code"] == "M"
        # PID-22 ethnic_group is ArrayType<CWE> (0..* per spec).
        assert row["ethnic_group"][0]["code"] == "H"

    def test_comprehensive_pid_full_fields(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["patient_names"][0]["family_name"] == "Martinez"
        assert row["patient_names"][0]["given_name"] == "Sofia"
        assert row["marital_status"]["code"] == "M"
        assert row["address"][0]["zip"] == "60614"
        assert row["ssn"] == "987-65-4321"

    def test_lyme_pid_ethnicity(self):
        msg = parse_first(load_sample("sample_oru_lyme.hl7"))
        row = extract_segment(msg, "PID", _extract_pid)
        assert row["ethnic_group"][0]["code"] == "2135-2"
        assert row["race"][0]["code"] == "2054-5"


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
        # PID-10 race is now ArrayType<CWE>; absent field yields None.
        assert row["race"] is None
        assert row["address"] is None
        assert row["ssn"] is None

    def test_pid_with_only_mrn(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1||MRN999^^^HOSP^MR"
        )
        row = _extract_pid(msg.get_segment("PID"))
        assert row["patient_id"][0]["id"] == "MRN999"
        assert row["patient_id"][0]["assigning_authority"] == "HOSP"
        assert row["patient_id"][0]["type_code"] == "MR"
        assert row["patient_names"] is None


class TestPIDArrayPromotion:
    """PID-3 (patient identifier list, CX 1..*), PID-11 (address, XAD 0..*),
    PID-13/14 (phone, XTN 0..*), and PID-21 (mother's identifier, CX 0..*)
    are all spec-typed 0..*/1..* — must be captured as ARRAY<STRUCT<...>>
    preserving every ~-separated repetition.
    """

    def test_pid3_captures_all_patient_identifier_repetitions(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1||MRN1^^^HOSP^MR~SSN^^^USA^SS~ACCT99^^^HOSP^AN||Doe^John"
        )
        row = _extract_pid(msg.get_segment("PID"))
        ids = row["patient_id"]
        assert len(ids) == 3
        assert ids[0]["id"] == "MRN1"
        assert ids[0]["assigning_authority"] == "HOSP"
        assert ids[0]["type_code"] == "MR"
        assert ids[1]["id"] == "SSN"
        assert ids[1]["type_code"] == "SS"
        assert ids[2]["id"] == "ACCT99"
        assert ids[2]["type_code"] == "AN"

    def test_pid11_captures_multiple_addresses(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1||MRN^^^HOSP||Doe^John||19800101|M|||"
            "123 Main^^Boston^MA^02101^USA^H~456 Work^Suite 5^Boston^MA^02110^USA^B"
        )
        row = _extract_pid(msg.get_segment("PID"))
        addrs = row["address"]
        assert len(addrs) == 2
        assert addrs[0]["street"] == "123 Main"
        assert addrs[0]["city"] == "Boston"
        assert addrs[0]["type"] == "H"
        assert addrs[1]["street"] == "456 Work"
        assert addrs[1]["other_designation"] == "Suite 5"
        assert addrs[1]["type"] == "B"

    def test_pid13_captures_multiple_phones(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1||MRN||Doe^John||19800101|M|||||"
            "(617)555-1212^PRN^PH^^^617^5551212~(617)555-1111^ORN^CP^^^617^5551111"
        )
        row = _extract_pid(msg.get_segment("PID"))
        phones = row["home_phone"]
        assert len(phones) == 2
        assert phones[0]["number"] == "(617)555-1212"
        assert phones[0]["use_code"] == "PRN"
        assert phones[0]["equipment_type"] == "PH"
        assert phones[0]["area_code"] == "617"
        assert phones[1]["number"] == "(617)555-1111"
        assert phones[1]["equipment_type"] == "CP"


class TestPIDExternalId:
    """PID-2 (patient_external_id) and PID-4 (alternate_patient_id) are CX type.
    Both were previously stored as raw strings; they must now be decomposed."""

    def test_pid2_external_id_cx_struct(self):
        # CX: comp1=id, comp3=check_digit_scheme, comp4=assigning_authority(HD.1), comp5=type_code
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1|EXT001^^M11^EXT_SYS^PI|||Doe^John"
        )
        row = _extract_pid(msg.get_segment("PID"))
        assert row["patient_external_id"]["id"] == "EXT001"
        assert row["patient_external_id"]["check_digit_scheme"] == "M11"
        assert row["patient_external_id"]["assigning_authority"] == "EXT_SYS"
        assert row["patient_external_id"]["type_code"] == "PI"

    def test_pid4_alternate_id_cx_struct(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1|||ALT999^^M11^ALT_SYS^AN||Doe^John"
        )
        row = _extract_pid(msg.get_segment("PID"))
        assert row["alternate_patient_id"]["id"] == "ALT999"
        assert row["alternate_patient_id"]["assigning_authority"] == "ALT_SYS"
        assert row["alternate_patient_id"]["type_code"] == "AN"

    def test_pid2_absent_yields_none(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1"
        )
        row = _extract_pid(msg.get_segment("PID"))
        assert row["patient_external_id"] is None
        assert row["alternate_patient_id"] is None


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
