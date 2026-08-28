"""Tests for DG1 (Diagnosis) segment extraction.

DG1 contains diagnosis data: coding method, ICD code, text, type, priority.
Multiple DG1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_dg1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestDG1Extraction:
    def test_adt_dg1(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "DG1", _extract_dg1)
        assert row["diagnosis_coding_method"] == "ICD10"
        assert row["diagnosis_code"]["code"] == "J18.9"
        assert row["diagnosis_code"]["text"] == "Pneumonia unspecified"
        assert row["diagnosis_type"]["code"] == "A"

    def test_comprehensive_multiple_dg1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        segs = segments_of_type(msg, "DG1")
        assert len(segs) == 2
        row1 = _extract_dg1(segs[0])
        assert row1["diagnosis_code"]["code"] == "I21.0"
        assert row1["diagnosis_priority"] == 1
        row2 = _extract_dg1(segs[1])
        assert row2["diagnosis_code"]["code"] == "I50.9"
        assert row2["diagnosis_type"]["code"] == "W"
        assert row2["diagnosis_code"]["text"] == "Heart failure unspecified"


class TestDG1MissingFields:
    def test_minimal_dg1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "DG1|1"
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["set_id"] == 1
        assert row["diagnosis_coding_method"] is None
        assert row["diagnosis_code"] is None
        assert row["diagnosis_type"] is None
        assert row["diagnosis_priority"] is None

    def test_dg1_code_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "DG1|1|ICD10|E11.9^Type 2 diabetes^I10"
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["diagnosis_coding_method"] == "ICD10"
        assert row["diagnosis_code"]["code"] == "E11.9"
        assert row["diagnosis_code"]["text"] == "Type 2 diabetes"
        assert row["diagnosis_code"]["coding_system"] == "I10"


class TestDG1CweLosslessExtraction:
    """Pin the design: every CWE 0..1 field (DG1-6, DG1-10, DG1-17, DG1-25, DG1-26)
    is decomposed into all 9 components, not just the code, so senders that
    populate `text` / `coding_system` past the bare enumeration value (allowed by
    HL7 v2.x for any CWE-typed field) are captured losslessly.
    """

    def test_diagnosis_type_with_text_and_coding_system(self):
        # DG1-6 (diagnosis_type) is spec-typed CWE 1..1 even though Table 0052
        # only allows A/W/F as codes. Senders may still send display text and
        # coding system.
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "DG1|1|ICD10|J18.9|||A^Admitting^HL70052"
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["diagnosis_type"]["code"] == "A"
        assert row["diagnosis_type"]["text"] == "Admitting"
        assert row["diagnosis_type"]["coding_system"] == "HL70052"

    def test_diagnosis_type_bare_code(self):
        # Common case: sender sends just the code, all other CWE components NULL.
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "DG1|1|ICD10|J18.9|||W"
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["diagnosis_type"]["code"] == "W"
        assert row["diagnosis_type"]["text"] is None
        assert row["diagnosis_type"]["coding_system"] is None

    def test_diagnosing_clinician_xcn_array_lossless(self):
        """DG1-16 (diagnosingClinician) is XCN 0..* per HL7 v2.9 — must capture
        every ~-separated repetition with the full XCN structure, not just the
        first repetition's identifier.
        """
        fields = [""] * 17
        fields[1] = "1"
        fields[2] = "ICD10"
        fields[3] = "J18.9"
        fields[6] = "A"
        fields[16] = (
            "DOC001^Smith^Alice^M^^Dr^MD^^HOSP&urn:hospital&ISO^L^^^^^DOC"
            "~DOC002^Jones^Bob^^^Dr^MD^^HOSP&urn:hospital&ISO^L^^^^^DOC"
        )
        segment = "DG1|" + "|".join(fields[1:])
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r" + segment
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        clinicians = row["diagnosing_clinician"]
        assert len(clinicians) == 2
        assert clinicians[0]["id"] == "DOC001"
        assert clinicians[0]["family_name"] == "Smith"
        assert clinicians[0]["given_name"] == "Alice"
        assert clinicians[0]["middle_name"] == "M"
        assert clinicians[0]["prefix"] == "Dr"
        assert clinicians[0]["degree"] == "MD"
        assert clinicians[0]["assigning_authority"] == "HOSP"
        assert clinicians[0]["assigning_authority_universal_id"] == "urn:hospital"
        assert clinicians[0]["name_type_code"] == "L"
        assert clinicians[1]["id"] == "DOC002"
        assert clinicians[1]["family_name"] == "Jones"
        assert clinicians[1]["given_name"] == "Bob"

    def test_diagnosing_clinician_absent_yields_none(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "DG1|1|ICD10|J18.9|||A"
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["diagnosing_clinician"] is None

    def test_present_on_admission_indicator_with_full_cwe(self):
        # DG1-26 — same pattern. Y/N/U/W are the conventional codes, but the
        # field is CWE and senders are allowed to attach text / alt codes.
        # Build the segment programmatically to avoid pipe-counting errors.
        # fields[N] maps to DG1-N (DG1-0 is unused, DG1-1=set_id).
        fields = [""] * 27
        fields[1] = "1"
        fields[2] = "ICD10"
        fields[3] = "J18.9"
        fields[6] = "A"
        fields[26] = "Y^Yes^HL70136"
        segment = "DG1|" + "|".join(fields[1:])
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r" + segment
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["present_on_admission_indicator"]["code"] == "Y"
        assert row["present_on_admission_indicator"]["text"] == "Yes"
        assert row["present_on_admission_indicator"]["coding_system"] == "HL70136"
