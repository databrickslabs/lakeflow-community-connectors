"""Tests for AL1 (Patient Allergy Information) segment extraction.

AL1 contains allergy data: allergen type, code, severity, reaction.
Multiple AL1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_al1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestAL1Extraction:
    def test_adt_al1(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "AL1", _extract_al1)
        assert row["allergen_type_code"] == "DA"
        assert row["allergen_code"] == "PENICILLIN"
        # AL1-5 is now ArrayType<CWE-shape struct>; first repetition's code lands in [0].code.
        assert row["allergy_reaction"][0]["code"] == "HIVES"

    def test_comprehensive_multiple_al1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        segs = segments_of_type(msg, "AL1")
        assert len(segs) == 2
        row1 = _extract_al1(segs[0])
        assert row1["allergen_code"] == "ASPIRIN"
        assert row1["allergy_severity_code"] == "SV"
        assert row1["allergy_reaction"][0]["code"] == "ANAPHYLAXIS"
        row2 = _extract_al1(segs[1])
        assert row2["allergen_type_code"] == "FA"
        assert row2["allergen_code"] == "SHELLFISH"


class TestAL1MissingFields:
    def test_minimal_al1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "AL1|1"
        )
        row = _extract_al1(msg.get_segment("AL1"))
        assert row["set_id"] == 1
        assert row["allergen_type_code"] is None
        assert row["allergen_code"] is None
        assert row["allergy_severity_code"] is None
        assert row["allergy_reaction"] is None
        assert row["identification_date"] is None

    def test_al1_type_and_code_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "AL1|1|DA^Drug Allergy|CODEINE^Codeine"
        )
        row = _extract_al1(msg.get_segment("AL1"))
        assert row["allergen_type_code"] == "DA"
        assert row["allergen_code"] == "CODEINE"
        assert row["allergen_code_text"] == "Codeine"


class TestAL1ReactionLenientCwe:
    """AL1-5 is spec-typed as ST 0..* in HL7 v2.9, but real EHRs send CWE-shape
    values here. Modeled as ArrayType<CWE-shape struct> so we (a) never lose
    repetitions and (b) leniently parse CWE components when present, while still
    storing plain ST values in [0].code.
    """

    def test_al1_cwe_shape_single_repetition(self):
        """Spec ST, but sender emits CWE components. All 9 components captured."""
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "AL1|1|DA^Drug allergy^HL70127^^^^^^|"
            "PEN^Penicillin^RXNORM^^^^^^|SV^Severity^HL70128^^^^^^|"
            "HIV^Hives^HL70129^^^^^^"
        )
        row = _extract_al1(msg.get_segment("AL1"))
        assert len(row["allergy_reaction"]) == 1
        assert row["allergy_reaction"][0]["code"] == "HIV"
        assert row["allergy_reaction"][0]["text"] == "Hives"
        assert row["allergy_reaction"][0]["coding_system"] == "HL70129"

    def test_al1_multiple_repetitions_preserved(self):
        """Regression: prior to the array refactor, reps 2..N were silently dropped."""
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "AL1|1|DA|PEN|SV|HIV^Hives^HL70129~RSH^Rash^HL70129~ITC^Itching^HL70129"
        )
        row = _extract_al1(msg.get_segment("AL1"))
        assert len(row["allergy_reaction"]) == 3
        assert [r["code"] for r in row["allergy_reaction"]] == ["HIV", "RSH", "ITC"]
        assert [r["text"] for r in row["allergy_reaction"]] == ["Hives", "Rash", "Itching"]

    def test_al1_pure_st_value_lands_in_code(self):
        """Spec ST sender (no components). Value lands in [0].code; other fields NULL."""
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "AL1|1|DA|PEN|SV|HIVES"
        )
        row = _extract_al1(msg.get_segment("AL1"))
        assert row["allergy_reaction"] == [{
            "code": "HIVES",
            "text": None,
            "coding_system": None,
            "alt_code": None,
            "alt_text": None,
            "alt_coding_system": None,
            "coding_system_version": None,
            "alt_coding_system_version": None,
            "original_text": None,
        }]
