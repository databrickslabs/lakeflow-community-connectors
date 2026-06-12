"""Tests for IAM (Patient Adverse Reaction Information) segment extraction.

IAM is the v2.4+ replacement for AL1, with richer allergy/adverse-reaction
fields including reporting details and clinical status.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_iam
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestIAMExtraction:
    def test_comprehensive_iam(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "IAM", _extract_iam)
        assert row["allergen_type_code"] == "DA"
        assert row["allergen_code"] == "SULFA"
        assert row["allergen_code_text"] == "Sulfonamides"
        assert row["allergy_severity_code"] == "SV"
        assert row["reported_datetime"] is not None
        assert row["reported_by_id"] == "PRN001"


class TestIAMMissingFields:
    def test_minimal_iam(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "IAM|1"
        )
        row = _extract_iam(msg.get_segment("IAM"))
        assert row["set_id"] == 1
        assert row["allergen_type_code"] is None
        assert row["allergen_code"] is None
        assert row["allergy_severity_code"] is None
        assert row["reported_datetime"] is None
        assert row["reported_by_id"] is None
        assert row["allergy_clinical_status_code"] is None

    def test_iam_with_allergen_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "IAM|1|DA^Drug|LATEX^Latex allergy^L"
        )
        row = _extract_iam(msg.get_segment("IAM"))
        assert row["allergen_type_code"] == "DA"
        assert row["allergen_code"] == "LATEX"
        assert row["allergen_code_text"] == "Latex allergy"
        assert row["allergen_code_coding_system"] == "L"
