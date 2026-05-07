"""Tests for GT1 (Guarantor) segment extraction.

GT1 contains guarantor data: name, address, SSN, employer, relationship
to patient. Multiple GT1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_gt1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestGT1Extraction:
    def test_comprehensive_gt1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "GT1", _extract_gt1)
        assert row["guarantor_names"][0]["family_name"] == "Martinez"
        assert row["guarantor_names"][0]["given_name"] == "Carlos"
        assert row["guarantor_administrative_sex"] == "M"
        assert row["guarantor_relationship"] == "SPO"

    def test_dft_multiple_gt1(self):
        msg = parse_first(load_sample("sample_dft_financial.hl7"))
        segs = segments_of_type(msg, "GT1")
        assert len(segs) == 2
        row1 = _extract_gt1(segs[0])
        assert row1["guarantor_names"][0]["family_name"] == "Wilson"
        assert row1["guarantor_ssn"] == "111-22-3333"
        # guarantor_employer_names is built via _xpn_array_fields too —
        # XPN.1 maps to "family_name" even when the source is an org name.
        assert row1["guarantor_employer_names"][0]["family_name"] == "TECH SOLUTIONS INC"
        row2 = _extract_gt1(segs[1])
        assert row2["guarantor_names"][0]["family_name"] == "Wilson"
        assert row2["guarantor_names"][0]["given_name"] == "Sarah"
        assert row2["guarantor_administrative_sex"] == "F"


class TestGT1MissingFields:
    def test_minimal_gt1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "GT1|1"
        )
        row = _extract_gt1(msg.get_segment("GT1"))
        assert row["set_id"] == 1
        assert row["guarantor_names"] is None
        assert row["guarantor_administrative_sex"] is None
        assert row["guarantor_ssn"] is None
        assert row["guarantor_relationship"] is None
        assert row["guarantor_employer_names"] is None

    def test_gt1_name_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "GT1|1||Jones^Robert"
        )
        row = _extract_gt1(msg.get_segment("GT1"))
        assert row["guarantor_names"][0]["family_name"] == "Jones"
        assert row["guarantor_names"][0]["given_name"] == "Robert"
