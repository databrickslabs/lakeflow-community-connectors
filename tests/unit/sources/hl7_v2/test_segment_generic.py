"""Tests for generic / Z-segment fallback extraction.

The generic extractor handles unknown segment types (Z-segments and others
not in the typed extractor registry). It produces segment_type + field_1
through field_25.
"""
from __future__ import annotations

import textwrap

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_generic
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestGenericExtraction:
    def test_z_segment_via_inline(self):
        raw = textwrap.dedent("""\
            MSH|^~\\&|SYS|HOSP|EHR|EHR|20240101120000||ADT^A01|MSG001|P|2.5.1
            PID|1||MRN001^^^HOSP^MR||Test^Patient||19900101|M
            ZPI|1|CUSTOM_FIELD_A|CUSTOM_FIELD_B|VALUE_C
        """)
        msg = parse_first(raw)
        segs = segments_of_type(msg, "ZPI")
        assert len(segs) == 1
        row = _extract_generic(segs[0])
        assert row["segment_type"] == "ZPI"
        assert row["field_1"] == "1"
        assert row["field_2"] == "CUSTOM_FIELD_A"
        assert row["field_3"] == "CUSTOM_FIELD_B"

    def test_generic_produces_25_field_keys(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "ZXX|A|B|C"
        )
        row = _extract_generic(msg.get_segment("ZXX"))
        assert row["segment_type"] == "ZXX"
        for i in range(1, 26):
            assert f"field_{i}" in row

    def test_generic_empty_segment(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "ZZZ"
        )
        row = _extract_generic(msg.get_segment("ZZZ"))
        assert row["segment_type"] == "ZZZ"
        assert row["field_1"] is None


class TestGenericMissingFields:
    def test_all_fields_none_for_empty_z_segment(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "ZAA"
        )
        row = _extract_generic(msg.get_segment("ZAA"))
        for i in range(1, 26):
            assert row[f"field_{i}"] is None
