"""Tests for EVN (Event Type) segment extraction.

EVN contains trigger event metadata — event type code, timestamps, operator.
One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_evn
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestEVNExtraction:
    def test_adt_evn(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "EVN", _extract_evn)
        assert row["recorded_datetime"] is not None
        assert row["operator"][0]["id"] == "OP001"

    def test_comprehensive_adt_evn(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "EVN", _extract_evn)
        assert row["event_type_code"] == "A01"
        assert row["operator"][0]["id"] == "ADM001"

    def test_dft_evn(self):
        msg = parse_first(load_sample("sample_dft_financial.hl7"))
        row = extract_segment(msg, "EVN", _extract_evn)
        assert row["event_type_code"] == "P03"


class TestEVNMissingFields:
    def test_minimal_evn(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "EVN||20240101120000"
        )
        row = _extract_evn(msg.get_segment("EVN"))
        assert row["event_type_code"] is None
        assert row["recorded_datetime"] is not None
        assert row["operator"] is None
        assert row["date_time_planned_event"] is None
        assert row["event_reason"] is None
        assert row["event_occurred"] is None
        assert row["event_facility"] is None
