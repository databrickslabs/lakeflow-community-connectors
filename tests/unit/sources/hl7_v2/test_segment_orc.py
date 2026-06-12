"""Tests for ORC (Common Order) segment extraction.

ORC contains order control data: order control code, placer/filler order
numbers, order status, ordering provider/facility. Multiple ORC segments
can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import (
    _extract_orc,
    _split_messages,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestORCExtraction:
    def test_celr_orc(self):
        msg = parse_first(load_sample("sample_oru_lab_celr.hl7"))
        row = extract_segment(msg, "ORC", _extract_orc)
        assert row["order_control"] == "RE"
        assert row["filler_order_number"]["entity_identifier"] == "N20V000178-01"
        assert row["order_status"] == "CM"

    def test_gc_orc_with_placer(self):
        msg = parse_first(load_sample("sample_oru_gc_testing.hl7"))
        row = extract_segment(msg, "ORC", _extract_orc)
        assert row["order_control"] == "RE"
        assert row["placer_order_number"]["entity_identifier"] == "201912"
        assert row["filler_order_number"]["entity_identifier"] == "21MP000052"
        # ORC-21 is XON repeatable per spec; flattened to an array of structs.
        assert row["ordering_facility_name"][0]["name"] == "San Francisco Public Health Lab"

    def test_batch_orc(self):
        raw = load_sample("sample_batch_mixed.hl7")
        msgs = _split_messages(raw)
        msg2 = parse_message(msgs[1])
        row = extract_segment(msg2, "ORC", _extract_orc)
        assert row["order_control"] == "RE"


class TestORCMissingFields:
    def test_minimal_orc(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "ORC|NW"
        )
        row = _extract_orc(msg.get_segment("ORC"))
        assert row["order_control"] == "NW"
        assert row["placer_order_number"] is None
        assert row["filler_order_number"] is None
        assert row["order_status"] is None
        # ORC-12 is XCN repeatable per spec; flattened to an array of structs (None when empty).
        assert row["ordering_provider"] is None
        assert row["ordering_facility_name"] is None

    def test_orc_new_order(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORM^O01|1|P|2.5\r"
            "ORC|NW|PLC001^PLACER|FIL001^FILLER||SC"
        )
        row = _extract_orc(msg.get_segment("ORC"))
        assert row["order_control"] == "NW"
        assert row["placer_order_number"]["entity_identifier"] == "PLC001"
        assert row["filler_order_number"]["entity_identifier"] == "FIL001"
        assert row["order_status"] == "SC"
