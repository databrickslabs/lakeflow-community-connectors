"""Tests for OBR (Observation Request) segment extraction.

OBR contains order/service information: service identifier, ordering
provider, result status. Multiple OBR segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_obr
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestOBRExtraction:
    def test_oru_obr(self):
        msg = parse_first(load_sample("sample_oru.hl7"))
        row = extract_segment(msg, "OBR", _extract_obr)
        assert row["service"] == "80048"
        assert row["service_text"] == "Basic Metabolic Panel"
        assert row["result_status"] == "F"

    def test_covid_obr_multiple(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        segs = segments_of_type(msg, "OBR")
        assert len(segs) == 2
        row1 = _extract_obr(segs[0])
        assert row1["service"] == "PERSUBJ"
        row2 = _extract_obr(segs[1])
        assert row2["service"] == "NOTF"

    def test_celr_obr(self):
        msg = parse_first(load_sample("sample_oru_lab_celr.hl7"))
        row = extract_segment(msg, "OBR", _extract_obr)
        assert row["service"] == "68991-9"
        assert row["diagnostic_service_section"] == "LAB"


class TestOBRMissingFields:
    def test_minimal_obr(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBR|1"
        )
        row = _extract_obr(msg.get_segment("OBR"))
        assert row["set_id"] == 1
        assert row["service"] is None
        assert row["service_text"] is None
        assert row["result_status"] is None
        assert row["ordering_provider_id"] is None
        assert row["diagnostic_service_section"] is None

    def test_obr_service_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBR|1||FIL001|CBC^Complete Blood Count^LN"
        )
        row = _extract_obr(msg.get_segment("OBR"))
        assert row["service"] == "CBC"
        assert row["service_text"] == "Complete Blood Count"
        assert row["service_coding_system"] == "LN"
        assert row["filler_order_number"] == "FIL001"
