"""Tests for OBX (Observation/Result) segment extraction.

OBX contains individual observation results: value type, observation ID,
value, units, reference range, interpretation. Multiple OBX segments per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_obx
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestOBXExtraction:
    def test_oru_obx_numeric(self):
        msg = parse_first(load_sample("sample_oru.hl7"))
        segs = segments_of_type(msg, "OBX")
        assert len(segs) == 5
        na = _extract_obx(segs[0])
        assert na["observation_id"] == "2951-2"
        assert na["observation_id_text"] == "Sodium"
        assert na["value_type"] == "NM"
        assert na["observation_value"] == "138"
        assert na["units"] == "mEq/L"
        assert na["references_range"] == "136-145"
        assert na["interpretation_codes"][0]["code"] == "N"

    def test_covid_obx_coded(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        segs = segments_of_type(msg, "OBX")
        row = _extract_obx(segs[0])
        assert row["value_type"] == "CWE"
        assert "U" in row["observation_value"]

    def test_gc_obx_structured_numeric(self):
        msg = parse_first(load_sample("sample_oru_gc_testing.hl7"))
        segs = segments_of_type(msg, "OBX")
        sn_obx = [s for s in segs if _extract_obx(s)["value_type"] == "SN"]
        assert len(sn_obx) >= 1

    def test_lyme_obx_string(self):
        msg = parse_first(load_sample("sample_oru_lyme.hl7"))
        segs = segments_of_type(msg, "OBX")
        st_obx = [s for s in segs if _extract_obx(s)["value_type"] == "ST"]
        assert len(st_obx) >= 1
        row = _extract_obx(st_obx[0])
        assert row["observation_value"] == "Botswanan"

    def test_flu_ar_obx_performing_org(self):
        msg = parse_first(load_sample("sample_oru_flu_ar.hl7"))
        segs = segments_of_type(msg, "OBX")
        row = _extract_obx(segs[0])
        assert row["performing_organization"] == "MERCY HOSPITAL ROGERS LAB"


class TestOBXMissingFields:
    def test_minimal_obx(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBX|1"
        )
        row = _extract_obx(msg.get_segment("OBX"))
        assert row["set_id"] == 1
        assert row["value_type"] is None
        assert row["observation_id"] is None
        assert row["observation_value"] is None
        assert row["units"] is None
        assert row["references_range"] is None
        assert row["interpretation_codes"] is None
        assert row["performing_organization"] is None

    def test_obx_no_units_no_range(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBX|1|ST|1234-5^Test||Result text"
        )
        row = _extract_obx(msg.get_segment("OBX"))
        assert row["value_type"] == "ST"
        assert row["observation_id"] == "1234-5"
        assert row["observation_value"] == "Result text"
        assert row["units"] is None
        assert row["references_range"] is None


class TestOBXEdgeCases:
    def test_obx_coded_value_raw(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBX|1|CWE|94500-6^COVID PCR^LN||260373001^Detected^SCT"
        )
        row = _extract_obx(msg.get_segment("OBX"))
        assert row["value_type"] == "CWE"
        assert row["observation_value"] == "260373001^Detected^SCT"

    def test_obx_multiple_value_types_in_message(self):
        msg = parse_first(load_sample("sample_oru.hl7"))
        segs = segments_of_type(msg, "OBX")
        types = {_extract_obx(s)["value_type"] for s in segs}
        assert "NM" in types
