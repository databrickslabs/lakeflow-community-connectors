"""Tests for OBX (Observation/Result) segment extraction.

OBX contains individual observation results: value type, observation ID,
value, units, reference range, interpretation. Multiple OBX segments per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first, segments_of_type

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
        assert na["observation_id"]["code"] == "2951-2"
        assert na["observation_id"]["text"] == "Sodium"
        assert na["value_type"] == "NM"
        assert na["observation_value"] == ["138"]
        assert na["units"]["code"] == "mEq/L"
        assert na["references_range"] == "136-145"
        assert na["interpretation_codes"][0]["code"] == "N"

    def test_covid_obx_coded(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        segs = segments_of_type(msg, "OBX")
        row = _extract_obx(segs[0])
        assert row["value_type"] == "CWE"
        assert any("U" in v for v in row["observation_value"])

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
        assert row["observation_value"] == ["Botswanan"]

    def test_flu_ar_obx_performing_org(self):
        msg = parse_first(load_sample("sample_oru_flu_ar.hl7"))
        segs = segments_of_type(msg, "OBX")
        row = _extract_obx(segs[0])
        assert row["performing_organization"]["name"] == "MERCY HOSPITAL ROGERS LAB"


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
        assert row["observation_id"]["code"] == "1234-5"
        assert row["observation_value"] == ["Result text"]
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
        assert row["observation_value"] == ["260373001^Detected^SCT"]

    def test_obx_multiple_value_types_in_message(self):
        msg = parse_first(load_sample("sample_oru.hl7"))
        segs = segments_of_type(msg, "OBX")
        types = {_extract_obx(s)["value_type"] for s in segs}
        assert "NM" in types


class TestOBXEipArrayPromotion:
    """OBX-33 observation-related specimen is EIP 0..* — modeled as
    ARRAY<STRUCT<parent: EI, child: EI>>. Confirms both repetitions and
    sub-components within each EIP are captured losslessly.
    """

    def test_observation_related_specimen_eip_pair(self):
        # OBX-33 = parent_ei&child_ei, with sub-components separated by `&`.
        # Build via explicit field-count to avoid pipe-counting mistakes.
        # OBX-33 is field index 33.
        fields = [""] * 34
        fields[1] = "1"
        fields[2] = "ST"
        fields[3] = "TEST1"
        fields[5] = "value"
        fields[33] = (
            "SP001&LAB&urn:lab&ISO^CHILD001&LAB&urn:lab&ISO"
            "~SP002&LAB&urn:lab&ISO^CHILD002&LAB&urn:lab&ISO"
        )
        segment = "OBX|" + "|".join(fields[1:])
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r" + segment
        )
        row = _extract_obx(msg.get_segment("OBX"))
        eips = row["observation_related_specimen"]
        assert len(eips) == 2
        assert eips[0]["placer_assigned_identifier"]["entity_identifier"] == "SP001"
        assert eips[0]["placer_assigned_identifier"]["namespace_id"] == "LAB"
        assert eips[0]["placer_assigned_identifier"]["universal_id"] == "urn:lab"
        assert eips[0]["placer_assigned_identifier"]["universal_id_type"] == "ISO"
        assert eips[0]["filler_assigned_identifier"]["entity_identifier"] == "CHILD001"
        assert eips[1]["placer_assigned_identifier"]["entity_identifier"] == "SP002"
        assert eips[1]["filler_assigned_identifier"]["entity_identifier"] == "CHILD002"

    def test_observation_related_specimen_absent_yields_none(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBX|1|ST|TEST||value"
        )
        row = _extract_obx(msg.get_segment("OBX"))
        assert row["observation_related_specimen"] is None
