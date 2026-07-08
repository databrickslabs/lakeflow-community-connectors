"""Tests for NK1 (Next of Kin / Associated Parties) segment extraction.

NK1 contains emergency contact / family information. Multiple NK1 segments
can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import (
    _extract_nk1,
    _split_messages,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestNK1Extraction:
    def test_adt_nk1(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "NK1", _extract_nk1)
        assert row["names"][0]["family_name"] == "Doe"
        assert row["names"][0]["given_name"] == "Jane"
        assert row["relationship"]["code"] == "SPO"

    def test_comprehensive_multiple_nk1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        segs = segments_of_type(msg, "NK1")
        assert len(segs) == 2
        row1 = _extract_nk1(segs[0])
        assert row1["names"][0]["family_name"] == "Martinez"
        assert row1["names"][0]["given_name"] == "Carlos"
        assert row1["relationship"]["code"] == "SPO"
        row2 = _extract_nk1(segs[1])
        assert row2["names"][0]["family_name"] == "Martinez"
        assert row2["relationship"]["code"] == "MTH"

    def test_batch_nk1(self):
        raw = load_sample("sample_batch_mixed.hl7")
        msgs = _split_messages(raw)
        msg3 = parse_message(msgs[2])
        row = extract_segment(msg3, "NK1", _extract_nk1)
        assert row["names"][0]["family_name"] == "Batch"
        assert row["relationship"]["code"] == "SPO"


class TestNK1MissingFields:
    def test_minimal_nk1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "NK1|1"
        )
        row = _extract_nk1(msg.get_segment("NK1"))
        assert row["set_id"] == 1
        assert row["names"] is None
        assert row["relationship"] is None
        assert row["phone_number"] is None
        assert row["administrative_sex"] is None

    def test_nk1_name_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "NK1|1|Smith^Jane"
        )
        row = _extract_nk1(msg.get_segment("NK1"))
        assert row["names"][0]["family_name"] == "Smith"
        assert row["names"][0]["given_name"] == "Jane"
        assert row["relationship"] is None


class TestNK1NewComposites:
    """NK1-11 job_code is JCC = CWE (job code) + CWE (job class) + TX (description)."""

    def test_nk1_jcc_field(self):
        fields = {1: "1", 2: "Smith^John", 11: "ENG&Engineer&L^IT&Information Tech&L^Senior software dev"}
        seg_fields = [fields.get(i, "") for i in range(1, 12)]
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "NK1|" + "|".join(seg_fields)
        )
        row = _extract_nk1(msg.get_segment("NK1"))
        assert row["job_code"]["code"] == "ENG"
        assert row["job_code"]["text"] == "Engineer"
        assert row["job_code"]["class"] == "IT"
        assert row["job_code"]["class_text"] == "Information Tech"
        assert row["job_code"]["description"] == "Senior software dev"
