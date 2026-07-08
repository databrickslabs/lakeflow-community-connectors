"""Tests for NTE (Notes and Comments) segment extraction.

NTE contains free-text notes: set ID, source of comment, comment text,
comment type. Multiple NTE segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_nte
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestNTEExtraction:
    def test_covid_nte(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        segs = segments_of_type(msg, "NTE")
        assert len(segs) >= 4
        row = _extract_nte(segs[0])
        assert row["set_id"] == 1
        assert row["source_of_comment"] == "text"
        assert any("Note pid" in c for c in row["comment"])

    def test_concat_notes_nte(self):
        msg = parse_first(load_sample("sample_oru_concat_notes.hl7"))
        segs = segments_of_type(msg, "NTE")
        assert len(segs) >= 5
        row1 = _extract_nte(segs[0])
        assert any("memo 1 for PID" in c for c in row1["comment"])
        obr_note = _extract_nte(segs[2])
        assert any("first OBR note" in c for c in obr_note["comment"])

    def test_celr_nte_long_text(self):
        msg = parse_first(load_sample("sample_oru_lab_celr.hl7"))
        segs = segments_of_type(msg, "NTE")
        assert len(segs) >= 1
        row = _extract_nte(segs[0])
        assert any("SARS-CoV-2" in c for c in row["comment"])

    def test_flu_ar_nte_comment_type(self):
        msg = parse_first(load_sample("sample_oru_flu_ar.hl7"))
        row = extract_segment(msg, "NTE", _extract_nte)
        assert row["source_of_comment"] == "L"
        assert row["comment_type"]["code"] == "RE"


class TestNTEMissingFields:
    def test_minimal_nte(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "NTE|1"
        )
        row = _extract_nte(msg.get_segment("NTE"))
        assert row["set_id"] == 1
        assert row["source_of_comment"] is None
        assert row["comment"] is None
        assert row["comment_type"] is None

    def test_nte_comment_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "NTE|1|L|This is a lab note"
        )
        row = _extract_nte(msg.get_segment("NTE"))
        assert row["source_of_comment"] == "L"
        assert row["comment"] == ["This is a lab note"]
        assert row["comment_type"] is None
