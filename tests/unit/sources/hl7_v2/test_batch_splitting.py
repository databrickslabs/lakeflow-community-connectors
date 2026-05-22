"""Tests for HL7 batch message splitting.

Validates _split_messages: single messages, FHS/BHS/BTS/FTS envelope
stripping, multi-message batches.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import load_sample

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _split_messages
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestBatchSplitting:
    def test_split_single_message(self):
        raw = load_sample("sample_adt.hl7")
        msgs = _split_messages(raw)
        assert len(msgs) == 1

    def test_split_batch_with_envelope(self):
        raw = load_sample("sample_batch_mixed.hl7")
        msgs = _split_messages(raw)
        assert len(msgs) == 3

    def test_batch_strips_fhs_bhs_bts_fts(self):
        raw = load_sample("sample_batch_mixed.hl7")
        msgs = _split_messages(raw)
        for m in msgs:
            for line in m.split("\r"):
                seg = line[:3].upper()
                assert seg not in {"FHS", "BHS", "BTS", "FTS"}

    def test_each_batch_message_starts_with_msh(self):
        raw = load_sample("sample_batch_mixed.hl7")
        msgs = _split_messages(raw)
        for m in msgs:
            assert m.startswith("MSH")

    def test_each_batch_message_parses(self):
        raw = load_sample("sample_batch_mixed.hl7")
        msgs = _split_messages(raw)
        for i, m in enumerate(msgs):
            msg = parse_message(m)
            assert msg is not None, f"Batch message {i} failed to parse"
            assert len(msg.segments) >= 2


class TestBatchSplittingEdgeCases:
    def test_empty_string(self):
        assert _split_messages("") == []

    def test_whitespace_only(self):
        assert _split_messages("   \n\t  ") == []

    def test_no_msh(self):
        msgs = _split_messages("PID|1||MRN\rPV1|1|I")
        for m in msgs:
            parsed = parse_message(m)
            assert parsed is None or parsed.get_segment("MSH") is None

    def test_two_messages_no_envelope(self):
        raw = (
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PID|1||MRN001\r"
            "MSH|^~\\&|A|B|C|D|20240102||ADT^A01|2|P|2.5\r"
            "PID|1||MRN002"
        )
        msgs = _split_messages(raw)
        assert len(msgs) == 2
        assert "MRN001" in msgs[0]
        assert "MRN002" in msgs[1]

    def test_envelope_only_no_messages(self):
        raw = "FHS|^~\\&|A|B\rBHS|^~\\&|A|B\rBTS|0\rFTS|0"
        msgs = _split_messages(raw)
        assert len(msgs) == 0
