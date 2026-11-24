"""Unit tests for hl7_v2_parser: parse_message, HL7Segment, HL7Message.

Tests the core parser independently of the segment extractors.
Covers encoding character detection, field/component/repetition access,
escape sequence decoding, line-ending normalisation, and edge cases.
"""
from __future__ import annotations

import textwrap

import pytest

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    HL7EncodingChars,
    HL7Message,
    HL7Segment,
    _decode_escape,
    parse_message,
)


# ── parse_message basics ────────────────────────────────────────────────────

class TestParseMessageBasics:
    def test_returns_none_for_empty_string(self):
        assert parse_message("") is None

    def test_returns_none_for_whitespace(self):
        assert parse_message("   \n\t  ") is None

    def test_returns_none_for_no_msh(self):
        assert parse_message("PID|1||MRN001") is None

    def test_returns_none_for_truncated_msh(self):
        assert parse_message("MSH") is None

    def test_minimal_msh_parses(self):
        msg = parse_message("MSH|^~\\&|SYS|HOSP|EHR|EHR|20240101||ADT^A01|1|P|2.5")
        assert msg is not None
        assert len(msg.segments) == 1
        assert msg.segments[0].segment_type == "MSH"

    def test_multi_segment_message(self):
        raw = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\rPID|1||MRN^^^HOSP^MR\rPV1|1|I"
        msg = parse_message(raw)
        assert msg is not None
        assert len(msg.segments) == 3
        types = [s.segment_type for s in msg.segments]
        assert types == ["MSH", "PID", "PV1"]

    def test_case_insensitive_segment_types(self):
        raw = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\rpid|1||MRN"
        msg = parse_message(raw)
        assert len(msg.segments) == 2
        assert msg.segments[1].segment_type == "PID"


# ── Line ending normalisation ───────────────────────────────────────────────

class TestLineEndingNormalisation:
    SIMPLE_MSG = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5{sep}PID|1||MRN"

    def test_cr_lf(self):
        msg = parse_message(self.SIMPLE_MSG.format(sep="\r\n"))
        assert len(msg.segments) == 2

    def test_cr(self):
        msg = parse_message(self.SIMPLE_MSG.format(sep="\r"))
        assert len(msg.segments) == 2

    def test_lf(self):
        msg = parse_message(self.SIMPLE_MSG.format(sep="\n"))
        assert len(msg.segments) == 2

    def test_mixed_endings(self):
        raw = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r\nPID|1||MRN\nPV1|1|I"
        msg = parse_message(raw)
        assert len(msg.segments) == 3


# ── Encoding character detection ────────────────────────────────────────────

class TestEncodingCharacters:
    def test_default_encoding(self):
        msg = parse_message("MSH|^~\\&|SYS|HOSP|EHR|EHR|20240101||ADT^A01|1|P|2.5")
        assert msg.enc.field_sep == "|"
        assert msg.enc.comp_sep == "^"
        assert msg.enc.rep_sep == "~"
        assert msg.enc.esc_char == "\\"
        assert msg.enc.sub_comp_sep == "&"

    def test_custom_encoding(self):
        raw = "MSH|#~\\&|SYS|HOSP|EHR|EHR|20240101||ADT#A01|1|P|2.5"
        msg = parse_message(raw)
        assert msg.enc.comp_sep == "#"

    def test_msh_field_1_is_separator(self):
        msg = parse_message("MSH|^~\\&|SYS|HOSP|EHR|EHR|20240101||ADT^A01|1|P|2.5")
        msh = msg.get_segment("MSH")
        assert msh.get_field(1) == "|"

    def test_msh_field_2_is_encoding_chars(self):
        msg = parse_message("MSH|^~\\&|SYS|HOSP|EHR|EHR|20240101||ADT^A01|1|P|2.5")
        msh = msg.get_segment("MSH")
        assert msh.get_field(2) == "^~\\&"


# ── HL7Segment field/component access ──────────────────────────────────────

class TestSegmentFieldAccess:
    @pytest.fixture()
    def pid_seg(self):
        raw = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\rPID|1||MRN001^^^HOSP^MR~MRN002^^^OTHER^MR||Doe^John^Q^^Jr|Smith|19800101|M"
        msg = parse_message(raw)
        return msg.get_segment("PID")

    def test_get_field_basic(self, pid_seg):
        assert pid_seg.get_field(1) == "1"

    def test_get_field_out_of_range_returns_default(self, pid_seg):
        assert pid_seg.get_field(99) == ""
        assert pid_seg.get_field(99, "N/A") == "N/A"

    def test_get_field_negative_returns_default(self, pid_seg):
        assert pid_seg.get_field(-1) == ""

    def test_get_component(self, pid_seg):
        assert pid_seg.get_component(5, 1) == "Doe"
        assert pid_seg.get_component(5, 2) == "John"
        assert pid_seg.get_component(5, 3) == "Q"

    def test_get_component_out_of_range(self, pid_seg):
        assert pid_seg.get_component(5, 99) == ""

    def test_get_repetition(self, pid_seg):
        assert pid_seg.get_repetition(3, 1) == "MRN001^^^HOSP^MR"
        assert pid_seg.get_repetition(3, 2) == "MRN002^^^OTHER^MR"

    def test_get_repetition_out_of_range(self, pid_seg):
        assert pid_seg.get_repetition(3, 5) == ""

    def test_get_rep_component(self, pid_seg):
        assert pid_seg.get_rep_component(3, 1, 1) == "MRN001"
        assert pid_seg.get_rep_component(3, 1, 4) == "HOSP"
        assert pid_seg.get_rep_component(3, 2, 1) == "MRN002"

    def test_num_fields(self, pid_seg):
        assert pid_seg.num_fields() >= 8

    def test_get_first_repetition_single_value(self, pid_seg):
        assert pid_seg.get_first_repetition(1) == "1"

    def test_empty_field_returns_default(self, pid_seg):
        assert pid_seg.get_field(2) == ""
        assert pid_seg.get_component(2, 1) == ""


# ── HL7Message helpers ──────────────────────────────────────────────────────

class TestHL7MessageHelpers:
    @pytest.fixture()
    def msg(self):
        raw = (
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "PID|1||MRN\r"
            "OBX|1|NM|2951-2^Sodium||138|mEq/L|136-145|N|||F\r"
            "OBX|2|NM|2823-3^Potassium||4.2|mEq/L|3.5-5.0|N|||F"
        )
        return parse_message(raw)

    def test_get_segment_found(self, msg):
        assert msg.get_segment("PID") is not None

    def test_get_segment_not_found(self, msg):
        assert msg.get_segment("NK1") is None

    def test_get_segments_multiple(self, msg):
        obx_list = msg.get_segments("OBX")
        assert len(obx_list) == 2

    def test_get_segments_case_insensitive(self, msg):
        assert len(msg.get_segments("obx")) == 2


# ── Escape sequence decoding ───────────────────────────────────────────────

class TestEscapeDecoding:
    ENC = HL7EncodingChars()

    def test_field_separator_escape(self):
        assert _decode_escape("A\\F\\B", self.ENC) == "A|B"

    def test_component_separator_escape(self):
        assert _decode_escape("A\\S\\B", self.ENC) == "A^B"

    def test_subcomponent_separator_escape(self):
        assert _decode_escape("A\\T\\B", self.ENC) == "A&B"

    def test_repetition_separator_escape(self):
        assert _decode_escape("A\\R\\B", self.ENC) == "A~B"

    def test_escape_character_escape(self):
        assert _decode_escape("A\\E\\B", self.ENC) == "A\\B"

    def test_highlighting_sequences_stripped(self):
        assert _decode_escape("\\H\\bold\\N\\", self.ENC) == "bold"

    def test_no_escape_passthrough(self):
        assert _decode_escape("plain text", self.ENC) == "plain text"

    def test_multiple_escapes(self):
        result = _decode_escape("A\\F\\B\\S\\C", self.ENC)
        assert result == "A|B^C"


# ── Edge cases / robustness ─────────────────────────────────────────────────

class TestParserEdgeCases:
    def test_blank_lines_ignored(self):
        raw = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r\r\rPID|1||MRN"
        msg = parse_message(raw)
        assert len(msg.segments) == 2

    def test_leading_trailing_whitespace_stripped(self):
        raw = "  \nMSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\rPID|1||MRN\n  "
        msg = parse_message(raw)
        assert len(msg.segments) == 2

    def test_indented_message_from_textwrap(self):
        raw = textwrap.dedent("""\
            MSH|^~\\&|SYS|HOSP|EHR|EHR|20240101120000||ADT^A01|MSG001|P|2.5.1
            PID|1||MRN001^^^HOSP^MR||Test^Patient||19900101|M
        """)
        msg = parse_message(raw)
        assert msg is not None
        assert len(msg.segments) == 2

    def test_raw_line_preserved(self):
        raw = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\rPID|1||MRN"
        msg = parse_message(raw)
        pid = msg.get_segment("PID")
        assert pid.raw_line == "PID|1||MRN"

    def test_msh_not_first_line(self):
        raw = "PID|1||MRN\rMSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5"
        msg = parse_message(raw)
        assert msg is not None
        assert len(msg.segments) == 2

    def test_z_segment_parsed(self):
        raw = "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\rZPI|1|CUSTOM|DATA"
        msg = parse_message(raw)
        zpi = msg.get_segment("ZPI")
        assert zpi is not None
        assert zpi.get_field(1) == "1"
        assert zpi.get_field(2) == "CUSTOM"


# ── Sample file parsing ────────────────────────────────────────────────────

class TestSampleFileParsing:
    """Every .hl7 file in samples/ must parse without error."""

    from tests.unit.sources.hl7_v2.hl7_v2_test_utils import SAMPLES_DIR as _SAMPLES_DIR

    SAMPLE_FILES = sorted(f.name for f in _SAMPLES_DIR.glob("*.hl7"))

    @pytest.mark.parametrize("filename", SAMPLE_FILES)
    def test_sample_parses(self, filename):
        from tests.unit.sources.hl7_v2.hl7_v2_test_utils import load_sample

        from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import (
            _split_messages,
        )

        raw = load_sample(filename)
        msgs = _split_messages(raw)
        assert len(msgs) >= 1, f"{filename}: no messages found"
        for i, msg_str in enumerate(msgs):
            msg = parse_message(msg_str)
            assert msg is not None, f"{filename}: message {i} failed to parse"
            assert len(msg.segments) >= 1
            msh = msg.get_segment("MSH")
            assert msh is not None, f"{filename}: message {i} missing MSH"
