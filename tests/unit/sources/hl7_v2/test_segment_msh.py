"""Tests for MSH (Message Header) segment extraction.

Validated against HL7 v2.5–v2.9 specification.
MSH is present in every HL7 message — one row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_msh
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_schemas import (
    get_schema,
)

_METADATA = {
    "message_id",
    "message_timestamp",
    "hl7_version",
    "source_file",
    "send_time",
    "create_time",
    "raw_segment",
}


# ── Extraction: happy path ──────────────────────────────────────────────────

class TestMSHExtraction:
    def test_adt_msh(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["sending_application"] == "HIS"
        assert row["sending_facility"] == "GENERAL_HOSPITAL"
        assert row["receiving_application"] == "LAB"
        assert row["message_code"] == "ADT"
        assert row["trigger_event"] == "A01"
        assert row["message_structure"] == "ADT_A01"
        assert row["version_id"]["id"] == "2.5.1"
        assert row["processing_id"]["id"] == "P"

    def test_oru_covid_msh(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["message_code"] == "ORU"
        assert row["trigger_event"] == "R01"
        assert row["version_id"]["id"] == "2.5"

    def test_celr_msh_with_timezone(self):
        msg = parse_first(load_sample("sample_oru_lab_celr.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["message_code"] == "ORU"
        assert row["version_id"]["id"] == "2.5.1"
        assert row["message_datetime"] is not None

    def test_flu_ar_msh_security(self):
        msg = parse_first(load_sample("sample_oru_flu_ar.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["security"] == "36225"

    def test_msh_pt_vid_composites(self):
        # MSH-11 is PT (Processing ID + Processing Mode); MSH-12 is VID
        # (Version ID + Internationalization CWE + International Version CWE).
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101120000||ADT^A01|MSG001|P^A|2.5.1^EN&English&L^EN-US&US English&L\r"
        )
        row = _extract_msh(msg.get_segment("MSH"))
        assert row["processing_id"]["id"] == "P"
        assert row["processing_id"]["mode"] == "A"
        assert row["version_id"]["id"] == "2.5.1"
        assert row["version_id"]["internationalization"] == "EN"
        assert row["version_id"]["internationalization_text"] == "English"
        assert row["version_id"]["internationalization_coding_system"] == "L"
        assert row["version_id"]["international_version"] == "EN-US"


# ── Extraction: field details ───────────────────────────────────────────────

class TestMSHFieldDetails:
    def test_field_separator_is_pipe(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["field_separator"] == "|"

    def test_encoding_characters(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["encoding_characters"] == "^~\\&"

    def test_message_control_id_present(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["message_control_id"] is not None


# ── Extraction: missing fields ──────────────────────────────────────────────

class TestMSHMissingFields:
    def test_minimal_msh(self):
        msg = parse_message("MSH|^~\\&|SYS|FAC|RCV|FAC|20240101||ADT^A01|1|P|2.5")
        msh = msg.get_segment("MSH")
        row = _extract_msh(msh)
        assert row["security"] is None
        assert row["sequence_number"] is None
        assert row["country_code"] is None
        assert row["character_set"] is None

    def test_all_output_keys_present(self):
        msg = parse_message("MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5")
        row = _extract_msh(msg.get_segment("MSH"))
        schema_keys = {f.name for f in get_schema("msh").fields}
        expected_keys = schema_keys - _METADATA
        assert set(row.keys()) == expected_keys
