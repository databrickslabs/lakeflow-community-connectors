"""Tests for TXA (Transcription Document Header) segment extraction.

TXA contains document metadata: document type, content presentation,
activity/origination datetimes, authenticators, document status.
One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_txa
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestTXAExtraction:
    def test_mdm_txa(self):
        msg = parse_first(load_sample("sample_mdt_txa.hl7"))
        row = extract_segment(msg, "TXA", _extract_txa)
        assert row["document_type"]["code"] == "DS"
        assert row["document_content_presentation"] == "TX"
        assert row["primary_activity_provider"][0]["id"] == "DOC005"
        assert row["originator"][0]["id"] == "DOC005"
        assert row["transcriptionist"][0]["id"] == "TRANS001"
        assert row["unique_document_number"]["entity_identifier"] == "TXA10001"
        assert row["unique_document_file_name"] == "DISCHSUM_20240320.txt"
        assert row["document_completion_status"] == "AU"
        assert row["document_availability_status"] == "AV"
        # TXA-25 is now ArrayType<STRING> (0..* per spec, v2.9+)
        assert row["document_title"][0] == "Discharge Summary for Baker, James"
        assert row["activity_datetime"] is not None
        assert row["origination_datetime"] is not None


class TestTXAMissingFields:
    def test_minimal_txa(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||MDM^T02|1|P|2.5\r"
            "TXA|1|DS"
        )
        row = _extract_txa(msg.get_segment("TXA"))
        assert row["set_id"] == 1
        assert row["document_type"]["code"] == "DS"
        assert row["document_content_presentation"] is None
        assert row["activity_datetime"] is None
        assert row["origination_datetime"] is None
        assert row["primary_activity_provider"] is None
        assert row["unique_document_number"] is None
        assert row["document_title"] is None

    def test_txa_basic_document(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||MDM^T02|1|P|2.5\r"
            "TXA|1|HP|TX|20240301120000|DOC001"
        )
        row = _extract_txa(msg.get_segment("TXA"))
        assert row["document_type"]["code"] == "HP"
        assert row["document_content_presentation"] == "TX"
        assert row["activity_datetime"] is not None
        assert row["primary_activity_provider"][0]["id"] == "DOC001"
