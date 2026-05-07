"""Tests for TXA (Transcription Document Header) segment extraction.

TXA contains document metadata: document type, content presentation,
activity/origination datetimes, authenticators, document status.
One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_txa
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestTXAExtraction:
    def test_mdm_txa(self):
        msg = parse_first(load_sample("sample_mdt_txa.hl7"))
        row = extract_segment(msg, "TXA", _extract_txa)
        assert row["document_type"] == "DS"
        assert row["document_content_presentation"] == "TX"
        assert row["primary_activity_provider_id"] == "DOC005"
        assert row["originator_id"] == "DOC005"
        assert row["transcriptionist_id"] == "TRANS001"
        assert row["unique_document_number"] == "TXA10001"
        assert row["unique_document_file_name"] == "DISCHSUM_20240320.txt"
        assert row["document_completion_status"] == "AU"
        assert row["document_availability_status"] == "AV"
        assert row["document_title"] == "Discharge Summary for Baker, James"
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
        assert row["document_type"] == "DS"
        assert row["document_content_presentation"] is None
        assert row["activity_datetime"] is None
        assert row["origination_datetime"] is None
        assert row["primary_activity_provider_id"] is None
        assert row["unique_document_number"] is None
        assert row["document_title"] is None

    def test_txa_basic_document(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||MDM^T02|1|P|2.5\r"
            "TXA|1|HP|TX|20240301120000|DOC001"
        )
        row = _extract_txa(msg.get_segment("TXA"))
        assert row["document_type"] == "HP"
        assert row["document_content_presentation"] == "TX"
        assert row["activity_datetime"] is not None
        assert row["primary_activity_provider_id"] == "DOC001"
