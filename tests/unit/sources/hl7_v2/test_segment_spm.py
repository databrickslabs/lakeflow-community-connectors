"""Tests for SPM (Specimen) segment extraction.

SPM contains specimen data: type, collection method, source site,
collection and received datetimes. Multiple SPM segments can appear.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_spm
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestSPMExtraction:
    def test_celr_spm(self):
        msg = parse_first(load_sample("sample_oru_lab_celr.hl7"))
        row = extract_segment(msg, "SPM", _extract_spm)
        assert row["specimen_type"] == "258500001"
        assert row["specimen_type_text"] == "Nasopharyngeal swab"

    def test_gc_spm(self):
        msg = parse_first(load_sample("sample_oru_gc_testing.hl7"))
        row = extract_segment(msg, "SPM", _extract_spm)
        assert row["specimen_type"] == "119393003"

    def test_flu_ar_spm_source_site(self):
        msg = parse_first(load_sample("sample_oru_flu_ar.hl7"))
        row = extract_segment(msg, "SPM", _extract_spm)
        assert row["specimen_type"] == "258604001"
        assert row["specimen_source_site"] == "181200003"

    def test_concat_notes_spm(self):
        msg = parse_first(load_sample("sample_oru_concat_notes.hl7"))
        row = extract_segment(msg, "SPM", _extract_spm)
        assert row["specimen_type"] == "258528007"
        assert row["specimen_type_text"] == "rectal swab"


class TestSPMMissingFields:
    def test_minimal_spm(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "SPM|1"
        )
        row = _extract_spm(msg.get_segment("SPM"))
        assert row["set_id"] == 1
        assert row["specimen_id_placer_assigned_identifier"] is None
        assert row["specimen_type"] is None
        assert row["specimen_type_text"] is None
        assert row["specimen_source_site"] is None
        assert row["specimen_collection_method"] is None

    def test_spm_type_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "SPM|1||||||||||||||||||Blood^Whole blood^SCT"
        )
        row = _extract_spm(msg.get_segment("SPM"))
        assert row["set_id"] == 1


class TestSPMNewComposites:
    """SPM-2 specimen_id (EIP single [0..1]), SPM-12 collection_amount (CQ), SPM-17 DR."""

    def test_spm_eip_cq_and_dr(self):
        # SPM-2 is EIP [0..1] — single instance, flat columns (not array)
        fields = {
            1: "1",
            2: "SPEC123&NS1&OID1&ISO^PARENT99&NS2&OID2&ISO",
            12: "10.5^mL&Milliliter&UCUM",
            17: "20240301120000^20240301140000",
        }
        seg_fields = [fields.get(i, "") for i in range(1, 18)]
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "SPM|" + "|".join(seg_fields)
        )
        row = _extract_spm(msg.get_segment("SPM"))
        assert row["specimen_id_placer_assigned_identifier"] == "SPEC123"
        assert row["specimen_id_placer_assigned_identifier_namespace_id"] == "NS1"
        assert row["specimen_id_placer_assigned_identifier_universal_id"] == "OID1"
        assert row["specimen_id_placer_assigned_identifier_universal_id_type"] == "ISO"
        assert row["specimen_id_filler_assigned_identifier"] == "PARENT99"

        assert row["specimen_collection_amount"] == "10.5"
        assert row["specimen_collection_amount_units"] == "mL"

        assert row["specimen_collection_datetime_start"] is not None
        assert row["specimen_collection_datetime_end"] is not None
