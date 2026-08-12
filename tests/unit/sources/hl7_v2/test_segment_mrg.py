"""Tests for MRG (Merge Patient Information) segment extraction.

MRG is used in merge/link/unlink events to convey the prior patient ID
being superseded. One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_mrg
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestMRGExtraction:
    def test_comprehensive_mrg(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "MRG", _extract_mrg)
        assert row["prior_patient_id"][0]["id"] == "MRN77001"
        assert row["prior_patient_account_number"]["id"] == "MRN66001"


class TestMRGMissingFields:
    def test_minimal_mrg(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A34|1|P|2.5\r"
            "MRG|OLD001^^^HOSP^MR"
        )
        row = _extract_mrg(msg.get_segment("MRG"))
        assert row["prior_patient_id"][0]["id"] == "OLD001"
        assert row["prior_alternate_patient_id"] is None
        assert row["prior_patient_account_number"] is None
        assert row["prior_patient_names"] is None

    def test_mrg_all_empty(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A34|1|P|2.5\r"
            "MRG"
        )
        row = _extract_mrg(msg.get_segment("MRG"))
        assert row["prior_patient_id"] is None
        assert row["prior_patient_names"] is None
