"""Tests for RXA (Pharmacy/Treatment Administration) segment extraction.

RXA contains administered medication/immunisation data: code, amount, units,
lot number, manufacturer, completion status, timestamps.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_rxa
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestRXAExtraction:
    def test_vxu_rxa(self):
        msg = parse_first(load_sample("sample_vxu_immunization.hl7"))
        row = extract_segment(msg, "RXA", _extract_rxa)
        assert row["administered_code"]["code"] == "141"
        assert row["administered_code"]["text"] == "Influenza, seasonal, injectable"
        assert row["administered_amount"] == "0.5"
        assert row["administered_units"]["code"] == "mL"
        # RXA-15 is now ArrayType<STRING> (0..* per spec, v2.9+)
        assert row["substance_lot_number"][0] == "LT12345"
        # RXA-17 substance_manufacturer_name is ArrayType<CWE> (0..* per spec).
        assert row["substance_manufacturer_name"][0]["code"] == "MFR001"
        assert row["completion_status"] == "CP"
        assert row["datetime_start_of_administration"] is not None
        assert row["datetime_end_of_administration"] is not None
        assert row["system_entry_datetime"] is not None


class TestRXAMissingFields:
    def test_minimal_rxa(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||VXU^V04|1|P|2.5\r"
            "RXA|0|1|20240101||998^No vaccine^CVX"
        )
        row = _extract_rxa(msg.get_segment("RXA"))
        assert row["administration_sub_id_counter"] == 1
        assert row["administered_code"]["code"] == "998"
        assert row["administered_code"]["text"] == "No vaccine"
        assert row["administered_amount"] is None
        assert row["administered_units"] is None
        assert row["substance_lot_number"] is None
        assert row["completion_status"] is None

    def test_rxa_code_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||VXU^V04|1|P|2.5\r"
            "RXA|0|1|20240101|20240101|141^Influenza^CVX|0.5|mL^mL^UCUM"
        )
        row = _extract_rxa(msg.get_segment("RXA"))
        assert row["administered_code"]["code"] == "141"
        assert row["administered_amount"] == "0.5"
        assert row["administered_units"]["code"] == "mL"
