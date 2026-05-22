"""Tests for FT1 (Financial Transaction) segment extraction.

FT1 contains charge/billing data: transaction type, code, description,
quantity, batch ID. Multiple FT1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_ft1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestFT1Extraction:
    def test_comprehensive_ft1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "FT1", _extract_ft1)
        assert row["transaction_type"] == "CG"
        assert row["transaction_code"] == "99285"
        assert row["transaction_code_text"] == "Emergency dept visit, high severity"
        assert row["transaction_quantity"] == 1
        assert row["transaction_description"] == "Emergency Department Visit"
        assert row["transaction_date_start"] == "2024-03-01T00:00:00"
        assert row["transaction_date_end"] is None

    def test_dft_multiple_ft1(self):
        msg = parse_first(load_sample("sample_dft_financial.hl7"))
        segs = segments_of_type(msg, "FT1")
        assert len(segs) == 2
        row1 = _extract_ft1(segs[0])
        assert row1["transaction_code"] == "27447"
        assert row1["transaction_batch_id"] == "BATCH001"
        assert row1["transaction_date_start"] == "2024-03-10T00:00:00"
        row2 = _extract_ft1(segs[1])
        assert row2["transaction_code"] == "01402"
        assert row2["transaction_description"] == "Anesthesia for Knee Surgery"

    def test_ft1_date_range(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||DFT^P03|1|P|2.5\r"
            "FT1|1|||20240301080000^20240301170000|||CG"
        )
        row = _extract_ft1(msg.get_segment("FT1"))
        assert row["transaction_date_start"] == "2024-03-01T08:00:00"
        assert row["transaction_date_end"] == "2024-03-01T17:00:00"

    def test_ft1_cx_xon_cq_si_components(self):
        # Build FT1 by field index to keep the message readable.
        # Field positions (1-indexed) per HL7 v2.8 FT1 spec.
        fields: dict[int, str] = {
            1: "1",
            4: "20240301",
            6: "CG",
            7: "99213",
            30: "CHK12345^7^M11^MEDIBANK&2.16.840.1.113883.3.51&ISO^PRN",  # CX
            31: "10~11~12",  # SI repeat
            32: "Perf Hosp^L^123^^^AA1^^^^9999~Sub Clinic^L^456^^^AA2^^^^8888",  # XON repeat
            33: "Order Hosp^L^789^^^AA3^^^^7777",  # XON
            38: "REF999^^^AUTH&1.2.3&ISO^RN",  # CX
            39: "AUTH777^^^PAYER&1.2.4&ISO^AUT",  # CX
            43: "30.0^mg^UCUM",  # CQ
        }
        max_field = max(fields)
        body = "|".join(fields.get(i, "") for i in range(1, max_field + 1))
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||DFT^P03|1|P|2.5\r"
            f"FT1|{body}"
        )
        row = _extract_ft1(msg.get_segment("FT1"))
        # FT1.30 — Payment Reference ID (CX)
        assert row["payment_reference_id"] == "CHK12345"
        assert row["payment_reference_id_check_digit"] == "7"
        assert row["payment_reference_id_check_digit_scheme"] == "M11"
        assert row["payment_reference_id_assigning_authority"] == "MEDIBANK"
        assert row["payment_reference_id_assigning_authority_universal_id"] == "2.16.840.1.113883.3.51"
        assert row["payment_reference_id_assigning_authority_universal_id_type"] == "ISO"
        assert row["payment_reference_id_type_code"] == "PRN"
        # FT1.31 — Transaction Reference Key (SI, repeatable)
        assert row["transaction_reference_key"] == ["10", "11", "12"]
        # FT1.32 — Performing Facility (XON, repeatable)
        perf = row["performing_facility"]
        assert isinstance(perf, list) and len(perf) == 2
        assert perf[0]["name"] == "Perf Hosp"
        assert perf[0]["id"] == "123"
        assert perf[1]["name"] == "Sub Clinic"
        assert perf[1]["id"] == "456"
        # FT1.33 — Ordering Facility (XON, non-repeating)
        assert row["ordering_facility"] == "Order Hosp"
        assert row["ordering_facility_id"] == "789"
        # FT1.38 — Referral Number (CX)
        assert row["referral_number"] == "REF999"
        assert row["referral_number_assigning_authority"] == "AUTH"
        assert row["referral_number_type_code"] == "RN"
        # FT1.39 — Authorization Number (CX)
        assert row["authorization_number"] == "AUTH777"
        assert row["authorization_number_assigning_authority"] == "PAYER"
        assert row["authorization_number_type_code"] == "AUT"
        # FT1.43 — NDC Qty and UoM (CQ)
        assert row["ndc_qty_and_uom"] == "30.0"
        assert row["ndc_qty_and_uom_units"] == "mg"

    def test_ft1_cp_and_pl_components(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||DFT^P03|1|P|2.5\r"
            "FT1|1|||20240301|20240301|CG|99213||Office visit|2|"
            "150.00&USD^UP|75.00&USD^UP|ED|BCBS001|"
            "50.00&USD|ICU^101^A^MED^O^N^BLDG1^F2"
            "||||||90.00&USD"
        )
        row = _extract_ft1(msg.get_segment("FT1"))
        # FT1.11 — Transaction Amount Extended (CP)
        assert row["transaction_amount_extended"] == "150.00"
        assert row["transaction_amount_extended_currency"] == "USD"
        assert row["transaction_amount_extended_price_type"] == "UP"
        # FT1.12 — Transaction Amount Unit (CP)
        assert row["transaction_amount_unit"] == "75.00"
        assert row["transaction_amount_unit_currency"] == "USD"
        # FT1.15 — Insurance Amount (CP)
        assert row["insurance_amount"] == "50.00"
        assert row["insurance_amount_currency"] == "USD"
        # FT1.16 — Assigned Patient Location (PL)
        assert row["assigned_patient_location_point_of_care"] == "ICU"
        assert row["assigned_patient_location_room"] == "101"
        assert row["assigned_patient_location_bed"] == "A"
        assert row["assigned_patient_location_facility"] == "MED"
        assert row["assigned_patient_location_status"] == "O"
        assert row["assigned_patient_location_type"] == "N"
        assert row["assigned_patient_location_building"] == "BLDG1"
        assert row["assigned_patient_location_floor"] == "F2"
        # FT1.22 — Unit Cost (CP)
        assert row["unit_cost"] == "90.00"
        assert row["unit_cost_currency"] == "USD"


class TestFT1MissingFields:
    def test_minimal_ft1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||DFT^P03|1|P|2.5\r"
            "FT1|1"
        )
        row = _extract_ft1(msg.get_segment("FT1"))
        assert row["set_id"] == 1
        assert row["transaction_id"] is None
        assert row["transaction_batch_id"] is None
        assert row["transaction_type"] is None
        assert row["transaction_code"] is None
        assert row["transaction_description"] is None
        assert row["transaction_quantity"] is None
        assert row["transaction_date_start"] is None
        assert row["transaction_date_end"] is None

    def test_ft1_charge_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||DFT^P03|1|P|2.5\r"
            "FT1|1||BATCH99|||CG|99213^Office Visit^CPT4|Office visit"
        )
        row = _extract_ft1(msg.get_segment("FT1"))
        assert row["transaction_batch_id"] == "BATCH99"
        assert row["transaction_type"] == "CG"
        assert row["transaction_code"] == "99213"
        assert row["transaction_code_text"] == "Office Visit"
        assert row["transaction_description"] == "Office visit"
