"""Tests for IN1 (Insurance) segment extraction.

IN1 contains insurance plan data: plan ID, company name, group number,
plan type, insured's name and relationship. Multiple IN1 segments can appear.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_in1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestIN1Extraction:
    def test_comprehensive_in1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "IN1", _extract_in1)
        assert row["insurance_plan"] == "BCBS001"
        assert row["insurance_plan_text"] == "Blue Cross Blue Shield"
        assert row["insurance_company_name"][0]["name"] == "Blue Cross Blue Shield of Illinois"
        assert row["group_number"] == "GRP7700"
        assert row["plan_type"] == "PPO"
        assert row["insured_names"][0]["family_name"] == "Martinez"
        assert row["insureds_relationship_to_patient"] == "SEL"

    def test_dft_in1(self):
        msg = parse_first(load_sample("sample_dft_financial.hl7"))
        row = extract_segment(msg, "IN1", _extract_in1)
        assert row["insurance_plan"] == "AETNA01"
        assert row["plan_type"] == "HMO"
        assert row["group_number"] == "GRP4400"


class TestIN1MissingFields:
    def test_minimal_in1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "IN1|1"
        )
        row = _extract_in1(msg.get_segment("IN1"))
        assert row["set_id"] == 1
        assert row["insurance_plan"] is None
        assert row["insurance_company_name"] is None
        assert row["group_number"] is None
        assert row["plan_type"] is None
        assert row["insured_names"] is None

    def test_in1_plan_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "IN1|1|PLAN001^Basic Plan|COMP001^^^INS"
        )
        row = _extract_in1(msg.get_segment("IN1"))
        assert row["insurance_plan"] == "PLAN001"
        assert row["insurance_plan_text"] == "Basic Plan"
        assert row["insurance_company"] == "COMP001"


class TestIN1ArrayPromotion:
    """IN1-4 (insurance_company_name, XON 0..*) and IN1-5 (insurance_company_address,
    XAD 0..*) are spec-typed 0..* — must be captured as ARRAY<STRUCT<...>>
    preserving every ~-separated repetition.
    """

    def test_insurance_company_name_xon_array(self):
        # IN1-4 carries one or more organization names with full XON sub-components.
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "IN1|1|PLAN001|COMP001|"
            "BCBS Illinois^L^IL01^^^BCBS&urn:bcbs&ISO^XX"
            "~Anthem Inc^L^AN02^^^ANTHEM&urn:anthem&ISO^XX"
        )
        row = _extract_in1(msg.get_segment("IN1"))
        names = row["insurance_company_name"]
        assert len(names) == 2
        assert names[0]["name"] == "BCBS Illinois"
        assert names[0]["type_code"] == "L"
        assert names[0]["id"] == "IL01"
        assert names[0]["assigning_authority"] == "BCBS"
        assert names[0]["assigning_authority_universal_id"] == "urn:bcbs"
        assert names[0]["id_type_code"] == "XX"
        assert names[1]["name"] == "Anthem Inc"
        assert names[1]["id"] == "AN02"
