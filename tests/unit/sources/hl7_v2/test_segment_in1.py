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
        assert row["insurance_plan"]["code"] == "BCBS001"
        assert row["insurance_plan"]["text"] == "Blue Cross Blue Shield"
        assert row["insurance_company_name"][0]["name"] == "Blue Cross Blue Shield of Illinois"
        assert row["group_number"] == "GRP7700"
        assert row["plan_type"]["code"] == "PPO"
        assert row["insured_names"][0]["family_name"] == "Martinez"
        assert row["insureds_relationship_to_patient"]["code"] == "SEL"

    def test_dft_in1(self):
        msg = parse_first(load_sample("sample_dft_financial.hl7"))
        row = extract_segment(msg, "IN1", _extract_in1)
        assert row["insurance_plan"]["code"] == "AETNA01"
        assert row["plan_type"]["code"] == "HMO"
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
        assert row["insurance_plan"]["code"] == "PLAN001"
        assert row["insurance_plan"]["text"] == "Basic Plan"
        # IN1-3 is now CX array (v2.9): check first repetition's ID
        assert row["insurance_company"][0]["id"] == "COMP001"
        assert row["insurance_company"][0]["assigning_authority"] == "INS"


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


class TestIN1NewComposites:
    """IN1-14 authorization_information (AUI), IN1-54 external health plan (CWE array)."""

    def test_in1_aui_and_ehp_array(self):
        # IN1-12 (DT), IN1-13 (DT), IN1-14 (AUI: ST + DT + ST), IN1-54 (CWE array).
        fields = {
            1: "1",
            2: "PLAN001",
            12: "20240101",
            13: "20251231",
            14: "AUTH123^20240115^InsuranceCo",
            54: "ABC123^Plan Identifier 1^L~XYZ789^Plan Identifier 2^L",
        }
        seg_fields = [fields.get(i, "") for i in range(1, 55)]
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "IN1|" + "|".join(seg_fields)
        )
        row = _extract_in1(msg.get_segment("IN1"))
        assert row["plan_effective_date"] is not None
        assert row["plan_expiration_date"] is not None
        assert row["authorization_information"]["number"] == "AUTH123"
        assert row["authorization_information"]["date"] is not None
        assert row["authorization_information"]["source"] == "InsuranceCo"
        ehp = row["external_health_plan_identifiers"]
        assert len(ehp) == 2
        assert ehp[0]["code"] == "ABC123"
        assert ehp[0]["text"] == "Plan Identifier 1"
        assert ehp[1]["code"] == "XYZ789"
