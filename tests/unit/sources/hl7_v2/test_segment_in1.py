"""Tests for IN1 (Insurance) segment extraction.

IN1 contains insurance plan data: plan ID, company name, group number,
plan type, insured's name and relationship. Multiple IN1 segments can appear.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first

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
        assert row["insurance_company_name"] == "Blue Cross Blue Shield of Illinois"
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
