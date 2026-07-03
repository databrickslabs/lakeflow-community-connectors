"""Tests for OBR (Observation Request) segment extraction.

OBR contains order/service information: service identifier, ordering
provider, result status. Multiple OBR segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_obr
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestOBRExtraction:
    def test_oru_obr(self):
        msg = parse_first(load_sample("sample_oru.hl7"))
        row = extract_segment(msg, "OBR", _extract_obr)
        assert row["service"]["code"] == "80048"
        assert row["service"]["text"] == "Basic Metabolic Panel"
        assert row["result_status"] == "F"

    def test_covid_obr_multiple(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        segs = segments_of_type(msg, "OBR")
        assert len(segs) == 2
        row1 = _extract_obr(segs[0])
        assert row1["service"]["code"] == "PERSUBJ"
        row2 = _extract_obr(segs[1])
        assert row2["service"]["code"] == "NOTF"

    def test_celr_obr(self):
        msg = parse_first(load_sample("sample_oru_lab_celr.hl7"))
        row = extract_segment(msg, "OBR", _extract_obr)
        assert row["service"]["code"] == "68991-9"
        assert row["diagnostic_service_section"] == "LAB"


class TestOBRMissingFields:
    def test_minimal_obr(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBR|1"
        )
        row = _extract_obr(msg.get_segment("OBR"))
        assert row["set_id"] == 1
        assert row["service"] is None
        assert row["result_status"] is None
        assert row["ordering_provider"] is None
        assert row["diagnostic_service_section"] is None

    def test_obr_service_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r"
            "OBR|1||FIL001|CBC^Complete Blood Count^LN"
        )
        row = _extract_obr(msg.get_segment("OBR"))
        assert row["service"]["code"] == "CBC"
        assert row["service"]["text"] == "Complete Blood Count"
        assert row["service"]["coding_system"] == "LN"
        assert row["filler_order_number"]["entity_identifier"] == "FIL001"


class TestOBRCompositeFields:
    """OBR composite gap fixes: CQ(9), MOC(23), PRL(26), EIP(29), NDL(32-35), EIP(54)."""

    def test_obr_cq_moc_prl_eip_ndl_fields(self):
        # Build a message exercising the recently-added composite expansions.
        fields = {
            1: "1",
            9: "10.5^mL^UCUM",
            23: "150.00&USD^OFFICE&Office charge&L",
            26: "GLU&Glucose result&LN^SUB1^Some descriptor",
            29: "PARENT123&NS1&OID1&ISO^CHILD456&NS2&OID2&ISO",
            32: "DOC1&Smith&Robert&M&Jr&Dr&MD^20240101120000^20240101130000^WARDA^101^B1^GENHOSP^A^IP^B2^F3",
            33: "ASSIST1&Doe&Jane&&&Dr",
            34: "TECH1&Lab&Technician",
            35: "TRANS1&Transcriber&Pat",
            54: "ORDPARENT&NSP&OIDP&ISO&^CHILDORD&NSC&OIDC&ISO",
        }
        seg_fields = []
        for i in range(1, max(fields) + 1):
            seg_fields.append(fields.get(i, ""))
        obr_line = "OBR|" + "|".join(seg_fields)
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ORU^R01|1|P|2.5\r" + obr_line
        )
        row = _extract_obr(msg.get_segment("OBR"))

        assert row["collection_volume"]["quantity"] == "10.5"
        assert row["collection_volume"]["units"] == "mL"

        assert row["charge_to_practice"]["monetary_amount"] == "150.00"
        assert row["charge_to_practice"]["monetary_amount_currency"] == "USD"
        assert row["charge_to_practice"]["charge_code"] == "OFFICE"
        assert row["charge_to_practice"]["charge_code_text"] == "Office charge"

        assert row["parent_result"]["code"] == "GLU"
        assert row["parent_result"]["text"] == "Glucose result"
        assert row["parent_result"]["coding_system"] == "LN"
        assert row["parent_result"]["sub_id"] == "SUB1"
        assert row["parent_result"]["descriptor"] == "Some descriptor"

        assert row["parent_results_observation_identifier"]["placer_assigned_identifier"]["entity_identifier"] == "PARENT123"
        assert row["parent_results_observation_identifier"]["filler_assigned_identifier"]["entity_identifier"] == "CHILD456"

        principal = row
        assert principal["principal_result_interpreter"]["id"] == "DOC1"
        assert principal["principal_result_interpreter"]["family_name"] == "Smith"
        assert principal["principal_result_interpreter"]["given_name"] == "Robert"
        assert principal["principal_result_interpreter"]["start_datetime"] is not None
        assert principal["principal_result_interpreter"]["end_datetime"] is not None
        assert principal["principal_result_interpreter"]["point_of_care"] == "WARDA"
        assert principal["principal_result_interpreter"]["facility"] == "GENHOSP"

        assert row["assistant_result_interpreter"][0]["id"] == "ASSIST1"
        assert row["technician"][0]["family_name"] == "Lab"
        assert row["transcriptionist"][0]["given_name"] == "Pat"

        assert row["parent_order"][0]["placer_assigned_identifier"]["entity_identifier"] == "ORDPARENT"
