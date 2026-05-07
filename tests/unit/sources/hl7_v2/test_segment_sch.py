"""Tests for SCH (Scheduling Activity Information) segment extraction.

SCH contains appointment data: placer/filler IDs, occurrence number,
appointment reason/type, duration, filler status. One row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_sch
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestSCHExtraction:
    def test_siu_sch(self):
        msg = parse_first(load_sample("sample_siu_scheduling.hl7"))
        row = extract_segment(msg, "SCH", _extract_sch)
        assert row["placer_appointment_id"] == "APT10001"
        assert row["filler_appointment_id"] == "APT10001"
        assert row["occurrence_number"] == 3
        assert row["appointment_reason"] == "CHECKUP"
        assert row["appointment_type"] == "FU"
        assert row["appointment_duration"] == 30
        assert row["filler_status_code"] == "BOOKED"
        assert row["placer_contact_person_id"] == "DOC002"


class TestSCHMissingFields:
    def test_minimal_sch(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||SIU^S12|1|P|2.5\r"
            "SCH|APT001"
        )
        row = _extract_sch(msg.get_segment("SCH"))
        assert row["placer_appointment_id"] == "APT001"
        assert row["filler_appointment_id"] is None
        assert row["occurrence_number"] is None
        assert row["appointment_reason"] is None
        assert row["appointment_type"] is None
        assert row["appointment_duration"] is None
        assert row["filler_status_code"] is None

    def test_sch_basic_appointment(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||SIU^S12|1|P|2.5\r"
            "SCH|APT100^PLACER|APT100^FILLER|1||||ROUTINE^Routine checkup||60|min^minutes"
        )
        row = _extract_sch(msg.get_segment("SCH"))
        assert row["placer_appointment_id"] == "APT100"
        assert row["filler_appointment_id"] == "APT100"
        assert row["occurrence_number"] == 1
        assert row["appointment_reason"] == "ROUTINE"
        assert row["appointment_duration"] == 60
        assert row["appointment_duration_units"] == "min"
