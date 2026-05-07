"""HL7 v2 community connector — ingests HL7 messages from multiple source types.

Supported source modes (configured via ``source_type`` connection option):

* ``gcp`` (default) — fetches messages from a Google Cloud Healthcare API
  HL7v2 store via REST.
* ``delta`` — reads pre-loaded messages from a Bronze Delta table.  The table
  must contain columns ``data`` (raw HL7 pipe-delimited text), ``createTime``
  (RFC3339 string), and optionally ``name`` (source identifier).

Each HL7 segment type becomes its own table (msh, pid, pv1, obr, obx, …).

Schemas follow the HL7 v2.9 specification (the latest version, which is a
superset of all prior versions).

Incremental cursor: ``createTime`` (RFC3339 timestamp).
The connector uses a sliding time-window strategy to bound each micro-batch.
"""

from __future__ import annotations

import base64
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    HL7Message,
    HL7Segment,
    parse_message,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_schemas import (
    SEGMENT_SCHEMAS,
    SEGMENT_TABLES,
    TABLE_DESCRIPTIONS,
    get_schema,
)

_DEFAULT_WINDOW_SECONDS = 86_400
_RETRIABLE_STATUS_CODES = (429, 500, 503)
_MAX_RETRIES = 3
_INITIAL_BACKOFF = 1
_REQUEST_TIMEOUT = 30
_MAX_PAGE_SIZE = 1000

_SINGLE_SEGMENT_TABLES = frozenset(
    {"msh", "evn", "pid", "pd1", "pv1", "pv2", "mrg", "sch", "txa"}
)


# ---------------------------------------------------------------------------
# Null-safe helpers
# ---------------------------------------------------------------------------


def _v(s: str) -> str | None:
    """Return *s* if non-empty, else None."""
    return s if s else None


def _i(s: str) -> int | None:
    """Parse *s* as int; return None on failure."""
    if not s:
        return None
    try:
        return int(s.strip())
    except ValueError:
        return None


_DTM_RE = re.compile(
    r"^(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?(?:\.\d+)?([+-]\d{4})?$"
)


def _parse_dtm(s: str) -> str | None:
    """Parse an HL7 DTM string to an ISO-8601 UTC string.

    Handles partial precision (YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHHMMSS)
    and optional timezone offset (e.g. +0500, -0800).  If a timezone offset
    is present the value is converted to UTC first.  Returns an ISO-8601
    string (no timezone suffix) so the schema can use StringType and avoid
    Arrow timestamp-timezone mismatches.
    Returns None for empty or unparseable input.
    """
    if not s:
        return None
    cleaned = s.strip().strip("()")
    m = _DTM_RE.match(cleaned)
    if not m:
        return None
    y, mo, d, h, mi, sec, tz = m.groups()
    try:
        dt = datetime(
            int(y),
            int(mo or 1),
            int(d or 1),
            int(h or 0),
            int(mi or 0),
            int(sec or 0),
        )
        if tz:
            sign = 1 if tz[0] == "+" else -1
            offset = timedelta(hours=int(tz[1:3]), minutes=int(tz[3:5]))
            dt = dt.replace(tzinfo=timezone(sign * offset))
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.isoformat()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Composite-type helpers — extract all components with consistent naming
# ---------------------------------------------------------------------------


def _xpn_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = True
) -> dict:
    """XPN (Extended Person Name) — 14 active components."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))

    return {
        f"{prefix}_family_name": gc(1),
        f"{prefix}_given_name": gc(2),
        f"{prefix}_middle_name": gc(3),
        f"{prefix}_suffix": gc(4),
        f"{prefix}_prefix": gc(5),
        f"{prefix}_degree": gc(6),
        f"{prefix}_name_type_code": gc(7),
        f"{prefix}_name_representation_code": gc(8),
        f"{prefix}_name_context": gc(9),
        f"{prefix}_name_assembly_order": gc(11),
        f"{prefix}_name_effective_date": gc(12),
        f"{prefix}_name_expiration_date": gc(13),
        f"{prefix}_professional_suffix": gc(14),
        f"{prefix}_called_by": gc(15),
    }


def _xpn_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """XPN (Extended Person Name) — all repetitions as a list of dicts."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        gc = lambda i: _v(parts[i - 1]) if len(parts) >= i else None
        result.append({
            "family_name": gc(1),
            "given_name": gc(2),
            "middle_name": gc(3),
            "suffix": gc(4),
            "prefix": gc(5),
            "degree": gc(6),
            "name_type_code": gc(7),
            "name_representation_code": gc(8),
            "name_context": gc(9),
            "name_assembly_order": gc(11),
            "name_effective_date": gc(12),
            "name_expiration_date": gc(13),
            "professional_suffix": gc(14),
            "called_by": gc(15),
        })
    return {column_name: result if result else None}


def _xcn_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = True
) -> dict:
    """XCN (Extended Composite ID Number and Name for Persons) — 21 active components."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
        gsc = lambda comp, sub: _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
        gsc = lambda comp, sub: _v(seg.get_sub_component(field_n, comp, sub))
    return {
        f"{prefix}_id": gc(1),
        f"{prefix}_family_name": gc(2),
        f"{prefix}_given_name": gc(3),
        f"{prefix}_middle_name": gc(4),
        f"{prefix}_suffix": gc(5),
        f"{prefix}_prefix": gc(6),
        f"{prefix}_degree": gc(7),
        f"{prefix}_source_table": gc(8),
        f"{prefix}_assigning_authority": gsc(9, 1),
        f"{prefix}_assigning_authority_universal_id": gsc(9, 2),
        f"{prefix}_assigning_authority_universal_id_type": gsc(9, 3),
        f"{prefix}_name_type_code": gc(10),
        f"{prefix}_check_digit": gc(11),
        f"{prefix}_check_digit_scheme": gc(12),
        f"{prefix}_identifier_type_code": gc(13),
        f"{prefix}_assigning_facility": gsc(14, 1),
        f"{prefix}_assigning_facility_universal_id": gsc(14, 2),
        f"{prefix}_assigning_facility_universal_id_type": gsc(14, 3),
        f"{prefix}_name_representation_code": gc(15),
        f"{prefix}_name_assembly_order": gc(18),
        f"{prefix}_effective_date": gc(19),
        f"{prefix}_expiration_date": gc(20),
        f"{prefix}_professional_suffix": gc(21),
    }


def _cwe_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = False
) -> dict:
    """CWE (Coded With Exceptions) — 9 active components (10-22 are OID/value-set metadata, rarely populated)."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
    return {
        f"{prefix}": gc(1),
        f"{prefix}_text": gc(2),
        f"{prefix}_coding_system": gc(3),
        f"{prefix}_alt_code": gc(4),
        f"{prefix}_alt_text": gc(5),
        f"{prefix}_alt_coding_system": gc(6),
        f"{prefix}_coding_system_version": gc(7),
        f"{prefix}_alt_coding_system_version": gc(8),
        f"{prefix}_original_text": gc(9),
    }


def _hd_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = False
) -> dict:
    """HD (Hierarchic Designator) — 3 components."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
    return {
        f"{prefix}": gc(1),
        f"{prefix}_universal_id": gc(2),
        f"{prefix}_universal_id_type": gc(3),
    }


def _ei_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = False
) -> dict:
    """EI (Entity Identifier) — 4 components."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
    return {
        f"{prefix}": gc(1),
        f"{prefix}_namespace_id": gc(2),
        f"{prefix}_universal_id": gc(3),
        f"{prefix}_universal_id_type": gc(4),
    }


def _cwe_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """CWE (Coded With Exceptions) — all repetitions as a list of dicts."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        gc = lambda i, _p=parts: _v(_p[i - 1]) if len(_p) >= i else None
        result.append({
            "code": gc(1),
            "text": gc(2),
            "coding_system": gc(3),
            "alt_code": gc(4),
            "alt_text": gc(5),
            "alt_coding_system": gc(6),
            "coding_system_version": gc(7),
            "alt_coding_system_version": gc(8),
            "original_text": gc(9),
        })
    return {column_name: result if result else None}


def _s_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """Simple repeatable ID/IS field — all repetitions as a list of strings."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = [_v(r) for r in raw.split(seg._enc.rep_sep) if r]
    return {column_name: reps if reps else None}


def _ei_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """EI (Entity Identifier) — all repetitions as a list of dicts."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        result.append({
            "entity_identifier": _v(parts[0]) if len(parts) > 0 else None,
            "namespace_id": _v(parts[1]) if len(parts) > 1 else None,
            "universal_id": _v(parts[2]) if len(parts) > 2 else None,
            "universal_id_type": _v(parts[3]) if len(parts) > 3 else None,
        })
    return {column_name: result if result else None}


def _xon_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = True
) -> dict:
    """XON (Extended Composite Name and Number for Organizations) — 10 components."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
        gsc = lambda comp, sub: _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
        gsc = lambda comp, sub: _v(seg.get_sub_component(field_n, comp, sub))
    return {
        f"{prefix}": gc(1),
        f"{prefix}_type_code": gc(2),
        f"{prefix}_id": gc(3),
        f"{prefix}_check_digit": gc(4),
        f"{prefix}_check_digit_scheme": gc(5),
        f"{prefix}_assigning_authority": gsc(6, 1),
        f"{prefix}_assigning_authority_universal_id": gsc(6, 2),
        f"{prefix}_assigning_authority_universal_id_type": gsc(6, 3),
        f"{prefix}_id_type_code": gc(7),
        f"{prefix}_assigning_facility": gsc(8, 1),
        f"{prefix}_assigning_facility_universal_id": gsc(8, 2),
        f"{prefix}_assigning_facility_universal_id_type": gsc(8, 3),
        f"{prefix}_name_rep_code": gc(9),
        f"{prefix}_identifier": gc(10),
    }


def _cx_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = True
) -> dict:
    """CX (Extended Composite ID with Check Digit) — 12 components + HD sub-components."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
        gsc = lambda comp, sub: _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
        gsc = lambda comp, sub: _v(seg.get_sub_component(field_n, comp, sub))
    return {
        f"{prefix}": gc(1),
        f"{prefix}_check_digit": gc(2),
        f"{prefix}_check_digit_scheme": gc(3),
        f"{prefix}_assigning_authority": gsc(4, 1),
        f"{prefix}_assigning_authority_universal_id": gsc(4, 2),
        f"{prefix}_assigning_authority_universal_id_type": gsc(4, 3),
        f"{prefix}_type_code": gc(5),
        f"{prefix}_assigning_facility": gsc(6, 1),
        f"{prefix}_assigning_facility_universal_id": gsc(6, 2),
        f"{prefix}_assigning_facility_universal_id_type": gsc(6, 3),
        f"{prefix}_effective_date": gc(7),
        f"{prefix}_expiration_date": gc(8),
        f"{prefix}_assigning_jurisdiction": gc(9),
        f"{prefix}_assigning_agency": gc(10),
        f"{prefix}_security_check": gc(11),
        f"{prefix}_security_check_scheme": gc(12),
    }


def _xtn_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = True
) -> dict:
    """XTN (Extended Telecommunication Number) — 18 components."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
        gsc = lambda comp, sub: _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
        gsc = lambda comp, sub: _v(seg.get_sub_component(field_n, comp, sub))
    return {
        f"{prefix}_number": gc(1),
        f"{prefix}_use_code": gc(2),
        f"{prefix}_equipment_type": gc(3),
        f"{prefix}_communication_address": gc(4),
        f"{prefix}_country_code": gc(5),
        f"{prefix}_area_code": gc(6),
        f"{prefix}_local_number": gc(7),
        f"{prefix}_extension": gc(8),
        f"{prefix}_any_text": gc(9),
        f"{prefix}_extension_prefix": gc(10),
        f"{prefix}_speed_dial_code": gc(11),
        f"{prefix}_unformatted_number": gc(12),
        f"{prefix}_effective_start_date": gc(13),
        f"{prefix}_expiration_date": gc(14),
        f"{prefix}_expiration_reason": gsc(15, 1),
        f"{prefix}_protection_code": gsc(16, 1),
        f"{prefix}_shared_telecom_id": gsc(17, 1),
        f"{prefix}_preference_order": gc(18),
    }


def _xad_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = True
) -> dict:
    """XAD (Extended Address) — 23 components (12 is deprecated/skipped)."""
    if repeating:
        gc = lambda comp: _v(seg.get_rep_component(field_n, 1, comp))
        gsc = lambda comp, sub: _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
    else:
        gc = lambda comp: _v(seg.get_component(field_n, comp))
        gsc = lambda comp, sub: _v(seg.get_sub_component(field_n, comp, sub))
    return {
        f"{prefix}_street": gc(1),
        f"{prefix}_other_designation": gc(2),
        f"{prefix}_city": gc(3),
        f"{prefix}_state": gc(4),
        f"{prefix}_zip": gc(5),
        f"{prefix}_country": gc(6),
        f"{prefix}_type": gc(7),
        f"{prefix}_other_geographic": gc(8),
        f"{prefix}_county_parish_code": gsc(9, 1),
        f"{prefix}_county_parish_text": gsc(9, 2),
        f"{prefix}_county_parish_coding_system": gsc(9, 3),
        f"{prefix}_census_tract": gsc(10, 1),
        f"{prefix}_census_tract_text": gsc(10, 2),
        f"{prefix}_census_tract_coding_system": gsc(10, 3),
        f"{prefix}_representation_code": gc(11),
        f"{prefix}_effective_date": gc(13),
        f"{prefix}_expiration_date": gc(14),
        f"{prefix}_expiration_reason": gsc(15, 1),
        f"{prefix}_expiration_reason_text": gsc(15, 2),
        f"{prefix}_expiration_reason_coding_system": gsc(15, 3),
        f"{prefix}_temporary_indicator": gc(16),
        f"{prefix}_bad_address_indicator": gc(17),
        f"{prefix}_usage": gc(18),
        f"{prefix}_addressee": gc(19),
        f"{prefix}_comment": gc(20),
        f"{prefix}_preference_order": gc(21),
        f"{prefix}_protection_code": gsc(22, 1),
        f"{prefix}_protection_code_text": gsc(22, 2),
        f"{prefix}_protection_code_coding_system": gsc(22, 3),
        f"{prefix}_identifier": gsc(23, 1),
    }


# ---------------------------------------------------------------------------
# Metadata builder (from MSH segment, added to every row)
# ---------------------------------------------------------------------------


def _metadata(msh: HL7Segment | None, source_file: str, send_time: str, create_time: str) -> dict:
    if msh is None:
        return {
            "message_id": None,
            "message_timestamp": None,
            "hl7_version": None,
            "source_file": source_file,
            "send_time": send_time,
            "create_time": create_time,
        }
    return {
        "message_id": _v(msh.get_field(10)),
        "message_timestamp": _v(msh.get_field(7)),
        "hl7_version": _v(msh.get_field(12)),
        "source_file": source_file,
        "send_time": send_time,
        "create_time": create_time,
    }


# ---------------------------------------------------------------------------
# Per-segment field extractors
# ---------------------------------------------------------------------------


def _extract_msh(seg: HL7Segment) -> dict:
    return {
        "field_separator": _v(seg.get_field(1)),
        "encoding_characters": _v(seg.get_field(2)),
        "sending_application": _v(seg.get_component(3, 1)),
        "sending_application_universal_id": _v(seg.get_component(3, 2)),
        "sending_application_universal_id_type": _v(seg.get_component(3, 3)),
        "sending_facility": _v(seg.get_component(4, 1)),
        "sending_facility_universal_id": _v(seg.get_component(4, 2)),
        "sending_facility_universal_id_type": _v(seg.get_component(4, 3)),
        "receiving_application": _v(seg.get_component(5, 1)),
        "receiving_application_universal_id": _v(seg.get_component(5, 2)),
        "receiving_application_universal_id_type": _v(seg.get_component(5, 3)),
        "receiving_facility": _v(seg.get_component(6, 1)),
        "receiving_facility_universal_id": _v(seg.get_component(6, 2)),
        "receiving_facility_universal_id_type": _v(seg.get_component(6, 3)),
        "message_datetime": _parse_dtm(seg.get_field(7)),
        "security": _v(seg.get_field(8)),
        "message_code": _v(seg.get_component(9, 1)),
        "trigger_event": _v(seg.get_component(9, 2)),
        "message_structure": _v(seg.get_component(9, 3)),
        "message_control_id": _v(seg.get_field(10)),
        "processing_id": _v(seg.get_component(11, 1)),
        "version_id": _v(seg.get_field(12)),
        "sequence_number": _i(seg.get_field(13)),
        "continuation_pointer": _v(seg.get_field(14)),
        "accept_acknowledgment_type": _v(seg.get_field(15)),
        "application_acknowledgment_type": _v(seg.get_field(16)),
        "country_code": _v(seg.get_field(17)),
        **_s_array_fields(seg, 18, "character_set"),
        **_cwe_fields(seg, 19, "principal_language", repeating=False),
        "alt_character_set_handling": _v(seg.get_field(20)),
        **_ei_array_fields(seg, 21, "message_profile_identifiers"),
        **_xon_fields(seg, 22, "sending_responsible_org", repeating=False),
        **_xon_fields(seg, 23, "receiving_responsible_org", repeating=False),
        **_hd_fields(seg, 24, "sending_network_address", repeating=False),
        **_hd_fields(seg, 25, "receiving_network_address", repeating=False),
        **_cwe_fields(seg, 26, "security_classification_tag", repeating=False),
        **_cwe_array_fields(seg, 27, "security_handling_instructions"),
        **_cwe_array_fields(seg, 28, "special_access_restriction"),
    }


def _extract_pid(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)),
        "patient_external_id": _v(seg.get_field(2)),
        **_cx_fields(seg, 3, "patient_id"),
        "alternate_patient_id": _v(seg.get_first_repetition(4)),
        **_xpn_array_fields(seg, 5, "patient_names"),
        **_xpn_array_fields(seg, 6, "mothers_maiden_names"),
        "date_of_birth": _parse_dtm(seg.get_field(7)),
        **_cwe_fields(seg, 8, "administrative_sex", repeating=False),
        "patient_alias": _v(seg.get_first_repetition(9)),
        **_cwe_fields(seg, 10, "race", repeating=True),
        **_xad_fields(seg, 11, "address"),
        "county_code": _v(seg.get_field(12)),
        **_xtn_fields(seg, 13, "home_phone"),
        **_xtn_fields(seg, 14, "business_phone"),
        **_cwe_fields(seg, 15, "primary_language", repeating=False),
        **_cwe_fields(seg, 16, "marital_status", repeating=False),
        **_cwe_fields(seg, 17, "religion", repeating=False),
        **_cx_fields(seg, 18, "patient_account", repeating=False),
        "ssn": _v(seg.get_field(19)),
        "drivers_license": _v(seg.get_field(20)),
        **_cx_fields(seg, 21, "mothers_identifier"),
        **_cwe_fields(seg, 22, "ethnic_group", repeating=True),
        "birth_place": _v(seg.get_field(23)),
        "multiple_birth_indicator": _v(seg.get_field(24)),
        "birth_order": _i(seg.get_field(25)),
        **_cwe_fields(seg, 26, "citizenship", repeating=True),
        **_cwe_fields(seg, 27, "veterans_military_status", repeating=False),
        **_cwe_fields(seg, 28, "nationality", repeating=False),
        "patient_death_datetime": _parse_dtm(seg.get_field(29)),
        "patient_death_indicator": _v(seg.get_field(30)),
        "identity_unknown_indicator": _v(seg.get_field(31)),
        **_cwe_fields(seg, 32, "identity_reliability_code", repeating=True),
        "last_update_datetime": _parse_dtm(seg.get_field(33)),
        **_hd_fields(seg, 34, "last_update_facility", repeating=False),
        **_cwe_fields(seg, 35, "species_code", repeating=False),
        **_cwe_fields(seg, 36, "breed_code", repeating=False),
        "strain": _v(seg.get_field(37)),
        **_cwe_fields(seg, 38, "production_class_code", repeating=False),
        **_cwe_fields(seg, 39, "tribal_citizenship", repeating=False),
        **_xtn_fields(seg, 40, "patient_telecom"),
    }


def _extract_pv1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)),
        **_cwe_fields(seg, 2, "patient_class", repeating=False),
        "assigned_patient_location": _v(seg.get_field(3)),
        "location_point_of_care": _v(seg.get_component(3, 1)),
        "location_room": _v(seg.get_component(3, 2)),
        "location_bed": _v(seg.get_component(3, 3)),
        "location_facility": _v(seg.get_component(3, 4)),
        "location_status": _v(seg.get_component(3, 5)),
        "location_type": _v(seg.get_component(3, 9)),
        **_cwe_fields(seg, 4, "admission_type", repeating=False),
        **_cx_fields(seg, 5, "preadmit_number", repeating=False),
        "prior_patient_location": _v(seg.get_field(6)),
        **_xcn_fields(seg, 7, "attending_doctor"),
        **_xcn_fields(seg, 8, "referring_doctor"),
        **_xcn_fields(seg, 9, "consulting_doctor"),
        **_cwe_fields(seg, 10, "hospital_service", repeating=False),
        "temporary_location": _v(seg.get_field(11)),
        "preadmit_test_indicator": _v(seg.get_field(12)),
        "readmission_indicator": _v(seg.get_field(13)),
        **_cwe_fields(seg, 14, "admit_source", repeating=False),
        **_cwe_array_fields(seg, 15, "ambulatory_status"),
        **_cwe_fields(seg, 16, "vip_indicator", repeating=False),
        **_xcn_fields(seg, 17, "admitting_doctor"),
        **_cwe_fields(seg, 18, "patient_type", repeating=False),
        **_cx_fields(seg, 19, "visit_number", repeating=False),
        "financial_class": _v(seg.get_rep_component(20, 1, 1)),
        **_cwe_fields(seg, 21, "charge_price_indicator", repeating=False),
        **_cwe_fields(seg, 22, "courtesy_code", repeating=False),
        **_cwe_fields(seg, 23, "credit_rating", repeating=False),
        "contract_code": _v(seg.get_first_repetition(24)),
        "contract_effective_date": _v(seg.get_first_repetition(25)),
        "contract_amount": _v(seg.get_first_repetition(26)),
        "contract_period": _v(seg.get_first_repetition(27)),
        "interest_code": _v(seg.get_field(28)),
        "transfer_to_bad_debt_code": _v(seg.get_field(29)),
        "transfer_to_bad_debt_date": _v(seg.get_field(30)),
        "bad_debt_agency_code": _v(seg.get_field(31)),
        "bad_debt_transfer_amount": _v(seg.get_field(32)),
        "bad_debt_recovery_amount": _v(seg.get_field(33)),
        "delete_account_indicator": _v(seg.get_field(34)),
        "delete_account_date": _v(seg.get_field(35)),
        **_cwe_fields(seg, 36, "discharge_disposition", repeating=False),
        "discharged_to_location": _v(seg.get_component(37, 1)),
        **_cwe_fields(seg, 38, "diet_type", repeating=False),
        **_cwe_fields(seg, 39, "servicing_facility", repeating=False),
        "bed_status": _v(seg.get_field(40)),
        **_cwe_fields(seg, 41, "account_status", repeating=False),
        "pending_location": _v(seg.get_field(42)),
        "prior_temporary_location": _v(seg.get_field(43)),
        "admit_datetime": _parse_dtm(seg.get_first_repetition(44)),
        "discharge_datetime": _parse_dtm(seg.get_first_repetition(45)),
        "current_patient_balance": _v(seg.get_field(46)),
        "total_charges": _v(seg.get_field(47)),
        "total_adjustments": _v(seg.get_field(48)),
        "total_payments": _v(seg.get_field(49)),
        **_cx_fields(seg, 50, "alternate_visit_id", repeating=False),
        **_cwe_fields(seg, 51, "visit_indicator", repeating=False),
        **_xcn_fields(seg, 52, "other_healthcare_provider"),
        "service_episode_description": _v(seg.get_field(53)),
        **_ei_fields(seg, 54, "service_episode_identifier", repeating=False),
    }


def _extract_obr(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_ei_fields(seg, 2, "placer_order_number", repeating=False),
        **_ei_fields(seg, 3, "filler_order_number", repeating=False),
        **_cwe_fields(seg, 4, "service", repeating=False),
        "priority": _v(seg.get_field(5)),
        "requested_datetime": _parse_dtm(seg.get_field(6)),
        "observation_datetime": _parse_dtm(seg.get_field(7)),
        "observation_end_datetime": _parse_dtm(seg.get_field(8)),
        "collection_volume": _v(seg.get_component(9, 1)),
        "collection_volume_units": _v(seg.get_component(9, 2)),
        **_xcn_fields(seg, 10, "collector"),
        "specimen_action_code": _v(seg.get_field(11)),
        **_cwe_fields(seg, 12, "danger_code", repeating=False),
        "relevant_clinical_information": _v(seg.get_field(13)),
        "specimen_received_datetime": _parse_dtm(seg.get_field(14)),
        "specimen_source": _v(seg.get_field(15)),
        **_xcn_fields(seg, 16, "ordering_provider"),
        **_xtn_fields(seg, 17, "order_callback_phone", repeating=True),
        "placer_field_1": _v(seg.get_field(18)),
        "placer_field_2": _v(seg.get_field(19)),
        "filler_field_1": _v(seg.get_field(20)),
        "filler_field_2": _v(seg.get_field(21)),
        "results_rpt_status_chng_datetime": _parse_dtm(seg.get_field(22)),
        "charge_to_practice": _v(seg.get_field(23)),
        "diagnostic_service_section": _v(seg.get_field(24)),
        "result_status": _v(seg.get_field(25)),
        "parent_result": _v(seg.get_field(26)),
        "quantity_timing": _v(seg.get_first_repetition(27)),
        **_xcn_fields(seg, 28, "result_copies_to"),
        "parent_placer_order_number": _v(seg.get_component(29, 1)),
        "transportation_mode": _v(seg.get_field(30)),
        **_cwe_array_fields(seg, 31, "reason_for_study"),
        "principal_result_interpreter": _v(seg.get_component(32, 1)),
        "assistant_result_interpreter": _v(seg.get_rep_component(33, 1, 1)),
        "technician": _v(seg.get_rep_component(34, 1, 1)),
        "transcriptionist": _v(seg.get_rep_component(35, 1, 1)),
        "scheduled_datetime": _parse_dtm(seg.get_field(36)),
        "number_of_sample_containers": _i(seg.get_field(37)),
        **_cwe_fields(seg, 38, "transport_logistics", repeating=True),
        **_cwe_fields(seg, 39, "collectors_comment", repeating=True),
        **_cwe_fields(seg, 40, "transport_arrangement_responsibility", repeating=False),
        "transport_arranged": _v(seg.get_field(41)),
        "escort_required": _v(seg.get_field(42)),
        **_cwe_fields(seg, 43, "planned_patient_transport_comment", repeating=True),
        **_cwe_fields(seg, 44, "procedure_code", repeating=False),
        **_cwe_fields(seg, 45, "procedure_code_modifier", repeating=True),
        **_cwe_fields(seg, 46, "placer_supplemental_service_info", repeating=True),
        **_cwe_fields(seg, 47, "filler_supplemental_service_info", repeating=True),
        **_cwe_fields(seg, 48, "medically_necessary_dup_proc_reason", repeating=False),
        **_cwe_fields(seg, 49, "result_handling", repeating=False),
        **_cwe_fields(seg, 50, "parent_universal_service_id", repeating=False),
        **_ei_fields(seg, 51, "observation_group", repeating=False),
        **_ei_fields(seg, 52, "parent_observation_group", repeating=False),
        **_cx_fields(seg, 53, "alternate_placer_order", repeating=False),
        "parent_order": _v(seg.get_component(54, 1)),
        "obr_action_code": _v(seg.get_field(55)),
    }


def _extract_obx(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "value_type": _v(seg.get_field(2)),
        **_cwe_fields(seg, 3, "observation_id", repeating=False),
        "observation_sub_id": _v(seg.get_field(4)),
        "observation_value": _v(seg.get_first_repetition(5)),
        **_cwe_fields(seg, 6, "units", repeating=False),
        "references_range": _v(seg.get_field(7)),
        **_cwe_array_fields(seg, 8, "interpretation_codes"),
        "probability": _v(seg.get_field(9)),
        **_s_array_fields(seg, 10, "nature_of_abnormal_test"),
        "observation_result_status": _v(seg.get_field(11)),
        "effective_date_of_ref_range": _parse_dtm(seg.get_field(12)),
        "user_defined_access_checks": _v(seg.get_field(13)),
        "datetime_of_observation": _parse_dtm(seg.get_field(14)),
        **_cwe_fields(seg, 15, "producers_id", repeating=False),
        **_xcn_fields(seg, 16, "responsible_observer"),
        **_cwe_fields(seg, 17, "observation_method", repeating=True),
        **_ei_fields(seg, 18, "equipment_instance_identifier", repeating=True),
        "datetime_of_analysis": _parse_dtm(seg.get_field(19)),
        **_cwe_fields(seg, 20, "observation_site", repeating=True),
        **_ei_fields(seg, 21, "observation_instance_identifier", repeating=False),
        **_cwe_fields(seg, 22, "mood_code", repeating=False),
        **_xon_fields(seg, 23, "performing_organization", repeating=False),
        **_xad_fields(seg, 24, "performing_org_address", repeating=False),
        **_xcn_fields(seg, 25, "performing_org_medical_director"),
        "patient_results_release_category": _v(seg.get_field(26)),
        **_cwe_fields(seg, 27, "root_cause", repeating=False),
        **_cwe_fields(seg, 28, "local_process_control", repeating=True),
        "observation_type": _v(seg.get_field(29)),
        "observation_sub_type": _v(seg.get_field(30)),
        "obx_action_code": _v(seg.get_field(31)),
        **_cwe_fields(seg, 32, "observation_value_absent_reason", repeating=False),
        "observation_related_specimen": _v(seg.get_field(33)),
    }


def _extract_al1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cwe_fields(seg, 2, "allergen_type_code", repeating=False),
        **_cwe_fields(seg, 3, "allergen_code", repeating=False),
        **_cwe_fields(seg, 4, "allergy_severity_code", repeating=False),
        **_cwe_fields(seg, 5, "allergy_reaction", repeating=True),
        "identification_date": _parse_dtm(seg.get_field(6)),
    }


def _extract_dg1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "diagnosis_coding_method": _v(seg.get_field(2)),
        **_cwe_fields(seg, 3, "diagnosis_code", repeating=False),
        "diagnosis_description": _v(seg.get_field(4)),
        "diagnosis_datetime": _parse_dtm(seg.get_field(5)),
        "diagnosis_type": _v(seg.get_field(6)),
        **_cwe_fields(seg, 7, "major_diagnostic_category", repeating=False),
        **_cwe_fields(seg, 8, "diagnostic_related_group", repeating=False),
        "drg_approval_indicator": _v(seg.get_field(9)),
        "drg_grouper_review_code": _v(seg.get_field(10)),
        **_cwe_fields(seg, 11, "outlier_type", repeating=False),
        "outlier_days": _i(seg.get_field(12)),
        "outlier_cost": _v(seg.get_field(13)),
        "grouper_version_and_type": _v(seg.get_field(14)),
        "diagnosis_priority": _i(seg.get_field(15)),
        **_xcn_fields(seg, 16, "diagnosing_clinician"),
        "diagnosis_classification": _v(seg.get_field(17)),
        "confidential_indicator": _v(seg.get_field(18)),
        "attestation_datetime": _parse_dtm(seg.get_field(19)),
        **_ei_fields(seg, 20, "diagnosis_identifier", repeating=False),
        "diagnosis_action_code": _v(seg.get_field(21)),
        **_ei_fields(seg, 22, "parent_diagnosis", repeating=False),
        **_cwe_fields(seg, 23, "drg_ccl_value_code", repeating=False),
        "drg_grouping_usage": _v(seg.get_field(24)),
        "drg_diagnosis_determination_status": _v(seg.get_field(25)),
        "present_on_admission_indicator": _v(seg.get_field(26)),
    }


def _extract_nk1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_xpn_array_fields(seg, 2, "nk_names"),
        "relationship": _v(seg.get_field(3)),
        **_cwe_fields(seg, 3, "relationship_code", repeating=False),
        **_xad_fields(seg, 4, "address"),
        **_xtn_fields(seg, 5, "phone_number"),
        **_xtn_fields(seg, 6, "business_phone"),
        **_cwe_fields(seg, 7, "contact_role", repeating=False),
        "start_date": _parse_dtm(seg.get_field(8)),
        "end_date": _parse_dtm(seg.get_field(9)),
        "job_title": _v(seg.get_field(10)),
        "job_code": _v(seg.get_component(11, 1)),
        **_cx_fields(seg, 12, "employee_number", repeating=False),
        **_xon_fields(seg, 13, "organization_name"),
        **_cwe_fields(seg, 14, "marital_status", repeating=False),
        **_cwe_fields(seg, 15, "administrative_sex", repeating=False),
        "date_of_birth": _parse_dtm(seg.get_field(16)),
        **_cwe_fields(seg, 17, "living_dependency", repeating=True),
        **_cwe_fields(seg, 18, "ambulatory_status", repeating=True),
        **_cwe_fields(seg, 19, "citizenship", repeating=True),
        **_cwe_fields(seg, 20, "primary_language", repeating=False),
        **_cwe_fields(seg, 21, "living_arrangement", repeating=False),
        **_cwe_fields(seg, 22, "publicity_code", repeating=False),
        "protection_indicator": _v(seg.get_field(23)),
        "student_indicator": _v(seg.get_field(24)),
        **_cwe_fields(seg, 25, "religion", repeating=False),
        **_xpn_array_fields(seg, 26, "mothers_maiden_names"),
        **_cwe_fields(seg, 27, "nationality", repeating=False),
        **_cwe_fields(seg, 28, "ethnic_group", repeating=True),
        **_cwe_fields(seg, 29, "contact_reason", repeating=True),
        **_xpn_array_fields(seg, 30, "contact_persons"),
        **_xtn_fields(seg, 31, "contact_person_telephone"),
        **_xad_fields(seg, 32, "contact_persons_address"),
        **_cx_fields(seg, 33, "associated_party_identifiers"),
        "job_status": _v(seg.get_field(34)),
        **_cwe_fields(seg, 35, "race", repeating=True),
        "handicap": _v(seg.get_field(36)),
        "contact_ssn": _v(seg.get_field(37)),
        "nk_birth_place": _v(seg.get_field(38)),
        "vip_indicator": _v(seg.get_field(39)),
        **_xtn_fields(seg, 40, "nk_telecommunication_info"),
        **_xtn_fields(seg, 41, "contact_telecommunication_info"),
    }


def _extract_evn(seg: HL7Segment) -> dict:
    return {
        "event_type_code": _v(seg.get_field(1)),
        "recorded_datetime": _parse_dtm(seg.get_field(2)),
        "date_time_planned_event": _parse_dtm(seg.get_field(3)),
        **_cwe_fields(seg, 4, "event_reason", repeating=False),
        **_xcn_fields(seg, 5, "operator"),
        "event_occurred": _parse_dtm(seg.get_field(6)),
        **_hd_fields(seg, 7, "event_facility", repeating=False),
    }


def _extract_pd1(seg: HL7Segment) -> dict:
    return {
        **_cwe_array_fields(seg, 1, "living_dependency"),
        **_cwe_fields(seg, 2, "living_arrangement", repeating=False),
        **_xon_fields(seg, 3, "patient_primary_facility"),
        **_xcn_fields(seg, 4, "patient_primary_care_provider"),
        **_cwe_fields(seg, 5, "student_indicator", repeating=False),
        **_cwe_fields(seg, 6, "handicap", repeating=False),
        **_cwe_fields(seg, 7, "living_will_code", repeating=False),
        **_cwe_fields(seg, 8, "organ_donor_code", repeating=False),
        "separate_bill": _v(seg.get_field(9)),
        **_cx_fields(seg, 10, "duplicate_patient"),
        **_cwe_fields(seg, 11, "publicity_code", repeating=False),
        "protection_indicator": _v(seg.get_field(12)),
        "protection_indicator_effective_date": _v(seg.get_field(13)),
        **_xon_fields(seg, 14, "place_of_worship"),
        **_cwe_fields(seg, 15, "advance_directive_code", repeating=True),
        **_cwe_fields(seg, 16, "immunization_registry_status", repeating=False),
        "immunization_registry_status_effective_date": _v(seg.get_field(17)),
        "publicity_code_effective_date": _v(seg.get_field(18)),
        **_cwe_fields(seg, 19, "military_branch", repeating=False),
        **_cwe_fields(seg, 20, "military_rank_grade", repeating=False),
        **_cwe_fields(seg, 21, "military_status", repeating=False),
        "advance_directive_last_verified_date": _v(seg.get_field(22)),
        "retirement_date": _v(seg.get_field(23)),
    }


def _extract_pv2(seg: HL7Segment) -> dict:
    return {
        "prior_pending_location": _v(seg.get_field(1)),
        **_cwe_fields(seg, 2, "accommodation_code", repeating=False),
        **_cwe_fields(seg, 3, "admit_reason", repeating=False),
        **_cwe_fields(seg, 4, "transfer_reason", repeating=False),
        "patient_valuables": _v(seg.get_first_repetition(5)),
        "patient_valuables_location": _v(seg.get_field(6)),
        **_cwe_array_fields(seg, 7, "visit_user_code"),
        "expected_admit_datetime": _parse_dtm(seg.get_field(8)),
        "expected_discharge_datetime": _parse_dtm(seg.get_field(9)),
        "estimated_length_of_inpatient_stay": _i(seg.get_field(10)),
        "actual_length_of_inpatient_stay": _i(seg.get_field(11)),
        "visit_description": _v(seg.get_field(12)),
        **_xcn_fields(seg, 13, "referral_source"),
        "previous_service_date": _v(seg.get_field(14)),
        "employment_illness_related_indicator": _v(seg.get_field(15)),
        "purge_status_code": _v(seg.get_field(16)),
        "purge_status_date": _v(seg.get_field(17)),
        **_cwe_fields(seg, 18, "special_program_code", repeating=False),
        "retention_indicator": _v(seg.get_field(19)),
        "expected_number_of_insurance_plans": _i(seg.get_field(20)),
        **_cwe_fields(seg, 21, "visit_publicity_code", repeating=False),
        "visit_protection_indicator": _v(seg.get_field(22)),
        **_xon_fields(seg, 23, "clinic_organization"),
        **_cwe_fields(seg, 24, "patient_status_code", repeating=False),
        **_cwe_fields(seg, 25, "visit_priority_code", repeating=False),
        "previous_treatment_date": _v(seg.get_field(26)),
        "expected_discharge_disposition": _v(seg.get_field(27)),
        "signature_on_file_date": _v(seg.get_field(28)),
        "first_similar_illness_date": _v(seg.get_field(29)),
        **_cwe_fields(seg, 30, "patient_charge_adjustment_code", repeating=False),
        **_cwe_fields(seg, 31, "recurring_service_code", repeating=False),
        "billing_media_code": _v(seg.get_field(32)),
        "expected_surgery_datetime": _parse_dtm(seg.get_field(33)),
        "military_partnership_code": _v(seg.get_field(34)),
        "military_non_availability_code": _v(seg.get_field(35)),
        "newborn_baby_indicator": _v(seg.get_field(36)),
        "baby_detained_indicator": _v(seg.get_field(37)),
        **_cwe_fields(seg, 38, "mode_of_arrival_code", repeating=False),
        **_cwe_fields(seg, 39, "recreational_drug_use_code", repeating=True),
        **_cwe_fields(seg, 40, "admission_level_of_care_code", repeating=False),
        **_cwe_fields(seg, 41, "precaution_code", repeating=True),
        **_cwe_fields(seg, 42, "patient_condition_code", repeating=False),
        **_cwe_fields(seg, 43, "living_will_code_pv2", repeating=False),
        **_cwe_fields(seg, 44, "organ_donor_code_pv2", repeating=False),
        **_cwe_fields(seg, 45, "advance_directive_code_pv2", repeating=True),
        "patient_status_effective_date": _v(seg.get_field(46)),
        "expected_loa_return_datetime": _parse_dtm(seg.get_field(47)),
        "expected_preadmission_testing_datetime": _parse_dtm(seg.get_field(48)),
        **_cwe_array_fields(seg, 49, "notify_clergy_code"),
        "advance_directive_last_verified_date_pv2": _v(seg.get_field(50)),
    }


def _extract_mrg(seg: HL7Segment) -> dict:
    return {
        **_cx_fields(seg, 1, "prior_patient_id", repeating=False),
        "prior_alternate_patient_id": _v(seg.get_first_repetition(2)),
        **_cx_fields(seg, 3, "prior_patient_account_number", repeating=False),
        **_cx_fields(seg, 4, "prior_patient_id_mrg4", repeating=False),
        **_cx_fields(seg, 5, "prior_visit_number", repeating=False),
        **_cx_fields(seg, 6, "prior_alternate_visit_id", repeating=False),
        **_xpn_array_fields(seg, 7, "prior_patient_names"),
    }


def _extract_iam(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cwe_fields(seg, 2, "allergen_type_code", repeating=False),
        **_cwe_fields(seg, 3, "allergen_code", repeating=False),
        **_cwe_fields(seg, 4, "allergy_severity_code", repeating=False),
        **_cwe_fields(seg, 5, "allergy_reaction", repeating=True),
        **_cwe_fields(seg, 6, "allergy_action_code", repeating=False),
        **_ei_fields(seg, 7, "allergy_unique_identifier", repeating=False),
        "action_reason": _v(seg.get_field(8)),
        **_cwe_fields(seg, 9, "sensitivity_to_causative_agent_code", repeating=False),
        **_cwe_fields(seg, 10, "allergen_group_code", repeating=False),
        "onset_date": _v(seg.get_field(11)),
        "onset_date_text": _v(seg.get_field(12)),
        "reported_datetime": _parse_dtm(seg.get_field(13)),
        **_xcn_fields(seg, 14, "reported_by"),
        **_cwe_fields(seg, 15, "relationship_to_patient_code", repeating=False),
        **_cwe_fields(seg, 16, "alert_device_code", repeating=False),
        **_cwe_fields(seg, 17, "allergy_clinical_status_code", repeating=False),
        **_xcn_fields(seg, 18, "statused_by_person", repeating=False),
        **_xon_fields(seg, 19, "statused_by_organization", repeating=False),
        "statused_at_datetime": _parse_dtm(seg.get_field(20)),
        **_xcn_fields(seg, 21, "inactivated_by_person", repeating=False),
        "inactivated_datetime": _parse_dtm(seg.get_field(22)),
        **_xcn_fields(seg, 23, "initially_recorded_by_person", repeating=False),
        "initially_recorded_datetime": _parse_dtm(seg.get_field(24)),
        **_xcn_fields(seg, 25, "modified_by_person", repeating=False),
        "modified_datetime": _parse_dtm(seg.get_field(26)),
        **_cwe_fields(seg, 27, "clinician_identified_code", repeating=False),
        **_xon_fields(seg, 28, "initially_recorded_by_organization", repeating=False),
        **_xon_fields(seg, 29, "modified_by_organization", repeating=False),
        **_xon_fields(seg, 30, "inactivated_by_organization", repeating=False),
    }


def _extract_pr1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "procedure_coding_method": _v(seg.get_field(2)),
        **_cwe_fields(seg, 3, "procedure_code", repeating=False),
        "procedure_description": _v(seg.get_field(4)),
        "procedure_datetime": _parse_dtm(seg.get_field(5)),
        "procedure_functional_type": _v(seg.get_field(6)),
        "procedure_minutes": _i(seg.get_field(7)),
        **_xcn_fields(seg, 8, "anesthesiologist"),
        "anesthesia_code": _v(seg.get_field(9)),
        "anesthesia_minutes": _i(seg.get_field(10)),
        **_xcn_fields(seg, 11, "surgeon"),
        **_xcn_fields(seg, 12, "procedure_practitioner"),
        **_cwe_fields(seg, 13, "consent_code", repeating=False),
        "procedure_priority": _v(seg.get_field(14)),
        **_cwe_fields(seg, 15, "associated_diagnosis_code", repeating=False),
        **_cwe_fields(seg, 16, "procedure_code_modifier", repeating=True),
        "procedure_drg_type": _v(seg.get_field(17)),
        **_cwe_fields(seg, 18, "tissue_type_code", repeating=True),
        **_ei_fields(seg, 19, "procedure_identifier", repeating=False),
        "procedure_action_code": _v(seg.get_field(20)),
        **_cwe_fields(seg, 21, "drg_procedure_determination_status", repeating=False),
        **_cwe_fields(seg, 22, "drg_procedure_relevance", repeating=False),
        "treating_organizational_unit": _v(seg.get_field(23)),
        "respiratory_within_surgery": _v(seg.get_field(24)),
        **_ei_fields(seg, 25, "parent_procedure_id", repeating=False),
    }


def _extract_orc(seg: HL7Segment) -> dict:
    return {
        "order_control": _v(seg.get_field(1)),
        **_ei_fields(seg, 2, "placer_order_number", repeating=False),
        **_ei_fields(seg, 3, "filler_order_number", repeating=False),
        **_ei_fields(seg, 4, "placer_group_number", repeating=False),
        "order_status": _v(seg.get_field(5)),
        "response_flag": _v(seg.get_field(6)),
        "quantity_timing": _v(seg.get_first_repetition(7)),
        "parent_order": _v(seg.get_field(8)),
        "datetime_of_transaction": _parse_dtm(seg.get_field(9)),
        **_xcn_fields(seg, 10, "entered_by"),
        **_xcn_fields(seg, 11, "verified_by"),
        **_xcn_fields(seg, 12, "ordering_provider"),
        "enterers_location": _v(seg.get_field(13)),
        **_xtn_fields(seg, 14, "call_back_phone", repeating=True),
        "order_effective_datetime": _parse_dtm(seg.get_field(15)),
        **_cwe_fields(seg, 16, "order_control_code_reason", repeating=False),
        **_cwe_fields(seg, 17, "entering_organization", repeating=False),
        **_cwe_fields(seg, 18, "entering_device", repeating=False),
        **_xcn_fields(seg, 19, "action_by"),
        **_cwe_fields(seg, 20, "advanced_beneficiary_notice_code", repeating=False),
        **_xon_fields(seg, 21, "ordering_facility_name"),
        **_xad_fields(seg, 22, "ordering_facility_address"),
        **_xtn_fields(seg, 23, "ordering_facility_phone"),
        **_xad_fields(seg, 24, "ordering_provider_address"),
        **_cwe_fields(seg, 25, "order_status_modifier", repeating=False),
        **_cwe_fields(seg, 26, "abn_override_reason", repeating=False),
        "fillers_expected_availability_datetime": _parse_dtm(seg.get_field(27)),
        **_cwe_fields(seg, 28, "confidentiality_code", repeating=False),
        **_cwe_fields(seg, 29, "order_type", repeating=False),
        **_cwe_fields(seg, 30, "enterer_authorization_mode", repeating=False),
        **_cwe_fields(seg, 31, "parent_universal_service_id", repeating=False),
        "advanced_beneficiary_notice_date": _v(seg.get_field(32)),
        **_cx_fields(seg, 33, "alternate_placer_order_number", repeating=False),
        **_ei_fields(seg, 34, "order_workflow_profile", repeating=False),
        "orc_action_code": _v(seg.get_field(35)),
        "order_status_date_range": _v(seg.get_field(36)),
        "order_creation_datetime": _parse_dtm(seg.get_field(37)),
        **_ei_fields(seg, 38, "filler_order_group_number", repeating=False),
    }


def _extract_nte(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "source_of_comment": _v(seg.get_field(2)),
        "comment": _v(seg.get_first_repetition(3)),
        **_cwe_fields(seg, 4, "comment_type", repeating=False),
        **_xcn_fields(seg, 5, "entered_by", repeating=False),
        "entered_datetime": _parse_dtm(seg.get_field(6)),
        "effective_start_date": _parse_dtm(seg.get_field(7)),
        "expiration_date": _parse_dtm(seg.get_field(8)),
        **_cwe_fields(seg, 9, "coded_comment", repeating=False),
    }


def _extract_spm(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_ei_fields(seg, 2, "specimen_id", repeating=False),
        "specimen_parent_ids": _v(seg.get_rep_component(3, 1, 1)),
        **_cwe_fields(seg, 4, "specimen_type", repeating=False),
        **_cwe_fields(seg, 5, "specimen_type_modifier", repeating=True),
        **_cwe_fields(seg, 6, "specimen_additives", repeating=True),
        **_cwe_fields(seg, 7, "specimen_collection_method", repeating=False),
        **_cwe_fields(seg, 8, "specimen_source_site", repeating=False),
        **_cwe_fields(seg, 9, "specimen_source_site_modifier", repeating=True),
        **_cwe_fields(seg, 10, "specimen_collection_site", repeating=False),
        **_cwe_fields(seg, 11, "specimen_role", repeating=True),
        "specimen_collection_amount": _v(seg.get_component(12, 1)),
        "grouped_specimen_count": _i(seg.get_field(13)),
        "specimen_description": _v(seg.get_first_repetition(14)),
        **_cwe_fields(seg, 15, "specimen_handling_code", repeating=True),
        **_cwe_fields(seg, 16, "specimen_risk_code", repeating=True),
        "specimen_collection_datetime": _v(seg.get_component(17, 1)),
        "specimen_received_datetime": _parse_dtm(seg.get_field(18)),
        "specimen_expiration_datetime": _parse_dtm(seg.get_field(19)),
        "specimen_availability": _v(seg.get_field(20)),
        **_cwe_fields(seg, 21, "specimen_reject_reason", repeating=True),
        **_cwe_fields(seg, 22, "specimen_quality", repeating=False),
        **_cwe_fields(seg, 23, "specimen_appropriateness", repeating=False),
        **_cwe_fields(seg, 24, "specimen_condition", repeating=True),
        "specimen_current_quantity": _v(seg.get_component(25, 1)),
        "number_of_specimen_containers": _i(seg.get_field(26)),
        **_cwe_fields(seg, 27, "container_type", repeating=False),
        **_cwe_fields(seg, 28, "container_condition", repeating=False),
        **_cwe_fields(seg, 29, "specimen_child_role", repeating=False),
        **_cx_fields(seg, 30, "accession_id", repeating=False),
        **_cx_fields(seg, 31, "other_specimen_id", repeating=False),
        **_ei_fields(seg, 32, "shipment_id", repeating=False),
        "culture_start_datetime": _parse_dtm(seg.get_field(33)),
        "culture_final_datetime": _parse_dtm(seg.get_field(34)),
        "spm_action_code": _v(seg.get_field(35)),
    }


def _extract_in1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cwe_fields(seg, 2, "insurance_plan", repeating=False),
        **_xon_fields(seg, 3, "insurance_company"),
        **_xon_fields(seg, 4, "insurance_company_name"),
        **_xad_fields(seg, 5, "insurance_company_address"),
        **_xpn_array_fields(seg, 6, "insurance_co_contacts"),
        **_xtn_fields(seg, 7, "insurance_co_phone_number"),
        "group_number": _v(seg.get_field(8)),
        **_xon_fields(seg, 9, "group_name"),
        **_xon_fields(seg, 10, "insureds_group_emp"),
        **_xon_fields(seg, 11, "insureds_group_emp_name"),
        "plan_effective_date": _v(seg.get_field(12)),
        "plan_expiration_date": _v(seg.get_field(13)),
        "authorization_information": _v(seg.get_component(14, 1)),
        "plan_type": _v(seg.get_field(15)),
        **_xpn_array_fields(seg, 16, "insured_names"),
        **_cwe_fields(seg, 17, "insureds_relationship_to_patient", repeating=False),
        "insureds_date_of_birth": _parse_dtm(seg.get_field(18)),
        **_xad_fields(seg, 19, "insureds_address"),
        "assignment_of_benefits": _v(seg.get_field(20)),
        "coordination_of_benefits": _v(seg.get_field(21)),
        "coord_of_ben_priority": _v(seg.get_field(22)),
        "notice_of_admission_flag": _v(seg.get_field(23)),
        "notice_of_admission_date": _v(seg.get_field(24)),
        "report_of_eligibility_flag": _v(seg.get_field(25)),
        "report_of_eligibility_date": _v(seg.get_field(26)),
        "release_information_code": _v(seg.get_field(27)),
        "pre_admit_cert": _v(seg.get_field(28)),
        "verification_datetime": _parse_dtm(seg.get_field(29)),
        **_xcn_fields(seg, 30, "verification_by"),
        "type_of_agreement_code": _v(seg.get_field(31)),
        "billing_status": _v(seg.get_field(32)),
        "lifetime_reserve_days": _i(seg.get_field(33)),
        "delay_before_lr_day": _i(seg.get_field(34)),
        "company_plan_code": _v(seg.get_field(35)),
        "policy_number": _v(seg.get_field(36)),
        "policy_deductible": _v(seg.get_component(37, 1)),
        "policy_limit_amount": _v(seg.get_component(38, 1)),
        "policy_limit_days": _i(seg.get_field(39)),
        "room_rate_semi_private": _v(seg.get_component(40, 1)),
        "room_rate_private": _v(seg.get_component(41, 1)),
        **_cwe_fields(seg, 42, "insureds_employment_status", repeating=False),
        **_cwe_fields(seg, 43, "insureds_administrative_sex", repeating=False),
        **_xad_fields(seg, 44, "insureds_employers_address"),
        "verification_status": _v(seg.get_field(45)),
        "prior_insurance_plan_id": _v(seg.get_field(46)),
        "coverage_type": _v(seg.get_field(47)),
        "handicap": _v(seg.get_field(48)),
        **_cx_fields(seg, 49, "insureds_id_number"),
        "signature_code": _v(seg.get_field(50)),
        "signature_code_date": _v(seg.get_field(51)),
        "insureds_birth_place": _v(seg.get_field(52)),
        "vip_indicator": _v(seg.get_field(53)),
        "external_health_plan_identifiers": _v(seg.get_component(54, 1)),
        "insurance_action_code": _v(seg.get_field(55)),
    }


def _extract_gt1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cx_fields(seg, 2, "guarantor_number"),
        **_xpn_array_fields(seg, 3, "guarantor_names"),
        **_xpn_array_fields(seg, 4, "guarantor_spouse_names"),
        **_xad_fields(seg, 5, "guarantor_address"),
        **_xtn_fields(seg, 6, "guarantor_ph_num_home"),
        **_xtn_fields(seg, 7, "guarantor_ph_num_business"),
        "guarantor_date_of_birth": _parse_dtm(seg.get_field(8)),
        **_cwe_fields(seg, 9, "guarantor_administrative_sex", repeating=False),
        "guarantor_type": _v(seg.get_field(10)),
        **_cwe_fields(seg, 11, "guarantor_relationship", repeating=False),
        "guarantor_ssn": _v(seg.get_field(12)),
        "guarantor_date_begin": _v(seg.get_field(13)),
        "guarantor_date_end": _v(seg.get_field(14)),
        "guarantor_priority": _i(seg.get_field(15)),
        **_xpn_array_fields(seg, 16, "guarantor_employer_names"),
        **_xad_fields(seg, 17, "guarantor_employer_address"),
        **_xtn_fields(seg, 18, "guarantor_employer_phone_number"),
        **_cx_fields(seg, 19, "guarantor_employee_id_number"),
        "guarantor_employment_status": _v(seg.get_field(20)),
        **_xon_fields(seg, 21, "guarantor_organization_name"),
        "guarantor_billing_hold_flag": _v(seg.get_field(22)),
        **_cwe_fields(seg, 23, "guarantor_credit_rating_code", repeating=False),
        "guarantor_death_date_and_time": _parse_dtm(seg.get_field(24)),
        "guarantor_death_flag": _v(seg.get_field(25)),
        **_cwe_fields(seg, 26, "guarantor_charge_adjustment_code", repeating=False),
        "guarantor_household_annual_income": _v(seg.get_component(27, 1)),
        "guarantor_household_size": _i(seg.get_field(28)),
        **_cx_fields(seg, 29, "guarantor_employer_id_number"),
        **_cwe_fields(seg, 30, "guarantor_marital_status_code", repeating=False),
        "guarantor_hire_effective_date": _v(seg.get_field(31)),
        "employment_stop_date": _v(seg.get_field(32)),
        "living_dependency": _v(seg.get_field(33)),
        "ambulatory_status": _v(seg.get_first_repetition(34)),
        **_cwe_fields(seg, 35, "citizenship", repeating=True),
        **_cwe_fields(seg, 36, "primary_language", repeating=False),
        "living_arrangement": _v(seg.get_field(37)),
        **_cwe_fields(seg, 38, "publicity_code", repeating=False),
        "protection_indicator": _v(seg.get_field(39)),
        "student_indicator": _v(seg.get_field(40)),
        **_cwe_fields(seg, 41, "religion", repeating=False),
        **_xpn_array_fields(seg, 42, "gt1_mothers_maiden_names"),
        **_cwe_fields(seg, 43, "nationality", repeating=False),
        **_cwe_fields(seg, 44, "ethnic_group", repeating=True),
        **_xpn_array_fields(seg, 45, "gt1_contact_persons"),
        **_xtn_fields(seg, 46, "contact_persons_telephone_number"),
        **_cwe_fields(seg, 47, "contact_reason", repeating=False),
        "contact_relationship": _v(seg.get_field(48)),
        "job_title": _v(seg.get_field(49)),
        **_cwe_fields(seg, 50, "job_code_class", repeating=False),
        **_xon_fields(seg, 51, "guarantor_employers_org_name"),
        "handicap": _v(seg.get_field(52)),
        "job_status": _v(seg.get_field(53)),
        "guarantor_financial_class": _v(seg.get_component(54, 1)),
        **_cwe_fields(seg, 55, "guarantor_race", repeating=True),
        "guarantor_birth_place": _v(seg.get_field(56)),
        "vip_indicator": _v(seg.get_field(57)),
    }


def _extract_ft1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "transaction_id": _v(seg.get_field(2)),
        "transaction_batch_id": _v(seg.get_field(3)),
        "transaction_date": _v(seg.get_component(4, 1)),
        "transaction_posting_date": _parse_dtm(seg.get_field(5)),
        "transaction_type": _v(seg.get_field(6)),
        **_cwe_fields(seg, 7, "transaction_code", repeating=False),
        "transaction_description": _v(seg.get_field(8)),
        "transaction_description_alt": _v(seg.get_field(9)),
        "transaction_quantity": _i(seg.get_field(10)),
        "transaction_amount_extended": _v(seg.get_component(11, 1)),
        "transaction_amount_unit": _v(seg.get_component(12, 1)),
        **_cwe_fields(seg, 13, "department_code", repeating=False),
        "insurance_plan_id": _v(seg.get_component(14, 1)),
        "insurance_amount": _v(seg.get_component(15, 1)),
        "assigned_patient_location": _v(seg.get_field(16)),
        "fee_schedule": _v(seg.get_field(17)),
        "patient_type": _v(seg.get_field(18)),
        **_cwe_fields(seg, 19, "diagnosis_code", repeating=True),
        **_xcn_fields(seg, 20, "performed_by"),
        **_xcn_fields(seg, 21, "ordered_by"),
        "unit_cost": _v(seg.get_component(22, 1)),
        **_ei_fields(seg, 23, "filler_order_number", repeating=False),
        **_xcn_fields(seg, 24, "entered_by"),
        **_cwe_fields(seg, 25, "ft1_procedure_code", repeating=False),
        **_cwe_fields(seg, 26, "ft1_procedure_code_modifier", repeating=True),
        "advanced_beneficiary_notice_code": _v(seg.get_component(27, 1)),
        "medically_necessary_dup_proc_reason": _v(seg.get_component(28, 1)),
        "ndc_code": _v(seg.get_component(29, 1)),
        "payment_reference_id": _v(seg.get_component(30, 1)),
        "transaction_reference_key": _v(seg.get_first_repetition(31)),
        "performing_facility": _v(seg.get_component(32, 1)),
        "ordering_facility": _v(seg.get_component(33, 1)),
        "item_number": _v(seg.get_component(34, 1)),
        "model_number": _v(seg.get_field(35)),
        "special_processing_code": _v(seg.get_component(36, 1)),
        "clinic_code": _v(seg.get_component(37, 1)),
        "referral_number": _v(seg.get_component(38, 1)),
        "authorization_number": _v(seg.get_component(39, 1)),
        "service_provider_taxonomy_code": _v(seg.get_component(40, 1)),
        "revenue_code": _v(seg.get_component(41, 1)),
        "prescription_number": _v(seg.get_field(42)),
        "ndc_qty_and_uom": _v(seg.get_field(43)),
        "dme_certificate_of_medical_necessity_transmission_code": _v(seg.get_component(44, 1)),
        "dme_certification_type_code": _v(seg.get_component(45, 1)),
        "dme_duration_value": _v(seg.get_field(46)),
        "dme_certification_revision_date": _v(seg.get_field(47)),
        "dme_initial_certification_date": _v(seg.get_field(48)),
        "dme_last_certification_date": _v(seg.get_field(49)),
        "dme_length_of_medical_necessity_days": _v(seg.get_field(50)),
        "dme_rental_price": _v(seg.get_field(51)),
        "dme_purchase_price": _v(seg.get_field(52)),
        "dme_frequency_code": _v(seg.get_component(53, 1)),
        "dme_certification_condition_indicator": _v(seg.get_field(54)),
        "dme_condition_indicator_code": _v(seg.get_component(55, 1)),
        "service_reason_code": _v(seg.get_component(56, 1)),
    }


def _extract_rxa(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "administration_sub_id_counter": _i(seg.get_field(2)),
        "datetime_start_of_administration": _parse_dtm(seg.get_field(3)),
        "datetime_end_of_administration": _parse_dtm(seg.get_field(4)),
        **_cwe_fields(seg, 5, "administered_code", repeating=False),
        "administered_amount": _v(seg.get_field(6)),
        **_cwe_fields(seg, 7, "administered_units", repeating=False),
        **_cwe_fields(seg, 8, "administered_dosage_form", repeating=False),
        **_cwe_fields(seg, 9, "administration_notes", repeating=True),
        **_xcn_fields(seg, 10, "administering_provider"),
        "administered_at_location": _v(seg.get_field(11)),
        "administered_per_time_unit": _v(seg.get_field(12)),
        "administered_strength": _v(seg.get_field(13)),
        "administered_strength_units": _v(seg.get_component(14, 1)),
        "substance_lot_number": _v(seg.get_first_repetition(15)),
        "substance_expiration_date": _parse_dtm(seg.get_first_repetition(16)),
        **_cwe_fields(seg, 17, "substance_manufacturer_name", repeating=True),
        **_cwe_fields(seg, 18, "substance_treatment_refusal_reason", repeating=True),
        **_cwe_fields(seg, 19, "indication", repeating=True),
        "completion_status": _v(seg.get_field(20)),
        "action_code_rxa": _v(seg.get_field(21)),
        "system_entry_datetime": _parse_dtm(seg.get_field(22)),
        "administered_drug_strength_volume": _v(seg.get_field(23)),
        "administered_drug_strength_volume_units": _v(seg.get_component(24, 1)),
        "administered_barcode_identifier": _v(seg.get_component(25, 1)),
        "pharmacy_order_type": _v(seg.get_field(26)),
        "administer_at": _v(seg.get_field(27)),
        "administered_at_address": _v(seg.get_field(28)),
        "administered_tag_identifier": _v(seg.get_component(29, 1)),
    }


def _extract_sch(seg: HL7Segment) -> dict:
    return {
        **_ei_fields(seg, 1, "placer_appointment_id", repeating=False),
        **_ei_fields(seg, 2, "filler_appointment_id", repeating=False),
        "occurrence_number": _i(seg.get_field(3)),
        **_ei_fields(seg, 4, "placer_group_number", repeating=False),
        **_cwe_fields(seg, 5, "schedule_id", repeating=False),
        **_cwe_fields(seg, 6, "event_reason", repeating=False),
        **_cwe_fields(seg, 7, "appointment_reason", repeating=False),
        **_cwe_fields(seg, 8, "appointment_type", repeating=False),
        "appointment_duration": _i(seg.get_field(9)),
        **_cwe_fields(seg, 10, "appointment_duration_units", repeating=False),
        "appointment_timing_quantity": _v(seg.get_first_repetition(11)),
        **_xcn_fields(seg, 12, "placer_contact_person"),
        **_xtn_fields(seg, 13, "placer_contact_phone_number", repeating=False),
        **_xad_fields(seg, 14, "placer_contact_address"),
        "placer_contact_location": _v(seg.get_field(15)),
        **_xcn_fields(seg, 16, "filler_contact_person"),
        **_xtn_fields(seg, 17, "filler_contact_phone_number", repeating=False),
        **_xad_fields(seg, 18, "filler_contact_address"),
        "filler_contact_location": _v(seg.get_field(19)),
        **_xcn_fields(seg, 20, "entered_by_person"),
        **_xtn_fields(seg, 21, "entered_by_phone_number", repeating=False),
        "entered_by_location": _v(seg.get_field(22)),
        **_ei_fields(seg, 23, "parent_placer_appointment_id", repeating=False),
        **_ei_fields(seg, 24, "parent_filler_appointment_id", repeating=False),
        **_cwe_fields(seg, 25, "filler_status_code", repeating=False),
        **_ei_fields(seg, 26, "sch_placer_order_number", repeating=True),
        **_ei_fields(seg, 27, "sch_filler_order_number", repeating=True),
        **_ei_fields(seg, 28, "alternate_placer_order_group_number", repeating=False),
    }


def _extract_txa(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "document_type": _v(seg.get_field(2)),
        "document_content_presentation": _v(seg.get_field(3)),
        "activity_datetime": _parse_dtm(seg.get_field(4)),
        **_xcn_fields(seg, 5, "primary_activity_provider"),
        "origination_datetime": _parse_dtm(seg.get_field(6)),
        "transcription_datetime": _parse_dtm(seg.get_field(7)),
        "edit_datetime": _parse_dtm(seg.get_first_repetition(8)),
        **_xcn_fields(seg, 9, "originator"),
        **_xcn_fields(seg, 10, "assigned_document_authenticator"),
        **_xcn_fields(seg, 11, "transcriptionist"),
        **_ei_fields(seg, 12, "unique_document_number", repeating=False),
        **_ei_fields(seg, 13, "parent_document_number", repeating=False),
        **_ei_fields(seg, 14, "placer_order_number", repeating=True),
        **_ei_fields(seg, 15, "filler_order_number", repeating=False),
        "unique_document_file_name": _v(seg.get_field(16)),
        "document_completion_status": _v(seg.get_field(17)),
        "document_confidentiality_status": _v(seg.get_field(18)),
        "document_availability_status": _v(seg.get_field(19)),
        "document_storage_status": _v(seg.get_field(20)),
        "document_change_reason": _v(seg.get_field(21)),
        "authentication_person_time_stamp": _v(seg.get_first_repetition(22)),
        **_xcn_fields(seg, 23, "distributed_copies"),
        **_cwe_fields(seg, 24, "folder_assignment", repeating=False),
        "document_title": _v(seg.get_field(25)),
        "agreed_due_datetime": _parse_dtm(seg.get_field(26)),
        **_hd_fields(seg, 27, "creating_facility", repeating=False),
        **_cwe_fields(seg, 28, "creating_specialty", repeating=False),
    }


def _extract_generic(seg: HL7Segment) -> dict:
    """Fallback extractor for Z-segments and unknown segment types."""
    return {"segment_type": seg.segment_type} | {
        f"field_{i}": _v(seg.get_field(i)) for i in range(1, 26)
    }


_EXTRACTORS = {
    "msh": _extract_msh,
    "evn": _extract_evn,
    "pid": _extract_pid,
    "pd1": _extract_pd1,
    "pv1": _extract_pv1,
    "pv2": _extract_pv2,
    "nk1": _extract_nk1,
    "mrg": _extract_mrg,
    "al1": _extract_al1,
    "iam": _extract_iam,
    "dg1": _extract_dg1,
    "pr1": _extract_pr1,
    "orc": _extract_orc,
    "obr": _extract_obr,
    "obx": _extract_obx,
    "nte": _extract_nte,
    "spm": _extract_spm,
    "in1": _extract_in1,
    "gt1": _extract_gt1,
    "ft1": _extract_ft1,
    "rxa": _extract_rxa,
    "sch": _extract_sch,
    "txa": _extract_txa,
}


# ---------------------------------------------------------------------------
# Multi-message splitter
# ---------------------------------------------------------------------------


def _split_messages(text: str) -> list[str]:
    """Split an HL7 batch into individual message strings.

    Each message starts with an MSH line.  FHS/BHS/BTS/FTS batch-envelope
    segments are skipped.
    """
    normalised = text.strip().replace("\r\n", "\r").replace("\n", "\r")
    lines = normalised.split("\r")

    messages: list[str] = []
    current: list[str] = []
    _ENVELOPE = {"FHS", "BHS", "BTS", "FTS"}

    for line in lines:
        if not line.strip():
            continue
        seg_type = line[:3].upper()
        if seg_type in _ENVELOPE:
            continue
        if seg_type == "MSH":
            if current:
                messages.append("\r".join(current))
            current = [line]
        else:
            current.append(line)

    if current:
        messages.append("\r".join(current))

    return messages


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class HL7V2LakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for HL7 v2 messages.

    Supports two source modes controlled by the ``source_type`` option:

    * ``gcp`` (default) — fetches from a Google Cloud Healthcare API HL7v2 store.
    * ``delta`` — reads from a Bronze Delta table containing pre-loaded HL7
      messages with columns ``data`` (raw text), ``createTime``, and optionally ``name``.

    Each HL7 segment type is a separate table.  Incremental loading is driven
    by ``createTime`` using a sliding time-window.

    GCP mode connection options:
        project_id, location, dataset_id, hl7v2_store_id, service_account_json

    Delta mode connection options:
        delta_table_name (fully-qualified catalog.schema.table)
        delta_query_mode (str): ``"preload"`` (default) loads the entire table
            into memory at init — fast for small tables.  ``"per_window"``
            issues a live SQL query per micro-batch window — scales to
            arbitrarily large tables with no memory overhead.

    Table options (both modes):
        segment_type (str): Override segment type for custom/Z-segments.
        window_seconds (str): Duration of the sliding time-window in seconds
            (default 86400).  Smaller values produce smaller batches.
        start_timestamp (str): RFC3339 timestamp to start reading from when no
            prior offset exists and auto-discovery is not possible.
        max_records_per_batch (str): Hard upper bound on rows yielded by a
            single ``read_table`` call.  Once this many output rows are
            produced, the iterator stops early and the cursor advances only
            up to the last source ``createTime`` actually consumed, so the
            next batch resumes from there.  Use this to bound memory when
            ``window_seconds`` is large or messages are dense.
    """

    _GCP_REQUIRED_KEYS = ("project_id", "location", "dataset_id", "hl7v2_store_id", "service_account_json")

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._source_type = options.get("source_type", "gcp").lower()
        self._init_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._oldest_create_time: str | None = None

        if self._source_type == "delta":
            self._init_delta(options)
        elif self._source_type == "gcp":
            self._init_gcp(options)
        else:
            raise ValueError(
                f"Unsupported source_type '{self._source_type}'. "
                "Must be 'gcp' or 'delta'."
            )

    def _init_gcp(self, options: dict[str, str]) -> None:
        import requests
        from google.auth.transport import requests as google_auth_requests
        from google.oauth2 import service_account as google_sa

        for key in self._GCP_REQUIRED_KEYS:
            if key not in options:
                raise ValueError(f"'{key}' is required in connector options for source_type 'gcp'.")

        self._base_url = (
            f"https://healthcare.googleapis.com/v1"
            f"/projects/{options['project_id']}"
            f"/locations/{options['location']}"
            f"/datasets/{options['dataset_id']}"
            f"/hl7V2Stores/{options['hl7v2_store_id']}/messages"
        )

        raw_sa = options["service_account_json"]
        sa_info = self._parse_service_account_json(raw_sa)
        self._creds = google_sa.Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self._google_request = google_auth_requests.Request()
        self._session = requests.Session()
        self._creds.refresh(self._google_request)

    _DELTA_REQUIRED_KEYS = ("delta_table_name", "databricks_host", "databricks_token", "sql_warehouse_id")

    def _init_delta(self, options: dict[str, str]) -> None:
        for key in self._DELTA_REQUIRED_KEYS:
            if key not in options:
                raise ValueError(f"'{key}' is required in connector options for source_type 'delta'.")
        self._delta_table = options["delta_table_name"]
        self._dbx_host = options["databricks_host"]
        self._dbx_token = options["databricks_token"]
        self._sql_warehouse_id = options["sql_warehouse_id"]
        self._delta_query_mode = options.get("delta_query_mode", "preload").lower()

        if self._delta_query_mode == "per_window":
            self._delta_cache = None
            self._delta_preload_error = None
            self._ws_client = self._create_workspace_client()
        elif self._delta_query_mode == "preload":
            self._ws_client = None
            self._delta_cache: list[dict] | None = None
            self._delta_preload_error: str | None = None
            self._preload_delta()
        else:
            raise ValueError(
                f"Unsupported delta_query_mode '{self._delta_query_mode}'. "
                "Must be 'preload' (default) or 'per_window'."
            )

    @staticmethod
    def _parse_service_account_json(raw: str | dict) -> dict:
        """Parse a service-account JSON value that may arrive in several forms.

        UC connection options can deliver the value as:
        * A ``dict`` (already parsed by the framework)
        * A well-formed JSON string
        * A double-serialised JSON string (``'"{\\"type\\":…}"'``)
        * A base64-encoded JSON string (some UI flows encode binary-like values)
        """
        if isinstance(raw, dict):
            return raw

        raw = raw.strip()

        # Attempt 1: direct parse
        try:
            parsed = json.loads(raw, strict=False)
            if isinstance(parsed, dict):
                return parsed
            # Double-serialised — json.loads returned a string, parse again
            if isinstance(parsed, str):
                inner = json.loads(parsed, strict=False)
                if isinstance(inner, dict):
                    return inner
        except (json.JSONDecodeError, TypeError):
            pass

        # Attempt 2: base64-decode then parse
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            parsed = json.loads(decoded, strict=False)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        # Attempt 3: find a JSON object embedded in the string
        brace_start = raw.find("{")
        brace_end = raw.rfind("}")
        if brace_start != -1 and brace_end > brace_start:
            try:
                parsed = json.loads(raw[brace_start:brace_end + 1], strict=False)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass

        preview = raw[:120] + ("…" if len(raw) > 120 else "")
        raise ValueError(
            f"Could not parse 'service_account_json' as a JSON object. "
            f"Received value (first 120 chars): {preview!r}. "
            f"Ensure the entire contents of the GCP service account JSON key "
            f"file are pasted into the connection parameter — it should start "
            f"with '{{' and end with '}}'."
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_headers(self) -> dict[str, str]:
        if not self._creds.valid:
            self._creds.refresh(self._google_request)
        return {"Authorization": f"Bearer {self._creds.token}"}

    def _api_get(self, params: dict[str, str]) -> dict:
        """GET the messages endpoint with retry on transient errors."""
        backoff = _INITIAL_BACKOFF
        last_resp = None
        for attempt in range(_MAX_RETRIES):
            resp = self._session.get(
                self._base_url,
                headers=self._get_headers(),
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )
            last_resp = resp
            if resp.status_code not in _RETRIABLE_STATUS_CODES:
                resp.raise_for_status()
                return resp.json()
            if attempt < _MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        last_resp.raise_for_status()
        return last_resp.json()

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SEGMENT_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name)
        return get_schema(segment_type)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).lower()
        is_known = segment_type in SEGMENT_SCHEMAS
        is_single = segment_type in _SINGLE_SEGMENT_TABLES
        if is_known and not is_single:
            pks = ["message_id", "set_id"]
        else:
            pks = ["message_id"]
        return {
            "ingestion_type": "append",
            "cursor_field": "create_time",
            "primary_keys": pks,
            "description": TABLE_DESCRIPTIONS.get(segment_type, ""),
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding time-window incremental read.

        Fetches all messages whose ``createTime`` falls in
        ``(since, since + window_seconds]``, parses them, and returns rows
        for the requested segment type.  The cursor advances to the window
        end regardless of whether data was found, ensuring forward progress.

        Works identically for both GCP and Delta source modes — only the
        fetch method differs.
        """
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).upper()

        since = start_offset.get("cursor") if start_offset is not None else None
        if not since:
            since = table_options.get("start_timestamp")
        if not since:
            since = self._peek_oldest_create_time()
        if not since:
            print(
                f"[HL7v2] read_table({table_name}): no start cursor resolved "
                f"(start_offset={start_offset}, source_type={self._source_type}). "
                f"Returning empty."
            )
            return iter([]), start_offset or {}

        if since >= self._init_ts:
            return iter([]), start_offset or {}

        window_seconds = int(table_options.get("window_seconds", str(_DEFAULT_WINDOW_SECONDS)))
        max_records_raw = table_options.get("max_records_per_batch")
        max_records = int(max_records_raw) if max_records_raw else None

        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        window_end_dt = since_dt + timedelta(seconds=window_seconds)
        window_end = min(
            window_end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            self._init_ts,
        )

        if self._source_type == "delta":
            api_messages = self._fetch_messages_from_delta(since, window_end)
        else:
            api_messages = self._fetch_messages_in_window(since, window_end)

        records = self._parse_api_messages(
            api_messages, segment_type, decode_base64=(self._source_type != "delta")
        )

        # Admission control: cap rows yielded per batch.  Cuts only at
        # message boundaries (one HL7 message can produce many rows when
        # the requested segment repeats — e.g. several OBX per ORU), so
        # rows from the same source message stay together.  The cursor
        # rewinds to the createTime of the last fully-consumed message;
        # the next batch resumes from there because the GCP / Delta
        # filter is strict ``createTime > since``.
        if max_records is not None and len(records) > max_records:
            cut = max_records
            # Walk forward until create_time changes — keep all rows
            # belonging to the message that straddles ``max_records``.
            straddle_ct = records[cut - 1].get("create_time")
            while cut < len(records) and records[cut].get("create_time") == straddle_ct:
                cut += 1
            records = records[:cut]
            last_ct = records[-1].get("create_time") if records else None
            if last_ct and last_ct < window_end:
                end_offset = {"cursor": last_ct}
                if start_offset is not None and start_offset == end_offset:
                    return iter([]), start_offset
                return iter(records), end_offset

        end_offset = {"cursor": window_end}
        if start_offset is not None and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str, table_options: dict) -> None:
        if table_name not in SEGMENT_TABLES and "segment_type" not in table_options:
            raise ValueError(
                f"Unknown table '{table_name}'. "
                f"Supported tables: {SEGMENT_TABLES}. "
                "For custom/Z-segments, provide 'segment_type' in table_options."
            )

    def _peek_oldest_create_time(self) -> str | None:
        """Auto-discover the earliest createTime by fetching the first message.

        The result is cached for the lifetime of this connector instance since
        the oldest message never changes once discovered.
        """
        if self._oldest_create_time is not None:
            return self._oldest_create_time

        if self._source_type == "delta":
            self._oldest_create_time = self._peek_oldest_create_time_delta()
            return self._oldest_create_time

        body = self._api_get({
            "view": "FULL",
            "pageSize": "1",
            "orderBy": "sendTime asc",
        })
        messages = body.get("hl7V2Messages", [])
        if messages:
            ts = messages[0].get("createTime")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                dt -= timedelta(seconds=1)
                ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            self._oldest_create_time = ts
        return self._oldest_create_time

    def _fetch_messages_in_window(self, since: str, until: str) -> list[dict]:
        """Fetch all API messages with createTime in (since, until]."""
        filter_str = f'createTime > "{since}" AND createTime <= "{until}"'
        messages: list[dict] = []
        page_token: str | None = None

        while True:
            params: dict[str, str] = {
                "view": "FULL",
                "pageSize": str(_MAX_PAGE_SIZE),
                "filter": filter_str,
                "orderBy": "sendTime asc",
            }
            if page_token:
                params["pageToken"] = page_token

            body = self._api_get(params)
            batch = body.get("hl7V2Messages", [])
            messages.extend(batch)

            page_token = body.get("nextPageToken")
            if not page_token:
                break

        return messages

    def _create_workspace_client(self):
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(host=self._dbx_host, token=self._dbx_token)

    def _execute_delta_sql(self, stmt: str) -> list[list[str]]:
        """Execute a SQL statement against the Delta table via the Statement Execution API.

        Returns the raw ``data_array`` (list of rows, each row a list of strings).
        """
        w = self._ws_client or self._create_workspace_client()
        result = w.statement_execution.execute_statement(
            warehouse_id=self._sql_warehouse_id,
            statement=stmt,
            wait_timeout="50s",
        )
        while result.status and result.status.state.value in ("PENDING", "RUNNING"):
            time.sleep(1)
            result = w.statement_execution.get_statement(result.statement_id)

        state = getattr(getattr(result, "status", None), "state", None)
        if state and state.value not in ("SUCCEEDED",):
            error_msg = getattr(getattr(result, "status", None), "error", None)
            raise RuntimeError(
                f"Delta SQL statement ended in state '{state.value}'. "
                f"Error: {error_msg}. Statement: {stmt[:200]}"
            )
        if result.result is None:
            raise RuntimeError(
                f"Delta SQL statement returned no result object. "
                f"State: {state}. Statement: {stmt[:200]}"
            )
        return result.result.data_array or []

    def _preload_delta(self) -> None:
        """Pre-load Delta table data via the Databricks SQL Statement Execution API.

        SparkSession is unavailable in both ``DataSource.__init__`` and the
        streaming reader subprocess, so we use the Databricks SDK with explicit
        credentials (``databricks_host``, ``databricks_token``) provided as
        connection parameters — the same pattern used by the GCP mode with
        ``service_account_json``.
        """
        try:
            stmt = (
                f"SELECT data, "
                f"date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS createTime, "
                f"name "
                f"FROM {self._delta_table} "
                f"ORDER BY createTime"
            )
            data_array = self._execute_delta_sql(stmt)
            rows = []
            for row in data_array:
                rows.append({
                    "data": row[0] or "",
                    "createTime": row[1] or "",
                    "name": row[2] if len(row) > 2 else "",
                })
            self._delta_cache = rows
            print(
                f"[HL7v2 Delta] Preloaded {len(rows)} message(s) from "
                f"{self._delta_table}"
            )
        except Exception as exc:
            self._delta_cache = None
            self._delta_preload_error = f"{type(exc).__name__}: {exc}"
            print(
                f"[HL7v2 Delta] ERROR — _preload_delta failed for "
                f"table={self._delta_table}, "
                f"host={self._dbx_host}, "
                f"warehouse={self._sql_warehouse_id}: "
                f"{self._delta_preload_error}"
            )
            raise RuntimeError(
                f"Failed to preload Delta table '{self._delta_table}'. "
                f"Verify that 'databricks_host', 'databricks_token', and "
                f"'sql_warehouse_id' in the connection are valid and the token "
                f"has not expired. Error: {self._delta_preload_error}"
            ) from exc

    def _fetch_messages_from_delta(self, since: str, until: str) -> list[dict]:
        """Fetch messages with createTime in (since, until].

        In ``preload`` mode, filters the in-memory cache.
        In ``per_window`` mode, issues a live SQL query scoped to the window.

        Returns a list of dicts with the same shape as the GCP API response
        (keys: ``data``, ``createTime``, ``name``) so that ``_parse_api_messages``
        works unchanged.
        """
        if self._delta_query_mode == "per_window":
            return self._fetch_messages_from_delta_live(since, until)

        if self._delta_cache is None:
            raise RuntimeError(
                f"Delta cache was not populated. "
                f"Preload error: {self._delta_preload_error}. "
                f"Table: {self._delta_table}"
            )

        return [
            row for row in self._delta_cache
            if since < str(row.get("createTime", "")) <= until
        ]

    def _fetch_messages_from_delta_live(self, since: str, until: str) -> list[dict]:
        """Issue a live SQL query for messages in (since, until]."""
        stmt = (
            f"SELECT data, "
            f"date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS createTime, "
            f"name "
            f"FROM {self._delta_table} "
            f"WHERE date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") > '{since}' "
            f"  AND date_format(createTime, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") <= '{until}' "
            f"ORDER BY createTime"
        )
        data_array = self._execute_delta_sql(stmt)
        return [
            {
                "data": row[0] or "",
                "createTime": row[1] or "",
                "name": row[2] if len(row) > 2 else "",
            }
            for row in data_array
        ]

    def _peek_oldest_create_time_delta(self) -> str | None:
        """Return the earliest createTime from the Delta table.

        In ``preload`` mode, reads from the in-memory cache.
        In ``per_window`` mode, issues a ``SELECT MIN(createTime)`` query.
        """
        if self._delta_query_mode == "per_window":
            return self._peek_oldest_create_time_delta_live()

        if self._delta_cache is None:
            print(
                f"[HL7v2 Delta] _peek_oldest_create_time_delta: cache is None. "
                f"Preload error: {self._delta_preload_error}"
            )
            return None
        if len(self._delta_cache) == 0:
            print(
                f"[HL7v2 Delta] _peek_oldest_create_time_delta: cache is empty "
                f"(0 rows returned from {self._delta_table})"
            )
            return None

        first_ts = str(self._delta_cache[0].get("createTime", ""))
        if not first_ts:
            return None

        dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
        dt -= timedelta(seconds=1)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _peek_oldest_create_time_delta_live(self) -> str | None:
        """Discover the earliest createTime via a live SQL query."""
        stmt = (
            f"SELECT date_format(MIN(createTime), \"yyyy-MM-dd'T'HH:mm:ss'Z'\") "
            f"FROM {self._delta_table}"
        )
        data_array = self._execute_delta_sql(stmt)
        if not data_array or not data_array[0] or not data_array[0][0]:
            return None

        first_ts = data_array[0][0]
        dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
        dt -= timedelta(seconds=1)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _parse_api_messages(
        self, api_messages: list[dict], segment_type: str, *, decode_base64: bool = True
    ) -> list[dict]:
        """Decode, parse, and extract rows from message payloads.

        Args:
            decode_base64: When True (GCP mode), the ``data`` field is
                base64-decoded.  When False (Delta mode), ``data`` is
                treated as raw HL7 text.
        """
        records: list[dict] = []

        for msg_data in api_messages:
            send_time = msg_data.get("sendTime", "")
            create_time = msg_data.get("createTime", "") or send_time
            raw_data = msg_data.get("data", "")
            if not raw_data:
                continue
            if decode_base64:
                raw_hl7 = base64.b64decode(raw_data).decode("utf-8", errors="replace")
            else:
                raw_hl7 = raw_data
            source_name = msg_data.get("name", "")

            for msg_text in _split_messages(raw_hl7):
                msg: HL7Message | None = parse_message(msg_text)
                if msg is None:
                    continue
                msh = msg.get_segment("MSH")
                meta = _metadata(msh, source_name, send_time, create_time)

                if segment_type == "MSH":
                    if msh is not None:
                        records.append(meta | _extract_msh(msh) | {"raw_segment": msh.raw_line})
                else:
                    extractor = _EXTRACTORS.get(segment_type.lower(), _extract_generic)
                    for idx, seg in enumerate(msg.get_segments(segment_type), start=1):
                        row = meta | extractor(seg) | {"raw_segment": seg.raw_line}
                        if segment_type.lower() not in _SINGLE_SEGMENT_TABLES and "set_id" not in row:
                            row["set_id"] = idx
                        records.append(row)

        return records
