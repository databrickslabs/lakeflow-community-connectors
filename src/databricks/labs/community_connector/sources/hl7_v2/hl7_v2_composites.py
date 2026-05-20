"""HL7 v2 composite-type helpers and null-safe primitives.

These helpers extract every component (and sub-component, where present)
of HL7 v2 composite data types into individually named keys.  They are
used by the per-segment extractors in :mod:`hl7_v2_extractors` and exposed
at module level so per-segment unit tests can import them directly.

Naming convention: ``_xpn_fields(seg, n, prefix)`` produces keys like
``{prefix}_family_name``, ``{prefix}_given_name``, ... .  Array variants
(``_xpn_array_fields``) produce a single key holding a list of dicts so
all repetitions are preserved.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    HL7Segment,
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
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

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
        def gc(i, _p=parts):
            return _v(_p[i - 1]) if len(_p) >= i else None
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
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

    def gsc(comp, sub):
        if repeating:
            return _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
        return _v(seg.get_sub_component(field_n, comp, sub))

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
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

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
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

    return {
        f"{prefix}": gc(1),
        f"{prefix}_universal_id": gc(2),
        f"{prefix}_universal_id_type": gc(3),
    }


def _ei_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = False
) -> dict:
    """EI (Entity Identifier) — 4 components."""
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

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
        def gc(i, _p=parts):
            return _v(_p[i - 1]) if len(_p) >= i else None
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


def _xtn_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """XTN (Extended Telecommunication Number) — all repetitions as a list of dicts.

    Walks every ~-separated repetition and decomposes its 18 components.
    Sub-components of components 15/16/17 are flattened to their .1 value
    (matching the flat ``_xtn_fields`` helper), since downstream callers
    have not historically needed the deeper structure.
    """
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        def gc(i, _p=parts):
            return _v(_p[i - 1]) if len(_p) >= i else None
        def gsc(i, sub, _p=parts):
            if len(_p) < i or not _p[i - 1]:
                return None
            subs = _p[i - 1].split(seg._enc.sub_comp_sep)
            return _v(subs[sub - 1]) if len(subs) >= sub else None
        result.append({
            "number": gc(1),
            "use_code": gc(2),
            "equipment_type": gc(3),
            "communication_address": gc(4),
            "country_code": gc(5),
            "area_code": gc(6),
            "local_number": gc(7),
            "extension": gc(8),
            "any_text": gc(9),
            "extension_prefix": gc(10),
            "speed_dial_code": gc(11),
            "unformatted_number": gc(12),
            "effective_start_date": gc(13),
            "expiration_date": gc(14),
            "expiration_reason": gsc(15, 1),
            "protection_code": gsc(16, 1),
            "shared_telecom_id": gsc(17, 1),
            "preference_order": gc(18),
        })
    return {column_name: result if result else None}


def _xcn_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """XCN (Extended Composite ID Number and Name) — all repetitions as a list of dicts."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        def gc(i, _p=parts):
            return _v(_p[i - 1]) if len(_p) >= i else None
        def gsc(i, sub, _p=parts):
            if len(_p) < i or not _p[i - 1]:
                return None
            subs = _p[i - 1].split(seg._enc.sub_comp_sep)
            return _v(subs[sub - 1]) if len(subs) >= sub else None
        result.append({
            "id": gc(1),
            "family_name": gc(2),
            "given_name": gc(3),
            "middle_name": gc(4),
            "suffix": gc(5),
            "prefix": gc(6),
            "degree": gc(7),
            "source_table": gc(8),
            "assigning_authority": gsc(9, 1),
            "assigning_authority_universal_id": gsc(9, 2),
            "assigning_authority_universal_id_type": gsc(9, 3),
            "name_type_code": gc(10),
            "check_digit": gc(11),
            "check_digit_scheme": gc(12),
            "identifier_type_code": gc(13),
            "assigning_facility": gsc(14, 1),
            "assigning_facility_universal_id": gsc(14, 2),
            "assigning_facility_universal_id_type": gsc(14, 3),
            "name_representation_code": gc(15),
            "name_assembly_order": gc(18),
            "effective_date": gc(19),
            "expiration_date": gc(20),
            "professional_suffix": gc(21),
        })
    return {column_name: result if result else None}


def _xon_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """XON (Extended Composite Name and Number for Organizations) — all repetitions as a list of dicts."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        def gc(i, _p=parts):
            return _v(_p[i - 1]) if len(_p) >= i else None
        def gsc(i, sub, _p=parts):
            if len(_p) < i or not _p[i - 1]:
                return None
            subs = _p[i - 1].split(seg._enc.sub_comp_sep)
            return _v(subs[sub - 1]) if len(subs) >= sub else None
        result.append({
            "name": gc(1),
            "type_code": gc(2),
            "id": gc(3),
            "check_digit": gc(4),
            "check_digit_scheme": gc(5),
            "assigning_authority": gsc(6, 1),
            "assigning_authority_universal_id": gsc(6, 2),
            "assigning_authority_universal_id_type": gsc(6, 3),
            "id_type_code": gc(7),
            "assigning_facility": gsc(8, 1),
            "assigning_facility_universal_id": gsc(8, 2),
            "assigning_facility_universal_id_type": gsc(8, 3),
            "name_rep_code": gc(9),
            "identifier": gc(10),
        })
    return {column_name: result if result else None}


def _cx_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """CX (Extended Composite ID with Check Digit) — all repetitions as a list of dicts."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        def gc(i, _p=parts):
            return _v(_p[i - 1]) if len(_p) >= i else None
        def gsc(i, sub, _p=parts):
            if len(_p) < i or not _p[i - 1]:
                return None
            subs = _p[i - 1].split(seg._enc.sub_comp_sep)
            return _v(subs[sub - 1]) if len(subs) >= sub else None
        result.append({
            "id": gc(1),
            "check_digit": gc(2),
            "check_digit_scheme": gc(3),
            "assigning_authority": gsc(4, 1),
            "assigning_authority_universal_id": gsc(4, 2),
            "assigning_authority_universal_id_type": gsc(4, 3),
            "type_code": gc(5),
            "assigning_facility": gsc(6, 1),
            "assigning_facility_universal_id": gsc(6, 2),
            "assigning_facility_universal_id_type": gsc(6, 3),
            "effective_date": gc(7),
            "expiration_date": gc(8),
            "assigning_jurisdiction": gc(9),
            "assigning_agency": gc(10),
            "security_check": gc(11),
            "security_check_scheme": gc(12),
        })
    return {column_name: result if result else None}


def _xad_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """XAD (Extended Address) — all repetitions as a list of dicts."""
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)
        def gc(i, _p=parts):
            return _v(_p[i - 1]) if len(_p) >= i else None
        def gsc(i, sub, _p=parts):
            if len(_p) < i or not _p[i - 1]:
                return None
            subs = _p[i - 1].split(seg._enc.sub_comp_sep)
            return _v(subs[sub - 1]) if len(subs) >= sub else None
        result.append({
            "street": gc(1),
            "other_designation": gc(2),
            "city": gc(3),
            "state": gc(4),
            "zip": gc(5),
            "country": gc(6),
            "type": gc(7),
            "other_geographic": gc(8),
            "county_parish_code": gsc(9, 1),
            "county_parish_text": gsc(9, 2),
            "county_parish_coding_system": gsc(9, 3),
            "census_tract": gsc(10, 1),
            "census_tract_text": gsc(10, 2),
            "census_tract_coding_system": gsc(10, 3),
            "representation_code": gc(11),
            "effective_date": gc(13),
            "expiration_date": gc(14),
            "expiration_reason": gsc(15, 1),
            "expiration_reason_text": gsc(15, 2),
            "expiration_reason_coding_system": gsc(15, 3),
            "temporary_indicator": gc(16),
            "bad_address_indicator": gc(17),
            "usage": gc(18),
            "addressee": gc(19),
            "comment": gc(20),
            "preference_order": gc(21),
            "protection_code": gsc(22, 1),
            "protection_code_text": gsc(22, 2),
            "protection_code_coding_system": gsc(22, 3),
            "identifier": gsc(23, 1),
        })
    return {column_name: result if result else None}


def _eip_array_fields(
    seg: HL7Segment, field_n: int, column_name: str
) -> dict:
    """EIP (Entity Identifier Pair) — all repetitions as a list of {parent, child} dicts.

    Each EIP has two components (EIP.1 = parent EI, EIP.2 = child EI), each
    further decomposable into 4 sub-components via the `&` separator.
    """
    raw = seg.get_field(field_n)
    if not raw:
        return {column_name: None}
    reps = raw.split(seg._enc.rep_sep)
    result = []
    for rep in reps:
        if not rep:
            continue
        parts = rep.split(seg._enc.comp_sep)

        def _ei_from(comp_value: str | None) -> dict | None:
            if not comp_value:
                return None
            subs = comp_value.split(seg._enc.sub_comp_sep)
            return {
                "entity_identifier": _v(subs[0]) if len(subs) > 0 else None,
                "namespace_id": _v(subs[1]) if len(subs) > 1 else None,
                "universal_id": _v(subs[2]) if len(subs) > 2 else None,
                "universal_id_type": _v(subs[3]) if len(subs) > 3 else None,
            }

        result.append({
            "parent": _ei_from(parts[0] if len(parts) > 0 else None),
            "child": _ei_from(parts[1] if len(parts) > 1 else None),
        })
    return {column_name: result if result else None}


def _xon_fields(
    seg: HL7Segment, field_n: int, prefix: str, *, repeating: bool = True
) -> dict:
    """XON (Extended Composite Name and Number for Organizations) — 10 components."""
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

    def gsc(comp, sub):
        if repeating:
            return _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
        return _v(seg.get_sub_component(field_n, comp, sub))

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
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

    def gsc(comp, sub):
        if repeating:
            return _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
        return _v(seg.get_sub_component(field_n, comp, sub))

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
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

    def gsc(comp, sub):
        if repeating:
            return _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
        return _v(seg.get_sub_component(field_n, comp, sub))

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
    def gc(comp):
        if repeating:
            return _v(seg.get_rep_component(field_n, 1, comp))
        return _v(seg.get_component(field_n, comp))

    def gsc(comp, sub):
        if repeating:
            return _v(seg.get_rep_sub_component(field_n, 1, comp, sub))
        return _v(seg.get_sub_component(field_n, comp, sub))

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
