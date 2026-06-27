"""Spark StructType schemas for HL7 v2 segment tables.

Every schema follows the HL7 v2.9 specification — the latest version, which
is itself a superset of all prior versions (v2.1–v2.8).  Fields absent in a
given message version are returned as ``None`` by the connector, and Spark
receives them as ``null``.

All wire-format values are strings; only ``set_id`` / sequence-number
fields are typed as ``LongType`` because they are guaranteed-numeric.

Every table includes seven common metadata columns:
    message_id        — MSH-10 (join key across all segment tables)
    message_timestamp — MSH-7  (raw HL7 DTM string)
    hl7_version       — MSH-12 (e.g. "2.5.1")
    source_file       — GCP Healthcare API resource name of the source message
    send_time         — RFC3339 sendTime from the API (original HL7 message send time)
    create_time       — RFC3339 createTime from the GCP API (incremental cursor)
    raw_segment       — raw pipe-delimited segment text for lossless recovery
"""
# Schema definitions use visually-aligned ``_s(name, comment)`` rows so that
# columns line up with the HL7 v2.9 spec tables for easy cross-reference;
# breaking each row at 100 chars would destroy that alignment for negligible
# readability gain. Same convention as ``sap_successfactors/table_schemas.py``
# and ``fhir/profiles/base_r4.py``.
# pylint: disable=line-too-long,too-many-lines

from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _s(name: str, comment: str = "") -> StructField:
    """Nullable StringType field.

    The *comment* parameter is used for inline documentation only and is NOT
    stored in StructField metadata.  Arrow metadata mismatches between the JVM
    and Python data-source workers cause ARROW_TYPE_MISMATCH errors when
    StructField metadata is present.
    """
    return StructField(name, StringType(), nullable=True)


def _int_field(name: str, comment: str = "") -> StructField:
    """Nullable LongType field (see ``_s`` for metadata rationale)."""
    return StructField(name, LongType(), nullable=True)


def _ts(name: str, comment: str = "") -> StructField:
    """Nullable StringType field for ISO-8601 timestamps.

    Uses StringType to avoid Arrow timestamp-timezone mismatches.
    Downstream consumers can CAST to TIMESTAMP in SQL when needed.
    """
    return StructField(name, StringType(), nullable=True)


def _pk_s(name: str, comment: str = "") -> StructField:
    """NOT NULL StringType field used as (part of) a primary key.

    NOT NULL is required for Unity Catalog PRIMARY KEY constraints.
    """
    return StructField(name, StringType(), nullable=False)


def _pk_int_field(name: str, comment: str = "") -> StructField:
    """NOT NULL LongType field used as (part of) a composite primary key."""
    return StructField(name, LongType(), nullable=False)


_XPN_STRUCT = StructType([
    StructField("family_name", StringType(), nullable=True),
    StructField("given_name", StringType(), nullable=True),
    StructField("middle_name", StringType(), nullable=True),
    StructField("suffix", StringType(), nullable=True),
    StructField("prefix", StringType(), nullable=True),
    StructField("degree", StringType(), nullable=True),
    StructField("name_type_code", StringType(), nullable=True),
    StructField("name_representation_code", StringType(), nullable=True),
    StructField("name_context", StringType(), nullable=True),
    StructField("name_assembly_order", StringType(), nullable=True),
    StructField("name_effective_date", StringType(), nullable=True),
    StructField("name_expiration_date", StringType(), nullable=True),
    StructField("professional_suffix", StringType(), nullable=True),
    StructField("called_by", StringType(), nullable=True),
])


_CWE_STRUCT = StructType([
    StructField("code", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("coding_system", StringType(), nullable=True),
    StructField("alt_code", StringType(), nullable=True),
    StructField("alt_text", StringType(), nullable=True),
    StructField("alt_coding_system", StringType(), nullable=True),
    StructField("coding_system_version", StringType(), nullable=True),
    StructField("alt_coding_system_version", StringType(), nullable=True),
    StructField("original_text", StringType(), nullable=True),
])


_HD_STRUCT = StructType([
    StructField("namespace_id", StringType(), nullable=True),
    StructField("universal_id", StringType(), nullable=True),
    StructField("universal_id_type", StringType(), nullable=True),
])


_XTN_STRUCT = StructType([
    StructField("number", StringType(), nullable=True),
    StructField("use_code", StringType(), nullable=True),
    StructField("equipment_type", StringType(), nullable=True),
    StructField("communication_address", StringType(), nullable=True),
    StructField("country_code", StringType(), nullable=True),
    StructField("area_code", StringType(), nullable=True),
    StructField("local_number", StringType(), nullable=True),
    StructField("extension", StringType(), nullable=True),
    StructField("any_text", StringType(), nullable=True),
    StructField("extension_prefix", StringType(), nullable=True),
    StructField("speed_dial_code", StringType(), nullable=True),
    StructField("unformatted_number", StringType(), nullable=True),
    StructField("effective_start_date", StringType(), nullable=True),
    StructField("expiration_date", StringType(), nullable=True),
    StructField("expiration_reason", StringType(), nullable=True),
    StructField("protection_code", StringType(), nullable=True),
    StructField("shared_telecom_id", StringType(), nullable=True),
    StructField("preference_order", StringType(), nullable=True),
])


_XCN_STRUCT = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("family_name", StringType(), nullable=True),
    StructField("given_name", StringType(), nullable=True),
    StructField("middle_name", StringType(), nullable=True),
    StructField("suffix", StringType(), nullable=True),
    StructField("prefix", StringType(), nullable=True),
    StructField("degree", StringType(), nullable=True),
    StructField("source_table", StringType(), nullable=True),
    StructField("assigning_authority", StringType(), nullable=True),
    StructField("assigning_authority_universal_id", StringType(), nullable=True),
    StructField("assigning_authority_universal_id_type", StringType(), nullable=True),
    StructField("name_type_code", StringType(), nullable=True),
    StructField("check_digit", StringType(), nullable=True),
    StructField("check_digit_scheme", StringType(), nullable=True),
    StructField("identifier_type_code", StringType(), nullable=True),
    StructField("assigning_facility", StringType(), nullable=True),
    StructField("assigning_facility_universal_id", StringType(), nullable=True),
    StructField("assigning_facility_universal_id_type", StringType(), nullable=True),
    StructField("name_representation_code", StringType(), nullable=True),
    StructField("name_assembly_order", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
    StructField("expiration_date", StringType(), nullable=True),
    StructField("professional_suffix", StringType(), nullable=True),
])


_XON_STRUCT = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("type_code", StringType(), nullable=True),
    StructField("id", StringType(), nullable=True),
    StructField("check_digit", StringType(), nullable=True),
    StructField("check_digit_scheme", StringType(), nullable=True),
    StructField("assigning_authority", StringType(), nullable=True),
    StructField("assigning_authority_universal_id", StringType(), nullable=True),
    StructField("assigning_authority_universal_id_type", StringType(), nullable=True),
    StructField("id_type_code", StringType(), nullable=True),
    StructField("assigning_facility", StringType(), nullable=True),
    StructField("assigning_facility_universal_id", StringType(), nullable=True),
    StructField("assigning_facility_universal_id_type", StringType(), nullable=True),
    StructField("name_rep_code", StringType(), nullable=True),
    StructField("identifier", StringType(), nullable=True),
])


_CX_STRUCT = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("check_digit", StringType(), nullable=True),
    StructField("check_digit_scheme", StringType(), nullable=True),
    StructField("assigning_authority", StringType(), nullable=True),
    StructField("assigning_authority_universal_id", StringType(), nullable=True),
    StructField("assigning_authority_universal_id_type", StringType(), nullable=True),
    StructField("type_code", StringType(), nullable=True),
    StructField("assigning_facility", StringType(), nullable=True),
    StructField("assigning_facility_universal_id", StringType(), nullable=True),
    StructField("assigning_facility_universal_id_type", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
    StructField("expiration_date", StringType(), nullable=True),
    StructField("assigning_jurisdiction", StringType(), nullable=True),
    StructField("assigning_agency", StringType(), nullable=True),
    StructField("security_check", StringType(), nullable=True),
    StructField("security_check_scheme", StringType(), nullable=True),
])


_XAD_STRUCT = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("other_designation", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("zip", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("type", StringType(), nullable=True),
    StructField("other_geographic", StringType(), nullable=True),
    StructField("county_parish_code", StringType(), nullable=True),
    StructField("county_parish_text", StringType(), nullable=True),
    StructField("county_parish_coding_system", StringType(), nullable=True),
    StructField("census_tract", StringType(), nullable=True),
    StructField("census_tract_text", StringType(), nullable=True),
    StructField("census_tract_coding_system", StringType(), nullable=True),
    StructField("representation_code", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
    StructField("expiration_date", StringType(), nullable=True),
    StructField("expiration_reason", StringType(), nullable=True),
    StructField("expiration_reason_text", StringType(), nullable=True),
    StructField("expiration_reason_coding_system", StringType(), nullable=True),
    StructField("temporary_indicator", StringType(), nullable=True),
    StructField("bad_address_indicator", StringType(), nullable=True),
    StructField("usage", StringType(), nullable=True),
    StructField("addressee", StringType(), nullable=True),
    StructField("comment", StringType(), nullable=True),
    StructField("preference_order", StringType(), nullable=True),
    StructField("protection_code", StringType(), nullable=True),
    StructField("protection_code_text", StringType(), nullable=True),
    StructField("protection_code_coding_system", StringType(), nullable=True),
    StructField("identifier", StringType(), nullable=True),
])


# EIP (Entity Identifier Pair) — parent and child EI separated by `&`. Used for
# OBX-33 observation-related specimen ID and ORC-8 parent order. Modeled as a
# nested struct so consumers can address each EI's 4 components individually.
_EI_INNER_STRUCT = StructType([
    StructField("entity_identifier", StringType(), nullable=True),
    StructField("namespace_id", StringType(), nullable=True),
    StructField("universal_id", StringType(), nullable=True),
    StructField("universal_id_type", StringType(), nullable=True),
])
_EIP_STRUCT = StructType([
    StructField("placer_assigned_identifier", _EI_INNER_STRUCT, nullable=True),
    StructField("filler_assigned_identifier", _EI_INNER_STRUCT, nullable=True),
])


_PL_STRUCT = StructType([
    StructField("point_of_care", StringType(), nullable=True),
    StructField("room", StringType(), nullable=True),
    StructField("bed", StringType(), nullable=True),
    StructField("facility", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("type", StringType(), nullable=True),
    StructField("building", StringType(), nullable=True),
    StructField("floor", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("comprehensive_id", StringType(), nullable=True),
    StructField("assigning_authority", StringType(), nullable=True),
])


_FC_STRUCT = StructType([
    StructField("code", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("coding_system", StringType(), nullable=True),
    StructField("alt_code", StringType(), nullable=True),
    StructField("alt_text", StringType(), nullable=True),
    StructField("alt_coding_system", StringType(), nullable=True),
    StructField("coding_system_version", StringType(), nullable=True),
    StructField("alt_coding_system_version", StringType(), nullable=True),
    StructField("original_text", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
])


_TQ_STRUCT = StructType([
    StructField("quantity",                StringType(), nullable=True),  # TQ.1.1 (CQ quantity value, NM)
    StructField("quantity_units",          StringType(), nullable=True),  # TQ.1.2 (CQ units, CWE.1)
    StructField("interval_repeat_pattern", StringType(), nullable=True),  # TQ.2.1 (RI repeat pattern)
    StructField("interval_explicit_time",  StringType(), nullable=True),  # TQ.2.2 (RI explicit time)
    StructField("duration",                StringType(), nullable=True),  # TQ.3 (ST)
    StructField("start_datetime",          StringType(), nullable=True),  # TQ.4 (TS, ISO-8601)
    StructField("end_datetime",            StringType(), nullable=True),  # TQ.5 (TS, ISO-8601)
    StructField("priority",                StringType(), nullable=True),  # TQ.6 (ID)
    StructField("condition",               StringType(), nullable=True),  # TQ.7 (ST)
    StructField("text",                    StringType(), nullable=True),  # TQ.8 (TX)
    StructField("conjunction",             StringType(), nullable=True),  # TQ.9 (ID)
    StructField("order_sequencing",        StringType(), nullable=True),  # TQ.10 (OSD, raw)
    StructField("occurrence_duration",     StringType(), nullable=True),  # TQ.11.1 (CE/CWE.1)
    StructField("total_occurrences",       StringType(), nullable=True),  # TQ.12 (NM)
])


_NDL_STRUCT = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("family_name", StringType(), nullable=True),
    StructField("given_name", StringType(), nullable=True),
    StructField("middle_name", StringType(), nullable=True),
    StructField("suffix", StringType(), nullable=True),
    StructField("prefix", StringType(), nullable=True),
    StructField("degree", StringType(), nullable=True),
    StructField("start_datetime", StringType(), nullable=True),
    StructField("end_datetime", StringType(), nullable=True),
    StructField("point_of_care", StringType(), nullable=True),
    StructField("room", StringType(), nullable=True),
    StructField("bed", StringType(), nullable=True),
    StructField("facility", StringType(), nullable=True),
    StructField("location_status", StringType(), nullable=True),
    StructField("patient_location_type", StringType(), nullable=True),
    StructField("building", StringType(), nullable=True),
    StructField("floor", StringType(), nullable=True),
])


# --- Single-occurrence composite STRUCTs (no repeating/array counterpart) ---

_CP_STRUCT = StructType([
    StructField("price", StringType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("price_type", StringType(), nullable=True),
    StructField("from_value", StringType(), nullable=True),
    StructField("to_value", StringType(), nullable=True),
    StructField("range_units", StringType(), nullable=True),
    StructField("range_units_text", StringType(), nullable=True),
    StructField("range_units_coding_system", StringType(), nullable=True),
    StructField("range_type", StringType(), nullable=True),
])


_PT_STRUCT = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("mode", StringType(), nullable=True),
])


_VID_STRUCT = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("internationalization", StringType(), nullable=True),
    StructField("internationalization_text", StringType(), nullable=True),
    StructField("internationalization_coding_system", StringType(), nullable=True),
    StructField("international_version", StringType(), nullable=True),
    StructField("international_version_text", StringType(), nullable=True),
    StructField("international_version_coding_system", StringType(), nullable=True),
])


_SPS_STRUCT = StructType([
    StructField("code", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("additives", StringType(), nullable=True),
    StructField("collection_method", StringType(), nullable=True),
    StructField("body_site", StringType(), nullable=True),
    StructField("site_modifier", StringType(), nullable=True),
    StructField("collection_method_modifier", StringType(), nullable=True),
    StructField("role", StringType(), nullable=True),
])


_AUI_STRUCT = StructType([
    StructField("number", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
])


_DLN_STRUCT = StructType([
    StructField("number", StringType(), nullable=True),
    StructField("issuing_state", StringType(), nullable=True),
    StructField("expiration_date", StringType(), nullable=True),
])


_DLD_STRUCT = StructType([
    StructField("code", StringType(), nullable=True),
    StructField("effective_date", StringType(), nullable=True),
])


_JCC_STRUCT = StructType([
    StructField("code", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("coding_system", StringType(), nullable=True),
    StructField("class", StringType(), nullable=True),
    StructField("class_text", StringType(), nullable=True),
    StructField("class_coding_system", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
])


_MOC_STRUCT = StructType([
    StructField("monetary_amount", StringType(), nullable=True),
    StructField("monetary_amount_currency", StringType(), nullable=True),
    StructField("charge_code", StringType(), nullable=True),
    StructField("charge_code_text", StringType(), nullable=True),
    StructField("charge_code_coding_system", StringType(), nullable=True),
])


_PRL_STRUCT = StructType([
    StructField("code", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("coding_system", StringType(), nullable=True),
    StructField("sub_id", StringType(), nullable=True),
    StructField("descriptor", StringType(), nullable=True),
])


_CQ_STRUCT = StructType([
    StructField("quantity", StringType(), nullable=True),
    StructField("units", StringType(), nullable=True),
])


_MO_STRUCT = StructType([
    StructField("amount", StringType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
])


_OG_STRUCT = StructType([
    StructField("sub_identifier", StringType(), nullable=True),
    StructField("group", StringType(), nullable=True),
    StructField("sequence", StringType(), nullable=True),
    StructField("identifier", StringType(), nullable=True),
])


def _xpn_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """XPN (Extended Person Name) — repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_XPN_STRUCT, containsNull=True), nullable=True)]


def _cwe_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """CWE (Coded With Exceptions) — repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_CWE_STRUCT, containsNull=True), nullable=True)]


def _xtn_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """XTN (Extended Telecommunication Number) — repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_XTN_STRUCT, containsNull=True), nullable=True)]


def _xcn_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """XCN (Extended Composite ID Number and Name) — repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_XCN_STRUCT, containsNull=True), nullable=True)]


def _xon_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """XON (Extended Composite Name and Number for Organizations) — repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_XON_STRUCT, containsNull=True), nullable=True)]


def _cx_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """CX (Extended Composite ID with Check Digit) — repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_CX_STRUCT, containsNull=True), nullable=True)]


def _xad_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """XAD (Extended Address) — repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_XAD_STRUCT, containsNull=True), nullable=True)]


def _eip_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """EIP (Entity Identifier Pair) — repeating field as ArrayType(StructType<placer_assigned_identifier: EI, filler_assigned_identifier: EI>)."""
    return [StructField(column_name, ArrayType(_EIP_STRUCT, containsNull=True), nullable=True)]


def _tq_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """TQ (Timing Quantity) — repeating field as ArrayType(StructType([...])). Deprecated in v2.5."""
    return [StructField(column_name, ArrayType(_TQ_STRUCT, containsNull=True), nullable=True)]


def _s_array(name: str, comment: str = "") -> StructField:
    """ArrayType(StringType) for repeating simple (ID/IS) fields."""
    return StructField(name, ArrayType(StringType(), containsNull=True), nullable=True)


def _xcn_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """XCN (Extended Composite ID Number and Name for Persons) single occurrence as STRUCT (21 components)."""
    return [StructField(prefix, _XCN_STRUCT, nullable=True)]


def _cwe_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """CWE (Coded With Exceptions) single occurrence as STRUCT (9 active components)."""
    return [StructField(prefix, _CWE_STRUCT, nullable=True)]


def _hd_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """HD (Hierarchic Designator) single occurrence as STRUCT (3 components)."""
    return [StructField(prefix, _HD_STRUCT, nullable=True)]


# ---------------------------------------------------------------------------
# Struct-typed single-occurrence composite fields
#
# For 0..1 / 1..1 composites we now model the whole composite as one nested
# STRUCT column (e.g. ``event_reason STRUCT<code, text, ...>``) rather than a
# row of flat ``<prefix>_<subfield>`` columns. This mirrors the convention used
# by the ``fhir`` connector (CodeableConcept, HumanName, ...) and keeps each HL7
# composite isomorphic to its FHIR target type. Repeating composites continue to
# use ``ARRAY<STRUCT<...>>`` via the ``_*_array_schema`` helpers.
# ---------------------------------------------------------------------------


def _cwe_struct_field(name: str, label: str, field_ref: str) -> StructField:
    """CWE (Coded With Exceptions) single occurrence as ``STRUCT`` (9 components)."""
    return StructField(name, _CWE_STRUCT, nullable=True)


def _hd_struct_field(name: str, label: str, field_ref: str) -> StructField:
    """HD (Hierarchic Designator) single occurrence as ``STRUCT`` (3 components)."""
    return StructField(name, _HD_STRUCT, nullable=True)


def _ei_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """EI (Entity Identifier) single occurrence as STRUCT (4 components)."""
    return [StructField(prefix, _EI_INNER_STRUCT, nullable=True)]


def _cp_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """CP (Composite Price) — 6 component fields + MO currency sub-component.

    Components: Price (MO; MO.1 quantity stored as ``{prefix}`` and MO.2 ISO
    4217 denomination stored as ``{prefix}_currency``), Price Type (CP.2,
    table 0205), From Value (CP.3, NM), To Value (CP.4, NM), Range Units
    (CP.5, CWE code only), Range Type (CP.6, table 0298).
    """
    return [StructField(prefix, _CP_STRUCT, nullable=True)]


def _pt_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """PT (Processing Type) — 2 ID components."""
    return [StructField(prefix, _PT_STRUCT, nullable=True)]


def _vid_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """VID (Version Identifier) — ID + 2 CWE components."""
    return [StructField(prefix, _VID_STRUCT, nullable=True)]


def _sps_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """SPS (Specimen Source) — 7 components. Withdrawn in v2.7; used for backward compatibility with v2.3–v2.6."""
    return [StructField(prefix, _SPS_STRUCT, nullable=True)]


def _aui_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """AUI (Authorization Information) — ST + DT + ST."""
    return [StructField(prefix, _AUI_STRUCT, nullable=True)]


def _dln_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """DLN (Driver's License Number) — 3 components: license number (ST) + issuing state (IS) + expiration date (DT)."""
    return [StructField(prefix, _DLN_STRUCT, nullable=True)]


def _dld_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """DLD (Discharge Location and Date) — 2 components: location code (CWE.1) + effective date (DTM)."""
    return [StructField(prefix, _DLD_STRUCT, nullable=True)]


def _fc_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """FC (Financial Class) single-rep — CWE + DTM."""
    return [StructField(prefix, _FC_STRUCT, nullable=True)]


def _fc_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """FC (Financial Class) repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_FC_STRUCT, containsNull=True), nullable=True)]


def _jcc_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """JCC (Job Code/Class) — CWE + CWE + TX."""
    return [StructField(prefix, _JCC_STRUCT, nullable=True)]


def _moc_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """MOC (Money and Code) — MOC.1: MO (Monetary Amount) + MOC.2: CWE (Charge Code)."""
    return [StructField(prefix, _MOC_STRUCT, nullable=True)]


def _prl_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """PRL (Parent Result Link) — CWE + ST + TX."""
    return [StructField(prefix, _PRL_STRUCT, nullable=True)]


def _ndl_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """NDL (Name with Date and Location) single-rep — 11 components."""
    return [StructField(prefix, _NDL_STRUCT, nullable=True)]


def _ndl_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """NDL repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_NDL_STRUCT, containsNull=True), nullable=True)]


def _pl_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """PL (Person Location) repeating field as ArrayType(StructType([...]))."""
    return [StructField(column_name, ArrayType(_PL_STRUCT, containsNull=True), nullable=True)]


def _cq_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """CQ (Composite Quantity with Units) — 2 component fields.

    CQ.1 = Quantity (NM) — stored as ``{prefix}``.
    CQ.2 = Units (CWE) — code only from CQ.2.1, stored as ``{prefix}_units``.
    """
    return [StructField(prefix, _CQ_STRUCT, nullable=True)]


def _pl_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """PL (Person Location) — 11 component fields.

    Each HD-typed sub-component (point_of_care, room, bed, facility, building,
    floor, assigning_authority) is captured as its HD.1 namespace ID, matching
    PV1.3 / NK1.3 single-component composite flattening precedent.
    """
    return [StructField(prefix, _PL_STRUCT, nullable=True)]


def _eip_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """EIP (Entity Identifier Pair) — single instance: 8 flat fields for placer and filler EI."""
    return [StructField(prefix, _EIP_STRUCT, nullable=True)]


def _mo_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """MO (Money) — 2 component fields: quantity (NM) + ISO 4217 denomination (ID)."""
    return [StructField(prefix, _MO_STRUCT, nullable=True)]


def _og_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """OG (Observation Grouper, v2.8.2+) — 4 component fields.

    OG.1 = Original Sub-Identifier (ST) — backward-compatible with the legacy OBX-4 ST value.
    OG.2 = Group (NM), OG.3 = Sequence (NM), OG.4 = Identifier (ST).
    """
    return [StructField(prefix, _OG_STRUCT, nullable=True)]


def _ei_array_schema(column_name: str, label: str, field_ref: str) -> list[StructField]:
    """EI (Entity Identifier) — repeating field as ArrayType(StructType([...]))."""
    ei_struct = StructType([
        StructField("entity_identifier", StringType(), nullable=True),
        StructField("namespace_id", StringType(), nullable=True),
        StructField("universal_id", StringType(), nullable=True),
        StructField("universal_id_type", StringType(), nullable=True),
    ])
    return [StructField(column_name, ArrayType(ei_struct, containsNull=True), nullable=True)]


def _xon_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """XON (Extended Composite Name and Number for Organizations) — 10+ component fields."""
    return [StructField(prefix, _XON_STRUCT, nullable=True)]


def _cx_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """CX (Extended Composite ID with Check Digit) — 12 components + HD sub-components."""
    return [StructField(prefix, _CX_STRUCT, nullable=True)]


def _xtn_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """XTN (Extended Telecommunication Number) — 18 component fields."""
    return [StructField(prefix, _XTN_STRUCT, nullable=True)]


def _xad_schema(prefix: str, label: str, field_ref: str) -> list[StructField]:
    """XAD (Extended Address) single occurrence as STRUCT (23 components, 12 deprecated)."""
    return [StructField(prefix, _XAD_STRUCT, nullable=True)]


# Fields present in every segment table.
_METADATA_FIELDS: list[StructField] = [
    _pk_s("message_id",     "Unique message identifier (MSH-10); primary join key across all segment tables"),
    _s("message_timestamp", "Message creation date/time (MSH-7) in HL7 DTM format, e.g. 20240101120000"),
    _s("hl7_version",       "HL7 version string (MSH-12), e.g. 2.5.1"),
    _s("source_file",       "API resource name of the source HL7 message for traceability"),
    _s("send_time",         "Original HL7 message send time from the GCP Healthcare API in RFC3339 format"),
    _s("create_time",       "GCP Healthcare API createTime (when the message was ingested into the HL7v2 store); used as incremental cursor"),
    _s("raw_segment",       "Raw pipe-delimited text of this HL7 segment for lossless recovery and debugging"),
]

# ---------------------------------------------------------------------------
# Table-level descriptions (surfaced via read_table_metadata)
# ---------------------------------------------------------------------------

TABLE_DESCRIPTIONS: dict[str, str] = {
    "msh": (
        "Message Header — present in every HL7 message. Contains routing information "
        "(sending/receiving application and facility), message type, trigger event, "
        "timestamp, and HL7 version. One row per message."
    ),
    "evn": (
        "Event Type — trigger event metadata. Contains the event type code, the date/time "
        "the event was recorded, the date/time it occurred, the event reason, and the "
        "operator who initiated the event. One row per message."
    ),
    "pid": (
        "Patient Identification — core patient demographics. Contains name, date of birth, "
        "sex, medical record number (MRN), address, phone numbers, and insurance account number. "
        "One row per message."
    ),
    "pd1": (
        "Patient Additional Demographic — supplementary patient data including living will, "
        "organ donor status, primary care facility, student indicator, and military information. "
        "One row per message."
    ),
    "pv1": (
        "Patient Visit — encounter details. Contains patient class (inpatient/outpatient), "
        "bed location, attending and admitting physician, admit source, discharge disposition, "
        "and admit/discharge timestamps. One row per message."
    ),
    "pv2": (
        "Patient Visit Additional — extended visit details including admit reason, expected "
        "dates (admit/discharge/surgery), mode of arrival, patient condition, and visit priority. "
        "One row per message."
    ),
    "nk1": (
        "Next of Kin / Associated Parties — emergency contact or guarantor. Contains "
        "contact name, relationship to patient, address, phone number, and contact role. "
        "One row per associated party."
    ),
    "mrg": (
        "Merge Patient Information — prior patient identifiers used in merge/link/unlink events. "
        "Contains prior MRN, account number, visit number, and patient name. One row per message."
    ),
    "al1": (
        "Patient Allergy — allergy or adverse reaction record. Contains allergen code and "
        "description, allergy type (drug/food/environmental), reaction, severity, and "
        "identification date. One row per allergy."
    ),
    "iam": (
        "Patient Adverse Reaction Information — action-code based allergy tracking (newer "
        "replacement for AL1). Contains allergen, severity, reaction, action code (add/delete/update), "
        "unique identifier, and clinical status. One row per adverse reaction."
    ),
    "dg1": (
        "Diagnosis — ICD or SNOMED diagnosis with type (A=admitting, W=working, F=final) "
        "and optional DRG grouping details. Multiple DG1 rows per encounter are common. "
        "One row per diagnosis."
    ),
    "pr1": (
        "Procedures — surgical, diagnostic, and therapeutic procedures. Contains procedure code "
        "(CPT/ICD), date/time, duration, anesthesia details, and associated diagnosis. "
        "One row per procedure."
    ),
    "orc": (
        "Common Order — order control and status information. Contains order control code "
        "(new/cancel/change), placer and filler order numbers, ordering provider, order status, "
        "and transaction date/time. One row per order."
    ),
    "obr": (
        "Observation Request — lab or radiology order. Contains the ordered test "
        "(universal service identifier), specimen details, ordering provider, and overall "
        "result status. One row per order; paired with OBX rows via message_id."
    ),
    "obx": (
        "Observation Result — individual clinical result. Contains a single lab value, "
        "vital sign, or coded observation including value, units, reference range, and "
        "abnormal flag. Multiple OBX rows per OBR (one per result component)."
    ),
    "nte": (
        "Notes and Comments — free-text annotations attached to orders, results, or other "
        "segments. Contains the comment source, text content, and type. One row per comment."
    ),
    "spm": (
        "Specimen — specimen type, collection details, handling instructions, and condition. "
        "Contains specimen identifier, type, source site, collection method, and date/time. "
        "One row per specimen."
    ),
    "in1": (
        "Insurance — policy coverage and billing information. Contains insurance plan, company "
        "name and ID, group number, policy number, insured person details, and plan effective "
        "dates. One row per insurance plan."
    ),
    "gt1": (
        "Guarantor — financially responsible party. Contains guarantor name, address, phone, "
        "relationship to patient, employer information, and financial class. "
        "One row per guarantor."
    ),
    "ft1": (
        "Financial Transaction — charges, payments, and adjustments. Contains transaction "
        "type (charge/credit/payment), code, amount, date, performing provider, and associated "
        "diagnosis and procedure codes. One row per transaction."
    ),
    "rxa": (
        "Pharmacy/Treatment Administration — medication administration records. Contains "
        "drug/vaccine code, amount, units, administration date/time, provider, lot number, "
        "and completion status. One row per administration."
    ),
    "sch": (
        "Scheduling Activity Information — appointment details. Contains placer and filler "
        "appointment IDs, event reason, appointment type, duration, and contact information "
        "for placer and filler. One row per scheduling message."
    ),
    "txa": (
        "Transcription Document Header — document metadata and status. Contains document type, "
        "unique document number, completion status (dictated/authenticated), originator, "
        "transcriptionist, and authentication details. One row per document message."
    ),
}

# ---------------------------------------------------------------------------
# MSH — Message Header
# ---------------------------------------------------------------------------

MSH_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("field_separator",                  "Field separator character (MSH-1); always '|' per the HL7 standard"),
        _s("encoding_characters",              "Encoding characters (MSH-2): component ^, repetition ~, escape \\, subcomponent &"),
        _s("sending_application",                      "Namespace ID of the sending application (MSH-3.1)"),
        _s("sending_application_universal_id",         "Universal ID (e.g. OID) of the sending application (MSH-3.2)"),
        _s("sending_application_universal_id_type",    "Type of universal ID for the sending application, e.g. ISO (MSH-3.3)"),
        _s("sending_facility",                         "Namespace ID of the sending facility (MSH-4.1)"),
        _s("sending_facility_universal_id",            "Universal ID (e.g. OID) of the sending facility (MSH-4.2)"),
        _s("sending_facility_universal_id_type",       "Type of universal ID for the sending facility, e.g. ISO (MSH-4.3)"),
        _s("receiving_application",                    "Namespace ID of the receiving application (MSH-5.1)"),
        _s("receiving_application_universal_id",       "Universal ID (e.g. OID) of the receiving application (MSH-5.2)"),
        _s("receiving_application_universal_id_type",  "Type of universal ID for the receiving application, e.g. ISO (MSH-5.3)"),
        _s("receiving_facility",                       "Namespace ID of the receiving facility (MSH-6.1)"),
        _s("receiving_facility_universal_id",          "Universal ID (e.g. OID) of the receiving facility (MSH-6.2)"),
        _s("receiving_facility_universal_id_type",     "Type of universal ID for the receiving facility, e.g. ISO (MSH-6.3)"),
        _ts("message_datetime",                "Date/time the message was created (MSH-7); typed version of message_timestamp"),
        _s("security",                         "Security or access-restriction string (MSH-8); rarely populated"),
        _s("message_code",                     "Message code, first component of message type (MSH-9.1), e.g. ADT, ORU, ORM, ACK"),
        _s("trigger_event",                    "Trigger event, second component of message type (MSH-9.2), e.g. A01, A08, R01"),
        _s("message_structure",                "Message structure, third component of message type (MSH-9.3), e.g. ADT_A01, ORU_R01"),
        _s("message_control_id",               "Unique message control ID assigned by the sending application (MSH-10); same as message_id"),
    ]
    + _pt_schema("processing_id", "Processing ID (PT)", "MSH-11")
    + _vid_schema("version_id", "Version identifier (VID)", "MSH-12")
    + [
        _int_field("sequence_number",                  "Optional sequence number for application-level message ordering (MSH-13)"),
        _s("continuation_pointer",             "Pointer used to continue a fragmented message (MSH-14); rarely populated"),
        _s("accept_acknowledgment_type",       "Conditions requiring an accept (transport-level) acknowledgment (MSH-15): AL, NE, SU, ER"),
        _s("application_acknowledgment_type",  "Conditions requiring an application-level acknowledgment (MSH-16): AL, NE, SU, ER"),
        _s("country_code",                     "ISO 3166 three-letter country code for the message (MSH-17)"),
        _s_array("character_set",              "Character encoding used in the message (MSH-18, ID repeatable), e.g. ASCII, UTF-8, 8859/1"),
    ]
    + _cwe_schema("principal_language", "Principal language of the message (CWE)", "MSH-19")
    + [
        _s("alt_character_set_handling",       "Alternate character set handling scheme (MSH-20)"),
        *_ei_array_schema("message_profile_identifiers", "Message profile identifiers", "MSH-21"),
    ]
    + _xon_schema("sending_responsible_org", "Sending responsible organization", "MSH-22")
    + _xon_schema("receiving_responsible_org", "Receiving responsible organization", "MSH-23")
    + _hd_schema("sending_network_address", "Sending network address", "MSH-24")
    + _hd_schema("receiving_network_address", "Receiving network address", "MSH-25")
    + _cwe_schema("security_classification_tag", "Security classification", "MSH-26")
    + _cwe_array_schema("security_handling_instructions", "Security handling instructions (CWE, repeatable)", "MSH-27")
    + _cwe_array_schema("special_access_restriction", "Special access restriction instructions (CWE, repeatable, v2.7.1+)", "MSH-28")
)

# ---------------------------------------------------------------------------
# PID — Patient Identification
# ---------------------------------------------------------------------------

PID_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _int_field("set_id",                        "Sequence number when multiple PID segments appear in a message (PID-1)"),
        *_cx_schema("patient_external_id", "External patient ID from a prior system (CX, PID-2, deprecated in v2.7)", "PID-2"),
        *_cx_array_schema("patient_id", "Patient identifier list (CX, repeatable per spec)", "PID-3"),
        *_cx_schema("alternate_patient_id", "Alternate patient identifier (CX, PID-4, deprecated in v2.7)", "PID-4"),
        *_xpn_array_schema("patient_names", "Patient names", "PID-5"),
        *_xpn_array_schema("mothers_maiden_names", "Mother's maiden names", "PID-6"),
        _ts("date_of_birth",                "Date of birth parsed to timestamp (PID-7)"),
        *_cwe_schema("administrative_sex", "Administrative sex", "PID-8"),
        _s("patient_alias",                 "Alias name(s) for the patient (PID-9, deprecated in v2.7)"),
        *_cwe_array_schema("race", "Race (CWE, repeatable per spec)", "PID-10"),
        *_xad_array_schema("address", "Patient address (XAD, repeatable per spec)", "PID-11"),
        _s("county_code",                   "County or parish code (PID-12, deprecated in v2.6)"),
        *_xtn_array_schema("home_phone", "Home phone (XTN, repeatable per spec)", "PID-13"),
        *_xtn_array_schema("business_phone", "Business phone (XTN, repeatable per spec)", "PID-14"),
        *_cwe_schema("primary_language", "Primary language", "PID-15"),
        *_cwe_schema("marital_status", "Marital status", "PID-16"),
        *_cwe_schema("religion", "Religion", "PID-17"),
        *_cx_schema("patient_account_number", "Patient account number", "PID-18"),
        _s("ssn",                           "Social Security Number (PID-19, deprecated in v2.7)"),
        *_dln_schema("drivers_license", "Driver's license", "PID-20"),
        *_cx_array_schema("mothers_identifier", "Mother's identifier (CX, repeatable per spec)", "PID-21"),
        *_cwe_array_schema("ethnic_group", "Ethnic group (CWE, repeatable per spec)", "PID-22"),
        _s("birth_place",                   "Birthplace as free text (PID-23)"),
        _s("multiple_birth_indicator",      "Whether the patient is one of a multiple birth: Y or N (PID-24)"),
        _int_field("birth_order",                   "Birth sequence number for multiple-birth patients, e.g. 1, 2, 3 (PID-25)"),
        *_cwe_array_schema("citizenship", "Citizenship (CWE, repeatable per spec)", "PID-26"),
        *_cwe_schema("veterans_military_status", "Veterans military status", "PID-27"),
        *_cwe_schema("nationality", "Nationality", "PID-28"),
        _ts("patient_death_datetime",       "Date/time of patient death parsed to timestamp (PID-29)"),
        _s("patient_death_indicator",       "Death indicator: Y=deceased, N=alive (PID-30)"),
        _s("identity_unknown_indicator",    "Whether the patient's identity is unknown: Y or N (PID-31, v2.5+)"),
        *_cwe_array_schema("identity_reliability_code", "Identity reliability (CWE, repeatable per spec)", "PID-32"),
        _ts("last_update_datetime",         "Date/time the patient record was last updated (PID-33, v2.5+)"),
        *_hd_schema("last_update_facility", "Last update facility", "PID-34"),
        *_cwe_schema("species_code", "Species/taxonomic classification", "PID-35"),
        *_cwe_schema("breed_code", "Breed", "PID-36"),
        _s("strain",                        "Strain description for veterinary use (PID-37, v2.5+)"),
        *_cwe_schema("production_class_code", "Production class", "PID-38"),
        *_cwe_array_schema("tribal_citizenship", "Tribal citizenship (CWE, repeatable per spec)", "PID-39"),
        *_xtn_array_schema("patient_telecom", "Patient telecommunication (XTN, repeatable per spec)", "PID-40"),
    ]
)

# ---------------------------------------------------------------------------
# PV1 — Patient Visit
# ---------------------------------------------------------------------------

PV1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _int_field("set_id",                       "Sequence number when multiple PV1 segments appear (PV1-1)"),
    ]
    + _cwe_schema("patient_class", "Patient class (CWE)", "PV1-2")
    + _pl_schema("assigned_patient_location", "Assigned patient location (PL)", "PV1-3")
    + _cwe_schema("admission_type", "Admission type (CWE)", "PV1-4")
    + _cx_schema("preadmit_number", "Pre-admission number", "PV1-5")
    + _pl_schema("prior_patient_location", "Prior patient location (PL)", "PV1-6")
    + _xcn_array_schema("attending_doctor", "Attending physician (XCN, repeatable per spec)", "PV1-7")
    + _xcn_array_schema("referring_doctor", "Referring physician (XCN, repeatable per spec)", "PV1-8")
    + _xcn_array_schema("consulting_doctor", "Consulting physician (XCN, repeatable per spec)", "PV1-9")
    + _cwe_schema("hospital_service", "Hospital service (CWE)", "PV1-10")
    + _pl_schema("temporary_location", "Temporary location (PL)", "PV1-11")
    + _cwe_schema("preadmit_test_indicator", "Pre-admit test indicator (CWE; e.g. Y/N)", "PV1-12")
    + _cwe_schema("readmission_indicator", "Re-admission indicator (CWE; e.g. R=Readmission)", "PV1-13")
    + _cwe_schema("admit_source", "Admit source (CWE)", "PV1-14")
    + _cwe_array_schema("ambulatory_status", "Ambulatory status (CWE, repeatable)", "PV1-15")
    + _cwe_schema("vip_indicator", "VIP indicator (CWE)", "PV1-16")
    + _xcn_array_schema("admitting_doctor", "Admitting physician (XCN, repeatable per spec)", "PV1-17")
    + _cwe_schema("patient_type", "Patient type (CWE)", "PV1-18")
    + _cx_schema("visit_number", "Visit/encounter number", "PV1-19")
    + _fc_array_schema("financial_class", "Financial class (FC, repeatable per spec)", "PV1-20")
    + _cwe_schema("charge_price_indicator", "Charge price indicator (CWE)", "PV1-21")
    + _cwe_schema("courtesy_code", "Courtesy code (CWE)", "PV1-22")
    + _cwe_schema("credit_rating", "Credit rating (CWE)", "PV1-23")
    + _cwe_array_schema("contract_code", "Contract code (CWE, repeatable per spec)", "PV1-24")
    + [
        _s_array("contract_effective_date", "Effective dates of the contract (PV1-25, DT, repeatable per spec)"),
        _s_array("contract_amount",         "Amounts owed under the contract (PV1-26, NM, repeatable per spec)"),
        _s_array("contract_period",         "Durations of the contract in days (PV1-27, NM, repeatable per spec)"),
    ]
    + _cwe_schema("interest_code", "Interest code (CWE)", "PV1-28")
    + _cwe_schema("transfer_to_bad_debt_code", "Transfer-to-bad-debt code (CWE)", "PV1-29")
    + [
        _ts("transfer_to_bad_debt_date",   "Date the account was transferred to bad debt (PV1-30, DT)"),
    ]
    + _cwe_schema("bad_debt_agency_code", "Bad-debt agency code (CWE)", "PV1-31")
    + [
        _s("bad_debt_transfer_amount",     "Amount transferred to bad debt (PV1-32)"),
        _s("bad_debt_recovery_amount",     "Amount recovered from bad debt (PV1-33)"),
    ]
    + _cwe_schema("delete_account_indicator", "Delete-account indicator (CWE)", "PV1-34")
    + [
        _ts("delete_account_date",         "Date the account was deleted (PV1-35, DT)"),
    ]
    + _cwe_schema("discharge_disposition", "Discharge disposition (CWE)", "PV1-36")
    + _dld_schema("discharged_to_location", "Discharged to location (DLD)", "PV1-37")
    + _cwe_schema("diet_type", "Diet type", "PV1-38")
    + _cwe_schema("servicing_facility", "Servicing facility (CWE)", "PV1-39")
    + _cwe_schema("bed_status", "Bed status (CWE, deprecated in v2.6)", "PV1-40")
    + _cwe_schema("account_status", "Account status (CWE)", "PV1-41")
    + _pl_schema("pending_location", "Pending location (PL)", "PV1-42")
    + _pl_schema("prior_temporary_location", "Prior temporary location (PL)", "PV1-43")
    + [
        _ts("admit_datetime",              "Date/time of admission parsed to timestamp (PV1-44)"),
        _ts("discharge_datetime",          "Date/time of discharge parsed to timestamp (PV1-45)"),
        _s("current_patient_balance",      "Current outstanding patient balance (PV1-46)"),
        _s("total_charges",                "Total charges for the visit (PV1-47)"),
        _s("total_adjustments",            "Total adjustments applied to the visit charges (PV1-48)"),
        _s("total_payments",               "Total payments received for the visit (PV1-49)"),
    ]
    + _cx_array_schema("alternate_visit_id", "Alternate visit ID (CX, repeatable per spec)", "PV1-50")
    + _cwe_schema("visit_indicator", "Visit indicator (CWE)", "PV1-51")
    + _xcn_array_schema("other_healthcare_provider", "Other healthcare provider (XCN, repeatable per spec, deprecated v2.7)", "PV1-52")
    + [
        _s("service_episode_description",   "Free-text description of the service episode (PV1-53, v2.8+)"),
    ]
    + _cx_schema("service_episode_identifier", "Service episode identifier (CX, v2.9+)", "PV1-54")
)

# ---------------------------------------------------------------------------
# OBR — Observation Request
# ---------------------------------------------------------------------------

OBR_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                           "Sequence number of this OBR within the message; part of composite primary key (OBR-1)"),
    ]
    + _ei_schema("placer_order_number", "Placer order number (EI)", "OBR-2")
    + _ei_schema("filler_order_number", "Filler order number (EI)", "OBR-3")
    + _cwe_schema("service", "Universal service identifier (CWE)", "OBR-4")
    + [
        _s("priority",                            "Order priority (OBR-5, deprecated in v2.7): R=Routine, S=STAT, A=ASAP"),
        _ts("requested_datetime",                 "Requested date/time for the observation, parsed to timestamp (OBR-6, deprecated in v2.7)"),
        _ts("observation_datetime",               "Date/time specimen was collected or observation started, parsed to timestamp (OBR-7)"),
        _ts("observation_end_datetime",           "Date/time observation ended or specimen collection completed, parsed to timestamp (OBR-8)"),
    ]
    + _cq_schema("collection_volume", "Volume of specimen collected (CQ)", "OBR-9")
    + _xcn_array_schema("collector", "Specimen collector (XCN, repeatable per spec)", "OBR-10")
    + [
        _s("specimen_action_code",                "Action to take on the specimen (OBR-11): A=Add, G=Generated, L=Lab, O=Obtained"),
    ]
    + _cwe_schema("danger_code", "Danger code (CWE)", "OBR-12")
    + _cwe_array_schema("relevant_clinical_information", "Relevant clinical information (CWE, repeatable per spec)", "OBR-13")
    + [
        _ts("specimen_received_datetime",         "Date/time the specimen was received by the lab, parsed to timestamp (OBR-14)"),
    ]
    + _sps_schema("specimen_source", "Specimen source (SPS, withdrawn in v2.7; backward-compatible)", "OBR-15")
    + _xcn_array_schema("ordering_provider", "Ordering physician (XCN, repeatable; withdrawn v2.9 — backward-compatible)", "OBR-16")
    + _xtn_array_schema("order_callback_phone", "Order callback phone (XTN, repeatable per spec)", "OBR-17")
    + [
        _s("placer_field_1",                      "Placer-defined field 1 for local use (OBR-18)"),
        _s("placer_field_2",                      "Placer-defined field 2 for local use (OBR-19)"),
        _s("filler_field_1",                      "Filler-defined field 1 for local use (OBR-20)"),
        _s("filler_field_2",                      "Filler-defined field 2 for local use (OBR-21)"),
        _ts("results_rpt_status_chng_datetime",   "Date/time the result status last changed, parsed to timestamp (OBR-22)"),
    ]
    + _moc_schema("charge_to_practice", "Charge to practice (MOC)", "OBR-23")
    + [
        _s("diagnostic_service_section",          "Diagnostic service section ID code (OBR-24)"),
        _s("result_status",                       "Overall result status (OBR-25): F=Final, P=Preliminary, C=Corrected, X=Canceled"),
    ]
    + _prl_schema("parent_result", "Parent result link (PRL)", "OBR-26")
    + _tq_array_schema("quantity_timing", "Quantity/timing of the order (TQ, repeatable, deprecated in v2.5)", "OBR-27")
    + _xcn_array_schema("result_copies_to", "Result copy-to provider (XCN, repeatable per spec)", "OBR-28")
    + _eip_schema("parent_results_observation_identifier", "Parent results observation identifier — links child result to parent observation (EIP, [0..1])", "OBR-29")
    + [
        _s("transportation_mode",                  "Specimen transportation mode code (OBR-30)"),
    ]
    + _cwe_array_schema("reason_for_study", "Reason for study (CWE, repeatable)", "OBR-31")
    + _ndl_schema("principal_result_interpreter",  "Principal result interpreter (NDL)", "OBR-32")
    + _ndl_array_schema("assistant_result_interpreter", "Assistant result interpreter (NDL, repeatable per spec)", "OBR-33")
    + _ndl_array_schema("technician",              "Technician (NDL, repeatable per spec)", "OBR-34")
    + _ndl_array_schema("transcriptionist",        "Transcriptionist (NDL, repeatable per spec)", "OBR-35")
    + [
        _ts("scheduled_datetime",                 "Scheduled date/time for the observation, parsed to timestamp (OBR-36)"),
        _int_field("number_of_sample_containers",         "Number of specimen containers required (OBR-37)"),
    ]
    + _cwe_array_schema("transport_logistics", "Transport logistics (CWE, repeatable per spec)", "OBR-38")
    + _cwe_array_schema("collectors_comment", "Collector comment (CWE, repeatable per spec)", "OBR-39")
    + _cwe_schema("transport_arrangement_responsibility", "Transport arrangement responsibility (CWE)", "OBR-40")
    + [
        _s("transport_arranged",                   "Transport arranged indicator code (OBR-41)"),
        _s("escort_required",                      "Escort required indicator code (OBR-42)"),
    ]
    + _cwe_array_schema("planned_patient_transport_comment", "Planned patient transport comment (CWE, repeatable per spec)", "OBR-43")
    + _cwe_schema("procedure_code", "Procedure code (CNE; CWE-compatible struct)", "OBR-44")
    + _cwe_array_schema("procedure_code_modifier", "Procedure code modifier (CNE, repeatable per spec; uses CWE-shape struct since CNE and CWE share components)", "OBR-45")
    + _cwe_array_schema("placer_supplemental_service_info", "Placer supplemental service info (CWE, repeatable per spec)", "OBR-46")
    + _cwe_array_schema("filler_supplemental_service_info", "Filler supplemental service info (CWE, repeatable per spec)", "OBR-47")
    + _cwe_schema("medically_necessary_dup_proc_reason", "Medically necessary duplicate procedure reason (CWE)", "OBR-48")
    + _cwe_schema("result_handling", "Result handling (CWE)", "OBR-49")
    + _cwe_schema("parent_universal_service_id", "Parent universal service ID (CWE)", "OBR-50")
    + _ei_schema("observation_group", "Observation group (EI)", "OBR-51")
    + _ei_schema("parent_observation_group", "Parent observation group (EI)", "OBR-52")
    + _cx_array_schema("alternate_placer_order", "Alternate placer order (CX, repeatable per spec)", "OBR-53")
    + _eip_array_schema("parent_order", "Parent order identifier (EIP, repeatable per spec, v2.9+)", "OBR-54")
    + [
        _s("action_code",                     "Action code (OBR-55, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# OBX — Observation Result
# ---------------------------------------------------------------------------

OBX_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                       "Sequence number of this OBX within the message; part of composite primary key (OBX-1)"),
        _s("value_type",                      "Data type of the observation value (OBX-2): NM=Numeric, ST=String, CWE=Coded, TX=Text, TS=Timestamp"),
    ]
    + _cwe_schema("observation_id", "Observation identifier (CWE)", "OBX-3")
    + _og_schema("observation_sub_id", "Observation sub-ID (OG, v2.8.2+; OG.1 is backward-compatible with legacy ST sub-ID)", "OBX-4")
    + [
        _s_array("observation_value",          "The result value(s) (OBX-5, Varies [0..*]); each repetition is a raw string — check value_type (OBX-2) to interpret"),
    ]
    + _cwe_schema("units", "Units of measure (CWE)", "OBX-6")
    + [
        _s("references_range",                "Normal or reference range for the result, e.g. 136-145 (OBX-7)"),
    ]
    + _cwe_array_schema("interpretation_codes", "Interpretation codes (CWE, repeatable)", "OBX-8")
    + [
        _s("probability",                     "Probability of the observation being correct, 0-1 scale (OBX-9)"),
        _s_array("nature_of_abnormal_test",   "What the reference range is based on: A=Age, S=Sex, R=Race (OBX-10, ID repeatable)"),
        _s("observation_result_status",       "Result status (OBX-11): F=Final, P=Preliminary, C=Corrected, X=Deleted, R=Not yet verified"),
        _ts("effective_date_of_ref_range",    "Date the reference range became effective, parsed to timestamp (OBX-12)"),
        _s("user_defined_access_checks",      "Site-defined access control value (OBX-13)"),
        _ts("datetime_of_observation",        "Date/time this specific observation was made, parsed to timestamp (OBX-14)"),
    ]
    + _cwe_schema("producers_id", "Producer ID (CWE)", "OBX-15")
    + _xcn_array_schema("responsible_observer", "Responsible observer (XCN, repeatable per spec)", "OBX-16")
    + _cwe_array_schema("observation_method", "Observation method (CWE, repeatable per spec)", "OBX-17")
    + _ei_array_schema("equipment_instance_identifier", "Equipment instance (EI, repeatable per spec)", "OBX-18")
    + [
        _ts("datetime_of_analysis",           "Date/time the specimen was analyzed on the instrument, parsed to timestamp (OBX-19, v2.5+)"),
    ]
    + _cwe_array_schema("observation_site", "Observation site (CWE, repeatable per spec)", "OBX-20")
    + _ei_schema("observation_instance_identifier", "Observation instance (EI)", "OBX-21")
    + _cwe_schema("mood_code", "Mood code (CWE)", "OBX-22")
    + _xon_schema("performing_organization", "Performing organization (XON)", "OBX-23")
    + _xad_schema("performing_org_address", "Performing organization address (XAD)", "OBX-24")
    + _xcn_schema("performing_org_medical_director", "Medical director", "OBX-25")
    + [
        _s("patient_results_release_category","Category controlling release of results to the patient (OBX-26, v2.8+)"),
    ]
    + _cwe_schema("root_cause", "Root cause (CWE)", "OBX-27")
    + _cwe_array_schema("local_process_control", "Local process control (CWE, repeatable per spec)", "OBX-28")
    + [
        _s("observation_type",                "Observation type (OBX-29, v2.8.2+)"),
        _s("observation_sub_type",            "Observation sub-type (OBX-30, v2.8.2+)"),
        _s("action_code",                 "Action code (OBX-31, v2.9+)"),
    ]
    + _cwe_array_schema("observation_value_absent_reason", "Observation value absent reason (CWE, repeatable per spec)", "OBX-32")
    + _eip_array_schema("observation_related_specimen", "Observation-related specimen identifier (EIP, repeatable per spec)", "OBX-33")
)

# ---------------------------------------------------------------------------
# AL1 — Patient Allergy
# ---------------------------------------------------------------------------

AL1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",               "Sequence number of this allergy within the message; part of composite primary key (AL1-1)"),
    ]
    + _cwe_schema("allergen_type_code", "Allergen type", "AL1-2")
    + _cwe_schema("allergen_code", "Allergen (CWE)", "AL1-3")
    + _cwe_schema("allergy_severity_code", "Allergy severity", "AL1-4")
    + _cwe_array_schema(
        "allergy_reaction",
        # AL1-5 is spec-typed as ST 0..* in HL7 v2.9, but in practice EHRs routinely emit
        # CWE-shaped values here (e.g. "HIV^Hives^HL70129~RSH^Rash^HL70129"). We model
        # this leniently: when senders send plain ST the value lands in element 0's `code`
        # subfield with the rest NULL; when senders send CWE-shape, all 9 components are
        # populated; in either case every ~-separated repetition is preserved.
        "Allergy reaction (spec ST 0..*; modeled as repeating CWE-shape struct for lenient parsing)",
        "AL1-5",
    )
    + [
        _ts("identification_date",    "Date the allergy was first identified or recorded, parsed to timestamp (AL1-6, deprecated in v2.6)"),
    ]
)

# ---------------------------------------------------------------------------
# DG1 — Diagnosis
# ---------------------------------------------------------------------------

DG1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                            "Sequence number of this diagnosis within the message; part of composite primary key (DG1-1)"),
        _s("diagnosis_coding_method",              "Diagnosis coding method (DG1-2, deprecated in v2.7); use diagnosis_coding_system"),
    ]
    + _cwe_schema("diagnosis_code", "Diagnosis (CWE)", "DG1-3")
    + [
        _s("diagnosis_description",                "Free-text diagnosis description (DG1-4, deprecated in v2.7)"),
        _ts("diagnosis_datetime",                  "Date/time the diagnosis was established, parsed to timestamp (DG1-5)"),
    ]
    + _cwe_schema("diagnosis_type", "Diagnosis type (CWE; e.g. A=Admitting, W=Working, F=Final)", "DG1-6")
    + _cwe_schema("major_diagnostic_category", "Major diagnostic category (MDC)", "DG1-7")
    + _cwe_schema("diagnostic_related_group", "Diagnostic related group (DRG)", "DG1-8")
    + [
        _s("drg_approval_indicator",               "Whether the DRG assignment was approved: Y or N (DG1-9)"),
    ]
    + _cwe_schema("drg_grouper_review_code", "DRG grouper review code (CWE)", "DG1-10")
    + _cwe_schema("outlier_type", "Outlier type", "DG1-11")
    + [
        _int_field("outlier_days",                         "Number of outlier days beyond the DRG length-of-stay threshold (DG1-12)"),
    ]
    + _cp_schema("outlier_cost", "Outlier cost amount beyond the DRG cost threshold (CP, deprecated)", "DG1-13")
    + [
        _s("grouper_version_and_type",             "Version and type of the DRG grouper software (DG1-14)"),
        _int_field("diagnosis_priority",                   "Priority rank of this diagnosis; 1=principal diagnosis (DG1-15)"),
    ]
    + _xcn_array_schema("diagnosing_clinician", "Diagnosing clinician (XCN, repeatable per spec)", "DG1-16")
    + _cwe_schema("diagnosis_classification", "Diagnosis classification (CWE; e.g. C=Chronic, A=Acute)", "DG1-17")
    + [
        _s("confidential_indicator",               "Whether the diagnosis is confidential and access-restricted: Y or N (DG1-18)"),
        _ts("attestation_datetime",                "Date/time the diagnosis was attested by the physician, parsed to timestamp (DG1-19)"),
    ]
    + _ei_schema("diagnosis_identifier", "Diagnosis instance identifier (EI)", "DG1-20")
    + [
        _s("diagnosis_action_code",                "Action to take on this diagnosis: A=Add, U=Update, D=Delete (DG1-21, v2.5+)"),
    ]
    + _ei_schema("parent_diagnosis", "Parent diagnosis (EI)", "DG1-22")
    + _cwe_schema("drg_ccl_value_code", "DRG complication/comorbidity level (CWE)", "DG1-23")
    + [
        _s("drg_grouping_usage",                   "Whether this diagnosis was used in DRG grouping: Y or N (DG1-24, v2.7+)"),
    ]
    + _cwe_schema("drg_diagnosis_determination_status", "DRG diagnosis determination status (CWE, v2.7+)", "DG1-25")
    + _cwe_schema("present_on_admission_indicator", "Present-on-admission indicator (CWE, v2.7+; e.g. Y=Yes, N=No, U=Unknown, W=Clinically undetermined)", "DG1-26")
)

# ---------------------------------------------------------------------------
# NK1 — Next of Kin / Associated Parties
# ---------------------------------------------------------------------------

NK1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                  "Sequence number of this next-of-kin record within the message; part of composite primary key (NK1-1)"),
        *_xpn_array_schema("names", "Next of kin names", "NK1-2"),
    ]
    + _cwe_schema("relationship", "Relationship to patient (CWE)", "NK1-3")
    + _xad_array_schema("address", "Next of kin address (XAD, repeatable per spec)", "NK1-4")
    + _xtn_array_schema("phone_number", "Next of kin home or primary phone (XTN, repeatable per spec)", "NK1-5")
    + _xtn_array_schema("business_phone", "Next of kin business phone (XTN, repeatable per spec)", "NK1-6")
    + _cwe_schema("contact_role", "Contact role", "NK1-7")
    + [
        _ts("start_date",                "Date this contact relationship became effective, parsed to timestamp (NK1-8)"),
        _ts("end_date",                  "Date this contact relationship ended, parsed to timestamp (NK1-9)"),
        _s("job_title",                  "Next of kin job title or occupation (NK1-10)"),
    ]
    + _jcc_schema("job_code", "Next of kin job code/class (JCC)", "NK1-11")
    + _cx_schema("employee_number", "Employee number", "NK1-12")
    + _xon_array_schema("organization_name", "Employer organization (XON, repeatable per spec)", "NK1-13")
    + _cwe_schema("marital_status", "Marital status", "NK1-14")
    + _cwe_schema("administrative_sex", "Administrative sex", "NK1-15")
    + [
        _ts("date_of_birth",             "Date of birth of the next of kin, parsed to timestamp (NK1-16)"),
    ]
    + _cwe_array_schema("living_dependency", "Living dependency (CWE, repeatable per spec)", "NK1-17")
    + _cwe_array_schema("ambulatory_status", "Ambulatory status (CWE, repeatable per spec)", "NK1-18")
    + _cwe_array_schema("citizenship", "Citizenship (CWE, repeatable per spec)", "NK1-19")
    + _cwe_schema("primary_language", "Primary language", "NK1-20")
    + _cwe_schema("living_arrangement", "Living arrangement (CWE)", "NK1-21")
    + _cwe_schema("publicity_code", "Publicity / consent to contact", "NK1-22")
    + [
        _s("protection_indicator",       "Whether to restrict sharing of this contact's information: Y or N (NK1-23)"),
    ]
    + _cwe_schema("student_indicator", "Student indicator (CWE; e.g. F=Full-time, P=Part-time)", "NK1-24")
    + _cwe_schema("religion", "Religion", "NK1-25")
    + [
        *_xpn_array_schema("mothers_maiden_names", "NK1 mother's maiden names", "NK1-26"),
    ]
    + _cwe_schema("nationality", "Nationality", "NK1-27")
    + _cwe_array_schema("ethnic_group", "Ethnic group (CWE, repeatable per spec)", "NK1-28")
    + _cwe_array_schema("contact_reason", "Contact reason (CWE, repeatable per spec)", "NK1-29")
    + [
        *_xpn_array_schema("contact_persons", "Contact persons", "NK1-30"),
    ]
    + _xtn_array_schema("contact_person_telephone", "Contact person telephone (XTN, repeatable per spec)", "NK1-31")
    + _xad_array_schema("contact_persons_address", "Contact person address (XAD, repeatable per spec)", "NK1-32")
    + _cx_array_schema("associated_party_identifiers", "Associated party identifier (CX, repeatable per spec)", "NK1-33")
    + _cwe_schema("job_status", "Job status (CWE)", "NK1-34")
    + _cwe_array_schema("race", "Race (CWE, repeatable per spec)", "NK1-35")
    + _cwe_schema("handicap", "Handicap (CWE)", "NK1-36")
    + [
        _s("contact_ssn",                "Social Security Number of the contact person (NK1-37, deprecated in v2.7)"),
        _s("birth_place",                "Birth place of the next of kin (NK1-38, v2.6+)"),
    ]
    + _cwe_schema("vip_indicator", "VIP indicator (CWE)", "NK1-39")
    + _xtn_schema("telecommunication_info", "Next of kin telecommunication (XTN)", "NK1-40")
    + _xtn_schema("contact_telecommunication_info", "Contact person telecommunication", "NK1-41")
)

# ---------------------------------------------------------------------------
# EVN — Event Type
# ---------------------------------------------------------------------------

EVN_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("event_type_code",          "Event type code, withdrawn as of v2.7 (EVN-1)"),
        _ts("recorded_datetime",       "Date/time the event was recorded in the sending system, parsed to timestamp (EVN-2)"),
        _ts("date_time_planned_event", "Date/time the event was planned to occur, parsed to timestamp (EVN-3)"),
        _cwe_struct_field("event_reason", "Event reason", "EVN-4"),
        *_xcn_array_schema("operator", "Operator (XCN, repeatable per spec)", "EVN-5"),
        _ts("event_occurred",          "Actual date/time the event occurred, parsed to timestamp (EVN-6)"),
        _hd_struct_field("event_facility", "Event facility", "EVN-7"),
    ]
)

# ---------------------------------------------------------------------------
# PD1 — Patient Additional Demographic
# ---------------------------------------------------------------------------

PD1_SCHEMA = StructType(
    _METADATA_FIELDS
    + _cwe_array_schema("living_dependency", "Living dependency (CWE, repeatable)", "PD1-1")
    + _cwe_schema("living_arrangement", "Living arrangement (CWE)", "PD1-2")
    + _xon_array_schema("patient_primary_facility", "Primary care facility (XON, repeatable per spec)", "PD1-3")
    + _xcn_schema("patient_primary_care_provider", "Primary care provider", "PD1-4")
    + _cwe_schema("student_indicator", "Student indicator (CWE)", "PD1-5")
    + _cwe_schema("handicap", "Handicap (CWE)", "PD1-6")
    + _cwe_schema("living_will_code", "Living will code (CWE)", "PD1-7")
    + _cwe_schema("organ_donor_code", "Organ donor code (CWE)", "PD1-8")
    + [
        _s("separate_bill",                            "Separate billing flag Y/N (PD1-9)"),
    ]
    + _cx_array_schema("duplicate_patient", "Duplicate patient (CX, repeatable per spec)", "PD1-10")
    + _cwe_schema("publicity_code", "Publicity code", "PD1-11")
    + [
        _s("protection_indicator",                     "Protection indicator Y/N (PD1-12); if Y, patient info is restricted"),
        _ts("protection_indicator_effective_date",     "Date the protection indicator became effective (PD1-13, DT)"),
    ]
    + _xon_array_schema("place_of_worship", "Place of worship (XON, repeatable per spec)", "PD1-14")
    + _cwe_array_schema("advance_directive_code", "Advance directive (CWE, repeatable per spec)", "PD1-15")
    + _cwe_schema("immunization_registry_status", "Immunization registry status (CWE)", "PD1-16")
    + [
        _ts("immunization_registry_status_effective_date", "Immunization registry status effective date (PD1-17, DT)"),
        _ts("publicity_code_effective_date",           "Publicity code effective date (PD1-18, DT)"),
    ]
    + _cwe_schema("military_branch", "Military branch (CWE)", "PD1-19")
    + _cwe_schema("military_rank_grade", "Military rank/grade (CWE)", "PD1-20")
    + _cwe_schema("military_status", "Military status (CWE)", "PD1-21")
    + [
        _ts("advance_directive_last_verified_date",    "Date advance directive was last verified (PD1-22, DT, v2.8+)"),
        _ts("retirement_date",                         "Date the patient retired (PD1-23, DT, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# PV2 — Patient Visit Additional Information
# ---------------------------------------------------------------------------

PV2_SCHEMA = StructType(
    _METADATA_FIELDS
    + _pl_schema("prior_pending_location", "Prior pending transfer location (PL)", "PV2-1")
    + _cwe_schema("accommodation_code", "Accommodation code", "PV2-2")
    + _cwe_schema("admit_reason", "Admit reason", "PV2-3")
    + _cwe_schema("transfer_reason", "Transfer reason", "PV2-4")
    + [
        _s_array("patient_valuables",                  "Patient's valuable items descriptions (ST, repeatable per spec, PV2-5)"),
        _s("patient_valuables_location",               "Location of patient's valuables (PV2-6)"),
    ]
    + _cwe_array_schema("visit_user_code", "Visit user code (CWE, repeatable)", "PV2-7")
    + [
        _ts("expected_admit_datetime",                 "Expected admission date/time (PV2-8)"),
        _ts("expected_discharge_datetime",             "Expected discharge date/time (PV2-9)"),
        _int_field("estimated_length_of_inpatient_stay",       "Estimated length of inpatient stay in days (PV2-10)"),
        _int_field("actual_length_of_inpatient_stay",          "Actual length of inpatient stay in days (PV2-11)"),
        _s("visit_description",                        "Free-text visit description (PV2-12)"),
    ]
    + _xcn_array_schema("referral_source", "Referral source (XCN, repeatable per spec)", "PV2-13")
    + [
        _ts("previous_service_date",                   "Date of previous service (PV2-14, DT)"),
        _s("employment_illness_related_indicator",     "Employment illness related Y/N (PV2-15)"),
    ]
    + _cwe_schema("purge_status_code", "Purge status code (CWE)", "PV2-16")
    + [
        _ts("purge_status_date",                       "Purge status date (PV2-17, DT)"),
    ]
    + _cwe_schema("special_program_code", "Special program code (CWE)", "PV2-18")
    + [
        _s("retention_indicator",                      "Retention indicator Y/N (PV2-19)"),
        _int_field("expected_number_of_insurance_plans",       "Expected number of insurance plans (PV2-20)"),
    ]
    + _cwe_schema("visit_publicity_code", "Visit publicity code (CWE)", "PV2-21")
    + [
        _s("visit_protection_indicator",               "Visit protection indicator ID code (PV2-22)"),
    ]
    + _xon_array_schema("clinic_organization", "Clinic organization (XON, repeatable per spec)", "PV2-23")
    + _cwe_schema("patient_status_code", "Patient status code (CWE)", "PV2-24")
    + _cwe_schema("visit_priority_code", "Visit priority code (CWE)", "PV2-25")
    + [
        _ts("previous_treatment_date",                 "Previous treatment date (PV2-26, DT)"),
    ]
    + _cwe_schema("expected_discharge_disposition", "Expected discharge disposition (CWE)", "PV2-27")
    + [
        _ts("signature_on_file_date",                  "Signature on file date (PV2-28, DT)"),
        _ts("first_similar_illness_date",              "Date of first similar illness (PV2-29, DT)"),
    ]
    + _cwe_schema("patient_charge_adjustment_code", "Patient charge adjustment code", "PV2-30")
    + _cwe_schema("recurring_service_code", "Recurring service code (CWE)", "PV2-31")
    + [
        _s("billing_media_code",                       "Billing media code Y/N (PV2-32)"),
        _ts("expected_surgery_datetime",               "Expected surgery date/time (PV2-33)"),
        _s("military_partnership_code",                "Military partnership code Y/N (PV2-34)"),
        _s("military_non_availability_code",           "Military non-availability code Y/N (PV2-35)"),
        _s("newborn_baby_indicator",                   "Newborn baby indicator Y/N (PV2-36)"),
        _s("baby_detained_indicator",                  "Baby detained indicator Y/N (PV2-37)"),
    ]
    + _cwe_schema("mode_of_arrival_code", "Mode of arrival code", "PV2-38")
    + _cwe_array_schema("recreational_drug_use_code", "Recreational drug use code (CWE, repeatable per spec)", "PV2-39")
    + _cwe_schema("admission_level_of_care_code", "Admission level of care code", "PV2-40")
    + _cwe_array_schema("precaution_code", "Precaution code (CWE, repeatable per spec)", "PV2-41")
    + _cwe_schema("patient_condition_code", "Patient condition code", "PV2-42")
    + _cwe_schema("living_will_code", "Living will code (CWE)", "PV2-43")
    + _cwe_schema("organ_donor_code", "Organ donor code (CWE)", "PV2-44")
    + _cwe_array_schema("advance_directive_code", "Advance directive (CWE, repeatable per spec)", "PV2-45")
    + [
        _ts("patient_status_effective_date",           "Patient status effective date (PV2-46, DT)"),
        _ts("expected_loa_return_datetime",            "Expected leave of absence return date/time (PV2-47)"),
        _ts("expected_preadmission_testing_datetime",  "Expected pre-admission testing date/time (PV2-48)"),
    ]
    + _cwe_array_schema("notify_clergy_code", "Notify clergy code (CWE, repeatable)", "PV2-49")
    + [
        _ts("advance_directive_last_verified_date",    "Date advance directive was last verified (PV2-50, DT, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# MRG — Merge Patient Information
# ---------------------------------------------------------------------------

MRG_SCHEMA = StructType(
    _METADATA_FIELDS
    + _cx_array_schema("prior_patient_id", "Prior patient identifier list (CX, repeatable per spec)", "MRG-1")
    + _cx_array_schema("prior_alternate_patient_id", "Prior alternate patient ID list (CX, repeatable per spec)", "MRG-2")
    + _cx_schema("prior_patient_account_number", "Prior patient account number", "MRG-3")
    + _cx_schema("prior_patient_id_external", "Prior patient ID — external (MRG-4, backward-compat v2.3)", "MRG-4")
    + _cx_schema("prior_visit_number", "Prior visit number", "MRG-5")
    + _cx_schema("prior_alternate_visit_id", "Prior alternate visit ID (CX)", "MRG-6")
    + [
        *_xpn_array_schema("prior_patient_names", "Prior patient names", "MRG-7"),
    ]
)

# ---------------------------------------------------------------------------
# IAM — Patient Adverse Reaction Information
# ---------------------------------------------------------------------------

IAM_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                          "Sequence number for this IAM segment within the message (IAM-1)"),
    ]
    + _cwe_schema("allergen_type_code", "Allergen type", "IAM-2")
    + _cwe_schema("allergen_code", "Allergen (CWE)", "IAM-3")
    + _cwe_schema("allergy_severity_code", "Allergy severity", "IAM-4")
    + _cwe_array_schema(
        "allergy_reaction",
        # Same lenient ST 0..* -> CWE-shape struct modeling as AL1-5.
        "Allergy reaction (spec ST 0..*; modeled as repeating CWE-shape struct for lenient parsing)",
        "IAM-5",
    )
    + _cwe_schema("allergy_action_code", "Allergy action code", "IAM-6")
    + _ei_schema("allergy_unique_identifier", "Unique allergy identifier", "IAM-7")
    + [
        _s("action_reason",                       "Reason for the action (IAM-8)"),
    ]
    + _cwe_schema("sensitivity_to_causative_agent_code", "Sensitivity to causative agent", "IAM-9")
    + _cwe_schema("allergen_group_code", "Allergen group", "IAM-10")
    + [
        _ts("onset_date",                         "Allergy onset date (IAM-11, DT)"),
        _s("onset_date_text",                     "Free-text onset date description (IAM-12)"),
        _ts("reported_datetime",                  "When the allergy was reported (IAM-13)"),
    ]
    + _xcn_schema("reported_by", "Reported by (spec XPN; modeled as XCN for legacy senders that ship ID in comp 1)", "IAM-14")
    + _cwe_schema("relationship_to_patient_code", "Relationship to patient", "IAM-15")
    + _cwe_schema("alert_device_code", "Alert device code", "IAM-16")
    + _cwe_schema("allergy_clinical_status_code", "Allergy clinical status", "IAM-17")
    + _xcn_schema("statused_by_person", "Person who set the status", "IAM-18")
    + _xon_schema("statused_by_organization", "Organization that set the status", "IAM-19")
    + [
        _ts("statused_at_datetime",               "Date/time status was set (IAM-20)"),
    ]
    + _xcn_schema("inactivated_by_person", "Person who inactivated the record", "IAM-21")
    + [
        _ts("inactivated_datetime",               "Date/time the record was inactivated (IAM-22, v2.6+)"),
    ]
    + _xcn_schema("initially_recorded_by_person", "Person who initially recorded the reaction", "IAM-23")
    + [
        _ts("initially_recorded_datetime",        "Date/time the reaction was initially recorded (IAM-24, v2.6+)"),
    ]
    + _xcn_schema("modified_by_person", "Person who last modified the record", "IAM-25")
    + [
        _ts("modified_datetime",                  "Date/time the record was last modified (IAM-26, v2.6+)"),
    ]
    + _cwe_schema("clinician_identified_allergen_code", "Clinician-identified allergen code (CWE)", "IAM-27")
    + _xon_schema("initially_recorded_by_organization", "Organization that initially recorded the reaction", "IAM-28")
    + _xon_schema("modified_by_organization", "Organization that last modified the record", "IAM-29")
    + _xon_schema("inactivated_by_organization", "Organization that inactivated the record", "IAM-30")
)

# ---------------------------------------------------------------------------
# PR1 — Procedures
# ---------------------------------------------------------------------------

PR1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                    "Sequence number for this PR1 segment within the message (PR1-1)"),
        _s("procedure_coding_method",      "Procedure coding method (PR1-2, deprecated)"),
    ]
    + _cwe_schema("procedure_code", "Procedure (CWE)", "PR1-3")
    + [
        _s("procedure_description",        "Procedure description (PR1-4, deprecated)"),
        _ts("procedure_datetime",          "When the procedure was performed (PR1-5)"),
    ]
    + _cwe_schema("procedure_functional_type", "Procedure functional type (CWE; e.g. A=Anesthesia, P=Procedure, I=Invasion)", "PR1-6")
    + [
        _int_field("procedure_minutes",            "Duration of the procedure in minutes (PR1-7)"),
    ]
    + _xcn_array_schema("anesthesiologist", "Anesthesiologist (XCN, repeatable, deprecated v2.3)", "PR1-8")
    + _cwe_schema("anesthesia_code", "Anesthesia code (CWE)", "PR1-9")
    + [
        _int_field("anesthesia_minutes",           "Anesthesia duration in minutes (PR1-10)"),
    ]
    + _xcn_array_schema("surgeon", "Surgeon (XCN, repeatable, deprecated v2.3)", "PR1-11")
    + _xcn_array_schema("procedure_practitioner", "Procedure practitioner (XCN, repeatable, deprecated v2.3)", "PR1-12")
    + _cwe_schema("consent_code", "Consent", "PR1-13")
    + [
        _s("procedure_priority",           "Procedure priority (PR1-14)"),
    ]
    + _cwe_schema("associated_diagnosis_code", "Associated diagnosis", "PR1-15")
    + _cwe_array_schema("procedure_code_modifier", "Procedure code modifier (CNE, repeatable per spec; CWE-shape struct since CNE shares components)", "PR1-16")
    + _cwe_schema("procedure_drg_type", "Procedure DRG type (CWE)", "PR1-17")
    + _cwe_array_schema("tissue_type_code", "Tissue type (CWE, repeatable per spec)", "PR1-18")
    + _ei_schema("procedure_identifier", "Procedure identifier (EI)", "PR1-19")
    + [
        _s("procedure_action_code",        "Action code (PR1-20): A=Add, D=Delete, U=Update"),
    ]
    + _cwe_schema("drg_procedure_determination_status", "DRG procedure determination status", "PR1-21")
    + _cwe_schema("drg_procedure_relevance", "DRG procedure relevance", "PR1-22")
    + _pl_array_schema("treating_organizational_unit",  "Treating organizational unit (PL, repeatable per spec)", "PR1-23")
    + [
        _s("respiratory_within_surgery",    "Respiratory within surgery indicator (PR1-24, v2.7+)"),
    ]
    + _ei_schema("parent_procedure_id", "Parent procedure (EI)", "PR1-25")
)

# ---------------------------------------------------------------------------
# ORC — Common Order
# ---------------------------------------------------------------------------

ORC_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                               "Synthetic sequence number for multiple ORC segments per message"),
        _s("order_control",                            "Order control code (ORC-1): NW=New, CA=Cancel, DC=Discontinue, XO=Change, SC=Status"),
    ]
    + _ei_schema("placer_order_number", "Placer order number (EI)", "ORC-2")
    + _ei_schema("filler_order_number", "Filler order number (EI)", "ORC-3")
    + _ei_schema("placer_group_number", "Placer group number (EI)", "ORC-4")
    + [
        _s("order_status",                             "Order status (ORC-5): IP=In Process, CM=Completed, SC=Scheduled, CA=Cancelled"),
        _s("response_flag",                            "Response flag (ORC-6): E=Report exceptions, R=Same as initiation, D=Deferred, N=Notification"),
    ]
    + _tq_array_schema("quantity_timing", "Quantity/timing (TQ, repeatable, deprecated in v2.5)", "ORC-7")
    + _eip_array_schema("parent_order", "Parent order reference (EIP, repeatable per spec)", "ORC-8")
    + [
        _ts("datetime_of_transaction",                 "Transaction date/time (ORC-9)"),
    ]
    + _xcn_array_schema("entered_by", "Person who entered the order (XCN, repeatable per spec)", "ORC-10")
    + _xcn_array_schema("verified_by", "Person who verified the order (XCN, repeatable per spec)", "ORC-11")
    + _xcn_array_schema("ordering_provider", "Ordering provider (XCN, repeatable per spec)", "ORC-12")
    + _pl_schema("enterers_location", "Location where order was entered (PL)", "ORC-13")
    + _xtn_array_schema("call_back_phone", "Callback phone (XTN, repeatable per spec)", "ORC-14")
    + [
        _ts("order_effective_datetime",                "Order effective date/time (ORC-15)"),
    ]
    + _cwe_schema("order_control_code_reason", "Order control code reason", "ORC-16")
    + _cwe_schema("entering_organization", "Entering organization", "ORC-17")
    + _cwe_schema("entering_device", "Entering device", "ORC-18")
    + _xcn_array_schema("action_by", "Person who actioned the order (XCN, repeatable per spec)", "ORC-19")
    + _cwe_schema("advanced_beneficiary_notice_code", "Advanced beneficiary notice (ABN) code", "ORC-20")
    + _xon_array_schema("ordering_facility_name", "Ordering facility name (XON, repeatable per spec)", "ORC-21")
    + _xad_array_schema("ordering_facility_address", "Ordering facility address (XAD, repeatable per spec)", "ORC-22")
    + _xtn_array_schema("ordering_facility_phone", "Ordering facility phone (XTN, repeatable per spec)", "ORC-23")
    + _xad_array_schema("ordering_provider_address", "Ordering provider address (XAD, repeatable per spec)", "ORC-24")
    + _cwe_schema("order_status_modifier", "Order status modifier", "ORC-25")
    + _cwe_schema("abn_override_reason", "ABN override reason", "ORC-26")
    + [
        _ts("fillers_expected_availability_datetime",  "Filler's expected availability date/time (ORC-27)"),
    ]
    + _cwe_schema("confidentiality_code", "Confidentiality code", "ORC-28")
    + _cwe_schema("order_type", "Order type", "ORC-29")
    + _cwe_schema("enterer_authorization_mode", "Enterer authorization mode", "ORC-30")
    + _cwe_schema("parent_universal_service_id", "Parent universal service identifier", "ORC-31")
    + [
        _ts("advanced_beneficiary_notice_date",         "Advanced beneficiary notice date (ORC-32, DT, v2.6+)"),
    ]
    + _cx_array_schema("alternate_placer_order_number", "Alternate placer order number (CX, repeatable per spec)", "ORC-33")
    + _cwe_array_schema("order_workflow_profile", "Order workflow profile (CWE, repeatable per spec, v2.9+)", "ORC-34")
    + [
        _s("action_code",                           "Action code (ORC-35, v2.9+)"),
        _ts("order_status_date_range_start",            "Order status date range start (ORC-36.1, DR, v2.9+)"),
        _ts("order_status_date_range_end",              "Order status date range end (ORC-36.2, DR, v2.9+)"),
        _ts("order_creation_datetime",                  "Order creation date/time (ORC-37, v2.9+)"),
    ]
    + _ei_schema("filler_order_group_number", "Filler order group number (EI)", "ORC-38")
)

# ---------------------------------------------------------------------------
# NTE — Notes and Comments
# ---------------------------------------------------------------------------

NTE_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",            "Sequence number for this NTE segment within the message (NTE-1)"),
        _s("source_of_comment",    "Source of comment (NTE-2): L=Ancillary/Filler, P=Orderer/Placer, O=Other"),
        _s_array("comment",        "Free-text comment/note content (NTE-3, FT, repeatable per spec) — ARRAY<STRING>"),
    ]
    + _cwe_schema("comment_type", "Comment type", "NTE-4")
    + _xcn_schema("entered_by", "Person who entered the note", "NTE-5")
    + [
        _ts("entered_datetime",    "Date/time the note was entered (NTE-6, v2.6+)"),
        _ts("effective_start_date","Effective start date of the note (NTE-7, v2.6+)"),
        _ts("expiration_date",     "Expiration date of the note (NTE-8, v2.6+)"),
    ]
    + _cwe_array_schema("coded_comment", "Coded comment (CWE, repeatable per spec)", "NTE-9")
)

# ---------------------------------------------------------------------------
# SPM — Specimen
# ---------------------------------------------------------------------------

SPM_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                      "Sequence number for this SPM segment within the message (SPM-1)"),
    ]
    + _eip_schema("specimen_id", "Specimen identifier (EIP, single instance per spec [0..1], SPM-2)", "SPM-2")
    + _eip_array_schema("specimen_parent_ids", "Parent specimen identifiers (EIP, repeatable per spec)", "SPM-3")
    + _cwe_schema("specimen_type", "Specimen type", "SPM-4")
    + _cwe_array_schema("specimen_type_modifier", "Specimen type modifier (CWE, repeatable per spec)", "SPM-5")
    + _cwe_array_schema("specimen_additives", "Specimen additives/preservatives (CWE, repeatable per spec)", "SPM-6")
    + _cwe_schema("specimen_collection_method", "Collection method", "SPM-7")
    + _cwe_schema("specimen_source_site", "Source body site", "SPM-8")
    + _cwe_array_schema("specimen_source_site_modifier", "Source site modifier (CWE, repeatable per spec)", "SPM-9")
    + _cwe_schema("specimen_collection_site", "Collection site", "SPM-10")
    + _cwe_array_schema("specimen_role", "Specimen role (CWE, repeatable per spec)", "SPM-11")
    + _cq_schema("specimen_collection_amount", "Collection amount with units (CQ)", "SPM-12")
    + [
        _int_field("grouped_specimen_count",          "Number of grouped specimens (SPM-13)"),
    ]
    + [
        _s_array("specimen_description",      "Free-text specimen description (SPM-14, ST, repeatable per spec) — ARRAY<STRING>"),
    ]
    + _cwe_array_schema("specimen_handling_code", "Handling instructions code (CWE, repeatable per spec)", "SPM-15")
    + _cwe_array_schema("specimen_risk_code", "Risk code (CWE, repeatable per spec)", "SPM-16")
    + [
        _ts("specimen_collection_datetime_start",  "Specimen collection date/time range start (SPM-17.1, DR)"),
        _ts("specimen_collection_datetime_end",    "Specimen collection date/time range end (SPM-17.2, DR)"),
        _ts("specimen_received_datetime",     "When specimen was received (SPM-18)"),
        _ts("specimen_expiration_datetime",   "Specimen expiration date/time (SPM-19)"),
        _s("specimen_availability",           "Specimen availability Y/N (SPM-20)"),
    ]
    + _cwe_array_schema("specimen_reject_reason", "Reject reason (CWE, repeatable per spec)", "SPM-21")
    + _cwe_schema("specimen_quality", "Quality assessment", "SPM-22")
    + _cwe_schema("specimen_appropriateness", "Appropriateness assessment", "SPM-23")
    + _cwe_array_schema("specimen_condition", "Specimen condition (CWE, repeatable per spec)", "SPM-24")
    + _cq_schema("specimen_current_quantity", "Current specimen quantity (CQ)", "SPM-25")
    + [
        _int_field("number_of_specimen_containers",   "Number of specimen containers (SPM-26)"),
    ]
    + _cwe_schema("container_type", "Container type", "SPM-27")
    + _cwe_schema("container_condition", "Container condition", "SPM-28")
    + _cwe_schema("specimen_child_role", "Specimen child role", "SPM-29")
    + _cx_array_schema("accession_id", "Accession identifier (CX, repeatable per spec)", "SPM-30")
    + _cx_array_schema("other_specimen_id", "Other specimen identifier (CX, repeatable per spec)", "SPM-31")
    + _ei_schema("shipment_id", "Shipment identifier (EI)", "SPM-32")
    + [
        _ts("culture_start_datetime",         "Culture start date/time (SPM-33, v2.9+)"),
        _ts("culture_final_datetime",         "Culture final date/time (SPM-34, v2.9+)"),
        _s("action_code",                 "Action code (SPM-35, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# IN1 — Insurance
# ---------------------------------------------------------------------------

IN1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                       "Sequence number for this IN1 segment within the message (IN1-1)"),
    ]
    + _cwe_schema("insurance_plan", "Insurance plan", "IN1-2")
    + _cx_array_schema("insurance_company", "Insurance company ID (CX, repeatable per spec, v2.9+)", "IN1-3")
    + _xon_array_schema("insurance_company_name", "Insurance company name (XON, repeatable per spec)", "IN1-4")
    + _xad_array_schema("insurance_company_address", "Insurance company address (XAD, repeatable per spec)", "IN1-5")
    + [
        *_xpn_array_schema("insurance_co_contacts", "Insurance contacts", "IN1-6"),
    ]
    + _xtn_array_schema("insurance_co_phone_number", "Insurance company phone (XTN, repeatable per spec)", "IN1-7")
    + [
        _s("group_number",                     "Insurance group/policy group number (IN1-8)"),
    ]
    + _xon_array_schema("group_name", "Insurance group name (XON, repeatable per spec)", "IN1-9")
    + _cx_array_schema("insureds_group_emp", "Insured's group employer ID (CX, repeatable per spec, v2.9+)", "IN1-10")
    + _xon_array_schema("insureds_group_emp_name", "Insured's group employer name (XON, repeatable per spec)", "IN1-11")
    + [
        _ts("plan_effective_date",             "Plan effective date (IN1-12, DT)"),
        _ts("plan_expiration_date",            "Plan expiration date (IN1-13, DT)"),
    ]
    + _aui_schema("authorization_information", "Authorization information (AUI)", "IN1-14")
    + _cwe_schema("plan_type", "Plan type (CWE)", "IN1-15")
    + [
        *_xpn_array_schema("insured_names", "Insured person names", "IN1-16"),
    ]
    + _cwe_schema("insureds_relationship_to_patient", "Insured's relationship to patient", "IN1-17")
    + [
        _ts("insureds_date_of_birth",          "Insured's date of birth (IN1-18)"),
    ]
    + _xad_array_schema("insureds_address", "Insured's address (XAD, repeatable per spec)", "IN1-19")
    + _cwe_schema("assignment_of_benefits", "Assignment of benefits (CWE)", "IN1-20")
    + _cwe_schema("coordination_of_benefits", "Coordination of benefits (CWE)", "IN1-21")
    + [
        _s("coord_of_ben_priority",            "COB priority (IN1-22)"),
        _s("notice_of_admission_flag",         "Notice of admission flag Y/N (IN1-23)"),
        _ts("notice_of_admission_date",        "Admission notice date (IN1-24, DT)"),
        _s("report_of_eligibility_flag",       "Eligibility report flag Y/N (IN1-25)"),
        _ts("report_of_eligibility_date",      "Eligibility report date (IN1-26, DT)"),
    ]
    + _cwe_schema("release_information_code", "Release information code (CWE)", "IN1-27")
    + [
        _s("pre_admit_cert",                   "Pre-admission certification number (IN1-28)"),
        _ts("verification_datetime",           "Verification date/time (IN1-29)"),
    ]
    + _xcn_array_schema("verification_by", "Verified by (XCN, repeatable per spec)", "IN1-30")
    + _cwe_schema("type_of_agreement_code", "Type-of-agreement code (CWE)", "IN1-31")
    + _cwe_schema("billing_status", "Billing status (CWE)", "IN1-32")
    + [
        _int_field("lifetime_reserve_days",            "Lifetime reserve days (IN1-33)"),
        _int_field("delay_before_lr_day",              "Delay before lifetime reserve day (IN1-34)"),
    ]
    + _cwe_schema("company_plan_code", "Company plan code (CWE)", "IN1-35")
    + [
        _s("policy_number",                    "Policy number (IN1-36)"),
    ]
    + _cp_schema("policy_deductible", "Policy deductible amount (CP)", "IN1-37")
    + _cp_schema("policy_limit_amount", "Policy limit amount (CP, withdrawn v2.8; retained for backward compatibility)", "IN1-38")
    + [
        _int_field("policy_limit_days",                "Policy limit in days (IN1-39)"),
    ]
    + _cp_schema("room_rate_semi_private", "Semi-private room rate (CP, withdrawn v2.8; retained for backward compatibility)", "IN1-40")
    + _cp_schema("room_rate_private", "Private room rate (CP, withdrawn v2.8; retained for backward compatibility)", "IN1-41")
    + _cwe_schema("insureds_employment_status", "Insured's employment status", "IN1-42")
    + _cwe_schema("insureds_administrative_sex", "Insured's administrative sex", "IN1-43")
    + _xad_array_schema("insureds_employers_address", "Insured's employer address (XAD, repeatable per spec)", "IN1-44")
    + [
        _s("verification_status",              "Verification status (IN1-45)"),
    ]
    + _cwe_schema("prior_insurance_plan_id", "Prior insurance plan ID (CWE)", "IN1-46")
    + _cwe_schema("coverage_type", "Coverage type (CWE)", "IN1-47")
    + _cwe_schema("handicap", "Handicap (CWE)", "IN1-48")
    + _cx_array_schema("insureds_id_number", "Insured's ID (CX, repeatable per spec)", "IN1-49")
    + _cwe_schema("signature_code", "Signature code (CWE)", "IN1-50")
    + [
        _ts("signature_code_date",             "Signature code date (IN1-51, DT)"),
        _s("insureds_birth_place",             "Insured's birth place (IN1-52)"),
    ]
    + _cwe_schema("vip_indicator", "VIP indicator (CWE)", "IN1-53")
    + _cwe_array_schema("external_health_plan_identifiers", "External health plan identifiers (CWE, repeatable per spec, v2.8+)", "IN1-54")
    + [
        _s("insurance_action_code",            "Insurance action code (IN1-55, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# GT1 — Guarantor
# ---------------------------------------------------------------------------

GT1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                           "Sequence number for this GT1 segment within the message (GT1-1)"),
    ]
    + _cx_array_schema("guarantor_number", "Guarantor number (CX, repeatable per spec)", "GT1-2")
    + [
        *_xpn_array_schema("guarantor_names", "Guarantor names", "GT1-3"),
        *_xpn_array_schema("guarantor_spouse_names", "Guarantor spouse names", "GT1-4"),
    ]
    + _xad_array_schema("guarantor_address", "Guarantor address (XAD, repeatable per spec)", "GT1-5")
    + _xtn_array_schema("guarantor_ph_num_home", "Guarantor home phone (XTN, repeatable per spec)", "GT1-6")
    + _xtn_array_schema("guarantor_ph_num_business", "Guarantor business phone (XTN, repeatable per spec)", "GT1-7")
    + [
        _ts("guarantor_date_of_birth",             "Guarantor date of birth (GT1-8)"),
    ]
    + _cwe_schema("guarantor_administrative_sex", "Guarantor administrative sex", "GT1-9")
    + _cwe_schema("guarantor_type", "Guarantor type (CWE)", "GT1-10")
    + _cwe_schema("guarantor_relationship", "Guarantor relationship to patient", "GT1-11")
    + [
        _s("guarantor_ssn",                        "Guarantor social security number (GT1-12)"),
        _ts("guarantor_date_begin",                "Guarantor start date (GT1-13, DT)"),
        _ts("guarantor_date_end",                  "Guarantor end date (GT1-14, DT)"),
        _int_field("guarantor_priority",                   "Guarantor priority (GT1-15)"),
        *_xpn_array_schema("guarantor_employer_names", "Guarantor employer names", "GT1-16"),
    ]
    + _xad_array_schema("guarantor_employer_address", "Guarantor employer address (XAD, repeatable per spec)", "GT1-17")
    + _xtn_array_schema("guarantor_employer_phone_number", "Guarantor employer phone (XTN, repeatable per spec)", "GT1-18")
    + _cx_array_schema("guarantor_employee_id_number", "Guarantor employee ID (CX, repeatable per spec)", "GT1-19")
    + _cwe_schema("guarantor_employment_status", "Guarantor employment status (CWE)", "GT1-20")
    + _xon_array_schema("guarantor_organization_name", "Guarantor organization name (XON, repeatable per spec)", "GT1-21")
    + [
        _s("guarantor_billing_hold_flag",          "Billing hold flag Y/N (GT1-22)"),
    ]
    + _cwe_schema("guarantor_credit_rating_code", "Guarantor credit rating", "GT1-23")
    + [
        _ts("guarantor_death_date_and_time",       "Guarantor death date/time (GT1-24)"),
        _s("guarantor_death_flag",                 "Guarantor death flag Y/N (GT1-25)"),
    ]
    + _cwe_schema("guarantor_charge_adjustment_code", "Guarantor charge adjustment", "GT1-26")
    + _cp_schema("guarantor_household_annual_income", "Guarantor household annual income (CP)", "GT1-27")
    + [
        _int_field("guarantor_household_size",             "Household size (GT1-28)"),
    ]
    + _cx_array_schema("guarantor_employer_id_number", "Guarantor employer ID (CX, repeatable per spec)", "GT1-29")
    + _cwe_schema("guarantor_marital_status_code", "Guarantor marital status", "GT1-30")
    + [
        _ts("guarantor_hire_effective_date",       "Guarantor hire date (GT1-31, DT)"),
        _ts("employment_stop_date",                "Employment stop date (GT1-32, DT)"),
    ]
    + _cwe_schema("living_dependency", "Living dependency (CWE)", "GT1-33")
    + _cwe_array_schema("ambulatory_status", "Ambulatory status (CWE, repeatable per spec)", "GT1-34")
    + _cwe_array_schema("citizenship", "Citizenship (CWE, repeatable per spec)", "GT1-35")
    + _cwe_schema("primary_language", "Primary language", "GT1-36")
    + _cwe_schema("living_arrangement", "Living arrangement (CWE)", "GT1-37")
    + _cwe_schema("publicity_code", "Publicity code", "GT1-38")
    + [
        _s("protection_indicator",                 "Protection indicator Y/N (GT1-39)"),
    ]
    + _cwe_schema("student_indicator", "Student indicator (CWE)", "GT1-40")
    + _cwe_schema("religion", "Religion", "GT1-41")
    + [
        *_xpn_array_schema("mothers_maiden_names", "GT1 mother's maiden names", "GT1-42"),
    ]
    + _cwe_schema("nationality", "Nationality", "GT1-43")
    + _cwe_array_schema("ethnic_group", "Ethnic group (CWE, repeatable per spec)", "GT1-44")
    + [
        *_xpn_array_schema("contact_persons", "GT1 contact persons", "GT1-45"),
    ]
    + _xtn_array_schema("contact_persons_telephone_number", "Contact person phone (XTN, repeatable per spec)", "GT1-46")
    + _cwe_schema("contact_reason", "Contact reason", "GT1-47")
    + _cwe_schema("contact_relationship", "Contact relationship (CWE)", "GT1-48")
    + [
        _s("job_title",                            "Job title (GT1-49)"),
    ]
    + _jcc_schema("job_code_class", "Job code/class (JCC)", "GT1-50")
    + _xon_array_schema("guarantor_employers_org_name", "Guarantor employer organization name (XON, repeatable per spec)", "GT1-51")
    + _cwe_schema("handicap", "Handicap (CWE)", "GT1-52")
    + _cwe_schema("job_status", "Job status (CWE)", "GT1-53")
    + _fc_schema("guarantor_financial_class", "Guarantor financial class (FC)", "GT1-54")
    + _cwe_array_schema("guarantor_race", "Guarantor race (CWE, repeatable per spec)", "GT1-55")
    + [
        _s("guarantor_birth_place",                "Guarantor birth place (GT1-56)"),
    ]
    + _cwe_schema("vip_indicator", "VIP indicator (CWE)", "GT1-57")
)

# ---------------------------------------------------------------------------
# FT1 — Financial Transaction
# ---------------------------------------------------------------------------

FT1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                              "Sequence number for this FT1 segment within the message (FT1-1)"),
    ]
    + _cx_schema("transaction_id", "Transaction identifier (CX, v2.9+)", "FT1-2")
    + [
        _s("transaction_batch_id",                    "Batch identifier (FT1-3)"),
        _ts("transaction_date_start",                 "Transaction date/time range start (FT1-4.1, DR)"),
        _ts("transaction_date_end",                   "Transaction date/time range end (FT1-4.2, DR)"),
        _ts("transaction_posting_date",               "Posting date/time (FT1-5)"),
    ]
    + _cwe_schema("transaction_type", "Transaction type (CWE; e.g. CG=Charge, CR=Credit, PA=Payment, AJ=Adjustment)", "FT1-6")
    + _cwe_schema("transaction_code", "Transaction / charge code", "FT1-7")
    + [
        _s("transaction_description",                 "Transaction description (FT1-8, deprecated)"),
        _s("transaction_description_alt",             "Alternate transaction description (FT1-9, deprecated)"),
        _int_field("transaction_quantity",                    "Transaction quantity (FT1-10)"),
    ]
    + _cp_schema("transaction_amount_extended", "Extended amount: quantity x unit price (CP)", "FT1-11")
    + _cp_schema("transaction_amount_unit", "Unit price (CP)", "FT1-12")
    + _cwe_schema("department_code", "Department", "FT1-13")
    + _cwe_schema("insurance_plan_id", "Insurance plan / health plan ID (CWE)", "FT1-14")
    + _cp_schema("insurance_amount", "Insurance amount (CP)", "FT1-15")
    + _pl_schema("assigned_patient_location", "Assigned patient location (PL)", "FT1-16")
    + _cwe_schema("fee_schedule", "Fee schedule (CWE)", "FT1-17")
    + _cwe_schema("patient_type", "Patient type (CWE)", "FT1-18")
    + _cwe_array_schema("diagnosis_code", "Diagnosis (CWE, repeatable per spec)", "FT1-19")
    + _xcn_array_schema("performed_by", "Performed by (XCN, repeatable per spec)", "FT1-20")
    + _xcn_array_schema("ordered_by", "Ordered by (XCN, repeatable per spec)", "FT1-21")
    + _cp_schema("unit_cost", "Unit cost (CP)", "FT1-22")
    + _ei_schema("filler_order_number", "Filler order number (EI)", "FT1-23")
    + _xcn_array_schema("entered_by", "Entered by (XCN, repeatable per spec)", "FT1-24")
    + _cwe_schema("procedure_code", "Procedure code (CNE; CWE-compatible struct)", "FT1-25")
    + _cwe_array_schema("procedure_code_modifier", "Procedure code modifier (CNE, repeatable per spec; CWE-compatible struct)", "FT1-26")
    + _cwe_schema("advanced_beneficiary_notice_code", "Advanced beneficiary notice (ABN) code (CWE)", "FT1-27")
    + _cwe_schema("medically_necessary_dup_proc_reason", "Medically-necessary duplicate procedure reason (CWE)", "FT1-28")
    + _cwe_schema("ndc_code", "NDC code (CWE)", "FT1-29")
    + _cx_schema("payment_reference_id", "Payment reference ID (CX)", "FT1-30")
    + [
        _s_array("transaction_reference_key",         "Transaction reference key (FT1-31, SI; repeatable per spec — array of FT1-1 set-IDs linking payment to corresponding charges)"),
    ]
    + _xon_array_schema("performing_facility", "Performing facility (XON, repeatable per spec, v2.6+)", "FT1-32")
    + _xon_schema("ordering_facility", "Ordering facility (XON, v2.6+)", "FT1-33")
    + _cwe_schema("item_number", "Item number (CWE, v2.6+)", "FT1-34")
    + [
        _s("model_number",                            "Model number (FT1-35, v2.6+)"),
    ]
    + _cwe_array_schema("special_processing_code", "Special processing code (CWE, repeatable per spec, v2.6+)", "FT1-36")
    + _cwe_schema("clinic_code", "Clinic code (CWE, v2.6+)", "FT1-37")
    + _cx_schema("referral_number", "Referral number (CX, v2.6+)", "FT1-38")
    + _cx_schema("authorization_number", "Authorization number (CX, v2.6+)", "FT1-39")
    + _cwe_schema("service_provider_taxonomy_code", "Service provider taxonomy code (CWE, v2.6+)", "FT1-40")
    + _cwe_schema("revenue_code", "Revenue code (CWE, v2.6+)", "FT1-41")
    + [
        _s("prescription_number",                     "Prescription number (FT1-42, v2.6+)"),
    ]
    + _cq_schema("ndc_qty_and_uom", "NDC quantity and unit of measure (CQ, v2.6+)", "FT1-43")
    + _cwe_schema("dme_certificate_of_medical_necessity_transmission_code", "DME certificate of medical necessity transmission code (CWE, v2.9+)", "FT1-44")
    + _cwe_schema("dme_certification_type_code", "DME certification type code (CWE, v2.9+)", "FT1-45")
    + [
        _s("dme_duration_value",                      "DME duration value (FT1-46, v2.9+)"),
        _s("dme_certification_revision_date",         "DME certification revision date (FT1-47, v2.9+)"),
        _s("dme_initial_certification_date",          "DME initial certification date (FT1-48, v2.9+)"),
        _s("dme_last_certification_date",             "DME last certification date (FT1-49, v2.9+)"),
        _s("dme_length_of_medical_necessity_days",    "DME length of medical necessity in days (FT1-50, v2.9+)"),
    ]
    + _mo_schema("dme_rental_price", "DME rental price (MO, v2.9+)", "FT1-51")
    + _mo_schema("dme_purchase_price", "DME purchase price (MO, v2.9+)", "FT1-52")
    + _cwe_schema("dme_frequency_code", "DME frequency code (CWE, v2.9+)", "FT1-53")
    + [
        _s("dme_certification_condition_indicator",   "DME certification condition indicator (FT1-54, v2.9+)"),
    ]
    + _cwe_array_schema("dme_condition_indicator_code", "DME condition indicator code (CWE, repeatable per spec, v2.9+)", "FT1-55")
    + _cwe_schema("service_reason_code", "Service reason code (CWE, v2.9+)", "FT1-56")
)

# ---------------------------------------------------------------------------
# RXA — Pharmacy/Treatment Administration
# ---------------------------------------------------------------------------

RXA_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_int_field("set_id",                                "Give sub-ID counter (RXA-1)"),
        _int_field("administration_sub_id_counter",            "Administration sub-ID counter (RXA-2)"),
        _ts("datetime_start_of_administration",        "Administration start date/time (RXA-3)"),
        _ts("datetime_end_of_administration",          "Administration end date/time (RXA-4)"),
    ]
    + _cwe_schema("administered_code", "Drug / vaccine administered", "RXA-5")
    + [
        _s("administered_amount",                      "Amount administered (RXA-6)"),
    ]
    + _cwe_schema("administered_units", "Units of measure", "RXA-7")
    + _cwe_schema("administered_dosage_form", "Dosage form", "RXA-8")
    + _cwe_array_schema("administration_notes", "Administration notes (CWE, repeatable per spec)", "RXA-9")
    + _xcn_array_schema("administering_provider", "Provider who administered (XCN, repeatable per spec)", "RXA-10")
    + [
        _s("administered_at_location",                 "Administration location (RXA-11)"),
        _s("administered_per_time_unit",               "Rate time unit (RXA-12)"),
        _s("administered_strength",                    "Strength administered (RXA-13)"),
    ]
    + _cwe_schema("administered_strength_units", "Administered strength units (CWE)", "RXA-14")
    + [
        _s_array("substance_lot_number",               "Lot numbers (RXA-15, ST repeatable per spec)"),
        _s_array("substance_expiration_date",          "Substance expiration dates (RXA-16, DTM repeatable per spec)"),
    ]
    + _cwe_array_schema("substance_manufacturer_name", "Substance manufacturer (CWE, repeatable per spec)", "RXA-17")
    + _cwe_array_schema("substance_treatment_refusal_reason", "Treatment/refusal reason (CWE, repeatable per spec)", "RXA-18")
    + _cwe_array_schema("indication", "Indication for administration (CWE, repeatable per spec)", "RXA-19")
    + [
        _s("completion_status",                        "Completion status (RXA-20): CP=Complete, RE=Refused, NA=Not Administered, PA=Partial"),
        _s("action_code_rxa",                      "Action Code – RXA (RXA-21); vaccine record action, ID Table 0206"),
        _ts("system_entry_datetime",                   "System entry date/time (RXA-22)"),
        _s("administered_drug_strength_volume",        "Drug strength volume (RXA-23)"),
    ]
    + _cwe_schema("administered_drug_strength_volume_units", "Drug strength volume units (CWE)", "RXA-24")
    + _cwe_schema("administered_barcode_identifier", "Administered barcode identifier (CWE)", "RXA-25")
    + [
        _s("pharmacy_order_type",                      "Pharmacy order type (RXA-26)"),
    ]
    + _pl_schema("administer_at", "Administration location (PL, v2.6+)", "RXA-27")
    + _xad_schema("administered_at_address", "Administered-at address (XAD, v2.6+)", "RXA-28")
    + _ei_array_schema("administered_tag_identifier", "Administered tag identifier (EI, repeatable per spec, v2.9+)", "RXA-29")
)

# ---------------------------------------------------------------------------
# SCH — Scheduling Activity Information
# ---------------------------------------------------------------------------

SCH_SCHEMA = StructType(
    _METADATA_FIELDS
    + _ei_schema("placer_appointment_id", "Placer appointment ID (EI)", "SCH-1")
    + _ei_schema("filler_appointment_id", "Filler appointment ID (EI)", "SCH-2")
    + [
        _int_field("occurrence_number",            "Occurrence number (SCH-3)"),
    ]
    + _ei_schema("placer_group_number", "Placer group number (EI)", "SCH-4")
    + _cwe_schema("schedule_id", "Schedule identifier", "SCH-5")
    + _cwe_schema("event_reason", "Event reason", "SCH-6")
    + _cwe_schema("appointment_reason", "Reason for appointment", "SCH-7")
    + _cwe_schema("appointment_type", "Appointment type", "SCH-8")
    + [
        _int_field("appointment_duration",         "Appointment duration in minutes (SCH-9, deprecated)"),
    ]
    + _cwe_schema("appointment_duration_units", "Duration units", "SCH-10")
    + [
        _s("appointment_timing_quantity",  "Appointment timing quantity (SCH-11, deprecated)"),
    ]
    + _xcn_array_schema("placer_contact_person", "Placer contact person (XCN, repeatable per spec)", "SCH-12")
    + _xtn_schema("placer_contact_phone", "Placer contact phone", "SCH-13")
    + _xad_array_schema("placer_contact_address", "Placer contact address (XAD, repeatable per spec)", "SCH-14")
    + _pl_schema("placer_contact_location", "Placer contact location (PL)", "SCH-15")
    + _xcn_array_schema("filler_contact_person", "Filler contact person (XCN, repeatable per spec)", "SCH-16")
    + _xtn_schema("filler_contact_phone", "Filler contact phone", "SCH-17")
    + _xad_array_schema("filler_contact_address", "Filler contact address (XAD, repeatable per spec)", "SCH-18")
    + _pl_schema("filler_contact_location", "Filler contact location (PL)", "SCH-19")
    + _xcn_array_schema("entered_by_person", "Person who entered the schedule (XCN, repeatable per spec)", "SCH-20")
    + _xtn_array_schema("entered_by_phone_number", "Entered by phone (XTN, repeatable per spec)", "SCH-21")
    + _pl_schema("entered_by_location", "Entered by location (PL)", "SCH-22")
    + _ei_schema("parent_placer_appointment_id", "Parent placer appointment ID (EI)", "SCH-23")
    + _ei_schema("parent_filler_appointment_id", "Parent filler appointment ID (EI)", "SCH-24")
    + _cwe_schema("filler_status_code", "Filler status code", "SCH-25")
    + _ei_array_schema("placer_order_number", "Placer order number (EI, repeatable per spec)", "SCH-26")
    + _ei_array_schema("filler_order_number", "Filler order number (EI, repeatable per spec)", "SCH-27")
    + _eip_schema("alternate_placer_order_group_number", "Alternate placer order group number (EIP, [0..1], v2.9+)", "SCH-28")
)

# ---------------------------------------------------------------------------
# TXA — Transcription Document Header
# ---------------------------------------------------------------------------

TXA_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _int_field("set_id",                              "Sequence number (TXA-1)"),
    ]
    + _cwe_schema("document_type", "Document type (CWE; e.g. DS=Discharge Summary, HP=History and Physical, OP=Operative Note)", "TXA-2")
    + [
        _s("document_content_presentation",        "Content presentation type (TXA-3)"),
        _ts("activity_datetime",                   "Activity date/time (TXA-4)"),
    ]
    + _xcn_array_schema("primary_activity_provider", "Primary activity provider (XCN, repeatable per spec)", "TXA-5")
    + [
        _ts("origination_datetime",                "Origination date/time (TXA-6)"),
        _ts("transcription_datetime",              "Transcription date/time (TXA-7)"),
        _s_array("edit_datetime",                  "Edit date/times (TXA-8, DTM repeatable per spec)"),
    ]
    + _xcn_array_schema("originator", "Document originator (XCN, repeatable per spec)", "TXA-9")
    + _xcn_array_schema("assigned_document_authenticator", "Assigned document authenticator (XCN, repeatable per spec)", "TXA-10")
    + _xcn_array_schema("transcriptionist", "Transcriptionist (XCN, repeatable per spec)", "TXA-11")
    + _ei_schema("unique_document_number", "Unique document number (EI)", "TXA-12")
    + _ei_schema("parent_document_number", "Parent document number (EI)", "TXA-13")
    + _ei_array_schema("placer_order_number", "Placer order number (EI, repeatable per spec)", "TXA-14")
    + _ei_schema("filler_order_number", "Filler order number (EI)", "TXA-15")
    + [
        _s("unique_document_file_name",            "Document file name (TXA-16)"),
        _s("document_completion_status",           "Completion status (TXA-17): DI=Dictated, DO=Documented, AU=Authenticated, LA=Legally Authenticated"),
        _s("document_confidentiality_status",      "Confidentiality status (TXA-18)"),
        _s("document_availability_status",         "Availability status (TXA-19)"),
        _s("document_storage_status",              "Storage status (TXA-20)"),
        _s("document_change_reason",               "Reason for document change (TXA-21)"),
    ]
    + _xcn_array_schema("authentication_person_time_stamp", "Authentication person with timestamp (PPN→XCN approximation, repeatable per spec, TXA-22)", "TXA-22")
    + _xcn_array_schema("distributed_copies", "Distributed copy recipient (XCN, repeatable per spec)", "TXA-23")
    + _cwe_array_schema("folder_assignment", "Folder assignment (CWE, repeatable per spec)", "TXA-24")
    + [
        _s_array("document_title",                 "Document titles (TXA-25, ST repeatable per spec, v2.6+)"),
        _ts("agreed_due_datetime",                 "Agreed due date/time (TXA-26, v2.8+)"),
    ]
    + _hd_schema("creating_facility", "Creating facility", "TXA-27")
    + _cwe_schema("creating_specialty", "Creating specialty", "TXA-28")
)

# ---------------------------------------------------------------------------
# Generic segment schema — used for Z-segments and any unknown segments.
# Returns segment_type + up to 25 field_N columns.
# ---------------------------------------------------------------------------

_GENERIC_FIELD_COUNT = 25

GENERIC_SEGMENT_SCHEMA = StructType(
    _METADATA_FIELDS
    + [_s("segment_type", "HL7 segment type identifier, e.g. ZPD for a custom Z-segment")]
    + [
        _s(f"field_{i}", f"Raw value of field {i} in the segment (1-based HL7 field index)")
        for i in range(1, _GENERIC_FIELD_COUNT + 1)
    ]
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

#: Segment tables exposed by the connector by default.
SEGMENT_TABLES: list[str] = [
    "msh", "evn", "pid", "pd1", "pv1", "pv2", "nk1", "mrg",
    "al1", "iam", "dg1", "pr1",
    "orc", "obr", "obx", "nte", "spm",
    "in1", "gt1", "ft1",
    "rxa", "sch", "txa",
]

#: Map from lowercase segment name → StructType.
SEGMENT_SCHEMAS: dict[str, StructType] = {
    "msh": MSH_SCHEMA,
    "evn": EVN_SCHEMA,
    "pid": PID_SCHEMA,
    "pd1": PD1_SCHEMA,
    "pv1": PV1_SCHEMA,
    "pv2": PV2_SCHEMA,
    "nk1": NK1_SCHEMA,
    "mrg": MRG_SCHEMA,
    "al1": AL1_SCHEMA,
    "iam": IAM_SCHEMA,
    "dg1": DG1_SCHEMA,
    "pr1": PR1_SCHEMA,
    "orc": ORC_SCHEMA,
    "obr": OBR_SCHEMA,
    "obx": OBX_SCHEMA,
    "nte": NTE_SCHEMA,
    "spm": SPM_SCHEMA,
    "in1": IN1_SCHEMA,
    "gt1": GT1_SCHEMA,
    "ft1": FT1_SCHEMA,
    "rxa": RXA_SCHEMA,
    "sch": SCH_SCHEMA,
    "txa": TXA_SCHEMA,
}


def get_schema(segment_type: str) -> StructType:
    """Return the StructType for *segment_type* (case-insensitive).

    Falls back to :data:`GENERIC_SEGMENT_SCHEMA` for unknown / Z-segments.
    """
    return SEGMENT_SCHEMAS.get(segment_type.lower(), GENERIC_SEGMENT_SCHEMA)
