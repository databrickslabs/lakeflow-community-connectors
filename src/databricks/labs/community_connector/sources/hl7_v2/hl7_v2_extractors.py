"""HL7 v2 per-segment extractors and message splitter.

Each ``_extract_<segment>`` function takes an :class:`HL7Segment` and
returns a flat ``dict`` of column-name → value.  The composite-type helpers
that build these rows live in :mod:`hl7_v2_composites`.

``_metadata`` builds the common columns (message_id, hl7_version, etc.)
that are merged into every output row.  ``_split_messages`` cuts a batched
HL7 payload into one string per message.  ``_EXTRACTORS`` is the dispatch
table the connector consults at parse time.
"""

from __future__ import annotations

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_composites import (
    _aui_fields,
    _cp_fields,
    _cq_fields,
    _cwe_array_fields,
    _cwe_fields,
    _cwe_struct,
    _cx_array_fields,
    _cx_fields,
    _dld_fields,
    _dln_fields,
    _ei_array_fields,
    _ei_fields,
    _eip_array_fields,
    _eip_fields,
    _fc_array_fields,
    _fc_fields,
    _hd_fields,
    _hd_struct,
    _dtm_array_fields,
    _i,
    _id_array_fields,
    _jcc_fields,
    _mo_fields,
    _moc_fields,
    _ndl_array_fields,
    _ndl_fields,
    _og_fields,
    _parse_dtm,
    _pl_array_fields,
    _pl_fields,
    _prl_fields,
    _pt_fields,
    _tq_array_fields,
    _s_array_fields,
    _sps_fields,
    _v,
    _vid_fields,
    _xad_array_fields,
    _xad_fields,
    _xcn_array_fields,
    _xcn_fields,
    _xon_array_fields,
    _xon_fields,
    _xpn_array_fields,
    _xtn_array_fields,
    _xtn_fields,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    HL7Segment,
)


# Segment types that, by HL7 v2 design, appear at most once per message.
# Used by the connector to choose between message-id-only and
# (message-id, set-id) primary keys.
_SINGLE_SEGMENT_TABLES = frozenset(
    {"msh", "evn", "pid", "pd1", "pv1", "pv2", "mrg", "sch", "txa"}
)

# HL7 batch-envelope segment types — skipped by ``_split_messages`` since
# they wrap (but are not part of) the individual messages in a batch.
_BATCH_ENVELOPE_SEGMENTS = frozenset({"FHS", "BHS", "BTS", "FTS"})


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
        **_pt_fields(seg, 11, "processing_id"),
        **_vid_fields(seg, 12, "version_id"),
        "sequence_number": _i(seg.get_field(13)),
        "continuation_pointer": _v(seg.get_field(14)),
        "accept_acknowledgment_type": _v(seg.get_field(15)),
        "application_acknowledgment_type": _v(seg.get_field(16)),
        "country_code": _v(seg.get_field(17)),
        **_id_array_fields(seg, 18, "character_set"),
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
        **_cx_fields(seg, 2, "patient_external_id", repeating=False),
        **_cx_array_fields(seg, 3, "patient_id"),
        **_cx_fields(seg, 4, "alternate_patient_id", repeating=False),
        **_xpn_array_fields(seg, 5, "patient_names"),
        **_xpn_array_fields(seg, 6, "mothers_maiden_names"),
        "date_of_birth": _parse_dtm(seg.get_field(7)),
        **_cwe_fields(seg, 8, "administrative_sex", repeating=False),
        "patient_alias": _v(seg.get_first_repetition(9)),
        **_cwe_array_fields(seg, 10, "race"),
        **_xad_array_fields(seg, 11, "address"),
        "county_code": _v(seg.get_field(12)),
        **_xtn_array_fields(seg, 13, "home_phone"),
        **_xtn_array_fields(seg, 14, "business_phone"),
        **_cwe_fields(seg, 15, "primary_language", repeating=False),
        **_cwe_fields(seg, 16, "marital_status", repeating=False),
        **_cwe_fields(seg, 17, "religion", repeating=False),
        **_cx_fields(seg, 18, "patient_account_number", repeating=False),
        "ssn": _v(seg.get_field(19)),
        **_dln_fields(seg, 20, "drivers_license"),
        **_cx_array_fields(seg, 21, "mothers_identifier"),
        **_cwe_array_fields(seg, 22, "ethnic_group"),
        "birth_place": _v(seg.get_field(23)),
        "multiple_birth_indicator": _v(seg.get_field(24)),
        "birth_order": _i(seg.get_field(25)),
        **_cwe_array_fields(seg, 26, "citizenship"),
        **_cwe_fields(seg, 27, "veterans_military_status", repeating=False),
        **_cwe_fields(seg, 28, "nationality", repeating=False),
        "patient_death_datetime": _parse_dtm(seg.get_field(29)),
        "patient_death_indicator": _v(seg.get_field(30)),
        "identity_unknown_indicator": _v(seg.get_field(31)),
        **_cwe_array_fields(seg, 32, "identity_reliability_code"),
        "last_update_datetime": _parse_dtm(seg.get_field(33)),
        **_hd_fields(seg, 34, "last_update_facility", repeating=False),
        **_cwe_fields(seg, 35, "species_code", repeating=False),
        **_cwe_fields(seg, 36, "breed_code", repeating=False),
        "strain": _v(seg.get_field(37)),
        **_cwe_fields(seg, 38, "production_class_code", repeating=False),
        **_cwe_array_fields(seg, 39, "tribal_citizenship"),
        **_xtn_array_fields(seg, 40, "patient_telecom"),
    }


def _extract_pv1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)),
        **_cwe_fields(seg, 2, "patient_class", repeating=False),
        **_pl_fields(seg, 3, "assigned_patient_location"),
        **_cwe_fields(seg, 4, "admission_type", repeating=False),
        **_cx_fields(seg, 5, "preadmit_number", repeating=False),
        **_pl_fields(seg, 6, "prior_patient_location"),
        **_xcn_array_fields(seg, 7, "attending_doctor"),
        **_xcn_array_fields(seg, 8, "referring_doctor"),
        **_xcn_array_fields(seg, 9, "consulting_doctor"),
        **_cwe_fields(seg, 10, "hospital_service", repeating=False),
        **_pl_fields(seg, 11, "temporary_location"),
        **_cwe_fields(seg, 12, "preadmit_test_indicator", repeating=False),
        **_cwe_fields(seg, 13, "readmission_indicator", repeating=False),
        **_cwe_fields(seg, 14, "admit_source", repeating=False),
        **_cwe_array_fields(seg, 15, "ambulatory_status"),
        **_cwe_fields(seg, 16, "vip_indicator", repeating=False),
        **_xcn_array_fields(seg, 17, "admitting_doctor"),
        **_cwe_fields(seg, 18, "patient_type", repeating=False),
        **_cx_fields(seg, 19, "visit_number", repeating=False),
        **_fc_array_fields(seg, 20, "financial_class"),
        **_cwe_fields(seg, 21, "charge_price_indicator", repeating=False),
        **_cwe_fields(seg, 22, "courtesy_code", repeating=False),
        **_cwe_fields(seg, 23, "credit_rating", repeating=False),
        **_cwe_array_fields(seg, 24, "contract_code"),
        **_dtm_array_fields(seg, 25, "contract_effective_date"),
        **_s_array_fields(seg, 26, "contract_amount"),
        **_s_array_fields(seg, 27, "contract_period"),
        **_cwe_fields(seg, 28, "interest_code", repeating=False),
        **_cwe_fields(seg, 29, "transfer_to_bad_debt_code", repeating=False),
        "transfer_to_bad_debt_date": _parse_dtm(seg.get_field(30)),
        **_cwe_fields(seg, 31, "bad_debt_agency_code", repeating=False),
        "bad_debt_transfer_amount": _v(seg.get_field(32)),
        "bad_debt_recovery_amount": _v(seg.get_field(33)),
        **_cwe_fields(seg, 34, "delete_account_indicator", repeating=False),
        "delete_account_date": _parse_dtm(seg.get_field(35)),
        **_cwe_fields(seg, 36, "discharge_disposition", repeating=False),
        **_dld_fields(seg, 37, "discharged_to_location"),
        **_cwe_fields(seg, 38, "diet_type", repeating=False),
        **_cwe_fields(seg, 39, "servicing_facility", repeating=False),
        **_cwe_fields(seg, 40, "bed_status", repeating=False),
        **_cwe_fields(seg, 41, "account_status", repeating=False),
        **_pl_fields(seg, 42, "pending_location"),
        **_pl_fields(seg, 43, "prior_temporary_location"),
        "admit_datetime": _parse_dtm(seg.get_first_repetition(44)),
        "discharge_datetime": _parse_dtm(seg.get_first_repetition(45)),
        "current_patient_balance": _v(seg.get_field(46)),
        "total_charges": _v(seg.get_field(47)),
        "total_adjustments": _v(seg.get_field(48)),
        "total_payments": _v(seg.get_field(49)),
        **_cx_array_fields(seg, 50, "alternate_visit_id"),
        **_cwe_fields(seg, 51, "visit_indicator", repeating=False),
        **_xcn_array_fields(seg, 52, "other_healthcare_provider"),
        "service_episode_description": _v(seg.get_field(53)),
        **_cx_fields(seg, 54, "service_episode_identifier", repeating=False),
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
        **_cq_fields(seg, 9, "collection_volume"),
        **_xcn_array_fields(seg, 10, "collector"),
        "specimen_action_code": _v(seg.get_field(11)),
        **_cwe_fields(seg, 12, "danger_code", repeating=False),
        **_cwe_array_fields(seg, 13, "relevant_clinical_information"),
        "specimen_received_datetime": _parse_dtm(seg.get_field(14)),
        **_sps_fields(seg, 15, "specimen_source"),
        **_xcn_array_fields(seg, 16, "ordering_provider"),
        **_xtn_array_fields(seg, 17, "order_callback_phone"),
        "placer_field_1": _v(seg.get_field(18)),
        "placer_field_2": _v(seg.get_field(19)),
        "filler_field_1": _v(seg.get_field(20)),
        "filler_field_2": _v(seg.get_field(21)),
        "results_rpt_status_chng_datetime": _parse_dtm(seg.get_field(22)),
        **_moc_fields(seg, 23, "charge_to_practice"),
        "diagnostic_service_section": _v(seg.get_field(24)),
        "result_status": _v(seg.get_field(25)),
        **_prl_fields(seg, 26, "parent_result"),
        **_tq_array_fields(seg, 27, "quantity_timing"),
        **_xcn_array_fields(seg, 28, "result_copies_to"),
        **_eip_fields(seg, 29, "parent_results_observation_identifier"),
        "transportation_mode": _v(seg.get_field(30)),
        **_cwe_array_fields(seg, 31, "reason_for_study"),
        **_ndl_fields(seg, 32, "principal_result_interpreter"),
        **_ndl_array_fields(seg, 33, "assistant_result_interpreter"),
        **_ndl_array_fields(seg, 34, "technician"),
        **_ndl_array_fields(seg, 35, "transcriptionist"),
        "scheduled_datetime": _parse_dtm(seg.get_field(36)),
        "number_of_sample_containers": _i(seg.get_field(37)),
        **_cwe_array_fields(seg, 38, "transport_logistics"),
        **_cwe_array_fields(seg, 39, "collectors_comment"),
        **_cwe_fields(seg, 40, "transport_arrangement_responsibility", repeating=False),
        "transport_arranged": _v(seg.get_field(41)),
        "escort_required": _v(seg.get_field(42)),
        **_cwe_array_fields(seg, 43, "planned_patient_transport_comment"),
        **_cwe_fields(seg, 44, "procedure_code", repeating=False),
        **_cwe_array_fields(seg, 45, "procedure_code_modifier"),
        **_cwe_array_fields(seg, 46, "placer_supplemental_service_info"),
        **_cwe_array_fields(seg, 47, "filler_supplemental_service_info"),
        **_cwe_fields(seg, 48, "medically_necessary_dup_proc_reason", repeating=False),
        **_cwe_fields(seg, 49, "result_handling", repeating=False),
        **_cwe_fields(seg, 50, "parent_universal_service_id", repeating=False),
        **_ei_fields(seg, 51, "observation_group", repeating=False),
        **_ei_fields(seg, 52, "parent_observation_group", repeating=False),
        **_cx_array_fields(seg, 53, "alternate_placer_order"),
        **_eip_array_fields(seg, 54, "parent_order"),
        "action_code": _v(seg.get_field(55)),
    }


def _extract_obx(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "value_type": _v(seg.get_field(2)),
        **_cwe_fields(seg, 3, "observation_id", repeating=False),
        **_og_fields(seg, 4, "observation_sub_id"),
        **_s_array_fields(seg, 5, "observation_value"),
        **_cwe_fields(seg, 6, "units", repeating=False),
        "references_range": _v(seg.get_field(7)),
        **_cwe_array_fields(seg, 8, "interpretation_codes"),
        "probability": _v(seg.get_field(9)),
        **_id_array_fields(seg, 10, "nature_of_abnormal_test"),
        "observation_result_status": _v(seg.get_field(11)),
        "effective_date_of_ref_range": _parse_dtm(seg.get_field(12)),
        "user_defined_access_checks": _v(seg.get_field(13)),
        "datetime_of_observation": _parse_dtm(seg.get_field(14)),
        **_cwe_fields(seg, 15, "producers_id", repeating=False),
        **_xcn_array_fields(seg, 16, "responsible_observer"),
        **_cwe_array_fields(seg, 17, "observation_method"),
        **_ei_array_fields(seg, 18, "equipment_instance_identifier"),
        "datetime_of_analysis": _parse_dtm(seg.get_field(19)),
        **_cwe_array_fields(seg, 20, "observation_site"),
        **_ei_fields(seg, 21, "observation_instance_identifier", repeating=False),
        **_cwe_fields(seg, 22, "mood_code", repeating=False),
        **_xon_fields(seg, 23, "performing_organization", repeating=False),
        **_xad_fields(seg, 24, "performing_org_address", repeating=False),
        **_xcn_fields(seg, 25, "performing_org_medical_director"),
        "patient_results_release_category": _v(seg.get_field(26)),
        **_cwe_fields(seg, 27, "root_cause", repeating=False),
        **_cwe_array_fields(seg, 28, "local_process_control"),
        "observation_type": _v(seg.get_field(29)),
        "observation_sub_type": _v(seg.get_field(30)),
        "action_code": _v(seg.get_field(31)),
        **_cwe_array_fields(seg, 32, "observation_value_absent_reason"),
        **_eip_array_fields(seg, 33, "observation_related_specimen"),
    }


def _extract_al1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cwe_fields(seg, 2, "allergen_type_code", repeating=False),
        **_cwe_fields(seg, 3, "allergen_code", repeating=False),
        **_cwe_fields(seg, 4, "allergy_severity_code", repeating=False),
        # AL1-5 is spec-typed as ST 0..* but real EHRs routinely send CWE-shape
        # (e.g. "HIV^Hives^HL70129~RSH^Rash^HL70129"). Modeling as a repeating
        # CWE-shape struct handles both: pure ST values land in element 0's `code`
        # with the rest NULL; CWE-shape values populate all components; every
        # ~-separated repetition is preserved.
        **_cwe_array_fields(seg, 5, "allergy_reaction"),
        "identification_date": _parse_dtm(seg.get_field(6)),
    }


def _extract_dg1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "diagnosis_coding_method": _v(seg.get_field(2)),
        **_cwe_fields(seg, 3, "diagnosis_code", repeating=False),
        "diagnosis_description": _v(seg.get_field(4)),
        "diagnosis_datetime": _parse_dtm(seg.get_field(5)),
        **_cwe_fields(seg, 6, "diagnosis_type", repeating=False),
        **_cwe_fields(seg, 7, "major_diagnostic_category", repeating=False),
        **_cwe_fields(seg, 8, "diagnostic_related_group", repeating=False),
        "drg_approval_indicator": _v(seg.get_field(9)),
        **_cwe_fields(seg, 10, "drg_grouper_review_code", repeating=False),
        **_cwe_fields(seg, 11, "outlier_type", repeating=False),
        "outlier_days": _i(seg.get_field(12)),
        **_cp_fields(seg, 13, "outlier_cost"),
        "grouper_version_and_type": _v(seg.get_field(14)),
        "diagnosis_priority": _i(seg.get_field(15)),
        **_xcn_array_fields(seg, 16, "diagnosing_clinician"),
        **_cwe_fields(seg, 17, "diagnosis_classification", repeating=False),
        "confidential_indicator": _v(seg.get_field(18)),
        "attestation_datetime": _parse_dtm(seg.get_field(19)),
        **_ei_fields(seg, 20, "diagnosis_identifier", repeating=False),
        "diagnosis_action_code": _v(seg.get_field(21)),
        **_ei_fields(seg, 22, "parent_diagnosis", repeating=False),
        **_cwe_fields(seg, 23, "drg_ccl_value_code", repeating=False),
        "drg_grouping_usage": _v(seg.get_field(24)),
        **_cwe_fields(seg, 25, "drg_diagnosis_determination_status", repeating=False),
        **_cwe_fields(seg, 26, "present_on_admission_indicator", repeating=False),
    }


def _extract_nk1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_xpn_array_fields(seg, 2, "names"),
        **_cwe_fields(seg, 3, "relationship", repeating=False),
        **_xad_array_fields(seg, 4, "address"),
        **_xtn_array_fields(seg, 5, "phone_number"),
        **_xtn_array_fields(seg, 6, "business_phone"),
        **_cwe_fields(seg, 7, "contact_role", repeating=False),
        "start_date": _parse_dtm(seg.get_field(8)),
        "end_date": _parse_dtm(seg.get_field(9)),
        "job_title": _v(seg.get_field(10)),
        **_jcc_fields(seg, 11, "job_code"),
        **_cx_fields(seg, 12, "employee_number", repeating=False),
        **_xon_array_fields(seg, 13, "organization_name"),
        **_cwe_fields(seg, 14, "marital_status", repeating=False),
        **_cwe_fields(seg, 15, "administrative_sex", repeating=False),
        "date_of_birth": _parse_dtm(seg.get_field(16)),
        **_cwe_array_fields(seg, 17, "living_dependency"),
        **_cwe_array_fields(seg, 18, "ambulatory_status"),
        **_cwe_array_fields(seg, 19, "citizenship"),
        **_cwe_fields(seg, 20, "primary_language", repeating=False),
        **_cwe_fields(seg, 21, "living_arrangement", repeating=False),
        **_cwe_fields(seg, 22, "publicity_code", repeating=False),
        "protection_indicator": _v(seg.get_field(23)),
        **_cwe_fields(seg, 24, "student_indicator", repeating=False),
        **_cwe_fields(seg, 25, "religion", repeating=False),
        **_xpn_array_fields(seg, 26, "mothers_maiden_names"),
        **_cwe_fields(seg, 27, "nationality", repeating=False),
        **_cwe_array_fields(seg, 28, "ethnic_group"),
        **_cwe_array_fields(seg, 29, "contact_reason"),
        **_xpn_array_fields(seg, 30, "contact_persons"),
        **_xtn_array_fields(seg, 31, "contact_person_telephone"),
        **_xad_array_fields(seg, 32, "contact_persons_address"),
        **_cx_array_fields(seg, 33, "associated_party_identifiers"),
        **_cwe_fields(seg, 34, "job_status", repeating=False),
        **_cwe_array_fields(seg, 35, "race"),
        **_cwe_fields(seg, 36, "handicap", repeating=False),
        "contact_ssn": _v(seg.get_field(37)),
        "birth_place": _v(seg.get_field(38)),
        **_cwe_fields(seg, 39, "vip_indicator", repeating=False),
        **_xtn_fields(seg, 40, "telecommunication_info"),
        **_xtn_fields(seg, 41, "contact_telecommunication_info"),
    }


def _extract_evn(seg: HL7Segment) -> dict:
    return {
        "event_type_code": _v(seg.get_field(1)),
        "recorded_datetime": _parse_dtm(seg.get_field(2)),
        "date_time_planned_event": _parse_dtm(seg.get_field(3)),
        "event_reason": _cwe_struct(seg, 4),
        **_xcn_array_fields(seg, 5, "operator"),
        "event_occurred": _parse_dtm(seg.get_field(6)),
        "event_facility": _hd_struct(seg, 7),
    }


def _extract_pd1(seg: HL7Segment) -> dict:
    return {
        **_cwe_array_fields(seg, 1, "living_dependency"),
        **_cwe_fields(seg, 2, "living_arrangement", repeating=False),
        **_xon_array_fields(seg, 3, "patient_primary_facility"),
        **_xcn_fields(seg, 4, "patient_primary_care_provider"),
        **_cwe_fields(seg, 5, "student_indicator", repeating=False),
        **_cwe_fields(seg, 6, "handicap", repeating=False),
        **_cwe_fields(seg, 7, "living_will_code", repeating=False),
        **_cwe_fields(seg, 8, "organ_donor_code", repeating=False),
        "separate_bill": _v(seg.get_field(9)),
        **_cx_array_fields(seg, 10, "duplicate_patient"),
        **_cwe_fields(seg, 11, "publicity_code", repeating=False),
        "protection_indicator": _v(seg.get_field(12)),
        "protection_indicator_effective_date": _parse_dtm(seg.get_field(13)),
        **_xon_array_fields(seg, 14, "place_of_worship"),
        **_cwe_array_fields(seg, 15, "advance_directive_code"),
        **_cwe_fields(seg, 16, "immunization_registry_status", repeating=False),
        "immunization_registry_status_effective_date": _parse_dtm(seg.get_field(17)),
        "publicity_code_effective_date": _parse_dtm(seg.get_field(18)),
        **_cwe_fields(seg, 19, "military_branch", repeating=False),
        **_cwe_fields(seg, 20, "military_rank_grade", repeating=False),
        **_cwe_fields(seg, 21, "military_status", repeating=False),
        "advance_directive_last_verified_date": _parse_dtm(seg.get_field(22)),
        "retirement_date": _parse_dtm(seg.get_field(23)),
    }


def _extract_pv2(seg: HL7Segment) -> dict:
    return {
        **_pl_fields(seg, 1, "prior_pending_location"),
        **_cwe_fields(seg, 2, "accommodation_code", repeating=False),
        **_cwe_fields(seg, 3, "admit_reason", repeating=False),
        **_cwe_fields(seg, 4, "transfer_reason", repeating=False),
        **_s_array_fields(seg, 5, "patient_valuables"),
        "patient_valuables_location": _v(seg.get_field(6)),
        **_cwe_array_fields(seg, 7, "visit_user_code"),
        "expected_admit_datetime": _parse_dtm(seg.get_field(8)),
        "expected_discharge_datetime": _parse_dtm(seg.get_field(9)),
        "estimated_length_of_inpatient_stay": _i(seg.get_field(10)),
        "actual_length_of_inpatient_stay": _i(seg.get_field(11)),
        "visit_description": _v(seg.get_field(12)),
        **_xcn_array_fields(seg, 13, "referral_source"),
        "previous_service_date": _parse_dtm(seg.get_field(14)),
        "employment_illness_related_indicator": _v(seg.get_field(15)),
        **_cwe_fields(seg, 16, "purge_status_code", repeating=False),
        "purge_status_date": _parse_dtm(seg.get_field(17)),
        **_cwe_fields(seg, 18, "special_program_code", repeating=False),
        "retention_indicator": _v(seg.get_field(19)),
        "expected_number_of_insurance_plans": _i(seg.get_field(20)),
        **_cwe_fields(seg, 21, "visit_publicity_code", repeating=False),
        "visit_protection_indicator": _v(seg.get_field(22)),
        **_xon_array_fields(seg, 23, "clinic_organization"),
        **_cwe_fields(seg, 24, "patient_status_code", repeating=False),
        **_cwe_fields(seg, 25, "visit_priority_code", repeating=False),
        "previous_treatment_date": _parse_dtm(seg.get_field(26)),
        **_cwe_fields(seg, 27, "expected_discharge_disposition", repeating=False),
        "signature_on_file_date": _parse_dtm(seg.get_field(28)),
        "first_similar_illness_date": _parse_dtm(seg.get_field(29)),
        **_cwe_fields(seg, 30, "patient_charge_adjustment_code", repeating=False),
        **_cwe_fields(seg, 31, "recurring_service_code", repeating=False),
        "billing_media_code": _v(seg.get_field(32)),
        "expected_surgery_datetime": _parse_dtm(seg.get_field(33)),
        "military_partnership_code": _v(seg.get_field(34)),
        "military_non_availability_code": _v(seg.get_field(35)),
        "newborn_baby_indicator": _v(seg.get_field(36)),
        "baby_detained_indicator": _v(seg.get_field(37)),
        **_cwe_fields(seg, 38, "mode_of_arrival_code", repeating=False),
        **_cwe_array_fields(seg, 39, "recreational_drug_use_code"),
        **_cwe_fields(seg, 40, "admission_level_of_care_code", repeating=False),
        **_cwe_array_fields(seg, 41, "precaution_code"),
        **_cwe_fields(seg, 42, "patient_condition_code", repeating=False),
        **_cwe_fields(seg, 43, "living_will_code", repeating=False),
        **_cwe_fields(seg, 44, "organ_donor_code", repeating=False),
        **_cwe_array_fields(seg, 45, "advance_directive_code"),
        "patient_status_effective_date": _parse_dtm(seg.get_field(46)),
        "expected_loa_return_datetime": _parse_dtm(seg.get_field(47)),
        "expected_preadmission_testing_datetime": _parse_dtm(seg.get_field(48)),
        **_cwe_array_fields(seg, 49, "notify_clergy_code"),
        "advance_directive_last_verified_date": _parse_dtm(seg.get_field(50)),
    }


def _extract_mrg(seg: HL7Segment) -> dict:
    return {
        **_cx_array_fields(seg, 1, "prior_patient_id"),
        **_cx_array_fields(seg, 2, "prior_alternate_patient_id"),
        **_cx_fields(seg, 3, "prior_patient_account_number", repeating=False),
        **_cx_fields(seg, 4, "prior_patient_id_external", repeating=False),
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
        # Same lenient ST 0..* -> CWE-shape array as AL1-5.
        **_cwe_array_fields(seg, 5, "allergy_reaction"),
        **_cwe_fields(seg, 6, "allergy_action_code", repeating=False),
        **_ei_fields(seg, 7, "allergy_unique_identifier", repeating=False),
        "action_reason": _v(seg.get_field(8)),
        **_cwe_fields(seg, 9, "sensitivity_to_causative_agent_code", repeating=False),
        **_cwe_fields(seg, 10, "allergen_group_code", repeating=False),
        "onset_date": _parse_dtm(seg.get_field(11)),
        "onset_date_text": _v(seg.get_field(12)),
        "reported_datetime": _parse_dtm(seg.get_field(13)),
        **_xcn_fields(seg, 14, "reported_by", repeating=False),
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
        **_cwe_fields(seg, 27, "clinician_identified_allergen_code", repeating=False),
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
        **_cwe_fields(seg, 6, "procedure_functional_type", repeating=False),
        "procedure_minutes": _i(seg.get_field(7)),
        **_xcn_array_fields(seg, 8, "anesthesiologist"),
        **_cwe_fields(seg, 9, "anesthesia_code", repeating=False),
        "anesthesia_minutes": _i(seg.get_field(10)),
        **_xcn_array_fields(seg, 11, "surgeon"),
        **_xcn_array_fields(seg, 12, "procedure_practitioner"),
        **_cwe_fields(seg, 13, "consent_code", repeating=False),
        "procedure_priority": _v(seg.get_field(14)),
        **_cwe_fields(seg, 15, "associated_diagnosis_code", repeating=False),
        **_cwe_array_fields(seg, 16, "procedure_code_modifier"),
        **_cwe_fields(seg, 17, "procedure_drg_type", repeating=False),
        **_cwe_array_fields(seg, 18, "tissue_type_code"),
        **_ei_fields(seg, 19, "procedure_identifier", repeating=False),
        "procedure_action_code": _v(seg.get_field(20)),
        **_cwe_fields(seg, 21, "drg_procedure_determination_status", repeating=False),
        **_cwe_fields(seg, 22, "drg_procedure_relevance", repeating=False),
        **_pl_array_fields(seg, 23, "treating_organizational_unit"),
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
        **_tq_array_fields(seg, 7, "quantity_timing"),
        **_eip_array_fields(seg, 8, "parent_order"),
        "datetime_of_transaction": _parse_dtm(seg.get_field(9)),
        **_xcn_array_fields(seg, 10, "entered_by"),
        **_xcn_array_fields(seg, 11, "verified_by"),
        **_xcn_array_fields(seg, 12, "ordering_provider"),
        **_pl_fields(seg, 13, "enterers_location"),
        **_xtn_array_fields(seg, 14, "call_back_phone"),
        "order_effective_datetime": _parse_dtm(seg.get_field(15)),
        **_cwe_fields(seg, 16, "order_control_code_reason", repeating=False),
        **_cwe_fields(seg, 17, "entering_organization", repeating=False),
        **_cwe_fields(seg, 18, "entering_device", repeating=False),
        **_xcn_array_fields(seg, 19, "action_by"),
        **_cwe_fields(seg, 20, "advanced_beneficiary_notice_code", repeating=False),
        **_xon_array_fields(seg, 21, "ordering_facility_name"),
        **_xad_array_fields(seg, 22, "ordering_facility_address"),
        **_xtn_array_fields(seg, 23, "ordering_facility_phone"),
        **_xad_array_fields(seg, 24, "ordering_provider_address"),
        **_cwe_fields(seg, 25, "order_status_modifier", repeating=False),
        **_cwe_fields(seg, 26, "abn_override_reason", repeating=False),
        "fillers_expected_availability_datetime": _parse_dtm(seg.get_field(27)),
        **_cwe_fields(seg, 28, "confidentiality_code", repeating=False),
        **_cwe_fields(seg, 29, "order_type", repeating=False),
        **_cwe_fields(seg, 30, "enterer_authorization_mode", repeating=False),
        **_cwe_fields(seg, 31, "parent_universal_service_id", repeating=False),
        "advanced_beneficiary_notice_date": _parse_dtm(seg.get_field(32)),
        **_cx_array_fields(seg, 33, "alternate_placer_order_number"),
        **_cwe_array_fields(seg, 34, "order_workflow_profile"),
        "action_code": _v(seg.get_field(35)),
        "order_status_date_range_start": _parse_dtm(seg.get_component(36, 1)),
        "order_status_date_range_end": _parse_dtm(seg.get_component(36, 2)),
        "order_creation_datetime": _parse_dtm(seg.get_field(37)),
        **_ei_fields(seg, 38, "filler_order_group_number", repeating=False),
    }


def _extract_nte(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "source_of_comment": _v(seg.get_field(2)),
        **_s_array_fields(seg, 3, "comment"),
        **_cwe_fields(seg, 4, "comment_type", repeating=False),
        **_xcn_fields(seg, 5, "entered_by", repeating=False),
        "entered_datetime": _parse_dtm(seg.get_field(6)),
        "effective_start_date": _parse_dtm(seg.get_field(7)),
        "expiration_date": _parse_dtm(seg.get_field(8)),
        **_cwe_array_fields(seg, 9, "coded_comment"),
    }


def _extract_spm(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_eip_fields(seg, 2, "specimen_id"),
        **_eip_array_fields(seg, 3, "specimen_parent_ids"),
        **_cwe_fields(seg, 4, "specimen_type", repeating=False),
        **_cwe_array_fields(seg, 5, "specimen_type_modifier"),
        **_cwe_array_fields(seg, 6, "specimen_additives"),
        **_cwe_fields(seg, 7, "specimen_collection_method", repeating=False),
        **_cwe_fields(seg, 8, "specimen_source_site", repeating=False),
        **_cwe_array_fields(seg, 9, "specimen_source_site_modifier"),
        **_cwe_fields(seg, 10, "specimen_collection_site", repeating=False),
        **_cwe_array_fields(seg, 11, "specimen_role"),
        **_cq_fields(seg, 12, "specimen_collection_amount"),
        "grouped_specimen_count": _i(seg.get_field(13)),
        **_s_array_fields(seg, 14, "specimen_description"),
        **_cwe_array_fields(seg, 15, "specimen_handling_code"),
        **_cwe_array_fields(seg, 16, "specimen_risk_code"),
        "specimen_collection_datetime_start": _parse_dtm(seg.get_component(17, 1)),
        "specimen_collection_datetime_end": _parse_dtm(seg.get_component(17, 2)),
        "specimen_received_datetime": _parse_dtm(seg.get_field(18)),
        "specimen_expiration_datetime": _parse_dtm(seg.get_field(19)),
        "specimen_availability": _v(seg.get_field(20)),
        **_cwe_array_fields(seg, 21, "specimen_reject_reason"),
        **_cwe_fields(seg, 22, "specimen_quality", repeating=False),
        **_cwe_fields(seg, 23, "specimen_appropriateness", repeating=False),
        **_cwe_array_fields(seg, 24, "specimen_condition"),
        **_cq_fields(seg, 25, "specimen_current_quantity"),
        "number_of_specimen_containers": _i(seg.get_field(26)),
        **_cwe_fields(seg, 27, "container_type", repeating=False),
        **_cwe_fields(seg, 28, "container_condition", repeating=False),
        **_cwe_fields(seg, 29, "specimen_child_role", repeating=False),
        **_cx_array_fields(seg, 30, "accession_id"),
        **_cx_array_fields(seg, 31, "other_specimen_id"),
        **_ei_fields(seg, 32, "shipment_id", repeating=False),
        "culture_start_datetime": _parse_dtm(seg.get_field(33)),
        "culture_final_datetime": _parse_dtm(seg.get_field(34)),
        "action_code": _v(seg.get_field(35)),
    }


def _extract_in1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cwe_fields(seg, 2, "insurance_plan", repeating=False),
        **_cx_array_fields(seg, 3, "insurance_company"),
        **_xon_array_fields(seg, 4, "insurance_company_name"),
        **_xad_array_fields(seg, 5, "insurance_company_address"),
        **_xpn_array_fields(seg, 6, "insurance_co_contacts"),
        **_xtn_array_fields(seg, 7, "insurance_co_phone_number"),
        "group_number": _v(seg.get_field(8)),
        **_xon_array_fields(seg, 9, "group_name"),
        **_cx_array_fields(seg, 10, "insureds_group_emp"),
        **_xon_array_fields(seg, 11, "insureds_group_emp_name"),
        "plan_effective_date": _parse_dtm(seg.get_field(12)),
        "plan_expiration_date": _parse_dtm(seg.get_field(13)),
        **_aui_fields(seg, 14, "authorization_information"),
        **_cwe_fields(seg, 15, "plan_type", repeating=False),
        **_xpn_array_fields(seg, 16, "insured_names"),
        **_cwe_fields(seg, 17, "insureds_relationship_to_patient", repeating=False),
        "insureds_date_of_birth": _parse_dtm(seg.get_field(18)),
        **_xad_array_fields(seg, 19, "insureds_address"),
        **_cwe_fields(seg, 20, "assignment_of_benefits", repeating=False),
        **_cwe_fields(seg, 21, "coordination_of_benefits", repeating=False),
        "coord_of_ben_priority": _v(seg.get_field(22)),
        "notice_of_admission_flag": _v(seg.get_field(23)),
        "notice_of_admission_date": _parse_dtm(seg.get_field(24)),
        "report_of_eligibility_flag": _v(seg.get_field(25)),
        "report_of_eligibility_date": _parse_dtm(seg.get_field(26)),
        **_cwe_fields(seg, 27, "release_information_code", repeating=False),
        "pre_admit_cert": _v(seg.get_field(28)),
        "verification_datetime": _parse_dtm(seg.get_field(29)),
        **_xcn_array_fields(seg, 30, "verification_by"),
        **_cwe_fields(seg, 31, "type_of_agreement_code", repeating=False),
        **_cwe_fields(seg, 32, "billing_status", repeating=False),
        "lifetime_reserve_days": _i(seg.get_field(33)),
        "delay_before_lr_day": _i(seg.get_field(34)),
        **_cwe_fields(seg, 35, "company_plan_code", repeating=False),
        "policy_number": _v(seg.get_field(36)),
        **_cp_fields(seg, 37, "policy_deductible"),
        **_cp_fields(seg, 38, "policy_limit_amount"),
        "policy_limit_days": _i(seg.get_field(39)),
        **_cp_fields(seg, 40, "room_rate_semi_private"),
        **_cp_fields(seg, 41, "room_rate_private"),
        **_cwe_fields(seg, 42, "insureds_employment_status", repeating=False),
        **_cwe_fields(seg, 43, "insureds_administrative_sex", repeating=False),
        **_xad_array_fields(seg, 44, "insureds_employers_address"),
        "verification_status": _v(seg.get_field(45)),
        **_cwe_fields(seg, 46, "prior_insurance_plan_id", repeating=False),
        **_cwe_fields(seg, 47, "coverage_type", repeating=False),
        **_cwe_fields(seg, 48, "handicap", repeating=False),
        **_cx_array_fields(seg, 49, "insureds_id_number"),
        **_cwe_fields(seg, 50, "signature_code", repeating=False),
        "signature_code_date": _parse_dtm(seg.get_field(51)),
        "insureds_birth_place": _v(seg.get_field(52)),
        **_cwe_fields(seg, 53, "vip_indicator", repeating=False),
        **_cwe_array_fields(seg, 54, "external_health_plan_identifiers"),
        "insurance_action_code": _v(seg.get_field(55)),
    }


def _extract_gt1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cx_array_fields(seg, 2, "guarantor_number"),
        **_xpn_array_fields(seg, 3, "guarantor_names"),
        **_xpn_array_fields(seg, 4, "guarantor_spouse_names"),
        **_xad_array_fields(seg, 5, "guarantor_address"),
        **_xtn_array_fields(seg, 6, "guarantor_ph_num_home"),
        **_xtn_array_fields(seg, 7, "guarantor_ph_num_business"),
        "guarantor_date_of_birth": _parse_dtm(seg.get_field(8)),
        **_cwe_fields(seg, 9, "guarantor_administrative_sex", repeating=False),
        **_cwe_fields(seg, 10, "guarantor_type", repeating=False),
        **_cwe_fields(seg, 11, "guarantor_relationship", repeating=False),
        "guarantor_ssn": _v(seg.get_field(12)),
        "guarantor_date_begin": _parse_dtm(seg.get_field(13)),
        "guarantor_date_end": _parse_dtm(seg.get_field(14)),
        "guarantor_priority": _i(seg.get_field(15)),
        **_xpn_array_fields(seg, 16, "guarantor_employer_names"),
        **_xad_array_fields(seg, 17, "guarantor_employer_address"),
        **_xtn_array_fields(seg, 18, "guarantor_employer_phone_number"),
        **_cx_array_fields(seg, 19, "guarantor_employee_id_number"),
        **_cwe_fields(seg, 20, "guarantor_employment_status", repeating=False),
        **_xon_array_fields(seg, 21, "guarantor_organization_name"),
        "guarantor_billing_hold_flag": _v(seg.get_field(22)),
        **_cwe_fields(seg, 23, "guarantor_credit_rating_code", repeating=False),
        "guarantor_death_date_and_time": _parse_dtm(seg.get_field(24)),
        "guarantor_death_flag": _v(seg.get_field(25)),
        **_cwe_fields(seg, 26, "guarantor_charge_adjustment_code", repeating=False),
        **_cp_fields(seg, 27, "guarantor_household_annual_income"),
        "guarantor_household_size": _i(seg.get_field(28)),
        **_cx_array_fields(seg, 29, "guarantor_employer_id_number"),
        **_cwe_fields(seg, 30, "guarantor_marital_status_code", repeating=False),
        "guarantor_hire_effective_date": _parse_dtm(seg.get_field(31)),
        "employment_stop_date": _parse_dtm(seg.get_field(32)),
        **_cwe_fields(seg, 33, "living_dependency", repeating=False),
        **_cwe_array_fields(seg, 34, "ambulatory_status"),
        **_cwe_array_fields(seg, 35, "citizenship"),
        **_cwe_fields(seg, 36, "primary_language", repeating=False),
        **_cwe_fields(seg, 37, "living_arrangement", repeating=False),
        **_cwe_fields(seg, 38, "publicity_code", repeating=False),
        "protection_indicator": _v(seg.get_field(39)),
        **_cwe_fields(seg, 40, "student_indicator", repeating=False),
        **_cwe_fields(seg, 41, "religion", repeating=False),
        **_xpn_array_fields(seg, 42, "mothers_maiden_names"),
        **_cwe_fields(seg, 43, "nationality", repeating=False),
        **_cwe_array_fields(seg, 44, "ethnic_group"),
        **_xpn_array_fields(seg, 45, "contact_persons"),
        **_xtn_array_fields(seg, 46, "contact_persons_telephone_number"),
        **_cwe_fields(seg, 47, "contact_reason", repeating=False),
        **_cwe_fields(seg, 48, "contact_relationship", repeating=False),
        "job_title": _v(seg.get_field(49)),
        **_jcc_fields(seg, 50, "job_code_class"),
        **_xon_array_fields(seg, 51, "guarantor_employers_org_name"),
        **_cwe_fields(seg, 52, "handicap", repeating=False),
        **_cwe_fields(seg, 53, "job_status", repeating=False),
        **_fc_fields(seg, 54, "guarantor_financial_class"),
        **_cwe_array_fields(seg, 55, "guarantor_race"),
        "guarantor_birth_place": _v(seg.get_field(56)),
        **_cwe_fields(seg, 57, "vip_indicator", repeating=False),
    }


def _extract_ft1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cx_fields(seg, 2, "transaction_id", repeating=False),
        "transaction_batch_id": _v(seg.get_field(3)),
        "transaction_date_start": _parse_dtm(seg.get_component(4, 1)),
        "transaction_date_end": _parse_dtm(seg.get_component(4, 2)),
        "transaction_posting_date": _parse_dtm(seg.get_field(5)),
        **_cwe_fields(seg, 6, "transaction_type", repeating=False),
        **_cwe_fields(seg, 7, "transaction_code", repeating=False),
        "transaction_description": _v(seg.get_field(8)),
        "transaction_description_alt": _v(seg.get_field(9)),
        "transaction_quantity": _i(seg.get_field(10)),
        **_cp_fields(seg, 11, "transaction_amount_extended"),
        **_cp_fields(seg, 12, "transaction_amount_unit"),
        **_cwe_fields(seg, 13, "department_code", repeating=False),
        **_cwe_fields(seg, 14, "insurance_plan_id", repeating=False),
        **_cp_fields(seg, 15, "insurance_amount"),
        **_pl_fields(seg, 16, "assigned_patient_location"),
        **_cwe_fields(seg, 17, "fee_schedule", repeating=False),
        **_cwe_fields(seg, 18, "patient_type", repeating=False),
        **_cwe_array_fields(seg, 19, "diagnosis_code"),
        **_xcn_array_fields(seg, 20, "performed_by"),
        **_xcn_array_fields(seg, 21, "ordered_by"),
        **_cp_fields(seg, 22, "unit_cost"),
        **_ei_fields(seg, 23, "filler_order_number", repeating=False),
        **_xcn_array_fields(seg, 24, "entered_by"),
        **_cwe_fields(seg, 25, "procedure_code", repeating=False),
        **_cwe_array_fields(seg, 26, "procedure_code_modifier"),
        **_cwe_fields(seg, 27, "advanced_beneficiary_notice_code", repeating=False),
        **_cwe_fields(seg, 28, "medically_necessary_dup_proc_reason", repeating=False),
        **_cwe_fields(seg, 29, "ndc_code", repeating=False),
        **_cx_fields(seg, 30, "payment_reference_id", repeating=False),
        **_s_array_fields(seg, 31, "transaction_reference_key"),
        **_xon_array_fields(seg, 32, "performing_facility"),
        **_xon_fields(seg, 33, "ordering_facility"),
        **_cwe_fields(seg, 34, "item_number", repeating=False),
        "model_number": _v(seg.get_field(35)),
        **_cwe_array_fields(seg, 36, "special_processing_code"),
        **_cwe_fields(seg, 37, "clinic_code", repeating=False),
        **_cx_fields(seg, 38, "referral_number", repeating=False),
        **_cx_fields(seg, 39, "authorization_number", repeating=False),
        **_cwe_fields(seg, 40, "service_provider_taxonomy_code", repeating=False),
        **_cwe_fields(seg, 41, "revenue_code", repeating=False),
        "prescription_number": _v(seg.get_field(42)),
        **_cq_fields(seg, 43, "ndc_qty_and_uom"),
        **_cwe_fields(  # dme_certificate_of_medical_necessity_transmission_code
            seg, 44, "dme_certificate_of_medical_necessity_transmission_code", repeating=False),
        **_cwe_fields(seg, 45, "dme_certification_type_code", repeating=False),
        "dme_duration_value": _v(seg.get_field(46)),
        "dme_certification_revision_date": _v(seg.get_field(47)),
        "dme_initial_certification_date": _v(seg.get_field(48)),
        "dme_last_certification_date": _v(seg.get_field(49)),
        "dme_length_of_medical_necessity_days": _v(seg.get_field(50)),
        **_mo_fields(seg, 51, "dme_rental_price"),
        **_mo_fields(seg, 52, "dme_purchase_price"),
        **_cwe_fields(seg, 53, "dme_frequency_code", repeating=False),
        "dme_certification_condition_indicator": _v(seg.get_field(54)),
        **_cwe_array_fields(seg, 55, "dme_condition_indicator_code"),
        **_cwe_fields(seg, 56, "service_reason_code", repeating=False),
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
        **_cwe_array_fields(seg, 9, "administration_notes"),
        **_xcn_array_fields(seg, 10, "administering_provider"),
        "administered_at_location": _v(seg.get_field(11)),
        "administered_per_time_unit": _v(seg.get_field(12)),
        "administered_strength": _v(seg.get_field(13)),
        **_cwe_fields(seg, 14, "administered_strength_units", repeating=False),
        **_s_array_fields(seg, 15, "substance_lot_number"),
        **_dtm_array_fields(seg, 16, "substance_expiration_date"),
        **_cwe_array_fields(seg, 17, "substance_manufacturer_name"),
        **_cwe_array_fields(seg, 18, "substance_treatment_refusal_reason"),
        **_cwe_array_fields(seg, 19, "indication"),
        "completion_status": _v(seg.get_field(20)),
        "action_code_rxa": _v(seg.get_field(21)),
        "system_entry_datetime": _parse_dtm(seg.get_field(22)),
        "administered_drug_strength_volume": _v(seg.get_field(23)),
        **_cwe_fields(seg, 24, "administered_drug_strength_volume_units", repeating=False),
        **_cwe_fields(seg, 25, "administered_barcode_identifier", repeating=False),
        "pharmacy_order_type": _v(seg.get_field(26)),
        **_pl_fields(seg, 27, "administer_at"),
        **_xad_fields(seg, 28, "administered_at_address", repeating=False),
        **_ei_array_fields(seg, 29, "administered_tag_identifier"),
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
        **_xcn_array_fields(seg, 12, "placer_contact_person"),
        **_xtn_fields(seg, 13, "placer_contact_phone", repeating=False),
        **_xad_array_fields(seg, 14, "placer_contact_address"),
        **_pl_fields(seg, 15, "placer_contact_location"),
        **_xcn_array_fields(seg, 16, "filler_contact_person"),
        **_xtn_fields(seg, 17, "filler_contact_phone", repeating=False),
        **_xad_array_fields(seg, 18, "filler_contact_address"),
        **_pl_fields(seg, 19, "filler_contact_location"),
        **_xcn_array_fields(seg, 20, "entered_by_person"),
        **_xtn_array_fields(seg, 21, "entered_by_phone_number"),
        **_pl_fields(seg, 22, "entered_by_location"),
        **_ei_fields(seg, 23, "parent_placer_appointment_id", repeating=False),
        **_ei_fields(seg, 24, "parent_filler_appointment_id", repeating=False),
        **_cwe_fields(seg, 25, "filler_status_code", repeating=False),
        **_ei_array_fields(seg, 26, "placer_order_number"),
        **_ei_array_fields(seg, 27, "filler_order_number"),
        **_eip_fields(seg, 28, "alternate_placer_order_group_number"),
    }


def _extract_txa(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        **_cwe_fields(seg, 2, "document_type", repeating=False),
        "document_content_presentation": _v(seg.get_field(3)),
        "activity_datetime": _parse_dtm(seg.get_field(4)),
        **_xcn_array_fields(seg, 5, "primary_activity_provider"),
        "origination_datetime": _parse_dtm(seg.get_field(6)),
        "transcription_datetime": _parse_dtm(seg.get_field(7)),
        **_dtm_array_fields(seg, 8, "edit_datetime"),
        **_xcn_array_fields(seg, 9, "originator"),
        **_xcn_array_fields(seg, 10, "assigned_document_authenticator"),
        **_xcn_array_fields(seg, 11, "transcriptionist"),
        **_ei_fields(seg, 12, "unique_document_number", repeating=False),
        **_ei_fields(seg, 13, "parent_document_number", repeating=False),
        **_ei_array_fields(seg, 14, "placer_order_number"),
        **_ei_fields(seg, 15, "filler_order_number", repeating=False),
        "unique_document_file_name": _v(seg.get_field(16)),
        "document_completion_status": _v(seg.get_field(17)),
        "document_confidentiality_status": _v(seg.get_field(18)),
        "document_availability_status": _v(seg.get_field(19)),
        "document_storage_status": _v(seg.get_field(20)),
        "document_change_reason": _v(seg.get_field(21)),
        **_xcn_array_fields(seg, 22, "authentication_person_time_stamp"),
        **_xcn_array_fields(seg, 23, "distributed_copies"),
        **_cwe_array_fields(seg, 24, "folder_assignment"),
        **_s_array_fields(seg, 25, "document_title"),
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

    for line in lines:
        if not line.strip():
            continue
        seg_type = line[:3].upper()
        if seg_type in _BATCH_ENVELOPE_SEGMENTS:
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
