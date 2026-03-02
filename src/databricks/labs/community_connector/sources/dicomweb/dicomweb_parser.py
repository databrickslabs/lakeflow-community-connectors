"""
DICOM JSON tag parser for QIDO-RS responses.

QIDO-RS returns arrays of DICOM JSON objects keyed by 8-digit uppercase hex tags:
    {
        "00080020": {"vr": "DA", "Value": ["20231215"]},
        "00100010": {"vr": "PN", "Value": [{"Alphabetic": "Doe^John"}]}
    }

This module maps those tags to human-readable field names and extracts values
with awareness of DICOM Value Representations (VR).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Tag maps
# ---------------------------------------------------------------------------

STUDY_TAG_MAP: dict[str, str] = {
    "0020000D": "StudyInstanceUID",
    "00080020": "StudyDate",
    "00080030": "StudyTime",
    "00080050": "AccessionNumber",
    "00100010": "PatientName",
    "00100020": "PatientID",
    "00081030": "StudyDescription",
    "00080061": "ModalitiesInStudy",
    "00201206": "NumberOfStudyRelatedSeries",
    "00201208": "NumberOfStudyRelatedInstances",
}

SERIES_TAG_MAP: dict[str, str] = {
    "0020000E": "SeriesInstanceUID",
    "0020000D": "StudyInstanceUID",
    "00080020": "StudyDate",
    "00200011": "SeriesNumber",
    "0008103E": "SeriesDescription",
    "00080060": "Modality",
    "00180015": "BodyPartExamined",
    "00080021": "SeriesDate",
}

INSTANCE_TAG_MAP: dict[str, str] = {
    "00080018": "SOPInstanceUID",
    "0020000E": "SeriesInstanceUID",
    "0020000D": "StudyInstanceUID",
    "00080016": "SOPClassUID",
    "00200013": "InstanceNumber",
    "00080020": "StudyDate",
    "00080023": "ContentDate",
    "00080033": "ContentTime",
}

# VRs that carry a single string scalar (or list → join / first element)
_STRING_VRS = {"DA", "TM", "CS", "LO", "UI", "SH", "LT", "ST", "UT", "AE", "AS", "DT", "UC", "UR"}
# VRs that carry numeric values
_NUMERIC_VRS = {"IS", "DS", "FL", "FD", "SL", "SS", "UL", "US", "AT", "OB", "OW", "OF", "OD", "OL", "OV"}
# Multi-valued string VRs (arrays stay as lists)
_MULTI_STRING_VRS = {"CS"}  # ModalitiesInStudy uses CS and can be multi-valued


# ---------------------------------------------------------------------------
# Low-level value extraction
# ---------------------------------------------------------------------------


def _extract_value(tag_obj: dict, field_name: str) -> object:
    """Extract a Python value from a DICOM JSON tag object."""
    vr = tag_obj.get("vr", "")
    values = tag_obj.get("Value", [])

    if not values:
        return None

    if vr == "PN":
        # Person Name — take Alphabetic component of the first element
        first = values[0]
        if isinstance(first, dict):
            return first.get("Alphabetic") or first.get("Ideographic") or first.get("Phonetic")
        return str(first)

    if vr in _STRING_VRS:
        # ModalitiesInStudy (CS, multi-valued) → return as list
        if field_name == "ModalitiesInStudy":
            return [str(v) for v in values]
        return str(values[0]) if len(values) == 1 else str(values[0])

    if vr in _NUMERIC_VRS:
        first = values[0]
        try:
            if vr in {"IS", "US", "UL", "SS", "SL"}:
                return int(first)
            return float(first)
        except (TypeError, ValueError):
            return None

    # Fallback: return first value as-is or stringified
    first = values[0]
    if isinstance(first, (str, int, float, bool)):
        return first
    return str(first)


# ---------------------------------------------------------------------------
# Public parse functions
# ---------------------------------------------------------------------------


def parse_dicom_json(dicom_obj: dict, tag_map: dict[str, str]) -> dict:
    """
    Convert a single DICOM JSON object to a Python dict using the given tag map.

    Args:
        dicom_obj: Dict keyed by 8-character uppercase hex tags.
        tag_map:   Mapping of hex tag → field name.

    Returns:
        Dict of {field_name: value} for all recognised tags that have a value.
    """
    result: dict = {}
    for tag, field_name in tag_map.items():
        tag_upper = tag.upper()
        tag_obj = dicom_obj.get(tag_upper) or dicom_obj.get(tag.lower())
        if tag_obj is None:
            result[field_name] = None
            continue
        result[field_name] = _extract_value(tag_obj, field_name)
    return result


def parse_study(dicom_obj: dict) -> dict:
    """Parse a QIDO-RS study-level DICOM JSON object."""
    return parse_dicom_json(dicom_obj, STUDY_TAG_MAP)


def parse_series(dicom_obj: dict) -> dict:
    """Parse a QIDO-RS series-level DICOM JSON object."""
    return parse_dicom_json(dicom_obj, SERIES_TAG_MAP)


def parse_instance(dicom_obj: dict) -> dict:
    """Parse a QIDO-RS instance-level DICOM JSON object."""
    record = parse_dicom_json(dicom_obj, INSTANCE_TAG_MAP)
    # dicom_file_path is filled in later by the connector when WADO-RS retrieval is enabled
    record.setdefault("dicom_file_path", None)
    # metadata is filled in later when fetch_metadata=true
    record.setdefault("metadata", None)
    return record
