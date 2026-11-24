"""Focused column-value tests for ADME flatteners.

The generic LakeflowConnect suite exercises record iteration and schema
shape, but does not assert that typed columns are actually populated.
This file closes that gap: each flattener is fed a hand-crafted record
that uses the canonical ``master-data`` payload structure documented in
``adme_schemas.py``, and we assert that the typed columns it emits are
non-null and carry the expected values.

If the simulator corpus ever drifts to a non-canonical OSDU kind (for
example a ``work-product-component`` shape that substring-matches the
target kind name), these tests still pass because they bypass the
simulator entirely.
"""

from databricks.labs.community_connector.sources.adme.adme import (
    _flatten_reservoir,
    _flatten_sample,
    _flatten_wellbore,
    _redact_body,
)


# ---------------------------------------------------------------------------
# Reservoir (osdu:wks:master-data--Reservoir:1.2.0)
# ---------------------------------------------------------------------------

def test_flatten_reservoir_populates_typed_columns():
    record_data = {
        "ReservoirID": "opendes:master-data--Reservoir:RES-001:",
        "ReservoirName": "Wolfcamp A Upper",
        "ReservoirType": "Unconventional",
        "ReservoirDescription": "Wolfcamp A upper bench",
        "FieldID": "opendes:master-data--Field:MidlandBasinWest:",
        "BasinID": "opendes:master-data--Basin:PermianBasin:",
        "FormationID": "opendes:master-data--GeologicalFormation:Wolfcamp:",
        "FluidTypeID": "opendes:reference-data--FluidType:Oil:",
        "DepthTopMD": 2285.0,
        "DepthBaseMD": 2524.5,
        "DepthTopTVD": 2103.8,
        "DepthBaseTVD": 2325.6,
        "GrossThickness": 239.5,
        "NetPayThickness": 180.0,
        "PorosityAverage": 0.078,
        "WaterSaturationAverage": 0.280,
        "PermeabilityHorizontal": 0.0012,
        "PermeabilityVertical": 0.00018,
        "InitialReservoirPressure": 4820.0,
        "ReservoirTemperature": 85.0,
        "GeoContexts": [{"GeoTypeID": "Country"}],
        "NameAliases": [{"AliasName": "Wolfcamp A"}],
        "ExtensionProperties": {"BasinSubtype": "Delaware-adjacent"},
    }

    out = _flatten_reservoir(record_data)

    assert out["ReservoirID"] == record_data["ReservoirID"]
    assert out["ReservoirName"] == "Wolfcamp A Upper"
    assert out["ReservoirType"] == "Unconventional"
    assert out["FieldID"] == record_data["FieldID"]
    assert out["FluidTypeID"] == record_data["FluidTypeID"]
    assert out["DepthTopMD"] == 2285.0
    assert isinstance(out["DepthTopMD"], float)
    assert out["DepthBaseTVD"] == 2325.6
    assert out["PorosityAverage"] == 0.078
    assert out["PermeabilityHorizontal"] == 0.0012
    assert out["PermeabilityVertical"] == 0.00018
    assert out["InitialReservoirPressure"] == 4820.0
    assert out["ReservoirTemperature"] == 85.0
    assert out["GeoContexts_json"] is not None
    assert "GeoTypeID" in out["GeoContexts_json"]
    assert out["NameAliases_json"] is not None
    assert "Wolfcamp A" in out["NameAliases_json"]
    assert out["ExtensionProperties_json"] is not None


def test_flatten_reservoir_with_reservoirzone_payload_emits_nulls():
    """Regression for review feedback: the wrong OSDU kind shape (here a
    ``work-product-component--ReservoirZone`` payload) must NOT silently
    populate the master-data--Reservoir typed columns. Field names don't
    overlap, so every typed column should come back null."""
    zone_payload = {
        "ZoneName": "Wolfcamp A Upper",
        "TopDepthMD": 2285.0,
        "BaseDepthMD": 2524.5,
        "Porosity": 0.078,
        "Permeability": 0.0012,
        "WaterSaturation": 0.280,
        "ReservoirTemperature": 85.0,
    }

    out = _flatten_reservoir(zone_payload)

    assert out["ReservoirID"] is None
    assert out["ReservoirName"] is None
    assert out["DepthTopMD"] is None
    assert out["DepthBaseMD"] is None
    assert out["PorosityAverage"] is None
    assert out["PermeabilityHorizontal"] is None
    assert out["WaterSaturationAverage"] is None
    assert out["ReservoirTemperature"] == 85.0


# ---------------------------------------------------------------------------
# Rock_and_Fluid (osdu:wks:master-data--Sample:2.1.0)
# ---------------------------------------------------------------------------

def test_flatten_sample_populates_typed_columns():
    record_data = {
        "SampleAcquisition": {
            "SampleAcquisitionJobID": "opendes:master-data--SampleAcquisitionJob:JOB-001:",
            "SampleAcquisitionTypeID": "opendes:reference-data--SampleAcquisitionType:CorePlug:",
            "SampleAcquisitionContainerID": "opendes:master-data--SampleContainer:CP-047:",
            "AcquisitionStartDate": "2023-09-22T08:00:00.000Z",
            "AcquisitionEndDate": "2023-09-22T16:30:00.000Z",
            "CollectionServiceCompanyID": "opendes:master-data--Organisation:Halliburton:",
            "HandlingServiceCompanyID": "opendes:master-data--Organisation:CoreLab:",
            "SampleAcquisitionDetail": {
                "WellboreID": "opendes:master-data--Wellbore:WB-001:",
                "ToolKind": "ConventionalCore",
                "RunNumber": "3",
                "TopDepth": 2418.5,
                "BaseDepth": 2420.0,
                "FormationCondition": {
                    "Pressure": 3500.0,
                    "Temperature": 23.9,
                },
            },
        },
        "ExtensionProperties": {"PorosityMeasured": 0.078},
    }

    out = _flatten_sample(record_data)

    assert out["SampleAcquisitionJobID"] == record_data["SampleAcquisition"]["SampleAcquisitionJobID"]
    assert out["SampleAcquisitionTypeID"].endswith(":CorePlug:")
    assert out["AcquisitionStartDate"] == "2023-09-22T08:00:00.000Z"
    assert out["WellboreID"] == "opendes:master-data--Wellbore:WB-001:"
    assert out["ToolKind"] == "ConventionalCore"
    assert out["RunNumber"] == "3"
    assert out["TopDepth"] == 2418.5
    assert out["BaseDepth"] == 2420.0
    assert out["FormationPressure"] == 3500.0
    assert out["FormationTemperature"] == 23.9
    assert out["SampleAcquisition_json"] is not None
    assert "SampleAcquisitionDetail" in out["SampleAcquisition_json"]
    assert out["ExtensionProperties_json"] is not None


def test_flatten_sample_with_rockandfluidsample_payload_emits_nulls():
    """Regression: a ``work-product-component--RockAndFluidSample`` payload
    (top-level scalars like SampleName/Porosity/Permeability) must NOT
    populate any master-data--Sample typed column."""
    wpc_payload = {
        "SampleName": "PBE-1 Wolfcamp A Core Plug CP-047",
        "SampleID": "CP-047",
        "Porosity": 0.078,
        "Permeability": 0.0009,
        "WaterSaturation": 0.285,
    }

    out = _flatten_sample(wpc_payload)

    assert out["SampleAcquisitionJobID"] is None
    assert out["SampleAcquisitionTypeID"] is None
    assert out["WellboreID"] is None
    assert out["ToolKind"] is None
    assert out["TopDepth"] is None
    assert out["BaseDepth"] is None
    assert out["FormationPressure"] is None
    assert out["FormationTemperature"] is None


# ---------------------------------------------------------------------------
# Wellbore — sanity check the canonical-shape flattener still works.
# ---------------------------------------------------------------------------

def test_flatten_wellbore_populates_typed_columns():
    record_data = {
        "FacilityID": "opendes:master-data--Wellbore:WB-001:",
        "FacilityName": "PBE-1",
        "FacilityTypeID": "opendes:reference-data--FacilityType:Wellbore:",
        "WellID": "opendes:master-data--Well:WELL-001:",
        "KickOffWellboreID": None,
        "StatusSummary": "Producing",
        "TargetFormation": "Wolfcamp A",
    }

    out = _flatten_wellbore(record_data)

    assert out["FacilityID"] == record_data["FacilityID"]
    assert out["FacilityName"] == "PBE-1"
    assert out["WellID"] == record_data["WellID"]
    assert out["StatusSummary"] == "Producing"


# ---------------------------------------------------------------------------
# Error-body redaction helper
# ---------------------------------------------------------------------------

def test_redact_body_strips_crlf_and_truncates():
    long_body = "x" * 1000 + "\nleak"
    out = _redact_body(long_body)

    assert "\n" not in out
    assert "\r" not in out
    assert out.endswith("...[truncated]")
    assert len(out) <= 256 + len("...[truncated]")


def test_redact_body_passthrough_short():
    assert _redact_body("short body") == "short body"
    assert _redact_body("") == ""
