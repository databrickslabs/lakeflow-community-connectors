"""Static schema definitions for the ADME (OSDU) connector tables.

OSDU records share a common envelope (``id``, ``kind``, ``version``,
``acl``, ``legal``, ``createTime``, ``modifyTime``, ``data``...). The
``data`` payload differs per OSDU kind.

Approach: top-level envelope fields are first-class typed columns;
``data.*`` fields are flattened to a curated subset of typed columns
per kind based on the canonical OSDU master-data shapes documented
in ``adme_api_doc.md``. Complex nested structures (SpatialLocation,
SampleAcquisitionDetail, FacilityNameAliases, VerticalMeasurements,
ExtensionProperties, etc.) are kept as JSON-encoded strings to avoid
fragile StructType layouts that would break on minor schema drift
across OSDU versions and ADME instances.

Reference kinds (per ``adme_api_doc.md``):
  - Wellbore       → osdu:wks:master-data--Wellbore:1.0.0
  - Reservoir      → osdu:wks:master-data--Reservoir:1.2.0
  - Rock_and_Fluid → osdu:wks:master-data--Sample:2.1.0
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Connector tables -> OSDU kind strings
# ---------------------------------------------------------------------------
#
# Use the wildcard form for queries so the connector picks up minor
# schema-version updates (e.g. 1.0.0 -> 1.0.1) without code changes;
# the canonical version is recorded for documentation only.
TABLE_TO_KIND_QUERY: dict[str, str] = {
    "Wellbore": "osdu:wks:master-data--Wellbore:*",
    "Reservoir": "osdu:wks:master-data--Reservoir:*",
    "Rock_and_Fluid": "osdu:wks:master-data--Sample:*",
}

TABLE_TO_KIND_CANONICAL: dict[str, str] = {
    "Wellbore": "osdu:wks:master-data--Wellbore:1.0.0",
    "Reservoir": "osdu:wks:master-data--Reservoir:1.2.0",
    "Rock_and_Fluid": "osdu:wks:master-data--Sample:2.1.0",
}

SUPPORTED_TABLES: list[str] = list(TABLE_TO_KIND_QUERY.keys())


# ---------------------------------------------------------------------------
# Common OSDU envelope fields shared by every record kind
# ---------------------------------------------------------------------------
def _envelope_fields() -> list[StructField]:
    return [
        StructField("id", StringType(), False),
        StructField("kind", StringType(), True),
        # ``version`` is microsecond-precision Unix epoch encoded as int64.
        StructField("version", LongType(), True),
        StructField("createTime", StringType(), True),
        StructField("createUser", StringType(), True),
        StructField("modifyTime", StringType(), True),
        StructField("modifyUser", StringType(), True),
        # acl/legal are small structured objects; flatten the most useful
        # arrays so they're queryable without a JSON parse.
        StructField("acl_owners", ArrayType(StringType()), True),
        StructField("acl_viewers", ArrayType(StringType()), True),
        StructField("legal_legaltags", ArrayType(StringType()), True),
        StructField("legal_status", StringType(), True),
        StructField(
            "legal_otherRelevantDataCountries", ArrayType(StringType()), True
        ),
        # Free-form OSDU bits we keep as JSON for fidelity without
        # committing to a specific Spark struct shape.
        StructField("tags_json", StringType(), True),
        StructField("meta_json", StringType(), True),
        # Full original ``data`` envelope as JSON — gives downstream
        # users an escape hatch for fields not lifted to typed columns.
        StructField("data_json", StringType(), True),
    ]


# ---------------------------------------------------------------------------
# Wellbore (osdu:wks:master-data--Wellbore:1.0.0)
# ---------------------------------------------------------------------------
WELLBORE_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        StructField("FacilityID", StringType(), True),
        StructField("FacilityName", StringType(), True),
        StructField("FacilityTypeID", StringType(), True),
        StructField("WellID", StringType(), True),
        StructField("KickOffWellboreID", StringType(), True),
        StructField("StatusSummary", StringType(), True),
        StructField("TargetFormation", StringType(), True),
        StructField("FacilityNameAliases_json", StringType(), True),
        StructField("FacilityOperators_json", StringType(), True),
        StructField("VerticalMeasurements_json", StringType(), True),
        StructField("SpatialLocation_json", StringType(), True),
        StructField("GeoContexts_json", StringType(), True),
        StructField("DrillingReasons_json", StringType(), True),
        StructField("InitialCompletion_json", StringType(), True),
        StructField("ExtensionProperties_json", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Reservoir (osdu:wks:master-data--Reservoir:1.2.0)
# ---------------------------------------------------------------------------
RESERVOIR_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        StructField("ReservoirID", StringType(), True),
        StructField("ReservoirName", StringType(), True),
        StructField("ReservoirType", StringType(), True),
        StructField("ReservoirDescription", StringType(), True),
        StructField("FieldID", StringType(), True),
        StructField("BasinID", StringType(), True),
        StructField("FormationID", StringType(), True),
        StructField("FluidTypeID", StringType(), True),
        StructField("DepthTopMD", DoubleType(), True),
        StructField("DepthBaseMD", DoubleType(), True),
        StructField("DepthTopTVD", DoubleType(), True),
        StructField("DepthBaseTVD", DoubleType(), True),
        StructField("GrossThickness", DoubleType(), True),
        StructField("NetPayThickness", DoubleType(), True),
        StructField("PorosityAverage", DoubleType(), True),
        StructField("WaterSaturationAverage", DoubleType(), True),
        StructField("PermeabilityHorizontal", DoubleType(), True),
        StructField("PermeabilityVertical", DoubleType(), True),
        StructField("InitialReservoirPressure", DoubleType(), True),
        StructField("ReservoirTemperature", DoubleType(), True),
        StructField("GeoContexts_json", StringType(), True),
        StructField("NameAliases_json", StringType(), True),
        StructField("ExtensionProperties_json", StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Rock_and_Fluid (osdu:wks:master-data--Sample:2.1.0)
# ---------------------------------------------------------------------------
# The OSDU Sample kind keeps almost everything inside the
# ``data.SampleAcquisition`` nested struct. Lift the most useful
# scalars to typed columns; preserve the whole nested object as JSON
# for fidelity.
ROCK_AND_FLUID_SCHEMA: StructType = StructType(
    _envelope_fields()
    + [
        StructField("SampleAcquisitionJobID", StringType(), True),
        StructField("SampleAcquisitionTypeID", StringType(), True),
        StructField("SampleAcquisitionContainerID", StringType(), True),
        StructField("AcquisitionStartDate", StringType(), True),
        StructField("AcquisitionEndDate", StringType(), True),
        StructField("CollectionServiceCompanyID", StringType(), True),
        StructField("HandlingServiceCompanyID", StringType(), True),
        StructField("WellboreID", StringType(), True),
        StructField("ToolKind", StringType(), True),
        StructField("RunNumber", StringType(), True),
        StructField("TopDepth", DoubleType(), True),
        StructField("BaseDepth", DoubleType(), True),
        StructField("FormationPressure", DoubleType(), True),
        StructField("FormationTemperature", DoubleType(), True),
        StructField("SampleAcquisition_json", StringType(), True),
        StructField("ExtensionProperties_json", StringType(), True),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "Wellbore": WELLBORE_SCHEMA,
    "Reservoir": RESERVOIR_SCHEMA,
    "Rock_and_Fluid": ROCK_AND_FLUID_SCHEMA,
}


# ---------------------------------------------------------------------------
# Per-table metadata
# ---------------------------------------------------------------------------
# All three tables have ``id`` as PK and use ``modifyTime`` for CDC.
# Deletes happen at the OSDU Storage layer and are not reflected in the
# Search index — so ``cdc`` (no deletes) is the correct ingestion type.
TABLE_METADATA: dict[str, dict] = {
    "Wellbore": {
        "primary_keys": ["id"],
        "cursor_field": "modifyTime",
        "ingestion_type": "cdc",
    },
    "Reservoir": {
        "primary_keys": ["id"],
        "cursor_field": "modifyTime",
        "ingestion_type": "cdc",
    },
    "Rock_and_Fluid": {
        "primary_keys": ["id"],
        "cursor_field": "modifyTime",
        "ingestion_type": "cdc",
    },
}
