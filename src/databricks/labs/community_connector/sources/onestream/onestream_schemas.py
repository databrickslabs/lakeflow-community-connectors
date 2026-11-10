"""Static table definitions, Spark schemas and OneStream encoding helpers.

Column lists come from the platform's INFORMATION_SCHEMA (see
``onestream_api_doc.md``). They are pinned here so the connector issues
explicit column lists and never depends on the shape of an API response
(empty results carry no columns).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Generated from INFORMATION_SCHEMA of a OneStream 9.3.3 application (2026-11-10).
# (column_name, sql_type, precision, scale, nullable)
SOURCE_COLUMNS: dict[str, list[tuple]] = {
    "Dim": [
        ("DimTypeId", "int", 10, 0, False),
        ("DimId", "int", 10, 0, False),
        ("Name", "nvarchar", None, None, False),
        ("Description", "nvarchar", None, None, False),
        ("AccessGroupUniqueID", "uniqueidentifier", None, None, False),
        ("MaintenanceGroupUniqueID", "uniqueidentifier", None, None, False),
        ("InheritedDimId", "int", 10, 0, False),
        ("DimMemberSourceTypeId", "int", 10, 0, False),
        ("DimMemberSourcePath", "nvarchar", None, None, False),
        ("DimMemberSourceNVPairs", "nvarchar", None, None, False),
        ("XmlData", "nvarchar", None, None, False),
    ],
    "Member": [
        ("DimTypeId", "int", 10, 0, False),
        ("MemberId", "int", 10, 0, False),
        ("Name", "nvarchar", None, None, False),
        ("Alias", "nvarchar", None, None, False),
        ("Description", "nvarchar", None, None, False),
        ("DimId", "int", 10, 0, False),
        ("DisplayMemberGroupUniqueID", "uniqueidentifier", None, None, False),
        ("ReadDataGroupUniqueID", "uniqueidentifier", None, None, False),
        ("ReadDataGroupUniqueID2", "uniqueidentifier", None, None, False),
        ("ReadWriteDataGroupUniqueID", "uniqueidentifier", None, None, False),
        ("ReadWriteDataGroupUniqueID2", "uniqueidentifier", None, None, False),
        ("UseCubeDataAccessSecurity", "bit", None, None, False),
        ("DataCellAccessCategories", "nvarchar", None, None, False),
        ("ConditionalInputCategories", "nvarchar", None, None, False),
        ("DataMgmtAccessCategories", "nvarchar", None, None, False),
    ],
    "Relationship": [
        ("DimTypeId", "int", 10, 0, False),
        ("ParentId", "int", 10, 0, False),
        ("ChildId", "int", 10, 0, False),
        ("DimId", "int", 10, 0, False),
        ("SiblingSortOrder", "bigint", 19, 0, False),
        ("UDAggWeight", "decimal", 28, 9, False),
    ],
    "MemberProperty": [
        ("DimTypeId", "int", 10, 0, False),
        ("MemberId", "int", 10, 0, False),
        ("PropertyId", "int", 10, 0, False),
        ("VaryByCubeTypeId", "int", 10, 0, False),
        ("VaryByScenarioTypeId", "int", 10, 0, False),
        ("VaryByTimeId", "int", 10, 0, False),
        ("RevertToDefaultCubeType", "bit", None, None, False),
        ("RevertToDefaultScenarioType", "bit", None, None, False),
        ("PropStorageTypeId", "int", 10, 0, False),
        ("DecimalValue", "decimal", 28, 9, False),
        ("TextValue", "nvarchar", None, None, True),
    ],
    "Cube": [
        ("CubeId", "int", 10, 0, False),
        ("Name", "nvarchar", None, None, False),
        ("Description", "nvarchar", None, None, False),
        ("CubeType", "int", 10, 0, False),
        ("NumberType", "int", 10, 0, False),
        ("StorageType", "int", 10, 0, False),
        ("IsTopLevelCube", "bit", None, None, False),
        ("TimeDimProfileID", "uniqueidentifier", None, None, False),
        ("AccessGroupUniqueID", "uniqueidentifier", None, None, False),
        ("MaintenanceGroupUniqueID", "uniqueidentifier", None, None, False),
        ("ConsAlgorithmType", "int", 10, 0, False),
        ("TransAlgorithmType", "int", 10, 0, False),
        ("CalcNoneConsIfNoData", "bit", None, None, False),
        ("CalcLocalCurrIfNoData", "bit", None, None, False),
        ("CalcTransCurrsIfNoData", "bit", None, None, False),
        ("CalcOwnerPreAdjIfNoData", "bit", None, None, False),
        ("CalcShareIfNoData", "bit", None, None, False),
        ("CalcElimIfNoData", "bit", None, None, False),
        ("CalcOwnerPostAdjIfNoData", "bit", None, None, False),
        ("BR1Name", "nvarchar", None, None, False),
        ("BR2Name", "nvarchar", None, None, False),
        ("BR3Name", "nvarchar", None, None, False),
        ("BR4Name", "nvarchar", None, None, False),
        ("BR5Name", "nvarchar", None, None, False),
        ("BR6Name", "nvarchar", None, None, False),
        ("BR7Name", "nvarchar", None, None, False),
        ("BR8Name", "nvarchar", None, None, False),
        ("DefaultCurrencyId", "int", 10, 0, False),
        ("FxRateTypeIDForRevExp", "uniqueidentifier", None, None, False),
        ("FxRuleTypeIdForRevExp", "int", 10, 0, False),
        ("FxRateTypeIDForAssetLiab", "uniqueidentifier", None, None, False),
        ("FxRuleTypeIdForAssetLiab", "int", 10, 0, False),
        ("XmlData", "nvarchar", None, None, False),
    ],
    "CubeDim": [
        ("CubeId", "int", 10, 0, False),
        ("DimTypeId", "int", 10, 0, False),
        ("ScenarioTypeId", "int", 10, 0, False),
        ("DimId", "int", 10, 0, False),
    ],
    "FxRate": [
        ("FxRateTypeID", "uniqueidentifier", None, None, False),
        ("TimeId", "int", 10, 0, False),
        ("SourceCurrencyId", "int", 10, 0, False),
        ("DestCurrencyId", "int", 10, 0, False),
        ("Amount", "decimal", 28, 9, False),
    ],
    "FxRateType": [
        ("UniqueID", "uniqueidentifier", None, None, False),
        ("Name", "nvarchar", None, None, False),
        ("Description", "nvarchar", None, None, False),
        ("AccessGroupUniqueID", "uniqueidentifier", None, None, False),
        ("MaintenanceGroupUniqueID", "uniqueidentifier", None, None, False),
        ("XmlData", "nvarchar", None, None, False),
    ],
}


def spark_type(sql_type: str, precision, scale) -> DataType:
    """Map a SQL Server type to a Spark type (integers widen to LongType)."""
    t = sql_type.lower()
    if t in ("int", "smallint", "tinyint", "bigint"):
        return LongType()
    if t == "bit":
        return BooleanType()
    if t in ("decimal", "numeric"):
        return DecimalType(int(precision), int(scale))
    if t in ("money", "smallmoney"):
        return DecimalType(19, 4)
    if t in ("float", "real"):
        return DoubleType()
    if t in ("datetime", "datetime2", "smalldatetime", "datetimeoffset"):
        return TimestampType()
    return StringType()


class SourceTable:
    """A connector table backed by one OneStream application table."""

    def __init__(
        self,
        name: str,
        source_table: str,
        primary_keys: list[str],
        exclude_columns: list[str] | None = None,
    ) -> None:
        self.name = name
        self.source_table = source_table  # table in the dbo schema
        self.primary_keys = list(primary_keys)
        self.exclude_columns = list(exclude_columns or [])

    @property
    def columns(self) -> list[tuple]:
        return [c for c in SOURCE_COLUMNS[self.source_table] if c[0] not in self.exclude_columns]

    @property
    def column_names(self) -> list[str]:
        return [c[0] for c in self.columns]

    def schema(self) -> StructType:
        return StructType(
            [StructField(c[0], spark_type(c[1], c[2], c[3]), c[4]) for c in self.columns]
        )


SNAPSHOT_TABLES: dict[str, SourceTable] = {
    t.name: t
    for t in [
        SourceTable("dimensions", "Dim", ["DimTypeId", "DimId"]),
        SourceTable("members", "Member", ["DimTypeId", "MemberId"]),
        SourceTable("member_relationships", "Relationship", ["DimTypeId", "ParentId", "ChildId"]),
        SourceTable(
            "member_properties",
            "MemberProperty",
            [
                "DimTypeId",
                "MemberId",
                "PropertyId",
                "VaryByCubeTypeId",
                "VaryByScenarioTypeId",
                "VaryByTimeId",
            ],
        ),
        SourceTable("cubes", "Cube", ["CubeId"]),
        SourceTable("cube_dimensions", "CubeDim", ["CubeId", "DimTypeId", "ScenarioTypeId"]),
        SourceTable(
            "fx_rates", "FxRate", ["FxRateTypeID", "TimeId", "SourceCurrencyId", "DestCurrencyId"]
        ),
        SourceTable("fx_rate_types", "FxRateType", ["UniqueID"]),
    ]
}

CUBE_DATA = "cube_data"
SUPPORTED_TABLES = [CUBE_DATA, *SNAPSHOT_TABLES]

# ---------------------------------------------------------------------------
# cube_data (DataRecord{YYYY}, unpivoted to one row per month)
# ---------------------------------------------------------------------------

# Data-unit columns (in clustered-key order after PartitionId).
DATA_UNIT_COLUMNS = ["CubeId", "EntityId", "ParentId", "ConsId", "ScenarioId", "YearId"]
# Remaining clustered-key columns of DataRecord{YYYY} (PK order).
DATA_RECORD_PK = [
    "PartitionId",
    *DATA_UNIT_COLUMNS,
    "AccountId",
    "FlowId",
    "OriginId",
    "ICId",
    "UD1Id",
    "UD2Id",
    "UD34Id",
    "UD56Id",
    "UD78Id",
]
INTERSECTION_COLUMNS = [
    "AccountId",
    "FlowId",
    "OriginId",
    "ICId",
    "UD1Id",
    "UD2Id",
    "UD3Id",
    "UD4Id",
    "UD5Id",
    "UD6Id",
    "UD7Id",
    "UD8Id",
]
MONTH_VALUE_COLUMNS = [f"M{m}Value" for m in range(1, 13)]
MONTH_STATUS_COLUMNS = [f"M{m}Status" for m in range(1, 13)]
DATA_RECORD_SELECT = [
    "PartitionId",
    *DATA_UNIT_COLUMNS,
    *INTERSECTION_COLUMNS,
    *[c for m in range(1, 13) for c in (f"M{m}Value", f"M{m}Status")],
]

CUBE_DATA_CURSOR = "_du_timestamp_ticks"
CUBE_DATA_PRIMARY_KEYS = [
    "CubeId",
    "EntityId",
    "ParentId",
    "ConsId",
    "ScenarioId",
    "TimeId",
    *INTERSECTION_COLUMNS,
]
CUBE_DATA_SCHEMA = StructType(
    [StructField(c, LongType(), False) for c in DATA_UNIT_COLUMNS]
    + [StructField("TimeId", LongType(), False), StructField("Period", LongType(), False)]
    + [StructField(c, LongType(), False) for c in INTERSECTION_COLUMNS]
    + [
        StructField("Amount", DecimalType(28, 9), False),
        StructField("CellStatus", LongType(), False),
        StructField(CUBE_DATA_CURSOR, LongType(), False),
        StructField("_du_modified_at", TimestampType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Encodings
# ---------------------------------------------------------------------------

# TimeId = Year * 1_000_000 + PeriodIndex * 1_000 (standard monthly profile).
MONTH_PERIOD_INDEX = {
    1: 3,
    2: 4,
    3: 5,
    4: 7,
    5: 8,
    6: 9,
    7: 12,
    8: 13,
    9: 14,
    10: 16,
    11: 17,
    12: 18,
}


def time_id(year: int, month: int) -> int:
    return year * 1_000_000 + MONTH_PERIOD_INDEX[month] * 1_000


DIM_TYPE_ENTITY = 0
DIM_TYPE_SCENARIO = 2

# .NET DateTime.Ticks: 100 ns intervals since 0001-01-01 (UTC on OneStream cloud).
_TICKS_EPOCH = datetime(1, 1, 1, tzinfo=timezone.utc)


def ticks_to_iso(ticks: int) -> str:
    return (_TICKS_EPOCH + timedelta(microseconds=int(ticks) // 10)).isoformat()


def datetime_to_ticks(dt: datetime) -> int:
    delta = dt.astimezone(timezone.utc) - _TICKS_EPOCH
    return (delta.days * 86_400 + delta.seconds) * 10_000_000 + delta.microseconds * 10
