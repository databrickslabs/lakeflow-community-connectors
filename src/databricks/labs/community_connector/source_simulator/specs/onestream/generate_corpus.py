"""Regenerate ``corpus/onestream_db.json`` -- synthetic OneStream application tables.

All names and ids are invented. Run: ``python generate_corpus.py``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from databricks.labs.community_connector.sources.onestream.onestream_schemas import (
    DATA_RECORD_SELECT,
    SOURCE_COLUMNS,
    datetime_to_ticks,
    time_id,
)

GUID = "00000000-0000-0000-0000-{:012d}"


def _default(sql_type: str, i: int, col: str):
    if sql_type in ("int", "smallint", "tinyint", "bigint"):
        return i
    if sql_type == "bit":
        return i % 2 == 0
    if sql_type in ("decimal", "numeric"):
        return round(1.25 * (i + 1), 2)
    if sql_type == "uniqueidentifier":
        return GUID.format(1000 + i)
    if col == "XmlData":
        return f'<Definition id="{i}" />'
    return f"{col}_{i}"


def _rows(table: str, overrides: list[dict]) -> list[dict]:
    out = []
    for i, ov in enumerate(overrides):
        row = {c[0]: _default(c[1], i, c[0]) for c in SOURCE_COLUMNS[table]}
        row.update(ov)
        out.append(row)
    return out


def build() -> dict:
    db: dict[str, list[dict]] = {}
    db["Dim"] = _rows(
        "Dim",
        [
            {"DimTypeId": 0, "DimId": 1, "Name": "SimEntityDim", "InheritedDimId": -1},
            {"DimTypeId": 2, "DimId": 2, "Name": "SimScenarioDim", "InheritedDimId": -1},
            {"DimTypeId": 5, "DimId": 3, "Name": "SimAccountDim", "InheritedDimId": -1},
        ],
    )
    members = [
        (0, 100, "SimCorp"),
        (0, 101, "SimEast"),
        (0, 102, "SimWest"),
        (0, 103, "SimNorth"),
        (2, 200, "Actual"),
        (2, 201, "Budget"),
        (5, 300, "Revenue"),
        (5, 301, "COGS"),
        (5, 302, "Opex"),
    ]
    db["Member"] = _rows(
        "Member",
        [
            {
                "DimTypeId": t,
                "MemberId": m,
                "Name": n,
                "Alias": n.lower(),
                "Description": f"{n} member",
                "DimId": {0: 1, 2: 2, 5: 3}[t],
            }
            for t, m, n in members
        ],
    )
    db["Relationship"] = _rows(
        "Relationship",
        [
            {
                "DimTypeId": 0,
                "ParentId": 100,
                "ChildId": c,
                "DimId": 1,
                "SiblingSortOrder": i,
                "UDAggWeight": 1.0,
            }
            for i, c in enumerate([101, 102, 103])
        ]
        + [
            {
                "DimTypeId": 5,
                "ParentId": 300,
                "ChildId": 301,
                "DimId": 3,
                "SiblingSortOrder": 0,
                "UDAggWeight": -1.0,
            },
        ],
    )
    db["MemberProperty"] = _rows(
        "MemberProperty",
        [
            {
                "DimTypeId": 0,
                "MemberId": m,
                "PropertyId": 10,
                "VaryByCubeTypeId": -1,
                "VaryByScenarioTypeId": -1,
                "VaryByTimeId": -1,
                "TextValue": "USD",
            }
            for m in (101, 102, 103)
        ],
    )
    db["Cube"] = _rows(
        "Cube",
        [
            {
                "CubeId": 0,
                "Name": "SimFinance",
                "Description": "Simulated cube",
                "IsTopLevelCube": True,
                "DefaultCurrencyId": 176,
            }
        ],
    )
    db["CubeDim"] = _rows(
        "CubeDim",
        [
            {"CubeId": 0, "DimTypeId": t, "ScenarioTypeId": -1, "DimId": d}
            for t, d in ((0, 1), (2, 2), (5, 3))
        ],
    )
    db["FxRateType"] = _rows(
        "FxRateType",
        [
            {"UniqueID": GUID.format(1), "Name": "AverageRate"},
            {"UniqueID": GUID.format(2), "Name": "ClosingRate"},
        ],
    )
    db["FxRate"] = _rows(
        "FxRate",
        [
            {
                "FxRateTypeID": GUID.format(t),
                "TimeId": 2026000000 + p * 1000,
                "SourceCurrencyId": 61,
                "DestCurrencyId": 176,
                "Amount": round(1.05 + 0.01 * p + 0.1 * t, 4),
            }
            for t in (1, 2)
            for p in (3, 4, 5)
        ],
    )

    # Data units: 3 entities (each in its own PartitionId) x 2 scenarios x 2 years.
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    stamps, records = [], {2025: [], 2026: []}
    partition_of = {101: 7, 102: 8, 103: 9}
    k = 0
    for year in (2025, 2026):
        for entity in (101, 102, 103):
            for scenario in (200, 201):
                k += 1
                ticks = datetime_to_ticks(base + timedelta(days=7 * k))
                stamps.append(
                    {
                        "PartitionId": 0,
                        "ScenarioId": scenario,
                        "YearId": year,
                        "CubeId": 0,
                        "EntityId": entity,
                        "ParentId": -1,
                        "ConsId": 176,
                        "TimestampTicks": ticks,
                        "IsInCellUpdateLoop": False,
                    }
                )
                for a_i, account in enumerate((300, 301, 302)):
                    row = {
                        "PartitionId": partition_of[entity],
                        "CubeId": 0,
                        "EntityId": entity,
                        "ParentId": -1,
                        "ConsId": 176,
                        "ScenarioId": scenario,
                        "YearId": year,
                        "AccountId": account,
                        "FlowId": 16777219,
                        "OriginId": -30,
                        "ICId": -999,
                        "UD1Id": 9000 + a_i,
                        "UD2Id": -999,
                        "UD3Id": -999,
                        "UD4Id": -999,
                        "UD5Id": -999,
                        "UD6Id": -999,
                        "UD7Id": -999,
                        "UD8Id": -999,
                        "UD34Id": -1,
                        "UD56Id": -1,
                        "UD78Id": -1,
                    }
                    for m in range(1, 13):
                        value = (
                            0.0
                            if (m == 12 and a_i == 2)
                            else round(1000 * (a_i + 1) + 10.5 * m + k, 2)
                        )
                        row[f"M{m}Value"] = value
                        row[f"M{m}Status"] = 16 if value == 0 else 65
                    records[year].append(
                        {c: row[c] for c in [*DATA_RECORD_SELECT, "UD34Id", "UD56Id", "UD78Id"]}
                    )
    # A data unit stamped far in the future exercises the connector's init-time cap.
    stamps.append(
        {
            "PartitionId": 0,
            "ScenarioId": 200,
            "YearId": 2026,
            "CubeId": 0,
            "EntityId": 101,
            "ParentId": 100,
            "ConsId": -12,
            "TimestampTicks": datetime_to_ticks(datetime(2099, 1, 1, tzinfo=timezone.utc)),
            "IsInCellUpdateLoop": False,
        }
    )
    db["DataUnitCacheTimestamp"] = stamps
    # CalcStatus holds one row per calculated data unit and period. The connector
    # only reads its distinct TimeIds to confirm the standard monthly time profile.
    db["CalcStatus"] = [
        {
            "PartitionId": partition_of[e],
            "CubeId": 0,
            "EntityId": e,
            "ParentId": -1,
            "ConsId": 176,
            "ScenarioId": s_,
            "TimeId": time_id(y, m),
            "TimeModified": 599266080000000000,
        }
        for y in (2025, 2026)
        for e in (101, 102, 103)
        for s_ in (200, 201)
        for m in range(1, 13)
    ]
    db["DataRecord2025"] = records[2025]
    db["DataRecord2026"] = records[2026]
    return db


if __name__ == "__main__":
    out = Path(__file__).parent / "corpus" / "onestream_db.json"
    out.write_text(json.dumps(build(), indent=1) + "\n", encoding="utf-8")
    print(f"wrote {out}")
