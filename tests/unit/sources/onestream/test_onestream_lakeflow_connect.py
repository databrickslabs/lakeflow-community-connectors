"""Tests for the OneStream LakeflowConnect connector.

Simulate mode (default) serves every request from the in-process simulator in
``source_simulator/specs/onestream/``: the handler executes the connector's
generated SQL against a synthetic SQLite copy of the OneStream tables.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from databricks.labs.community_connector.source_simulator.specs.onestream.handlers import (
    sql_command as sim,
)
from databricks.labs.community_connector.sources.onestream import onestream_client
from databricks.labs.community_connector.sources.onestream import onestream_sql as q
from databricks.labs.community_connector.sources.onestream.onestream import (
    OneStreamLakeflowConnect,
    _parse_years,
    _unpivot,
)
from databricks.labs.community_connector.sources.onestream.onestream_client import (
    OneStreamClient,
    OneStreamError,
    _rows,
    normalize_base_url,
)
from databricks.labs.community_connector.sources.onestream.onestream_offsets import (
    cube_offset_advance,
    cube_offset_decode,
    cube_offset_encode,
)
from databricks.labs.community_connector.sources.onestream.onestream_schemas import (
    datetime_to_ticks,
    ticks_to_iso,
    time_id,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests

SEC = 10_000_000  # .NET ticks per second
EAST, WEST, NORTH = 101, 102, 103  # synthetic entities (see generate_corpus.py)
ACTUAL, BUDGET = 200, 201


@pytest.fixture
def sim_db():
    """A fresh simulated OneStream DB that a test may mutate; reset afterwards."""
    sim.reset_database()
    yield sim
    sim.reset_database()


def _make_recent(column, values, ticks):
    """Restamp the already-in-scope data units as recent, so anything added to the
    scope later is *older* than the cursor -- the case a single cursor gets wrong."""
    marks = ", ".join("?" for _ in values)
    sim.execute(
        "UPDATE dbo.DataUnitCacheTimestamp SET TimestampTicks = ? "
        f"WHERE ParentId = -1 AND {column} IN ({marks})",
        (ticks, *values),
    )


def _clone(table, where, params=(), **overrides):
    """Copy rows of a simulated table, overriding some columns (e.g. a new cube)."""
    cols = [r[1] for r in sim._connection().execute(f"PRAGMA dbo.table_info([{table}])")]
    select = ", ".join(f"? AS [{c}]" if c in overrides else f"[{c}]" for c in cols)
    values = tuple(overrides[c] for c in cols if c in overrides)
    sim.execute(
        f"INSERT INTO dbo.[{table}] SELECT {select} FROM dbo.[{table}] WHERE {where}",
        values + tuple(params),
    )


def _restamp(year, scenario, entity, ticks, in_flight=False):
    sim.execute(
        "UPDATE dbo.DataUnitCacheTimestamp SET TimestampTicks = ?, IsInCellUpdateLoop = ? "
        "WHERE YearId = ? AND ScenarioId = ? AND EntityId = ? AND ParentId = -1",
        (ticks, 1 if in_flight else 0, year, scenario, entity),
    )


class TestOneStreamConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = OneStreamLakeflowConnect
    simulator_source = "onestream"
    sample_records = 500
    replay_config = {
        "base_url": "https://simulated.onestreamcloud.com",
        "application_name": "SimApp",
        "personal_access_token": "simulator-fake-pat",
    }

    def test_cube_data_incremental_by_data_unit(self):
        opts = dict(self._opts("cube_data"), max_data_units_per_batch="1000")
        records, offset = self.connector.read_table("cube_data", {}, opts)
        records = list(records)
        # 2 years x 3 entities x 2 scenarios x 3 accounts x 12 months; the
        # future-stamped data unit is excluded by the init-time cap.
        assert len(records) == 432
        assert offset["fmt"] == 2 and "c|2026|200|0" in offset
        again, offset2 = self.connector.read_table("cube_data", offset, opts)
        assert list(again) == [] and offset2 == offset

    # -- helpers -----------------------------------------------------------

    def _connector(self, now=None):
        conn = OneStreamLakeflowConnect(dict(self.replay_config))
        if now is not None:
            conn._init_ticks = now
        return conn

    @staticmethod
    def _drain(connector, opts, offset=None, max_iter=60):
        """Run read_table until the offset stops changing (= a terminated trigger)."""
        records, offset = [], offset or {}
        for _ in range(max_iter):
            batch, nxt = connector.read_table("cube_data", offset, opts)
            records.extend(batch)
            if nxt == offset:
                return records, offset
            offset = nxt
        pytest.fail(f"cube_data did not converge within {max_iter} read_table calls")

    def _base_opts(self, **extra):
        return {"years": "2025,2026", "max_data_units_per_batch": "2", "page_size": "2", **extra}

    # -- scope widening ----------------------------------------------------

    def test_widen_years_backfills_only_new_year(self, sim_db):
        _make_recent("YearId", [2025], self.connector._init_ticks - 3600 * SEC)
        _, offset = self._drain(self._connector(), self._base_opts(years="2025"))
        added, _ = self._drain(self._connector(), self._base_opts(years="2025,2026"), offset)
        assert len(added) == 216 and {r["YearId"] for r in added} == {2026}

    def test_widen_scenarios_backfills_only_new_scenario(self, sim_db):
        _make_recent("ScenarioId", [ACTUAL], self.connector._init_ticks - 3600 * SEC)
        _, offset = self._drain(self._connector(), self._base_opts(scenarios="Actual"))
        added, _ = self._drain(
            self._connector(), self._base_opts(scenarios="Actual,Budget"), offset
        )
        assert len(added) == 216 and {r["ScenarioId"] for r in added} == {BUDGET}

    def test_widen_entities_catches_up_only_new_entity(self, sim_db):
        _make_recent("EntityId", [EAST], self.connector._init_ticks - 3600 * SEC)
        _, offset = self._drain(self._connector(), self._base_opts(entities="SimEast"))
        added, final = self._drain(
            self._connector(), self._base_opts(entities="SimEast,SimWest"), offset
        )
        assert len(added) == 144 and {r["EntityId"] for r in added} == {WEST}
        assert "x" not in final  # catch-up finished and was cleared

    def test_hierarchy_change_catches_up_new_descendant(self, sim_db):
        opts = self._base_opts(entities="SimCorp", include_descendants="true")
        sim.execute(
            "DELETE FROM dbo.Relationship WHERE DimTypeId = 0 AND ParentId = 100 AND ChildId = ?",
            (NORTH,),
        )
        _make_recent("EntityId", [EAST, WEST], self.connector._init_ticks - 3600 * SEC)
        first, offset = self._drain(self._connector(), opts)
        assert {r["EntityId"] for r in first} == {EAST, WEST}
        # Finance re-parents SimNorth under SimCorp; its history is older than the cursor.
        sim.execute(
            "INSERT INTO dbo.Relationship (DimTypeId, ParentId, ChildId, DimId, SiblingSortOrder, "
            "UDAggWeight) VALUES (0, 100, ?, 1, 2, 1.0)",
            (NORTH,),
        )
        added, _ = self._drain(self._connector(), opts, offset)
        assert len(added) == 144 and {r["EntityId"] for r in added} == {NORTH}

    # -- portability: cubes, partitions, versions, profiles ------------------

    def test_widen_cube_backfills_new_cube(self, sim_db):
        _clone("Cube", "CubeId = 0", CubeId=1, Name="SimOpex")
        east_actual = "EntityId = 101 AND ScenarioId = 200"
        _clone(
            "DataUnitCacheTimestamp", f"{east_actual} AND YearId = 2026 AND ParentId = -1", CubeId=1
        )
        _clone("DataRecord2026", east_actual, CubeId=1)
        _make_recent("CubeId", [0], self.connector._init_ticks - 3600 * SEC)
        _, offset = self._drain(self._connector(), self._base_opts(years="2026", cube="SimFinance"))
        added, _ = self._drain(self._connector(), self._base_opts(years="2026"), offset)
        assert len(added) == 36 and {r["CubeId"] for r in added} == {1}

    def test_entity_in_two_partitions_is_read_fully(self, sim_db):
        _clone(
            "DataRecord2026",
            "EntityId = 101 AND ScenarioId = 200 AND AccountId = 300",
            PartitionId=99,
            AccountId=303,
        )
        records, _ = self._drain(
            self._connector(), self._base_opts(years="2026", scenarios="Actual", entities="SimEast")
        )
        assert len(records) == 48 and 303 in {r["AccountId"] for r in records}

    def test_server_row_cap_fails_loudly(self, sim_db):
        sim.set_row_cap(30)
        with pytest.raises(OneStreamError, match="only 30 rows"):
            self._connector().read_table("members", {}, {"page_size": "40"})

    def test_missing_optional_column_is_tolerated(self, sim_db):
        sim.drop_column("Member", "Alias")
        conn = self._connector()
        assert "Alias" not in conn.get_table_schema("members", {}).fieldNames()
        rows = list(conn.read_table("members", {}, {})[0])
        assert rows and all("Alias" not in r for r in rows)

    def test_missing_key_column_fails_loudly(self, sim_db):
        sim.drop_column("Member", "MemberId")
        with pytest.raises(ValueError, match="missing key column"):
            self._connector().read_table("members", {}, {})

    def test_non_standard_time_profile_fails_loudly(self, sim_db):
        _clone(
            "CalcStatus",
            "TimeId = 2026003000 AND EntityId = 101 AND ScenarioId = 200",
            TimeId=2026006000,
        )
        with pytest.raises(ValueError, match="standard monthly"):
            self._connector().read_table("cube_data", {}, self._base_opts(years="2026"))

    def test_ambiguous_member_name_fails(self, sim_db):
        sim.execute(
            "INSERT INTO dbo.[Member] (DimTypeId, MemberId, Name, DimId) "
            "VALUES (2, 299, 'Actual', 9)"
        )
        with pytest.raises(ValueError, match="more than one"):
            self._connector().read_table("cube_data", {}, self._base_opts(scenarios="Actual"))

    def test_invalid_option_values_fail(self):
        with pytest.raises(ValueError, match="refresh_mode"):
            self.connector.read_table("cube_data", {}, self._base_opts(refresh_mode="snapshots"))
        with pytest.raises(ValueError, match="include_descendants"):
            self.connector.read_table(
                "cube_data", {}, self._base_opts(entities="SimCorp", include_descendants="ture")
            )

    # -- confirmation re-read (lookback) -------------------------------------

    def _late_commit(self, **extra):
        now = self.connector._init_ticks
        opts = self._base_opts(years="2026", scenarios="Actual", **extra)
        _restamp(2026, ACTUAL, EAST, now - 60 * SEC)  # a change just before the run
        _, offset = self._drain(self._connector(now), opts)
        # A change stamped *earlier* than the one already read becomes visible late.
        _restamp(2026, ACTUAL, WEST, now - 120 * SEC)
        later, offset2 = self._drain(self._connector(now + 1000 * SEC), opts, offset)
        settled, _ = self._drain(self._connector(now + 2000 * SEC), opts, offset2)
        return now, later, settled

    def test_late_commit_caught_by_confirmation_reread(self, sim_db):
        now, later, settled = self._late_commit()
        west = [r for r in later if r["EntityId"] == WEST]
        assert len(west) == 36 and {r["_du_timestamp_ticks"] for r in west} == {now - 120 * SEC}
        assert settled == []  # each change is re-read once, not on every run

    def test_late_commit_missed_without_lookback(self, sim_db):
        _, later, _ = self._late_commit(lookback_seconds="0")
        assert [r for r in later if r["EntityId"] == WEST] == []

    # -- in-flight hold-back --------------------------------------------------

    def _in_flight_first_pass(self):
        now = self.connector._init_ticks
        opts = self._base_opts(years="2026", scenarios="Actual", hold_in_flight_data_units="true")
        _restamp(2026, ACTUAL, EAST, now - 60 * SEC, in_flight=True)
        first, offset = self._drain(self._connector(now), opts)
        assert {r["EntityId"] for r in first} == {WEST, NORTH}  # EAST is held back
        return now, opts, offset

    def test_in_flight_unit_held_until_flag_clears(self, sim_db):
        now, opts, offset = self._in_flight_first_pass()
        still, offset = self._drain(self._connector(now + 10 * SEC), opts, offset)
        assert still == []
        _restamp(2026, ACTUAL, EAST, now - 60 * SEC, in_flight=False)
        released, _ = self._drain(self._connector(now + 20 * SEC), opts, offset)
        assert {r["EntityId"] for r in released} == {EAST} and len(released) == 36

    def test_in_flight_flag_released_after_timeout(self, sim_db):
        now, opts, offset = self._in_flight_first_pass()
        released, _ = self._drain(self._connector(now + 3601 * SEC), opts, offset)
        assert {r["EntityId"] for r in released} == {EAST}

    def test_in_flight_ignored_when_hold_disabled(self, sim_db):
        now = self.connector._init_ticks
        _restamp(2026, ACTUAL, EAST, now - 60 * SEC, in_flight=True)
        records, _ = self._drain(
            self._connector(now), self._base_opts(years="2026", scenarios="Actual")
        )
        assert EAST in {r["EntityId"] for r in records}

    def test_cube_data_scoped_by_names(self):
        opts = dict(
            self._opts("cube_data"),
            years="2026",
            scenarios="Actual",
            entities="SimCorp",
            include_descendants="true",
            max_data_units_per_batch="1000",
        )
        records = list(self.connector.read_table("cube_data", {}, opts)[0])
        assert {r["EntityId"] for r in records} == {101, 102, 103}
        assert {r["ScenarioId"] for r in records} == {200}
        assert {r["YearId"] for r in records} == {2026}
        assert all(isinstance(r["Amount"], Decimal) for r in records)

    def test_cube_data_unknown_member_rejected(self):
        opts = dict(self._opts("cube_data"), scenarios="NoSuchScenario")
        with pytest.raises(ValueError, match="unknown member"):
            self.connector.read_table("cube_data", {}, opts)

    def test_cube_data_requires_years(self):
        with pytest.raises(ValueError, match="years"):
            self.connector.read_table("cube_data", {}, {})

    def test_cube_data_snapshot_mode(self):
        opts = dict(self._opts("cube_data"), refresh_mode="snapshot")
        assert self.connector.read_table_metadata("cube_data", opts)["ingestion_type"] == "snapshot"
        records, offset = self.connector.read_table("cube_data", {}, opts)
        assert offset is None and len(list(records)) == 432


# ---------------------------------------------------------------------------
# Pure unit tests
# ---------------------------------------------------------------------------


def test_ident_rejects_injection():
    assert q.sql_ident("DataRecord2026") == "[DataRecord2026]"
    for bad in ["x]; DROP TABLE Member;--", "a b", "", "1abc", "Member;"]:
        with pytest.raises(ValueError):
            q.sql_ident(bad)


def test_int_literals_only():
    assert q.sql_int_list([1, -999, 12345678]) == "1, -999, 12345678"
    for bad in ["1", 1.5, True, None, "1 OR 1=1"]:
        with pytest.raises(ValueError):
            q.sql_int_lit(bad)
    with pytest.raises(ValueError):
        q.sql_int_list([])


def test_data_record_table_bounds():
    assert q.sql_data_record_table(2026) == "[dbo].[DataRecord2026]"
    with pytest.raises(ValueError):
        q.sql_data_record_table(1800)


def test_select_paging_requires_order():
    sql = q.sql_select(
        ["A", "B"], q.sql_table_ref("Member"), q.sql_eq("DimTypeId", 0), ["A"], offset=10, limit=5
    )
    assert sql == (
        "SELECT [A], [B] FROM [dbo].[Member] WHERE [DimTypeId] = 0 ORDER BY [A] "
        "OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY"
    )
    with pytest.raises(ValueError):
        q.sql_select(["A"], q.sql_table_ref("Member"), limit=5)


def test_pair_filter():
    assert (
        q.sql_pair_filter("ParentId", "ConsId", [[-1, 176]])
        == "(([ParentId] = -1 AND [ConsId] = 176))"
    )


def test_time_id_encoding():
    assert time_id(2025, 1) == 2025003000
    assert time_id(2025, 6) == 2025009000
    assert time_id(2025, 12) == 2025018000


def test_ticks_round_trip():
    dt = datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
    ticks = datetime_to_ticks(dt)
    assert ticks == 638713838456789010
    assert ticks_to_iso(ticks) == dt.isoformat()


def test_parse_years():
    assert _parse_years("2026, 2024-2025") == [2024, 2025, 2026]
    with pytest.raises(ValueError):
        _parse_years("1800")


def test_unpivot_emits_twelve_months_including_zero():
    row = {
        "CubeId": 0,
        "EntityId": 1,
        "ParentId": -1,
        "ConsId": 176,
        "ScenarioId": 2,
        "YearId": 2026,
        "AccountId": 3,
        "FlowId": 4,
        "OriginId": -30,
        "ICId": -999,
        **{f"UD{i}Id": -999 for i in range(1, 9)},
        **{f"M{m}Value": Decimal("0") if m == 12 else Decimal(m) for m in range(1, 13)},
        **{f"M{m}Status": 16 if m == 12 else 65 for m in range(1, 13)},
    }
    out = list(_unpivot(row, 2026, 638713838456789010))
    assert [r["TimeId"] for r in out][:3] == [2026003000, 2026004000, 2026005000]
    assert out[-1]["Amount"] == Decimal("0") and out[-1]["CellStatus"] == 16
    assert out[0]["_du_timestamp_ticks"] == 638713838456789010


def test_legacy_offset_requires_full_refresh():
    with pytest.raises(ValueError, match="full refresh"):
        cube_offset_decode({"ticks": 638713838456789010})


def test_offset_round_trip():
    offset = {
        "fmt": 2,
        "e": "101,102",
        "c|2026|200|0": 50,
        "w|2026|200|0": 40,
        "x": "103",
        "xp|2026|200|0": 10,
        "xm|2026|200|0": 50,
    }
    assert cube_offset_encode(cube_offset_decode(offset)) == offset


def test_out_of_scope_slice_is_left_untouched():
    state = cube_offset_decode({"fmt": 2, "e": "*", "c|2025|200|0": 100, "w|2025|200|0": 50})
    nxt = cube_offset_advance(
        state,
        {},
        {},
        now=10_000,
        lookback=10,
        max_units=10,
        hold=False,
        hold_timeout=0,
        in_scope={(2026, 200, 0)},
    )
    assert nxt["w"][(2025, 200, 0)] == 50  # not advanced without being re-read


def test_unexpected_response_body_is_an_error():
    assert _rows({"Table": []}) == []
    with pytest.raises(OneStreamError):
        _rows({"Message": "Logon succeeded."})


def test_retries_transient_status_then_succeeds(monkeypatch):
    class Resp:
        def __init__(self, status, text):
            self.status_code, self.text = status, text

    replies = iter([Resp(429, "slow down"), Resp(200, '{"Table":[{"one":1}]}')])
    monkeypatch.setattr(onestream_client.requests, "post", lambda *a, **k: next(replies))
    monkeypatch.setattr(onestream_client.time, "sleep", lambda _s: None)
    client = OneStreamClient("https://t.onestreamcloud.com", "App", "pat")
    assert client.run_sql("SELECT 1 AS one") == [{"one": 1}]


def test_decimal_precision_preserved():
    payload = json.loads('{"Table":[{"d":12345678901234567.123456789}]}', parse_float=Decimal)
    assert _rows(payload)[0]["d"] == Decimal("12345678901234567.123456789")


def test_normalize_base_url():
    assert (
        normalize_base_url("https://t.onestreamcloud.com/OneStreamWeb/")
        == "https://t.onestreamcloud.com"
    )
    for bad in ["t.onestreamcloud.com", "http://t.onestreamcloud.com"]:
        with pytest.raises(ValueError):
            normalize_base_url(bad)
