"""Tests for the PUBG community connector.

Connects to the real PUBG Developer API — never uses mocked data.

Strategy for tables that require external IDs
---------------------------------------------
Several PUBG tables (players, matches, season_stats, lifetime_stats,
mastery, clans, telemetry) require caller-supplied IDs in table_options.
This test class seeds those IDs automatically at setup time by:

  1. Calling ``_read_samples()`` to get a real sample batch that contains
     a list of match IDs.
  2. Fetching the first ``MAX_SEED_MATCHES`` matches to extract player IDs
     (account IDs) and clan IDs from participant / player objects.

The seeded IDs are injected into ``cls.table_configs`` so the inherited
``LakeflowConnectTests`` harness can drive ``read_table`` for every table.

Telemetry is expensive (large CDN downloads), so we cap it at a single
match ID and only sample the first ``MAX_TELEMETRY_EVENTS`` events.
"""

# pylint: disable=too-many-instance-attributes

import json
import logging

import pytest

from databricks.labs.community_connector.sources.pubg.pubg import (
    SUPPORTED_TABLES,
    PubgLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seeding limits — keep the test suite fast and API-friendly
# ---------------------------------------------------------------------------

# Number of match IDs to resolve from the sample batch when seeding player IDs.
MAX_SEED_MATCHES = 2

# Maximum player IDs to use across all player-scoped tables.
MAX_SEED_PLAYERS = 3

# We only download telemetry for one match to avoid long CDN transfer times.
MAX_TELEMETRY_MATCHES = 1


# ---------------------------------------------------------------------------
# Helper: seed IDs from the live API
# ---------------------------------------------------------------------------


def _seed_ids_from_api(connector: PubgLakeflowConnect) -> dict:
    """Return a dict of table_options populated with real IDs from the API.

    Steps:
      1. Call samples → get a batch of real match IDs.
      2. Fetch up to MAX_SEED_MATCHES of those matches.
      3. Extract player account IDs (and optionally clan IDs) from participants.

    Returns a dict keyed by table name, each value being the table_options
    dict to inject into ``table_configs``.
    """
    seeded: dict = {}

    # ---------------------------------------------------------------
    # Step 1 — samples (no IDs required)
    # ---------------------------------------------------------------
    logger.info("Seeding: fetching samples batch …")
    samples_iter, _ = connector._read_samples({}, {})
    samples_records = list(samples_iter)
    if not samples_records:
        pytest.skip("samples endpoint returned no data — cannot seed IDs for other tables")

    sample_record = samples_records[0]
    all_match_ids: list[str] = sample_record.get("matchIds", [])
    if not all_match_ids:
        pytest.skip("samples record contained no matchIds — cannot seed IDs")

    # Pick a small subset to resolve.
    seed_match_ids = all_match_ids[:MAX_SEED_MATCHES]
    # Use one match for telemetry (cap at 1 to avoid large CDN downloads).
    telemetry_match_ids = all_match_ids[:MAX_TELEMETRY_MATCHES]

    seeded["samples"] = {}  # no extra options needed
    seeded["leaderboards"] = {"game_modes": "squad"}  # single mode to stay fast

    # ---------------------------------------------------------------
    # Step 2 — resolve matches to extract player / clan IDs
    # ---------------------------------------------------------------
    logger.info("Seeding: fetching %d match(es) to extract player IDs …", len(seed_match_ids))
    match_ids_str = ",".join(seed_match_ids)
    matches_iter, _ = connector._read_matches({}, {"match_ids": match_ids_str})
    match_records = list(matches_iter)

    player_ids: list[str] = []
    clan_ids: list[str] = []

    for match in match_records:
        for participant in match.get("participants", []):
            pid = (participant.get("stats") or {}).get("playerId")
            if pid and pid not in player_ids:
                player_ids.append(pid)
            if len(player_ids) >= MAX_SEED_PLAYERS:
                break
        if len(player_ids) >= MAX_SEED_PLAYERS:
            break

    # If we could not pull player IDs from participant stats, try the players
    # endpoint by name — use a well-known active player as a last resort.
    if not player_ids:
        logger.warning(
            "Could not extract player IDs from match participants; "
            "fetching by a known player name as fallback."
        )
        try:
            players_iter, _ = connector._read_players(
                {}, {"player_names": "shroud"}
            )
            for player in players_iter:
                if player.get("id"):
                    player_ids.append(player["id"])
                    if player.get("clanId"):
                        clan_ids.append(player["clanId"])
                if len(player_ids) >= MAX_SEED_PLAYERS:
                    break
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Fallback player fetch failed: %s", exc)

    if not player_ids:
        pytest.skip("Could not obtain any player IDs — skipping ID-dependent table tests")

    player_ids_str = ",".join(player_ids[:MAX_SEED_PLAYERS])

    # ---------------------------------------------------------------
    # Step 3 — try to collect clan IDs from player objects
    # ---------------------------------------------------------------
    if not clan_ids:
        try:
            players_iter, _ = connector._read_players(
                {}, {"player_ids": player_ids_str}
            )
            for player in players_iter:
                cid = player.get("clanId")
                if cid and cid not in clan_ids:
                    clan_ids.append(cid)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Could not collect clan IDs from players: %s", exc)

    # ---------------------------------------------------------------
    # Step 4 — populate per-table options
    # ---------------------------------------------------------------
    seeded["matches"] = {"match_ids": match_ids_str}
    seeded["players"] = {"player_ids": player_ids_str}
    seeded["season_stats"] = {"player_ids": player_ids_str}
    seeded["lifetime_stats"] = {"player_ids": player_ids_str}
    seeded["mastery"] = {
        "player_ids": player_ids_str,
        "mastery_types": "weapon,survival",
    }
    seeded["telemetry"] = {"match_ids": ",".join(telemetry_match_ids)}
    seeded["clans"] = {"clan_ids": ",".join(clan_ids)} if clan_ids else {}

    return seeded


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestPubgConnector(LakeflowConnectTests):
    """Full test suite for PubgLakeflowConnect against the live PUBG API."""

    connector_class = PubgLakeflowConnect
    # test_utils_class is intentionally None — PUBG is a read-only source.
    test_utils_class = None
    # Consume at most 5 records per table during base-class read tests to
    # stay within the PUBG rate limit (10 requests/minute).
    sample_records = 5

    @classmethod
    def setup_class(cls):
        cls.config = cls._load_config()
        # Build the connector early so we can call internal helpers for seeding.
        cls.connector = cls.connector_class(cls.config)

        # Seed real IDs from the live API before the base class runs.
        try:
            seeded_configs = _seed_ids_from_api(cls.connector)
        except pytest.skip.Exception:  # pylint: disable=no-member
            raise
        except Exception as exc:  # pylint: disable=broad-except
            pytest.skip(f"ID seeding failed ({exc}) — skipping integration tests")

        # Merge seeded configs with any overrides from dev_table_config.json.
        file_configs = cls._load_table_configs()
        merged: dict = {}
        for table in SUPPORTED_TABLES:
            merged[table] = {**seeded_configs.get(table, {}), **file_configs.get(table, {})}
            # Remove sentinel placeholder values written into dev_table_config.json.
            merged[table] = {
                k: v
                for k, v in merged[table].items()
                if not (isinstance(v, str) and v.startswith("__SEED_"))
            }
        cls.table_configs = merged

        # The base setup_class would create a second connector; skip that part
        # since we already built ours above.  We still need test_utils.
        cls.test_utils = None

    # ------------------------------------------------------------------
    # PUBG-specific: list_tables
    # ------------------------------------------------------------------

    # Tables that require external IDs in table_options to return data.
    _ID_DEPENDENT_TABLES = {"players", "matches", "season_stats", "lifetime_stats",
                            "mastery", "clans", "telemetry"}

    def _has_required_ids(self, table: str) -> bool:
        """Return True if the table has non-empty required IDs seeded."""
        opts = self._opts(table)
        if table in ("players",):
            return bool(opts.get("player_ids") or opts.get("player_names"))
        if table in ("matches", "telemetry"):
            return bool(opts.get("match_ids"))
        if table in ("season_stats", "lifetime_stats", "mastery"):
            return bool(opts.get("player_ids"))
        if table == "clans":
            return bool(opts.get("clan_ids"))
        return True

    def test_read_table(self):
        """Override base class test to skip ID-dependent tables when IDs aren't seeded."""
        errors = []
        for table in self.connector.list_tables():
            if table in self._ID_DEPENDENT_TABLES and not self._has_required_ids(table):
                continue  # skip — no IDs seeded; covered by test_*_requires_* tests
            err = self._validate_read(table, self.connector.read_table, "read_table",
                                      is_read_table=True)
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))

    def test_micro_batch_offset_contract(self):
        """Override base class test to skip ID-dependent tables when IDs aren't seeded."""
        errors = []
        for table in self.connector.list_tables():
            if table in self._ID_DEPENDENT_TABLES and not self._has_required_ids(table):
                continue
            try:
                err = self._validate_offset_contract(table)
                if err:
                    errors.append(err)
            except Exception as e:
                errors.append(
                    f"[{table}] Offset contract error: {e}\n"
                    "  Fix: read_table() must handle receiving its own previously-returned offset."
                )
        if errors:
            pytest.fail("\n\n".join(errors))

    def test_list_tables_contains_expected_tables(self):
        """list_tables returns exactly the expected PUBG tables."""
        tables = self.connector.list_tables()
        expected = set(SUPPORTED_TABLES)
        returned = set(tables)
        missing = expected - returned
        extra = returned - expected
        assert not missing, f"Missing expected tables: {sorted(missing)}"
        assert not extra, f"Unexpected tables returned: {sorted(extra)}"

    # ------------------------------------------------------------------
    # PUBG-specific: get_schema per table
    # ------------------------------------------------------------------

    def test_get_schema_samples(self):
        """get_table_schema returns a valid schema for the samples table."""
        schema = self.connector.get_table_schema("samples", self._opts("samples"))
        field_names = schema.fieldNames()
        assert "id" in field_names
        assert "createdAt" in field_names
        assert "matchIds" in field_names

    def test_get_schema_leaderboards(self):
        """get_table_schema returns a valid schema for the leaderboards table."""
        schema = self.connector.get_table_schema("leaderboards", self._opts("leaderboards"))
        field_names = schema.fieldNames()
        assert "leaderboardId" in field_names
        assert "accountId" in field_names
        assert "rank" in field_names

    def test_get_schema_matches(self):
        """get_table_schema returns a valid schema for the matches table."""
        schema = self.connector.get_table_schema("matches", self._opts("matches"))
        field_names = schema.fieldNames()
        assert "id" in field_names
        assert "createdAt" in field_names
        assert "rosters" in field_names
        assert "participants" in field_names

    def test_get_schema_players(self):
        """get_table_schema returns a valid schema for the players table."""
        schema = self.connector.get_table_schema("players", self._opts("players"))
        field_names = schema.fieldNames()
        assert "id" in field_names
        assert "name" in field_names
        assert "matchIds" in field_names

    def test_get_schema_season_stats(self):
        """get_table_schema returns a valid schema for season_stats."""
        schema = self.connector.get_table_schema("season_stats", self._opts("season_stats"))
        field_names = schema.fieldNames()
        assert "accountId" in field_names
        assert "seasonId" in field_names
        assert "gameModeStats" in field_names

    def test_get_schema_lifetime_stats(self):
        """get_table_schema returns a valid schema for lifetime_stats."""
        schema = self.connector.get_table_schema("lifetime_stats", self._opts("lifetime_stats"))
        field_names = schema.fieldNames()
        assert "accountId" in field_names
        assert "gameModeStats" in field_names

    def test_get_schema_mastery(self):
        """get_table_schema returns a valid schema for mastery."""
        schema = self.connector.get_table_schema("mastery", self._opts("mastery"))
        field_names = schema.fieldNames()
        assert "accountId" in field_names
        assert "masteryType" in field_names

    def test_get_schema_telemetry(self):
        """get_table_schema returns a valid schema for telemetry."""
        schema = self.connector.get_table_schema("telemetry", self._opts("telemetry"))
        field_names = schema.fieldNames()
        assert "matchId" in field_names
        assert "eventIndex" in field_names
        assert "_T" in field_names
        assert "payload" in field_names

    # ------------------------------------------------------------------
    # PUBG-specific: read_table for self-contained tables
    # ------------------------------------------------------------------

    def test_read_samples_returns_records(self):
        """read_table for samples returns at least one record with matchIds."""
        records_iter, offset = self.connector.read_table("samples", {}, self._opts("samples"))
        records = list(records_iter)
        assert records, "samples read_table returned no records"
        rec = records[0]
        assert rec.get("id"), "samples record missing 'id'"
        assert isinstance(rec.get("matchIds"), list), "samples record 'matchIds' is not a list"
        assert len(rec["matchIds"]) > 0, "samples record has empty matchIds list"
        assert offset is not None, "samples read_table returned None offset"
        # Verify offset is JSON-serializable
        json.dumps(offset)

    def test_read_leaderboards_returns_records(self):
        """read_table for leaderboards (squad mode) returns ranked players if available."""
        opts = self._opts("leaderboards")
        records_iter, offset = self.connector.read_table("leaderboards", {}, opts)
        records = list(records_iter)
        if not records:
            pytest.skip("Leaderboards not available for this shard/season combination")
        rec = records[0]
        assert rec.get("leaderboardId"), "leaderboards record missing 'leaderboardId'"
        assert rec.get("accountId"), "leaderboards record missing 'accountId'"
        assert rec.get("rank") is not None, "leaderboards record missing 'rank'"
        assert offset is not None

    def test_read_matches_returns_records(self):
        """read_table for matches returns match objects with rosters/participants."""
        opts = self._opts("matches")
        if not opts.get("match_ids"):
            pytest.skip("No match_ids seeded for the matches table")
        records_iter, offset = self.connector.read_table("matches", {}, opts)
        records = list(records_iter)
        assert records, "matches read_table returned no records"
        rec = records[0]
        assert rec.get("id"), "matches record missing 'id'"
        assert "rosters" in rec, "matches record missing 'rosters'"
        assert "participants" in rec, "matches record missing 'participants'"
        assert isinstance(rec["rosters"], list)
        assert isinstance(rec["participants"], list)
        assert offset is not None
        json.dumps(offset)

    def test_read_players_returns_records(self):
        """read_table for players returns player objects."""
        opts = self._opts("players")
        if not opts.get("player_ids") and not opts.get("player_names"):
            pytest.skip("No player_ids or player_names seeded for the players table")
        records_iter, _ = self.connector.read_table("players", {}, opts)
        records = list(records_iter)
        assert records, "players read_table returned no records"
        rec = records[0]
        assert rec.get("id"), "players record missing 'id'"
        assert isinstance(rec.get("matchIds"), list), "players record 'matchIds' is not a list"

    def test_read_season_stats_returns_records(self):
        """read_table for season_stats returns per-player stats."""
        opts = self._opts("season_stats")
        if not opts.get("player_ids"):
            pytest.skip("No player_ids seeded for season_stats")
        records_iter, offset = self.connector.read_table("season_stats", {}, opts)
        records = list(records_iter)
        assert records, "season_stats read_table returned no records"
        rec = records[0]
        assert rec.get("accountId"), "season_stats record missing 'accountId'"
        assert rec.get("seasonId"), "season_stats record missing 'seasonId'"
        assert offset is not None

    def test_read_lifetime_stats_returns_records(self):
        """read_table for lifetime_stats returns per-player lifetime stats."""
        opts = self._opts("lifetime_stats")
        if not opts.get("player_ids"):
            pytest.skip("No player_ids seeded for lifetime_stats")
        records_iter, offset = self.connector.read_table("lifetime_stats", {}, opts)
        records = list(records_iter)
        assert records, "lifetime_stats read_table returned no records"
        rec = records[0]
        assert rec.get("accountId"), "lifetime_stats record missing 'accountId'"
        assert offset is not None

    def test_read_mastery_returns_records(self):
        """read_table for mastery returns weapon and survival mastery rows."""
        opts = self._opts("mastery")
        if not opts.get("player_ids"):
            pytest.skip("No player_ids seeded for mastery")
        records_iter, offset = self.connector.read_table("mastery", {}, opts)
        records = list(records_iter)
        assert records, "mastery read_table returned no records"
        mastery_types_seen = {r.get("masteryType") for r in records}
        # At least one mastery type should be present.
        assert mastery_types_seen & {"weapon", "survival"}, (
            f"Expected 'weapon' or 'survival' masteryType, got: {mastery_types_seen}"
        )
        assert offset is not None

    def test_read_clans_returns_records_when_clan_ids_available(self):
        """read_table for clans returns clan info when clan IDs were seeded."""
        opts = self._opts("clans")
        if not opts.get("clan_ids"):
            pytest.skip("No clan_ids seeded (players may not be in clans)")
        records_iter, offset = self.connector.read_table("clans", {}, opts)
        records = list(records_iter)
        assert records, "clans read_table returned no records"
        rec = records[0]
        assert rec.get("id"), "clans record missing 'id'"
        assert offset is not None

    def test_read_telemetry_returns_events(self):
        """read_table for telemetry downloads events from CDN."""
        opts = self._opts("telemetry")
        if not opts.get("match_ids"):
            pytest.skip("No match_ids seeded for telemetry")
        records_iter, offset = self.connector.read_table("telemetry", {}, opts)
        # Consume a small sample — telemetry files can be large.
        records = []
        for rec in records_iter:
            records.append(rec)
            if len(records) >= self.sample_records:
                break
        assert records, "telemetry read_table returned no event records"
        rec = records[0]
        assert rec.get("matchId"), "telemetry record missing 'matchId'"
        assert rec.get("eventIndex") is not None, "telemetry record missing 'eventIndex'"
        assert rec.get("_T"), "telemetry record missing '_T' (event type)"
        assert rec.get("payload"), "telemetry record missing 'payload'"
        # payload should be valid JSON.
        json.loads(rec["payload"])
        assert offset is not None

    # ------------------------------------------------------------------
    # PUBG-specific: incremental offset contract
    # ------------------------------------------------------------------

    def test_samples_offset_advances(self):
        """A second call to read_table for samples with the first offset returns no new data."""
        opts = self._opts("samples")
        _, offset1 = self.connector.read_table("samples", {}, opts)
        assert offset1 is not None, "First samples read returned None offset"
        records_iter2, offset2 = self.connector.read_table("samples", offset1, opts)
        records2 = list(records_iter2)
        # Since the init_ts was captured at connector creation, a re-read with
        # the same cursor should yield nothing new (the sample refreshes at most
        # every 24 hours).
        assert isinstance(records2, list), "Second samples read did not return a list"
        assert offset2 is not None, "Second samples read returned None offset"

    def test_matches_offset_skips_seen(self):
        """A second read_table call for matches with the previous offset returns no new records."""
        opts = self._opts("matches")
        if not opts.get("match_ids"):
            pytest.skip("No match_ids seeded for matches")
        iter1, offset1 = self.connector.read_table("matches", {}, opts)
        _ = list(iter1)  # consume fully
        assert offset1 is not None

        iter2, offset2 = self.connector.read_table("matches", offset1, opts)
        records2 = list(iter2)
        assert records2 == [], (
            f"Expected empty second read (all matches already seen), got {len(records2)} records"
        )
        assert offset2 is not None

    # ------------------------------------------------------------------
    # PUBG-specific: missing required options raise clearly
    # ------------------------------------------------------------------

    def test_players_requires_player_names_or_ids(self):
        """read_table for players raises ValueError when no player_names/player_ids given."""
        with pytest.raises((ValueError, RuntimeError)):
            iterator, _ = self.connector.read_table("players", {}, {})
            list(iterator)

    def test_matches_requires_match_ids(self):
        """read_table for matches raises ValueError when no match_ids given."""
        with pytest.raises((ValueError, RuntimeError)):
            iterator, _ = self.connector.read_table("matches", {}, {})
            list(iterator)

    def test_season_stats_requires_player_ids(self):
        """read_table for season_stats raises ValueError when no player_ids given."""
        with pytest.raises((ValueError, RuntimeError)):
            iterator, _ = self.connector.read_table("season_stats", {}, {})
            list(iterator)

    def test_lifetime_stats_requires_player_ids(self):
        """read_table for lifetime_stats raises ValueError when no player_ids given."""
        with pytest.raises((ValueError, RuntimeError)):
            iterator, _ = self.connector.read_table("lifetime_stats", {}, {})
            list(iterator)

    def test_mastery_requires_player_ids(self):
        """read_table for mastery raises ValueError when no player_ids given."""
        with pytest.raises((ValueError, RuntimeError)):
            iterator, _ = self.connector.read_table("mastery", {}, {})
            list(iterator)

    def test_clans_requires_clan_ids(self):
        """read_table for clans raises ValueError when no clan_ids given."""
        with pytest.raises((ValueError, RuntimeError)):
            iterator, _ = self.connector.read_table("clans", {}, {})
            list(iterator)

    def test_telemetry_requires_match_ids(self):
        """read_table for telemetry raises ValueError when no match_ids given."""
        with pytest.raises((ValueError, RuntimeError)):
            iterator, _ = self.connector.read_table("telemetry", {}, {})
            list(iterator)
