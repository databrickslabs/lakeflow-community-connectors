"""PUBG community connector — ingests data from the PUBG Developer API into Databricks."""

# pylint: disable=too-many-lines

import time
from datetime import datetime, timezone
from typing import Iterator

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import LakeflowConnect

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_TABLES = [
    "players",
    "matches",
    "season_stats",
    "lifetime_stats",
    "leaderboards",
    "mastery",
    "clans",
    "samples",
    "telemetry",
]

# HTTP status codes that warrant a retry.
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Exponential backoff configuration.
INITIAL_BACKOFF = 6  # seconds — conservative start given 10 req/min limit
MAX_RETRIES = 5

# ---------------------------------------------------------------------------
# Static schemas
# ---------------------------------------------------------------------------

# Reusable sub-types
_STATS_FIELDS = [
    StructField("assists", IntegerType()),
    StructField("boosts", IntegerType()),
    StructField("dBNOs", IntegerType()),
    StructField("dailyKills", IntegerType()),
    StructField("dailyWins", IntegerType()),
    StructField("damageDealt", FloatType()),
    StructField("days", IntegerType()),
    StructField("headshotKills", IntegerType()),
    StructField("heals", IntegerType()),
    StructField("killPoints", FloatType()),
    StructField("kills", IntegerType()),
    StructField("longestKill", FloatType()),
    StructField("longestTimeSurvived", FloatType()),
    StructField("losses", IntegerType()),
    StructField("maxKillStreaks", IntegerType()),
    StructField("mostSurvivalTime", FloatType()),
    StructField("rankPoints", FloatType()),
    StructField("rankPointsTitle", StringType()),
    StructField("revives", IntegerType()),
    StructField("rideDistance", FloatType()),
    StructField("roadKills", IntegerType()),
    StructField("roundMostKills", IntegerType()),
    StructField("roundsPlayed", IntegerType()),
    StructField("suicides", IntegerType()),
    StructField("swimDistance", FloatType()),
    StructField("teamKills", IntegerType()),
    StructField("timeSurvived", FloatType()),
    StructField("top10s", IntegerType()),
    StructField("vehicleDestroys", IntegerType()),
    StructField("walkDistance", FloatType()),
    StructField("weaponsAcquired", IntegerType()),
    StructField("weeklyKills", IntegerType()),
    StructField("weeklyWins", IntegerType()),
    StructField("wins", IntegerType()),
]

_GAME_MODE_STATS_TYPE = StructType(_STATS_FIELDS)

_GAME_MODE_STATS_STRUCT = StructType(
    [
        StructField("solo", _GAME_MODE_STATS_TYPE),
        StructField("solo-fpp", _GAME_MODE_STATS_TYPE),
        StructField("duo", _GAME_MODE_STATS_TYPE),
        StructField("duo-fpp", _GAME_MODE_STATS_TYPE),
        StructField("squad", _GAME_MODE_STATS_TYPE),
        StructField("squad-fpp", _GAME_MODE_STATS_TYPE),
    ]
)

_PARTICIPANT_STATS_TYPE = StructType(
    [
        StructField("DBNOs", IntegerType()),
        StructField("assists", IntegerType()),
        StructField("boosts", IntegerType()),
        StructField("damageDealt", FloatType()),
        StructField("deathType", StringType()),
        StructField("headshotKills", IntegerType()),
        StructField("heals", IntegerType()),
        StructField("killPlace", IntegerType()),
        StructField("killStreaks", IntegerType()),
        StructField("kills", IntegerType()),
        StructField("longestKill", FloatType()),
        StructField("name", StringType()),
        StructField("playerId", StringType()),
        StructField("revives", IntegerType()),
        StructField("rideDistance", FloatType()),
        StructField("roadKills", IntegerType()),
        StructField("swimDistance", FloatType()),
        StructField("teamKills", IntegerType()),
        StructField("timeSurvived", FloatType()),
        StructField("vehicleDestroys", IntegerType()),
        StructField("walkDistance", FloatType()),
        StructField("weaponsAcquired", IntegerType()),
        StructField("winPlace", IntegerType()),
    ]
)

_PARTICIPANT_TYPE = StructType(
    [
        StructField("id", StringType()),
        StructField("actor", StringType()),
        StructField("shardId", StringType()),
        StructField("stats", _PARTICIPANT_STATS_TYPE),
    ]
)

_ROSTER_STATS_TYPE = StructType(
    [
        StructField("rank", IntegerType()),
        StructField("teamId", IntegerType()),
    ]
)

_ROSTER_TYPE = StructType(
    [
        StructField("id", StringType()),
        StructField("shardId", StringType()),
        StructField("won", StringType()),
        StructField("stats", _ROSTER_STATS_TYPE),
        StructField("participantIds", ArrayType(StringType())),
    ]
)

_LEADERBOARD_PLAYER_STATS_TYPE = StructType(
    [
        StructField("rankPoints", FloatType()),
        StructField("wins", IntegerType()),
        StructField("games", IntegerType()),
        StructField("winRatio", FloatType()),
        StructField("averageDamage", FloatType()),
        StructField("kills", IntegerType()),
        StructField("killDeathRatio", FloatType()),
        StructField("kda", FloatType()),
        StructField("averageRank", FloatType()),
    ]
)

_WEAPON_STATS_TYPE = StructType(
    [
        StructField("Kills", IntegerType()),
        StructField("HeadShots", IntegerType()),
        StructField("Groggies", IntegerType()),
        StructField("DamagePlayer", FloatType()),
        StructField("LongestKill", FloatType()),
        StructField("MostKillsInAGame", IntegerType()),
    ]
)

_WEAPON_SUMMARY_ENTRY_TYPE = StructType(
    [
        StructField("weaponId", StringType()),
        StructField("XPTotal", IntegerType()),
        StructField("LevelCurrent", IntegerType()),
        StructField("TierCurrent", IntegerType()),
        StructField("StatsTotal", _WEAPON_STATS_TYPE),
        StructField("OfficialStatsTotal", _WEAPON_STATS_TYPE),
        StructField("CompetitiveStatsTotal", _WEAPON_STATS_TYPE),
    ]
)

# Top-level table schemas
TABLE_SCHEMAS: dict[str, StructType] = {
    "players": StructType(
        [
            StructField("id", StringType()),  # accountId — primary key
            StructField("name", StringType()),
            StructField("shardId", StringType()),
            StructField("titleId", StringType()),
            StructField("banType", StringType()),
            StructField("clanId", StringType()),
            StructField("patchVersion", StringType()),
            StructField("matchIds", ArrayType(StringType())),  # recent match IDs
        ]
    ),
    "matches": StructType(
        [
            StructField("id", StringType()),  # match UUID — primary key
            StructField("createdAt", StringType()),
            StructField("duration", IntegerType()),
            StructField("matchType", StringType()),
            StructField("gameMode", StringType()),
            StructField("mapName", StringType()),
            StructField("isCustomMatch", BooleanType()),
            StructField("patchVersion", StringType()),
            StructField("seasonState", StringType()),
            StructField("shardId", StringType()),
            StructField("titleId", StringType()),
            StructField("telemetryUrl", StringType()),
            StructField("rosters", ArrayType(_ROSTER_TYPE)),
            StructField("participants", ArrayType(_PARTICIPANT_TYPE)),
        ]
    ),
    "season_stats": StructType(
        [
            StructField("accountId", StringType()),  # composite PK part 1
            StructField("seasonId", StringType()),  # composite PK part 2
            StructField("gameModeStats", _GAME_MODE_STATS_STRUCT),
            StructField("bestRankPoint", FloatType()),
            StructField(
                "seasonMatchIds",
                StructType(
                    [
                        StructField("solo", ArrayType(StringType())),
                        StructField("solo-fpp", ArrayType(StringType())),
                        StructField("duo", ArrayType(StringType())),
                        StructField("duo-fpp", ArrayType(StringType())),
                        StructField("squad", ArrayType(StringType())),
                        StructField("squad-fpp", ArrayType(StringType())),
                    ]
                ),
            ),
        ]
    ),
    "lifetime_stats": StructType(
        [
            StructField("accountId", StringType()),  # primary key
            StructField("gameModeStats", _GAME_MODE_STATS_STRUCT),
            StructField("bestRankPoint", FloatType()),
        ]
    ),
    "leaderboards": StructType(
        [
            StructField("leaderboardId", StringType()),  # composite PK part 1 (season+mode)
            StructField("accountId", StringType()),  # composite PK part 2
            StructField("seasonId", StringType()),
            StructField("gameMode", StringType()),
            StructField("shardId", StringType()),
            StructField("name", StringType()),
            StructField("rank", IntegerType()),
            StructField("stats", _LEADERBOARD_PLAYER_STATS_TYPE),
        ]
    ),
    "mastery": StructType(
        [
            StructField("accountId", StringType()),  # composite PK part 1
            StructField("masteryType", StringType()),  # "weapon" or "survival" — PK part 2
            # Weapon mastery — a row per player, weapons stored as array
            StructField("weapons", ArrayType(_WEAPON_SUMMARY_ENTRY_TYPE)),
            # Survival mastery fields
            StructField("xp", IntegerType()),
            StructField("level", IntegerType()),
            StructField("tier", IntegerType()),
            StructField("totalMatchesPlayed", IntegerType()),
            StructField(
                "survivalStats",
                StructType(
                    [
                        StructField("airDropsCalled", IntegerType()),
                        StructField("damageDealt", FloatType()),
                        StructField("damageFromBlueZone", FloatType()),
                        StructField("damageFromEnemies", FloatType()),
                        StructField("damageFromExplosions", FloatType()),
                        StructField("distanceByFreefall", FloatType()),
                        StructField("distanceBySwimming", FloatType()),
                        StructField("distanceByVehicle", FloatType()),
                        StructField("distanceOnFoot", FloatType()),
                        StructField("enemiesKilled", IntegerType()),
                        StructField("healed", FloatType()),
                        StructField("objectsDestroyed", IntegerType()),
                        StructField("revived", IntegerType()),
                        StructField("teammateDamageDealt", FloatType()),
                        StructField("throwablesThrown", IntegerType()),
                        StructField("top10Finishes", IntegerType()),
                        StructField("wins", IntegerType()),
                    ]
                ),
            ),
        ]
    ),
    "clans": StructType(
        [
            StructField("id", StringType()),  # clanId — primary key
            StructField("clanName", StringType()),
            StructField("clanTag", StringType()),
            StructField("clanLevel", IntegerType()),
            StructField("clanMemberCount", IntegerType()),
        ]
    ),
    "samples": StructType(
        [
            StructField("id", StringType()),  # sample batch ID — primary key
            StructField("createdAt", StringType()),
            StructField("titleId", StringType()),
            StructField("shardId", StringType()),
            StructField("matchIds", ArrayType(StringType())),
        ]
    ),
    "telemetry": StructType(
        [
            StructField("matchId", StringType()),  # composite PK part 1
            StructField("eventIndex", IntegerType()),  # composite PK part 2 (position in file)
            StructField("_D", StringType()),  # event timestamp
            StructField("_T", StringType()),  # event type
            StructField("common", MapType(StringType(), StringType())),
            StructField("payload", StringType()),  # full event serialized as JSON string
        ]
    ),
}

# Table metadata — primary keys, cursor fields, ingestion types.
# Tables that require external IDs (accountId, matchId, etc.) are snapshot mode
# since we cannot enumerate all records without a seed list. The caller must
# supply the relevant IDs via table_options or a seeding mechanism.
TABLE_METADATA: dict[str, dict] = {
    "players": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "matches": {
        "primary_keys": ["id"],
        "cursor_field": "createdAt",
        "ingestion_type": "append",
    },
    "season_stats": {
        "primary_keys": ["accountId", "seasonId"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "lifetime_stats": {
        "primary_keys": ["accountId"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "leaderboards": {
        "primary_keys": ["leaderboardId", "accountId"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "mastery": {
        "primary_keys": ["accountId", "masteryType"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "clans": {
        "primary_keys": ["id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "samples": {
        "primary_keys": ["id"],
        "cursor_field": "createdAt",
        "ingestion_type": "append",
    },
    "telemetry": {
        "primary_keys": ["matchId", "eventIndex"],
        "cursor_field": "_D",
        "ingestion_type": "append",
    },
}


# ---------------------------------------------------------------------------
# Connector implementation
# ---------------------------------------------------------------------------


class PubgLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the PUBG Developer API.

    Authentication: Bearer token (``api_key``) scoped to a ``shard``.
    Base URL: ``https://api.pubg.com/shards/{shard}/``

    Required options:
        api_key: PUBG JWT obtained from https://developer.pubg.com.
        shard:   Platform shard (steam, psn, xbox, kakao, stadia, console).

    Optional options (passed via table_options at read time):
        player_names:   Comma-separated player display names (players table).
        player_ids:     Comma-separated account IDs (players, season_stats,
                        lifetime_stats, mastery tables).
        match_ids:      Comma-separated match UUIDs (matches, telemetry tables).
        season_id:      Season ID for season_stats and leaderboards.
        game_modes:     Comma-separated game modes for leaderboards
                        (default: solo,solo-fpp,duo,duo-fpp,squad,squad-fpp).
        clan_ids:       Comma-separated clan IDs (clans table).
        mastery_types:  Comma-separated mastery types: weapon,survival
                        (default: weapon,survival).
        created_at_start: ISO 8601 timestamp for the samples filter.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("PUBG connector requires 'api_key' in options")

        shard = options.get("shard")
        if not shard:
            raise ValueError("PUBG connector requires 'shard' in options")

        self._shard = shard
        self._base_url = f"https://api.pubg.com/shards/{shard}"

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/vnd.api+json",
                "Accept-Encoding": "gzip",
            }
        )

        # Capture init time to avoid chasing records created after this run starts.
        self._init_ts = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _request_with_retry(self, url: str, params: dict | None = None) -> requests.Response:
        """GET ``url`` with exponential backoff on rate-limit / server errors.

        Respects the ``X-RateLimit-Reset`` header when present on 429 responses
        so we wait exactly as long as the server requires, rather than sleeping
        a fixed amount.
        """
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(url, params=params)

            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp

            if attempt < MAX_RETRIES - 1:
                if resp.status_code == 429:
                    reset_ts = resp.headers.get("X-RateLimit-Reset")
                    if reset_ts:
                        wait = max(0.0, float(reset_ts) - time.time()) + 1
                    else:
                        wait = backoff
                else:
                    wait = backoff
                time.sleep(wait)
                backoff = min(backoff * 2, 60)

        return resp  # return the last response even if still an error

    def _api_get(self, path: str, params: dict | None = None) -> requests.Response:
        """Issue a rate-limited GET against the PUBG API (shard-relative path)."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._request_with_retry(url, params=params)

    def _cdn_get(self, url: str) -> requests.Response:
        """Download telemetry from the PUBG CDN (no auth header required, no rate limit)."""
        # Use a plain session without the Authorization header for CDN calls.
        return self._request_with_retry(url)

    def _get_current_season_id(self) -> str:
        """Fetch the list of seasons and return the current season ID."""
        resp = self._api_get("seasons")
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch seasons: {resp.text}")
        for season in resp.json().get("data", []):
            if season.get("attributes", {}).get("isCurrentSeason"):
                return season["id"]
        # Fallback: return the last season in the list.
        seasons = resp.json().get("data", [])
        if not seasons:
            raise RuntimeError("No seasons returned by the API")
        return seasons[-1]["id"]

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)

        dispatch = {
            "players": self._read_players,
            "matches": self._read_matches,
            "season_stats": self._read_season_stats,
            "lifetime_stats": self._read_lifetime_stats,
            "leaderboards": self._read_leaderboards,
            "mastery": self._read_mastery,
            "clans": self._read_clans,
            "samples": self._read_samples,
            "telemetry": self._read_telemetry,
        }
        return dispatch[table_name](start_offset, table_options)

    # ------------------------------------------------------------------
    # Table readers
    # ------------------------------------------------------------------

    def _read_players(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch player objects by name or account ID.

        table_options:
            player_names: comma-separated display names (max 10).
            player_ids:   comma-separated account IDs (max 10).
        At least one must be provided.
        """
        player_names = table_options.get("player_names", "").strip()
        player_ids = table_options.get("player_ids", "").strip()

        if not player_names and not player_ids:
            raise ValueError(
                "players table requires 'player_names' or 'player_ids' in table_options"
            )

        params: dict = {}
        if player_names:
            params["filter[playerNames]"] = player_names
        else:
            params["filter[playerIds]"] = player_ids

        resp = self._api_get("players", params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch players: {resp.text}")

        records = []
        for item in resp.json().get("data", []):
            attrs = item.get("attributes", {})
            match_ids = [
                m["id"]
                for m in item.get("relationships", {})
                .get("matches", {})
                .get("data", [])
            ]
            records.append(
                {
                    "id": item["id"],
                    "name": attrs.get("name"),
                    "shardId": attrs.get("shardId"),
                    "titleId": attrs.get("titleId"),
                    "banType": attrs.get("banType"),
                    "clanId": attrs.get("clanId"),
                    "patchVersion": attrs.get("patchVersion"),
                    "matchIds": match_ids,
                }
            )

        # Players is a snapshot — no offset to checkpoint.
        return iter(records), {}

    def _read_matches(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch one or more matches by ID.

        The matches endpoint is not rate-limited and returns a single match per
        request. We iterate over the provided match IDs, skipping any already
        seen (tracked by offset).

        table_options:
            match_ids: comma-separated match UUIDs.
        """
        raw_ids = table_options.get("match_ids", "").strip()
        if not raw_ids:
            raise ValueError("matches table requires 'match_ids' in table_options")

        all_ids = [mid.strip() for mid in raw_ids.split(",") if mid.strip()]
        seen_ids: set[str] = set(start_offset.get("seen_ids", [])) if start_offset else set()

        records = []
        last_created_at = start_offset.get("cursor") if start_offset else None

        for match_id in all_ids:
            if match_id in seen_ids:
                continue

            resp = self._api_get(f"matches/{match_id}")
            if resp.status_code == 404:
                # Match data expired (>14 days) — skip silently.
                seen_ids.add(match_id)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch match {match_id}: {resp.text}")

            body = resp.json()
            data = body.get("data", {})
            included = body.get("included", [])

            attrs = data.get("attributes", {})
            created_at = attrs.get("createdAt")

            # Extract rosters
            roster_ids = {
                item["id"]
                for item in data.get("relationships", {})
                .get("rosters", {})
                .get("data", [])
            }
            rosters = []
            participants = []
            telemetry_url = None

            for inc in included:
                inc_type = inc.get("type")
                inc_id = inc.get("id")
                inc_attrs = inc.get("attributes", {})

                if inc_type == "roster" and inc_id in roster_ids:
                    participant_ids = [
                        p["id"]
                        for p in inc.get("relationships", {})
                        .get("participants", {})
                        .get("data", [])
                    ]
                    roster_stats = inc_attrs.get("stats", {})
                    rosters.append(
                        {
                            "id": inc_id,
                            "shardId": inc_attrs.get("shardId"),
                            "won": inc_attrs.get("won"),
                            "stats": {
                                "rank": roster_stats.get("rank"),
                                "teamId": roster_stats.get("teamId"),
                            },
                            "participantIds": participant_ids,
                        }
                    )

                elif inc_type == "participant":
                    p_stats = inc_attrs.get("stats", {})
                    participants.append(
                        {
                            "id": inc_id,
                            "actor": inc_attrs.get("actor"),
                            "shardId": inc_attrs.get("shardId"),
                            "stats": {
                                "DBNOs": p_stats.get("DBNOs"),
                                "assists": p_stats.get("assists"),
                                "boosts": p_stats.get("boosts"),
                                "damageDealt": p_stats.get("damageDealt"),
                                "deathType": p_stats.get("deathType"),
                                "headshotKills": p_stats.get("headshotKills"),
                                "heals": p_stats.get("heals"),
                                "killPlace": p_stats.get("killPlace"),
                                "killStreaks": p_stats.get("killStreaks"),
                                "kills": p_stats.get("kills"),
                                "longestKill": p_stats.get("longestKill"),
                                "name": p_stats.get("name"),
                                "playerId": p_stats.get("playerId"),
                                "revives": p_stats.get("revives"),
                                "rideDistance": p_stats.get("rideDistance"),
                                "roadKills": p_stats.get("roadKills"),
                                "swimDistance": p_stats.get("swimDistance"),
                                "teamKills": p_stats.get("teamKills"),
                                "timeSurvived": p_stats.get("timeSurvived"),
                                "vehicleDestroys": p_stats.get("vehicleDestroys"),
                                "walkDistance": p_stats.get("walkDistance"),
                                "weaponsAcquired": p_stats.get("weaponsAcquired"),
                                "winPlace": p_stats.get("winPlace"),
                            },
                        }
                    )

                elif inc_type == "asset" and inc_attrs.get("name") == "telemetry":
                    telemetry_url = inc_attrs.get("URL")

            record = {
                "id": data.get("id"),
                "createdAt": created_at,
                "duration": attrs.get("duration"),
                "matchType": attrs.get("matchType"),
                "gameMode": attrs.get("gameMode"),
                "mapName": attrs.get("mapName"),
                "isCustomMatch": attrs.get("isCustomMatch"),
                "patchVersion": attrs.get("patchVersion"),
                "seasonState": attrs.get("seasonState"),
                "shardId": attrs.get("shardId"),
                "titleId": attrs.get("titleId"),
                "telemetryUrl": telemetry_url,
                "rosters": rosters,
                "participants": participants,
            }
            records.append(record)
            seen_ids.add(match_id)

            if created_at and (last_created_at is None or created_at > last_created_at):
                last_created_at = created_at

        if not records:
            return iter([]), start_offset or {}

        end_offset: dict = {"seen_ids": sorted(seen_ids)}
        if last_created_at:
            end_offset["cursor"] = last_created_at

        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_season_stats(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch per-player, per-season aggregate statistics.

        table_options:
            player_ids: comma-separated account IDs.
            season_id:  Season ID (default: current season).
        """
        player_ids = table_options.get("player_ids", "").strip()
        if not player_ids:
            raise ValueError("season_stats table requires 'player_ids' in table_options")

        season_id = table_options.get("season_id", "").strip()
        if not season_id:
            season_id = self._get_current_season_id()

        all_ids = [pid.strip() for pid in player_ids.split(",") if pid.strip()]
        seen_key = f"seen_{season_id}"
        seen_ids: set[str] = set(start_offset.get(seen_key, [])) if start_offset else set()

        records = []
        for account_id in all_ids:
            if account_id in seen_ids:
                continue

            resp = self._api_get(f"players/{account_id}/seasons/{season_id}")
            if resp.status_code == 404:
                seen_ids.add(account_id)
                continue
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch season stats for {account_id}/{season_id}: {resp.text}"
                )

            data = resp.json().get("data", {})
            attrs = data.get("attributes", {})
            gms = attrs.get("gameModeStats", {})
            season_match_ids = attrs.get("seasonMatchIds", {})

            records.append(
                {
                    "accountId": account_id,
                    "seasonId": season_id,
                    "gameModeStats": _normalize_game_mode_stats(gms),
                    "bestRankPoint": attrs.get("bestRankPoint"),
                    "seasonMatchIds": {
                        "solo": season_match_ids.get("solo", []),
                        "solo-fpp": season_match_ids.get("solo-fpp", []),
                        "duo": season_match_ids.get("duo", []),
                        "duo-fpp": season_match_ids.get("duo-fpp", []),
                        "squad": season_match_ids.get("squad", []),
                        "squad-fpp": season_match_ids.get("squad-fpp", []),
                    },
                }
            )
            seen_ids.add(account_id)

        if not records:
            return iter([]), start_offset or {}

        end_offset = {seen_key: sorted(seen_ids)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_lifetime_stats(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch lifetime cumulative stats per player.

        table_options:
            player_ids: comma-separated account IDs.
        """
        player_ids = table_options.get("player_ids", "").strip()
        if not player_ids:
            raise ValueError("lifetime_stats table requires 'player_ids' in table_options")

        all_ids = [pid.strip() for pid in player_ids.split(",") if pid.strip()]
        seen_ids: set[str] = set(start_offset.get("seen_ids", [])) if start_offset else set()

        records = []
        for account_id in all_ids:
            if account_id in seen_ids:
                continue

            resp = self._api_get(f"players/{account_id}/seasons/lifetime")
            if resp.status_code == 404:
                seen_ids.add(account_id)
                continue
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch lifetime stats for {account_id}: {resp.text}"
                )

            data = resp.json().get("data", {})
            attrs = data.get("attributes", {})
            gms = attrs.get("gameModeStats", {})

            records.append(
                {
                    "accountId": account_id,
                    "gameModeStats": _normalize_game_mode_stats(gms),
                    "bestRankPoint": attrs.get("bestRankPoint"),
                }
            )
            seen_ids.add(account_id)

        if not records:
            return iter([]), start_offset or {}

        end_offset = {"seen_ids": sorted(seen_ids)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_leaderboards(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch top-500 leaderboard players per game mode for the current season.

        table_options:
            season_id:  Season ID (default: current season).
            game_modes: Comma-separated game modes
                        (default: solo,solo-fpp,duo,duo-fpp,squad,squad-fpp).
        """
        season_id = table_options.get("season_id", "").strip()
        if not season_id:
            season_id = self._get_current_season_id()

        raw_modes = table_options.get("game_modes", "solo,solo-fpp,duo,duo-fpp,squad,squad-fpp")
        game_modes = [m.strip() for m in raw_modes.split(",") if m.strip()]

        records = []
        for game_mode in game_modes:
            page = 1
            while True:
                resp = self._api_get(
                    f"leaderboards/{season_id}/{game_mode}",
                    params={"page[number]": str(page)},
                )
                if resp.status_code in (400, 404):
                    # 404: season/mode not found. 400: shard may not support leaderboards.
                    break
                if resp.status_code != 200:
                    raise RuntimeError(
                        f"Failed to fetch leaderboard {season_id}/{game_mode}: {resp.text}"
                    )

                body = resp.json()
                data = body.get("data", {})
                lb_attrs = data.get("attributes", {})
                leaderboard_id = data.get("id", f"leaderboard-{self._shard}-{game_mode}-{season_id}")

                for player in body.get("included", []):
                    if player.get("type") != "player":
                        continue
                    p_attrs = player.get("attributes", {})
                    p_stats = p_attrs.get("stats", {})
                    records.append(
                        {
                            "leaderboardId": leaderboard_id,
                            "accountId": player["id"],
                            "seasonId": lb_attrs.get("seasonId", season_id),
                            "gameMode": lb_attrs.get("gameMode", game_mode),
                            "shardId": lb_attrs.get("shardId", self._shard),
                            "name": p_attrs.get("name"),
                            "rank": p_attrs.get("rank"),
                            "stats": {
                                "rankPoints": p_stats.get("rankPoints"),
                                "wins": p_stats.get("wins"),
                                "games": p_stats.get("games"),
                                "winRatio": p_stats.get("winRatio"),
                                "averageDamage": p_stats.get("averageDamage"),
                                "kills": p_stats.get("kills"),
                                "killDeathRatio": p_stats.get("killDeathRatio"),
                                "kda": p_stats.get("kda"),
                                "averageRank": p_stats.get("averageRank"),
                            },
                        }
                    )

                # Leaderboards max out at 500 players per page; stop after page 1
                # (the API only has one page of results).
                break

        # Leaderboard is a snapshot — return no offset to re-fetch fresh data each run.
        return iter(records), {}

    def _read_mastery(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch weapon mastery and/or survival mastery per player.

        table_options:
            player_ids:    Comma-separated account IDs.
            mastery_types: Comma-separated types: weapon, survival
                           (default: weapon,survival).
        """
        player_ids = table_options.get("player_ids", "").strip()
        if not player_ids:
            raise ValueError("mastery table requires 'player_ids' in table_options")

        raw_types = table_options.get("mastery_types", "weapon,survival")
        mastery_types = [t.strip() for t in raw_types.split(",") if t.strip()]

        all_ids = [pid.strip() for pid in player_ids.split(",") if pid.strip()]
        seen_ids: set[str] = set(start_offset.get("seen_ids", [])) if start_offset else set()

        records = []
        for account_id in all_ids:
            for mastery_type in mastery_types:
                key = f"{account_id}:{mastery_type}"
                if key in seen_ids:
                    continue

                if mastery_type == "weapon":
                    resp = self._api_get(f"players/{account_id}/weapon_mastery")
                elif mastery_type == "survival":
                    resp = self._api_get(f"players/{account_id}/survival_mastery")
                else:
                    raise ValueError(f"Unknown mastery type: {mastery_type!r}")

                if resp.status_code == 404:
                    seen_ids.add(key)
                    continue
                if resp.status_code != 200:
                    raise RuntimeError(
                        f"Failed to fetch {mastery_type} mastery for {account_id}: {resp.text}"
                    )

                data = resp.json().get("data", {})
                attrs = data.get("attributes", {})

                if mastery_type == "weapon":
                    weapon_summary = attrs.get("weaponsummary", {})
                    weapons = []
                    for weapon_id, ws in weapon_summary.items():
                        entry = {
                            "weaponId": weapon_id,
                            "XPTotal": ws.get("XPTotal"),
                            "LevelCurrent": ws.get("LevelCurrent"),
                            "TierCurrent": ws.get("TierCurrent"),
                            "StatsTotal": _normalize_weapon_stats(ws.get("StatsTotal")),
                            "OfficialStatsTotal": _normalize_weapon_stats(
                                ws.get("OfficialStatsTotal")
                            ),
                            "CompetitiveStatsTotal": _normalize_weapon_stats(
                                ws.get("CompetitiveStatsTotal")
                            ),
                        }
                        weapons.append(entry)

                    records.append(
                        {
                            "accountId": account_id,
                            "masteryType": "weapon",
                            "weapons": weapons,
                            "xp": None,
                            "level": None,
                            "tier": None,
                            "totalMatchesPlayed": None,
                            "survivalStats": None,
                        }
                    )

                else:  # survival
                    survival_stats = attrs.get("stats", {})
                    records.append(
                        {
                            "accountId": account_id,
                            "masteryType": "survival",
                            "weapons": None,
                            "xp": attrs.get("xp"),
                            "level": attrs.get("level"),
                            "tier": attrs.get("tier"),
                            "totalMatchesPlayed": attrs.get("totalMatchesPlayed"),
                            "survivalStats": {
                                "airDropsCalled": survival_stats.get("airDropsCalled"),
                                "damageDealt": survival_stats.get("damageDealt"),
                                "damageFromBlueZone": survival_stats.get("damageFromBlueZone"),
                                "damageFromEnemies": survival_stats.get("damageFromEnemies"),
                                "damageFromExplosions": survival_stats.get("damageFromExplosions"),
                                "distanceByFreefall": survival_stats.get("distanceByFreefall"),
                                "distanceBySwimming": survival_stats.get("distanceBySwimming"),
                                "distanceByVehicle": survival_stats.get("distanceByVehicle"),
                                "distanceOnFoot": survival_stats.get("distanceOnFoot"),
                                "enemiesKilled": survival_stats.get("enemiesKilled"),
                                "healed": survival_stats.get("healed"),
                                "objectsDestroyed": survival_stats.get("objectsDestroyed"),
                                "revived": survival_stats.get("revived"),
                                "teammateDamageDealt": survival_stats.get("teammateDamageDealt"),
                                "throwablesThrown": survival_stats.get("throwablesThrown"),
                                "top10Finishes": survival_stats.get("top10Finishes"),
                                "wins": survival_stats.get("wins"),
                            },
                        }
                    )

                seen_ids.add(key)

        if not records:
            return iter([]), start_offset or {}

        end_offset = {"seen_ids": sorted(seen_ids)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_clans(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch clan information by clan ID.

        table_options:
            clan_ids: Comma-separated clan IDs.
        """
        raw_ids = table_options.get("clan_ids", "").strip()
        if not raw_ids:
            raise ValueError("clans table requires 'clan_ids' in table_options")

        all_ids = [cid.strip() for cid in raw_ids.split(",") if cid.strip()]
        seen_ids: set[str] = set(start_offset.get("seen_ids", [])) if start_offset else set()

        records = []
        for clan_id in all_ids:
            if clan_id in seen_ids:
                continue

            resp = self._api_get(f"clans/{clan_id}")
            if resp.status_code == 404:
                seen_ids.add(clan_id)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch clan {clan_id}: {resp.text}")

            data = resp.json().get("data", {})
            attrs = data.get("attributes", {})
            records.append(
                {
                    "id": data.get("id"),
                    "clanName": attrs.get("clanName"),
                    "clanTag": attrs.get("clanTag"),
                    "clanLevel": attrs.get("clanLevel"),
                    "clanMemberCount": attrs.get("clanMemberCount"),
                }
            )
            seen_ids.add(clan_id)

        if not records:
            return iter([]), start_offset or {}

        end_offset = {"seen_ids": sorted(seen_ids)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _read_samples(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Fetch the random sample of match IDs (refreshes every 24h).

        table_options:
            created_at_start: ISO 8601 datetime to filter sample start
                              (optional — defaults to most recent sample).

        Uses ``data.attributes.createdAt`` as the incremental cursor.
        """
        since = start_offset.get("cursor") if start_offset else None
        if not since:
            since = table_options.get("created_at_start", "").strip() or None

        # If we already have a cursor that is at or beyond init time, no new data.
        if since and since >= self._init_ts:
            return iter([]), start_offset or {}

        params: dict = {}
        if since:
            params["filter[createdAt-start]"] = since

        resp = self._api_get("samples", params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch samples: {resp.text}")

        data = resp.json().get("data", {})
        attrs = data.get("attributes", {})
        created_at = attrs.get("createdAt")

        match_ids = [
            m["id"]
            for m in data.get("relationships", {}).get("matches", {}).get("data", [])
        ]

        record = {
            "id": data.get("id"),
            "createdAt": created_at,
            "titleId": attrs.get("titleId"),
            "shardId": attrs.get("shardId"),
            "matchIds": match_ids,
        }

        end_offset = {"cursor": created_at} if created_at else {}

        # If offset did not advance, there's no new data.
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter([record]), end_offset

    def _read_telemetry(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Download and flatten telemetry event files from the PUBG CDN.

        Each match's telemetry file is a JSON array of events. We first fetch
        the match to get the CDN URL, then download the file (no auth required).

        table_options:
            match_ids: Comma-separated match UUIDs whose telemetry to download.

        Offset tracks which match IDs have already been processed.
        """
        raw_ids = table_options.get("match_ids", "").strip()
        if not raw_ids:
            raise ValueError("telemetry table requires 'match_ids' in table_options")

        all_ids = [mid.strip() for mid in raw_ids.split(",") if mid.strip()]
        seen_ids: set[str] = set(start_offset.get("seen_ids", [])) if start_offset else set()

        import json as _json  # local import to keep module-level imports minimal

        records = []
        for match_id in all_ids:
            if match_id in seen_ids:
                continue

            # Step 1: Fetch match to get telemetry URL (not rate-limited).
            match_resp = self._api_get(f"matches/{match_id}")
            if match_resp.status_code == 404:
                seen_ids.add(match_id)
                continue
            if match_resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch match {match_id} for telemetry: {match_resp.text}"
                )

            body = match_resp.json()
            telemetry_url = None
            for inc in body.get("included", []):
                if inc.get("type") == "asset" and inc.get("attributes", {}).get("name") == "telemetry":
                    telemetry_url = inc["attributes"].get("URL")
                    break

            if not telemetry_url:
                seen_ids.add(match_id)
                continue

            # Step 2: Download the telemetry file from the CDN (no auth, no rate limit).
            cdn_resp = self._cdn_get(telemetry_url)
            if cdn_resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to download telemetry for match {match_id}: {cdn_resp.text}"
                )

            events = cdn_resp.json()
            if not isinstance(events, list):
                seen_ids.add(match_id)
                continue

            for idx, event in enumerate(events):
                common = event.get("common", {})
                # Serialize non-scalar common fields to strings for the MapType schema.
                common_str = {k: str(v) for k, v in common.items()} if common else {}
                records.append(
                    {
                        "matchId": match_id,
                        "eventIndex": idx,
                        "_D": event.get("_D"),
                        "_T": event.get("_T"),
                        "common": common_str,
                        "payload": _json.dumps(event),
                    }
                )

            seen_ids.add(match_id)

        if not records:
            return iter([]), start_offset or {}

        end_offset = {"seen_ids": sorted(seen_ids)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table {table_name!r} is not supported. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )


# ---------------------------------------------------------------------------
# Module-level helper functions
# ---------------------------------------------------------------------------


def _normalize_game_mode_stats(gms: dict) -> dict:
    """Normalize the gameModeStats dict, filling missing modes with None."""
    modes = ["solo", "solo-fpp", "duo", "duo-fpp", "squad", "squad-fpp"]
    result = {}
    for mode in modes:
        mode_data = gms.get(mode)
        if mode_data is None:
            result[mode] = None
        else:
            result[mode] = {
                "assists": mode_data.get("assists"),
                "boosts": mode_data.get("boosts"),
                "dBNOs": mode_data.get("dBNOs"),
                "dailyKills": mode_data.get("dailyKills"),
                "dailyWins": mode_data.get("dailyWins"),
                "damageDealt": mode_data.get("damageDealt"),
                "days": mode_data.get("days"),
                "headshotKills": mode_data.get("headshotKills"),
                "heals": mode_data.get("heals"),
                "killPoints": mode_data.get("killPoints"),
                "kills": mode_data.get("kills"),
                "longestKill": mode_data.get("longestKill"),
                "longestTimeSurvived": mode_data.get("longestTimeSurvived"),
                "losses": mode_data.get("losses"),
                "maxKillStreaks": mode_data.get("maxKillStreaks"),
                "mostSurvivalTime": mode_data.get("mostSurvivalTime"),
                "rankPoints": mode_data.get("rankPoints"),
                "rankPointsTitle": mode_data.get("rankPointsTitle"),
                "revives": mode_data.get("revives"),
                "rideDistance": mode_data.get("rideDistance"),
                "roadKills": mode_data.get("roadKills"),
                "roundMostKills": mode_data.get("roundMostKills"),
                "roundsPlayed": mode_data.get("roundsPlayed"),
                "suicides": mode_data.get("suicides"),
                "swimDistance": mode_data.get("swimDistance"),
                "teamKills": mode_data.get("teamKills"),
                "timeSurvived": mode_data.get("timeSurvived"),
                "top10s": mode_data.get("top10s"),
                "vehicleDestroys": mode_data.get("vehicleDestroys"),
                "walkDistance": mode_data.get("walkDistance"),
                "weaponsAcquired": mode_data.get("weaponsAcquired"),
                "weeklyKills": mode_data.get("weeklyKills"),
                "weeklyWins": mode_data.get("weeklyWins"),
                "wins": mode_data.get("wins"),
            }
    return result


def _normalize_weapon_stats(stats: dict | None) -> dict | None:
    """Normalize a weapon stats sub-object, returning None if absent."""
    if stats is None:
        return None
    return {
        "Kills": stats.get("Kills"),
        "HeadShots": stats.get("HeadShots"),
        "Groggies": stats.get("Groggies"),
        "DamagePlayer": stats.get("DamagePlayer"),
        "LongestKill": stats.get("LongestKill"),
        "MostKillsInAGame": stats.get("MostKillsInAGame"),
    }
