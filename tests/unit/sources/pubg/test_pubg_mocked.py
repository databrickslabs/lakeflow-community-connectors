"""Mock-based unit tests for PubgLakeflowConnect.

Stubs ``requests.Session`` so tests run without credentials in CI.
Live-API integration tests live in ``test_pubg_lakeflow_connect.py``
and are excluded from CI via test_exclude.txt.
"""

from unittest.mock import MagicMock

import pytest

from databricks.labs.community_connector.sources.pubg.pubg import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    PubgLakeflowConnect,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _response(status_code: int = 200, json_body=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body if json_body is not None else {}
    resp.text = str(json_body) if json_body is not None else ""
    resp.headers = {}
    return resp


@pytest.fixture
def conn():
    """PubgLakeflowConnect with a stubbed requests.Session."""
    c = PubgLakeflowConnect({"api_key": "fake_jwt", "shard": "steam"})
    c._session = MagicMock()
    return c


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------

def test_init_requires_api_key():
    with pytest.raises(ValueError, match="api_key"):
        PubgLakeflowConnect({"shard": "steam"})


def test_init_requires_shard():
    with pytest.raises(ValueError, match="shard"):
        PubgLakeflowConnect({"api_key": "fake_jwt"})


def test_init_sets_base_url(conn):
    assert "steam" in conn._base_url
    assert "api.pubg.com" in conn._base_url


# ---------------------------------------------------------------------------
# list_tables / get_table_schema / read_table_metadata
# ---------------------------------------------------------------------------

def test_list_tables_returns_all(conn):
    tables = conn.list_tables()
    assert set(tables) == set(SUPPORTED_TABLES)
    assert len(tables) == len(SUPPORTED_TABLES)


def test_get_table_schema_all_tables(conn):
    for table in SUPPORTED_TABLES:
        schema = conn.get_table_schema(table, {})
        assert schema is not None
        assert schema == TABLE_SCHEMAS[table]


def test_get_table_schema_unknown_raises(conn):
    with pytest.raises(ValueError):
        conn.get_table_schema("nonexistent_table", {})


def test_read_table_metadata_all_tables(conn):
    for table in SUPPORTED_TABLES:
        meta = conn.read_table_metadata(table, {})
        assert "primary_keys" in meta
        assert isinstance(meta["primary_keys"], list)
        assert len(meta["primary_keys"]) > 0


def test_read_table_metadata_unknown_raises(conn):
    with pytest.raises(ValueError):
        conn.read_table_metadata("nonexistent_table", {})


def test_metadata_ingestion_types_valid(conn):
    valid_types = {"snapshot", "append", "cdc", "cdc_with_deletes"}
    for table, meta in TABLE_METADATA.items():
        assert meta["ingestion_type"] in valid_types, (
            f"{table} has unknown ingestion_type '{meta['ingestion_type']}'"
        )


# ---------------------------------------------------------------------------
# read_table — input validation
# ---------------------------------------------------------------------------

def test_read_players_requires_names_or_ids(conn):
    with pytest.raises(ValueError, match="player_names.*player_ids|player_ids.*player_names"):
        conn.read_table("players", None, {})


def test_read_matches_requires_match_ids(conn):
    with pytest.raises(ValueError, match="match_ids"):
        conn.read_table("matches", None, {})


def test_read_season_stats_requires_player_ids(conn):
    with pytest.raises(ValueError, match="player_ids"):
        conn.read_table("season_stats", None, {})


def test_read_lifetime_stats_requires_player_ids(conn):
    with pytest.raises(ValueError, match="player_ids"):
        conn.read_table("lifetime_stats", None, {})


def test_read_mastery_requires_player_ids(conn):
    with pytest.raises(ValueError, match="player_ids"):
        conn.read_table("mastery", None, {})


def test_read_clans_requires_clan_ids(conn):
    with pytest.raises(ValueError, match="clan_ids"):
        conn.read_table("clans", None, {})


def test_read_telemetry_requires_match_ids(conn):
    with pytest.raises(ValueError, match="match_ids"):
        conn.read_table("telemetry", None, {})


def test_read_unknown_table_raises(conn):
    with pytest.raises(ValueError):
        conn.read_table("nonexistent_table", None, {})


# ---------------------------------------------------------------------------
# read_table — samples (no required options, mocked HTTP)
# ---------------------------------------------------------------------------

# samples returns a single data object (not a list)
_SAMPLE_RESPONSE = {
    "data": {
        "id": "samples.2024-01-15T10:00:00Z",
        "type": "sample",
        "attributes": {
            "createdAt": "2024-01-15T10:00:00Z",
            "titleId": "bluehole-pubg",
            "shardId": "steam",
        },
        "relationships": {
            "matches": {
                "data": [
                    {"type": "match", "id": "match.abc123"},
                ]
            }
        },
    },
}

_SEASONS_RESPONSE = {
    "data": [
        {
            "id": "division.bro.official.pc-2018-01",
            "type": "season",
            "attributes": {"isCurrentSeason": True, "isOffseason": False},
        }
    ]
}


def test_read_samples_returns_records(conn):
    conn._session.get.return_value = _response(200, _SAMPLE_RESPONSE)
    records, offset = conn.read_table("samples", None, {})
    record_list = list(records)
    assert len(record_list) == 1
    assert record_list[0]["id"] == "samples.2024-01-15T10:00:00Z"
    assert "match.abc123" in record_list[0]["matchIds"]


def test_read_samples_empty_response(conn):
    conn._session.get.return_value = _response(200, {"data": {}})
    records, offset = conn.read_table("samples", None, {})
    # Empty data dict yields one record with None id (connector returns iter([record]))
    record_list = list(records)
    assert isinstance(record_list, list)


def test_read_samples_offset_advances(conn):
    conn._session.get.return_value = _response(200, _SAMPLE_RESPONSE)
    _, offset = conn.read_table("samples", None, {})
    assert offset is not None
    assert isinstance(offset, dict)
    assert offset.get("cursor") == "2024-01-15T10:00:00Z"


# ---------------------------------------------------------------------------
# read_table — leaderboards (no required options, mocked HTTP)
# ---------------------------------------------------------------------------

_LEADERBOARD_RESPONSE = {
    "data": {
        "id": "division.bro.official.pc-2018-01.squad-fpp",
        "type": "leaderboard",
        "attributes": {"gameMode": "squad-fpp", "seasonId": "division.bro.official.pc-2018-01"},
    },
    "included": [
        {
            "type": "player",
            "id": "account.abc",
            "attributes": {
                "name": "TestPlayer",
                "rank": 1,
                "stats": {
                    "rankPoints": 5000.0,
                    "wins": 10,
                    "games": 50,
                    "winRatio": 0.2,
                    "averageDamage": 300.0,
                    "kills": 100,
                    "killDeathRatio": 3.0,
                    "kda": 3.0,
                    "averageRank": 5.0,
                    "tier": "diamond",
                    "subTier": "1",
                },
            },
        }
    ],
}


def test_read_leaderboards_returns_records(conn):
    # leaderboards first fetches the current season, then fetches each game mode
    conn._session.get.side_effect = [
        _response(200, _SEASONS_RESPONSE),   # _get_current_season_id()
        _response(200, _LEADERBOARD_RESPONSE),  # first game mode page
        _response(404, {}),  # subsequent game modes — terminate
        _response(404, {}),
        _response(404, {}),
        _response(404, {}),
        _response(404, {}),
    ]
    records, offset = conn.read_table("leaderboards", None, {})
    record_list = list(records)
    assert len(record_list) >= 1
    assert record_list[0]["accountId"] == "account.abc"


def test_read_leaderboards_400_returns_empty(conn):
    """400 from the API (unsupported shard/season) should yield empty records."""
    conn._session.get.side_effect = [
        _response(200, _SEASONS_RESPONSE),
        _response(400, {}),
        _response(400, {}),
        _response(400, {}),
        _response(400, {}),
        _response(400, {}),
        _response(400, {}),
    ]
    records, offset = conn.read_table("leaderboards", None, {})
    assert list(records) == []


def test_read_leaderboards_404_returns_empty(conn):
    conn._session.get.side_effect = [
        _response(200, _SEASONS_RESPONSE),
        _response(404, {}),
        _response(404, {}),
        _response(404, {}),
        _response(404, {}),
        _response(404, {}),
        _response(404, {}),
    ]
    records, offset = conn.read_table("leaderboards", None, {})
    assert list(records) == []
