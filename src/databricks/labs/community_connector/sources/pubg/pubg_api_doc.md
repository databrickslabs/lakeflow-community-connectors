# PUBG Developer API — Comprehensive Reference

**Primary documentation**: https://documentation.pubg.com/en/introduction.html
**Developer portal**: https://developer.pubg.com
**API status**: https://developer.pubg.com/status
**Discord**: https://discord.gg/FcsT7t3

---

## Table of Contents

1. [Authentication](#authentication)
2. [Rate Limits](#rate-limits)
3. [Base URL and Shards](#base-url-and-shards)
4. [Request Format](#request-format)
5. [Response Format](#response-format)
6. [Data Retention](#data-retention)
7. [Versioning](#versioning)
8. [Endpoints](#endpoints)
   - [players](#players)
   - [matches](#matches)
   - [season_stats](#season_stats)
   - [lifetime_stats](#lifetime_stats)
   - [leaderboards](#leaderboards)
   - [mastery](#mastery)
   - [clans](#clans)
   - [samples](#samples)
   - [telemetry](#telemetry)
9. [Telemetry Events Reference](#telemetry-events-reference)
10. [Telemetry Objects Reference](#telemetry-objects-reference)
11. [Known Issues](#known-issues)

---

## Authentication

### Obtaining an API Key

Register at https://developer.pubg.com to receive a free API key. The free tier includes **10 requests per minute**. To request a higher limit, use the "MY APPS" dashboard and submit documentation of:
- Number of users
- Request frequency
- Caching strategy
- A working sample of your application

API keys are **JSON Web Tokens (JWTs)** — they are generated for you upon registration and do not need to be constructed manually.

### Using the API Key

All requests (except telemetry CDN downloads) must include the API key as a **Bearer token** in the `Authorization` header:

```
Authorization: Bearer <your-api-key>
```

**Security requirement**: API keys must **never** be stored client-side. All API calls must originate from a secure server-side application.

### Required Headers

| Header | Value | Required |
|--------|-------|----------|
| `Authorization` | `Bearer <api-key>` | Yes (except telemetry CDN) |
| `Accept` | `application/vnd.api+json` | Yes (or `application/json`) |
| `Accept-Encoding` | `gzip` | Recommended for large responses |

The server mirrors the requested `Accept` content-type in its response. CORS is supported for all origins (`Access-Control-Allow-Origin: *`).

### Title ID

The `TitleID` for a request is determined automatically by the API key and sent in the `Authorization` header. No manual configuration is needed; each key is associated with a specific game title.

---

## Rate Limits

| Tier | Limit |
|------|-------|
| Default (free) | 10 requests per minute |
| Custom (approved) | Higher limits granted on application |

### Rate Limit Response Headers

| Header | Description |
|--------|-------------|
| `X-RateLimit-Limit` | Request limit per day / per minute |
| `X-RateLimit-Remaining` | Requests remaining in the current window |
| `X-RateLimit-Reset` | UNIX timestamp when the limit resets |

### Exceeding the Limit

An HTTP **429 Too Many Requests** error is returned. Requests resume within one minute.

### Exempted Endpoints

The `/matches` and `/telemetry` endpoints are **not rate-limited**. Rate limit quota is consumed primarily by:
- `/players?filter[playerNames]=...` — player lookup by name
- `/players/{accountId}` — player lookup by ID
- `/players/{accountId}/seasons/{seasonId}` — season stats

A typical player lookup workflow consumes **two requests** (one for player object, one for season stats).

---

## Base URL and Shards

```
https://api.pubg.com/shards/{shard}/
```

The API shards data by platform. Most requests require a **platform shard**. Some older historical queries use **platform-region shards** (deprecated for most uses).

### Platform Shards (current)

| Shard | Description |
|-------|-------------|
| `steam` | PC (Steam) |
| `psn` | PlayStation Network |
| `xbox` | Xbox |
| `kakao` | Kakao (Korea PC) |
| `stadia` | Google Stadia |
| `console` | Console (cross-platform) |
| `tournament` | Tournament matches |

> **Note**: Use the platform shard when making requests for Stadia players' season stats, PC and PSN players' season stats for seasons after `division.bro.official.2018-09`.

### Platform-Region Shards (legacy, mostly deprecated)

| PC | PSN | Xbox |
|----|-----|------|
| `pc-as`, `pc-eu`, `pc-jp`, `pc-kakao` | `psn-as`, `psn-eu` | `xbox-as`, `xbox-eu` |
| `pc-krjp`, `pc-na`, `pc-oc`, `pc-ru` | `psn-na`, `psn-oc` | `xbox-na`, `xbox-oc`, `xbox-sa` |
| `pc-sa`, `pc-sea`, `pc-tournament` | | |

---

## Request Format

URL pattern:

```
https://api.pubg.com/shards/{shard}/{endpoint}?filter[param]=value
```

### Common URL Parameters

| Parameter | Format | Description |
|-----------|--------|-------------|
| `filter[playerNames]` | Comma-separated strings | Filter players by display name (max 10) |
| `filter[playerIds]` | Comma-separated strings | Filter players by account ID (max 10) |
| `filter[createdAt-start]` | ISO 8601 datetime | Start of time window for samples |
| `filter[gamepad]` | `true` / `false` | Stadia gamepad filter |
| `page[number]` | Integer | Page number for paginated endpoints (500 players/page) |

### Game Modes

| Mode | Description |
|------|-------------|
| `solo` | Solo third-person |
| `solo-fpp` | Solo first-person |
| `duo` | Duo third-person |
| `duo-fpp` | Duo first-person |
| `squad` | Squad third-person |
| `squad-fpp` | Squad first-person |

---

## Response Format

All responses conform to the **JSON:API specification** (http://jsonapi.org/). The root object contains:

| Key | Required | Description |
|-----|----------|-------------|
| `data` | Yes (or `errors`) | Primary resource data (object or array) |
| `errors` | Yes (or `data`) | Error details array |
| `links` | No | Pagination and self-links |
| `included` | No | Related resources (e.g., rosters, participants, assets in match responses) |
| `meta` | No | Non-standard metadata |

Each resource object has the shape:
```json
{
  "type": "resource-type",
  "id": "unique-id",
  "attributes": { ... },
  "relationships": { ... },
  "links": { ... }
}
```

---

## Data Retention

| Data type | Retention window |
|-----------|-----------------|
| Match data | **14 days** — matches older than 14 days are not retrievable |
| Player match history | 14 days via the players endpoint |
| Season stats match list | Up to **32 most recent matches** within the 14-day window |
| Sample data | Updated every **24 hours** per platform |
| Leaderboards | Updated every **2 hours** for the current season |

---

## Versioning

The API follows **SEMVER** (MAJOR.MINOR.PATCH):

- **MAJOR** — incompatible API changes
- **MINOR** — backwards-compatible new functionality
- **PATCH** — backwards-compatible bug fixes

Two separate version numbers exist: the API service version (from `/status` endpoint) and the data version (from the changelog). Deprecated fields are typically hard-coded to zero for **14 days** before removal. Changes are announced at least one week in advance in the changelog.

---

## Endpoints

---

### players

Retrieve player objects by name or account ID. Each player object contains a list of recent match IDs.

#### List Players by Name or ID

```
GET https://api.pubg.com/shards/{shard}/players
```

**Authentication**: Required (`Authorization: Bearer <api-key>`)

**Query Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `filter[playerNames]` | One of these two | Comma-separated player display names (max 10) |
| `filter[playerIds]` | One of these two | Comma-separated account IDs (max 10) |

> Exactly one of `filter[playerNames]` or `filter[playerIds]` must be provided.

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/players?filter[playerNames]=shroud,drdisrespect" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Example Response**:

```json
{
  "data": [
    {
      "type": "player",
      "id": "account.0000000000000000000000000000000a",
      "attributes": {
        "name": "shroud",
        "shardId": "steam",
        "stats": null,
        "titleId": "bluehole-pubg",
        "patchVersion": "",
        "banType": "Innocent",
        "clanId": ""
      },
      "relationships": {
        "assets": { "data": [] },
        "matches": {
          "data": [
            { "type": "match", "id": "a86c06a3-d658-11e7-8bb8-0a586460fd5e" },
            { "type": "match", "id": "..." }
          ]
        }
      },
      "links": {
        "schema": "https://raw.githubusercontent.com/pubg/api-assets/master/schemas/player.json",
        "self": "https://api.pubg.com/shards/steam/players/account.0000000000000000000000000000000a"
      }
    }
  ]
}
```

**Key Response Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `data[].id` | string | Player account ID — use this as `{accountId}` in subsequent calls |
| `data[].attributes.name` | string | Player display name |
| `data[].attributes.shardId` | string | Platform shard |
| `data[].attributes.banType` | string | Ban status (`Innocent`, `Temp`, `Permanent`) |
| `data[].attributes.clanId` | string | Clan ID (added in v22.1.0; empty string if no clan) |
| `data[].relationships.matches.data` | array | List of recent match IDs (up to 14 days) |

#### Get Player by Account ID

```
GET https://api.pubg.com/shards/{shard}/players/{accountId}
```

**Path Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `shard` | Yes | Platform shard |
| `accountId` | Yes | Player account ID (e.g., `account.abc123`) |

**Pagination**: None — all recent matches returned in one response (up to 14 days).

**Rate Limit Impact**: Counts against rate limit quota.

**Incremental Sync Key**: `data[].relationships.matches.data` — poll for new match IDs. The newest matches appear first in the list.

---

### matches

Retrieve a single match by ID. Match data includes attributes, rosters, participants, and a reference to the telemetry asset.

```
GET https://api.pubg.com/shards/{shard}/matches/{matchId}
```

**Authentication**: Required (`Authorization: Bearer <api-key>`)
**Rate Limit**: This endpoint is **exempt** from rate limiting.

**Path Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `shard` | Yes | Platform shard |
| `matchId` | Yes | Match UUID (obtained from player relationships) |

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/matches/a86c06a3-d658-11e7-8bb8-0a586460fd5e" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response Structure**:

The response contains `data` (the match object) and `included` (an array of related objects: rosters, participants, assets).

**Match Object** (`data`):

```json
{
  "type": "match",
  "id": "a86c06a3-d658-11e7-8bb8-0a586460fd5e",
  "attributes": {
    "createdAt": "2018-01-01T00:00:00Z",
    "duration": 1877,
    "matchType": "official",
    "gameMode": "squad",
    "mapName": "Desert_Main",
    "isCustomMatch": false,
    "patchVersion": "14.1",
    "seasonState": "progress",
    "shardId": "steam",
    "stats": null,
    "tags": null,
    "titleId": "bluehole-pubg"
  },
  "relationships": {
    "assets": {
      "data": [{ "type": "asset", "id": "1ad97f85-cf9b-11e7-b84e-0a586460f004" }]
    },
    "rosters": {
      "data": [
        { "type": "roster", "id": "..." },
        ...
      ]
    }
  }
}
```

**Match Attributes**:

| Field | Type | Description |
|-------|------|-------------|
| `createdAt` | ISO 8601 datetime | Match start timestamp — key field for incremental sync |
| `duration` | int | Match duration in seconds |
| `matchType` | string | `official`, `custom`, `seasonal`, `event` |
| `gameMode` | string | `solo`, `solo-fpp`, `duo`, `duo-fpp`, `squad`, `squad-fpp` |
| `mapName` | string | Internal map identifier (e.g., `Desert_Main`, `Baltic_Main`, `DihorOtok_Main`) |
| `isCustomMatch` | bool | Whether this is a custom match |
| `patchVersion` | string | Game patch version |
| `seasonState` | string | Season phase at time of match |
| `shardId` | string | Platform shard |

**Roster Object** (in `included`):

```json
{
  "type": "roster",
  "id": "...",
  "attributes": {
    "shardId": "steam",
    "stats": {
      "rank": 1,
      "teamId": 4
    },
    "won": "true"
  },
  "relationships": {
    "participants": {
      "data": [
        { "type": "participant", "id": "..." }
      ]
    },
    "team": { "data": null }
  }
}
```

**Participant Object** (in `included`):

```json
{
  "type": "participant",
  "id": "...",
  "attributes": {
    "actor": "",
    "shardId": "steam",
    "stats": {
      "DBNOs": 2,
      "assists": 1,
      "boosts": 3,
      "damageDealt": 423.56,
      "deathType": "byplayer",
      "headshotKills": 1,
      "heals": 4,
      "killPlace": 5,
      "killStreaks": 2,
      "kills": 4,
      "longestKill": 248.12,
      "name": "shroud",
      "playerId": "account.0000000000000000000000000000000a",
      "revives": 1,
      "rideDistance": 1204.5,
      "roadKills": 0,
      "swimDistance": 0.0,
      "teamKills": 0,
      "timeSurvived": 1756.0,
      "vehicleDestroys": 0,
      "walkDistance": 3456.7,
      "weaponsAcquired": 7,
      "winPlace": 1
    }
  }
}
```

**Participant Stats Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `DBNOs` | int | Down-but-not-out events caused |
| `assists` | int | Kills assisted |
| `boosts` | int | Boost items used |
| `damageDealt` | float | Total damage dealt |
| `deathType` | string | `byplayer`, `byzone`, `suicide`, `logout` |
| `headshotKills` | int | Headshot kills |
| `heals` | int | Heal items used |
| `killPlace` | int | Kill ranking among all players |
| `kills` | int | Total kills |
| `longestKill` | float | Longest kill distance in meters |
| `name` | string | Player display name |
| `playerId` | string | Player account ID |
| `revives` | int | Teammates revived |
| `rideDistance` | float | Distance traveled in vehicles (meters) |
| `swimDistance` | float | Distance swum (meters) |
| `timeSurvived` | float | Seconds survived |
| `walkDistance` | float | Distance walked (meters) |
| `winPlace` | int | Final placement (1 = winner) |

**Asset Object** (in `included` — links to telemetry):

```json
{
  "type": "asset",
  "id": "1ad97f85-cf9b-11e7-b84e-0a586460f004",
  "attributes": {
    "URL": "https://telemetry-cdn.pubg.com/pc-krjp/2018/01/01/0/0/1ad97f85-cf9b-11e7-b84e-0a586460f004-telemetry.json",
    "createdAt": "2018-01-01T00:00:12Z",
    "description": "",
    "name": "telemetry"
  }
}
```

**Pagination**: Not applicable — single match returned per request.
**Data Retention**: Match data is available for **14 days** from match creation.
**Incremental Sync Key**: Use `data.attributes.createdAt` to order and filter matches chronologically.

---

### season_stats

Retrieve a player's aggregate statistics for a specific season or ranked season. Also used to get the list of available seasons.

#### List Seasons

```
GET https://api.pubg.com/shards/{shard}/seasons
```

**Authentication**: Required
**Rate Limit**: Counts against quota.

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/seasons" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response** — Season Object:

```json
{
  "type": "season",
  "id": "division.bro.official.2018-09",
  "attributes": {
    "isCurrentSeason": false,
    "isOffseason": false
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Season ID — use as `{seasonId}` in season stats requests |
| `attributes.isCurrentSeason` | bool | Whether this is the active season |
| `attributes.isOffseason` | bool | Whether this is an off-season period |

**Season ID Naming Convention**: `division.bro.official.{YYYY-MM}` for older seasons; newer seasons may use a different format.

#### Get Player Season Stats

```
GET https://api.pubg.com/shards/{shard}/players/{accountId}/seasons/{seasonId}
```

**Path Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `shard` | Yes | Platform shard |
| `accountId` | Yes | Player account ID |
| `seasonId` | Yes | Season ID from the seasons list |

**Shard Note**: For seasons after `division.bro.official.2018-09` on PC and PSN, use the **platform shard** (e.g., `steam`), not a platform-region shard.

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/players/account.0000000000000000000000000000000a/seasons/division.bro.official.2018-09" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response — Season Stats Attributes** (nested by game mode under `attributes.gameModeStats`):

```json
{
  "type": "playerSeason",
  "id": "...",
  "attributes": {
    "gameModeStats": {
      "squad": {
        "assists": 10,
        "boosts": 45,
        "dBNOs": 20,
        "dailyKills": 0,
        "dailyWins": 0,
        "damageDealt": 12345.67,
        "days": 0,
        "headshotKills": 5,
        "heals": 60,
        "killPoints": 0,
        "kills": 30,
        "longestKill": 300.0,
        "longestTimeSurvived": 1800.0,
        "losses": 25,
        "maxKillStreaks": 4,
        "mostSurvivalTime": 1800.0,
        "rankPoints": 0,
        "rankPointsTitle": "",
        "revives": 8,
        "rideDistance": 25000.0,
        "roadKills": 1,
        "roundMostKills": 7,
        "roundsPlayed": 26,
        "suicides": 0,
        "swimDistance": 200.0,
        "teamKills": 0,
        "timeSurvived": 35000.0,
        "top10s": 8,
        "vehicleDestroys": 2,
        "walkDistance": 80000.0,
        "weaponsAcquired": 150,
        "weeklyKills": 0,
        "weeklyWins": 0,
        "wins": 1
      },
      "squad-fpp": { ... },
      "solo": { ... },
      "solo-fpp": { ... },
      "duo": { ... },
      "duo-fpp": { ... }
    },
    "bestRankPoint": 0,
    "seasonMatchIds": {
      "squad": ["match-id-1", "match-id-2", ...],
      "squad-fpp": [...],
      ...
    }
  },
  "relationships": {
    "matchesSolo": { "data": [ ... ] },
    "matchesSoloFPP": { "data": [ ... ] },
    "matchesDuo": { "data": [ ... ] },
    "matchesDuoFPP": { "data": [ ... ] },
    "matchesSquad": { "data": [ ... ] },
    "matchesSquadFPP": { "data": [ ... ] },
    "player": { "data": { "type": "player", "id": "account...." } },
    "season": { "data": { "type": "season", "id": "division.bro.official.2018-09" } }
  }
}
```

**Key Stats Fields** (per game mode):

| Field | Type | Description |
|-------|------|-------------|
| `roundsPlayed` | int | Total matches played in this mode |
| `wins` | int | Number of wins (chicken dinners) |
| `top10s` | int | Top 10 finishes |
| `kills` | int | Total kills |
| `assists` | int | Total assists |
| `damageDealt` | float | Total damage dealt |
| `headshotKills` | int | Headshot kills |
| `longestKill` | float | Longest kill distance (meters) |
| `walkDistance` | float | Total walking distance (meters) |
| `rideDistance` | float | Total vehicle distance (meters) |
| `timeSurvived` | float | Total time survived (seconds) |
| `rankPoints` | float | Rank points (ranked mode) |

**Match IDs**: `attributes.seasonMatchIds.{gameMode}` — array of match IDs played in this season (up to 32 most recent within the last 14 days).

**Ranked Season Stats**:

```
GET https://api.pubg.com/shards/{shard}/players/{accountId}/seasons/{seasonId}/ranked
```

Ranked stats are available from Season 7 onward. The response has a similar structure with `rankedGameModeStats` instead of `gameModeStats`, and includes `currentRankPoint`, `bestRankPoint`, `currentTier`, `bestTier`, `currentSubTier`, `bestSubTier`.

**Batch Season Stats** (up to 10 players):

```
GET https://api.pubg.com/shards/{shard}/seasons/{seasonId}/gameMode/{gameMode}/players?filter[playerIds]={id1,id2,...}
```

**Pagination**: None.
**Incremental Sync Key**: `seasonId` — sync per season. Use `isCurrentSeason` flag to identify the current season.

---

### lifetime_stats

Retrieve a player's cumulative lifetime statistics across all seasons (excluding ranked). This uses `lifetime` as the season ID.

```
GET https://api.pubg.com/shards/{shard}/players/{accountId}/seasons/lifetime
```

**Authentication**: Required
**Rate Limit**: Counts against quota.

**Path Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `shard` | Yes | Platform shard |
| `accountId` | Yes | Player account ID |

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/players/account.0000000000000000000000000000000a/seasons/lifetime" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response**: Same structure as season stats, with `gameModeStats` aggregated across all time. The `seasonMatchIds` and match relationships contain the most recent matches (within the 14-day retention window).

**Response Type**: `playerSeason` (same as season stats)

**Key Difference from Season Stats**: The `id` for this resource uses the literal string `lifetime` as the season reference. Stats reflect cumulative totals. Match IDs in relationships still only go back 14 days.

**Incremental Sync Key**: No timestamp field on the stats themselves — lifetime stats are a snapshot. Track by `accountId` and compare total `roundsPlayed` to detect updates.

---

### leaderboards

Retrieve the top 500 players for a specific game mode in the current season. Leaderboards are updated every 2 hours. Pagination is supported (500 players per page).

```
GET https://api.pubg.com/shards/{shard}/leaderboards/{seasonId}/{gameMode}
```

**Authentication**: Required
**Rate Limit**: Counts against quota.

**Path Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `shard` | Yes | Platform shard |
| `seasonId` | Yes | Season ID (must be the current season) |
| `gameMode` | Yes | Game mode: `solo`, `solo-fpp`, `duo`, `duo-fpp`, `squad`, `squad-fpp` |

**Query Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `page[number]` | No | Page number (default: 1); each page returns up to 500 players |

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/leaderboards/division.bro.official.2018-09/squad?page[number]=1" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response**:

```json
{
  "data": {
    "type": "leaderboard",
    "id": "leaderboard-steam-squad-division.bro.official.2018-09",
    "attributes": {
      "shardId": "steam",
      "gameMode": "squad",
      "seasonId": "division.bro.official.2018-09"
    },
    "relationships": {
      "players": {
        "data": [
          { "type": "player", "id": "account...." }
        ]
      }
    }
  },
  "included": [
    {
      "type": "player",
      "id": "account.0000000000000000000000000000000a",
      "attributes": {
        "name": "shroud",
        "rank": 1,
        "stats": {
          "rankPoints": 6420.0,
          "wins": 120,
          "games": 450,
          "winRatio": 0.267,
          "averageDamage": 380.4,
          "kills": 1800,
          "killDeathRatio": 8.2,
          "kda": 9.1,
          "averageRank": 3.2
        }
      }
    }
  ]
}
```

**Key Response Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `included[].attributes.rank` | int | Leaderboard rank position |
| `included[].attributes.name` | string | Player display name |
| `included[].attributes.stats.rankPoints` | float | Rank points |
| `included[].attributes.stats.wins` | int | Season wins |
| `included[].attributes.stats.games` | int | Games played |
| `included[].attributes.stats.winRatio` | float | Win percentage |
| `included[].attributes.stats.averageDamage` | float | Average damage per match |
| `included[].attributes.stats.kills` | int | Total kills |
| `included[].attributes.stats.killDeathRatio` | float | K/D ratio |
| `included[].attributes.stats.kda` | float | K/D/A ratio |
| `included[].attributes.stats.averageRank` | float | Average placement |

**Pagination**:
- **Method**: Page-number-based
- **Page size**: 500 players per page
- **Parameter**: `page[number]`
- **Max players**: 500 total (only 1 page of results for the leaderboard)
- Leaderboards are only available for the **current season**.

**Incremental Sync Key**: No inherent timestamp. Poll periodically (leaderboards refresh every 2 hours). Use `rank` and `rankPoints` to detect changes.

---

### mastery

Retrieve a player's weapon mastery or survival mastery statistics.

#### Weapon Mastery

```
GET https://api.pubg.com/shards/{shard}/players/{accountId}/weapon_mastery
```

**Authentication**: Required
**Rate Limit**: Counts against quota.

**Path Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `shard` | Yes | Platform shard |
| `accountId` | Yes | Player account ID |

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/players/account.0000000000000000000000000000000a/weapon_mastery" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response**:

```json
{
  "data": {
    "type": "weaponMastery",
    "id": "account.0000000000000000000000000000000a",
    "attributes": {
      "weaponsummary": {
        "Item_Weapon_AK47_C": {
          "XPTotal": 54200,
          "LevelCurrent": 12,
          "TierCurrent": 2,
          "StatsTotal": {
            "Kills": 120,
            "HeadShots": 30,
            "Groggies": 15,
            "DamagePlayer": 42000.5,
            "LongestKill": 350.2,
            "MostKillsInAGame": 6
          },
          "OfficialStatsTotal": {
            "Kills": 80,
            "HeadShots": 20,
            "Groggies": 10,
            "DamagePlayer": 28000.3,
            "LongestKill": 350.2,
            "MostKillsInAGame": 6
          },
          "CompetitiveStatsTotal": {
            "Kills": 40,
            "HeadShots": 10,
            "Groggies": 5,
            "DamagePlayer": 14000.2,
            "LongestKill": 300.0,
            "MostKillsInAGame": 4
          }
        }
      }
    }
  }
}
```

**Key Response Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `attributes.weaponsummary` | object | Keys are weapon item IDs (e.g., `Item_Weapon_AK47_C`) |
| `weaponsummary.{weapon}.XPTotal` | int | Total XP earned with this weapon |
| `weaponsummary.{weapon}.LevelCurrent` | int | Current mastery level |
| `weaponsummary.{weapon}.TierCurrent` | int | Current mastery tier |
| `weaponsummary.{weapon}.StatsTotal` | object | All-time stats (official + competitive) |
| `weaponsummary.{weapon}.OfficialStatsTotal` | object | Official match stats only (from patch 18.2+) |
| `weaponsummary.{weapon}.CompetitiveStatsTotal` | object | Competitive match stats only (from patch 18.2+) |

> **Note**: `Medals` field within weapon mastery summaries was deprecated in v22.0.0. Weapon mastery data was reset in game update 18.2. `OfficialStatsTotal` and `CompetitiveStatsTotal` tracking began from patch 18.2.

#### Survival Mastery

```
GET https://api.pubg.com/shards/{shard}/players/{accountId}/survival_mastery
```

**Response**:

```json
{
  "data": {
    "type": "survivalMastery",
    "id": "account.0000000000000000000000000000000a",
    "attributes": {
      "xp": 120500,
      "level": 35,
      "tier": 5,
      "totalMatchesPlayed": 500,
      "stats": {
        "airDropsCalled": 12,
        "damageDealt": 150000.0,
        "damageFromBlueZone": 4500.2,
        "damageFromEnemies": 18000.3,
        "damageFromExplosions": 700.1,
        "distanceByFreefall": 25000.0,
        "distanceBySwimming": 3000.0,
        "distanceByVehicle": 180000.0,
        "distanceOnFoot": 600000.0,
        "enemiesKilled": 800,
        "healed": 5000.0,
        "objectsDestroyed": 50,
        "revived": 80,
        "teammateDamageDealt": 200.0,
        "throwablesThrown": 120,
        "top10Finishes": 150,
        "wins": 30
      }
    }
  }
}
```

**Key Field**: `attributes.level` and `attributes.tier` for tracking mastery progression. Added `tier` field in v22.0.2.

**Incremental Sync Key**: No timestamp field — poll by `accountId` and compare `xp` or `level` to detect updates.

---

### clans

Retrieve information about a clan by clan ID. The clan ID is available on player objects (added in v22.1.0).

```
GET https://api.pubg.com/shards/{shard}/clans/{clanId}
```

**Authentication**: Required
**Rate Limit**: Counts against quota.

**Path Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `shard` | Yes | Platform shard |
| `clanId` | Yes | Clan ID (obtained from `player.attributes.clanId`) |

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/clans/clan.abc123def456" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response**:

```json
{
  "data": {
    "type": "clan",
    "id": "clan.abc123def456",
    "attributes": {
      "clanId": "clan.abc123def456",
      "clanName": "ExampleClan",
      "clanTag": "EX",
      "clanLevel": 5,
      "clanMemberCount": 30
    }
  }
}
```

**Key Response Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `attributes.clanId` | string | Clan unique identifier |
| `attributes.clanName` | string | Full clan name |
| `attributes.clanTag` | string | Short clan tag (shown in-game) |
| `attributes.clanLevel` | int | Clan level |
| `attributes.clanMemberCount` | int | Number of members |

**Incremental Sync Key**: No timestamp — poll by `clanId` and track `clanLevel` or `clanMemberCount` for changes.

**Constraint**: Clan ID must be obtained from a player object. There is no endpoint to search or list all clans.

---

### samples

Retrieve a sample of random match IDs from the last 24 hours. Useful for building heatmaps or other aggregate analyses. Samples are updated every 24 hours per platform. This endpoint does **not** let you enumerate all matches.

```
GET https://api.pubg.com/shards/{shard}/samples
```

**Authentication**: Required
**Rate Limit**: Counts against quota.

**Query Parameters**:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `filter[createdAt-start]` | No | ISO 8601 datetime — fetch sample starting from this timestamp. If omitted, returns the most recent sample. |

**Example Request**:

```bash
curl "https://api.pubg.com/shards/steam/samples?filter[createdAt-start]=2018-01-01T00:00:00Z" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

**Response**:

```json
{
  "data": {
    "type": "sample",
    "id": "...",
    "attributes": {
      "createdAt": "2018-01-01T00:00:00Z",
      "titleId": "bluehole-pubg",
      "shardId": "steam"
    },
    "relationships": {
      "matches": {
        "data": [
          { "type": "match", "id": "match-uuid-1" },
          { "type": "match", "id": "match-uuid-2" },
          ...
        ]
      }
    }
  }
}
```

**Key Response Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `data.attributes.createdAt` | ISO 8601 datetime | When this sample batch was created |
| `data.relationships.matches.data` | array | List of match ID objects |

**Pagination**: None — all match IDs returned in a single response.

**Important Constraints**:
- Sample data is updated every **24 hours** per platform.
- Sample data **cannot and should not** be used to estimate the total number of matches or unique PUBG players.
- The sample represents a random subset, not a complete listing of all matches.
- Use `filter[createdAt-start]` to request historical samples (subject to the 14-day retention window).

**Incremental Sync Key**: `data.attributes.createdAt` — poll daily per platform and track the timestamp to avoid re-fetching.

---

### telemetry

Telemetry is not fetched via the standard API endpoint. Instead, the URL to a telemetry file is embedded inside a match object. The telemetry file itself is hosted on a CDN and does not require an API key.

#### Step 1 — Get the Match Object

```
GET https://api.pubg.com/shards/{shard}/matches/{matchId}
```

```bash
curl "https://api.pubg.com/shards/steam/matches/{matchId}" \
  -H "Authorization: Bearer <api-key>" \
  -H "Accept: application/vnd.api+json"
```

From the match response, find the asset reference in `data.relationships.assets.data`:

```json
"relationships": {
  "assets": {
    "data": [{
      "type": "asset",
      "id": "1ad97f85-cf9b-11e7-b84e-0a586460f004"
    }]
  }
}
```

#### Step 2 — Find the Asset in the `included` Array

Search `response.included` for the object where `type == "asset"` and `id` matches. Extract `attributes.URL`:

```json
{
  "type": "asset",
  "id": "1ad97f85-cf9b-11e7-b84e-0a586460f004",
  "attributes": {
    "URL": "https://telemetry-cdn.pubg.com/pc-krjp/2018/01/01/0/0/1ad97f85-cf9b-11e7-b84e-0a586460f004-telemetry.json",
    "createdAt": "2018-01-01T00:00:12Z",
    "description": "",
    "name": "telemetry"
  }
}
```

#### Step 3 — Download the Telemetry File

```
GET https://telemetry-cdn.pubg.com/{platform}/{year}/{month}/{day}/{hour}/{minute}/{assetId}-telemetry.json
```

```bash
curl --compressed \
  "https://telemetry-cdn.pubg.com/pc-krjp/2018/01/01/0/0/1ad97f85-cf9b-11e7-b84e-0a586460f004-telemetry.json" \
  -H "Accept: application/vnd.api+json"
```

**Authentication**: **No API key required** for the telemetry CDN download.
**Rate Limit**: The `/matches` endpoint (Step 1) is exempt from rate limiting. The CDN download (Step 3) has no rate limit.
**Compression**: Telemetry data is compressed with **gzip**. Use `Accept-Encoding: gzip` or `--compressed` with curl.

**Telemetry File Format**: A JSON array of event objects. Each event has:

| Field | Type | Description |
|-------|------|-------------|
| `_D` | ISO 8601 datetime | Event timestamp — key field for ordering events |
| `_T` | string | Event type name (e.g., `LogPlayerKill`, `LogMatchStart`) |
| `common` | object | Common game phase info (`isGame` float: 0=pregame, 0.1=airplane, 0.5=lobby, 1.0+=active) |

The remaining fields vary by event type. See [Telemetry Events Reference](#telemetry-events-reference) below.

**Incremental Sync Key**: `_D` (timestamp) on each event, plus the match's `createdAt` for match-level ordering.

---

## Telemetry Events Reference

Each event contains `_D` (timestamp), `_T` (type), and `common` (game phase). Additional fields per event type:

| Event Type | Key Fields |
|------------|-----------|
| `LogArmorDestroy` | `attackId`, `attacker` {Character}, `victim` {Character}, `damageTypeCategory`, `damageReason`, `damageCauserName`, `item` {Item}, `distance` |
| `LogBlackZoneEnded` | `survivors` [{Character}] |
| `LogCarePackageLand` | `itemPackage` {ItemPackage} |
| `LogCarePackageSpawn` | `itemPackage` {ItemPackage} |
| `LogCharacterCarry` | `character` {Character}, `carryState` |
| `LogEmPickupLiftOff` | `instigator` {Character}, `riders` [{Character}] |
| `LogGameStatePeriodic` | `gameState` {GameState} |
| `LogHeal` | `character` {Character}, `item` {Item}, `healamount` |
| `LogItemAttach` | `character` {Character}, `parentItem` {Item}, `childItem` {Item} |
| `LogItemDetach` | `character` {Character}, `parentItem` {Item}, `childItem` {Item} |
| `LogItemDrop` | `character` {Character}, `item` {Item} |
| `LogItemEquip` | `character` {Character}, `item` {Item} |
| `LogItemPickup` | `character` {Character}, `item` {Item} |
| `LogItemPickupFromCarepackage` | `character` {Character}, `item` {Item}, `carePackageUniqueId` |
| `LogItemPickupFromCustomPackage` | `character` {Character}, `item` {Item} |
| `LogItemPickupFromLootbox` | `character` {Character}, `item` {Item}, `ownerTeamId`, `creatorAccountId` |
| `LogItemPickupFromVehicleTrunk` | `character` {Character}, `vehicle` {Vehicle}, `item` {Item} |
| `LogItemPutToVehicleTrunk` | `character` {Character}, `vehicle` {Vehicle}, `item` {Item} |
| `LogItemUnequip` | `character` {Character}, `item` {Item} |
| `LogItemUse` | `character` {Character}, `item` {Item} |
| `LogMatchDefinition` | `MatchId`, `PingQuality`, `SeasonState` |
| `LogMatchEnd` | `characters` [{CharacterWrapper}], `gameResultOnFinished` {GameResultOnFinished} |
| `LogMatchStart` | `mapName`, `weatherId`, `characters` [{CharacterWrapper}], `cameraViewBehaviour`, `teamSize`, `isCustomGame`, `isEventMode`, `blueZoneCustomOptions` |
| `LogObjectDestroy` | `character` {Character}, `objectType`, `objectLocation` {Location} |
| `LogObjectInteraction` | `character` {Character}, `objectType`, `objectTypeStatus`, `objectTypeAdditionalInfo` |
| `LogParachuteLanding` | `character` {Character}, `distance` |
| `LogPhaseChange` | `phase`, `elapsedTime` |
| `LogPlayerAttack` | `attackId`, `fireWeaponStackCount`, `attacker` {Character}, `attackType`, `weapon` {Item}, `vehicle` {Vehicle} |
| `LogPlayerCreate` | `character` {Character} |
| `LogPlayerDestroyBreachableWall` | `attacker` {Character}, `weapon` {Item} |
| `LogPlayerDestroyProp` | `attacker` {Character}, `objectType`, `objectLocation` {Location} |
| `LogPlayerKill` (tournament only) | `attackId`, `killer` {Character}, `victim` {Character}, `assistant` {Character}, `dBNOId`, `damageReason`, `damageTypeCategory`, `damageCauserName`, `distance`, `victimGameResult` {GameResult}, `isThroughPenetrableWall` |
| `LogPlayerKillV2` | `attackId`, `dBNOId`, `victimGameResult` {GameResult}, `victim` {Character}, `victimWeapon`, `dBNOMaker` {Character}, `dBNODamageInfo` {DamageInfo}, `finisher` {Character}, `finishDamageInfo` {DamageInfo}, `killer` {Character}, `killerDamageInfo` {DamageInfo}, `assists_AccountId`, `teamKillers_AccountId`, `isSuicide` |
| `LogPlayerLogin` | `accountId` |
| `LogPlayerLogout` | `accountId` |
| `LogPlayerMakeGroggy` | `attackId`, `attacker` {Character}, `victim` {Character}, `damageReason`, `damageTypeCategory`, `damageCauserName`, `distance`, `isAttackerInVehicle`, `dBNOId`, `isThroughPenetrableWall` |
| `LogPlayerPosition` | `character` {Character}, `vehicle` {Vehicle}, `elapsedTime`, `numAlivePlayers` |
| `LogPlayerRedeploy` | `character` {Character} |
| `LogPlayerRedeployBRStart` | `characters` [{Character}] |
| `LogPlayerRevive` | `reviver` {Character}, `victim` {Character}, `dBNOId` |
| `LogPlayerTakeDamage` | `attackId`, `attacker` {Character}, `victim` {Character}, `damageTypeCategory`, `damageReason`, `damage`, `damageCauserName`, `isThroughPenetrableWall` |
| `LogPlayerUseFlareGun` | `attackId`, `fireWeaponStackCount`, `attacker` {Character}, `attackType`, `weapon` {Item} |
| `LogPlayerUseThrowable` | `attackId`, `fireWeaponStackCount`, `attacker` {Character}, `attackType`, `weapon` {Item} |
| `LogRedZoneEnded` | `drivers` [{Character}] |
| `LogSwimEnd` | `character` {Character}, `swimDistance`, `maxSwimDepthOfWater` |
| `LogSwimStart` | `character` {Character} |
| `LogVaultStart` | `character` {Character}, `isLedgeGrab` |
| `LogVehicleDamage` | `attackId`, `attacker` {Character}, `vehicle` {Vehicle}, `damageTypeCategory`, `damageCauserName`, `damage`, `distance` |
| `LogVehicleDestroy` | `attackId`, `attacker` {Character}, `vehicle` {Vehicle}, `damageTypeCategory`, `damageCauserName`, `distance` |
| `LogVehicleLeave` | `character` {Character}, `vehicle` {Vehicle}, `rideDistance`, `seatIndex`, `maxSpeed`, `fellowPassengers` [{Character}] |
| `LogVehicleRide` | `character` {Character}, `vehicle` {Vehicle}, `seatIndex`, `fellowPassengers` [{Character}] |
| `LogWeaponFireCount` | `character` {Character}, `weaponId`, `fireCount` |
| `LogWheelDestroy` | `attackId`, `attacker` {Character}, `vehicle` {Vehicle}, `damageTypeCategory`, `damageCauserName` |

---

## Telemetry Objects Reference

Objects referenced by `{ObjectName}` notation in event fields above:

### Character
| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Player display name |
| `teamId` | int | Team ID |
| `health` | float | Current health |
| `location` | Location | 3D position |
| `ranking` | int | Current ranking |
| `accountId` | string | Player account ID |
| `isInBlueZone` | bool | In blue zone |
| `isInRedZone` | bool | In red zone |
| `zone` | array | Array of region IDs occupied |

### CharacterWrapper
| Field | Type | Description |
|-------|------|-------------|
| `character` | Character | Character object |
| `primaryWeaponFirst` | string | Primary weapon slot 1 |
| `primaryWeaponSecond` | string | Primary weapon slot 2 |
| `secondaryWeapon` | string | Secondary weapon |
| `spawnKitIndex` | int | Spawn kit used |

### Common
| Field | Type | Description |
|-------|------|-------------|
| `isGame` | float | Game phase: 0=pregame, 0.1=airplane, 0.5=lobby, 1.0+=active gameplay |

### DamageInfo
| Field | Type | Description |
|-------|------|-------------|
| `damageReason` | string | Cause of damage |
| `damageTypeCategory` | string | Damage category |
| `damageCauserName` | string | Weapon/source name |
| `additionalInfo` | array | Additional string info |
| `distance` | float | Distance of damage event |
| `isThroughPenetrableWall` | bool | Penetrated wall |

### GameResult
| Field | Type | Description |
|-------|------|-------------|
| `rank` | int | Final placement |
| `gameResult` | string | Outcome string |
| `teamId` | int | Team ID |
| `stats` | Stats | Performance stats |
| `accountId` | string | Player account ID |

### GameResultOnFinished
| Field | Type | Description |
|-------|------|-------------|
| `results` | array of GameResult | Results for winning players |

### GameState
| Field | Type | Description |
|-------|------|-------------|
| `elapsedTime` | int | Elapsed match time (seconds) |
| `numAliveTeams` | int | Living teams |
| `numJoinPlayers` | int | Players who joined |
| `numStartPlayers` | int | Players at match start |
| `numAlivePlayers` | int | Living players |
| `safetyZonePosition` | object | Safe zone center |
| `safetyZoneRadius` | float | Safe zone radius |
| `poisonGasWarningPosition` | object | Blue zone center |
| `poisonGasWarningRadius` | float | Blue zone radius |
| `redZonePosition` | object | Red zone center |
| `redZoneRadius` | float | Red zone radius |
| `blackZonePosition` | object | Black zone center |
| `blackZoneRadius` | float | Black zone radius |

### Item
| Field | Type | Description |
|-------|------|-------------|
| `itemId` | string | Item identifier |
| `stackCount` | int | Stack count |
| `category` | string | Item category |
| `subCategory` | string | Item sub-category |
| `attachedItems` | array | Attached item IDs |

### ItemPackage
| Field | Type | Description |
|-------|------|-------------|
| `itemPackageId` | string | Package identifier |
| `location` | Location | 3D position |
| `items` | array of Item | Contents |

### Location
| Field | Type | Description |
|-------|------|-------------|
| `x` | float | X coordinate (centimeters; origin at map top-left) |
| `y` | float | Y coordinate |
| `z` | float | Z coordinate (elevation) |

### Stats (player performance in a match)
| Field | Type | Description |
|-------|------|-------------|
| `killCount` | int | Kills |
| `distanceOnFoot` | float | Distance walked |
| `distanceOnSwim` | float | Distance swum |
| `distanceOnVehicle` | float | Distance in vehicle |
| `distanceOnParachute` | float | Parachute distance |
| `distanceOnFreefall` | float | Freefall distance |

### Vehicle
| Field | Type | Description |
|-------|------|-------------|
| `vehicleType` | string | Vehicle type |
| `vehicleId` | string | Vehicle identifier |
| `vehicleUniqueId` | int | Unique instance ID |
| `healthPercent` | float | Health percentage |
| `feulPercent` | float | Fuel percentage (note: known typo in API) |
| `altitudeAbs` | float | Absolute altitude |
| `altitudeRel` | float | Relative altitude |
| `velocity` | float | Speed |
| `seatIndex` | int | Seat position |
| `isWheelsInAir` | bool | Airborne |
| `isInWaterVolume` | bool | In water |
| `isEngineOn` | bool | Engine running |

---

## Known Issues

The following bugs are documented by PUBG and exist in the API data. They will be present in raw telemetry or match data:

| Issue | Description |
|-------|-------------|
| Typo: `Cowbar_C` | Item ID for crowbar is misspelled |
| Typo: `ItemAmmo12GuageC` | Should be "Gauge" |
| Typo: `Vehicle.FeulPercent` | Should be "Fuel" — present in Vehicle object |
| Typo: `Carapackage_RedBox_C` | Misspelled in `itemPackageId` |
| Typo: `SimlateAIBeKilled` | Misspelled in `damageReason` |
| Missing season data | PC data from seasons prior to `division.bro.official.2018-04` is unavailable |
| Throwable logging gap | Grenade/smoke bomb drop counts may exceed pickup counts due to incomplete event logging |
| Weapon categorization | Scorpion and Flare Gun are incorrectly categorized as "Main" instead of "Handgun" in telemetry |
| Distance metric discrepancy | `participant.attributes.stats` distances may differ from `GameResult` object — use `GameResult` (from `LogPlayerKillV2` and `LogMatchEnd`) as authoritative |

Report undocumented bugs to: **pubgapi@pubg.com**

---

## Summary Table: Endpoints and Sync Strategy

| Table | Endpoint | Rate Limited | Auth Required | Incremental Key | Pagination |
|-------|----------|-------------|---------------|-----------------|------------|
| `players` | `GET /shards/{shard}/players` | Yes | Yes | Match IDs in relationships | None (batch up to 10) |
| `matches` | `GET /shards/{shard}/matches/{matchId}` | **No** | Yes | `attributes.createdAt` | None (1 match/request) |
| `season_stats` | `GET /shards/{shard}/players/{accountId}/seasons/{seasonId}` | Yes | Yes | `seasonId` | None |
| `lifetime_stats` | `GET /shards/{shard}/players/{accountId}/seasons/lifetime` | Yes | Yes | Poll by `accountId` | None |
| `leaderboards` | `GET /shards/{shard}/leaderboards/{seasonId}/{gameMode}` | Yes | Yes | `rank` / `rankPoints` | `page[number]` (500/page) |
| `mastery` (weapon) | `GET /shards/{shard}/players/{accountId}/weapon_mastery` | Yes | Yes | `XPTotal` | None |
| `mastery` (survival) | `GET /shards/{shard}/players/{accountId}/survival_mastery` | Yes | Yes | `xp` / `level` | None |
| `clans` | `GET /shards/{shard}/clans/{clanId}` | Yes | Yes | `clanLevel` | None |
| `samples` | `GET /shards/{shard}/samples` | Yes | Yes | `attributes.createdAt` | None |
| `telemetry` | CDN URL from match asset | **No** (CDN) | **No** (CDN) | `_D` per event | None (full file) |
