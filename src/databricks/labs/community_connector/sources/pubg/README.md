# PUBG Community Connector

Ingests data from the [PUBG Developer API](https://documentation.pubg.com/en/introduction.html) into Databricks using the Lakeflow Community Connector framework. The connector surfaces nine tables covering players, matches, per-season and lifetime statistics, leaderboards, weapon and survival mastery, clans, match samples, and raw telemetry events.

---

## Authentication

### Getting a PUBG API key

1. Register at [developer.pubg.com](https://developer.pubg.com).
2. Create an app under **MY APPS**. An API key (a JSON Web Token) is generated automatically — you do not construct it manually.
3. The free tier allows **10 requests per minute**. To request a higher limit, submit details about your use case (user count, request frequency, caching strategy, and a working sample) through the same dashboard.

The API key is passed as a connector option:

```python
options = {
    "api_key": "<your-jwt>",
    "shard": "steam",
}
```

The key must never be stored client-side. Store it in a Databricks secret and reference it from your pipeline configuration.

### The `shard` parameter

All PUBG API endpoints are scoped to a platform **shard**. Every request you make goes to `https://api.pubg.com/shards/{shard}/...`. Choose the shard that matches the player platform you want to ingest:

| Value | Platform |
|-------|----------|
| `steam` | PC (Steam) |
| `psn` | PlayStation Network |
| `xbox` | Xbox |
| `kakao` | Kakao (Korea PC) |
| `stadia` | Google Stadia |
| `console` | Cross-platform console |

Data from one shard is entirely separate from another. A player account on `steam` is a different resource from a player account on `psn`. You must instantiate a separate connector (or pipeline source) for each platform you want to cover.

---

## Tables

The connector exposes the following tables. Pass `table_options` as a dictionary alongside the table name when calling `read_table`.

---

### `players`

Retrieves player objects by display name or account ID. Each record includes the player's ban status, clan membership, and a list of recent match IDs (up to 14 days of history).

**Required options** — provide exactly one:

| Option | Description |
|--------|-------------|
| `player_names` | Comma-separated display names, up to 10 (e.g., `"shroud,drdisrespect"`) |
| `player_ids` | Comma-separated account IDs, up to 10 (e.g., `"account.abc,account.def"`) |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Player account ID — use this as `player_ids` in downstream tables |
| `name` | string | Display name |
| `shardId` | string | Platform shard |
| `banType` | string | Ban status: `Innocent`, `Temp`, or `Permanent` |
| `clanId` | string | Clan ID; empty string if the player has no clan |
| `matchIds` | array[string] | Recent match IDs (within the 14-day retention window) |

**Sync behavior:** Snapshot. The full player record is returned on every run. No cursor is maintained.

---

### `matches`

Retrieves full match objects by match UUID. Each record includes match metadata, all rosters (teams), all participant stats, and the CDN URL for the match's telemetry file.

**Required options:**

| Option | Description |
|--------|-------------|
| `match_ids` | Comma-separated match UUIDs (obtain from `players.matchIds` or `samples.matchIds`) |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Match UUID — primary key |
| `createdAt` | string | ISO 8601 match start timestamp — incremental cursor |
| `duration` | int | Match length in seconds |
| `matchType` | string | `official`, `custom`, `seasonal`, or `event` |
| `gameMode` | string | `solo`, `solo-fpp`, `duo`, `duo-fpp`, `squad`, or `squad-fpp` |
| `mapName` | string | Internal map name (e.g., `Desert_Main`, `Baltic_Main`) |
| `isCustomMatch` | bool | Whether this is a custom lobby match |
| `telemetryUrl` | string | CDN URL of the telemetry JSON file for this match |
| `rosters` | array | Teams, each with `rank`, `teamId`, `won`, and a list of participant IDs |
| `participants` | array | Per-player in-match stats (kills, damage, distance, placement, etc.) |

**Sync behavior:** Append. The connector tracks processed match IDs in the offset. Match IDs that return HTTP 404 (expired beyond the 14-day window) are silently skipped and marked as seen so they are not retried.

---

### `season_stats`

Retrieves per-player aggregate statistics for a specific season, broken down by game mode. Also exposes the list of match IDs played in the season (up to the 32 most recent within the 14-day retention window).

**Required options:**

| Option | Description |
|--------|-------------|
| `player_ids` | Comma-separated account IDs |

**Optional options:**

| Option | Default | Description |
|--------|---------|-------------|
| `season_id` | Current season | Season ID (e.g., `division.bro.official.2018-09`). If omitted, the connector fetches the current season automatically. |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `accountId` | string | Player account ID — composite primary key (part 1) |
| `seasonId` | string | Season ID — composite primary key (part 2) |
| `gameModeStats` | struct | Stats per game mode (`solo`, `solo-fpp`, `duo`, `duo-fpp`, `squad`, `squad-fpp`), each containing `roundsPlayed`, `wins`, `kills`, `damageDealt`, `rankPoints`, and 25+ additional fields |
| `bestRankPoint` | float | Highest rank point achieved in the season |
| `seasonMatchIds` | struct | Arrays of match IDs played per game mode in this season |

**Sync behavior:** Snapshot. Records are keyed by `(accountId, seasonId)`. Passing a different `season_id` on subsequent runs produces a new set of records.

---

### `lifetime_stats`

Retrieves cumulative all-time statistics per player, aggregated across every season (excluding ranked). Uses the same `gameModeStats` structure as `season_stats`.

**Required options:**

| Option | Description |
|--------|-------------|
| `player_ids` | Comma-separated account IDs |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `accountId` | string | Player account ID — primary key |
| `gameModeStats` | struct | All-time stats per game mode (same fields as `season_stats`) |
| `bestRankPoint` | float | All-time highest rank point |

**Sync behavior:** Snapshot. There is no timestamp on lifetime stats; the full record is returned on every run per player.

---

### `leaderboards`

Retrieves the top-500 ranked players for each requested game mode in the current (or specified) season. Leaderboards are refreshed by PUBG every 2 hours.

**Optional options:**

| Option | Default | Description |
|--------|---------|-------------|
| `season_id` | Current season | Season ID. Leaderboards are only available for the current season; requests for past seasons return no data. |
| `game_modes` | `solo,solo-fpp,duo,duo-fpp,squad,squad-fpp` | Comma-separated list of game modes to fetch |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `leaderboardId` | string | Composite identifier for the season + game mode leaderboard — composite primary key (part 1) |
| `accountId` | string | Player account ID — composite primary key (part 2) |
| `seasonId` | string | Season ID |
| `gameMode` | string | Game mode |
| `rank` | int | Leaderboard position (1 = top player) |
| `name` | string | Player display name |
| `stats.rankPoints` | float | Rank points |
| `stats.wins` | int | Season wins |
| `stats.games` | int | Games played |
| `stats.winRatio` | float | Win percentage |
| `stats.averageDamage` | float | Average damage per match |
| `stats.kills` | int | Total kills |
| `stats.killDeathRatio` | float | K/D ratio |
| `stats.kda` | float | K/D/A ratio |
| `stats.averageRank` | float | Average placement |

**Sync behavior:** Snapshot. The full leaderboard is fetched on every run; no cursor is maintained. Schedule this table for periodic refresh (every 2 hours at most) to track ranking changes.

---

### `mastery`

Retrieves weapon mastery and/or survival mastery statistics per player. Each player produces up to two rows: one with `masteryType = "weapon"` and one with `masteryType = "survival"`.

**Required options:**

| Option | Description |
|--------|-------------|
| `player_ids` | Comma-separated account IDs |

**Optional options:**

| Option | Default | Description |
|--------|---------|-------------|
| `mastery_types` | `weapon,survival` | Comma-separated mastery types to fetch |

**Key fields — weapon mastery** (`masteryType = "weapon"`):

| Field | Type | Description |
|-------|------|-------------|
| `accountId` | string | Player account ID — composite primary key (part 1) |
| `masteryType` | string | `"weapon"` — composite primary key (part 2) |
| `weapons` | array | One entry per weapon (e.g., `Item_Weapon_AK47_C`) with `XPTotal`, `LevelCurrent`, `TierCurrent`, and stat breakdowns for `StatsTotal`, `OfficialStatsTotal`, and `CompetitiveStatsTotal` |

**Key fields — survival mastery** (`masteryType = "survival"`):

| Field | Type | Description |
|-------|------|-------------|
| `accountId` | string | Player account ID — composite primary key (part 1) |
| `masteryType` | string | `"survival"` — composite primary key (part 2) |
| `xp` | int | Total survival XP |
| `level` | int | Current survival mastery level |
| `tier` | int | Current survival mastery tier |
| `totalMatchesPlayed` | int | Total matches contributing to survival mastery |
| `survivalStats` | struct | Lifetime survival metrics: `distanceOnFoot`, `distanceByVehicle`, `enemiesKilled`, `top10Finishes`, `wins`, and more |

**Sync behavior:** Snapshot. Records are keyed by `(accountId, masteryType)` and re-fetched on every run.

---

### `clans`

Retrieves basic clan information by clan ID.

**Required options:**

| Option | Description |
|--------|-------------|
| `clan_ids` | Comma-separated clan IDs (obtain from `players.clanId`) |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Clan ID — primary key |
| `clanName` | string | Full clan name |
| `clanTag` | string | Short in-game tag |
| `clanLevel` | int | Clan level |
| `clanMemberCount` | int | Current member count |

**Sync behavior:** Snapshot. Full clan record is returned on every run.

---

### `samples`

Returns a randomly sampled batch of match IDs from the last 24 hours. The sample is updated once per day per platform. This table is useful for building aggregate analyses (heatmaps, meta statistics) without needing a specific list of match IDs upfront.

**Optional options:**

| Option | Default | Description |
|--------|---------|-------------|
| `created_at_start` | Most recent sample | ISO 8601 datetime to request a historical sample. Samples older than 14 days are not available. |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Sample batch ID — primary key |
| `createdAt` | string | ISO 8601 timestamp when this sample batch was generated — incremental cursor |
| `shardId` | string | Platform shard |
| `matchIds` | array[string] | Random subset of match IDs from the sample window |

**Sync behavior:** Append. The connector uses `createdAt` as a cursor and only emits a new record when a newer sample batch is available. The sample does not represent all matches played and must not be used to estimate total match counts.

---

### `telemetry`

Downloads and flattens the raw per-event telemetry files for one or more matches. Telemetry files are hosted on the PUBG CDN (no API key required for the download step) and contain a dense time-series of every in-match event.

**Required options:**

| Option | Description |
|--------|-------------|
| `match_ids` | Comma-separated match UUIDs whose telemetry to download |

**Key fields:**

| Field | Type | Description |
|-------|------|-------------|
| `matchId` | string | Match UUID — composite primary key (part 1) |
| `eventIndex` | int | 0-based position of the event within the telemetry file — composite primary key (part 2) |
| `_D` | string | ISO 8601 event timestamp — incremental cursor |
| `_T` | string | Event type name (e.g., `LogPlayerKill`, `LogMatchStart`, `LogPlayerPosition`) |
| `common` | map[string, string] | Game phase context (`isGame`: `0`=pregame, `0.1`=airplane, `0.5`=lobby, `1.0+`=active gameplay) |
| `payload` | string | Full event object serialized as a JSON string — parse this for event-specific fields |

Common event types include `LogMatchStart`, `LogMatchEnd`, `LogPlayerKill`, `LogPlayerKillV2`, `LogPlayerTakeDamage`, `LogPlayerMakeGroggy`, `LogPlayerAttack`, `LogPlayerPosition`, `LogGameStatePeriodic`, `LogItemPickup`, `LogVehicleRide`, and many others. Full event schemas are documented in the [PUBG API reference](https://documentation.pubg.com/en/introduction.html).

**Sync behavior:** Append. The connector tracks processed match IDs in the offset. For each new match ID it first fetches the match object to obtain the telemetry CDN URL, then downloads and flattens the event array. Match IDs that have expired (HTTP 404) are skipped silently.

---

## Rate limits

| Tier | Limit |
|------|-------|
| Free (default) | 10 requests per minute |
| Custom (approved) | Higher limits granted on application via the developer portal |

Exceeding the limit returns HTTP 429. The connector handles this automatically with exponential backoff, respecting the `X-RateLimit-Reset` header to wait exactly until the window resets before retrying.

**Endpoints exempt from rate limiting:**

- `GET /shards/{shard}/matches/{matchId}` — match fetches do not count against the quota.
- Telemetry CDN downloads (`telemetry-cdn.pubg.com`) — no rate limit and no API key required.

A typical player ingestion workflow (fetch player object, then season stats) consumes two quota requests per player. Plan your pipeline schedule accordingly; at 10 req/min a pipeline covering 300 players will take approximately 1 minute just for the API calls.

---

## Known limitations

**Match data retention — 14 days.** The PUBG API does not store match data beyond 14 days from the match creation time. Any match ID passed to the `matches` or `telemetry` tables that refers to a match older than 14 days returns HTTP 404 and is silently skipped. Player match history (`players.matchIds`) and season match lists (`season_stats.seasonMatchIds`) are subject to the same window.

**Leaderboards are current-season only.** The `leaderboards` table only returns data for the active season. Passing a past `season_id` will result in no rows being returned (the API returns 404 for those requests). If you want to preserve historical leaderboard snapshots, schedule regular ingestion runs during the season.

**Leaderboard availability varies by shard.** Not all shards expose leaderboards. Some shards (notably `console`) may return HTTP 400 for leaderboard requests. The connector skips those responses rather than raising an error.

**Clan IDs must be discovered via player lookups.** There is no endpoint to search or list all clans. `clan_ids` must be obtained from `players.clanId` fields before you can query the `clans` table.

**Season stats use platform shards for modern seasons.** For PC and PSN seasons after `division.bro.official.2018-09`, you must use a platform shard (`steam`, `psn`) rather than a legacy platform-region shard. The connector always uses the `shard` you configure at the connector level.

**Weapon mastery stats were reset at patch 18.2.** The `OfficialStatsTotal` and `CompetitiveStatsTotal` breakdowns in the `mastery` table only reflect data from patch 18.2 onward. The `StatsTotal` field aggregates both periods. The `Medals` field was deprecated in v22.0.0 and is no longer returned by the API.

**Samples are not exhaustive.** The `samples` table returns a random subset of matches, not a complete listing. It is updated once every 24 hours per shard. Do not use sample data to estimate total match volume or unique player counts.
