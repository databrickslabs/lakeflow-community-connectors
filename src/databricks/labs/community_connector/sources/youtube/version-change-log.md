# `youtube` connector version history

Newest releases first. Versions match `version` in `sources/youtube/pyproject.toml`. See CONTRIBUTING.md (Versioning) for when to bump and how to write an entry.

## 0.1.0 — 2026-11-03 - INITIAL

### Changes
- First version published as a prebuilt wheel under `wheelhouse/`.
- **Source:** YouTube.
- **Connection:** API key, or Google OAuth (U2M).
- **Tables (9):** `activities`, `channels`, `comment_threads`, `playlist_items`, `playlists`, `search`, `subscriptions`, `video_categories` and `videos`.
- **Ingestion types:** `snapshot` (9).

### Compatibility
- Requires framework `lakeflow-community-connectors>=0.1.0`.

### Upgrade instructions
- None.

### Rollback
- Not applicable.

### References
- PR: https://github.com/databrickslabs/lakeflow-community-connectors/pull/295
