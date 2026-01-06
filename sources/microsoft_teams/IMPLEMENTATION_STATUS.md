# Microsoft Teams Connector Performance Optimization - Implementation Status

## Overview

Implementation of Delta API + ThreadPoolExecutor + Slow-lane reactions based on community feedback to optimize the connector from O(all_messages) to O(changed_messages).

---

## ✅ Phase 1: COMPLETED & PUSHED (Commit: 80230a4)

### What Was Implemented

#### 1. **Message Reactions Table** ✅
- Added new `message_reactions` table to `list_tables()`
- Created schema with proper fields (message_id, team_id, channel_id, reaction_type, user info, polled_at)
- Added metadata configuration in `_object_config`
- Primary keys: `["message_id", "reaction_type", "user_id", "polled_at"]`

#### 2. **Slow-Lane Polling Implementation** ✅
- Implemented `_read_message_reactions()` method with ThreadPoolExecutor
- Added `_fetch_recent_message_ids()` helper (fetches messages from last N days)
- Added `_fetch_message_reactions()` helper (fetches reactions for single message)
- Parallel polling using configurable thread workers

#### 3. **ThreadPoolExecutor Import** ✅
- Added `from concurrent.futures import ThreadPoolExecutor, as_completed`

#### 4. **Configuration Options** ✅
- `reaction_poll_window_days`: Days of messages to poll (default: 7)
- `reaction_poll_batch_size`: Max messages per run (default: 100)
- `max_concurrent_threads`: Parallel threads (default: 10)
- Updated `externalOptionsAllowList` in `uc_connection_example.sh`

### Architecture Benefits

✅ **Append-only design** - No MERGE/UPDATE operations needed
✅ **Parallel execution** - ThreadPoolExecutor for concurrent message polling
✅ **Configurable** - Adjustable polling window and batch size
✅ **Auto-discovery** - Works with `fetch_all_teams`/`fetch_all_channels`

---

## 🚧 Phase 2: TODO - Delta API Implementation

### What Needs to Be Implemented

#### 1. **Delta API for Messages**
Create `_read_messages_delta()` method:
```python
def _read_messages_delta(self, start_offset, table_options):
    """Use Microsoft Graph Delta API for efficient incremental sync"""
    delta_link = start_offset.get("deltaLink") if start_offset else None

    if delta_link:
        url = delta_link  # Use saved deltaLink for incremental sync
    else:
        # Initial sync
        url = f"{self.base_url}/teams/{team_id}/channels/{channel_id}/messages/delta"

    # Fetch and process messages
    # Handle @odata.deltaLink (end of sync)
    # Handle @odata.nextLink (pagination)
    # Handle @removed marker for deleted messages

    return iter(records), {"deltaLink": delta_link}
```

#### 2. **Delta API for Replies**
Create `_read_message_replies_delta()` method:
- Same pattern as messages delta
- Endpoint: `/teams/{id}/channels/{id}/messages/{id}/replies/delta`

#### 3. **Route to Delta Methods**
Modify `_read_messages()` and `_read_message_replies()`:
```python
def _read_messages(self, start_offset, table_options):
    use_delta_api = table_options.get("use_delta_api", "true").lower() == "true"

    if use_delta_api:
        return self._read_messages_delta(start_offset, table_options)
    else:
        return self._read_messages_legacy(start_offset, table_options)
```

#### 4. **Rename Existing Methods**
- `_read_messages()` → `_read_messages_legacy()`
- `_read_message_replies()` → `_read_message_replies_legacy()`

---

## 🚧 Phase 3: TODO - Parallel Reply Fetching

### What Needs to Be Implemented

#### 1. **Extract Reply Fetching Logic**
Create helper method:
```python
def _fetch_replies_for_message(self, team_id, channel_id, message_id, params, max_pages, cursor):
    """Fetch replies for a single message (for parallel execution)"""
    url = f"{self.base_url}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies"
    # ... fetch logic
    return reply_records, max_modified_for_this_message
```

#### 2. **Update _read_message_replies() with ThreadPoolExecutor**
```python
def _read_message_replies_legacy(self, start_offset, table_options):
    # ... existing setup code ...

    max_workers = int(table_options.get("max_concurrent_threads", 10))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                self._fetch_replies_for_message,
                team_id, channel_id, message_id,
                params, max_pages, cursor
            ): (team_id, channel_id, message_id)
            for team_id, channel_id, message_id in all_team_channel_message_triples
        }

        for future in as_completed(futures):
            reply_records, reply_max_modified = future.result()
            records.extend(reply_records)
            # Track max_modified
```

---

## 📝 Phase 4: TODO - Documentation Updates

### README.md Updates Needed

#### 1. **Add message_reactions Table Documentation**
Under "Supported Tables" section, add:

```markdown
### 6. message_reactions (Slow-Lane Polling)

Reactions to messages, tracked separately via slow-lane polling.

| Field | Type | Description |
|-------|------|-------------|
| message_id | String | Parent message ID |
| team_id | String | Team ID |
| channel_id | String | Channel ID |
| reaction_type | String | Emoji or reaction type (👍, ❤️, etc.) |
| display_name | String | Human-readable reaction name |
| created_datetime | String | When reaction was added |
| user_id | String | User who reacted |
| polled_at | String | When we polled this message |

**Ingestion Type:** Snapshot (append-only with polling timestamp)
**Primary Key:** (message_id, reaction_type, user_id, polled_at)
**Required Table Options:** team_id, channel_id (or fetch_all flags)
**Graph API Endpoint:** `GET /teams/{id}/channels/{id}/messages/{id}`
```

#### 2. **Add Performance Optimization Section**
```markdown
## Performance Optimizations

### Delta API (Fast-Lane) 🚀
The connector uses Microsoft Graph Delta API for efficient incremental sync:
- ✅ Server-side filtering (only changed messages returned)
- ✅ O(changed_messages) instead of O(all_messages)
- ✅ Handles message deletions with @removed marker
- ⚠️ Does NOT track reaction changes

**Enable:** `use_delta_api="true"` (default: true)

### Concurrent Thread Fetching ⚡
Message replies and reactions are fetched in parallel using ThreadPoolExecutor:
- Configure threads: `max_concurrent_threads` (default: 10)
- Significant speedup for channels with many threaded discussions
- Rate limiting respected with exponential backoff

### Slow-Lane Polling for Reactions 🐢
Separate `message_reactions` table polls recent messages to detect reaction changes:
- Configure window: `reaction_poll_window_days` (default: 7)
- Configure batch size: `reaction_poll_batch_size` (default: 100)
- Append-only design (no MERGE/UPDATE operations)
- Only polls messages from the last N days (configurable)
```

#### 3. **Update Table Configuration Options**
Add to the table configuration options section:

```markdown
### Performance Tuning Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `use_delta_api` | Boolean | `true` | Use Delta API for server-side filtering |
| `max_concurrent_threads` | Integer | `10` | Parallel threads for fetching |
| `reaction_poll_window_days` | Integer | `7` | Days of messages to poll for reactions |
| `reaction_poll_batch_size` | Integer | `100` | Max messages to poll per run |
```

### sample-ingest.py Updates Needed

Add message_reactions table example:

```python
# Message Reactions (Slow-Lane Polling) - NEW!
{
    "table": {
        "source_table": "message_reactions",
        "destination_catalog": "main",
        "destination_schema": "teams_data",
        "destination_table": "lakeflow_connector_message_reactions",
        "table_configuration": {
            **creds,
            "fetch_all_teams": "true",
            "fetch_all_channels": "true",
            "reaction_poll_window_days": "7",  # Poll last 7 days
            "reaction_poll_batch_size": "100",  # 100 messages per run
            "max_concurrent_threads": "10",  # 10 parallel threads
        }
    }
},
```

---

## 🔄 Phase 5: TODO - Regenerate Source

### Files to Regenerate

1. **_generated_microsoft_teams_python_source.py**
   - Run the source generation script
   - This file should mirror microsoft_teams.py

---

## 📊 Expected Performance Improvements

### Current Implementation (Before Optimization)
- **Messages:** O(all_messages) - fetches ALL messages every run, filters client-side
- **Replies:** Sequential fetching - one message at a time
- **Reactions:** Included inline with messages (captured automatically)

### After Full Implementation
- **Messages:** O(changed_messages) - only fetches new/modified messages from server
- **Replies:** Parallel fetching - 10x faster with ThreadPoolExecutor
- **Reactions:** Separate table - only polls recent messages (last 7 days by default)

### Performance Comparison

| Scenario | Before | After | Speedup |
|----------|--------|-------|---------|
| Large channel (10K messages, 10 changed) | Fetch 10,000 | Fetch 10 | **1000x** |
| Threaded channel (100 messages with replies) | Sequential | Parallel (10 threads) | **10x** |
| Reaction tracking (1000 recent messages) | Re-fetch all | Poll 1000 | **10x** |

---

## 🚀 Next Steps

To complete the implementation:

1. **Implement Delta API methods** (Phase 2)
2. **Add parallel reply fetching** (Phase 3)
3. **Update documentation** (Phase 4)
4. **Regenerate source file** (Phase 5)
5. **Test with real Teams data**
6. **Commit and push final changes**

---

## 📝 Summary

**✅ DONE (Phase 1):**
- Message reactions table with slow-lane polling
- ThreadPoolExecutor infrastructure
- Configuration options

**🚧 TODO (Phases 2-5):**
- Delta API implementation
- Parallel reply fetching
- Documentation updates
- Source regeneration

**Estimated Completion:** ~2-3 hours of focused work

**Benefits:** 10-1000x performance improvement for large Teams deployments
