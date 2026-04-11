-- Sample Queries for Microsoft Teams Connector Tables
-- These queries show the 10 most recent records from each table
-- Use these instead of table sampling to see actual latest data

-- =============================================================================
-- 1. TEAMS - Top 10 most recently created teams
-- =============================================================================
SELECT
    id,
    displayName,
    description,
    visibility,
    isArchived,
    createdDateTime
FROM main.teams_data.lakeflow_connector_teams
ORDER BY createdDateTime DESC
LIMIT 10;

-- =============================================================================
-- 2. CHANNELS - Top 10 most recently created channels
-- =============================================================================
SELECT
    id,
    team_id,
    displayName,
    description,
    membershipType,
    createdDateTime,
    isArchived
FROM main.teams_data.lakeflow_connector_channels
ORDER BY createdDateTime DESC
LIMIT 10;

-- =============================================================================
-- 3. MEMBERS - Top 10 most recent team memberships
-- =============================================================================
SELECT
    id,
    team_id,
    displayName,
    email,
    userId,
    roles,
    visibleHistoryStartDateTime
FROM main.teams_data.lakeflow_connector_members
ORDER BY visibleHistoryStartDateTime DESC
LIMIT 10;

-- =============================================================================
-- 4. MESSAGES - Top 10 most recently modified messages (CDC cursor field)
-- =============================================================================
SELECT
    id,
    team_id,
    channel_id,
    subject,
    LEFT(CAST(body.content AS STRING), 100) as content_preview,
    messageType,
    createdDateTime,
    lastModifiedDateTime,
    deletedDateTime,
    importance
FROM main.teams_data.lakeflow_connector_messages
ORDER BY lastModifiedDateTime DESC
LIMIT 10;

-- =============================================================================
-- 5. MESSAGE_REPLIES - Top 10 most recently modified replies (CDC cursor field)
-- =============================================================================
SELECT
    id,
    parent_message_id,
    team_id,
    channel_id,
    LEFT(CAST(body.content AS STRING), 100) as content_preview,
    messageType,
    createdDateTime,
    lastModifiedDateTime,
    deletedDateTime
FROM main.teams_data.lakeflow_connector_message_replies
ORDER BY lastModifiedDateTime DESC
LIMIT 10;

-- =============================================================================
-- BONUS: Summary statistics across all tables
-- =============================================================================

-- Table row counts
SELECT 'teams' as table_name, COUNT(*) as row_count
FROM main.teams_data.lakeflow_connector_teams
UNION ALL
SELECT 'channels', COUNT(*)
FROM main.teams_data.lakeflow_connector_channels
UNION ALL
SELECT 'members', COUNT(*)
FROM main.teams_data.lakeflow_connector_members
UNION ALL
SELECT 'messages', COUNT(*)
FROM main.teams_data.lakeflow_connector_messages
UNION ALL
SELECT 'message_replies', COUNT(*)
FROM main.teams_data.lakeflow_connector_message_replies
ORDER BY table_name;

-- Messages per channel (with team and channel names)
SELECT
    t.displayName as team_name,
    c.displayName as channel_name,
    COUNT(m.id) as message_count,
    MAX(m.lastModifiedDateTime) as latest_message,
    MIN(m.createdDateTime) as oldest_message
FROM main.teams_data.lakeflow_connector_messages m
LEFT JOIN main.teams_data.lakeflow_connector_teams t ON m.team_id = t.id
LEFT JOIN main.teams_data.lakeflow_connector_channels c ON m.channel_id = c.id
GROUP BY t.displayName, c.displayName
ORDER BY message_count DESC
LIMIT 20;

-- Messages with replies (find conversations)
SELECT
    m.id as message_id,
    m.subject,
    LEFT(CAST(m.body.content AS STRING), 80) as message_content,
    COUNT(r.id) as reply_count,
    m.createdDateTime,
    MAX(r.lastModifiedDateTime) as latest_reply
FROM main.teams_data.lakeflow_connector_messages m
INNER JOIN main.teams_data.lakeflow_connector_message_replies r
    ON m.id = r.parent_message_id
GROUP BY m.id, m.subject, m.body.content, m.createdDateTime
ORDER BY reply_count DESC, latest_reply DESC
LIMIT 10;
