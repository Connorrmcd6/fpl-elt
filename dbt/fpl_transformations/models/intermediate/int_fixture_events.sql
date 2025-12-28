{{ config(materialized='table', database='intermediate') }}

WITH fixtures AS (
    SELECT * FROM {{ ref('stg_fixtures') }}
),

players_enriched AS (
    SELECT * FROM {{ ref('int_players_enriched') }}
),

-- Unnest the stats array to get each stat type
stat_types AS (
    SELECT
        f.fixture_id,
        f.team_a,
        f.team_h,
        f.kickoff_time,
        f.finished,
        JSONExtractString(stat, 'identifier') as stat_identifier,
        JSONExtractArrayRaw(stat, 'a') as team_a_events,
        JSONExtractArrayRaw(stat, 'h') as team_h_events
    FROM fixtures f
    ARRAY JOIN JSONExtractArrayRaw(f.stats_json) as stat
),

-- Unnest team_a events
team_a_stats AS (
    SELECT
        fixture_id,
        team_a as team_id,
        'away' as team_location,
        stat_identifier,
        JSONExtractInt(event, 'element') as player_id,
        JSONExtractInt(event, 'value') as stat_value
    FROM stat_types
    ARRAY JOIN team_a_events as event
    WHERE event != ''
),

-- Unnest team_h events
team_h_stats AS (
    SELECT
        fixture_id,
        team_h as team_id,
        'home' as team_location,
        stat_identifier,
        JSONExtractInt(event, 'element') as player_id,
        JSONExtractInt(event, 'value') as stat_value
    FROM stat_types
    ARRAY JOIN team_h_events as event
    WHERE event != ''
),

-- Combine both teams
combined_stats AS (
    SELECT
        fixture_id,
        team_id,
        team_location,
        player_id,
        stat_identifier,
        stat_value
    FROM team_a_stats

    UNION ALL

    SELECT
        fixture_id,
        team_id,
        team_location,
        player_id,
        stat_identifier,
        stat_value
    FROM team_h_stats
)

-- Join with player information
SELECT
    cs.fixture_id,
    cs.team_id,
    cs.team_location,
    cs.player_id,
    cs.stat_identifier as event_type,
    cs.stat_value as event_value,
    
    -- Player Details
    p.player_name,
    p.team_name,
    p.position
FROM combined_stats cs
LEFT JOIN players_enriched p ON cs.player_id = p.player_id

ORDER BY cs.fixture_id, cs.team_location, cs.stat_identifier, cs.player_id