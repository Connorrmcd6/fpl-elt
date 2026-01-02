{{ config(database='marts') }}

-- Grain: One row per player per gameweek
WITH player_events AS (
    SELECT
        fixture_id,
        player_id,
        event_type,
        sum(event_value) as event_total
    FROM {{ ref('int_fixture_events') }}
    GROUP BY fixture_id, player_id, event_type
),

fixtures AS (
    SELECT 
        fixture_id,
        gameweek_id,
        kickoff_time
    FROM {{ ref('dim_fixtures') }} f
    JOIN {{ ref('stg_gameweeks') }} g ON toStartOfWeek(f.kickoff_time) = toStartOfWeek(g.deadline_time)
)

SELECT
    f.gameweek_id,
    pe.player_id,
    pe.fixture_id,
    toDate(f.kickoff_time) as fixture_date_key,
    
    -- Event metrics (pivoted)
    sumIf(pe.event_total, pe.event_type = 'goals_scored') as goals,
    sumIf(pe.event_total, pe.event_type = 'assists') as assists,
    sumIf(pe.event_total, pe.event_type = 'clean_sheets') as clean_sheets,
    sumIf(pe.event_total, pe.event_type = 'yellow_cards') as yellow_cards,
    sumIf(pe.event_total, pe.event_type = 'red_cards') as red_cards,
    sumIf(pe.event_total, pe.event_type = 'bonus') as bonus,
    sumIf(pe.event_total, pe.event_type = 'bps') as bps,
    sumIf(pe.event_total, pe.event_type = 'saves') as saves,
    sumIf(pe.event_total, pe.event_type = 'penalties_saved') as penalties_saved,
    sumIf(pe.event_total, pe.event_type = 'penalties_missed') as penalties_missed,
    sumIf(pe.event_total, pe.event_type = 'own_goals') as own_goals,
    
    -- Calculated points (FPL scoring rules - simplified)
    (sumIf(pe.event_total, pe.event_type = 'goals_scored') * 4 +
     sumIf(pe.event_total, pe.event_type = 'assists') * 3 +
     sumIf(pe.event_total, pe.event_type = 'clean_sheets') * 4 +
     sumIf(pe.event_total, pe.event_type = 'bonus')) as estimated_points

FROM player_events pe
JOIN fixtures f ON pe.fixture_id = f.fixture_id
GROUP BY f.gameweek_id, pe.player_id, pe.fixture_id, f.kickoff_time