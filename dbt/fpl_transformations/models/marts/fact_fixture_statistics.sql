{{ config(database='marts') }}

-- Grain: One row per fixture with aggregated stats
SELECT
    f.fixture_id,
    f.kickoff_time,
    toDate(f.kickoff_time) as fixture_date_key,
    f.home_team_id,
    f.away_team_id,
    
    -- Match result
    f.home_score,
    f.away_score,
    f.result,
    
    -- Home team stats
    sumIf(fe.event_value, fe.team_location = 'home' AND fe.event_type = 'goals_scored') as home_goals,
    sumIf(fe.event_value, fe.team_location = 'home' AND fe.event_type = 'yellow_cards') as home_yellow_cards,
    sumIf(fe.event_value, fe.team_location = 'home' AND fe.event_type = 'red_cards') as home_red_cards,
    
    -- Away team stats
    sumIf(fe.event_value, fe.team_location = 'away' AND fe.event_type = 'goals_scored') as away_goals,
    sumIf(fe.event_value, fe.team_location = 'away' AND fe.event_type = 'yellow_cards') as away_yellow_cards,
    sumIf(fe.event_value, fe.team_location = 'away' AND fe.event_type = 'red_cards') as away_red_cards,
    
    -- Totals
    sum(fe.event_value) as total_events

FROM {{ ref('dim_fixtures') }} f
LEFT JOIN {{ ref('int_fixture_events') }} fe ON f.fixture_id = fe.fixture_id
WHERE f.finished = true
GROUP BY 
    f.fixture_id,
    f.kickoff_time,
    f.home_team_id,
    f.away_team_id,
    f.home_score,
    f.away_score,
    f.result