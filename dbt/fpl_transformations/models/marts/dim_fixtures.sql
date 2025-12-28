{{ config(materialized='table', database='marts') }}

-- Fixture dimension
SELECT
    fixture_id,
    kickoff_time,
    team_h as home_team_id,
    team_a as away_team_id,
    team_h_difficulty as home_difficulty,
    team_a_difficulty as away_difficulty,
    finished,
    finished_provisional,
    started,
    minutes,
    team_h_score as home_score,
    team_a_score as away_score,
    
    -- Derived
    CASE 
        WHEN team_h_score > team_a_score THEN 'H'
        WHEN team_a_score > team_h_score THEN 'A'
        ELSE 'D'
    END as result,
    
    now() as last_updated

FROM {{ ref('stg_fixtures') }}