{{ config(database='intermediate') }}


WITH fixtures AS (
    SELECT * FROM {{ ref('stg_fixtures') }}
),
teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
)
SELECT
    f.fixture_id,
    f.kickoff_time,
    home_team.team_name AS home_team_name,
    away_team.team_name AS away_team_name,
    f.team_h_score AS home_team_score,
    f.team_a_score AS away_team_score
FROM fixtures f
LEFT JOIN teams AS home_team 
    ON f.team_h = home_team.team_id
LEFT JOIN teams AS away_team 
    ON f.team_a = away_team.team_id
WHERE f.finished = 1