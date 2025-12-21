{{ config(materialized='table', database='intermediate') }}


WITH players AS (
    SELECT * FROM {{ ref('stg_players') }}
),
teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
),
player_types AS (
    SELECT * FROM {{ ref('stg_player_types') }}
)
SELECT
    p.player_id,
    p.first_name || ' ' || p.second_name AS player_full_name,
    t.team_name,
    pt.singular_name AS position,
    p.now_cost / 10.0 AS player_cost,
    p.team_id as team_id
FROM players p
LEFT JOIN teams t ON p.team_id = t.team_id
LEFT JOIN player_types pt ON p.player_type_id = pt.player_type_id