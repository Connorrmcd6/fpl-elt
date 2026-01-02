{{ config(database='marts') }}

-- Player dimension with current attributes
SELECT
    player_id,
    player_name,
    team_name,
    position,
    player_cost,
    player_status,
    selected_by_percent,
    
    -- Current season totals
    total_points,
    minutes_played,
    goals_scored,
    assists,
    clean_sheets,
    
    -- Calculated attributes
    points_per_million,
    points_per_90,
    value_score,
    
    -- SCD Type 1 - always current state
    now() as last_updated

FROM {{ ref('int_players_enriched') }}