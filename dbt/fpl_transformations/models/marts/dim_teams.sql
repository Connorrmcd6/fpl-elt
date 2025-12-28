{{ config(materialized='table', database='marts') }}

-- Team dimension with strength ratings
SELECT
    team_id,
    team_name,
    team_short_name,
    
    -- Strength metrics
    strength_overall_home,
    strength_overall_away,
    strength_attack_home,
    strength_attack_away,
    strength_defence_home,
    strength_defence_away,
    
    -- Calculated
    (strength_overall_home + strength_overall_away) / 2 as avg_strength,
    (strength_attack_home + strength_attack_away) / 2 as avg_attack,
    (strength_defence_home + strength_defence_away) / 2 as avg_defence,
    
    now() as last_updated

FROM {{ ref('stg_teams') }}