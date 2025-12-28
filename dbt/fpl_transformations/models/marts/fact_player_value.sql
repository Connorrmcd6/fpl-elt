{{ config(materialized='table', database='marts') }}

-- Grain: Daily snapshot of player value metrics
SELECT
    player_id,
    today() as snapshot_date,
    
    -- Pricing
    player_cost,
    total_points,
    
    -- Ownership
    selected_by_percent,
    
    -- Value metrics
    points_per_million,
    points_per_90,
    value_score,
    
    -- Performance metrics
    goals_per_90,
    assists_per_90,
    contributions_per_90,
    
    -- Expected
    xg_per_90,
    xa_per_90,
    xgi_per_90,
    
    -- Influence
    avg_ict,
    
    now() as loaded_at

FROM {{ ref('int_players_enriched') }}