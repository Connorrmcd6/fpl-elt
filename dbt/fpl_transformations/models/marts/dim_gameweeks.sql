{{ config(materialized='table', database='marts') }}

-- Gameweek dimension
SELECT
    gameweek_id,
    gameweek_name,
    deadline_time,
    is_previous,
    is_current,
    is_next,
    is_finished,
    is_data_checked,
    
    -- Aggregated stats
    average_entry_score,
    highest_score,
    
    -- Chip plays
    bboost_plays,
    tc_plays,
    freehit_plays,
    wildcard_plays,
    
    now() as last_updated

FROM {{ ref('int_gameweeks_enriched') }}