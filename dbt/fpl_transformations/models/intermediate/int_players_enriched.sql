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
    -- Player Identifiers
    p.player_id,
    p.web_name AS player_name,
    t.team_name,
    pt.singular_name AS position,
    
    -- Basic Stats
    p.now_cost / 10.0 AS player_cost,
    p.total_points,
    p.status AS player_status,
    p.selected_by_percent,
    p.minutes AS minutes_played,
    
    -- Scoring Stats
    p.goals_scored,
    p.assists,
    p.clean_sheets,
    p.own_goals,
    p.penalties_saved,
    p.penalties_missed,
    p.yellow_cards,
    p.red_cards,
    p.saves,
    p.bonus,
    p.bps,
    
    -- Influence Metrics
    p.influence,
    p.creativity,
    p.threat,
    p.ict_index,
    
    -- Expected Stats
    p.expected_goals as xg_total,
    p.expected_assists as xa_total,
    p.expected_goal_involvements as xgi_total,
    p.expected_goals_conceded as xgc_total,
    
    -- Team Strength Metrics
    t.strength_overall_home AS team_strength_overall_home,
    t.strength_overall_away AS team_strength_overall_away,
    t.strength_attack_home AS team_strength_attack_home,
    t.strength_attack_away AS team_strength_attack_away,
    t.strength_defence_home AS team_strength_defence_home,
    t.strength_defence_away AS team_strength_defence_away,
    
    -- Value Metrics
    p.total_points / (p.now_cost / 10.0) AS points_per_million,
    if(p.minutes > 0, p.total_points / (p.minutes / 90.0), 0) AS points_per_90,
    
    -- Form & Efficiency
    if(p.minutes > 0, p.goals_scored / (p.minutes / 90.0), 0) AS goals_per_90,
    if(p.minutes > 0, p.assists / (p.minutes / 90.0), 0) AS assists_per_90,
    if(p.expected_goals > 0, p.goals_scored / p.expected_goals, 0) AS goal_conversion_rate,
    if(p.expected_assists > 0, p.assists / p.expected_assists, 0) AS assist_conversion_rate,
    
    -- Expected Performance
    if(p.minutes > 0, p.expected_goals / (p.minutes / 90.0), 0) AS xg_per_90,
    if(p.minutes > 0, p.expected_assists / (p.minutes / 90.0), 0) AS xa_per_90,
    if(p.minutes > 0, p.expected_goal_involvements / (p.minutes / 90.0), 0) AS xgi_per_90,
    
    -- Attack Metrics
    (p.goals_scored + p.assists) AS goal_contributions,
    if(p.minutes > 0, (p.goals_scored + p.assists) / (p.minutes / 90.0), 0) AS contributions_per_90,
    
    -- Defensive Metrics
    if(p.minutes > 0, p.clean_sheets / (p.minutes / 90.0), 0) AS clean_sheets_per_90,
    if(p.minutes > 0, p.expected_goals_conceded / (p.minutes / 90.0), 0) AS xgc_per_90,
    
    -- Bonus & Influence
    if(p.minutes > 0, p.bonus / (p.minutes / 90.0), 0) AS bonus_per_90,
    if(p.minutes > 0, p.bps / (p.minutes / 90.0), 0) AS bps_per_90,
    (toFloat64(p.influence) + toFloat64(p.creativity) + toFloat64(p.threat)) / 3 AS avg_ict,
    
    -- Discipline
    (p.yellow_cards + (p.red_cards * 3)) AS discipline_score,
    
    -- Team Context Scores
    (t.strength_attack_home + t.strength_attack_away) / 2 AS avg_team_attack,
    (t.strength_defence_home + t.strength_defence_away) / 2 AS avg_team_defence,
    (t.strength_overall_home + t.strength_overall_away) / 2 AS avg_team_strength,
    
    -- Combined Value Score
    (p.total_points / (p.now_cost / 10.0)) * 
    if(p.minutes > 0, (p.minutes / 3420.0), 0) AS value_score

FROM players p
LEFT JOIN teams t ON p.team_id = t.team_id
LEFT JOIN player_types pt ON p.player_type_id = pt.player_type_id