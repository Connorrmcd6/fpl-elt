{{ config(database='marts') }}

-- Grain: Team performance per gameweek
WITH fixture_results AS (
    SELECT
        CASE WHEN team_location = 'home' THEN f.home_team_id ELSE f.away_team_id END as team_id,
        f.fixture_id,
        toStartOfWeek(f.kickoff_time) as gameweek_start,
        
        -- Results
        CASE 
            WHEN team_location = 'home' AND f.result = 'H' THEN 3
            WHEN team_location = 'away' AND f.result = 'A' THEN 3
            WHEN f.result = 'D' THEN 1
            ELSE 0
        END as match_points,
        
        CASE 
            WHEN team_location = 'home' THEN f.home_score ELSE f.away_score 
        END as team_goals_for,
        
        CASE 
            WHEN team_location = 'home' THEN f.away_score ELSE f.home_score 
        END as team_goals_against,
        
        CASE 
            WHEN team_location = 'home' AND f.result = 'H' THEN 1
            WHEN team_location = 'away' AND f.result = 'A' THEN 1
            ELSE 0
        END as is_win,
        
        CASE WHEN f.result = 'D' THEN 1 ELSE 0 END as is_draw,
        
        CASE 
            WHEN team_location = 'home' AND f.result = 'A' THEN 1
            WHEN team_location = 'away' AND f.result = 'H' THEN 1
            ELSE 0
        END as is_loss
        
    FROM {{ ref('fact_fixture_statistics') }} f
    CROSS JOIN (SELECT 'home' as team_location UNION ALL SELECT 'away') locations
)

SELECT
    team_id,
    gameweek_start,
    
    count(*) as games_played,
    sum(match_points) as points,
    sum(team_goals_for) as goals_for,
    sum(team_goals_against) as goals_against,
    sum(team_goals_for - team_goals_against) as goal_difference,
    sum(is_win) as wins,
    sum(is_draw) as draws,
    sum(is_loss) as losses,
    sumIf(1, team_goals_against = 0) as clean_sheets

FROM fixture_results
GROUP BY team_id, gameweek_start