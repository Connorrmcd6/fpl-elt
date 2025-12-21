{{ config(materialized='table', database='staging') }}

with raw_players as (
  select
    raw_data,
    loaded_at
  from raw.players
)


  select
    JSONExtractInt(src.raw_data, 'id') as player_id,
    JSONExtractString(src.raw_data, 'first_name') as first_name,
    JSONExtractString(src.raw_data, 'second_name') as second_name,
    JSONExtractString(src.raw_data, 'web_name') as web_name,
    JSONExtractInt(src.raw_data, 'team') as team_id,
    JSONExtractInt(src.raw_data, 'element_type') as player_type_id,
    JSONExtractInt(src.raw_data, 'now_cost') as now_cost,
    JSONExtractInt(src.raw_data, 'total_points') as total_points,
    JSONExtractString(src.raw_data, 'status') as status,
    JSONExtractString(src.raw_data, 'news') as news,
    JSONExtractString(src.raw_data, 'news_added') as news_added,
    JSONExtractInt(src.raw_data, 'chance_of_playing_next_round') as chance_of_playing_next_round,
    toFloat64(JSONExtractString(src.raw_data, 'selected_by_percent')) as selected_by_percent,
    JSONExtractInt(src.raw_data, 'minutes') as minutes,
    JSONExtractInt(src.raw_data, 'goals_scored') as goals_scored,
    JSONExtractInt(src.raw_data, 'assists') as assists,
    JSONExtractInt(src.raw_data, 'clean_sheets') as clean_sheets,
    JSONExtractInt(src.raw_data, 'goals_conceded') as goals_conceded,
    JSONExtractInt(src.raw_data, 'own_goals') as own_goals,
    JSONExtractInt(src.raw_data, 'penalties_saved') as penalties_saved,
    JSONExtractInt(src.raw_data, 'penalties_missed') as penalties_missed,
    JSONExtractInt(src.raw_data, 'yellow_cards') as yellow_cards,
    JSONExtractInt(src.raw_data, 'red_cards') as red_cards,
    JSONExtractInt(src.raw_data, 'saves') as saves,
    JSONExtractInt(src.raw_data, 'bonus') as bonus,
    JSONExtractInt(src.raw_data, 'bps') as bps,
    toFloat64(JSONExtractString(src.raw_data, 'influence')) as influence,
    toFloat64(JSONExtractString(src.raw_data, 'creativity')) as creativity,
    toFloat64(JSONExtractString(src.raw_data, 'threat')) as threat,
    toFloat64(JSONExtractString(src.raw_data, 'ict_index')) as ict_index,
    toFloat64(JSONExtractString(src.raw_data, 'expected_goals')) as expected_goals,
    toFloat64(JSONExtractString(src.raw_data, 'expected_assists')) as expected_assists,
    toFloat64(JSONExtractString(src.raw_data, 'expected_goal_involvements')) as expected_goal_involvements,
    toFloat64(JSONExtractString(src.raw_data, 'expected_goals_conceded')) as expected_goals_conceded,
    src.loaded_at
  from raw_players as src
