{{ config(database='staging') }}

with raw_teams as (
  select
    raw_data,
    loaded_at
  from {{ source('raw', 'teams') }}
)

  select
    JSONExtractInt(src.raw_data, 'id') as team_id,
    JSONExtractInt(src.raw_data, 'code') as team_code,
    JSONExtractString(src.raw_data, 'name') as team_name,
    JSONExtractString(src.raw_data, 'short_name') as team_short_name,
    JSONExtractInt(src.raw_data, 'strength') as strength,
    JSONExtractInt(src.raw_data, 'strength_overall_home') as strength_overall_home,
    JSONExtractInt(src.raw_data, 'strength_overall_away') as strength_overall_away,
    JSONExtractInt(src.raw_data, 'strength_attack_home') as strength_attack_home,
    JSONExtractInt(src.raw_data, 'strength_attack_away') as strength_attack_away,
    JSONExtractInt(src.raw_data, 'strength_defence_home') as strength_defence_home,
    JSONExtractInt(src.raw_data, 'strength_defence_away') as strength_defence_away,
    JSONExtractInt(src.raw_data, 'pulse_id') as pulse_id,
    JSONExtractInt(src.raw_data, 'draw') as draw,
    JSONExtractInt(src.raw_data, 'loss') as loss,
    JSONExtractInt(src.raw_data, 'played') as played,
    JSONExtractInt(src.raw_data, 'points') as points,
    JSONExtractInt(src.raw_data, 'position') as position,
    JSONExtractInt(src.raw_data, 'win') as win,
    JSONExtractBool(src.raw_data, 'unavailable') as unavailable,
    src.loaded_at
  from raw_teams as src