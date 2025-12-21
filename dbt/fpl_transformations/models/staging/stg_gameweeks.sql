{{ config(materialized='table', database='staging') }}

with raw_gameweeks as (
  select
    raw_data,
    loaded_at
  from raw.gameweeks
)
  select
    JSONExtractInt(src.raw_data, 'id') as gameweek_id,
    JSONExtractString(src.raw_data, 'name') as gameweek_name,
    parseDateTimeBestEffort(JSONExtractString(src.raw_data, 'deadline_time')) as deadline_time,
    JSONExtractInt(src.raw_data, 'average_entry_score') as average_entry_score,
    JSONExtractBool(src.raw_data, 'finished') as is_finished,
    JSONExtractBool(src.raw_data, 'data_checked') as is_data_checked,
    JSONExtractInt(src.raw_data, 'highest_score') as highest_score,
    JSONExtractBool(src.raw_data, 'is_previous') as is_previous,
    JSONExtractBool(src.raw_data, 'is_current') as is_current,
    JSONExtractBool(src.raw_data, 'is_next') as is_next,
    JSONExtractRaw(src.raw_data, 'chip_plays') as chip_plays_json,
    src.loaded_at
  from raw_gameweeks as src