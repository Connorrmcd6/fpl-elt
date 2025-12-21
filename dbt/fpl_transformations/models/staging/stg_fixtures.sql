{{ config(materialized='table', database='staging') }}

with raw_fixtures as (
  select
    id as raw_table_id,
    raw_data,
    loaded_at
  from raw.fixtures
)

select
  src.raw_table_id,
  JSONExtractInt(src.raw_data, 'id')                          as fixture_id,
  parseDateTimeBestEffort(JSONExtractString(src.raw_data, 'kickoff_time')) as kickoff_time,
  JSONExtractBool(src.raw_data, 'finished')                   as finished,
  JSONExtractBool(src.raw_data, 'finished_provisional')       as finished_provisional,
  JSONExtractBool(src.raw_data, 'started')                    as started,
  JSONExtractInt(src.raw_data, 'minutes')                     as minutes,
  JSONExtractInt(src.raw_data, 'team_a')                      as team_a,
  JSONExtractInt(src.raw_data, 'team_h')                      as team_h,
  JSONExtractInt(src.raw_data, 'team_a_score')                as team_a_score,
  JSONExtractInt(src.raw_data, 'team_h_score')                as team_h_score,
  JSONExtractInt(src.raw_data, 'team_h_difficulty')           as team_h_difficulty,
  JSONExtractInt(src.raw_data, 'team_a_difficulty')           as team_a_difficulty,
  JSONExtractInt(src.raw_data, 'pulse_id')                    as pulse_id,
  JSONExtractRaw(src.raw_data, 'stats')                       as stats_json,
  src.loaded_at
from raw_fixtures as src