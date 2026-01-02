{{ config(database='staging') }}

with raw_chips as (
  select
    id as raw_table_id,
    raw_data,
    loaded_at
  from {{ source('raw', 'chips') }}
)

select
  src.raw_table_id,
  JSONExtractInt(src.raw_data, 'id')                          as chip_id,
  JSONExtractString(src.raw_data, 'name')                     as chip_name,
  JSONExtractInt(src.raw_data, 'number')                      as number,
  JSONExtractInt(src.raw_data, 'start_event')                 as start_event_gameweek,
  JSONExtractInt(src.raw_data, 'stop_event')                  as stop_event_gameweek,
  JSONExtractString(src.raw_data, 'chip_type')                as chip_type,
  src.loaded_at
from raw_chips as src