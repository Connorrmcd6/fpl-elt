{{ config(database='staging') }}

with raw_player_stats as (
  select
    id as raw_table_id,
    raw_data,
    loaded_at
  from {{ source('raw', 'player_stats') }}
)

select
  src.raw_table_id,
  JSONExtractString(src.raw_data, 'name') as stat_name,
  JSONExtractString(src.raw_data, 'label') as stat_label,
  src.loaded_at
from raw_player_stats as src