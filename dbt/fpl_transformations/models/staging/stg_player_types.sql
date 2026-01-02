{{ config(database='staging') }}

with raw_player_types as (
  select
    raw_data,
    loaded_at
  from {{ source('raw', 'player_types') }}
)

select
  JSONExtractInt(src.raw_data, 'id')                          as player_type_id,
  JSONExtractString(src.raw_data, 'plural_name')              as plural_name,
  JSONExtractString(src.raw_data, 'plural_name_short')        as plural_name_short,
  JSONExtractString(src.raw_data, 'singular_name')            as singular_name,
  JSONExtractString(src.raw_data, 'singular_name_short')      as singular_name_short,
  JSONExtractInt(src.raw_data, 'squad_select')                as squad_select,
  JSONExtractInt(src.raw_data, 'squad_min_play')              as squad_min_play,
  JSONExtractInt(src.raw_data, 'squad_max_play')              as squad_max_play,
  JSONExtractBool(src.raw_data, 'ui_shirt_specific')          as ui_shirt_specific,
  JSONExtractInt(src.raw_data, 'element_count')               as element_count,
  src.loaded_at
from raw_player_types as src