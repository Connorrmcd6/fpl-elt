{{ config(materialized='table', database='intermediate') }}

    SELECT 
    gw.gameweek_id,
    gw.gameweek_name,
    gw.deadline_time,
    gw.average_entry_score,
    gw.is_finished,
    gw.is_data_checked,
    gw.highest_score,
    gw.is_previous,
    gw.is_current,
    gw.is_next,
    coalesce(
        JSONExtractInt(
            arrayFirst(x -> JSONExtractString(x, 'chip_name') = 'bboost', 
                      JSONExtractArrayRaw(gw.chip_plays_json)),
            'num_played'
        ),
        0
    ) as bboost_plays,
    coalesce(
        JSONExtractInt(
            arrayFirst(x -> JSONExtractString(x, 'chip_name') = '3xc', 
                      JSONExtractArrayRaw(gw.chip_plays_json)),
            'num_played'
        ),
        0
    ) as tc_plays,
    coalesce(
        JSONExtractInt(
            arrayFirst(x -> JSONExtractString(x, 'chip_name') = 'freehit', 
                      JSONExtractArrayRaw(gw.chip_plays_json)),
            'num_played'
        ),
        0
    ) as freehit_plays,
    coalesce(
        JSONExtractInt(
            arrayFirst(x -> JSONExtractString(x, 'chip_name') = 'wildcard', 
                      JSONExtractArrayRaw(gw.chip_plays_json)),
            'num_played'
        ),
        0
    ) as wildcard_plays
    FROM {{ ref('stg_gameweeks') }} gw

