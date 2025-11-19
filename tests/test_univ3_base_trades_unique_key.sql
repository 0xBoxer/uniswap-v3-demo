with duplicate_keys as (
    select
        block_date
        , block_number
        , evt_index
        , count(*) as num_rows
    from {{ ref('univ3_base_trades') }}
    group by
        block_date
        , block_number
        , evt_index
    having count(*) > 1
)

select *
from duplicate_keys


