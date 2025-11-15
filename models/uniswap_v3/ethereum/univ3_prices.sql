{{ config(
  alias = 'univ3_prices'
  , materialized = 'incremental'
  , incremental_strategy = 'merge'
  , unique_key = ['block_date', 'block_number', 'evt_index']
) }}

with prices as (
    select
        blockchain
        , contract_address
        , minute
        , price
    from
        {{ source('prices', 'usd_with_native') }}
    where
        blockchain = 'ethereum'
        {% if is_incremental() %}
        and minute >= now() - interval '1' day
        {% else %}
        and minute >= now() - interval '30' day
        {% endif %}
)
, enrichments_with_prices as (
    -- Add USD amounts
    select
        bt.blockchain
        , bt.project
        , bt.version
        , bt.block_month
        , bt.block_date
        , bt.block_time
        , bt.block_number
        , bt.token_bought_symbol
        , bt.token_sold_symbol
        , bt.token_pair
        , bt.token_bought_amount
        , bt.token_sold_amount
        , bt.token_bought_amount_raw
        , bt.token_sold_amount_raw
        , bt.token_bought_address
        , bt.token_sold_address
        , bt.taker
        , bt.maker
        , bt.project_contract_address
        , bt.tx_hash
        , bt.tx_from
        , bt.tx_to
        , bt.evt_index
        , coalesce(bt.token_bought_amount * pb.price, bt.token_sold_amount * ps.price) as amount_usd
    from
        {{ ref('univ3_enrichments') }} bt
    left join prices pb
        on bt.token_bought_address = pb.contract_address
        and bt.blockchain = pb.blockchain
        and pb.minute = date_trunc('minute', bt.block_time)
    left join prices ps
        on bt.token_sold_address = ps.contract_address
        and bt.blockchain = ps.blockchain
        and ps.minute = date_trunc('minute', bt.block_time)
    {% if is_incremental() %}
    where bt.block_time >= now() - interval '1' day
    {% else %}
    where bt.block_time >= now() - interval '30' day
    {% endif %}
)

select
    blockchain
    , project
    , version
    , block_month
    , block_date
    , block_time
    , block_number
    , token_bought_symbol
    , token_sold_symbol
    , token_pair
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
from
    enrichments_with_prices


