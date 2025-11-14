{{ config(
  alias = 'univ3_base_trades'
  , materialized = 'incremental'
  , incremental_strategy = 'merge'
  , unique_key = ['block_date', 'tx_hash', 'evt_index']
) }}

with uniswap_v3_base_trades as (
    -- Base trades from Uniswap V3 swaps on Ethereum
    select
        t.evt_block_number as block_number
        , t.evt_block_time as block_time
        , t.recipient as taker
        , cast(null as varbinary) as maker
        , case when amount0 < INT256 '0' then abs(amount0) else abs(amount1) end as token_bought_amount_raw
        , case when amount0 < INT256 '0' then abs(amount1) else abs(amount0) end as token_sold_amount_raw
        , case when amount0 < INT256 '0' then f.token0 else f.token1 end as token_bought_address
        , case when amount0 < INT256 '0' then f.token1 else f.token0 end as token_sold_address
        , t.contract_address as project_contract_address
        , t.evt_tx_hash as tx_hash
        , t.evt_index
    from
        uniswap_v3_ethereum.Pair_evt_Swap t
    inner join uniswap_v3_ethereum.Factory_evt_PoolCreated f
        on f.pool = t.contract_address
    where
        {% if is_incremental() %}
        t.evt_block_time >= now() - interval '1' day
        {% else %}
        t.evt_block_time >= now() - interval '3' day
        {% endif %}
)

select
    'ethereum' as blockchain
    , 'uniswap' as project
    , '3' as version
    , cast(date_trunc('month', dexs.block_time) as date) as block_month
    , cast(date_trunc('day', dexs.block_time) as date) as block_date
    , dexs.block_time
    , dexs.block_number
    , cast(dexs.token_bought_amount_raw as UINT256) as token_bought_amount_raw
    , cast(dexs.token_sold_amount_raw as UINT256) as token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
from
    uniswap_v3_base_trades dexs
where
    dexs.tx_hash is not null
    and dexs.evt_index is not null
    and cast(date_trunc('day', dexs.block_time) as date) is not null


