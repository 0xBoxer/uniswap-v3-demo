{{ config(
  alias = 'univ3_enrichments'
  , materialized = 'incremental'
  , incremental_strategy = 'merge'
  , unique_key = ['block_date', 'block_number', 'evt_index']
) }}

with base_trades_with_tx as (
    -- Add transaction columns (from, to, index)
    select
        bt.blockchain
        , bt.project
        , bt.version
        , bt.block_month
        , bt.block_date
        , bt.block_time
        , bt.block_number
        , bt.token_bought_amount_raw
        , bt.token_sold_amount_raw
        , bt.token_bought_address
        , bt.token_sold_address
        , bt.taker
        , bt.maker
        , bt.project_contract_address
        , bt.tx_hash
        , tx."from" as tx_from
        , tx."to" as tx_to
        , tx."index" as tx_index
        , bt.evt_index
    from
        {{ ref('univ3_base_trades') }} bt
    inner join {{ source('ethereum', 'transactions') }} tx
        on bt.block_date = tx.block_date
        and bt.block_number = tx.block_number
        and bt.tx_hash = tx.hash
    {% if is_incremental() %}
    where bt.block_date >= current_date - interval '1' day
    {% else %}
    where bt.block_date >= current_date - interval '30' day
    {% endif %}
)
, tokens_metadata as (
    -- ERC20 token metadata
    select
        blockchain
        , contract_address
        , symbol
        , decimals
    from
        {{ source('tokens', 'erc20') }}
    where
        blockchain = 'ethereum'
)

select
    bt.blockchain
    , bt.project
    , bt.version
    , bt.block_month
    , bt.block_date
    , bt.block_time
    , bt.block_number
    , erc20_bought.symbol as token_bought_symbol
    , erc20_sold.symbol as token_sold_symbol
    , case
        when lower(erc20_bought.symbol) > lower(erc20_sold.symbol) then concat(erc20_sold.symbol, '-', erc20_bought.symbol)
        else concat(erc20_bought.symbol, '-', erc20_sold.symbol)
      end as token_pair
    , bt.token_bought_amount_raw / power(10, erc20_bought.decimals) as token_bought_amount
    , bt.token_sold_amount_raw / power(10, erc20_sold.decimals) as token_sold_amount
    , bt.token_bought_amount_raw
    , bt.token_sold_amount_raw
    , bt.token_bought_address
    , bt.token_sold_address
    , coalesce(bt.taker, bt.tx_from) as taker
    , bt.maker
    , bt.project_contract_address
    , bt.tx_hash
    , bt.tx_from
    , bt.tx_to
    , bt.evt_index
from
    base_trades_with_tx bt
left join tokens_metadata as erc20_bought
    on erc20_bought.contract_address = bt.token_bought_address
    and erc20_bought.blockchain = bt.blockchain
left join tokens_metadata as erc20_sold
    on erc20_sold.contract_address = bt.token_sold_address
    and erc20_sold.blockchain = bt.blockchain


