with prices AS (
    SELECT
        blockchain
        , contract_address
        , minute
        , price
    FROM
        prices.usd_with_native
    WHERE
        blockchain = 'ethereum'
)
, enrichments_with_prices AS (
    -- Add USD amounts
    SELECT
        bt.*
        , COALESCE(bt.token_bought_amount * pb.price, bt.token_sold_amount * ps.price) AS amount_usd
    FROM
        query_6210020 bt
    LEFT JOIN
        prices pb
        ON bt.token_bought_address = pb.contract_address
        AND bt.blockchain = pb.blockchain
        AND pb.minute = date_trunc('minute', bt.block_time)
    LEFT JOIN
        prices ps
        ON bt.token_sold_address = ps.contract_address
        AND bt.blockchain = ps.blockchain
        AND ps.minute = date_trunc('minute', bt.block_time)
)

-- Final output
SELECT
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
FROM
    enrichments_with_prices
ORDER BY
    block_time DESC
-- LIMIT 1000 -- Uncomment during testing

