

with base_trades_with_tx AS (
    -- Add transaction columns (from, to, index)
    SELECT
        bt.*
        , tx."from" AS tx_from
        , tx."to" AS tx_to
        , tx."index" AS tx_index
    FROM
        query_6210013 bt
    INNER JOIN
        ethereum.transactions tx
        ON bt.block_date = tx.block_date
        AND bt.block_number = tx.block_number
        AND bt.tx_hash = tx.hash
)
, tokens_metadata AS (
    -- ERC20 token metadata
    SELECT
        blockchain
        , contract_address
        , symbol
        , decimals
    FROM
        tokens.erc20
    WHERE
        blockchain = 'ethereum'
)

    SELECT
        bt.blockchain
        , bt.project
        , bt.version
        , bt.block_month
        , bt.block_date
        , bt.block_time
        , bt.block_number
        , erc20_bought.symbol AS token_bought_symbol
        , erc20_sold.symbol AS token_sold_symbol
        , CASE
            WHEN LOWER(erc20_bought.symbol) > LOWER(erc20_sold.symbol) THEN CONCAT(erc20_sold.symbol, '-', erc20_bought.symbol)
            ELSE CONCAT(erc20_bought.symbol, '-', erc20_sold.symbol)
        END AS token_pair
        , bt.token_bought_amount_raw / POWER(10, erc20_bought.decimals) AS token_bought_amount
        , bt.token_sold_amount_raw / POWER(10, erc20_sold.decimals) AS token_sold_amount
        , bt.token_bought_amount_raw
        , bt.token_sold_amount_raw
        , bt.token_bought_address
        , bt.token_sold_address
        , COALESCE(bt.taker, bt.tx_from) AS taker
        , bt.maker
        , bt.project_contract_address
        , bt.tx_hash
        , bt.tx_from
        , bt.tx_to
        , bt.evt_index
    FROM
        base_trades_with_tx bt
    LEFT JOIN
        tokens_metadata AS erc20_bought
        ON erc20_bought.contract_address = bt.token_bought_address
        AND erc20_bought.blockchain = bt.blockchain
    LEFT JOIN
        tokens_metadata AS erc20_sold
        ON erc20_sold.contract_address = bt.token_sold_address
        AND erc20_sold.blockchain = bt.blockchain
