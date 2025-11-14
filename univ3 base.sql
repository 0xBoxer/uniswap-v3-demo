WITH uniswap_v3_base_trades AS (
    -- Base trades from Uniswap V3 swaps
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.recipient AS taker
        , CAST(NULL AS varbinary) AS maker
        , CASE WHEN amount0 < INT256 '0' THEN ABS(amount0) ELSE ABS(amount1) END AS token_bought_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN ABS(amount1) ELSE ABS(amount0) END AS token_sold_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
        , CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
    FROM
        uniswap_v3_ethereum.Pair_evt_Swap t
    INNER JOIN
        uniswap_v3_ethereum.Factory_evt_PoolCreated f
        ON f.pool = t.contract_address
)
    SELECT
        'ethereum' AS blockchain
        , 'uniswap' AS project
        , '3' AS version
        , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
        , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
        , dexs.block_time
        , dexs.block_number
        , CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
        , CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
        , dexs.token_bought_address
        , dexs.token_sold_address
        , dexs.taker
        , dexs.maker
        , dexs.project_contract_address
        , dexs.tx_hash
        , dexs.evt_index
    FROM
        uniswap_v3_base_trades dexs

limit 100