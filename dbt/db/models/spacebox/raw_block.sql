
{{ config(
    materialized = 'materialized_view',
    engine       = 'ReplacingMergeTree',
    order_by     = 'height'
) }}

SELECT JSONExtractInt(message, 'block', 'header', 'height')                                 AS height,
       JSONExtractString(message, 'hash')                                                   AS hash,
       JSONExtractInt(message, 'num_txs')                                                   AS num_txs,
       JSONExtractInt(message, 'total_gas')                                                 AS total_gas,
       JSONExtractString(message, 'proposer_address')                                       AS proposer_address,
       parseDateTime64BestEffortOrZero(JSONExtractString(message, 'block', 'header', 'time')) AS timestamp,
       JSONExtractString(message, 'block', 'last_commit', 'signatures')                     AS signatures
FROM {{ source('kafka_pipeline', 'raw_block_topic') }}
--GROUP BY height, hash, num_txs, total_gas, proposer_address, timestamp, signatures;
