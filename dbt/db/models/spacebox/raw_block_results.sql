
{{ config(
    materialized = 'materialized_view',
    engine       = 'ReplacingMergeTree',
    order_by     = 'height'
) }}

SELECT JSONExtractInt(message, 'height')                                      AS height,
       JSONExtractString(message, 'txs_results')                              AS txs_results,
       JSONExtractString(message, 'finalize_block_events')                    AS finalize_block_events,
       JSONExtractString(message, 'validator_updates')                        AS validator_updates,
       JSONExtractString(message, 'consensus_param_updates')                  AS consensus_param_updates,
       parseDateTime64BestEffortOrZero(JSONExtractString(message, 'timestamp')) AS timestamp
FROM {{ source('kafka_pipeline', 'raw_block_results_topic') }}
GROUP BY height, txs_results, finalize_block_events, validator_updates, consensus_param_updates,
         timestamp;
