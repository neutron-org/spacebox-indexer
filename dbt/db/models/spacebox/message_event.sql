
{{ config(
    materialized = 'table',
    engine       = 'ReplacingMergeTree',
    order_by     = 'height'
) }}

SELECT * FROM {{ ref('message_event_block_writer') }}
UNION ALL
SELECT * FROM {{ ref('message_event_txs_writer') }}
