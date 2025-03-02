
CREATE TABLE spacebox.txs_messages
(
    `timestamp` DateTime,
    `height` UInt64,
    `tx_index` UInt32,
    -- msg data
    `msg_indexes` Array(UInt32),
    -- event data
    `msg_events` Array(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (
    height,
    tx_index,
    msg_indexes
)
SETTINGS index_granularity = 8192;

-- spacebox.txs_messages_writer source

CREATE MATERIALIZED VIEW spacebox.txs_messages_writer TO spacebox.txs_messages
(
    `timestamp` DateTime,
    `height` UInt64,
    `tx_index` UInt32,
    -- msg data
    `msg_indexes` Array(UInt32),
    -- event data
    `msg_events` Array(String)
) AS
WITH
    -- define join tuple parts for row fields
    tx_result_tuple.1 as `tx_index`,
    tx_result_tuple.2 as `msg_indexes`,
    tx_result_tuple.3 as `msg_events`
SELECT
    `timestamp`,
    `height`,
    `tx_index`,
    `msg_indexes`,
    `msg_events`
FROM
    spacebox.raw_block_results
    ARRAY JOIN (
        -- Extract "txs_results" events with tx_index
        arrayFlatten(
            arrayMap(
                (tx_result, tx_result_index) -> arrayMap(
                    (tx_result_code) -> arrayMap(
                        (tx_result_events, tx_result_events__msg_indexes) -> arrayMap(
                            (tx_result__msg_index_groups) -> arrayMap(
                                (tx_result_msg_indexes) -> (
                                    -- tx_result_tuple.1: tx_index
                                    tx_result_index,
                                    -- tx_result_tuple.2: msg_indexes
                                    tx_result_msg_indexes,
                                    -- tx_result_tuple.3: msg_events
                                    arrayFilter(
                                        -- filter tx events to matching msg_indexes
                                        (tx_result_event, tx_result_event_index) -> (
                                            tx_result_events__msg_indexes[tx_result_event_index] = tx_result_msg_indexes
                                        ),
                                        tx_result_events,
                                        arrayEnumerate(tx_result_events)
                                    )
                                ),
                                tx_result__msg_index_groups
                            ),
                            -- precompute tx_result "fields" as arrayMap lambda arguments
                            -- - tx_result "field" tx_result__msg_index_groups
                            [arrayReduce(
                                'groupUniqArray',
                                tx_result_events__msg_indexes
                            )]
                        ),
                        -- precompute tx_result "fields" as arrayMap lambda arguments
                        -- - tx_result "field" tx_result_events
                        [JSONExtractArrayRaw(tx_result, 'events')],
                        -- - tx_result "field" tx_result_events__msg_indexes
                        [arrayMap(
                            (tx_result_event) -> arrayMap(
                                (attr) -> JSONExtractUInt(attr, 'value'),
                                arrayFilter(
                                    attr -> endsWith(JSONExtractString(attr, 'key'), 'msg_index'),
                                    JSONExtractArrayRaw(tx_result_event, 'attributes')
                                )
                            ),
                            JSONExtractArrayRaw(tx_result, 'events')
                        )]
                    ),
                    -- filter to only successful transactions
                    arrayFilter(
                        (tx_result_code) -> tx_result_code = 0,
                        [JSONExtractUInt(tx_result, 'code')]
                    )
                ),
                -- enumerate each (tx_result, tx_result_index) within a txs_results
                JSONExtractArrayRaw(`txs_results`),
                arrayEnumerate(JSONExtractArrayRaw(`txs_results`))
            )
        )
    ) AS `tx_result_tuple`
;
