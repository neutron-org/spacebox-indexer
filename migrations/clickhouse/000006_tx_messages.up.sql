
CREATE TABLE spacebox.txs_messages
(
    `timestamp` DateTime,
    `height` UInt64,
    `tx_index` UInt32,
    -- msg data
    `msg_part_index` UInt32,
    `msg_indexes` Array(UInt32),
    -- event data
    `msg_events` Array(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (
    height,
    tx_index,
    msg_part_index
)
SETTINGS index_granularity = 8192;

-- spacebox.txs_messages_writer source

CREATE MATERIALIZED VIEW spacebox.txs_messages_writer TO spacebox.txs_messages
(
    `timestamp` DateTime,
    `height` UInt64,
    `tx_index` UInt32,
    -- msg data
    `msg_part_index` UInt32,
    `msg_indexes` Array(UInt32),
    -- event data
    `msg_events` Array(String)
) AS
WITH
    -- define join tuple parts for row fields
    tx_result_tuple.1 as `tx_index`,
    tx_result_tuple.2 as `msg_part_index`,
    tx_result_tuple.3 as `msg_indexes`,
    tx_result_tuple.4 as `msg_events`
SELECT
    `timestamp`,
    `height`,
    `tx_index`,
    `msg_part_index`,
    `msg_indexes`,
    `msg_events`
FROM
    spacebox.raw_block_results
    ARRAY JOIN (
        -- Extract "txs_results" events with tx_index
        arrayFlatten(
            arrayMap(
                (tx_result, tx_result_index) -> arrayMap(
                    (tx_result_code) -> arrayFold(
                        (acc, tx_result_event, tx_result_event__msg_indexes) -> (
                            if (
                                acc[-1].3 = tx_result_event__msg_indexes,
                                -- Append event to the last group of events
                                arrayConcat(arrayPopBack(acc), [(acc[-1].1, acc[-1].2, acc[-1].3, arrayConcat(acc[-1].4, [tx_result_event]))]),
                                -- Otherwise, create a new group
                                arrayConcat(acc, [(tx_result_index, toUInt32(acc[-1].2 + 1), tx_result_event__msg_indexes, [tx_result_event])])
                            )
                        ),
                        -- fold (reduce) over message events
                        arrayPopFront(JSONExtractArrayRaw(tx_result, 'events')),
                        -- - tx_result_event "field" tx_result_event__msg_indexes
                        arrayMap(
                            (tx_result_event) -> arrayMap(
                                (attr) -> JSONExtractUInt(attr, 'value'),
                                arrayFilter(
                                    attr -> endsWith(JSONExtractString(attr, 'key'), 'msg_index'),
                                    JSONExtractArrayRaw(tx_result_event, 'attributes')
                                )
                            ),
                            arrayPopFront(JSONExtractArrayRaw(tx_result, 'events'))
                        ),
                        [(
                            -- tx_result_tuple.1: tx_index
                            tx_result_index,
                            -- tx_result_tuple.2: msg_part_index
                            toUInt32(0),
                            -- tx_result_tuple.3: msg_indexes
                            arrayMap(
                                (attr) -> JSONExtractUInt(attr, 'value'),
                                arrayFilter(
                                    attr -> endsWith(JSONExtractString(attr, 'key'), 'msg_index'),
                                    JSONExtractArrayRaw(JSONExtractArrayRaw(tx_result, 'events')[1], 'attributes')
                                )
                            ),
                            -- tx_result_tuple.4: msg_events
                            [JSONExtractArrayRaw(tx_result, 'events')[1]]
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
