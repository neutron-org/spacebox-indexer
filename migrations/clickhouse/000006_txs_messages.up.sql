
CREATE TABLE spacebox.txs_messages
(
    `timestamp` DateTime,
    `height` Int64,
    `tx_index` Int32,
    -- msg data
    `msg_part_index` Int16,
    `msg_indexes` Array(Int16),
    -- event data
    `msg_events_index_offset` Int32,
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
    `height` Int64,
    `tx_index` Int32,
    -- msg data
    `msg_part_index` Int16,
    `msg_indexes` Array(Int16),
    -- event data
    `msg_events_index_offset` Int32,
    `msg_events` Array(String)
) AS
WITH
    -- define join tuple parts for row fields
    tx_result_tuple.1 as `tx_index`,
    tx_result_tuple.2 as `msg_part_index`,
    tx_result_tuple.3 as `msg_indexes`,
    tx_result_tuple.4 as `msg_events_index_offset`,
    tx_result_tuple.5 as `msg_events`
SELECT
    `timestamp`,
    `height`,
    `tx_index`,
    `msg_part_index`,
    `msg_indexes`,
    `msg_events_index_offset`,
    `msg_events`
FROM
    spacebox.raw_block_results
    ARRAY JOIN (
        -- Extract "txs_results" events with tx_index
        arrayFlatten(
            arrayMap(
                (tx_result, tx_result_index) -> arrayMap(
                    (tx_result_events) -> arrayMap(
                        (tx_result_events__msg_indexes) -> arrayFold(
                            (acc, tx_result_event, tx_result_event__msg_indexes) -> (
                                if (
                                    acc[-1].3 = tx_result_event__msg_indexes,
                                    -- Append event to the last group of events
                                    arrayConcat(
                                        arrayPopBack(acc),
                                        [(
                                            acc[-1].1,
                                            acc[-1].2,
                                            acc[-1].3,
                                            acc[-1].4,
                                            arrayConcat(acc[-1].5, [tx_result_event])
                                        )]
                                    ),
                                    -- Otherwise, create a new group
                                    arrayConcat(
                                        acc,
                                        [(
                                            tx_result_index,
                                            toInt16(acc[-1].2 + 1),
                                            tx_result_event__msg_indexes,
                                            toInt32(acc[-1].4 + if(empty(acc[-1].3), 0, length(acc[-1].5))),
                                            [tx_result_event]
                                        )]
                                    )
                                )
                            ),
                            -- fold (reduce) over subsequent message events
                            -- - tx_result_event "field" tx_result_event
                            arrayPopFront(tx_result_events),
                            -- - tx_result_event "field" tx_result_event__msg_indexes
                            arrayPopFront(tx_result_events__msg_indexes),
                            -- create arrayFold initial value from first event
                            [(
                                -- tx_result_tuple.1: tx_index
                                tx_result_index,
                                -- tx_result_tuple.2: msg_part_index
                                toInt16(0),
                                -- tx_result_tuple.3: msg_indexes
                                tx_result_events__msg_indexes[1],
                                -- tx_result_tuple.4: msg_events_index_offset
                                toInt32(0),
                                -- tx_result_tuple.5: msg_events
                                [tx_result_events[1]]
                            )]
                        ),
                        -- precompute tx_result "fields" as arrayMap lambda arguments
                        -- - tx_result "field" tx_result_events__msg_indexes
                        [arrayMap(
                            (tx_result_event) -> arrayMap(
                                (attr) -> JSONExtractUInt(attr, 'value'),
                                arrayReverse(
                                    arraySlice(
                                        JSONExtractArrayRaw(tx_result_event, 'attributes'),
                                        arrayLastIndex(
                                            attr -> not(endsWith(JSONExtractString(attr, 'key'), 'msg_index')),
                                            JSONExtractArrayRaw(tx_result_event, 'attributes')
                                        ) + 1
                                    )
                                )
                            ),
                            tx_result_events
                        )]
                    ),
                    -- filter to only successful transactions
                    arrayFilter(
                        (tx_result_events, tx_result_code) -> tx_result_code = 0,
                        [JSONExtractArrayRaw(tx_result, 'events')],
                        [JSONExtractUInt(tx_result, 'code')]
                    )
                ),
                -- enumerate each (tx_result, tx_result_index) within a txs_results
                JSONExtractArrayRaw(`txs_results`),
                arrayEnumerate(JSONExtractArrayRaw(`txs_results`))
            )
        )
    ) AS `tx_result_tuple`
SETTINGS
    -- split query execution into small chunks to reduce peak memory usage
    max_block_size = 50;
