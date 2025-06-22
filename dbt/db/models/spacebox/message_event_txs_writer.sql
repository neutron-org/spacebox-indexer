
{{ config(
    materialized = 'materialized_view',
    engine       = 'ReplacingMergeTree',
    order_by     = 'height'
) }}

WITH
    -- define join tuple parts for row fields
    tx_result_tuple.1 as `tx_index`,
    tx_result_tuple.2 as `msg_part_index`,
    tx_result_tuple.3 as `msg_indexes`,
    tx_result_tuple.4 as `msg_events_index_offset`,
    tx_result_tuple.5 as `msg_events`
SELECT
    toDateTime(`timestamp`) as `timestamp`,
    `height`,
    -- BeginBlock is 1, txs is 2, EndBlock/Other is 3
    2 as `block_part_index`,
    `tx_index`,
    `msg_part_index`,
    `msg_indexes`,
    `msg_events_index_offset`,
    `msg_events`
FROM
    {{ ref('raw_block_results') }}
    ARRAY JOIN (
        -- Extract "txs_results" events with tx_index
        arrayFlatten(
            arrayMap(
                (tx_result, tx_result_index) -> arrayMap(
                    (tx_result_events) -> arrayMap(
                        (tx_result_events__msg_indexes) -> arrayMap(
                            (tx_result_tuple) -> (
                                tx_result_tuple.1,
                                tx_result_tuple.2,
                                tx_result_tuple.3,
                                tx_result_tuple.4,
                                -- use start/length indexes to get message parts
                                arraySlice(
                                    tx_result_events,
                                    tx_result_tuple.4 + 1,
                                    tx_result_tuple.5
                                )
                            ),
                            arrayFold(
                            (acc, event_index, tx_result_event__msg_indexes) -> (
                                if (
                                    acc[-1].3 = tx_result_event__msg_indexes,
                                    -- Skip event
                                    acc,
                                    -- Otherwise, create a new group
                                    arrayConcat(
                                        arrayPopBack(acc),
                                        -- edit previous group (end offset)
                                        [(
                                            acc[-1].1,
                                            acc[-1].2,
                                            acc[-1].3,
                                            acc[-1].4,
                                            toUInt32(event_index - acc[-1].4)
                                        )],
                                        -- add new group
                                        [(
                                            tx_result_index,
                                            toInt16(acc[-1].2 + 1),
                                            tx_result_event__msg_indexes,
                                            event_index,
                                            toUInt32(length(tx_result_events) - event_index)
                                        )]
                                    )
                                )
                            ),
                            -- fold (reduce) over subsequent message events
                            -- - get zero-based index
                            arrayEnumerate(arrayPopFront(tx_result_events)),
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
                                -- tx_result_tuple.4: msg_events_index_offset_start
                                toUInt32(0),
                                -- tx_result_tuple.5: msg_events_index_offset_end
                                toUInt32(length(tx_result_events))
                            )]
                            )
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