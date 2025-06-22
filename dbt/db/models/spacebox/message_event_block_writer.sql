
{{ config(
    materialized = 'materialized_view',
    engine       = 'ReplacingMergeTree',
    order_by     = 'height'
) }}

WITH
    -- define join tuple parts for row fields
    block_event_tuple.1 as `is_begin_block`,
    block_event_tuple.2 as `msg_events_index_offset`,
    block_event_tuple.3 as `msg_events`
SELECT
    toDateTime(`timestamp`) as `timestamp`,
    `height`,
    -- BeginBlock is 1, txs is 2, EndBlock/Other is 3
    if(`is_begin_block`, 1, 3) as `block_part_index`,
    0 as `tx_index`, -- not a tx
    -- msg data
    0 as `msg_part_index`, -- not a tx
    [] as `msg_indexes`, -- not a tx
    `msg_events_index_offset`,
    `msg_events`
FROM
    {{ ref('raw_block_results') }}
    ARRAY JOIN (
        -- Extract "finalize_block_events" events with event_index
        arrayFlatten(
            arrayMap(
                (block_events) -> arrayMap(
                    (block_events__is_begin_block) -> arrayMap(
                        (block_event_tuple) -> (
                            block_event_tuple.1,
                            block_event_tuple.2,
                            -- use start/length indexes to get message parts
                            arraySlice(
                                block_events,
                                block_event_tuple.2 + 1,
                                block_event_tuple.3
                            )
                        ),
                        arrayFold(
                        (acc, event_index, block_event__is_begin_block) -> (
                            if (
                                acc[-1].1 = block_event__is_begin_block,
                                -- Skip event
                                acc,
                                -- Otherwise, create a new group
                                arrayConcat(
                                    arrayPopBack(acc),
                                    -- edit previous group (end offset)
                                    [(
                                        acc[-1].1,
                                        acc[-1].2,
                                        toUInt32(event_index - acc[-1].2)
                                    )],
                                    -- add new group
                                    [(
                                        block_event__is_begin_block,
                                        event_index,
                                        toUInt32(length(block_events) - event_index)
                                    )]
                                )
                            )
                        ),
                        -- fold (reduce) over subsequent message events
                        -- - get zero-based index
                        arrayEnumerate(arrayPopFront(block_events)),
                        -- - block_event "field" block_event__is_begin_block
                        arrayPopFront(block_events__is_begin_block),
                        -- create arrayFold initial value from first event
                        [(
                            -- block_event_tuple.1: is_begin_block
                            block_events__is_begin_block[1],
                            -- block_event_tuple.2: msg_events_index_offset_start
                            toUInt32(0),
                            -- block_event_tuple.3: msg_events_index_offset_end
                            toUInt32(length(block_events))
                        )]
                        )
                    ),
                    -- precompute block_events "fields" as arrayMap lambda arguments
                    -- - block_events "field" block_events__is_begin_block
                    [arrayMap(
                        (block_event) -> (
                            JSONExtractString(
                                arrayLast(
                                    attr -> JSONExtractString(attr, 'key') = 'mode',
                                    JSONExtractArrayRaw(block_event, 'attributes')
                                ),
                                'value'
                            ) = 'BeginBlock'
                        ),
                        block_events
                    )]
                ),
                -- precompute block_events "fields" as arrayMap lambda arguments
                -- - block_events "field" block_events
                [JSONExtractArrayRaw(`finalize_block_events`)]
            )
        )
    ) AS `block_event_tuple`