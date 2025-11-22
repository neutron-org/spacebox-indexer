
CREATE TABLE spacebox.message_event
(
    `timestamp` DateTime64(9),
    `height` Int64,
    `block_part_index` Int8,
    `tx_index` Int32,
    -- msg data
    `msg_part_index` Int16,
    `msg_indexes` Array(Int16),
    -- event data
    `msg_events_index_offset` Int32,
    `msg_events` Array(String),
    -- add data skipping index for time queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(`timestamp`) -- allow skipping irrelevant months
ORDER BY (
    `height`,
    `block_part_index`,
    `tx_index`,
    `msg_part_index`
)
TTL timestamp + toIntervalDay(30)
SETTINGS index_granularity = 8192;

-- spacebox.message_event_txs_writer source

CREATE MATERIALIZED VIEW spacebox.message_event_txs_writer TO spacebox.message_event
(
    `timestamp` DateTime64(9),
    `height` Int64,
    `block_part_index` Int8,
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
    spacebox.raw_block_results
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
SETTINGS
    -- split query execution into small chunks to reduce peak memory usage (~max 400MB each row)
    -- timed row query to be about 320ms for 500 msg parts or 280ms for 1 msg part of 4000 events
    max_block_size = 100,
    max_execution_time = 120;

-- spacebox.message_event_block_writer source

CREATE MATERIALIZED VIEW spacebox.message_event_block_writer TO spacebox.message_event
(
    `timestamp` DateTime64(9),
    `height` Int64,
    `block_part_index` Int8,
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
    spacebox.raw_block_results
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
SETTINGS
    -- split query execution into small chunks to reduce peak memory usage (~max 400MB each row)
    -- timed row query to be about 320ms for 500 msg parts or 280ms for 1 msg part of 4000 events
    max_block_size = 100,
    max_execution_time = 120;
