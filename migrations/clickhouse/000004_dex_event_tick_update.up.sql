CREATE TABLE spacebox.dex_event_tick_update
(
    `timestamp` DateTime,
    `height` UInt64,
    `block_part_index` UInt8,
    `tx_index` UInt32,
    `event_index` UInt32,
    -- event data
    `type` String,
    `action` String,
    `TokenZero` String,
    `TokenOne` String,
    `TokenIn` String,
    `TickIndex` Int32,
    `Fee` UInt8,
    -- note: cannot make TrancheKey nullable if it is going to be in an index
    --       LP type ticks will have empty string TrancheKey values
    `TrancheKey` String,
    `Reserves` UInt128,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for token pair specific queries
    INDEX `pair_index` (`TokenZero`, `TokenOne`, `TokenIn`) TYPE set(0),
    -- add index for tranche queries
    INDEX `tranche_key_index` (`TrancheKey`) TYPE bloom_filter(0.01))
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (
    -- the minimum unique parts needed to describe a unique TickUpdate position
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`
)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW spacebox.dex_event_tick_update_writer TO spacebox.dex_event_tick_update (
    `timestamp` DateTime,
    `height` UInt64,
    `block_part_index` UInt8,
    `tx_index` UInt32,
    `event_index` UInt32,
    -- event data
    `type` String,
    `action` String,
    `TokenZero` String,
    `TokenOne` String,
    `TokenIn` String,
    `TickIndex` Int32,
    `Fee` UInt8,
    `TrancheKey` String,
    `Reserves` UInt128
) AS
    WITH
        -- define event_tuple parts for row fields
        event_tuple.1 as `block_part_index`,
        event_tuple.2 as `tx_index`,
        event_tuple.3 as `event_index`,
        event_tuple.4 as `event_type`,
        event_tuple.5 as `event_attributes`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `event_type` as `type`,
        -- add event attributes
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenZero'), `event_attributes`), 'value') AS `TokenZero`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenOne'), `event_attributes`), 'value') AS `TokenOne`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenIn'), `event_attributes`), 'value') AS `TokenIn`,
        toInt32(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TickIndex'), `event_attributes`), 'value')) AS `TickIndex`,
        -- fix accidental incorrect Fee values added to tranche-ticks before v4 (see https://github.com/neutron-org/neutron/pull/473)
        -- tranches are always effectively a 0% LP fee
        if(
            notEmpty(`TrancheKey`),
            0,
            toUInt16OrZero(JSONExtractString(arrayLast(x -> (JSONExtractString(x, 'key') = 'Fee'), `event_attributes`), 'value'))
        ) AS `Fee`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TrancheKey'), `event_attributes`), 'value') AS `TrancheKey`,
        toUInt128(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Reserves'), `event_attributes`), 'value')) AS `Reserves`
    FROM spacebox.raw_block_results
    ARRAY JOIN (
        -- Combine all event types into one array
        arrayConcat(
            -- Extract "finalize_block_events" events
            arrayFlatten(
                arrayMap(
                    (evnt, event_index) -> (
                        -- for each TickUpdate event, process each part
                        arrayMap(
                            tick_update_event -> (
                                -- event_tuple.1: block_part_index
                                if(
                                    JSONExtractString(
                                        arrayLast(
                                            attr -> JSONExtractString(attr, 'key') = 'mode',
                                            JSONExtractArrayRaw(tick_update_event, 'attributes')
                                        ),
                                        'value'
                                    ) = 'BeginBlock',
                                    1, -- set BeginBlock as block part 1
                                    3  -- set EndBlock/unknown as block part 3
                                ),
                                -- event_tuple.2: tx_index (note: cannot be null because it is part of the table index)
                                0,
                                -- event_tuple.3: event_index
                                event_index,
                                -- event_tuple.4: event_type
                                JSONExtractString(tick_update_event, 'type'),
                                -- event_tuple.5: event_attributes
                                JSONExtractArrayRaw(tick_update_event, 'attributes')
                            ),
                            -- first filter to only TickUpdate events
                            arrayFilter(
                                (evnt) -> JSONExtractString(evnt, 'type') = 'TickUpdate',
                                [evnt]
                            )
                        )
                    ),
                    JSONExtractArrayRaw(`finalize_block_events`),
                    arrayEnumerate(JSONExtractArrayRaw(`finalize_block_events`))
                )
            ),
            -- Extract "txs_results" events with tx_index
            arrayFlatten(
                arrayMap(
                    (tx_result, tx_result_index) -> arrayMap(
                        (tx_result_code) -> arrayMap(
                            (tx_result_events) -> arrayMap(
                                (tx_result_event, tx_result_event_index) -> arrayMap(
                                    (tick_update_event) -> (
                                        -- event_tuple.1: block_part_index
                                        2, -- set tx result events as block part 2
                                        -- event_tuple.2: tx_index
                                        tx_result_index,
                                        -- event_tuple.3: event_index
                                        tx_result_event_index,
                                        -- event_tuple.4: event_type
                                        JSONExtractString(tick_update_event, 'type'),
                                        -- event_tuple.5: event_attributes
                                        JSONExtractArrayRaw(tick_update_event, 'attributes')
                                    ),
                                    -- filter to only TickUpdate events
                                    arrayFilter(
                                        tx_result_event -> JSONExtractString(tx_result_event, 'type') = 'TickUpdate',
                                        [tx_result_event]
                                    )
                                ),
                                -- enumerate all events within a tx_result
                                -- enumerate each (tx_result_event, tx_result_event_index) within a txs_results
                                tx_result_events,
                                arrayEnumerate(tx_result_events)
                            ),
                            -- precompute tx_result "fields" as arrayMap lambda arguments
                            -- - tx_result "field" tx_result_events
                            [JSONExtractArrayRaw(tx_result, 'events')]
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
        )
    ) AS `event_tuple`
