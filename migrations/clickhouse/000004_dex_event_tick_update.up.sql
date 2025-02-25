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
    -- added after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
    `SwapAmountIn` UInt256,
    `SwapAmountOut` UInt256,
    -- added to calculate SwapAmountIn/Out for DEX v<=5 events
    `is_swap` Boolean,
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
    `Reserves` UInt128,
    -- added after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
    `SwapAmountIn` UInt256,
    `SwapAmountOut` UInt256,
    -- added to calculate SwapAmountIn/Out for DEX v<=5 events
    `is_swap` Boolean
) AS
    WITH
        -- define DEX address constant
        'neutron1n58mly6f7er0zs6swtetqgfqs36jaarqlplf59' as dex_address,
        -- define event_tuple parts for row fields
        event_tuple.1 as `block_part_index`,
        event_tuple.2 as `tx_index`,
        event_tuple.3 as `event_index`,
        event_tuple.4 as `event_type`,
        event_tuple.5 as `event_attributes`,
        -- computed field
        event_tuple.6 as `is_swap`
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
        toUInt128(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Reserves'), `event_attributes`), 'value')) AS `Reserves`,
        -- add new fields after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
        toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'SwapAmountIn'), `event_attributes`), 'value')) AS `SwapAmountIn`,
        toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'SwapAmountOut'), `event_attributes`), 'value')) AS `SwapAmountOut`,
        -- add computed `is_swap` field for DEX v1-5 swap-volume fix
        `is_swap`
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
                                JSONExtractArrayRaw(tick_update_event, 'attributes'),
                                -- event_tuple.6: is_swap
                                false
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
                            (tx_result_events, tx_result_events__types, tx_result_events__msg_index_attributes) -> arrayMap(
                                (tx_result_event, tx_result_event_index) -> arrayMap(
                                    (tick_update_event) -> arrayMap(
                                        (tick_update_event_attributes) -> arrayMap(
                                            (tx_result_related_msg_events) -> arrayMap(
                                                (tx_result_related_msg_dex_spent_event, tx_result_related_msg_dex_received_event) -> (
                                                    -- event_tuple.1: block_part_index
                                                    2, -- set tx result events as block part 2
                                                    -- event_tuple.2: tx_index
                                                    tx_result_index,
                                                    -- event_tuple.3: event_index
                                                    tx_result_event_index,
                                                    -- event_tuple.4: event_type
                                                    JSONExtractString(tick_update_event, 'type'),
                                                    -- event_tuple.5: event_attributes
                                                    JSONExtractArrayRaw(tick_update_event, 'attributes'),
                                                    -- event_tuple.6: is_swap
                                                    notEmpty(tx_result_related_msg_dex_spent_event) AND
                                                    notEmpty(tx_result_related_msg_dex_received_event) AND
                                                    -- is_swap part: exclude if event denom is the related msg dex received denom
                                                    (
                                                        -- calculate related msg dex received denom
                                                        regexpExtract(
                                                            arrayFirst(
                                                                (coin) -> not(match(coin, '^\\d+neutron\/pool\/\\d+$')),
                                                                -- separate out each denom in the coin event
                                                                splitByChar(
                                                                    ',',
                                                                    JSONExtractString(
                                                                        arrayFirst(
                                                                            (attr) -> (JSONExtractString(attr, 'key') = 'amount'),
                                                                            JSONExtractArrayRaw(tx_result_related_msg_dex_received_event, 'attributes')
                                                                        ),
                                                                        'value'
                                                                    )
                                                                )
                                                            ),
                                                            '^\\d+(.+)$',
                                                            1
                                                        )
                                                        -- compare against tick_update_event denom
                                                        != JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenIn'), tick_update_event_attributes), 'value')
                                                    )
                                                ),
                                                -- precompute tx_result_related_msg "fields" as arrayMap lambda arguments
                                                -- - tx_result_related_msg "field" tx_result_related_msg_dex_spent_event
                                                [arrayFirst(
                                                    (tx_result_related_msg_event) -> (
                                                        -- get first non-neutron/pool/ denom coin event
                                                        arrayExists(
                                                            (coin) -> not(match(coin, '^\\d+neutron\/pool\/\\d+$')),
                                                            -- separate out each denom in the coin event
                                                            splitByChar(
                                                                ',',
                                                                JSONExtractString(
                                                                    arrayFirst(
                                                                        (attr) -> (JSONExtractString(attr, 'key') = 'amount'),
                                                                        JSONExtractArrayRaw(tx_result_related_msg_event, 'attributes')
                                                                    ),
                                                                    'value'
                                                                )
                                                            )
                                                        )
                                                    ),
                                                    -- filter related msg events to 'coin_spent' events
                                                    arrayFilter(
                                                        (tx_result_related_msg_event) -> (
                                                            JSONExtractString(tx_result_related_msg_event, 'type') = 'coin_spent' AND
                                                            arrayExists(
                                                                (tx_result_related_msg_event_attributes) -> (
                                                                    JSONExtractString(tx_result_related_msg_event_attributes, 'key') = 'spender' AND
                                                                    JSONExtractString(tx_result_related_msg_event_attributes, 'value') = dex_address
                                                                ),
                                                                JSONExtractArrayRaw(tx_result_related_msg_event, 'attributes')
                                                            )
                                                        ),
                                                        tx_result_related_msg_events
                                                    )
                                                )],
                                                -- - tx_result_related_msg "field" tx_result_related_msg_dex_received_event
                                                [arrayFirst(
                                                    (tx_result_related_msg_event) -> (
                                                        -- get first non-neutron/pool/ denom coin event
                                                        arrayExists(
                                                            (coin) -> not(match(coin, '^\\d+neutron\/pool\/\\d+$')),
                                                            -- separate out each denom in the coin event
                                                            splitByChar(
                                                                ',',
                                                                JSONExtractString(
                                                                    arrayFirst(
                                                                        (attr) -> (JSONExtractString(attr, 'key') = 'amount'),
                                                                        JSONExtractArrayRaw(tx_result_related_msg_event, 'attributes')
                                                                    ),
                                                                    'value'
                                                                )
                                                            )
                                                        )
                                                    ),
                                                    -- filter related msg events to 'coin_received' events
                                                    arrayFilter(
                                                        (tx_result_related_msg_event) -> (
                                                            JSONExtractString(tx_result_related_msg_event, 'type') = 'coin_received' AND
                                                            arrayExists(
                                                                (tx_result_related_msg_event_attributes) -> (
                                                                    JSONExtractString(tx_result_related_msg_event_attributes, 'key') = 'receiver' AND
                                                                    JSONExtractString(tx_result_related_msg_event_attributes, 'value') = dex_address
                                                                ),
                                                                JSONExtractArrayRaw(tx_result_related_msg_event, 'attributes')
                                                            )
                                                        ),
                                                        tx_result_related_msg_events
                                                    )
                                                )]
                                            ),
                                            -- pre-compute several tuple "fields" for is_swap computation
                                            -- - tx_result_related_msg "field" tx_result_related_msg_events
                                            [arrayFilter(
                                                -- filter tx_result_events to events that match the current event's msg_index_attributes
                                                (tx_result_related_event, tx_result_related_event_index) -> (
                                                    -- matches "msg_index" and "authz_msg_index" attributes
                                                    tx_result_events__msg_index_attributes[tx_result_event_index] =
                                                    tx_result_events__msg_index_attributes[tx_result_related_event_index]
                                                ),
                                                tx_result_events,
                                                arrayEnumerate(tx_result_events)
                                            )]
                                        ),
                                        -- pre-compute tick_update_event "fields" as arrayMap lambda arguments
                                        -- - tick_update_event "field" tick_update_event_attributes
                                        [JSONExtractArrayRaw(tick_update_event, 'attributes')]
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
                            [JSONExtractArrayRaw(tx_result, 'events')],
                            -- - tx_result "field" tx_result_events__types
                            [arrayMap(
                                (tx_result_event) -> JSONExtractString(tx_result_event, 'type'),
                                JSONExtractArrayRaw(tx_result, 'events')
                            )],
                            -- - tx_result "field" tx_result_events__msg_index_attributes
                            [arrayMap(
                                (tx_result_event) -> arrayFilter(
                                    attr -> endsWith(JSONExtractString(attr, 'key'), 'msg_index'),
                                    JSONExtractArrayRaw(tx_result_event, 'attributes')
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
        )
    ) AS `event_tuple`
SETTINGS
    -- the array joins in the `is_swap` fix are heavy on memory usage
    -- better for this to be slow than crash the application
    max_block_size = 100;
