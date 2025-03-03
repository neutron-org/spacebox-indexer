
CREATE TABLE spacebox.dex_txs_messages
(
    `timestamp` DateTime,
    `height` UInt64,
    `tx_index` UInt32,
    -- msg data
    `msg_part_index` UInt16,
    `msg_indexes` Array(UInt16),
    -- wasm msg data
    `wasm_part_index` UInt16,
    -- event data
    `msg_part_events_index_offset` UInt32,
    `msg_part_events` Array(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (
    height,
    tx_index,
    msg_part_index
)
SETTINGS index_granularity = 8192;

-- spacebox.dex_txs_messages_writer source

CREATE MATERIALIZED VIEW spacebox.dex_txs_messages_writer TO spacebox.dex_txs_messages
(
    `timestamp` DateTime,
    `height` UInt64,
    `tx_index` UInt32,
    -- msg data
    `msg_part_index` UInt16,
    `msg_indexes` Array(UInt16),
    -- wasm msg data
    `wasm_part_index` UInt16,
    -- event data
    `msg_part_events_index_offset` UInt32,
    `msg_part_events` Array(String)
) AS
WITH
    -- define join tuple parts for row fields
    tx_message_tuple.1 as `wasm_part_index`,
    tx_message_tuple.2 as `msg_part_events_index_offset`,
    tx_message_tuple.3 as `msg_part_events`
SELECT
    `timestamp`,
    `height`,
    `tx_index`,
    `msg_part_index`,
    `msg_indexes`,
    `wasm_part_index`,
    `msg_part_events_index_offset`,
    `msg_part_events`
FROM
    spacebox.txs_messages
    ARRAY JOIN (
        -- Extract "DEX wasm event fingerprint" from events to extract wasm msg events
        arrayFlatten(
            arrayMap(
                (msg_events__types) -> arrayMap(
                    (msg_events__wasm_dex_msg_event_indexes) -> arrayMap(
                        (wasm_dex_msg_event_index_lower_bound, wasm_dex_msg_event_index_upper_bound, wasm_part_index) -> (
                            -- tx_message_tuple.1: wasm_part_index
                            toUInt16(wasm_part_index - 1),
                            -- tx_message_tuple.2: msg_part_events_index_offset
                            toUInt32(`msg_events_index_offset` + wasm_dex_msg_event_index_lower_bound - 1),
                            -- tx_message_tuple.2: msg_part_events
                            arraySlice(
                                `msg_events`,
                                wasm_dex_msg_event_index_lower_bound,
                                wasm_dex_msg_event_index_upper_bound - wasm_dex_msg_event_index_lower_bound
                            )
                        ),
                        -- pass start of bounds
                        arrayConcat([1], msg_events__wasm_dex_msg_event_indexes),
                        -- pass end of bounds
                        arrayConcat(msg_events__wasm_dex_msg_event_indexes, [length(msg_events__types) + 1]),
                        -- wasm_part_index 
                        arrayEnumerate(arrayConcat([1], msg_events__wasm_dex_msg_event_indexes))
                    ),
                    arrayMap(
                        -- precompute msg_events "fields" as arrayMap lambda arguments
                        -- - msg_events "field" msg_events__wasm_dex_msg_event_indexes
                        (msg_events__wasm_dex_msg_regex_matches) -> arraySort(
                            arrayFlatten(
                                -- find msg event bounds within wasm actions
                                arrayMap(
                                    (match, match_count_index) -> (
                                        arrayFilter(
                                            i -> arraySlice(msg_events__types, i, length(splitByChar(',', match))) = splitByChar(',', match),
                                            arrayEnumerate(msg_events__types)
                                        )[match_count_index]
                                    ),
                                    msg_events__wasm_dex_msg_regex_matches,
                                    -- find the "match_count_index" number of the each match string by counting the number of previously seen matching match strings
                                    -- (eg. if a PlaceLimitOrder match was detected, is it PlaceLimitOrder 1 or 2 or N?)
                                    arrayMap(
                                        (match, i) -> arrayCount(x -> x = match, arraySlice(msg_events__wasm_dex_msg_regex_matches, 1, i - 1)) + 1,
                                        msg_events__wasm_dex_msg_regex_matches,
                                        arrayEnumerate(msg_events__wasm_dex_msg_regex_matches)
                                    )
                                )
                            )
                        ),
                        -- precompute msg_events "fields" as arrayMap lambda arguments
                        -- - msg_events "field" msg_events__wasm_dex_msg_regex_matches
                        [
                            if(
                                has(msg_events__types, 'TickUpdate') AND
                                has(msg_events__types, 'wasm'),
                                arrayFilter(
                                    x -> notEmpty(x),
                                    arrayFlatten(
                                        -- compare tx event types array as string against tx msg detection regex
                                        -- to find where the sub-msgs are in each CosmWasm tx `events` list
                                        extractAllGroupsHorizontal(
                                            arrayStringConcat(msg_events__types, ','),
                                            -- note: this is a msg action detection regex, it can determine which Dex v5 msg was used to create this order of events
                                            --       msgs: https://github.com/neutron-org/neutron/blob/v5.1.3/proto/neutron/dex/tx.proto#L16-L28
                                            arrayStringConcat(
                                                [
                                                    '(',
                                                    arrayStringConcat([
                                                        -- MsgDeposit
                                                        '(?:message,)?(?:(?:neutron,)?(?:neutron,)?(?:TickUpdate)?,TickUpdate,)+(?:message,)*(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer,(?:message,)?coin_received,coinbase,coin_spent,coin_received,transfer(?:,message)?',
                                                        -- MsgWithdrawal
                                                        '(?:message,)?(?:(?:neutron,)?TickUpdate,)+(?:message,)*coin_spent,coin_received,transfer,(?:message,)?coin_spent,burn,coin_spent,coin_received,transfer(?:,message)?,neutron',
                                                        -- MsgPlaceLimitOrder
                                                        '(?:message,)?(?:(?:neutron,)?TickUpdate(?:,TickUpdate)?,)*neutron,(?:neutron,)?(?:TickUpdate,)?TrancheUserUpdate,(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer(?:,message)?',
                                                        -- MsgWithdrawFilledLimitOrder
                                                        -- (unused) '(?:message,)?TrancheUserUpdate,coin_spent,coin_received,transfer(?:,message)?(?:,message)?',
                                                        -- MsgCancelLimitOrder
                                                        '(?:message,)?(?:TrancheUserUpdate,(?:neutron,)?TickUpdate,)+(?:coin_spent,coin_received,transfer,(?:message,)?)?coin_spent,coin_received,transfer(?:,message)?(?:,message)?',
                                                        -- MsgMultiHopSwap
                                                        '(?:message,)?(?:(?:neutron,)?TickUpdate(?:,TickUpdate)?,)*neutron,(?:TickUpdate,)?coin_spent,coin_received,transfer,(?:message,)?coin_spent,coin_received,transfer(?:,message)?'
                                                    ], ')|('),
                                                    ')'
                                                ],
                                                ''
                                            )
                                        )
                                    )
                                ),
                                []
                            )
                        ]
                    )
                ),
                -- precompute msg_events "fields" as arrayMap lambda arguments
                -- - msg_events "field" msg_events__types
                [arrayMap(
                    (msg_event) -> JSONExtractString(msg_event, 'type'),
                    msg_events
                )]
            )
        )
    ) AS `tx_message_tuple`
SETTINGS
    -- split query execution into small chunks to reduce peak memory usage
    max_block_size = 50;

-- spacebox.dex_event_tick_update table

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

-- spacebox.dex_event_tick_update source 1

CREATE MATERIALIZED VIEW spacebox.dex_block_event_tick_update_writer TO spacebox.dex_event_tick_update (
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
        )
    ) AS `event_tuple`
SETTINGS
    -- the array joins in the `is_swap` fix are heavy on memory usage
    -- better for this to be slow than crash the application
    max_block_size = 100;

-- spacebox.dex_event_tick_update source 2

CREATE MATERIALIZED VIEW spacebox.dex_txs_event_tick_update_writer TO spacebox.dex_event_tick_update (
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
        event_tuple.1 as `event_index`,
        event_tuple.2 as `event_type`,
        event_tuple.3 as `event_attributes`,
        -- computed field
        event_tuple.4 as `calculated_is_swap`
    SELECT
        `timestamp`,
        `height`,
        -- block part 1 is BeginBlock, 2 is txs, 3 is EndBlock/unknown
        2 as `block_part_index`,
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
        if (SwapAmountIn > 0, 1, `calculated_is_swap`) as `is_swap`
    FROM spacebox.dex_txs_messages
    ARRAY JOIN (
        -- Extract "txs_results" events with tx_index
        arrayFlatten(
            arrayMap(
                (msg_part_event, msg_part_event_index) -> arrayMap(
                    (tick_update_event) -> arrayMap(
                        (tx_result_related_msg_dex_spent_event, tx_result_related_msg_dex_received_event) -> (
                            -- event_tuple.1: event_index
                            toUInt32(`msg_part_events_index_offset` + msg_part_event_index - 1),
                            -- event_tuple.2: event_type
                            JSONExtractString(tick_update_event, 'type'),
                            -- event_tuple.3: event_attributes
                            JSONExtractArrayRaw(tick_update_event, 'attributes'),
                            -- event_tuple.4: is_swap
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
                                != JSONExtractString(
                                    arrayFirst(
                                        x -> JSONExtractString(x, 'key') = 'TokenIn',
                                        JSONExtractArrayRaw(tick_update_event, 'attributes')
                                    ),
                                    'value'
                                )
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
                                `msg_part_events`
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
                                `msg_part_events`
                            )
                        )]
                    ),
                    -- filter to only TickUpdate events
                    arrayFilter(
                        msg_part_event -> JSONExtractString(msg_part_event, 'type') = 'TickUpdate',
                        [msg_part_event]
                    )
                ),
                -- enumerate each (msg_part_event, msg_part_event_index) within a txs_result_msg_part
                `msg_part_events`,
                arrayEnumerate(`msg_part_events`)
            )
        )
    ) AS `event_tuple`
SETTINGS
    -- the array joins in the `is_swap` fix are heavy on memory usage
    -- better for this to be slow than crash the application
    max_block_size = 100;
