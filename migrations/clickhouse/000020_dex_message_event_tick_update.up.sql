
-- spacebox.dex_message_event_tick_update table

CREATE TABLE spacebox.dex_message_event_tick_update
(
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- add computed sort key for easier event ordering
    `sort_key`          Tuple(Int64, Int8, Int32, Int32)
                        MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`),
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    -- note: cannot make TrancheKey nullable if it is going to be in an index
    --       LP type ticks will have empty string TrancheKey values
    `TrancheKey`        String,
    `Reserves`          UInt256,
    -- added after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
    `SwapAmountIn`      UInt256,
    `SwapAmountOut`     UInt256,
    -- added to calculate SwapAmountIn/Out for DEX v<=5 events
    `is_swap`           Boolean,
    `is_estimated_swap` Boolean,
    `version`           UInt8 DEFAULT 1,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for token pair specific queries
    INDEX `pair_index` (`TokenZero`, `TokenOne`, `TokenIn`) TYPE set(0),
    -- add index for tranche queries
    INDEX `tranche_key_index` (`TrancheKey`) TYPE bloom_filter(0.01))
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree(`version`)
ORDER BY (
    -- the minimum unique parts needed to describe a unique TickUpdate position
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`
)
SETTINGS index_granularity = 8192;

-- spacebox.preparsed_dex_message_event_tick_update_writer source

CREATE MATERIALIZED VIEW spacebox.preparsed_dex_message_event_tick_update_writer TO spacebox.dex_message_event_tick_update (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `TrancheKey`        String,
    `Reserves`          UInt256,
    -- added after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
    `SwapAmountIn`      UInt256,
    `SwapAmountOut`     UInt256,
    -- added to calculate SwapAmountIn/Out for DEX v<=5 events
    `is_swap`           Boolean,
    `is_estimated_swap` Boolean
) AS
    WITH
        -- define DEX address constant
        'neutron1n58mly6f7er0zs6swtetqgfqs36jaarqlplf59' as dex_address,
        -- define event_tuple parts for row fields
        event_tuple.1 as `event_index`,
        event_tuple.2 as `event_type`,
        event_tuple.3 as `event_attributes`,
        -- computed field
        event_tuple.4 as `is_pre_v6_estimated_swap`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `event_type` as `type`,
        -- add event attributes
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'action'), `event_attributes`), 2) AS `action`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenZero'), `event_attributes`), 2) AS `TokenZero`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenOne'), `event_attributes`), 2) AS `TokenOne`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenIn'), `event_attributes`), 2) AS `TokenIn`,
        toInt32(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TickIndex'), `event_attributes`), 2)) AS `TickIndex`,
        -- fix accidental incorrect Fee values added to tranche-ticks before v4 (see https://github.com/neutron-org/neutron/pull/473)
        -- tranches are always effectively a 0% LP fee
        if(
            notEmpty(`TrancheKey`),
            0,
            toUInt64OrZero(tupleElement(arrayLast(x -> (tupleElement(x, 1) = 'Fee'), `event_attributes`), 2))
        ) AS `Fee`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TrancheKey'), `event_attributes`), 2) AS `TrancheKey`,
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'Reserves'), `event_attributes`), 2)) AS `Reserves`,
        -- add new fields after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'SwapAmountIn'), `event_attributes`), 2)) AS `SwapAmountIn`,
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'SwapAmountOut'), `event_attributes`), 2)) AS `SwapAmountOut`,
        -- add computed `is_swap` field for DEX v1-5 swap-volume fix
        if (`SwapAmountIn` > 0, 1, `is_pre_v6_estimated_swap`) as `is_swap`,
        if (`SwapAmountIn` > 0, 0, `is_pre_v6_estimated_swap`)  as `is_estimated_swap`
    FROM spacebox.parsed_dex_message_event
    ARRAY JOIN (
        -- Extract "message part" events with tx_index
        arrayFlatten(
            arrayMap(
                (dex_msg_part_events) -> arrayMap(
                    (related_msg_dex_spent_event, related_msg_dex_received_event) -> arrayMap(
                        (related_msg_dex_received_denom, is_v6) -> arrayMap(
                            (msg_part_event, msg_part_event_index) -> arrayMap(
                                (tick_update_event) -> (
                                    -- event_tuple.1: event_index
                                    toInt32(`msg_part_events_index_offset` + msg_part_event_index - 1),
                                    -- event_tuple.2: event_type
                                    tupleElement(tick_update_event, 1),
                                    -- event_tuple.3: event_attributes
                                    tupleElement(tick_update_event, 2),
                                    -- event_tuple.4: is_pre_v6_estimated_swap
                                    is_v6 = 0 AND
                                    notEmpty(related_msg_dex_received_denom) AND
                                    -- is_swap part: exclude if event denom is the related msg dex received denom
                                    related_msg_dex_received_denom != tupleElement(
                                        arrayFirst(
                                            x -> tupleElement(x, 1) = 'TokenIn',
                                            tupleElement(tick_update_event, 2)
                                        ),
                                        2
                                    )
                                ),
                                -- filter to only TickUpdate events
                                arrayFilter(
                                    msg_part_event -> tupleElement(msg_part_event, 1) = 'TickUpdate',
                                    [msg_part_event]
                                )
                            ),
                            -- enumerate each (msg_part_event, msg_part_event_index) within a message part
                            dex_msg_part_events,
                            arrayEnumerate(dex_msg_part_events)
                        ),
                        -- precompute related_msg "fields" as arrayMap lambda arguments
                        -- - related_msg "field" related_msg_dex_received_denom
                        [if(
                            notEmpty(related_msg_dex_spent_event.1) AND
                            notEmpty(related_msg_dex_received_event.1),
                            -- calculate related msg dex received denom
                            regexpExtract(
                                arrayFirst(
                                    (coin) -> not(match(coin, '^\\d+neutron\/pool\/\\d+$')),
                                    -- separate out each denom in the coin event
                                    splitByChar(
                                        ',',
                                        tupleElement(
                                            arrayFirst(
                                                (attr) -> (tupleElement(attr, 1) = 'amount'),
                                                tupleElement(related_msg_dex_received_event, 2)
                                            ),
                                            2
                                        )
                                    )
                                ),
                                '^\\d+(.+)$',
                                1
                            ),
                            ''
                        )],
                        -- - related_msg "field" is_v6 (to flag what is a v6 swap)
                        [
                            notEmpty(related_msg_dex_spent_event.1) AND
                            notEmpty(related_msg_dex_received_event.1) AND
                            arrayExists(
                                (msg_part_event) -> (
                                    tupleElement(msg_part_event, 1) = 'TickUpdate' AND
                                    arrayExists(
                                        (attr) -> tupleElement(attr, 1) = 'SwapAmountIn',
                                        tupleElement(msg_part_event, 2)
                                    )
                                ),
                                dex_msg_part_events
                            )
                        ]
                    ),
                    -- precompute related_msg "fields" as arrayMap lambda arguments
                    -- - related_msg "field" related_msg_dex_spent_event
                    [arrayFirst(
                        (related_msg_event) -> (
                            -- get first non-neutron/pool/ denom coin event
                            arrayExists(
                                (coin) -> not(match(coin, '^\\d+neutron\/pool\/\\d+$')),
                                -- separate out each denom in the coin event
                                splitByChar(
                                    ',',
                                    tupleElement(
                                        arrayFirst(
                                            (attr) -> (tupleElement(attr, 1) = 'amount'),
                                            tupleElement(related_msg_event, 2)
                                        ),
                                        2
                                    )
                                )
                            )
                        ),
                        -- filter related msg events to 'coin_spent' events
                        arrayFilter(
                            (related_msg_event) -> (
                                tupleElement(related_msg_event, 1) = 'coin_spent' AND
                                arrayExists(
                                    (related_msg_event_attributes) -> (
                                        tupleElement(related_msg_event_attributes, 1) = 'spender' AND
                                        tupleElement(related_msg_event_attributes, 2) = dex_address
                                    ),
                                    tupleElement(related_msg_event, 2)
                                )
                            ),
                            dex_msg_part_events
                        )
                    )],
                    -- - related_msg "field" related_msg_dex_received_event
                    [arrayFirst(
                        (related_msg_event) -> (
                            -- get first non-neutron/pool/ denom coin event
                            arrayExists(
                                (coin) -> not(match(coin, '^\\d+neutron\/pool\/\\d+$')),
                                -- separate out each denom in the coin event
                                splitByChar(
                                    ',',
                                    tupleElement(
                                        arrayFirst(
                                            (attr) -> (tupleElement(attr, 1) = 'amount'),
                                            tupleElement(related_msg_event, 2)
                                        ),
                                        2
                                    )
                                )
                            )
                        ),
                        -- filter related msg events to 'coin_received' events
                        arrayFilter(
                            (related_msg_event) -> (
                                tupleElement(related_msg_event, 1) = 'coin_received' AND
                                arrayExists(
                                    (related_msg_event_attributes) -> (
                                        tupleElement(related_msg_event_attributes, 1) = 'receiver' AND
                                        tupleElement(related_msg_event_attributes, 2) = dex_address
                                    ),
                                    tupleElement(related_msg_event, 2)
                                )
                            ),
                            dex_msg_part_events
                        )
                    )]
                ),
                -- filter to only messages that contain a tick update
                -- this allows us to calculate the dex_spent_event and dex_received_event for all tick parts only once
                arrayFilter(
                    (msg_part_events_parsed) -> arrayExists(
                        (msg_part_event) -> tupleElement(msg_part_event, 1) = 'TickUpdate',
                        msg_part_events_parsed
                    ),
                    [`msg_part_events_parsed`]
                )
            )
        )
    ) AS `event_tuple`
SETTINGS
    -- the array joins in the `is_pre_v6_estimated_swap` fix are heavy on memory usage
    -- better for this to be slow than crash the application
    max_block_size = 100;

-- spacebox.dex_message_event_tick_state latest version of reserve data table

CREATE TABLE spacebox.dex_message_event_tick_state
(
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `TrancheKey`        String,
    `Reserves`          UInt256,
    -- add field to hint to Clickhouse that field is "deleted" (will not actually be deleted)
    `ReservesZero`      Boolean MATERIALIZED `Reserves` = 0,
    -- create version number by combining all indexes together into a large (256 bit) space
    `version`           UInt256 MATERIALIZED
        -- add in order from lowest to highest ordering effect
        (`event_index`        * toUInt256(1))
        + (`tx_index`         * toUInt256(4294967296))              -- + shift by 32 event_index bits (2^32)
        + (`block_part_index` * toUInt256(18446744073709551616))    -- + shift by 32 tx_index bits (2^64)
        + (`height`           * toUInt256(4722366482869645213696))  -- + shift by 8 part_index bits (2^72)
)
-- use ReplacingMergeTree to ensure (eventually) no duplicates of the ORDER BY fields + `version`
ENGINE = ReplacingMergeTree(`version`, `ReservesZero`)
ORDER BY (
    -- in general the data will be queried in pairs
    -- "partition by" pairs
    `TokenZero`,
    `TokenOne`,
    `TokenIn`,
    -- "order by" pool
    `TickIndex`,
    `Fee`,
    `TrancheKey`
)
SETTINGS index_granularity = 8192;

-- spacebox.dex_message_event_tick_state_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_event_tick_state_writer TO spacebox.dex_message_event_tick_state (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `TrancheKey`        String,
    `Reserves`          UInt256
) AS
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    `TokenZero`,
    `TokenOne`,
    `TokenIn`,
    `TickIndex`,
    `Fee`,
    `TrancheKey`,
    `Reserves`
FROM spacebox.dex_message_event_tick_update;
