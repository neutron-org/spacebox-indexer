
-- spacebox.dex_message_event_tick_update table

CREATE TABLE spacebox.dex_message_event_tick_update
(
    `timestamp` DateTime,
    `height` Int64,
    `block_part_index` Int8,
    `tx_index` Int32,
    `event_index` Int32,
    -- event data
    `type` LowCardinality(String),
    `action` LowCardinality(String),
    `TokenZero` LowCardinality(String),
    `TokenOne` LowCardinality(String),
    `TokenIn` LowCardinality(String),
    `TickIndex` Int64,
    `Fee` UInt64,
    -- note: cannot make TrancheKey nullable if it is going to be in an index
    --       LP type ticks will have empty string TrancheKey values
    `TrancheKey` String,
    `Reserves` UInt256,
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

-- spacebox.dex_message_event_tick_update_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_event_tick_update_writer TO spacebox.dex_message_event_tick_update (
    `timestamp` DateTime,
    `height` Int64,
    `block_part_index` Int8,
    `tx_index` Int32,
    `event_index` Int32,
    -- event data
    `type` LowCardinality(String),
    `action` LowCardinality(String),
    `TokenZero` LowCardinality(String),
    `TokenOne` LowCardinality(String),
    `TokenIn` LowCardinality(String),
    `TickIndex` Int64,
    `Fee` UInt64,
    `TrancheKey` String,
    `Reserves` UInt256,
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
            toUInt64OrZero(JSONExtractString(arrayLast(x -> (JSONExtractString(x, 'key') = 'Fee'), `event_attributes`), 'value'))
        ) AS `Fee`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TrancheKey'), `event_attributes`), 'value') AS `TrancheKey`,
        toUInt256OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Reserves'), `event_attributes`), 'value')) AS `Reserves`,
        -- add new fields after DEX v5 (see https://github.com/neutron-org/neutron/pull/808)
        toUInt256OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'SwapAmountIn'), `event_attributes`), 'value')) AS `SwapAmountIn`,
        toUInt256OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'SwapAmountOut'), `event_attributes`), 'value')) AS `SwapAmountOut`,
        -- add computed `is_swap` field for DEX v1-5 swap-volume fix
        if (SwapAmountIn > 0, 1, `calculated_is_swap`) as `is_swap`
    FROM spacebox.dex_message_event
    ARRAY JOIN (
        -- Extract "message part" events with tx_index
        arrayFlatten(
            arrayMap(
                (dex_msg_part_events) -> arrayMap(
                    (related_msg_dex_spent_event, related_msg_dex_received_event) -> arrayMap(
                        (related_msg_dex_received_denom) -> arrayMap(
                            (msg_part_event, msg_part_event_index) -> arrayMap(
                                (tick_update_event) -> (
                                    -- event_tuple.1: event_index
                                    toInt32(`msg_part_events_index_offset` + msg_part_event_index - 1),
                                    -- event_tuple.2: event_type
                                    JSONExtractString(tick_update_event, 'type'),
                                    -- event_tuple.3: event_attributes
                                    JSONExtractArrayRaw(tick_update_event, 'attributes'),
                                    -- event_tuple.4: is_swap
                                    notEmpty(related_msg_dex_received_denom) AND
                                    -- is_swap part: exclude if event denom is the related msg dex received denom
                                    related_msg_dex_received_denom != JSONExtractString(
                                        arrayFirst(
                                            x -> JSONExtractString(x, 'key') = 'TokenIn',
                                            JSONExtractArrayRaw(tick_update_event, 'attributes')
                                        ),
                                        'value'
                                    )
                                ),
                                -- filter to only TickUpdate events
                                arrayFilter(
                                    msg_part_event -> JSONExtractString(msg_part_event, 'type') = 'TickUpdate',
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
                            notEmpty(related_msg_dex_spent_event) AND
                            notEmpty(related_msg_dex_received_event),
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
                                                JSONExtractArrayRaw(related_msg_dex_received_event, 'attributes')
                                            ),
                                            'value'
                                        )
                                    )
                                ),
                                '^\\d+(.+)$',
                                1
                            ),
                            ''
                        )]
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
                                    JSONExtractString(
                                        arrayFirst(
                                            (attr) -> (JSONExtractString(attr, 'key') = 'amount'),
                                            JSONExtractArrayRaw(related_msg_event, 'attributes')
                                        ),
                                        'value'
                                    )
                                )
                            )
                        ),
                        -- filter related msg events to 'coin_spent' events
                        arrayFilter(
                            (related_msg_event) -> (
                                JSONExtractString(related_msg_event, 'type') = 'coin_spent' AND
                                arrayExists(
                                    (related_msg_event_attributes) -> (
                                        JSONExtractString(related_msg_event_attributes, 'key') = 'spender' AND
                                        JSONExtractString(related_msg_event_attributes, 'value') = dex_address
                                    ),
                                    JSONExtractArrayRaw(related_msg_event, 'attributes')
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
                                    JSONExtractString(
                                        arrayFirst(
                                            (attr) -> (JSONExtractString(attr, 'key') = 'amount'),
                                            JSONExtractArrayRaw(related_msg_event, 'attributes')
                                        ),
                                        'value'
                                    )
                                )
                            )
                        ),
                        -- filter related msg events to 'coin_received' events
                        arrayFilter(
                            (related_msg_event) -> (
                                JSONExtractString(related_msg_event, 'type') = 'coin_received' AND
                                arrayExists(
                                    (related_msg_event_attributes) -> (
                                        JSONExtractString(related_msg_event_attributes, 'key') = 'receiver' AND
                                        JSONExtractString(related_msg_event_attributes, 'value') = dex_address
                                    ),
                                    JSONExtractArrayRaw(related_msg_event, 'attributes')
                                )
                            ),
                            dex_msg_part_events
                        )
                    )]
                ),
                -- filter to only messages that contain a tick update
                -- this allows us to calculate the dex_spent_event and dex_received_event for all tick parts only once
                arrayFilter(
                    (msg_part_events) -> arrayExists(
                        (msg_part_event) -> JSONExtractString(msg_part_event, 'type') = 'TickUpdate',
                        msg_part_events
                    ),
                    [`msg_part_events`]
                )
            )
        )
    ) AS `event_tuple`
SETTINGS
    -- the array joins in the `is_swap` fix are heavy on memory usage
    -- better for this to be slow than crash the application
    max_block_size = 100;

-- spacebox.dex_message_event_tick_state latest version of reserve data table

CREATE TABLE spacebox.dex_message_event_tick_state
(
    `timestamp`         DateTime,
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
    `timestamp`         DateTime,
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
