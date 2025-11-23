
-- spacebox.dex_message_event_tranche_user_update table

CREATE TABLE spacebox.dex_message_event_tranche_user_update
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
    `Creator`           String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `TrancheKey`        String,
    `SharesOwned`       UInt256,
    `SharesWithdrawn`   UInt256,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for token pair specific queries
    INDEX `pair_index` (`TokenZero`, `TokenOne`, `TokenIn`) TYPE set(0),
    -- add index for user tranche queries
    INDEX `user_pair_tranche_key_index` (`Creator`, `TokenZero`, `TokenOne`, `TokenIn`, `TrancheKey`) TYPE bloom_filter(0.01),
    -- add index for tranche queries
    INDEX `tranche_key_index` (`TrancheKey`) TYPE bloom_filter(0.01))
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (
    -- the minimum unique parts needed to describe a unique TrancheUserUpdate position
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`
)
SETTINGS index_granularity = 8192;

-- spacebox.preparsed_dex_message_event_tranche_user_update_writer source

CREATE MATERIALIZED VIEW spacebox.preparsed_dex_message_event_tranche_user_update_writer TO spacebox.dex_message_event_tranche_user_update (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `Creator`           String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `TrancheKey`        String,
    `SharesOwned`       UInt256,
    `SharesWithdrawn`   UInt256
) AS
    WITH
        -- define event_tuple parts for row fields
        event_tuple.1 as `event_index`,
        event_tuple.2 as `event_type`,
        event_tuple.3 as `event_attributes`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `event_type` as `type`,
        -- add event attributes
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'action'), `event_attributes`), 2) AS `action`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'Creator'), `event_attributes`), 2) AS `Creator`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenZero'), `event_attributes`), 2) AS `TokenZero`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenOne'), `event_attributes`), 2) AS `TokenOne`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenIn'), `event_attributes`), 2) AS `TokenIn`,
        toInt32(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TickIndex'), `event_attributes`), 2)) AS `TickIndex`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TrancheKey'), `event_attributes`), 2) AS `TrancheKey`,
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'SharesOwned'), `event_attributes`), 2)) AS `SharesOwned`,
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'SharesWithdrawn'), `event_attributes`), 2)) AS `SharesWithdrawn`
    FROM spacebox.parsed_dex_message_event
    ARRAY JOIN (
        -- Extract "message part" events with event_index
        arrayFlatten(
            arrayMap(
                (msg_part_event, msg_part_event_index) -> arrayMap(
                    (tranche_user_update_event) -> (
                        -- event_tuple.1: event_index
                        toInt32(`msg_part_events_index_offset` + msg_part_event_index - 1),
                        -- event_tuple.2: event_type
                        tupleElement(tranche_user_update_event, 1),
                        -- event_tuple.3: event_attributes
                        tupleElement(tranche_user_update_event, 2)
                    ),
                    -- filter to only TrancheUserUpdate events
                    arrayFilter(
                        msg_part_event -> tupleElement(msg_part_event, 1) = 'TrancheUserUpdate',
                        [msg_part_event]
                    )
                ),
                -- enumerate each (msg_part_event, msg_part_event_index) within a message part
                `msg_part_events_parsed`,
                arrayEnumerate(`msg_part_events_parsed`)
            )
        )
    ) AS `event_tuple`
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;
