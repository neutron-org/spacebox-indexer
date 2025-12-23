
-- spacebox.dex_message_event_withdraw_lp table

CREATE TABLE spacebox.dex_message_event_withdraw_lp
(
    `timestamp`             DateTime64(9),
    `height`                Int64,
    `block_part_index`      Int8,
    `tx_index`              Int32,
    `event_index`           Int32,
    -- add computed sort key for easier event ordering
    `sort_key`          Tuple(Int64, Int8, Int32, Int32)
                        MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`),
    -- event data
    `type`                  LowCardinality(String),
    `action`                LowCardinality(String),
    `Creator`               String,
    `Receiver`              String,
    `TokenZero`             LowCardinality(String),
    `TokenOne`              LowCardinality(String),
    `TickIndex`             Int64,
    `Fee`                   UInt64,
    `ReservesZeroWithdrawn` UInt256,
    `ReservesOneWithdrawn`  UInt256,
    `SharesRemoved`         UInt256,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for token pair specific queries
    INDEX `pair_index` (`TokenZero`, `TokenOne`) TYPE set(0),
    -- add index for user pool queries
    INDEX `user_pair_pool_index` (`Receiver`, `TokenZero`, `TokenOne`, `Fee`) TYPE bloom_filter(0.01)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (
    -- the minimum unique parts needed to describe a unique Dex event
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`
)
SETTINGS index_granularity = 8192;

-- spacebox.preparsed_dex_message_event_withdraw_lp_writer source

CREATE MATERIALIZED VIEW spacebox.preparsed_dex_message_event_withdraw_lp_writer TO spacebox.dex_message_event_withdraw_lp (
    `timestamp`             DateTime64(9),
    `height`                Int64,
    `block_part_index`      Int8,
    `tx_index`              Int32,
    `event_index`           Int32,
    -- event data
    `type`                  LowCardinality(String),
    `action`                LowCardinality(String),
    `Creator`               String,
    `Receiver`              String,
    `TokenZero`             LowCardinality(String),
    `TokenOne`              LowCardinality(String),
    `TickIndex`             Int64,
    `Fee`                   UInt64,
    `ReservesZeroWithdrawn` UInt256,
    `ReservesOneWithdrawn`  UInt256,
    `SharesRemoved`         UInt256
) AS
    WITH JSONExtractArrayRaw(`attributes`) as `event_attributes`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `type`,
        `action`,
        -- add DEX event attributes
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'Creator'), `event_attributes`), 2) AS `Creator`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'Receiver'), `event_attributes`), 2) AS `Receiver`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenZero'), `event_attributes`), 2) AS `TokenZero`,
        tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TokenOne'), `event_attributes`), 2) AS `TokenOne`,
        toInt64(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'TickIndex'), `event_attributes`), 2)) AS `TickIndex`,
        toUInt64(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'Fee'), `event_attributes`), 2)) AS `Fee`,
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'ReservesZeroWithdrawn'), `event_attributes`), 2)) AS `ReservesZeroWithdrawn`,
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'ReservesOneWithdrawn'), `event_attributes`), 2)) AS `ReservesOneWithdrawn`,
        toUInt256OrZero(tupleElement(arrayFirst(x -> (tupleElement(x, 1) = 'SharesRemoved'), `event_attributes`), 2)) AS `SharesRemoved`
    FROM spacebox.preparsed_dex_message_event_action
    WHERE `action` = 'WithdrawLP'
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;
