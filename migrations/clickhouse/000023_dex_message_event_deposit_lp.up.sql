
-- spacebox.dex_message_event_deposit_lp table

CREATE TABLE spacebox.dex_message_event_deposit_lp
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
    -- AmountIn first appears in mainnet height 22,061,642
    `AmountInTokenZero`     UInt256 DEFAULT `ReservesZeroDeposited`,
    `AmountInTokenOne`      UInt256 DEFAULT `ReservesOneDeposited`,
    `ReservesZeroDeposited` UInt256,
    `ReservesOneDeposited`  UInt256,
    `ReservesZeroSwappedIn` UInt256 DEFAULT 0,
    `ReservesOneSwappedIn`  UInt256 DEFAULT 0,
    `ReservesZeroSwappedOut` UInt256 DEFAULT 0,
    `ReservesOneSwappedOut` UInt256 DEFAULT 0,
    `SharesMinted`          UInt256,
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

-- spacebox.dex_message_event_deposit_lp_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_event_deposit_lp_writer TO spacebox.dex_message_event_deposit_lp (
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
    `AmountInTokenZero`     UInt256,
    `AmountInTokenOne`      UInt256,
    `ReservesZeroDeposited` UInt256,
    `ReservesOneDeposited`  UInt256,
    `ReservesZeroSwappedIn` UInt256,
    `ReservesOneSwappedIn`  UInt256,
    `ReservesZeroSwappedOut` UInt256,
    `ReservesOneSwappedOut` UInt256,
    `SharesMinted`          UInt256
) AS
    WITH
        JSONExtractArrayRaw(`attributes`) as `event_attributes`,
        -- get possibly defined (since Neutron v6.0) attributes
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'AmountInTokenZero'), `event_attributes`), 'value') AS `AmountInTokenZeroString`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'AmountInTokenOne'), `event_attributes`), 'value') AS `AmountInTokenOneString`
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `type`,
        `action`,
        -- add DEX event attributes
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Creator'), `event_attributes`), 'value') AS `Creator`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Receiver'), `event_attributes`), 'value') AS `Receiver`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenZero'), `event_attributes`), 'value') AS `TokenZero`,
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TokenOne'), `event_attributes`), 'value') AS `TokenOne`,
        toInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'TickIndex'), `event_attributes`), 'value')) AS `TickIndex`,
        toUInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'Fee'), `event_attributes`), 'value')) AS `Fee`,
        if(notEmpty(`AmountInTokenZeroString`), toUInt256OrZero(`AmountInTokenZeroString`), `ReservesZeroDeposited`) AS `AmountInTokenZero`,
        if(notEmpty(`AmountInTokenOneString`), toUInt256OrZero(`AmountInTokenOneString`), `ReservesOneDeposited`) AS `AmountInTokenOne`,
        toUInt256OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'ReservesZeroDeposited'), `event_attributes`), 'value')) AS `ReservesZeroDeposited`,
        toUInt256OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'ReservesOneDeposited'), `event_attributes`), 'value')) AS `ReservesOneDeposited`,
        if (`AmountInTokenZero` > `ReservesZeroDeposited`, toUInt256(`AmountInTokenZero` - `ReservesZeroDeposited`), 0) AS `ReservesZeroSwappedIn`,
        if (`AmountInTokenOne` > `ReservesOneDeposited`, toUInt256(`AmountInTokenOne` - `ReservesOneDeposited`), 0) AS `ReservesOneSwappedIn`,
        if (`AmountInTokenZero` < `ReservesZeroDeposited`, toUInt256(`ReservesZeroDeposited` - `AmountInTokenZero`), 0) AS `ReservesZeroSwappedOut`,
        if (`AmountInTokenOne` < `ReservesOneDeposited`, toUInt256(`ReservesOneDeposited` - `AmountInTokenOne`), 0) AS `ReservesOneSwappedOut`,
        toUInt256OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'SharesMinted'), `event_attributes`), 'value')) AS `SharesMinted`
    FROM spacebox.dex_message_event_action
    WHERE `action` = 'DepositLP'
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;
