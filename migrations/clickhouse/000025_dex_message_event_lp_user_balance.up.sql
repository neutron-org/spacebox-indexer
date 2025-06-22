
-- spacebox.dex_message_event_lp_user_balance table

CREATE TABLE spacebox.dex_message_event_lp_user_balance
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
    -- align Deposit/Withdrawal pool index to TickUpdate pool indexes by adding
    -- tick indexes specific to each token side
    `TickIndexZero`         Int64,
    `TickIndexOne`          Int64,
    `Fee`                   UInt64,
    `ReservesZeroDeposited` Int256,
    `ReservesOneDeposited`  Int256,
    `shares`                Int256,
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

-- spacebox.dex_message_event_deposit_lp_user_balance_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_event_deposit_lp_user_balance_writer TO spacebox.dex_message_event_lp_user_balance (
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
    `TickIndexZero`         Int64,
    `TickIndexOne`          Int64,
    `Fee`                   UInt64,
    `ReservesZeroDeposited` Int256,
    `ReservesOneDeposited`  Int256,
    `shares`                Int256
) AS
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `type`,
        `action`,
        `Creator`,
        `Receiver`,
        `TokenZero`,
        `TokenOne`,
        `TickIndex`,
        `Fee` - `TickIndex` AS `TickIndexZero`,
        `Fee` + `TickIndex` AS `TickIndexOne`,
        `Fee`,
        `ReservesZeroDeposited`,
        `ReservesOneDeposited`,
        `SharesMinted` as `shares`
    FROM spacebox.dex_message_event_deposit_lp
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;

-- spacebox.dex_message_event_withdraw_lp_user_balance_writer source

CREATE MATERIALIZED VIEW spacebox.dex_message_event_withdraw_lp_user_balance_writer TO spacebox.dex_message_event_lp_user_balance (
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
    `TickIndexZero`         Int64,
    `TickIndexOne`          Int64,
    `Fee`                   UInt64,
    `ReservesZeroDeposited` Int256,
    `ReservesOneDeposited`  Int256,
    `shares`                Int256
) AS
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `type`,
        `action`,
        `Creator`,
        `Receiver`,
        `TokenZero`,
        `TokenOne`,
        `TickIndex`,
        `Fee` - `TickIndex` AS `TickIndexZero`,
        `Fee` + `TickIndex` AS `TickIndexOne`,
        `Fee`,
        -1 * `ReservesZeroWithdrawn` as `ReservesZeroDeposited`,
        -1 * `ReservesOneWithdrawn` as `ReservesOneDeposited`,
        -1 * `SharesRemoved` as `shares`
    FROM spacebox.dex_message_event_withdraw_lp
SETTINGS
    -- allow bigger blocks because transformation is easier
    max_block_size = 1000;
