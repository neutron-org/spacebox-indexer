
-- spacebox.dex_shares_by_pool table

CREATE TABLE spacebox.dex_shares_by_pool
(
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- add computed sort key for easier event ordering
    `sort_key`          Tuple(Int64, Int8, Int32, Int32)
                        MATERIALIZED tuple(`height`, `block_part_index`, `tx_index`, `event_index`),
    -- event data
    `action`            LowCardinality(String),
    `Receiver`          String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `PoolId`            UInt64,
    -- save boolean for credit/debit
    `credit`            Boolean MATERIALIZED `action` = 'DepositLP',
    `shares`            UInt128,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for pool (by attributes) type queries
    INDEX `pool_attributes_index` (`TokenZero`, `TokenOne`, `TokenIn`, `TickIndex`, `Fee`) TYPE set(0),
    -- add index for pool_id type queries
    INDEX `pool_id_index` (`PoolId`) TYPE minmax(0)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`, `TokenIn`)
SETTINGS index_granularity = 8192;

-- spacebox.dex_shares_by_pool_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_shares_by_pool_deposit_writer TO spacebox.dex_shares_by_pool (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `Receiver`          String,
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TokenIn`           LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `PoolId`            UInt64,
    `shares`            UInt128
) AS
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `action`,
        `Receiver`,
        `TokenZero`,
        `TokenOne`,
        `TokenIn`,
        `Fee` + `TickIndex` * if(`TokenZero` = `TokenIn`, -1, 1) `TickIndex`,
        `Fee`,
        toUInt64(`PoolId`) as `PoolId`,
        `shares`
    FROM spacebox.dex_shares
    ARRAY JOIN [`TokenZero`, `TokenOne`] AS `TokenIn`
    -- limit to only known pool ids
    WHERE `PoolId` >= 0
