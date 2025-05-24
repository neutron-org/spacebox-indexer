
-- spacebox.dex_shares_by_pool_agg table

-- note: this is a temporary fix, aggregation should be done by pool ID not pool attributes
--       because pool shares can be transferred, bank transfers are the real source of truth
CREATE TABLE spacebox.dex_shares_by_pool_agg
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
    `TickIndex`         Int64,
    `Fee`               UInt64,
    -- save boolean for credit/debit
    `shares_in`         UInt128,
    `shares_out`        UInt128,
    -- aggregation state information
    `user_shares`       UInt128,
    `total_shares`      UInt128,
    `updated_at`        DateTime MATERIALIZED nowInBlock(),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for height timeseries queries
    INDEX `height_index` (`height`) TYPE minmax,
    -- add index for update queries
    INDEX `updated_at_index` (`updated_at`) TYPE minmax
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree(`updated_at`)
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS index_granularity = 8192;

-- spacebox.dex_shares_by_pool_agg_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_shares_by_pool_agg_writer
REFRESH EVERY 10 MINUTE OFFSET 1 MINUTE
TO spacebox.dex_shares_by_pool_agg (
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
    `TickIndex`         Int64,
    `Fee`               UInt64,
    -- save boolean for credit/debit
    `shares_in`         UInt128,
    `shares_out`        UInt128,
    -- aggregation state information
    `user_shares`       UInt128,
    `total_shares`      UInt128,
) AS
    WITH
        `shares_in` - `shares_out` as `shares_delta`
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
        `TickIndex`,
        `Fee`,
        `SharesMinted` as `shares_in`,
        `SharesRemoved` as `shares_out`,
        sum(`shares_delta`) OVER (
            -- partition sums to each user's pool
            PARTITION BY "TokenZero", "TokenOne", "TickIndex", "Fee", "Receiver"
            ORDER BY "sort_key" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as "user_shares",
        sum(`shares_delta`) OVER (
            -- partition sums to each pool
            PARTITION BY "TokenZero", "TokenOne", "TickIndex", "Fee"
            ORDER BY "sort_key" ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as "total_shares"
    FROM spacebox.dex_shares;
