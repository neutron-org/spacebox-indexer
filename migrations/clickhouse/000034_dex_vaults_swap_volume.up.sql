
-- spacebox.dex_swaps table

CREATE TABLE spacebox.dex_swaps
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
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `Receiver`          Nullable(String),
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `TrancheKey`        Nullable(String),
    `ReservesInZero`    UInt256,
    `ReservesInOne`     UInt256,
    `ReservesOutZero`   UInt256,
    `ReservesOutOne`    UInt256,
    -- aggregation state information
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


-- spacebox.dex_swaps_tick_update_writer source

CREATE MATERIALIZED VIEW spacebox.dex_swaps_tick_update_writer TO spacebox.dex_swaps (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `Receiver`          Nullable(String),
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `TrancheKey`        Nullable(String),
    `ReservesInZero`    UInt256,
    `ReservesInOne`     UInt256,
    `ReservesOutZero`   UInt256,
    `ReservesOutOne`    UInt256
) AS
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `type`,
        `action`,
        null as `Receiver`,
        `TokenZero`,
        `TokenOne`,
        if(
            `TokenIn` = `TokenZero`,
            `Fee` - `TickIndex`,
            `TickIndex` - `Fee`
        ) as `TickIndex`,
        `Fee`,
        if(notEmpty(`TrancheKey`), `TrancheKey`, NULL) as `TrancheKey`,
        -- note: a way to think about this is "a swap reduces the existing liquidity"
        --       because a trader has bought from the available liquidity
        if(`TokenIn` = `TokenOne`, `SwapAmountIn`, 0) as `ReservesInZero`,
        if(`TokenIn` = `TokenZero`, `SwapAmountIn`, 0) as `ReservesInOne`,
        if(`TokenIn` = `TokenZero`, `SwapAmountOut`, 0) as `ReservesOutZero`,
        if(`TokenIn` = `TokenOne`, `SwapAmountOut`, 0) as `ReservesOutOne`
    FROM spacebox.dex_message_event_tick_update
    -- only save swap updates
    WHERE `is_swap` = 1
      AND `SwapAmountIn` > 0;


-- spacebox.dex_swaps_deposit_lp_writer source

CREATE MATERIALIZED VIEW spacebox.dex_swaps_deposit_lp_writer TO spacebox.dex_swaps (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `type`              LowCardinality(String),
    `action`            LowCardinality(String),
    `Receiver`          Nullable(String),
    `TokenZero`         LowCardinality(String),
    `TokenOne`          LowCardinality(String),
    `TickIndex`         Int64,
    `Fee`               UInt64,
    `TrancheKey`        Nullable(String),
    `ReservesInZero`    UInt256,
    `ReservesInOne`     UInt256,
    `ReservesOutZero`   UInt256,
    `ReservesOutOne`    UInt256
) AS
    SELECT
        `timestamp`,
        `height`,
        `block_part_index`,
        `tx_index`,
        `event_index`,
        `type`,
        `action`,
        `Receiver`,
        `TokenZero`,
        `TokenOne`,
        `TickIndex`,
        `Fee`,
        NULL as `TrancheKey`,
        `ReservesZeroSwappedIn` as `ReservesInZero`,
        `ReservesOneSwappedIn` as `ReservesInOne`,
        `ReservesZeroSwappedOut` as `ReservesOutZero`,
        `ReservesOneSwappedOut` as `ReservesOutOne`
    FROM spacebox.dex_message_event_deposit_lp
    -- only save swap updates
    WHERE `ReservesZeroSwappedIn` > 0
       OR `ReservesOneSwappedIn` > 0;
