
-- spacebox.dex_swaps table

CREATE TABLE spacebox.dex_swaps_valued
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
    -- price information
    `price_timestamp`   DateTime64(9),
    `value_in_0`        Float64,
    `value_in_1`        Float64,
    `value_fee_0`       Float64,
    `value_fee_1`       Float64,
    `value_out_0`       Float64, -- should be equal to ~(value_in_1 - value_fee_1)
    `value_out_1`       Float64, -- should be equal to ~(value_in_0 - value_fee_0)
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree(`price_timestamp`)
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS
    -- see docs: https://clickhouse.com/docs/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.dex_swaps_valued_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_swaps_valued_writer TO spacebox.dex_swaps_valued (
    `timestamp`         DateTime64(9),
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
    `ReservesOutOne`    UInt256,
    -- price information
    `price_timestamp`   DateTime64(9),
    `value_in_0`        Float64,
    `value_in_1`        Float64,
    `value_fee_0`       Float64,
    `value_fee_1`       Float64,
    `value_out_0`       Float64, -- should be equal to ~(value_in_1 - value_fee_1)
    `value_out_1`       Float64  -- should be equal to ~(value_in_0 - value_fee_0)
) AS
WITH
    source AS (SELECT * FROM spacebox.dex_swaps),
    swaps_valued AS (
        WITH
            p."token_0_price" as "token_price_0",
            p."token_1_price" as "token_price_1",
            if(s."ReservesOutZero" < s."ReservesInZero", s."ReservesInZero" - s."ReservesOutZero", 0) as "amount_in_0",
            if(s."ReservesOutOne" < s."ReservesInOne", s."ReservesInOne" - s."ReservesOutOne", 0) as "amount_in_1",
            if(s."ReservesOutZero" > s."ReservesInZero", s."ReservesOutZero" - s."ReservesInZero", 0) as "amount_out_0",
            if(s."ReservesOutOne" > s."ReservesInOne", s."ReservesOutOne" - s."ReservesInOne", 0) as "amount_out_1",
            "token_price_0" * toFloat64("amount_in_0") as "value_in_0",
            "token_price_1" * toFloat64("amount_in_1") as "value_in_1",
            "value_in_0" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_0",
            "value_in_1" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_1",
            "token_price_0" * toFloat64("amount_out_0") as "value_out_0",
            "token_price_1" * toFloat64("amount_out_1") as "value_out_1"
        SELECT
            s."timestamp" as "timestamp",
            s."height" as "height",
            s."block_part_index" as "block_part_index",
            s."tx_index" as "tx_index",
            s."event_index" as "event_index",
            -- event data
            s."type" as "type",
            s."action" as "action",
            s."Receiver" as "Receiver",
            s."TokenZero" as "TokenZero",
            s."TokenOne" as "TokenOne",
            s."TickIndex" as "TickIndex",
            s."Fee" as "Fee",
            s."TrancheKey" as "TrancheKey",
            s."ReservesInZero" as "ReservesInZero",
            s."ReservesInOne" as "ReservesInOne",
            s."ReservesOutZero" as "ReservesOutZero",
            s."ReservesOutOne" as "ReservesOutOne",
            -- price information
            0 as "price_timestamp",
            "value_in_0",
            "value_in_1",
            "value_fee_0",
            "value_fee_1",
            "value_out_0",
            "value_out_1",
            p."timestamp" as "sort_key",
            p."contract_address" as "contract_address"
        FROM source as s
        -- join to multiple configured vaults
        LEFT JOIN spacebox.dex_vaults_config_state as v
            ON (s."TokenZero" = v."token_0_denom")
            AND (s."TokenOne" = v."token_1_denom")
        -- join to prices of multiple configured vaults
        LEFT JOIN spacebox.price_by_vault_denom_state as p
            ON (v."contract_address" = p."contract_address")
        WHERE v."token_0_quote_currency" = 'USD'
          AND v."token_1_quote_currency" = 'USD'
    )
  -- select rows with the freshest price data of all vault price data
  SELECT
    argMax("timestamp", "sort_key") as "timestamp",
    "height",
    "block_part_index",
    "tx_index",
    "event_index",
    -- event data
    argMax("type", "sort_key") as "type",
    argMax("action", "sort_key") as "action",
    argMax("Receiver", "sort_key") as "Receiver",
    argMax("TokenZero", "sort_key") as "TokenZero",
    argMax("TokenOne", "sort_key") as "TokenOne",
    argMax("TickIndex", "sort_key") as "TickIndex",
    argMax("Fee", "sort_key") as "Fee",
    argMax("TrancheKey", "sort_key") as "TrancheKey",
    argMax("ReservesInZero", "sort_key") as "ReservesInZero",
    argMax("ReservesInOne", "sort_key") as "ReservesInOne",
    argMax("ReservesOutZero", "sort_key") as "ReservesOutZero",
    argMax("ReservesOutOne", "sort_key") as "ReservesOutOne",
    -- price information
    argMax("price_timestamp", "sort_key") as "price_timestamp",
    argMax("value_in_0", "sort_key") as "value_in_0",
    argMax("value_in_1", "sort_key") as "value_in_1",
    argMax("value_fee_0", "sort_key") as "value_fee_0",
    argMax("value_fee_1", "sort_key") as "value_fee_1",
    argMax("value_out_0", "sort_key") as "value_out_0",
    argMax("value_out_1", "sort_key") as "value_out_1"
  FROM swaps_valued
  GROUP BY
    "height",
    "block_part_index",
    "tx_index",
    "event_index";


-- spacebox.dex_swaps_valued_again_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_swaps_valued_again_writer
REFRESH EVERY 5 MINUTE OFFSET 30 SECOND RANDOMIZE FOR 20 SECOND
APPEND
TO spacebox.dex_swaps_valued (
    `timestamp`         DateTime64(9),
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
    `ReservesOutOne`    UInt256,
    -- price information
    `price_timestamp`   DateTime64(9),
    `value_in_0`        Float64,
    `value_in_1`        Float64,
    `value_fee_0`       Float64,
    `value_fee_1`       Float64,
    `value_out_0`       Float64, -- should be equal to ~(value_in_1 - value_fee_1)
    `value_out_1`       Float64, -- should be equal to ~(value_in_0 - value_fee_0)
) AS
WITH
    source as (
        -- get recently valued rows of unsure price times
        SELECT
          `timestamp`,
          `height`,
          `block_part_index`,
          `tx_index`,
          `event_index`,
          -- event data
          `type`,
          `action`,
          `Receiver`,
          `TokenZero`,
          `TokenOne`,
          `TickIndex`,
          `Fee`,
          `TrancheKey`,
          `ReservesInZero`,
          `ReservesInOne`,
          `ReservesOutZero`,
          `ReservesOutOne`
        FROM spacebox.dex_swaps_valued
        -- allow overwriting valuation of new shares several times
        -- note: this data can be stale if shares or price data failed to
        --       update for the period of time within this WHERE condition
        WHERE `timestamp` > addHours(NOW(), -1)
            OR `price_timestamp` = 0
    ),
    swaps_valued AS (
        WITH
            p."token_0_price" as "token_price_0",
            p."token_1_price" as "token_price_1",
            if(s."ReservesOutZero" < s."ReservesInZero", s."ReservesInZero" - s."ReservesOutZero", 0) as "amount_in_0",
            if(s."ReservesOutOne" < s."ReservesInOne", s."ReservesInOne" - s."ReservesOutOne", 0) as "amount_in_1",
            if(s."ReservesOutZero" > s."ReservesInZero", s."ReservesOutZero" - s."ReservesInZero", 0) as "amount_out_0",
            if(s."ReservesOutOne" > s."ReservesInOne", s."ReservesOutOne" - s."ReservesInOne", 0) as "amount_out_1",
            "token_price_0" * toFloat64("amount_in_0") as "value_in_0",
            "token_price_1" * toFloat64("amount_in_1") as "value_in_1",
            "value_in_0" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_0",
            "value_in_1" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_1",
            "token_price_0" * toFloat64("amount_out_0") as "value_out_0",
            "token_price_1" * toFloat64("amount_out_1") as "value_out_1"
        SELECT
            s."timestamp" as "timestamp",
            s."height" as "height",
            s."block_part_index" as "block_part_index",
            s."tx_index" as "tx_index",
            s."event_index" as "event_index",
            -- event data
            s."type" as "type",
            s."action" as "action",
            s."Receiver" as "Receiver",
            s."TokenZero" as "TokenZero",
            s."TokenOne" as "TokenOne",
            s."TickIndex" as "TickIndex",
            s."Fee" as "Fee",
            s."TrancheKey" as "TrancheKey",
            s."ReservesInZero" as "ReservesInZero",
            s."ReservesInOne" as "ReservesInOne",
            s."ReservesOutZero" as "ReservesOutZero",
            s."ReservesOutOne" as "ReservesOutOne",
            -- price information
            -- prevent `price_timestamp` = 0 rows from being re-processed forever
            -- by setting price=0 with p_first timestamps for times that are too early
            if(s."timestamp" >= p_first."timestamp", p."timestamp", p_first."timestamp") as "price_timestamp",
            "value_in_0",
            "value_in_1",
            "value_fee_0",
            "value_fee_1",
            "value_out_0",
            "value_out_1",
            "price_timestamp" as "sort_key"
        FROM source as s
        LEFT JOIN spacebox.dex_vaults_config_state as v
            ON (s."TokenZero" = v."token_0_denom")
            AND (s."TokenOne" = v."token_1_denom")
        ANY LEFT JOIN spacebox.price_by_vault_denom_first_state as p_first
            ON (v."contract_address" = p_first."contract_address")
        ASOF LEFT JOIN spacebox.price_by_vault_denom as p
            ON (v."contract_address" = p."contract_address")
            AND s."timestamp" >= p."timestamp"
        WHERE p_first."timestamp" > 0
          AND v."token_0_quote_currency" = 'USD'
          AND v."token_1_quote_currency" = 'USD'
    )
  -- select rows with the freshest price data of all vault price data
  SELECT
    argMax("timestamp", "sort_key") as "timestamp",
    "height",
    "block_part_index",
    "tx_index",
    "event_index",
    -- event data
    argMax("type", "sort_key") as "type",
    argMax("action", "sort_key") as "action",
    argMax("Receiver", "sort_key") as "Receiver",
    argMax("TokenZero", "sort_key") as "TokenZero",
    argMax("TokenOne", "sort_key") as "TokenOne",
    argMax("TickIndex", "sort_key") as "TickIndex",
    argMax("Fee", "sort_key") as "Fee",
    argMax("TrancheKey", "sort_key") as "TrancheKey",
    argMax("ReservesInZero", "sort_key") as "ReservesInZero",
    argMax("ReservesInOne", "sort_key") as "ReservesInOne",
    argMax("ReservesOutZero", "sort_key") as "ReservesOutZero",
    argMax("ReservesOutOne", "sort_key") as "ReservesOutOne",
    -- price information
    argMax("price_timestamp", "sort_key") as "price_timestamp",
    argMax("value_in_0", "sort_key") as "value_in_0",
    argMax("value_in_1", "sort_key") as "value_in_1",
    argMax("value_fee_0", "sort_key") as "value_fee_0",
    argMax("value_fee_1", "sort_key") as "value_fee_1",
    argMax("value_out_0", "sort_key") as "value_out_0",
    argMax("value_out_1", "sort_key") as "value_out_1"
  FROM swaps_valued
  GROUP BY
    "height",
    "block_part_index",
    "tx_index",
    "event_index";


CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_swaps_valued_daily_writer
REFRESH EVERY 1 DAY RANDOMIZE FOR 1 HOUR
APPEND
TO spacebox.dex_swaps_valued (
    `timestamp`         DateTime64(9),
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
    `ReservesOutOne`    UInt256,
    -- price information
    `price_timestamp`   DateTime64(9),
    `value_in_0`        Float64,
    `value_in_1`        Float64,
    `value_fee_0`       Float64,
    `value_fee_1`       Float64,
    `value_out_0`       Float64, -- should be equal to ~(value_in_1 - value_fee_1)
    `value_out_1`       Float64, -- should be equal to ~(value_in_0 - value_fee_0)
) AS
WITH
    source as (
        -- get recently valued rows of unsure price times
        SELECT
          `timestamp`,
          `height`,
          `block_part_index`,
          `tx_index`,
          `event_index`,
          -- event data
          `type`,
          `action`,
          `Receiver`,
          `TokenZero`,
          `TokenOne`,
          `TickIndex`,
          `Fee`,
          `TrancheKey`,
          `ReservesInZero`,
          `ReservesInOne`,
          `ReservesOutZero`,
          `ReservesOutOne`
        FROM spacebox.dex_swaps_valued
        -- allow overwriting valuation of new shares several times
        -- note: this data can be stale if shares or price data failed to
        --       update for the period of time within this WHERE condition
        WHERE addMinutes(`price_timestamp`, 1) < `timestamp`
    ),
    swaps_valued AS (
        WITH
            p."token_0_price" as "token_price_0",
            p."token_1_price" as "token_price_1",
            if(s."ReservesOutZero" < s."ReservesInZero", s."ReservesInZero" - s."ReservesOutZero", 0) as "amount_in_0",
            if(s."ReservesOutOne" < s."ReservesInOne", s."ReservesInOne" - s."ReservesOutOne", 0) as "amount_in_1",
            if(s."ReservesOutZero" > s."ReservesInZero", s."ReservesOutZero" - s."ReservesInZero", 0) as "amount_out_0",
            if(s."ReservesOutOne" > s."ReservesInOne", s."ReservesOutOne" - s."ReservesInOne", 0) as "amount_out_1",
            "token_price_0" * toFloat64("amount_in_0") as "value_in_0",
            "token_price_1" * toFloat64("amount_in_1") as "value_in_1",
            "value_in_0" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_0",
            "value_in_1" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_1",
            "token_price_0" * toFloat64("amount_out_0") as "value_out_0",
            "token_price_1" * toFloat64("amount_out_1") as "value_out_1"
        SELECT
            s."timestamp" as "timestamp",
            s."height" as "height",
            s."block_part_index" as "block_part_index",
            s."tx_index" as "tx_index",
            s."event_index" as "event_index",
            -- event data
            s."type" as "type",
            s."action" as "action",
            s."Receiver" as "Receiver",
            s."TokenZero" as "TokenZero",
            s."TokenOne" as "TokenOne",
            s."TickIndex" as "TickIndex",
            s."Fee" as "Fee",
            s."TrancheKey" as "TrancheKey",
            s."ReservesInZero" as "ReservesInZero",
            s."ReservesInOne" as "ReservesInOne",
            s."ReservesOutZero" as "ReservesOutZero",
            s."ReservesOutOne" as "ReservesOutOne",
            -- price information
            -- prevent `price_timestamp` = 0 rows from being re-processed forever
            -- by setting price=0 with p_first timestamps for times that are too early
            if(s."timestamp" >= p_first."timestamp", p."timestamp", p_first."timestamp") as "price_timestamp",
            "value_in_0",
            "value_in_1",
            "value_fee_0",
            "value_fee_1",
            "value_out_0",
            "value_out_1",
            "price_timestamp" as "sort_key"
        FROM source as s
        LEFT JOIN spacebox.dex_vaults_config_state as v
            ON (s."TokenZero" = v."token_0_denom")
            AND (s."TokenOne" = v."token_1_denom")
        ANY LEFT JOIN spacebox.price_by_vault_denom_first_state as p_first
            ON (v."contract_address" = p_first."contract_address")
        ASOF LEFT JOIN spacebox.price_by_vault_denom as p
            ON (v."contract_address" = p."contract_address")
            AND s."timestamp" >= p."timestamp"
        WHERE p_first."timestamp" > 0
          AND v."token_0_quote_currency" = 'USD'
          AND v."token_1_quote_currency" = 'USD'
    )
  -- select rows with the freshest price data of all vault price data
  SELECT
    argMax("timestamp", "sort_key") as "timestamp",
    "height",
    "block_part_index",
    "tx_index",
    "event_index",
    -- event data
    argMax("type", "sort_key") as "type",
    argMax("action", "sort_key") as "action",
    argMax("Receiver", "sort_key") as "Receiver",
    argMax("TokenZero", "sort_key") as "TokenZero",
    argMax("TokenOne", "sort_key") as "TokenOne",
    argMax("TickIndex", "sort_key") as "TickIndex",
    argMax("Fee", "sort_key") as "Fee",
    argMax("TrancheKey", "sort_key") as "TrancheKey",
    argMax("ReservesInZero", "sort_key") as "ReservesInZero",
    argMax("ReservesInOne", "sort_key") as "ReservesInOne",
    argMax("ReservesOutZero", "sort_key") as "ReservesOutZero",
    argMax("ReservesOutOne", "sort_key") as "ReservesOutOne",
    -- price information
    argMax("price_timestamp", "sort_key") as "price_timestamp",
    argMax("value_in_0", "sort_key") as "value_in_0",
    argMax("value_in_1", "sort_key") as "value_in_1",
    argMax("value_fee_0", "sort_key") as "value_fee_0",
    argMax("value_fee_1", "sort_key") as "value_fee_1",
    argMax("value_out_0", "sort_key") as "value_out_0",
    argMax("value_out_1", "sort_key") as "value_out_1"
  FROM swaps_valued
  GROUP BY
    "height",
    "block_part_index",
    "tx_index",
    "event_index";

