
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
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `value_in_0`        Float64,
    `value_in_1`        Float64,
    `value_fee_0`       Float64,
    `value_fee_1`       Float64,
    `value_out_0`       Float64, -- should be equal to ~(value_in_1 - value_fee_1)
    `value_out_1`       Float64, -- should be equal to ~(value_in_0 - value_fee_0)
    -- determine most recent version by the recentness of the price data
    `price_version`     UInt64 MATERIALIZED
                            toUnixTimestamp64Milli(`price_timestamp_0`) +
                            toUnixTimestamp64Milli(`price_timestamp_1`),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree(`price_version`)
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
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `value_in_0`        Float64,
    `value_in_1`        Float64,
    `value_fee_0`       Float64,
    `value_fee_1`       Float64,
    `value_out_0`       Float64, -- should be equal to ~(value_in_1 - value_fee_1)
    `value_out_1`       Float64, -- should be equal to ~(value_in_0 - value_fee_0)
) AS
WITH
    source AS (
        SELECT *
        FROM spacebox.dex_swaps
        WHERE notEmpty("Receiver") OR (
            ("TrancheKey" IS NULL) AND (
                -- temp estimation of vault DEX pools by excluding normal DEX users
                ("Fee" NOT IN (1, 5, 10, 20, 50, 100, 150, 200)) OR
                ("block_part_index" = 1)
            )
        )
    ),
    vault_config AS (
        SELECT * FROM spacebox.dex_vaults_config_state
    ),
    vault_denom_price_ids AS (
        SELECT
            "token_0_denom",
            "token_1_denom",
            any("token_0_symbol") as "token_0_symbol",
            any("token_1_symbol") as "token_1_symbol",
            any("token_0_decimals") as "token_0_decimals",
            any("token_1_decimals") as "token_1_decimals"
        FROM vault_config
        GROUP BY "token_0_denom", "token_1_denom"
    ),
    swaps_with_price_ids AS (
        SELECT
            s.*,
            v."token_0_symbol" as "token_0_symbol",
            v."token_1_symbol" as "token_1_symbol",
            v."token_0_decimals" as "token_0_decimals",
            v."token_1_decimals" as "token_1_decimals"
        FROM source as s
        JOIN vault_denom_price_ids as v
            ON (s."TokenZero" = v."token_0_denom")
            AND (s."TokenOne" = v."token_1_denom")
    ),
    swaps_valued AS (
        WITH
            s."token_0_decimals" as "token_decimals_0",
            s."token_1_decimals" as "token_decimals_1",
            -- shortcut because all prices available within last 30 days
            p_0."price" as "slinky_price_0",
            p_1."price" as "slinky_price_1",
            p_0."decimals" as "slinky_decimals_0",
            p_1."decimals" as "slinky_decimals_1",
            toFloat64("slinky_price_0") * exp10(-("token_decimals_0" + "slinky_decimals_0")) as "token_price_0",
            toFloat64("slinky_price_1") * exp10(-("token_decimals_1" + "slinky_decimals_1")) as "token_price_1",
            if(s."ReservesOutZero" < s."ReservesInZero", s."ReservesInZero" - s."ReservesOutZero", 0) as "amount_in_0",
            if(s."ReservesOutOne" < s."ReservesInOne", s."ReservesInOne" - s."ReservesOutOne", 0) as "amount_in_1",
            if(s."ReservesOutZero" > s."ReservesInZero", s."ReservesOutZero" - s."ReservesInZero", 0) as "amount_out_0",
            if(s."ReservesOutOne" > s."ReservesInOne", s."ReservesOutOne" - s."ReservesInOne", 0) as "amount_out_1",
            "token_price_0" * toFloat64("amount_in_0") as "value_in_0",
            "token_price_1" * toFloat64("amount_in_1") as "value_in_1",
            "value_in_0" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_0",
            "value_in_1" * toFloat64(power(1.0001, s."Fee") - 1) as "value_fee_1",
            "token_price_0" * toFloat64("amount_out_0") as "value_out_0",
            "token_price_1" * toFloat64("amount_out_1") as "value_out_1",
            price_state AS (
                SELECT
                `base`,
                any(`price`) as `price`,
                any(`decimals`) as `decimals`
                FROM spacebox.slinky_prices_state
                WHERE "quote" = 'USD'
                GROUP BY "base", "quote"
            )
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
            0 as "price_timestamp_0",
            0 as "price_timestamp_1",
            "value_in_0",
            "value_in_1",
            "value_fee_0",
            "value_fee_1",
            "value_out_0",
            "value_out_1"
        FROM swaps_with_price_ids as s
        ANY LEFT JOIN price_state as p_0
            ON (p_0."base" = s."token_0_symbol")
        ANY LEFT JOIN price_state as p_1
            ON (p_1."base" = s."token_1_symbol")
    )
  SELECT * FROM swaps_valued;


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
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `value_in_0`        Float64,
    `value_in_1`        Float64,
    `value_fee_0`       Float64,
    `value_fee_1`       Float64,
    `value_out_0`       Float64, -- should be equal to ~(value_in_1 - value_fee_1)
    `value_out_1`       Float64, -- should be equal to ~(value_in_0 - value_fee_0)
) AS
WITH
    swaps as (
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
            OR `price_timestamp_0` = 0
            OR `price_timestamp_1` = 0
    ),
    source AS (
        SELECT *
        FROM swaps
        WHERE notEmpty("Receiver") OR (
            ("TrancheKey" IS NULL) AND (
                -- temp estimation of vault DEX pools by excluding normal DEX users
                ("Fee" NOT IN (1, 5, 10, 20, 50, 100, 150, 200)) OR
                ("block_part_index" = 1)
            )
        )
    ),
    vault_config AS (
        SELECT * FROM spacebox.dex_vaults_config_state
    ),
    vault_with_price_id AS (
        SELECT s.*, p_0."id" as "price_id_0", p_1."id" as "price_id_1"
        FROM vault_config as s
        LEFT JOIN spacebox.slinky_pairs_state as p_0
            ON (s."token_0_symbol" = p_0."base" AND s."token_0_quote_currency" = p_0."quote")
        LEFT JOIN spacebox.slinky_pairs_state as p_1
            ON (s."token_1_symbol" = p_1."base" AND s."token_1_quote_currency" = p_1."quote")
    ),
    vault_denom_price_ids AS (
        SELECT
            "token_0_denom",
            "token_1_denom",
            any("price_id_0") as "price_id_0",
            any("price_id_1") as "price_id_1",
            any("token_0_decimals") as "token_0_decimals",
            any("token_1_decimals") as "token_1_decimals"
        FROM vault_with_price_id
        GROUP BY "token_0_denom", "token_1_denom"
    ),
    slinky_prices_0 AS (
        SELECT
            "timestamp",
            "id",
            "price",
            "decimals"
        FROM spacebox.slinky_prices
        WHERE "id" IN (SELECT "price_id_0" FROM vault_denom_price_ids)
    ),
    slinky_prices_1 AS (
        SELECT
            "timestamp",
            "id",
            "price",
            "decimals"
        FROM spacebox.slinky_prices
        WHERE "id" IN (SELECT "price_id_1" FROM vault_denom_price_ids)
    ),
    swaps_with_price_ids AS (
        SELECT
            s.*,
            v."price_id_0" as "price_id_0",
            v."price_id_1" as "price_id_1",
            v."token_0_decimals" as "token_0_decimals",
            v."token_1_decimals" as "token_1_decimals"
        FROM source as s
        JOIN vault_denom_price_ids as v
            ON (s."TokenZero" = v."token_0_denom")
            AND (s."TokenOne" = v."token_1_denom")
    ),
    swaps_valued AS (
        WITH
            s."token_0_decimals" as "token_decimals_0",
            s."token_1_decimals" as "token_decimals_1",
            -- shortcut because all prices available within last 30 days
            if(p_0.timestamp = 0, 0, p_0."price") as "slinky_price_0",
            if(p_1.timestamp = 0, 0, p_1."price") as "slinky_price_1",
            if(p_0.timestamp = 0, 0, p_0."decimals") as "slinky_decimals_0",
            if(p_1.timestamp = 0, 0, p_1."decimals") as "slinky_decimals_1",
            toFloat64("slinky_price_0") * exp10(-("token_decimals_0" + "slinky_decimals_0")) as "token_price_0",
            toFloat64("slinky_price_1") * exp10(-("token_decimals_1" + "slinky_decimals_1")) as "token_price_1",
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
            p_0."timestamp" as "price_timestamp_0",
            p_1."timestamp" as "price_timestamp_1",
            "value_in_0",
            "value_in_1",
            "value_fee_0",
            "value_fee_1",
            "value_out_0",
            "value_out_1"
        FROM swaps_with_price_ids as s
        ASOF JOIN slinky_prices_0 as p_0
            ON (p_0."id" = s."price_id_0")
            AND p_0."timestamp" < s."timestamp"
        ASOF JOIN slinky_prices_1 as p_1
            ON (p_1."id" = s."price_id_1")
            AND p_1."timestamp" < s."timestamp"
    )
  SELECT * FROM swaps_valued;
