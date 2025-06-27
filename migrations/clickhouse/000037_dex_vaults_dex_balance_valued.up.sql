
-- spacebox.dex_vaults_dex_balance_valued table

CREATE TABLE spacebox.dex_vaults_dex_balance_valued
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
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32,
    -- price information
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `token_0_balance_value` Float64,
    `token_1_balance_value` Float64,
    `token_0_balance_before_deposit_value` Float64,
    `token_1_balance_before_deposit_value` Float64,
    `token_0_balance_hold_equivalent_amount` Float64,
    `token_1_balance_hold_equivalent_amount` Float64,
    -- determine most recent version by the recentness of the price data
    `price_version`     UInt64 MATERIALIZED
                            toUnixTimestamp64Milli(`price_timestamp_0`) +
                            toUnixTimestamp64Milli(`price_timestamp_1`),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    PROJECTION dex_vaults_dex_balance_valued_state (
        SELECT
            `contract_address`,
            argMax(`height`, `sort_key`) as `height`,
            argMax(`token_0_balance`, `sort_key`) as `token_0_balance`,
            argMax(`token_1_balance`, `sort_key`) as `token_1_balance`,
            argMax(`token_0_balance_before_deposit`, `sort_key`) as `token_0_balance_before_deposit`,
            argMax(`token_1_balance_before_deposit`, `sort_key`) as `token_1_balance_before_deposit`,
            argMax(`token_0_balance_value`, `sort_key`) as `token_0_balance_value`,
            argMax(`token_1_balance_value`, `sort_key`) as `token_1_balance_value`,
            argMax(`token_0_balance_before_deposit_value`, `sort_key`) as `token_0_balance_before_deposit_value`,
            argMax(`token_1_balance_before_deposit_value`, `sort_key`) as `token_1_balance_before_deposit_value`
        GROUP BY `contract_address`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree(`price_version`)
PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in timeseries queries
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.dex_vaults_dex_balance_valued dex_vaults_dex_balance_valued_state projection view

CREATE VIEW spacebox.dex_vaults_dex_balance_valued_state AS
    SELECT
        `contract_address`,
        argMax(`height`, `sort_key`) as `height`,
        argMax(`token_0_balance`, `sort_key`) as `token_0_balance`,
        argMax(`token_1_balance`, `sort_key`) as `token_1_balance`,
        argMax(`token_0_balance_before_deposit`, `sort_key`) as `token_0_balance_before_deposit`,
        argMax(`token_1_balance_before_deposit`, `sort_key`) as `token_1_balance_before_deposit`,
        argMax(`token_0_balance_value`, `sort_key`) as `token_0_balance_value`,
        argMax(`token_1_balance_value`, `sort_key`) as `token_1_balance_value`,
        argMax(`token_0_balance_before_deposit_value`, `sort_key`) as `token_0_balance_before_deposit_value`,
        argMax(`token_1_balance_before_deposit_value`, `sort_key`) as `token_1_balance_before_deposit_value`
    FROM spacebox.dex_vaults_dex_balance_valued
    GROUP BY `contract_address`;


-- spacebox.dex_vaults_dex_balance_valued_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_balance_valued_deposit_writer TO spacebox.dex_vaults_dex_balance_valued (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32,
    -- price information
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `token_0_balance_value` Float64,
    `token_1_balance_value` Float64,
    `token_0_balance_before_deposit_value` Float64,
    `token_1_balance_before_deposit_value` Float64,
    `token_0_balance_hold_equivalent_amount` Float64,
    `token_1_balance_hold_equivalent_amount` Float64
) AS
WITH
    source AS (SELECT * FROM spacebox.dex_vaults_dex_balance),
    vault_config AS (SELECT * FROM spacebox.dex_vaults_config_state),
    vault_denom_price_ids AS (
        SELECT
            "contract_address",
            any("token_0_symbol") as "token_0_symbol",
            any("token_1_symbol") as "token_1_symbol",
            any("token_0_decimals") as "token_0_decimals",
            any("token_1_decimals") as "token_1_decimals"
        FROM vault_config
        GROUP BY "contract_address"
    ),
    balances_with_price_ids AS (
        SELECT
            s.*,
            v."token_0_symbol" as "token_0_symbol",
            v."token_1_symbol" as "token_1_symbol",
            v."token_0_decimals" as "token_0_decimals",
            v."token_1_decimals" as "token_1_decimals"
        FROM source as s
        JOIN vault_denom_price_ids as v
            ON (s."contract_address" = v."contract_address")
    ),
    balances_valued AS (
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
            "token_price_0" * toFloat64("token_0_balance") as "token_0_balance_value",
            "token_price_1" * toFloat64("token_1_balance") as "token_1_balance_value",
            "token_price_0" * toFloat64("token_0_balance_before_deposit") as "token_0_balance_before_deposit_value",
            "token_price_1" * toFloat64("token_1_balance_before_deposit") as "token_1_balance_before_deposit_value",
            -- use estimated balance for balance equivalent amount hold amounts
            "token_0_balance_before_deposit_value" + "token_1_balance_before_deposit_value" as "balance_value",
            "balance_value" / 2 / "token_price_0" as "token_0_balance_hold_equivalent_amount",
            "balance_value" / 2 / "token_price_1" as "token_1_balance_hold_equivalent_amount",
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
            s."action" as "action",
            s."contract_address" as "contract_address",
            s."token_0_balance" as "token_0_balance",
            s."token_1_balance" as "token_1_balance",
            s."token_0_balance_before_deposit" as "token_0_balance_before_deposit",
            s."token_1_balance_before_deposit" as "token_1_balance_before_deposit",
            s."token_0_price" as "token_0_price",
            s."token_1_price" as "token_1_price",
            s."price_0_to_1" as "price_0_to_1",
            -- price information
            0 as "price_timestamp_0",
            0 as "price_timestamp_1",
            "token_0_balance_value",
            "token_1_balance_value",
            "token_0_balance_before_deposit_value",
            "token_1_balance_before_deposit_value",
            "token_0_balance_hold_equivalent_amount",
            "token_1_balance_hold_equivalent_amount"
        FROM balances_with_price_ids as s
        ANY LEFT JOIN price_state as p_0
            ON (p_0."base" = s."token_0_symbol")
        ANY LEFT JOIN price_state as p_1
            ON (p_1."base" = s."token_1_symbol")
    )
  SELECT * FROM balances_valued;


-- spacebox.dex_vaults_dex_balance_valued_again_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_vaults_dex_balance_valued_again_writer
REFRESH EVERY 5 MINUTE OFFSET 30 SECOND RANDOMIZE FOR 20 SECOND
APPEND
TO spacebox.dex_vaults_dex_balance_valued (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_balance_before_deposit`   UInt128,
    `token_1_balance_before_deposit`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32,
    -- price information
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `token_0_balance_value` Float64,
    `token_1_balance_value` Float64,
    `token_0_balance_before_deposit_value` Float64,
    `token_1_balance_before_deposit_value` Float64,
    `token_0_balance_hold_equivalent_amount` Float64,
    `token_1_balance_hold_equivalent_amount` Float64
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
            `action`,
            `contract_address`,
            `token_0_balance`,
            `token_1_balance`,
            `token_0_balance_before_deposit`,
            `token_1_balance_before_deposit`,
            `token_0_price`,
            `token_1_price`,
            `price_0_to_1`,
        FROM spacebox.dex_vaults_dex_balance_valued
        -- allow overwriting valuation of new shares several times
        -- note: this data can be stale if shares or price data failed to
        --       update for the period of time within this WHERE condition
        WHERE `timestamp` > addHours(NOW(), -1)
            OR `price_timestamp_0` = 0
            OR `price_timestamp_1` = 0
    ),
    vault_config AS (SELECT * FROM spacebox.dex_vaults_config_state),
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
            "contract_address",
            any("price_id_0") as "price_id_0",
            any("price_id_1") as "price_id_1",
            any("token_0_decimals") as "token_0_decimals",
            any("token_1_decimals") as "token_1_decimals"
        FROM vault_with_price_id
        GROUP BY "contract_address"
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
    balances_with_price_ids AS (
        SELECT
            s.*,
            v."price_id_0" as "price_id_0",
            v."price_id_1" as "price_id_1",
            v."token_0_decimals" as "token_0_decimals",
            v."token_1_decimals" as "token_1_decimals"
        FROM source as s
        JOIN vault_denom_price_ids as v
            ON (s."contract_address" = v."contract_address")
    ),
    balances_valued AS (
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
            "token_price_0" * toFloat64("token_0_balance") as "token_0_balance_value",
            "token_price_1" * toFloat64("token_1_balance") as "token_1_balance_value",
            "token_price_0" * toFloat64("token_0_balance_before_deposit") as "token_0_balance_before_deposit_value",
            "token_price_1" * toFloat64("token_1_balance_before_deposit") as "token_1_balance_before_deposit_value",
            -- use estimated balance for balance equivalent amount hold amounts
            "token_0_balance_before_deposit_value" + "token_1_balance_before_deposit_value" as "balance_value",
            "balance_value" / 2 / "token_price_0" as "token_0_balance_hold_equivalent_amount",
            "balance_value" / 2 / "token_price_1" as "token_1_balance_hold_equivalent_amount"
        SELECT
            s."timestamp" as "timestamp",
            s."height" as "height",
            s."block_part_index" as "block_part_index",
            s."tx_index" as "tx_index",
            s."event_index" as "event_index",
            -- event data
            s."action" as "action",
            s."contract_address" as "contract_address",
            s."token_0_balance" as "token_0_balance",
            s."token_1_balance" as "token_1_balance",
            s."token_0_balance_before_deposit" as "token_0_balance_before_deposit",
            s."token_1_balance_before_deposit" as "token_1_balance_before_deposit",
            s."token_0_price" as "token_0_price",
            s."token_1_price" as "token_1_price",
            s."price_0_to_1" as "price_0_to_1",
            -- price information
            p_0."timestamp" as "price_timestamp_0",
            p_1."timestamp" as "price_timestamp_1",
            "token_0_balance_value",
            "token_1_balance_value",
            "token_0_balance_before_deposit_value",
            "token_1_balance_before_deposit_value",
            "token_0_balance_hold_equivalent_amount",
            "token_1_balance_hold_equivalent_amount"
        FROM balances_with_price_ids as s
        ASOF JOIN slinky_prices_0 as p_0
            ON (p_0."id" = s."price_id_0")
            AND p_0."timestamp" < s."timestamp"
        ASOF JOIN slinky_prices_1 as p_1
            ON (p_1."id" = s."price_id_1")
            AND p_1."timestamp" < s."timestamp"
    )
  SELECT * FROM balances_valued;
