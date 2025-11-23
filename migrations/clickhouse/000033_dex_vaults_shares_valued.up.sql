
-- spacebox.dex_vaults_shares_valued table

-- note: this should be a temporary fix, aggregation should be done by pool ID not pool attributes
--       because pool shares can be transferred, bank transfers are the real source of truth
CREATE TABLE spacebox.dex_vaults_shares_valued
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
    `creator`           String,
    `action`            LowCardinality(String),
    `contract_address`  String,
    -- save boolean for credit/debit
    `credit`            Boolean MATERIALIZED `action` = 'deposit',
    `token_0_deposited` UInt128, -- amount deposited
    `token_1_deposited` UInt128, -- amount deposited
    `token_0_withdrawn` UInt128, -- amount withdrawn
    `token_1_withdrawn` UInt128, -- amount withdrawn
    `shares_in`         UInt128, -- shares added
    `shares_out`        UInt128, -- shares removed
    `total_shares`      UInt128, -- total shares
    -- price information
    `price_timestamp`   DateTime64(9),
    `price_0`           Float64,
    `price_1`           Float64,
    `value_deposited`   Float64,
    `value_withdrawn`   Float64,
    `hold_equivalent_0` Float64,
    `hold_equivalent_1` Float64,
    `balance_timestamp` DateTime64(9),
    `token_0_balance`   UInt128, -- amount held in vault (before deposit/withdrawal)
    `token_1_balance`   UInt128, -- amount held in vault (before deposit/withdrawal)
    `value_open`        Float64, -- value held in vault (before deposit/withdrawal)
    `value_close`       Float64, -- value held in vault (~after deposit/withdrawal)
    -- determine most recent version by the recentness of the price data
    `timestamp_version`     UInt64 MATERIALIZED
                            toUnixTimestamp64Milli(`price_timestamp`) +
                            toUnixTimestamp64Milli(`balance_timestamp`),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add projection for timeseries queries of each vault
    PROJECTION dex_vaults_shares_valued_timeseries (
        SELECT *
        ORDER BY `contract_address`, `timestamp`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree(`timestamp_version`)
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS
    -- see docs: https://clickhouse.com/docs/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.dex_vaults_shares_valued_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_vaults_shares_valued_writer TO spacebox.dex_vaults_shares_valued (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `creator`           String,
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_deposited` UInt128, -- amount deposited
    `token_1_deposited` UInt128, -- amount deposited
    `token_0_withdrawn` UInt128, -- amount withdrawn
    `token_1_withdrawn` UInt128, -- amount withdrawn
    `shares_in`         UInt128, -- shares added
    `shares_out`        UInt128, -- shares removed
    `total_shares`      UInt128, -- total shares
    -- price information
    `price_timestamp`   DateTime64(9),
    `price_0`           Float64,
    `price_1`           Float64,
    `value_deposited`   Float64,
    `value_withdrawn`   Float64,
    `hold_equivalent_0` Float64,
    `hold_equivalent_1` Float64,
    `balance_timestamp` DateTime64(9),
    `token_0_balance`   UInt128, -- amount held in vault (before deposit/withdrawal)
    `token_1_balance`   UInt128, -- amount held in vault (before deposit/withdrawal)
    `value_open`        Float64, -- value held in vault (before deposit/withdrawal)
    `value_close`       Float64  -- value held in vault (~after deposit/withdrawal)
) AS
WITH
    shares as (
        SELECT
            `timestamp`,
            `height`,
            `block_part_index`,
            `tx_index`,
            `event_index`,
            -- event data
            `creator`,
            `action`,
            `contract_address`,
            `token_0_deposited`,
            `token_1_deposited`,
            `token_0_withdrawn`,
            `token_1_withdrawn`,
            `shares_in`,
            `shares_out`,
            `total_shares`
        FROM spacebox.dex_vaults_shares
    ),
    shares_with_token_config_and_balance as (
        SELECT
            s.*
        FROM shares as s
        ANY LEFT JOIN spacebox.dex_vaults_config_state as c
            on s.`contract_address` = c.`contract_address`
        WHERE c."token_0_quote_currency" = 'USD'
          AND c."token_1_quote_currency" = 'USD'
    ),
    shares_valued AS (
        WITH
            p."token_0_price" as "token_price_0",
            p."token_1_price" as "token_price_1"
        SELECT
            s.`timestamp` as `timestamp`,
            s.`height` as `height`,
            s.`block_part_index` as `block_part_index`,
            s.`tx_index` as `tx_index`,
            s.`event_index` as `event_index`,
            -- event data
            s.`creator` as `creator`,
            s.`action` as `action`,
            s.`contract_address` as `contract_address`,
            s.`token_0_deposited` as `token_0_deposited`,
            s.`token_1_deposited` as `token_1_deposited`,
            s.`token_0_withdrawn` as `token_0_withdrawn`,
            s.`token_1_withdrawn` as `token_1_withdrawn`,
            s.`shares_in` as `shares_in`,
            s.`shares_out` as `shares_out`,
            s.`total_shares` as `total_shares`,
            -- price information
            0 as `price_timestamp`,
            "token_price_0" as "price_0",
            "token_price_1" as "price_1",
            toFloat64(`token_0_deposited`) * "token_price_0" +
            toFloat64(`token_1_deposited`) * "token_price_1" as `value_deposited`,
            toFloat64(`token_0_withdrawn`) * "token_price_0" +
            toFloat64(`token_1_withdrawn`) * "token_price_1" as `value_withdrawn`,
            if("token_price_0" > 0, ("value_deposited" - "value_withdrawn") / 2 / "token_price_0", 0) as "hold_equivalent_0",
            if("token_price_1" > 0, ("value_deposited" - "value_withdrawn") / 2 / "token_price_1", 0) as "hold_equivalent_1",
            0 as `balance_timestamp`,
            b.`token_0_balance` as "token_0_balance",
            b.`token_1_balance` as "token_1_balance",
            toFloat64(`token_0_balance`) * "token_price_0" +
            toFloat64(`token_1_balance`) * "token_price_1" as `value_open`,
            toFloat64(`token_0_balance` + `token_0_deposited` - `token_0_withdrawn`) * "token_price_0" +
            toFloat64(`token_1_balance` + `token_1_deposited` - `token_1_withdrawn`) * "token_price_1" as `value_close`
        FROM shares_with_token_config_and_balance as s
        ANY LEFT JOIN spacebox.price_by_vault_denom_state as p
            ON (s."contract_address" = p."contract_address")
        ANY LEFT JOIN spacebox.dex_vaults_events_dex_deposit_state as b
            on s.`contract_address` = b.`contract_address`
    )
    SELECT *
    FROM shares_valued;


-- spacebox.dex_vaults_shares_valued_again_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_shares_valued_again_writer
REFRESH EVERY 5 MINUTE OFFSET 10 SECOND
APPEND
TO spacebox.dex_vaults_shares_valued (
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `creator`           String,
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_deposited` UInt128, -- amount deposited
    `token_1_deposited` UInt128, -- amount deposited
    `token_0_withdrawn` UInt128, -- amount withdrawn
    `token_1_withdrawn` UInt128, -- amount withdrawn
    `shares_in`         UInt128, -- shares added
    `shares_out`        UInt128, -- shares removed
    `total_shares`      UInt128, -- total shares
    -- price information
    `price_timestamp`   DateTime64(9),
    `price_0`           Float64,
    `price_1`           Float64,
    `value_deposited`   Float64,
    `value_withdrawn`   Float64,
    `hold_equivalent_0` Float64,
    `hold_equivalent_1` Float64,
    `balance_timestamp` DateTime64(9),
    `token_0_balance`   UInt128, -- amount held in vault (before deposit/withdrawal)
    `token_1_balance`   UInt128, -- amount held in vault (before deposit/withdrawal)
    `value_open`        Float64, -- value held in vault (before deposit/withdrawal)
    `value_close`       Float64  -- value held in vault (~after deposit/withdrawal)
) AS
WITH
    shares as (
        SELECT
            `timestamp`,
            `height`,
            `block_part_index`,
            `tx_index`,
            `event_index`,
            -- event data
            `creator`,
            `action`,
            `contract_address`,
            `token_0_deposited`,
            `token_1_deposited`,
            `token_0_withdrawn`,
            `token_1_withdrawn`,
            `shares_in`,
            `shares_out`,
            `total_shares`
        FROM spacebox.dex_vaults_shares_valued
        -- allow overwriting valuation of new shares several times
        -- note: this data can be stale if shares or price data failed to
        --       update for the period of time within this WHERE condition
        WHERE `timestamp` > addHours(NOW(), -1)
            OR `balance_timestamp` = 0
            OR `price_timestamp` = 0
    ),
    shares_with_token_config as (
        SELECT s.*
        FROM shares as s
        ANY LEFT JOIN spacebox.dex_vaults_config_state as c
            on s.`contract_address` = c.`contract_address`
        ANY LEFT JOIN spacebox.price_by_vault_denom_first_state as p
            on s.`contract_address` = p.`contract_address`
        WHERE c."token_0_quote_currency" = 'USD'
          AND c."token_1_quote_currency" = 'USD'
    ),
    shares_valued AS (
        WITH
            if(p."timestamp" > 0, p."token_0_price", p_first."token_0_price") as "token_price_0",
            if(p."timestamp" > 0, p."token_1_price", p_first."token_1_price") as "token_price_1",
            if(p."timestamp" > 0, p."timestamp", p_first."timestamp") as `price_timestamp`
        SELECT
            s.`timestamp` as `timestamp`,
            s.`height` as `height`,
            s.`block_part_index` as `block_part_index`,
            s.`tx_index` as `tx_index`,
            s.`event_index` as `event_index`,
            -- event data
            s.`creator` as `creator`,
            s.`action` as `action`,
            s.`contract_address` as `contract_address`,
            s.`token_0_deposited` as `token_0_deposited`,
            s.`token_1_deposited` as `token_1_deposited`,
            s.`token_0_withdrawn` as `token_0_withdrawn`,
            s.`token_1_withdrawn` as `token_1_withdrawn`,
            s.`shares_in` as `shares_in`,
            s.`shares_out` as `shares_out`,
            s.`total_shares` as `total_shares`,
            -- price information
            `price_timestamp`,
            -- note: the prices here extend to before p_first."timestamp"
            --       so we can value user deposits as best we can before that time
            "token_price_0" as "price_0",
            "token_price_1" as "price_1",
            toFloat64(`token_0_deposited`) * "token_price_0" +
            toFloat64(`token_1_deposited`) * "token_price_1" as `value_deposited`,
            toFloat64(`token_0_withdrawn`) * "token_price_0" +
            toFloat64(`token_1_withdrawn`) * "token_price_1" as `value_withdrawn`,
            if("token_price_0" > 0, ("value_deposited" - "value_withdrawn") / 2 / "token_price_0", 0) as "hold_equivalent_0",
            if("token_price_1" > 0, ("value_deposited" - "value_withdrawn") / 2 / "token_price_1", 0) as "hold_equivalent_1",
            b.`timestamp` as `balance_timestamp`,
            b.`token_0_balance` as "token_0_balance",
            b.`token_1_balance` as "token_1_balance",
            toFloat64(`token_0_balance`) * "token_price_0" +
            toFloat64(`token_1_balance`) * "token_price_1" as `value_open`,
            toFloat64(`token_0_balance` + `token_0_deposited` - `token_0_withdrawn`) * "token_price_0" +
            toFloat64(`token_1_balance` + `token_1_deposited` - `token_1_withdrawn`) * "token_price_1" as `value_close`
        FROM shares_with_token_config as s
        ASOF LEFT JOIN spacebox.dex_vaults_events_dex_deposit as b
            ON (s.`contract_address` = b.`contract_address`)
            AND s.`height` >= b.`height`
        ASOF LEFT JOIN spacebox.price_by_vault_denom as p
            ON (s."contract_address" = p."contract_address")
            AND s."timestamp" >= p."timestamp"
        ANY LEFT JOIN spacebox.price_by_vault_denom_first_state as p_first
            ON (s."contract_address" = p_first."contract_address")
        WHERE p_first."timestamp" > 0
    )
    SELECT *
    FROM shares_valued;

