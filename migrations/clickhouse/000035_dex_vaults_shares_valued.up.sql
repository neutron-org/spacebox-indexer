
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
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `value_deposited`   Float64,
    `value_withdrawn`   Float64,
    `hold_equivalent_0` Float64,
    `hold_equivalent_1` Float64,
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
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `value_deposited`   Float64,
    `value_withdrawn`   Float64,
    `hold_equivalent_0` Float64,
    `hold_equivalent_1` Float64
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
    shares_with_token_config as (
        SELECT
            s.*,
            c.`token_0_denom` as `token_0_denom`,
            c.`token_1_denom` as `token_1_denom`,
            c.`token_0_decimals` as `token_0_decimals`,
            c.`token_1_decimals` as `token_1_decimals`,
            c.`token_0_symbol` as `token_0_symbol`,
            c.`token_1_symbol` as `token_1_symbol`
        FROM shares as s
        ANY LEFT JOIN spacebox.dex_vaults_config_state as c
            on s.`contract_address` = c.`contract_address`
    ),
    shares_valued AS (
        WITH
            toFloat64(p_0.`price`) * exp10(-(s."token_0_decimals" + p_0.`decimals`)) as "token_price_0",
            toFloat64(p_1.`price`) * exp10(-(s."token_1_decimals" + p_1.`decimals`)) as "token_price_1",
            price_state AS (
                SELECT
                `base`,
                `price`,
                `decimals`
                FROM spacebox.slinky_prices_state
                WHERE "quote" = 'USD'
            )
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
            0 as `price_timestamp_0`,
            0 as `price_timestamp_1`,
            toFloat64(`token_0_deposited`) * "token_price_0" +
            toFloat64(`token_1_deposited`) * "token_price_1" as `value_deposited`,
            toFloat64(`token_0_withdrawn`) * "token_price_0" +
            toFloat64(`token_1_withdrawn`) * "token_price_1" as `value_withdrawn`,
            ("value_deposited" - "value_withdrawn") / 2 / "token_price_0" as "hold_equivalent_0",
            ("value_deposited" - "value_withdrawn") / 2 / "token_price_1" as "hold_equivalent_1"
        FROM shares_with_token_config as s
        ANY LEFT JOIN price_state as p_0
            ON (p_0.`base` = s.`token_0_symbol`)
        ANY LEFT JOIN price_state as p_1
            ON (p_1.`base` = s.`token_1_symbol`)
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
    `price_timestamp_0` DateTime64(9),
    `price_timestamp_1` DateTime64(9),
    `value_deposited`   Float64,
    `value_withdrawn`   Float64,
    `hold_equivalent_0` Float64,
    `hold_equivalent_1` Float64
) AS
WITH
    shares as (
        -- get base data of unvalued share rows
        -- find already valued shares to exclude from the update list
        WITH old_valued_shares_events AS (
            SELECT `height`, `block_part_index`, `tx_index`, `event_index`
            FROM spacebox.dex_vaults_shares_valued
            -- allow overwriting valuation of new shares several times
            -- note: this data can be stale if shares or price data failed to
            --       update for the period of time within this WHERE condition
            WHERE `timestamp` < addHours(NOW(), -1) AND (
                `price_timestamp_0` > 0 OR
                `price_timestamp_1` > 0
            )
        )
        SELECT *
        FROM spacebox.dex_vaults_shares
        WHERE (`height`, `block_part_index`, `tx_index`, `event_index`) NOT IN (
            SELECT (`height`, `block_part_index`, `tx_index`, `event_index`)
            FROM old_valued_shares_events
        )
    ),
    shares_with_high_resolution_timestamp as (
        -- get shares data (with high-resolution timestamp)
        -- TODO: just have 64bit timestamps on all tables instead
        WITH block_times_subquery AS (
            SELECT `height`, `timestamp`
            FROM spacebox.raw_block_txhash
            WHERE `height` >= (
                SELECT min(`height`)
                FROM shares
            )
        )
        SELECT
            if(b.`timestamp` > 0, b.`timestamp`, s.`timestamp`) as `timestamp`,
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
        FROM shares as s
        ANY LEFT JOIN block_times_subquery as b
            on (b.`height` = s.`height`)
    ),
    shares_with_token_config as (
        SELECT
            s.*,
            c.`token_0_denom` as `token_0_denom`,
            c.`token_1_denom` as `token_1_denom`,
            c.`token_0_decimals` as `token_0_decimals`,
            c.`token_1_decimals` as `token_1_decimals`,
            c.`token_0_symbol` as `token_0_symbol`,
            c.`token_1_symbol` as `token_1_symbol`
        FROM shares_with_high_resolution_timestamp as s
        ANY LEFT JOIN spacebox.dex_vaults_config_state as c
            on s.`contract_address` = c.`contract_address`
    ),
    price_ids AS (
        SELECT
            `id`,
            `base`
        FROM spacebox.slinky_pairs_state
        WHERE "quote" = 'USD'
    ),
    price_ids_0 AS (
        SELECT `id`
        FROM price_ids
        WHERE "base" in (
            SELECT DISTINCT "token_0_symbol"
            FROM shares_with_token_config
        )
    ),
    price_ids_1 AS (
        SELECT `id`
        FROM price_ids
        WHERE "base" in (
            SELECT DISTINCT "token_1_symbol"
            FROM shares_with_token_config
        )
    ),
    shares_with_price_ids AS (
        SELECT s.*,
            p_0.`id` as `price_id_0`,
            p_1.`id` as `price_id_1`
        FROM shares_with_token_config as s 
        ANY LEFT JOIN price_ids as p_0 ON s.`token_0_symbol` = p_0.`base`
        ANY LEFT JOIN price_ids as p_1 ON s.`token_1_symbol` = p_1.`base`
    ),
    first_prices AS (
        SELECT
            `timestamp`,
            `base`,
            `price`,
            `decimals`
        FROM spacebox.slinky_prices_first_state
        WHERE "quote" = 'USD'
    ),
    shares_with_prices AS (
        SELECT s.*,
            p_0.`price` as `first_price_0`,
            p_0.`timestamp` as `first_price_timestamp_0`,
            p_0.`decimals` as `first_price_decimals_0`,
            p_1.`price` as `first_price_1`,
            p_1.`timestamp` as `first_price_timestamp_1`,
            p_1.`decimals` as `first_price_decimals_1`
        FROM shares_with_price_ids as s 
        ANY LEFT JOIN first_prices as p_0 ON s.`token_0_symbol` = p_0.`base`
        ANY LEFT JOIN first_prices as p_1 ON s.`token_1_symbol` = p_1.`base`
    ),
    shares_valued AS (
        WITH
            s.`token_0_decimals` as `token_0_decimals`,
            s.`token_1_decimals` as `token_1_decimals`,
            if(
                p_0.`timestamp`> 0,
                p_0.`timestamp`,
                s.`first_price_timestamp_0`
            ) as `price_timestamp_0`,
            if(
                p_0.`timestamp`> 0,
                p_0.`price`,
                s.`first_price_0`
            ) as `price_0`,
            if(
                p_0.`timestamp`> 0,
                p_0.`decimals`,
                s.`first_price_decimals_0`
            ) as `price_decimals_0`,
            if(
                p_1.`timestamp`> 0,
                p_1.`timestamp`,
                s.`first_price_timestamp_1`
            ) as `price_timestamp_1`,
            if(
                p_1.`timestamp`> 0,
                p_1.`price`,
                s.`first_price_1`
            ) as `price_1`,
            if(
                p_1.`timestamp`> 0,
                p_1.`decimals`,
                s.`first_price_decimals_1`
            ) as `price_decimals_1`,
            toFloat64(`price_0`) * exp10(-("token_0_decimals" + "price_decimals_0")) as "token_price_0",
            toFloat64(`price_1`) * exp10(-("token_1_decimals" + "price_decimals_1")) as "token_price_1"
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
            `price_timestamp_0`,
            `price_timestamp_1`,
            toFloat64(`token_0_deposited`) * "token_price_0" +
            toFloat64(`token_1_deposited`) * "token_price_1" as `value_deposited`,
            toFloat64(`token_0_withdrawn`) * "token_price_0" +
            toFloat64(`token_1_withdrawn`) * "token_price_1" as `value_withdrawn`,
            ("value_deposited" - "value_withdrawn") / 2 / "token_price_0" as "hold_equivalent_0",
            ("value_deposited" - "value_withdrawn") / 2 / "token_price_1" as "hold_equivalent_1"
        FROM shares_with_prices as s
        ASOF LEFT JOIN (SELECT * FROM spacebox.slinky_prices WHERE `id` IN price_ids_0) as p_0
            ON (p_0.`id` = s.`price_id_0`)
            AND p_0.`timestamp` <= s.`timestamp`
        ASOF LEFT JOIN (SELECT * FROM spacebox.slinky_prices WHERE `id` IN price_ids_1) as p_1
            ON (p_1.`id` = s.`price_id_1`)
            AND p_1.`timestamp` <= s.`timestamp`
    )
    SELECT *
    FROM shares_valued;

