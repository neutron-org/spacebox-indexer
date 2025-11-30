
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
    -- price information
    `price_timestamp`   DateTime64(9),
    `token_0_balance_value` Float64,
    `token_1_balance_value` Float64,
    `token_0_balance_before_deposit_value` Float64,
    `token_1_balance_before_deposit_value` Float64,
    `token_0_balance_hold_equivalent_amount` Float64,
    `token_1_balance_hold_equivalent_amount` Float64,
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
ENGINE = ReplacingMergeTree(`price_timestamp`)
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


-- spacebox.dex_vaults_dex_balance_valued_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_balance_valued_writer TO spacebox.dex_vaults_dex_balance_valued (
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
    -- price information
    `price_timestamp`   DateTime64(9),
    `token_0_balance_value` Float64,
    `token_1_balance_value` Float64,
    `token_0_balance_before_deposit_value` Float64,
    `token_1_balance_before_deposit_value` Float64,
    `token_0_balance_hold_equivalent_amount` Float64,
    `token_1_balance_hold_equivalent_amount` Float64
) AS
WITH
    source AS (SELECT * FROM spacebox.dex_vaults_dex_balance),
    balances_with_token_config AS (
        SELECT s.*
        FROM source as s
        ANY LEFT JOIN spacebox.dex_vaults_by_pair_config_state as c
            on s.`contract_address` = c.`contract_address`
        WHERE c."token_0_quote_currency" = 'USD'
          AND c."token_1_quote_currency" = 'USD'
    ),
    balances_valued AS (
        WITH
            p."token_0_price" as "token_price_0",
            p."token_1_price" as "token_price_1",
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
            if("token_price_0" > 0, "balance_value" / 2 / "token_price_0", 0) as "token_0_balance_hold_equivalent_amount",
            if("token_price_1" > 0, "balance_value" / 2 / "token_price_1", 0) as "token_1_balance_hold_equivalent_amount"
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
            -- price information
            0 as "price_timestamp",
            "token_0_balance_value",
            "token_1_balance_value",
            "token_0_balance_before_deposit_value",
            "token_1_balance_before_deposit_value",
            "token_0_balance_hold_equivalent_amount",
            "token_1_balance_hold_equivalent_amount"
        FROM balances_with_token_config as s
        ANY LEFT JOIN spacebox.price_by_vault_denom as p
            ON (s."contract_address" = p."contract_address")
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
    -- price information
    `price_timestamp` DateTime64(9),
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
            `token_1_price`
        FROM spacebox.dex_vaults_dex_balance_valued
        -- allow overwriting valuation of new shares several times
        -- note: this data can be stale if shares or price data failed to
        --       update for the period of time within this WHERE condition
        WHERE `timestamp` > addHours(NOW(), -1)
            OR `price_timestamp` = 0
    ),
    balances_with_token_config AS (
        SELECT s.*
        FROM source as s
        ANY LEFT JOIN spacebox.dex_vaults_by_pair_config_state as c
            on s.`contract_address` = c.`contract_address`
        WHERE c."token_0_quote_currency" = 'USD'
          AND c."token_1_quote_currency" = 'USD'
    ),
    balances_valued AS (
        WITH
            p."token_0_price" as "token_price_0",
            p."token_1_price" as "token_price_1",
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
            if("token_price_0" > 0, "balance_value" / 2 / "token_price_0", 0) as "token_0_balance_hold_equivalent_amount",
            if("token_price_1" > 0, "balance_value" / 2 / "token_price_1", 0) as "token_1_balance_hold_equivalent_amount"
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
            -- price information
            -- prevent `price_timestamp` = 0 rows from being re-processed forever
            -- by setting price=0 with p_first timestamps for times that are too early
            if(s."timestamp" >= p_first."timestamp", p."timestamp", p_first."timestamp") as "price_timestamp",
            "token_0_balance_value",
            "token_1_balance_value",
            "token_0_balance_before_deposit_value",
            "token_1_balance_before_deposit_value",
            "token_0_balance_hold_equivalent_amount",
            "token_1_balance_hold_equivalent_amount"
        FROM balances_with_token_config as s
        ANY LEFT JOIN spacebox.price_by_vault_denom_first_state as p_first
            ON (s."contract_address" = p_first."contract_address")
        ASOF LEFT JOIN spacebox.price_by_vault_denom as p
            ON (s."contract_address" = p."contract_address")
            AND s."timestamp" >= p."timestamp"
        WHERE p_first."timestamp" > 0
    )
  SELECT * FROM balances_valued;


CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_vaults_dex_balance_valued_daily_writer
REFRESH EVERY 1 DAY RANDOMIZE FOR 1 HOUR
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
    -- price information
    `price_timestamp` DateTime64(9),
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
            `token_1_price`
        FROM spacebox.dex_vaults_dex_balance_valued
        -- allow overwriting valuation of new shares several times
        -- note: this data can be stale if shares or price data failed to
        --       update for the period of time within this WHERE condition
        WHERE addMinutes(`price_timestamp`, 1) < `timestamp`
    ),
    balances_with_token_config AS (
        SELECT s.*
        FROM source as s
        ANY LEFT JOIN spacebox.dex_vaults_by_pair_config_state as c
            on s.`contract_address` = c.`contract_address`
        WHERE c."token_0_quote_currency" = 'USD'
          AND c."token_1_quote_currency" = 'USD'
    ),
    balances_valued AS (
        WITH
            p."token_0_price" as "token_price_0",
            p."token_1_price" as "token_price_1",
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
            if("token_price_0" > 0, "balance_value" / 2 / "token_price_0", 0) as "token_0_balance_hold_equivalent_amount",
            if("token_price_1" > 0, "balance_value" / 2 / "token_price_1", 0) as "token_1_balance_hold_equivalent_amount"
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
            -- price information
            -- prevent `price_timestamp` = 0 rows from being re-processed forever
            -- by setting price=0 with p_first timestamps for times that are too early
            if(s."timestamp" >= p_first."timestamp", p."timestamp", p_first."timestamp") as "price_timestamp",
            "token_0_balance_value",
            "token_1_balance_value",
            "token_0_balance_before_deposit_value",
            "token_1_balance_before_deposit_value",
            "token_0_balance_hold_equivalent_amount",
            "token_1_balance_hold_equivalent_amount"
        FROM balances_with_token_config as s
        ANY LEFT JOIN spacebox.price_by_vault_denom_first_state as p_first
            ON (s."contract_address" = p_first."contract_address")
        ASOF LEFT JOIN spacebox.price_by_vault_denom as p
            ON (s."contract_address" = p."contract_address")
            AND s."timestamp" >= p."timestamp"
        WHERE p_first."timestamp" > 0
    )
  SELECT * FROM balances_valued;

-- spacebox.dex_vaults_dex_balance_valued_by_minute_agg table
CREATE TABLE spacebox.dex_vaults_dex_balance_valued_by_minute_agg
(
    `timestamp`         DateTime,
    -- add height alias: as the height the price is valid from (for ASOF joins)
    `height`            ALIAS `height_from`,
    `height_from`       AggregateFunction(min, Int64),
    `height_to`         AggregateFunction(max, Int64),
    `contract_address`  LowCardinality(String),
    `token_0_balance`   AggregateFunction(argMax, UInt128, Tuple(Int64, DateTime64(9))),
    `token_1_balance`   AggregateFunction(argMax, UInt128, Tuple(Int64, DateTime64(9))),
    `price_timestamp`   AggregateFunction(argMax, DateTime64(9), Tuple(Int64, DateTime64(9))),
    `token_0_price`     AggregateFunction(argMax, Float32, Tuple(Int64, DateTime64(9))),
    `token_1_price`     AggregateFunction(argMax, Float32, Tuple(Int64, DateTime64(9))),
    `token_0_value`     AggregateFunction(argMax, Float64, Tuple(Int64, DateTime64(9))),
    `token_1_value`     AggregateFunction(argMax, Float64, Tuple(Int64, DateTime64(9))),
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax
)
ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in timeseries queries
    ORDER BY (`contract_address`, `timestamp`) -- queries should be to a specific pair id for max performance
SETTINGS index_granularity = 8192;

-- spacebox.dex_vaults_dex_balance_valued_by_minute_agg_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.dex_vaults_dex_balance_valued_by_minute_agg_writer TO spacebox.dex_vaults_dex_balance_valued_by_minute_agg AS
SELECT
    toStartOfInterval(t.`timestamp`, INTERVAL 1 MINUTE) as `timestamp`,
    minState(t.`height`) as `height_from`,
    maxState(t.`height`) as `height_to`,
    `contract_address`,
    argMaxState(t.`token_0_balance_before_deposit`, (t.`height`, t.`price_timestamp`)) as `token_0_balance`,
    argMaxState(t.`token_1_balance_before_deposit`, (t.`height`, t.`price_timestamp`)) as `token_1_balance`,
    argMaxState(t.`price_timestamp`, (t.`height`, t.`price_timestamp`)) as `price_timestamp`,
    argMaxState(t.`token_0_price`, (t.`height`, t.`price_timestamp`)) as `token_0_price`,
    argMaxState(t.`token_1_price`, (t.`height`, t.`price_timestamp`)) as `token_1_price`,
    argMaxState(t.`token_0_balance_before_deposit_value`, (t.`height`, t.`price_timestamp`)) as `token_0_value`,
    argMaxState(t.`token_1_balance_before_deposit_value`, (t.`height`, t.`price_timestamp`)) as `token_1_value`
FROM spacebox.dex_vaults_dex_balance_valued as t
WHERE t.`action` = 'dex_deposit'
GROUP BY `contract_address`, `timestamp`;

-- spacebox.dex_vaults_dex_balance_valued_by_minute view

CREATE VIEW spacebox.dex_vaults_dex_balance_valued_by_minute AS
    SELECT
        `timestamp`,
        minMerge(`height_from`) as `height_from`,
        maxMerge(`height_to`) as `height_to`,
        `contract_address`,
        argMaxMerge(`token_0_balance`) as `token_0_balance`,
        argMaxMerge(`token_1_balance`) as `token_1_balance`,
        argMaxMerge(`price_timestamp`) as `price_timestamp`,
        argMaxMerge(`token_0_price`) as `token_0_price`,
        argMaxMerge(`token_1_price`) as `token_1_price`,
        argMaxMerge(`token_0_value`) as `token_0_value`,
        argMaxMerge(`token_1_value`) as `token_1_value`
    FROM spacebox.dex_vaults_dex_balance_valued_by_minute_agg
    GROUP BY `contract_address`, `timestamp`;
