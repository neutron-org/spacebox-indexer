

-- spacebox.price_by_vault_denom table

CREATE TABLE spacebox.price_by_vault_denom
(
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `contract_address`  LowCardinality(String),
    `token_0_price`     Float64, -- price in denom (μtoken) amount, eg. $/untrn
    `token_1_price`     Float64, -- price in denom (μtoken) amount, eg. $/untrn
    PROJECTION price_by_vault_denom_state (
        SELECT
            argMax(`timestamp`, `height`) as `timestamp`,
            `contract_address`,
            argMax(`token_0_price`, `height`) as `token_0_price`,
            argMax(`token_1_price`, `height`) as `token_1_price`
        GROUP BY `contract_address`
    ),
    PROJECTION price_by_vault_denom_first_state (
        SELECT
            argMin(`timestamp`, `height`) as `timestamp`,
            `contract_address`,
            argMin(`token_0_price`, `height`) as `token_0_price`,
            argMin(`token_1_price`, `height`) as `token_1_price`
        GROUP BY `contract_address`
    ))
ENGINE = ReplacingMergeTree(`height`)
    PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in timeseries queries
    ORDER BY (`contract_address`, `timestamp`) -- queries should be to a specific vault for max performance
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;

-- spacebox.price_by_vault_denom price_by_vault_denom_state projection view

CREATE VIEW spacebox.price_by_vault_denom_state AS
    SELECT
        argMax(`timestamp`, `height`) as `timestamp`,
        `contract_address`,
        argMax(`token_0_price`, `height`) as `token_0_price`,
        argMax(`token_1_price`, `height`) as `token_1_price`
    FROM spacebox.price_by_vault_denom
    GROUP BY `contract_address`;

CREATE VIEW spacebox.price_by_vault_denom_first_state AS
    SELECT
        argMin(`timestamp`, `height`) as `timestamp`,
        `contract_address`,
        argMin(`token_0_price`, `height`) as `token_0_price`,
        argMin(`token_1_price`, `height`) as `token_1_price`
    FROM spacebox.price_by_vault_denom
    GROUP BY `contract_address`;


-- spacebox.price_by_vault_denom_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.price_by_vault_denom_writer TO spacebox.price_by_vault_denom AS
SELECT 
    `timestamp`,
    `height`,
    `contract_address`,
    `token_0_price`,
    `token_1_price`
FROM spacebox.dex_vaults_dex_balance as balance
WHERE `action` = 'dex_deposit'
  AND (`token_0_price` > 0 OR `token_1_price` > 0);


-- spacebox.price_by_vault_denom_by_minute_agg table

CREATE TABLE spacebox.price_by_vault_denom_by_minute_agg
(
    `timestamp`         DateTime,
    -- add height alias: as the height the price is valid from (for ASOF joins)
    `height`            ALIAS `height_from`,
    `height_from`       AggregateFunction(min, Int64),
    `height_to`         AggregateFunction(max, Int64),
    `contract_address`  LowCardinality(String),
    `token_0_price`     AggregateFunction(argMax, Float64, Int64), -- price in denom (μtoken) amount, eg. $/untrn
    `token_1_price`     AggregateFunction(argMax, Float64, Int64)  -- price in denom (μtoken) amount, eg. $/untrn
)
ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in timeseries queries
    ORDER BY (`contract_address`, `timestamp`) -- queries should be to a specific pair id for max performance
SETTINGS index_granularity = 8192;

-- spacebox.price_by_vault_denom_by_minute_agg_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.price_by_vault_denom_by_minute_agg_writer TO spacebox.price_by_vault_denom_by_minute_agg AS
SELECT
    toStartOfInterval(t.`timestamp`, INTERVAL 1 MINUTE) as `timestamp`,
    minState(t.`height`) as `height_from`,
    maxState(t.`height`) as `height_to`,
    `contract_address`,
    argMaxState(t.`token_0_price`, t.`height`) as `token_0_price`,
    argMaxState(t.`token_1_price`, t.`height`) as `token_1_price`
FROM spacebox.price_by_vault_denom as t
GROUP BY `contract_address`, `timestamp`;

-- spacebox.price_by_vault_denom_by_minute view

CREATE VIEW spacebox.price_by_vault_denom_by_minute AS
    SELECT
        `timestamp`,
        minMerge(`height_from`) as `height_from`,
        maxMerge(`height_to`) as `height_to`,
        `contract_address`,
        argMaxMerge(`token_0_price`) as `token_0_price`,
        argMaxMerge(`token_1_price`) as `token_1_price`
    FROM spacebox.price_by_vault_denom_by_minute_agg
    GROUP BY `contract_address`, `timestamp`;
