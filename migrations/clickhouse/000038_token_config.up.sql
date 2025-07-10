
-- spacebox.raw_token_data_from_skip definition

CREATE TABLE spacebox.raw_token_data_from_skip (
    `denom`                 String,
    `chain_id`              String,
    `origin_denom`          String,
    `origin_chain_id`       String,
    `trace`                 String,
    `is_cw20`               Boolean,
    `is_evm`                Boolean,
    `is_svm`                Boolean,
    `symbol`                String,
    `name`                  String,
    `logo_uri`              String,
    `decimals`              UInt8,
    `coingecko_id`          String,
    `token_contract`        String,
    `description`           String,
    `recommended_symbol`    String
)
ENGINE = ReplacingMergeTree()
ORDER BY `denom`
AS
SELECT
    `denom`,
    `chain_id`,
    `origin_denom`,
    `origin_chain_id`,
    `trace`,
    `is_cw20`,
    `is_evm`,
    `is_svm`,
    `symbol`,
    `name`,
    `logo_uri`,
    `decimals`,
    `coingecko_id`,
    `token_contract`,
    `description`,
    `recommended_symbol`
FROM file('../user_files/mainnet/skip-assets.json', 'JSONEachRow');

-- spacebox.token_config definition

CREATE TABLE spacebox.token_config (
    `denom`                 String,
    `origin_chain_id`       String,
    `origin_denom`          String,
    `name`                  String,
    `base_symbol`           String, -- eg. BTC
    `symbol`                String, -- eg. wBTC
    `recommended_symbol`    String, -- eg. wBTC.axl
    `decimals`              UInt8,
    `coingecko_id`          String,
    -- add projections for joining by symbol
    PROJECTION token_config_by_symbol (SELECT * ORDER BY `symbol`)
)
ENGINE = ReplacingMergeTree()
ORDER BY `denom`
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192
AS
WITH
    -- first make "symbol" more consistent by renaming Axelar wrapped assets
    -- and specific known cases
    if (
        `name` LIKE 'Wrapped %' OR
        source.`symbol` LIKE '%.axl' OR
        source.`symbol` IN ('WOSMO'),
        -- enforce lowercase 'w' as wrapping
        concat('w', extract(source.`recommended_symbol`, '^(?:[wW])?(.+)$')),
        source.`recommended_symbol`
    ) as `consistent_symbol`
SELECT
    `denom`,
    `origin_chain_id`,
    `origin_denom`,
    `name`,
    -- add improved resolution columns
    arrayStringConcat(
        extractGroups(
            COALESCE(`consistent_symbol`, ''),
            -- remove lowercase or whitelisted string prefix from symbol
            '(?:(?:[a-z]+|Solv)([A-Z-]{3,})|([a-zA-Z-]+))'
        )
    ) as `base_symbol`,
    -- remove dot-chain naming from consistent recommended symbols for "symbol"
    extract(`consistent_symbol`, '[a-zA-Z-]+') as `symbol`,
    `consistent_symbol` as `recommended_symbol`,
    `decimals`,
    `coingecko_id`
FROM spacebox.raw_token_data_from_skip as source;
