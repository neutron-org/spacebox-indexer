

-- spacebox.token_prices table

CREATE TABLE spacebox.token_prices
(
    `timestamp`         DateTime64(9),
    -- add height alias: as the height the price is valid from (for ASOF joins)
    `height`            ALIAS `height_from`,
    `height_from`       Int64,
    `height_to`         Int64,
    `symbol`            LowCardinality(String), -- symbol eg. BTC, wBTC, dATOM
    `quote_currency`    LowCardinality(String), -- probably 'USD'
    `price`             Float64, -- price in display token amount, eg. $/NTRN
    PROJECTION token_prices_state (
        SELECT
            argMax(`timestamp`, `height_to`) as `timestamp`,
            argMax(`height`, `height_to`) as `height`,
            `symbol`,
            `quote_currency`,
            argMax(`price`, `height_to`) as `price`
        GROUP BY `symbol`, `quote_currency`
    ),
    PROJECTION token_prices_first_state (
        SELECT
            argMin(`timestamp`, `height_to`) as `timestamp`,
            argMin(`height`, `height_to`) as `height`,
            `symbol`,
            `quote_currency`,
            argMin(`price`, `height_to`) as `price`
        GROUP BY `symbol`, `quote_currency`
    )
)
ENGINE = ReplacingMergeTree(`height_to`)
    PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in timeseries queries
    ORDER BY (`symbol`, `quote_currency`, `timestamp`) -- queries should be to a specific pair id for max performance
SETTINGS
    deduplicate_merge_projection_mode = 'rebuild',
    index_granularity = 8192;


-- spacebox.token_prices token_prices_state projection view

CREATE VIEW spacebox.token_prices_state AS
    SELECT
        argMax(`timestamp`, `height_to`) as `timestamp`,
        argMax(`height`, `height_to`) as `height`,
        `symbol`,
        `quote_currency`,
        argMax(`price`, `height_to`) as `price`
    FROM spacebox.token_prices
    GROUP BY `symbol`, `quote_currency`;

CREATE VIEW spacebox.token_prices_first_state AS
    SELECT
        argMin(`timestamp`, `height_to`) as `timestamp`,
        argMin(`height`, `height_to`) as `height`,
        `symbol`,
        `quote_currency`,
        argMin(`price`, `height_to`) as `price`
    FROM spacebox.token_prices
    GROUP BY `symbol`, `quote_currency`;


-- spacebox.token_prices_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.token_prices_writer TO spacebox.token_prices AS
WITH
    raw_slinky_prices_tuple.1 as `slinky_price_block_timestamp`,
    raw_slinky_prices_tuple.2 as `slinky_price_block_height`,
    raw_slinky_prices_tuple.3 as `slinky_price_base`,
    raw_slinky_prices_tuple.4 as `slinky_price_quote`,
    raw_slinky_prices_tuple.5 as `slinky_price`,
    raw_slinky_prices_tuple.6 as `slinky_price_decimals`
SELECT
    parseDateTime64BestEffortOrZero(`slinky_price_block_timestamp`) AS `timestamp`,
    toInt64OrZero(`slinky_price_block_height`)                      AS `height_from`,
    raw_slinky_price.`height`                                       AS `height_to`,
    `slinky_price_base`                                             AS `symbol`,
    `slinky_price_quote`                                            AS `quote_currency`,
    toFloat64(`slinky_price`)
        * exp10(-(toUInt8OrZero(`slinky_price_decimals`)))          AS `price`
FROM spacebox.raw_slinky_prices as raw_slinky_price
ARRAY JOIN
    arrayMap(
        (mapping, price) -> (
            JSONExtractString(price, 'price', 'block_timestamp'),
            JSONExtractString(price, 'price', 'block_height'),
            JSONExtractString(mapping, 'currency_pair', 'Base'),
            JSONExtractString(mapping, 'currency_pair', 'Quote'),
            JSONExtractString(price, 'price', 'price'),
            JSONExtractString(price, 'decimals')
        ),
        JSONExtractArrayRaw(`mappings`),
        JSONExtractArrayRaw(`prices`)
    ) as raw_slinky_prices_tuple
WHERE `height_from` > 0
    AND `symbol` IN (SELECT DISTINCT symbol FROM spacebox.token_config);
