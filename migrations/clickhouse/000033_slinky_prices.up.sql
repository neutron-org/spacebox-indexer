
-- spacebox.slinky_prices table

CREATE TABLE spacebox.slinky_prices
(
    `timestamp`         DateTime64(9),
    `query_height`      Int64,
    -- add height alias: the "query height" is the height the price is valid until
    `height_to`         ALIAS `query_height`,
    `height`            Int64,
    `id`                UInt16,
    `base`              LowCardinality(String),
    `quote`             LowCardinality(String),
    `price`             UInt128,
    `decimals`          UInt8,
    `nonce`             UInt64,
    `quote_id`          UInt16
)
    ENGINE = ReplacingMergeTree(`query_height`)
        PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in timeseries queries
        ORDER BY (`id`, `timestamp`) -- queries should be to a specific pair id for max performance
    SETTINGS index_granularity = 8192;

-- spacebox.slinky_prices_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.slinky_prices_writer TO spacebox.slinky_prices AS
SELECT
    parseDateTime64BestEffortOrZero(price_tuple.1)  AS `timestamp`,
    p.`height`                                      AS `query_height`,
    toInt64OrZero(price_tuple.2)                    AS `height`,
    toInt16OrZero(price_tuple.3)                    AS `id`,
    price_tuple.4                                   AS `base`,
    price_tuple.5                                   AS `quote`,
    toUInt128OrZero(price_tuple.6)                  AS `price`,
    toUInt8OrZero(price_tuple.7)                    AS `decimals`,
    toUInt64OrZero(price_tuple.8)                   AS `nonce`,
    -- add quote ID (when available) to double check any mis-matches
    toUInt16OrZero(price_tuple.9)                   AS `quote_id`
FROM spacebox.raw_slinky_prices as p
ARRAY JOIN
    arrayMap(
        (mapping, price) -> (
            JSONExtractString(price, 'price', 'block_timestamp'),
            JSONExtractString(price, 'price', 'block_height'),
            JSONExtractString(price, 'id'),
            JSONExtractString(mapping, 'currency_pair', 'Base'),
            JSONExtractString(mapping, 'currency_pair', 'Quote'),
            JSONExtractString(price, 'price', 'price'),
            JSONExtractString(price, 'decimals'),
            JSONExtractString(price, 'nonce'),
            JSONExtractString(mapping, 'id')
        ),
        JSONExtractArrayRaw(`mappings`),
        JSONExtractArrayRaw(`prices`)
    ) as price_tuple;
WHERE height > 0
