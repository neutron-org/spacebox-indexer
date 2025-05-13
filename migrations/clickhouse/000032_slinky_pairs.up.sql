
-- spacebox.slinky_pairs table

CREATE TABLE spacebox.slinky_pairs
(
    -- add pair_id for lookup queries
    `pair_id`			LowCardinality(String)
                        MATERIALIZED concat(`base`, '-', `quote`),
    `id`                UInt16,
    `base`              LowCardinality(String),
    `quote`             LowCardinality(String),
    `height_from`       AggregateFunction(min, Int64),
    `height_to`         AggregateFunction(max, Int64),
    `nonce`             AggregateFunction(max, UInt64)
)
    ENGINE = AggregatingMergeTree()
        ORDER BY (`base`, `quote`, `id`)
    SETTINGS index_granularity = 8192;

-- spacebox.slinky_pairs_writer source

CREATE MATERIALIZED VIEW IF NOT EXISTS spacebox.slinky_pairs_writer TO spacebox.slinky_pairs AS
SELECT
    toInt16OrZero(mapping_tuple.1)                      AS `id`,
    mapping_tuple.2                                     AS `base`,
    mapping_tuple.3                                     AS `quote`,
    minState(`height`)                                  AS `height_from`,
    maxState(`height`)                                  AS `height_to`,
    maxState(toUInt64OrZero(mapping_tuple.4))           AS `nonce`
FROM spacebox.raw_slinky_prices
ARRAY JOIN
    arrayMap(
        (mapping, price) -> (
            JSONExtractString(mapping, 'id'),
            JSONExtractString(mapping, 'currency_pair', 'Base'),
            JSONExtractString(mapping, 'currency_pair', 'Quote'),
            JSONExtractString(price, 'nonce')
        ),
        JSONExtractArrayRaw(`mappings`),
        JSONExtractArrayRaw(`prices`)
    ) as mapping_tuple
GROUP BY `base`, `quote`, `id`;

/*
-- how to query:
SELECT
    `id`,
    `base`,
    `quote`,
    minMerge(`height_from`) as `height_from`,
    maxMerge(`height_to`) as `height_to`,
    maxMerge(`nonce`) as `nonce`
FROM spacebox.slinky_pairs
GROUP BY `base`, `quote`, `id`
ORDER BY `id` ASC;
*/
