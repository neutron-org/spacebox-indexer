-- 000001_raw.up.sql
-- spacebox.raw_block_topic definition

CREATE TABLE spacebox.raw_block_topic
(
    `message` String
)
    ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka:9093',
            kafka_topic_list = 'raw_block',
            kafka_group_name = 'spacebox',
            kafka_format = 'JSONAsString';

-- spacebox.raw_block definition

CREATE TABLE spacebox.raw_block
(
    `height`           Int64,
    `hash`             String,
    `num_txs`          Int64,
    `total_gas`        Int64,
    `proposer_address` String,
    `timestamp`        DateTime64(9),
    `signatures`       String
)
    ENGINE = ReplacingMergeTree
        ORDER BY (height)
        SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS raw_block_consumer TO spacebox.raw_block AS
SELECT JSONExtractInt(message, 'block', 'header', 'height')                                 AS height,
       JSONExtractString(message, 'hash')                                                   AS hash,
       JSONExtractInt(message, 'num_txs')                                                   AS num_txs,
       JSONExtractInt(message, 'total_gas')                                                 AS total_gas,
       JSONExtractString(message, 'proposer_address')                                       AS proposer_address,
       parseDateTime64BestEffortOrZero(JSONExtractString(message, 'block', 'header', 'time')) AS timestamp,
       JSONExtractString(message, 'block', 'last_commit', 'signatures')                     AS signatures
FROM spacebox.raw_block_topic
GROUP BY height, hash, num_txs, total_gas, proposer_address, timestamp, signatures;

-- spacebox.raw_block_txhash definition

CREATE TABLE spacebox.raw_block_txhash
(
    `height`           Int64,
    `tx_index`         Int32,
    `txhash`           String,
    `timestamp`        DateTime64(9)
)
    ENGINE = ReplacingMergeTree
        ORDER BY (height, tx_index)
        SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_block_txhash_consumer TO spacebox.raw_block_txhash AS
SELECT JSONExtractInt(message, 'block', 'header', 'height')                                 AS height,
       tx_index,
       txhash,
       parseDateTime64BestEffortOrZero(JSONExtractString(message, 'block', 'header', 'time')) AS timestamp
FROM spacebox.raw_block_topic
--  get tx hashes in the correct block order
ARRAY JOIN
    arrayMap(
        txBase64 -> hex(SHA256(base64Decode(JSONExtractString(txBase64)))),
        JSONExtractArrayRaw(message, 'block', 'data', 'txs')
    ) as txhash,
    arrayEnumerate(JSONExtractArrayRaw(message, 'block', 'data', 'txs')) as tx_index
GROUP BY height, tx_index, txhash, timestamp;

-- spacebox.raw_block_results_topic definition

CREATE TABLE spacebox.raw_block_results_topic
(
    `message` String
)
    ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka:9093',
            kafka_topic_list = 'raw_block_results',
            kafka_group_name = 'spacebox',
            kafka_format = 'JSONAsString';


-- spacebox.raw_block_results definition

CREATE TABLE spacebox.raw_block_results
(
    `height`                  Int64,
    `txs_results`             String,
    `finalize_block_events`   String,
    `validator_updates`       String,
    `consensus_param_updates` String,
    `timestamp`               DateTime64(9)
)
    ENGINE = ReplacingMergeTree
        ORDER BY height
        SETTINGS index_granularity = 8192;

-- spacebox.raw_transaction_topic definition

CREATE TABLE spacebox.raw_transaction_topic
(

    `message` String
)
    ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka:9093',
            kafka_topic_list = 'raw_transaction',
            kafka_group_name = 'spacebox',
            kafka_format = 'JSONAsString';


CREATE MATERIALIZED VIEW IF NOT EXISTS raw_block_results_consumer TO spacebox.raw_block_results AS
SELECT JSONExtractInt(message, 'height')                                      AS height,
       JSONExtractString(message, 'txs_results')                              AS txs_results,
       JSONExtractString(message, 'finalize_block_events')                    AS finalize_block_events,
       JSONExtractString(message, 'validator_updates')                        AS validator_updates,
       JSONExtractString(message, 'consensus_param_updates')                  AS consensus_param_updates,
       parseDateTime64BestEffortOrZero(JSONExtractString(message, 'timestamp')) AS timestamp
FROM spacebox.raw_block_results_topic
GROUP BY height, txs_results, finalize_block_events, validator_updates, consensus_param_updates,
         timestamp;

-- spacebox.raw_transaction definition

CREATE TABLE spacebox.raw_transaction
(
    `timestamp`  DateTime64(9),
    `height`     Int64,
    `txhash`     String,
    `codespace`  String,
    `code`       Int64,
    `raw_log`    String,
    `logs`       String,
    `info`       String,
    `gas_wanted` Int64,
    `gas_used`   Int64,
    `tx`         String,
    `events`     String,
    `signer`     String
)
    ENGINE = ReplacingMergeTree
        ORDER BY (
                  height,
                  txhash,
                  signer,
                  code)
        SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS raw_transaction_consumer TO spacebox.raw_transaction AS
SELECT parseDateTime64BestEffortOrZero(JSONExtractString(message, 'tx_response', 'timestamp')) AS timestamp,
       JSONExtractInt(message, 'tx_response', 'height')                                      AS height,
       JSONExtractString(message, 'tx_response', 'txhash')                                   AS txhash,
       JSONExtractString(message, 'tx_response', 'codespace')                                AS codespace,
       JSONExtractInt(message, 'tx_response', 'code')                                        AS code,
       JSONExtractString(message, 'tx_response', 'rawLog')                                   AS raw_log,
       JSONExtractString(message, 'tx_response', 'logs')                                     AS logs,
       JSONExtractString(message, 'tx_response', 'info')                                     AS info,
       JSONExtractInt(message, 'tx_response', 'gasWanted')                                   AS gas_wanted,
       JSONExtractInt(message, 'tx_response', 'gasUsed')                                     AS gas_used,
       JSONExtractString(message, 'tx_response', 'tx')                                       AS tx,
       JSONExtractString(message, 'tx_response', 'events')                                   AS events,
       JSONExtractString(message, 'signer')                                                  AS signer
FROM spacebox.raw_transaction_topic
GROUP BY timestamp, height, txhash, codespace, code, raw_log, logs, info, gas_wanted, gas_used, tx, events, signer;

-- spacebox.raw_genesis_topic definition

CREATE TABLE spacebox.raw_genesis_topic
(
    `message` String
)
    ENGINE = Kafka('kafka:9093',
                   'raw_genesis',
                   'spacebox',
                   'JSONAsString');

-- spacebox.raw_genesis definition

CREATE TABLE spacebox.raw_genesis
(
    `genesis_time`     DateTime64(9),
    `chain_id`         String,
    `initial_height`   Int64,
    `consensus_params` String,
    `app_hash`         String,
    `app_state`        String
)
    ENGINE = ReplacingMergeTree
        ORDER BY (genesis_time,
                  chain_id)
        SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW IF NOT EXISTS raw_genesis_consumer TO spacebox.raw_genesis AS
SELECT parseDateTime64BestEffortOrZero(JSONExtractString(message, 'genesis_time')) AS genesis_time,
       JSONExtractString(message, 'chain_id')                                    AS chain_id,
       JSONExtractInt(message, 'initial_height')                                 AS initial_height,
       JSONExtractString(message, 'consensus_params')                            AS consensus_params,
       JSONExtractString(message, 'app_hash')                                    AS app_hash,
       JSONExtractString(message, 'app_state')                                   AS app_state
FROM spacebox.raw_genesis_topic
GROUP BY genesis_time, chain_id, initial_height, consensus_params, app_hash, app_state;

-- spacebox.raw_slinky_prices_topic definition

CREATE TABLE spacebox.raw_slinky_prices_topic
(
    `message` String
)
    ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka:9093',
            kafka_topic_list = 'raw_slinky_prices',
            kafka_group_name = 'spacebox',
            kafka_format = 'JSONAsString';

-- spacebox.raw_slinky_prices definition

CREATE TABLE spacebox.raw_slinky_prices
(
    -- add pair_id for optimized queries
    `pair_id`			LowCardinality(String)
                        MATERIALIZED concat(`base`, '-', `quote`),
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `id`                UInt16,
    `base`              LowCardinality(String),
    `quote`             LowCardinality(String),
    `price`             UInt128,
    `decimals`          UInt8,
    `nonce`             UInt64,
    `quote_id`          Nullable(UInt16)
)
    ENGINE = ReplacingMergeTree
        PARTITION BY toYYYYMM(`timestamp`) -- allows skipping irrelevant months in timeseries queries
        ORDER BY (`pair_id`, `timestamp`) -- queries should contain `base` and `quote` in a single string for max performance
    SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_slinky_prices_consumer TO spacebox.raw_slinky_prices AS
SELECT
    parseDateTime64BestEffortOrZero(price_tuple.1)  AS `timestamp`,
    toInt64OrZero(price_tuple.2)                    AS `height`,
    toInt16OrZero(price_tuple.3)                    AS `id`,
    price_tuple.4                                   AS `base`,
    price_tuple.5                                   AS `quote`,
    toUInt128OrZero(price_tuple.6)                  AS `price`,
    toUInt8OrZero(price_tuple.7)                    AS `decimals`,
    toUInt64OrZero(price_tuple.8)                   AS `nonce`,
    -- add quote ID (when available) to double check any mis-matches
    toUInt16OrNull(price_tuple.9)                   AS `quote_id`
FROM spacebox.raw_slinky_prices_topic
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
        JSONExtractArrayRaw(message, 'mappings'),
        JSONExtractArrayRaw(message, 'prices')
    ) as price_tuple
    WHERE height > 0

-- spacebox.raw_dex_pool_metadata_topic definition

CREATE TABLE spacebox.raw_dex_pool_metadata_topic
(
    `message` String
)
    ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka:9093',
            kafka_topic_list = 'raw_dex_pool_metadata',
            kafka_group_name = 'spacebox',
            kafka_format = 'JSONAsString';

-- spacebox.raw_dex_pool_metadata definition

CREATE TABLE spacebox.raw_dex_pool_metadata
(
    -- add pair_id for optimized queries
    `pair_id`			LowCardinality(String)
                        MATERIALIZED concat(`token0`, '<>', `token1`),
    `timestamp`         DateTime64(9),
    `height`            Int64,
    `id`                UInt64,
    `tick`              Int64,
    `fee`               UInt64,
    `token0`            LowCardinality(String),
    `token1`            LowCardinality(String),
    -- add reverse lookup index
    INDEX `tick_fee_index` (`token0`, `token1`, `tick`, `fee`) TYPE set(0)
)
    ENGINE = ReplacingMergeTree
        ORDER BY (`id`)
    SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_dex_pool_metadata_consumer TO spacebox.raw_dex_pool_metadata AS
SELECT
    parseDateTime64BestEffortOrZero(JSONExtractString(message, 'timestamp')) AS `timestamp`,
    toInt64OrZero(JSONExtractString(message, 'height'))                     AS `height`,
    toUInt64OrZero(pool_metadata_tuple.1)                                   AS `id`,
    toInt64OrZero(pool_metadata_tuple.2)                                    AS `tick`,
    toUInt64OrZero(pool_metadata_tuple.3)                                   AS `fee`,
    pool_metadata_tuple.4                                                   AS `token0`,
    pool_metadata_tuple.5                                                   AS `token1`
FROM spacebox.raw_dex_pool_metadata_topic
ARRAY JOIN
    arrayMap(
        (pool_metadata) -> (
            JSONExtractString(pool_metadata, 'id'),
            JSONExtractString(pool_metadata, 'tick'),
            JSONExtractString(pool_metadata, 'fee'),
            JSONExtractString(pool_metadata, 'pair_id', 'token0'),
            JSONExtractString(pool_metadata, 'pair_id', 'token1')
        ),
        JSONExtractArrayRaw(message, 'pool_metadata')
    ) as pool_metadata_tuple
