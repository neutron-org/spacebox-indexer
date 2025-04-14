
-- spacebox.dex_vaults_message_event_instantiate table

CREATE TABLE spacebox.dex_vaults_message_event_instantiate
(
    `timestamp`                 DateTime,
    `height`                    Int64,
    `block_part_index`          Int8,
    `tx_index`                  Int32,
    `event_index`               Int32,
    -- event data
    `type`                      LowCardinality(String),
    `action`                    LowCardinality(String),
    `contract`                  String,
    `owner`                     Array(String),
    `max_blocks_stale_token_a`  UInt64,
    `max_blocks_stale_token_b`  UInt64,
    `token_0_denom`             LowCardinality(String),
    `token_0_symbol`            LowCardinality(String),
    `token_0_quote_currency`    LowCardinality(String),
    `token_1_denom`             LowCardinality(String),
    `token_1_symbol`            LowCardinality(String),
    `token_1_quote_currency`    LowCardinality(String),
    `pool_id`                   LowCardinality(String),
    `deposit_cap`               UInt256,
    `oracle_contract`           String,
    `imbalance`                 UInt64,
    `fee_tier_config`           String,
    `timestamp_stale`           UInt64,
    `paused`                    Boolean,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for token pair specific queries
    INDEX `pair_index` (`token_0_denom`, `token_1_denom`) TYPE set(0),
    -- add index for contract queries
    INDEX `user_pair_pool_index` (`contract`) TYPE bloom_filter(0.01)
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (
    -- the minimum unique parts needed to describe a unique Dex event
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`
)
SETTINGS index_granularity = 8192;

-- spacebox.dex_vaults_message_event_instantiate_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_message_event_instantiate_writer TO spacebox.dex_vaults_message_event_instantiate (
    `timestamp`                 DateTime,
    `height`                    Int64,
    `block_part_index`          Int8,
    `tx_index`                  Int32,
    `event_index`               Int32,
    -- event data
    `type`                      LowCardinality(String),
    `action`                    LowCardinality(String),
    `contract`                  String,
    `owner`                     Array(String),
    `max_blocks_stale_token_a`  UInt64,
    `max_blocks_stale_token_b`  UInt64,
    `token_0_denom`             LowCardinality(String),
    `token_0_symbol`            LowCardinality(String),
    `token_0_quote_currency`    LowCardinality(String),
    `token_1_denom`             LowCardinality(String),
    `token_1_symbol`            LowCardinality(String),
    `token_1_quote_currency`    LowCardinality(String),
    `pool_id`                   LowCardinality(String),
    `deposit_cap`               UInt256,
    `oracle_contract`           String,
    `imbalance`                 UInt64,
    `fee_tier_config`           String,
    `timestamp_stale`           UInt64,
    `paused`                    Boolean
) AS
WITH
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `event_type`,
    event_tuple.3 as `event_attributes`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    `event_type` as `type`,
    -- add event attributes
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = '_contract_address'), `event_attributes`), 'value') AS `contract`,
    extractAllGroupsVertical(
        JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'owner'), `event_attributes`), 'value'),
        'Addr\(\"([a-z]+[a-z0-9]{30,})"\)'
    ) AS `owner`,
    toUInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'max_blocks_stale_token_a'), `event_attributes`), 'value')) AS `max_blocks_stale_token_a`,
    toUInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'max_blocks_stale_token_b'), `event_attributes`), 'value')) AS `max_blocks_stale_token_b`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_0_denom'), `event_attributes`), 'value') AS `token_0_denom`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_0_symbol'), `event_attributes`), 'value') AS `token_0_symbol`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_0_quote_currency'), `event_attributes`), 'value') AS `token_0_quote_currency`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_1_denom'), `event_attributes`), 'value') AS `token_1_denom`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_1_symbol'), `event_attributes`), 'value') AS `token_1_symbol`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_1_quote_currency'), `event_attributes`), 'value') AS `token_1_quote_currency`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'pool_id'), `event_attributes`), 'value') AS `pool_id`,
    toUInt256OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'deposit_cap'), `event_attributes`), 'value')) AS `deposit_cap`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'oracle_contract'), `event_attributes`), 'value') AS `oracle_contract`,
    toUInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'imbalance'), `event_attributes`), 'value')) AS `imbalance`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'fee_tier_config'), `event_attributes`), 'value') AS `fee_tier_config`,
    toUInt64(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'timestamp_stale'), `event_attributes`), 'value')) AS `timestamp_stale`,
    toBool(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'paused'), `event_attributes`), 'value')) AS `paused`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_event, msg_event_index) -> arrayMap(
                (msg_event_attributes) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_events_index_offset` + msg_event_index - 1),
                    -- event_tuple.2: event_type
                    JSONExtractString(msg_event, 'type'),
                    -- event_tuple.3: event_attributes
                    JSONExtractArrayRaw(msg_event, 'attributes')
                ),
                -- filter to only possible supervault instantiate events
                arrayFilter(
                    (msg_event_attributes) -> (
                        JSONExtractString(
                            arrayFirst(
                                (attr) -> JSONExtractString(attr, 'key') = 'action',
                                msg_event_attributes
                            ),
                            'value'
                        ) = 'instantiate IMM'
                    ),
                    arrayMap(
                        (msg_event) -> JSONExtractArrayRaw(msg_event, 'attributes'),
                        arrayFilter(
                            msg_event -> JSONExtractString(msg_event, 'type') = 'wasm',
                            [msg_event]
                        )
                    )
                )
            ),
            -- enumerate each (msg_event, msg_event_index) within a message part
            `msg_events`,
            arrayEnumerate(`msg_events`)
        )
    )
) AS `event_tuple`
