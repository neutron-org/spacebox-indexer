
-- spacebox.dex_vaults_message_event_create_denom table

CREATE TABLE spacebox.dex_vaults_message_event_create_denom
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
    `new_token_denom`           String,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
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

-- spacebox.dex_vaults_message_event_create_denom_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_message_event_create_denom_writer TO spacebox.dex_vaults_message_event_create_denom (
    `timestamp`                 DateTime,
    `height`                    Int64,
    `block_part_index`          Int8,
    `tx_index`                  Int32,
    `event_index`               Int32,
    -- event data
    `type`                      LowCardinality(String),
    `action`                    LowCardinality(String),
    `contract`                  String,
    `new_token_denom`           String
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
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'new_token_denom'), `event_attributes`), 'value') AS `new_token_denom`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_create_denom_event_attributes) -> arrayMap(
                (msg_event, msg_event_index) -> arrayMap(
                    (msg_event_attributes) -> (
                        -- event_tuple.1: event_index
                        toInt32(`msg_events_index_offset` + msg_event_index - 1),
                        -- event_tuple.2: event_type
                        JSONExtractString(msg_event, 'type'),
                        -- event_tuple.3: event_attributes
                        msg_event_attributes
                    ),
                    -- filter to only successful execution events
                    arrayFilter(
                        (msg_event_attributes) -> (
                            -- is action="create_token_reply_success"
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'action',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = 'create_token_reply_success' AND
                            -- matches create_denom event contract address
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'creator',
                                    msg_create_denom_event_attributes
                                ),
                                'value'
                            ) AND
                            -- matches create_denom event denom
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'new_token_denom',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'new_token_denom',
                                    msg_create_denom_event_attributes
                                ),
                                'value'
                            )
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
            ),
            -- get create_denom event attributes (if it exists)
            arrayMap(
                (msg_event) -> JSONExtractArrayRaw(msg_event, 'attributes'),
                arrayFilter(
                    (msg_event) -> JSONExtractString(msg_event, 'type') = 'create_denom',
                    `msg_events`
                )
            )
        )
    )
) AS `event_tuple`
