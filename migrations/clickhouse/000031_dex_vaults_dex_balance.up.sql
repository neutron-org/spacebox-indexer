
-- spacebox.dex_vaults_dex_balance table

CREATE TABLE spacebox.dex_vaults_dex_balance
(
    `timestamp`         DateTime,
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
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32,
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for contract_address type queries
    INDEX `contract_address_index` (`contract_address`) TYPE bloom_filter
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY `sort_key`
SETTINGS index_granularity = 8192;

-- spacebox.dex_vaults_dex_balance_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_balance_deposit_writer TO spacebox.dex_vaults_dex_balance (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32
) AS
WITH
    toFloat32OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'price_0_to_1'), `event_attributes`), 'value')) AS `price_ratio`,
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `event_attributes`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = '_contract_address'), `event_attributes`), 'value') AS `contract_address`,
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_0_balance'), `event_attributes`), 'value')) AS `token_0_balance`,
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_1_balance'), `event_attributes`), 'value')) AS `token_1_balance`,
    toFloat32OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_0_price'), `event_attributes`), 'value')) AS `token_0_price`,
    toFloat32OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'token_1_price'), `event_attributes`), 'value')) AS `token_1_price`,
    if(`token_1_price` > 0, `token_0_price` / `token_1_price`, `price_ratio`) AS `price_0_to_1`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_event, msg_event_index) -> arrayMap(
                (msg_event_attributes) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_events_index_offset` + msg_event_index - 1),
                    -- event_tuple.2: event_attributes
                    msg_event_attributes
                ),
                -- filter to only successful execution events
                arrayFilter(
                    (msg_event_attributes) -> (
                        -- is action="dex_deposit"
                        JSONExtractString(
                            arrayFirst(
                                (attr) -> JSONExtractString(attr, 'key') = 'action',
                                msg_event_attributes
                            ),
                            'value'
                        ) = 'dex_deposit' AND
                        -- has token_0_balance
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = 'token_0_balance',
                            msg_event_attributes
                        ) AND
                        -- has token_1_balance
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = 'token_1_balance',
                            msg_event_attributes
                        ) AND
                        -- has _contract_address
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                            msg_event_attributes
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
        )
    )
) AS `event_tuple`;

-- spacebox.dex_vaults_dex_balance_withdrawal_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_dex_balance_withdrawal_writer TO spacebox.dex_vaults_dex_balance (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `token_0_balance`   UInt128,
    `token_1_balance`   UInt128,
    `token_0_price`     Float32,
    `token_1_price`     Float32,
    `price_0_to_1`      Float32
) AS
WITH
    -- define event_tuple parts for row fields
    event_tuple.1 as `event_index`,
    event_tuple.2 as `event_attributes`
SELECT
    `timestamp`,
    `height`,
    `block_part_index`,
    `tx_index`,
    `event_index`,
    -- add event attributes
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'action'), `event_attributes`), 'value') AS `action`,
    JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = '_contract_address'), `event_attributes`), 'value') AS `contract_address`,
    0 AS `token_0_balance`,
    0 AS `token_1_balance`,
    0 AS `token_0_price`,
    0 AS `token_1_price`,
    0 AS `price_0_to_1`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_event, msg_event_index) -> arrayMap(
                (msg_event_attributes) -> (
                    -- event_tuple.1: event_index
                    toInt32(`msg_events_index_offset` + msg_event_index - 1),
                    -- event_tuple.2: event_attributes
                    msg_event_attributes
                ),
                -- filter to only successful execution events
                arrayFilter(
                    (msg_event_attributes) -> (
                        -- is action="dex_withdrawal"
                        JSONExtractString(
                            arrayFirst(
                                (attr) -> JSONExtractString(attr, 'key') = 'action',
                                msg_event_attributes
                            ),
                            'value'
                        ) = 'dex_withdrawal' AND
                        -- has _contract_address
                        arrayExists(
                            (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                            msg_event_attributes
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
        )
    )
) AS `event_tuple`;
