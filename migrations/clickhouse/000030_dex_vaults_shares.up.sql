
-- spacebox.dex_vaults_shares table

CREATE TABLE spacebox.dex_vaults_shares
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
    -- save boolean for credit/debit
    `credit`            Boolean MATERIALIZED `action` = 'deposit',
    `shares`            UInt128, -- shares delta
    `total_shares`      UInt128, -- total shares
    -- add index for timeseries queries
    INDEX `timestamp_index` (`timestamp`) TYPE minmax,
    -- add index for contract_address type queries
    INDEX `contract_address_index` (`contract_address`) TYPE bloom_filter,
    PROJECTION dex_vaults_shares_state (
        SELECT
            `contract_address`,
            argMax(`height`, `sort_key`) as `height`,
            argMax(`token_0_shares`, `sort_key`) as `token_0_shares`,
            argMax(`token_1_shares`, `sort_key`) as `token_1_shares`
        GROUP BY `contract_address`
    )
)
-- use ReplacingMergeTree ensure (eventually) no duplicates of the ORDER BY columns
ENGINE = ReplacingMergeTree()
ORDER BY (`height`, `block_part_index`, `tx_index`, `event_index`)
SETTINGS index_granularity = 8192;


-- spacebox.dex_vaults_shares dex_vaults_shares_state projection view

CREATE VIEW spacebox.dex_vaults_shares_state AS
    SELECT
        `contract_address`,
        argMax(`height`, `sort_key`) as `height`,
        argMax(`total_shares`, `sort_key`) as `shares`
    FROM spacebox.dex_vaults_shares
    GROUP BY `contract_address`;


-- spacebox.dex_vaults_shares_deposit_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_shares_deposit_writer TO spacebox.dex_vaults_shares (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `shares`            UInt128,
    `total_shares`      UInt128
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
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'minted_amount'), `event_attributes`), 'value')) AS `shares`,
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'total_shares'), `event_attributes`), 'value')) AS `total_shares`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_tf_mint_event_attributes) -> arrayMap(
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
                            -- is action="deposit"
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'action',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = 'deposit' AND
                            -- has total_shares>0
                            toUInt128OrZero(
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'total_shares',
                                        msg_event_attributes
                                    ),
                                    'value'
                                )
                            ) > 0 AND
                            -- matches tf_mint event contract address
                            has(
                                [
                                    JSONExtractString(
                                        arrayFirst(
                                            (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                                            msg_event_attributes
                                        ),
                                        'value'
                                    ),
                                    JSONExtractString(
                                        arrayFirst(
                                            (attr) -> JSONExtractString(attr, 'key') = 'from',
                                            msg_event_attributes
                                        ),
                                        'value'
                                    )
                                ],
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'mint_to_address',
                                        msg_tf_mint_event_attributes
                                    ),
                                    'value'
                                )
                            ) AND
                            -- matches tf_mint event amount
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'minted_amount',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = regexpExtract(
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'amount',
                                        msg_tf_mint_event_attributes
                                    ),
                                    'value'
                                ),
                                '^(\\d+)'
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
            -- get tf_mint event attributes (if it exists)
            arrayMap(
                (msg_event) -> JSONExtractArrayRaw(msg_event, 'attributes'),
                arrayFilter(
                    (msg_event) -> JSONExtractString(msg_event, 'type') = 'tf_mint',
                    `msg_events`
                )
            )
        )
    )
) AS `event_tuple`;


-- spacebox.dex_vaults_shares_withdrawal_writer source

CREATE MATERIALIZED VIEW spacebox.dex_vaults_shares_withdrawal_writer TO spacebox.dex_vaults_shares (
    `timestamp`         DateTime,
    `height`            Int64,
    `block_part_index`  Int8,
    `tx_index`          Int32,
    `event_index`       Int32,
    -- event data
    `action`            LowCardinality(String),
    `contract_address`  String,
    `shares`            UInt128,
    `total_shares`      UInt128
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
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'shares_burned'), `event_attributes`), 'value')) AS `shares`,
    toUInt128OrZero(JSONExtractString(arrayFirst(x -> (JSONExtractString(x, 'key') = 'total_shares'), `event_attributes`), 'value')) AS `total_shares`
FROM spacebox.message_event
ARRAY JOIN (
    -- Extract "message part" events with event_index
    arrayFlatten(
        arrayMap(
            (msg_tf_burn_event_attributes) -> arrayMap(
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
                            -- is action="withdrawal"
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'action',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = 'withdrawal' AND
                            -- has shares_burned>0
                            toUInt128OrZero(
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'shares_burned',
                                        msg_event_attributes
                                    ),
                                    'value'
                                )
                            ) > 0 AND
                            -- matches tf_burn event contract address
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = '_contract_address',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'burn_from_address',
                                    msg_tf_burn_event_attributes
                                ),
                                'value'
                            ) AND
                            -- matches tf_burn event amount
                            JSONExtractString(
                                arrayFirst(
                                    (attr) -> JSONExtractString(attr, 'key') = 'shares_burned',
                                    msg_event_attributes
                                ),
                                'value'
                            ) = regexpExtract(
                                JSONExtractString(
                                    arrayFirst(
                                        (attr) -> JSONExtractString(attr, 'key') = 'amount',
                                        msg_tf_burn_event_attributes
                                    ),
                                    'value'
                                ),
                                '^(\\d+)'
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
            -- get tf_burn event attributes (if it exists)
            arrayMap(
                (msg_event) -> JSONExtractArrayRaw(msg_event, 'attributes'),
                arrayFilter(
                    (msg_event) -> JSONExtractString(msg_event, 'type') = 'tf_burn',
                    `msg_events`
                )
            )
        )
    )
) AS `event_tuple`;